
use postgres::{types::ToSql, stmt::Statement};
use log::*;
use ndarray::{Array2, ArrayView1, ArrayView2, Axis};
use rand::distributions::{Normal, Distribution};

use crate::error::{GeneratorError, Result};
use crate::config::Config;

pub fn gen_training_data(config: &Config, thread_count: usize, data: Array2<f64>, output_file: &str) -> Result<()> {
    // Partition the data
    let trimmed_data = select(&data, &config.generator.fields.select_fields)?;
    let partitions = partition(&trimmed_data, thread_count);

    // Run [high, low]
    let mut count = 0;
    let normal = config.generator.normal.clone();
    for (means, stds) in normal.means.iter().zip(normal.std_devs) {
        // Process each partition
        let mut threads = Vec::new();
        for part_id in 0 .. thread_count {
            let partition = partitions[part_id].to_owned();
            let config = config.clone();
            let means = means.clone();
            let stds = stds.clone();
            let thread = std::thread::spawn(move || {
                process_partition(&config, partition, &means, &stds)
            });
            threads.push(thread);
        }

        // Wait for the threads
        let mut x_list = Vec::new();
        let mut y_list = Vec::new();
        for thread in threads {
            let (x, y) = thread.join().unwrap()?;
            x_list.push(x);
            y_list.push(y);
        }

        // Save to a CSV file
        let x_file_name = format!("{}-{}-X.csv", output_file, count);
        let y_file_name = format!("{}-{}-Y.csv", output_file, count);
        save_to_csv(&x_list, &x_file_name)?;
        save_to_csv(&y_list, &y_file_name)?;

        // Print info
        info!("Finish (mean, std) combination {}.", count);
        count += 1;
    }

    Ok(())
}

fn process_partition(config: &Config, partition: Array2<f64>, means: &[f64], stds: &[f64])
        -> Result<(Array2<f64>, Array2<f64>)> {
    // Generate ranges
    let ranges = generate_ranges(partition.view(), means, stds);

    // Get a read-only transaction
    let conn = config.connect_db()?;
    let mut txn_config = postgres::transaction::Config::new();
    txn_config.read_only(true);
    let txn = conn.transaction_with(&txn_config)?;

    // Prepare the SQL
    let sql = generate_sql(&config.db.table_name, &config.generator.fields.select_fields,
        &config.generator.fields.agg_fields);
    let stmt = txn.prepare(&sql)?;

    // Query DB for all the ranges
    let mut params: Vec<f64> = vec![0.0; config.generator.fields.select_fields.len() * 2];
    let mut y: Array2<f64> = Array2::zeros((partition.rows(), config.generator.fields.agg_fields.len() * 3 + 1));
    for xi in 0 .. ranges.rows() {
        convert_to_parameters(&mut params, ranges.row(xi), &config.generator.fields.group_fields);
        let (count, others) = read_true_answer(&params, &stmt)?;

        // Save Y
        y[[xi, 0]] = count as f64;
        for yi in 1 .. y.cols() {
            y[[xi, yi]] = others[yi - 1];
        }

        if (xi + 1) % 1000 == 0 {
            info!("{}/{} training pair are generated.", xi + 1, ranges.rows());
        }
    }

    // Commit the transaction
    txn.commit()?;

    Ok((ranges, y))
}

fn select(data: &Array2<f64>, select_fields: &[i32]) -> Result<Array2<f64>> {
    let mut cols = Vec::new();
    let view = data.view();
    for field in select_fields {
        cols.push(view.slice(s![.., *field..(*field+1)]));
    }
    Ok(ndarray::stack(Axis(1), &cols)?)
}

fn partition(data: &Array2<f64>, num_parts: usize) -> Vec<ArrayView2<f64>> {
    let mut partitions = Vec::new();

    let row_per_part = data.rows() / num_parts;
    let mut remaining = data.view();
    for _ in 1 .. num_parts {
        let cut_point = if row_per_part > remaining.rows() {
            remaining.rows()
        } else {
            row_per_part
        };
        let (before, after) = remaining.split_at(Axis(0), cut_point);
        partitions.push(before);
        remaining = after;
    }
    partitions.push(remaining);

    partitions
}

fn generate_ranges(data: ArrayView2<f64>, means: &[f64], stds: &[f64]) -> Array2<f64> {
    // Generate bias
    let mut range_bias: Array2<f64> = Array2::zeros((data.rows(), data.cols()));
    let mut random = rand::thread_rng();
    let mut col_id = 0;
    for (mean, std) in means.iter().zip(stds) {
        let normal = Normal::new(*mean, *std);
        for row_id in 0 .. range_bias.rows() {
            range_bias[[row_id, col_id]] = normal.sample(&mut random);
        }
        col_id += 1;
    }
    let range_bias = range_bias.map(|x| x.abs());

    // Generate ranges
    let lower_bound = &data - &range_bias;
    let upper_bound = &data + &range_bias;
    let range_bias = range_bias * 2.0;
    let ranges = stack![Axis(1), lower_bound, upper_bound, range_bias];

    ranges
}

fn generate_sql(table_name: &str, select_fields: &[i32], agg_fields: &[i32]) -> String {
    let mut sums = String::new();
    let mut maxs = String::new();
    let mut mins = String::new();
    let mut predicates = String::new();
    let mut param_count = 1;

    for col_id in select_fields {
        let col_str = col_id.to_string();

        // Project fields
        if agg_fields.contains(&col_id) {
            sums = sums + "SUM(c" + &col_str + "), ";
            maxs = maxs + "MAX(c" + &col_str + "), ";
            mins = mins + "MIN(c" + &col_str + "), ";
        }

        // Predicates
        predicates = predicates + "c" + &col_str + " >= $" +
            &param_count.to_string();
        param_count += 1;
        predicates = predicates + " AND c" + &col_str + " <= $" +
            &param_count.to_string();
        param_count += 1;
        predicates.push_str(" AND ");
    }

    // Merge
    let mut sql = String::new();
    sql = sql + "SELECT COUNT(c0), " + &sums + &maxs +
        &mins[..mins.len()-2] + " FROM " + table_name +
        " WHERE " + &predicates[..predicates.len()-5] +
        ";";
    sql
}

fn convert_to_parameters(parameters: &mut Vec<f64>, ranges: ArrayView1<f64>, group_fields: &[i32]) {
    let col_count = parameters.len() / 2;
    for ci in 0 .. col_count {
        let lower = ranges[ci];
        let upper = ranges[col_count + ci];
        if group_fields.contains(&(ci as i32)) {
            let middle = (lower + upper) / 2.0;
            parameters[2 * ci] = middle;
            parameters[2 * ci + 1] = middle;
        } else {
            parameters[2 * ci] = lower;
            parameters[2 * ci + 1] = upper;
        }
    }
}

fn read_true_answer(ranges: &Vec<f64>, stmt: &Statement) -> Result<(i64, Vec<f64>)> {
    let ranges: Vec<_> = ranges.iter().map(|f| f as &dyn ToSql).collect();
    let scan = stmt.query(&ranges[..])?;
    match scan.iter().next() {
        Some(row) => {
            let count: i64 = row.get(0);
            let mut result: Vec<f64> = Vec::new();
            for i in 1 .. row.len() {
                if count > 0 {
                    result.push(row.get(i));
                } else {
                    result.push(0.0);
                }
            }
            Ok((count, result))
        },
        None => {
            error!("WTF!!");
            Err(GeneratorError::Message(format!("WTF!")))
        }
    }
}

fn save_to_csv(arrays: &Vec<Array2<f64>>, file_path: &str) -> Result<()> {
    let mut writer = csv::Writer::from_path(file_path)?;

    for array in arrays {
        for ri in 0 .. array.rows() {
            let row = array.row(ri);
            writer.write_record(row.iter().map(|val| val.to_string()))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_generating_sql() {
        let agg_fields = vec![0, 1];
        let select_fields = vec![0, 1, 2, 3, 4];
        let sql = generate_sql("table", &select_fields, &agg_fields);
        let answer = concat!("SELECT COUNT(c0), SUM(c0), SUM(c1), ",
            "MAX(c0), MAX(c1), MIN(c0), MIN(c1) FROM table WHERE ",
            "c0 >= $1 AND c0 <= $2 AND c1 >= $3 AND c1 <= $4 AND ",
            "c2 >= $5 AND c2 <= $6 AND c3 >= $7 AND c3 <= $8 AND ",
            "c4 >= $9 AND c4 <= $10;");
        assert_eq!(answer, sql);
    }
}