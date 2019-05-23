
use postgres::{types::ToSql, stmt::Statement};
use log::*;
use ndarray::Array2;

use crate::error::{GeneratorError, Result};
use crate::config::Config;

struct Fields {
    agg_fields: Vec<usize>,
    select_fields: Vec<usize>,
    group_fields: Vec<usize>
}

pub fn gen_training_data(config: &Config, rows: Array2<f64>) -> Result<()> {
    let fields = Fields {
        agg_fields: vec![0],
        select_fields: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        group_fields: vec![],
    };
    let ranges = vec![1, 0, 2, 0, 40, 10, 25, 5, 65, 10, 65, 10, 1, 1, 125, 10, 
        16, 3, 120, 10, 225, 20, 250, 20, 1, 0, 3, 0, 8, 1, 6, 1].iter().map(|x| x.clone() as f64).collect();

    // Get a read-only transaction
    let conn = config.connect_db()?;
    let mut txn_config = postgres::transaction::Config::new();
    txn_config.read_only(true);
    let txn = conn.transaction_with(&txn_config)?;

    let sql = generate_sql(&config.db.table_name, &fields);
    let stmt = txn.prepare(&sql)?;
    let (count, others) = read_true_answer(&ranges, &stmt)?;
    dbg!(count);
    dbg!(others);

    txn.commit()?;

    Ok(())
}

fn generate_sql(table_name: &str, fields: &Fields) -> String {
    let mut sums = String::new();
    let mut maxs = String::new();
    let mut mins = String::new();
    let mut predicates = String::new();
    let mut param_count = 1;

    for col_id in &fields.select_fields {
        let col_str = col_id.to_string();

        // Project fields
        if fields.agg_fields.contains(&col_id) {
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

fn partition() {
    // Partition the data

    // Generate bias from normal distribution

    // [lower, upper, range]
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

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_generating_sql() {
        let fields = Fields {
            agg_fields: vec![0, 1],
            select_fields: vec![0, 1, 2, 3, 4],
            group_fields: vec![2],
        };
        let sql = generate_sql("table", &fields);
        let answer = concat!("SELECT COUNT(c0), SUM(c0), SUM(c1), ",
            "MAX(c0), MAX(c1), MIN(c0), MIN(c1) FROM table WHERE ",
            "c0 >= $1 AND c0 <= $2 AND c1 >= $3 AND c1 <= $4 AND ",
            "c2 >= $5 AND c2 <= $6 AND c3 >= $7 AND c3 <= $8 AND ",
            "c4 >= $9 AND c4 <= $10;");
        assert_eq!(answer, sql);
    }
}