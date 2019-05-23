
use postgres::{Connection, types::ToSql};
use log::*;
use ndarray::Array2;

use crate::config::Config;
use crate::error::{GeneratorError, Result};

pub fn load_data(config: &Config, data_file: Option<&str>)
        -> Result<Array2<f64>> {
    // Build the connection to the DB
    let conn = config.connect_db()?;

    // Read the data from the DB
    match read_data_from_db(&conn, &config.db.table_name)? {
        Some(data) => {
            Ok(data)
        },
        None => {
            // Check if the data file is provided
            let data_file = match data_file {
                Some(f) => f,
                None => {
                    error!("missing data file");
                    return Err(GeneratorError::Message(
                        format!("please provide the data file")));
                }
            };

            // Read the data from the data file
            let data = read_data_from_file(data_file)?;
            let col_num = data.cols();

            // Build the DB
            create_schema(&conn, &config.db.table_name, col_num)?;

            // Insert the data to the DB
            insert_data(&conn, &config.db.table_name, &data)?;

            Ok(data)
        }
    }
}

fn read_data_from_db(conn: &Connection, table_name: &str)
        -> Result<Option<Array2<f64>>> {
    let sql = format!("SELECT * FROM {}", table_name);

    // New transaction
    let mut txn_config = postgres::transaction::Config::new();
    txn_config.read_only(true);
    let transaction = conn.transaction_with(&txn_config)?;

    // Get the scan
    let scan = match conn.query(&sql, &[]) {
        Ok(s) => s,
        Err(e) => {
            // Check if it's due to non-existance of the table
            match e.as_db() {
                Some(db_err) => {
                    if db_err.message.contains("does not exist") {
                        warn!("DB Warning: {}", db_err.message);
                        return Ok(None);
                    }
                },
                None => { }
            }
            return Err(GeneratorError::DbError(e));
        }
    };

    // Read the data
    let (row_num, col_num) = (scan.len(), scan.columns().len());
    let mut data: Array2<f64> = Array2::zeros((row_num, col_num));
    let mut ri = 0;
    for row in scan.iter() {
        for ci in 0 .. row.len() {
            data[[ri, ci]] = row.get(ci);
        }
        ri += 1;
        if ri % 100 == 0 {
            info!("{} records have been read from the DB.", ri);
        }
    }

    // Commit
    transaction.commit()?;

    Ok(Some(data))
}

fn read_data_from_file(data_file: &str)
        -> Result<Array2<f64>> {
    let mut rows = Vec::new();
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);
    let mut csv_reader = builder.from_path(data_file)?;

    info!("Reading data from \"{}\"", data_file);

    // Read data from the csv file
    for result in csv_reader.records() {
        let record = result?;
        let row: std::result::Result<Vec<f64>, _> = record.iter()
                .map(|s| s.parse()).collect();
        rows.push(row?);
    }

    // Convert to Array2
    let shape = (rows.len(), rows[0].len());
    let raw_vec: Vec<f64> = rows.into_iter().flatten().collect();
    let data = Array2::from_shape_vec(shape, raw_vec)?;

    info!("Finished reading data from \"{}\"", data_file);
    Ok(data)
}

fn create_schema(conn: &Connection, table_name: &str, col_num: usize) -> Result<()> {
    info!("Creating schema for \"{}\"", table_name);

    // New transaction
    let transaction = conn.transaction()?;

    // Create table
    let mut sql = String::new();
    sql.push_str("CREATE TABLE ");
    sql.push_str(table_name);
    sql.push_str(" (");
    for idx in 0 .. col_num {
        sql.push_str("c");
        sql.push_str(&idx.to_string());
        sql.push_str(" DOUBLE PRECISION");
        if idx < col_num - 1 {
            sql.push_str(", ");
        }
    }
    sql.push_str(");");
    transaction.execute(&sql, &[])?;

    // Create index
    for idx in 0 .. col_num {
        let sql = format!("CREATE INDEX idx_{}_c{} ON {} (c{});",
            table_name, idx, table_name, idx);
        transaction.execute(&sql, &[])?;
    }

    // Commit
    transaction.commit()?;

    info!("Finished creating schema for \"{}\"", table_name);
    Ok(())
}

fn insert_data(conn: &Connection, table_name: &str, data: &Array2<f64>) -> Result<()> {
    info!("Inserting data into \"{}\"", table_name);

    // Generate the SQL
    let col_num = data.cols();
    let mut sql = String::new();
    sql.push_str("INSERT INTO ");
    sql.push_str(table_name);
    sql.push_str(" (");
    for idx in 0 .. col_num {
        sql.push_str("c");
        sql.push_str(&idx.to_string());
        if idx < col_num - 1 {
            sql.push_str(", ");
        }
    }
    sql.push_str(") VALUES (");
    for idx in 0 .. col_num {
        sql.push_str("$");
        sql.push_str(&(idx + 1).to_string());
        if idx < col_num - 1 {
            sql.push_str(", ");
        }
    }
    sql.push_str(");");

    // New transaction
    let transaction = conn.transaction()?;
    let stmt = transaction.prepare(&sql)?;

    for ri in 0 .. data.rows() {
        let row = data.row(ri);
        let row_vec: Vec<_> = row.iter().map(|f| f as &dyn ToSql).collect();
        stmt.execute(&row_vec[..])?;

        if (ri + 1) % 1000 == 0 {
            info!("{} records have been inserted.", (ri + 1));
        }
    }

    // Commit
    transaction.commit()?;

    info!("Finished inserting data into \"{}\"", table_name);
    Ok(())
}