
use postgres::{Connection, TlsMode, types::ToSql};

use log::*;

use crate::config::Config;
use crate::error::{GeneratorError, Result};

pub fn load_data(config: &Config, data_file: Option<&str>)
        -> Result<Vec<Vec<f64>>> {
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
            let col_num = data[0].len();

            // Build the DB
            create_schema(&conn, &config.db.table_name, col_num)?;

            // Insert the data to the DB
            insert_data(&conn, &config.db.table_name, &data)?;

            Ok(data)
        }
    }
}

fn read_data_from_db(conn: &Connection, table_name: &str)
        -> Result<Option<Vec<Vec<f64>>>> {
    let mut rows = Vec::new();
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
                        warn!("DB Error: {}", db_err.message);
                        return Ok(None);
                    }
                },
                None => { }
            }
            return Err(GeneratorError::DbError(e));
        }
    };

    // Read the data
    let mut count = 0;
    for row in scan.iter() {
        let mut row_vec: Vec<f64> = Vec::with_capacity(row.len());
        for ci in 0 .. row.len() {
            row_vec.push(row.get(ci));
        }
        rows.push(row_vec);
        count += 1;
        if count % 100 == 0 {
            info!("{} records have been read from the DB.", count);
        }
    }

    // Commit
    transaction.commit()?;

    Ok(Some(rows))
}

fn read_data_from_file(data_file: &str)
        -> Result<Vec<Vec<f64>>> {
    let mut rows = Vec::new();
    let mut builder = csv::ReaderBuilder::new();
    builder.has_headers(false);
    let mut csv_reader = builder.from_path(data_file)?;

    info!("Reading data from \"{}\"", data_file);

    for result in csv_reader.records() {
        let record = result?;
        let row: std::result::Result<Vec<f64>, _> = record.iter()
                .map(|s| s.parse()).collect();
        rows.push(row?);
    }

    info!("Finished reading data from \"{}\"", data_file);
    Ok(rows)
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

fn insert_data(conn: &Connection, table_name: &str, rows: &Vec<Vec<f64>>) -> Result<()> {
    info!("Inserting data into \"{}\"", table_name);

    // Generate the SQL
    let col_num = rows[0].len();
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

    let mut count = 0;
    for row in rows {
        let ref_vec: Vec<_> = row.iter().map(|f| f as &dyn ToSql).collect();
        stmt.execute(&ref_vec[..])?;
        count += 1;
        if count % 1000 == 0 {
            info!("{} records have been inserted.", count);
        }
    }

    // Commit
    transaction.commit()?;

    info!("Finished inserting data into \"{}\"", table_name);
    Ok(())
}