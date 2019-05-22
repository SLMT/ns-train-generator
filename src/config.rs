use std::fs::File;
use std::io::Read;

use serde::Deserialize;
use postgres::{Connection, TlsMode};
use log::*;

use crate::error::Result;

#[derive(Deserialize, Debug)]
pub struct Config {
    pub db: DbConfig,
    pub generator: GeneratorConfig,
}

#[derive(Deserialize, Debug)]
pub struct DbConfig {
    pub username: String,
    pub password: String,
    pub db_name: String,
    pub table_name: String,
    pub port: String,
    pub host: String,
}

#[derive(Deserialize, Debug)]
pub struct GeneratorConfig {
    pub high_mean: Vec<i32>,
    pub low_mean: Vec<i32>,
    pub high_variance: Vec<i32>,
    pub low_variance: Vec<i32>,
}


impl Config {
    pub fn from_file(path: &str) -> Result<Config> {
        // Read the file
        let mut config_file = File::open(&path)?;
        let mut config_str = String::new();
        config_file.read_to_string(&mut config_str)?;
        Ok(toml::from_str(&config_str)?)
    }

    pub fn connect_db(&self) -> Result<Connection> {
        let url = if self.db.password == "" {
            format!("postgresql://{}@{}:{}/{}",
                self.db.username, self.db.host,
                self.db.port, self.db.db_name)
        } else {
            format!("postgresql://{}:{}@{}:{}/{}",
                self.db.username, self.db.password,
                self.db.host, self.db.port, self.db.db_name)
        };
        debug!("connecting to {}", url);
        let conn = Connection::connect(url, TlsMode::None)?;
        Ok(conn)
    } 
}