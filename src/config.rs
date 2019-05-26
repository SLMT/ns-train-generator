use std::fs::File;
use std::io::Read;

use serde::Deserialize;
use postgres::{Connection, TlsMode};
use log::*;

use crate::error::Result;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub db: DbConfig,
    pub generator: GeneratorConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DbConfig {
    pub username: String,
    pub password: String,
    pub db_name: String,
    pub table_name: String,
    pub port: String,
    pub host: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GeneratorConfig {
    pub fields: GeneratorFields,
    pub normal: GeneratorNormal,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GeneratorFields {
    pub agg_fields: Vec<i32>,
    pub select_fields: Vec<i32>,
    pub group_fields: Vec<i32>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GeneratorNormal {
    pub means: Vec<Vec<f64>>,
    pub std_devs: Vec<Vec<f64>>,
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