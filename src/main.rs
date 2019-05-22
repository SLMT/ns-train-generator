
mod error;
mod config;
mod init;
mod generate;

use clap::{Arg, App};
use log::*;

use error::Result;
use config::Config;

fn main() {
    pretty_env_logger::init();

    let matches = App::new("NS Train Generator")
                       .version("1.0")
                       .author("Yu-Shan Lin <yslin@datalab.cs.nthu.edu.tw>")
                       .about("The generator that generates the training data set for neural storage project.")
                       .arg(Arg::with_name("OUTPUT FILE")
                            .help("Sets the path to the output data file")
                            .index(1)
                            .required(true))
                       .arg(Arg::with_name("DATA FILE")
                            .short("d")
                            .long("data")
                            .help("Sets the path to the input data file")
                            .takes_value(true))
                       .arg(Arg::with_name("CONFIG FILE")
                            .short("c")
                            .long("config")
                            .help("Sets the path to a config file")
                            .takes_value(true))
                       .get_matches();

    let out_file = matches.value_of("OUTPUT FILE").unwrap();
    let data_file = matches.value_of("DATA FILE");
    let config_file = matches.value_of("CONFIG FILE").unwrap_or("config.toml");
    
    match execute(out_file, data_file, config_file) {
        Ok(_) => println!("The generator finishes the job."),
        Err(e) => eprintln!("The generator exits with an error:\n{:#?}", e)
    }
}

fn execute(out_file: &str, data_file: Option<&str>, config_file: &str)
        -> Result<()> {
    
    let config = Config::from_file(&config_file)?;

    // Step 1: Read the DB (build the DB if it doesn't exist)
    info!("Loading all the data from the DB");
    let rows = init::load_data(&config, data_file)?;
    info!("Finished loading all the data from the DB");

    // Step 2: Generate training data
    info!("Generating training data set");
    generate::gen_training_data(&config, &rows)?;
    info!("Finished generating training data set");
    
    Ok(())
}
