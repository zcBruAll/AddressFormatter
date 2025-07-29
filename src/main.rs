mod db;
mod file;
mod address;

use dotenv::dotenv;                         // Loads environment variables from a `.env` file at startup.
use anyhow::Error;                          // A convenient error type that can represent any error (`?` will convert).
use std::env;                               // Handles environment variables and arguments

use db::SqlClient;
use address::{
    UnstructuredAddress,
    StructuredAddress
};

use crate::address::get_unstructured_addresses;

/// Maximum number of rows fetched per batch. Fetching in batches is more efficient than row-by-row.
const BATCH_SIZE: usize = 200;

fn main() -> Result<(), Error> {
    // Loads the .env file
    dotenv().ok();

    // Load env values or panic if one is missing
    let server  = env::var("DB_SERVER").expect("DB_SERVER must be set");
    let port    = env::var("DB_PORT").expect("DB_PORT must be set");
    let name    = env::var("DB_NAME").expect("DB_NAME must be set");
    let user    = env::var("DB_USER").expect("DB_USER must be set");
    let pwd    = env::var("DB_PASS").expect("DB_PASS must be set");

    // Initialize the SQL client
    let client = SqlClient::new(&server, &port, &name, &user, &pwd, BATCH_SIZE)?;

    let unstructured_addresses: Vec<UnstructuredAddress> = get_unstructured_addresses("FCF_DEMANDS", "TOP 5 IDDEMAND", &["RECEIVER1", "RECEIVER2", "RECEIVER3", "RECEIVER4", "RECEIVER5"], &client)?;

    for unstructured in unstructured_addresses {
        let structured: StructuredAddress = unstructured.clone().try_into()?;
        println!("UNSTRUCTURED");
        println!("{:#?}", unstructured);
        println!("STRUCTURED");
        println!("{:#?}", structured);
    }

    // Return Ok if no error occured
    Ok(())
}
