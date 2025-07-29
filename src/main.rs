mod db;
mod file;

use dotenv::dotenv;                         // Loads environment variables from a `.env` file at startup.
use anyhow::Error;                          // A convenient error type that can represent any error (`?` will convert).
use std::env;                               // Handles environment variables and arguments

use db::SqlClient;
use file::write_csv_record;

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

    // Retrieve the result of the query with the header
    let result = client.execute_query("SELECT TOP 15 * FROM FCF_NOTIFICATION", true)?;

    // Write result in CSV
    write_csv_record("output.csv", result)?;
    println!("Done! Results written to output.csv");

    // Return Ok if no error occured
    Ok(())
}
