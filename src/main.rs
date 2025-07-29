mod db;

use csv::WriterBuilder;                     // Allow to export data to CSV
use dotenv::dotenv;                         // Loads environment variables from a `.env` file at startup.
use anyhow::Error;                          // A convenient error type that can represent any error (`?` will convert).
use std::{                                  // Import standard-library items:
    env,                                    //      Handles environment variables and arguments
    fs::File                                //      Provide access to open files
};
use db::SqlClient;

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

    // Prepare the .csv output file
    let file = File::create("output.csv")?;
    let mut writer = WriterBuilder::new()
        .delimiter(b';').from_writer(file);

    // Initialize the SQL client
    let client = SqlClient::new(&server, &port, &name, &user, &pwd, BATCH_SIZE)?;

    // Retrieve the result of the query with the header
    let result = client.execute_query("SELECT TOP 15 * FROM FCF_NOTIFICATION", true)?;

    // For each row in result, write it in the csv file
    for row in result {
        writer.write_record(row)?;
    }

    // Flush the content in the file
    writer.flush()?;
    println!("Done! Results written to output.csv");

    // Return Ok if no error occured
    Ok(())
}
