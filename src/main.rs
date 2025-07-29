//! A program executing a query and printing the result as CSV to standard out.
//! Requires the `anyhow` and `csv` crates in your Cargo.toml.

use dotenv::dotenv;                          // Loads environment variables from a `.env` file at startup.
use anyhow::Error;                           // A convenient error type that can represent any error (`?` will convert).
use odbc_api::{                              // Import types from the odbc_api crate:
    buffers::TextRowSet,                     //   TextRowSet: buffer type for fetching rows in batches as text.
    ConnectionOptions,                       //   ConnectionOptions: options for opening ODBC connections.
    Cursor,                                  //   Cursor: for iterating over query results.
    Environment,                             //   Environment: the ODBC environment; must be created once per process.
    ResultSetMetadata,                       //   ResultSetMetadata: metadata about columns (used for names).
};
use std::{                                   // Import standard-library items:
    env, fs::File
};

/// Maximum number of rows fetched per batch. Fetching in batches is more efficient than row-by-row.
const BATCH_SIZE: usize = 200;

fn main() -> Result<(), Error> {
    //───────────────────────────────────────────────────────────────────
    // 1) Load environment variables from `.env`
    //───────────────────────────────────────────────────────────────────
    dotenv().ok();                            // Try to read `.env`; ignore errors if the file is missing or malformed.

    //───────────────────────────────────────────────────────────────────
    // 2) Read required DB settings from the environment (or panic)
    //───────────────────────────────────────────────────────────────────
    let server  = env::var("DB_SERVER").expect("DB_SERVER must be set");
    let port    = env::var("DB_PORT").expect("DB_PORT must be set");
    let name    = env::var("DB_NAME").expect("DB_NAME must be set");
    let user    = env::var("DB_USER").expect("DB_USER must be set");
    let pass    = env::var("DB_PASS").expect("DB_PASS must be set");

    //───────────────────────────────────────────────────────────────────
    // 3) Prepare CSV writer in a CSV file
    //───────────────────────────────────────────────────────────────────
    let file = File::create("output.csv")?;
    let mut writer = csv::Writer::from_writer(file);
    // The CSV writer will escape fields, add commas, newlines, etc.

    //───────────────────────────────────────────────────────────────────
    // 4) Initialize the ODBC Environment (only once per process)
    //───────────────────────────────────────────────────────────────────
    let environment = Environment::new()?;
    // This sets up ODBC’s global state (thread safety, version, etc.)

    //───────────────────────────────────────────────────────────────────
    // 5) Build a DSN‑less connection string at runtime
    //───────────────────────────────────────────────────────────────────
    let conn_str = format!(
        // { and } in the Driver name must be doubled to escape in format!()
        "Driver={{ODBC Driver 18 for SQL Server}};\
         Server={},{};\
         Database={};\
         UID={};\
         PWD={};\
         TrustServerCertificate=Yes;",
        server,   // host name or IP
        port,     // TCP port
        name,     // default database
        user,     // login user
        pass      // login password
    );
    // TrustServerCertificate=Yes skips CA validation but still encrypts the connection.

    //───────────────────────────────────────────────────────────────────
    // 6) Open the connection using the full connection string
    //───────────────────────────────────────────────────────────────────
    let connection = environment
        .connect_with_connection_string(&conn_str, ConnectionOptions::default())?;
    // `connect_with_connection_string` is used when you provide the entire DSN-less string.

    //───────────────────────────────────────────────────────────────────
    // 7) Execute a simple query without parameters
    //───────────────────────────────────────────────────────────────────
    match connection.execute("SELECT * FROM FCF_NOTIFICATION", (), None)? {
        Some(mut cursor) => {
            //───────────────────────────────────────────────────────────
            // 7a) Write header row: fetch column names, collect into Vec<String>
            //───────────────────────────────────────────────────────────
            let header: Vec<String> = cursor
                .column_names()?              // Returns an iterator over &str names
                .collect::<Result<_, _>>()?;  // Collect into Vec<String>, propagating any error
            writer.write_record(header)?;     // Write the header row as CSV

            //───────────────────────────────────────────────────────────
            // 7b) Prepare a text‑mode row buffer for batched fetching
            //───────────────────────────────────────────────────────────
            let mut buffers = TextRowSet::for_cursor(
                BATCH_SIZE,    // how many rows per batch
                &mut cursor,   // the cursor whose schema we’re buffering
                Some(4096),    // max bytes per text column (4 KiB)
            )?;

            // Bind the buffer to the cursor: subsequent `fetch()` calls populate `buffers`
            let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;

            //───────────────────────────────────────────────────────────
            // 7c) Loop over fetched batches until none remain
            //───────────────────────────────────────────────────────────
            while let Some(batch) = row_set_cursor.fetch()? {
                // For each row index in the current batch (0 .. num_rows):
                for row_index in 0..batch.num_rows() {
                    // Build a record by iterating columns and pulling the text slice
                    let record = (0..batch.num_cols()).map(|col_index| {
                        // at(col, row) returns Option<&[u8]>; unwrap_or empty slice on NULL
                        batch.at(col_index, row_index).unwrap_or(&[])
                    });
                    // Write this row to CSV (it accepts any IntoIterator of AsRef<[u8]>)
                    writer.write_record(record)?;
                }
            }
        }
        None => {
            // The query returned no result set at all (rare for SELECT)
            eprintln!("Query returned empty. No CSV output created.");
        }
    }

    writer.flush()?;
    println!("Done! Results written to output.csv");

    Ok(())
}
