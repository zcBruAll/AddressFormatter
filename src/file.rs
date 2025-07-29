use csv::WriterBuilder;                     // Allow to export data to CSV
use std::fs::File;                          // Allow managing files on filesystem
use anyhow::{Error, Ok};                    // A convenient error type that can represent any error (`?` will convert).

pub fn write_csv_record(filename: &str, content: Vec<Vec<String>>) -> Result<(), Error> {
    // Prepare the .csv output file
    let file = File::create(filename)?;
    let mut writer = WriterBuilder::new()
        .delimiter(b';').from_writer(file);

    // For each row in result, write it in the csv file
    for row in content {
        writer.write_record(row)?;
    }

    // Flush the content in the file
    writer.flush()?;

    Ok(())
}