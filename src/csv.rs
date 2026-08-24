use crate::processor::Processor;
use crate::types::HasCoordinates;
use anyhow::Result;
use csv::ReaderBuilder;
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

fn create_reader(boxed_reader: Box<dyn Read>) -> csv::Reader<Box<dyn Read>> {
    ReaderBuilder::new()
        .comment(Some(b'#'))
        .has_headers(true)
        .from_reader(boxed_reader)
}

// CSV files can be very large and have no mean to get their length
// without reading the whole file. To provide a progress bar we
// estimate the number of lines in the file by reading the first
// 100,000 lines and calculating the average line length, then
// we use the file size to estimate the total number of lines.
fn estimate_lines_in_file(path: &str) -> Result<usize> {
    let metadata = std::fs::metadata(path)?;
    // let file_size = metadata.len() as usize;
    // if the file is gzipped, the file size is not accurate,
    // so we estimate the total file size by assuming a compression
    // ratio of 3:1
    let file_size = if path.ends_with(".gz") {
        (metadata.len() as usize) * 3
    } else {
        metadata.len() as usize
    };

    let mut rdr = {
        if path.ends_with(".gz") {
            let file = File::open(path)?;
            let decoder = flate2::read::GzDecoder::new(file);
            let boxed_reader: Box<dyn Read> = Box::new(BufReader::new(decoder));
            ReaderBuilder::new()
                .comment(Some(b'#'))
                .has_headers(true)
                .from_reader(boxed_reader)
        } else {
            let file = File::open(path)?;
            let boxed_reader: Box<dyn Read> = Box::new(BufReader::new(file));
            ReaderBuilder::new()
                .comment(Some(b'#'))
                .has_headers(true)
                .from_reader(boxed_reader)
        }
    };

    let mut total_length = 0;
    let mut line_count = 0;

    for result in rdr.records().take(100_000) {
        let record = result?;
        total_length += record.as_byte_record().len();
        line_count += 1;
    }

    if line_count == 0 {
        return Ok(0);
    }
    // if line_count is less than 100,000 then we read
    // the whole file and we can return the actual count
    if line_count < 100_000 {
        return Ok(line_count);
    }

    let average_line_length = total_length / line_count;
    let estimated_lines = file_size / average_line_length;

    Ok(estimated_lines)
}

pub async fn process_csv<T>(
    mongodb_uri: String,
    db_name: String,
    collection_name: String,
    csv_path: String,
    num_workers: usize,
    batch_size: usize,
    channel_capacity: usize,
    init_indexes: bool,
) -> Result<(), anyhow::Error>
where
    T: Serialize + Send + 'static + for<'de> Deserialize<'de> + std::fmt::Debug + HasCoordinates,
{
    // Check that the CSV path exists
    if !Path::new(&csv_path).exists() {
        anyhow::bail!("CSV file does not exist: {}", csv_path);
    }
    // Check that the CSV file has a .csv extension
    if !(csv_path.ends_with(".csv") || csv_path.ends_with(".csv.gz")) {
        anyhow::bail!("CSV file must have .csv or .csv.gz extension: {}", csv_path);
    }

    let num_rows = estimate_lines_in_file(&csv_path)?;

    let processor = Processor::new(
        mongodb_uri,
        db_name,
        collection_name,
        num_workers,
        batch_size,
        channel_capacity,
    )
    .await?;
    let (s, workers) = processor.init_workers();

    let mut rdr = {
        let boxed_reader: Box<dyn Read> = if csv_path.ends_with(".gz") {
            let file = File::open(&csv_path)?;
            let decoder = flate2::read::GzDecoder::new(file);
            Box::new(BufReader::new(decoder))
        } else {
            let file = File::open(&csv_path)?;
            Box::new(BufReader::new(file))
        };
        create_reader(boxed_reader)
    };

    let progress_bar = ProgressBar::new(num_rows as u64)
        .with_message("Processing CSV file")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );

    let mut line_count = 0;
    for result in rdr.deserialize::<T>() {
        let record = match result {
            Ok(rec) => rec,
            Err(e) => {
                // let's print the string value of that record
                let str_record = rdr.records().nth(line_count).unwrap()?;
                eprintln!(
                    "Error deserializing record at line {}: {:?}",
                    line_count + 1,
                    str_record
                );
                return Err(anyhow::anyhow!("Error deserializing record: {}", e));
            }
        };
        line_count += 1;
        match s.send(record).await {
            Ok(_) => {
                progress_bar.inc(1);
            }
            Err(e) => {
                eprintln!("Failed to send record to workers: {}", e);
                break;
            }
        }
    }

    // Close the sender to signal workers to finish
    drop(s);
    // Wait for all workers to complete
    let _ = processor.close_workers(workers).await;

    if init_indexes {
        processor.init_indexes::<T>().await?;
    }

    Ok(())
}
