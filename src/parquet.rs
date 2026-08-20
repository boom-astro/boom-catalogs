use crate::{processor::Processor, types::HasCoordinates};
use anyhow::Result;
use indicatif::ProgressBar;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub trait ParquetRowBatch {
    fn from_dataframe(df: &polars::prelude::DataFrame) -> Result<Vec<Self>>
    where
        Self: Sized;
}

/// Estimate the number of rows in a parquet file by reading metadata
fn estimate_rows_in_parquet(path: &str) -> Result<usize> {
    let file = std::fs::File::open(path)?;
    let mut reader = ParquetReader::new(file);

    // Get row count from metadata without reading the entire file
    match reader.num_rows() {
        Ok(count) => Ok(count),
        Err(_) => {
            // Fallback: try to get schema and estimate
            // This shouldn't normally happen with valid parquet files
            Ok(0)
        }
    }
}

pub async fn process_parquet<T>(
    mongodb_uri: String,
    db_name: String,
    collection_name: String,
    parquet_path: String,
    num_workers: usize,
    batch_size: usize,
    channel_capacity: usize,
    init_indexes: bool,
) -> Result<(), anyhow::Error>
where
    T: Serialize
        + Send
        + 'static
        + for<'de> Deserialize<'de>
        + std::fmt::Debug
        + ParquetRowBatch
        + HasCoordinates,
{
    // Check that the Parquet path exists
    if !Path::new(&parquet_path).exists() {
        anyhow::bail!("Parquet file does not exist: {}", parquet_path);
    }
    // Check that the Parquet file has a .parquet extension
    if !parquet_path.ends_with(".parquet") {
        anyhow::bail!("File must have .parquet extension: {}", parquet_path);
    }

    println!(
        "Estimating number of rows in Parquet file: {}",
        parquet_path
    );
    let num_rows = estimate_rows_in_parquet(&parquet_path)?;
    println!("Estimated number of rows: {}", num_rows);

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

    // Read the parquet file
    // let file = std::fs::File::open(&parquet_path)?;
    // let mut reader = ParquetReader::new(file);

    let progress_bar = ProgressBar::new(num_rows as u64)
        .with_message("Processing Parquet file")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );

    let mut line_count = 0;
    let chunk_size = 100_000; // Adjust chunk size as needed
    // loop over dataframe in slices
    while line_count < num_rows {
        let length = std::cmp::min(chunk_size, num_rows - line_count);
        // let slice = df.slice(line_count as i64, length);
        // let records = T::from_dataframe(&slice)?;
        let file = std::fs::File::open(&parquet_path)?;
        let reader = ParquetReader::new(file);
        let slice = reader.with_slice(Some((line_count, length))).finish()?;
        let records = T::from_dataframe(&slice)?;
        for record in records {
            match s.send(record).await {
                Ok(_) => {
                    progress_bar.inc(1);
                }
                Err(e) => {
                    eprintln!("Failed to send record to workers: {}", e);
                    break;
                }
            }
            line_count += 1;
        }
    }

    println!("Finished reading Parquet file.");

    // Close the sender to signal workers to finish
    drop(s);
    // Wait for all workers to complete
    let _ = processor.close_workers(workers).await;

    if init_indexes {
        processor.init_indexes::<T>().await?;
    }

    progress_bar.finish_with_message("Parquet processing complete");

    Ok(())
}
