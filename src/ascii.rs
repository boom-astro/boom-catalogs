use crate::processor::Processor;
use crate::types::HasCoordinates;
use anyhow::Result;
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

// Example usage to find the correct column positions:
// Uncomment this function and call it to help determine column positions
#[allow(dead_code)]
fn print_line_with_positions(line: &str) {
    println!("Position ruler:");
    for i in (0..line.len()).step_by(10) {
        print!("{:<10}", i);
    }
    println!();
    for i in 0..line.len() {
        print!("{}", i % 10);
    }
    println!("\n{}", line);
}

pub trait FromAsciiRow {
    fn from_line(line: &str) -> Result<Self, String>
    where
        Self: Sized;
}

struct TableReader<R: BufRead> {
    reader: R,
}

impl<R: BufRead> TableReader<R> {
    fn new(reader: R) -> Self {
        Self { reader }
    }

    fn rows<T: FromAsciiRow>(self) -> impl Iterator<Item = Result<T, String>> {
        self.reader
            .lines()
            .enumerate()
            .filter_map(|(line_num, line_result)| {
                match line_result {
                    Ok(line) => {
                        // print_line_with_positions(&line);
                        let trimmed = line.trim_end();
                        if trimmed.is_empty() {
                            None // Skip empty lines
                        } else {
                            Some(T::from_line(&line).map_err(|e| {
                                format!("Line {}: {} (\"{}\")", line_num + 1, e, line)
                            }))
                        }
                    }
                    Err(e) => Some(Err(format!("Line {}: IO error - {}", line_num + 1, e))),
                }
            })
    }
}

fn open_table<P: AsRef<Path>>(path: P) -> std::io::Result<TableReader<Box<dyn BufRead>>> {
    let file = File::open(&path)?;
    if let Some(ext) = path.as_ref().extension() {
        if ext == "gz" {
            let decoder = flate2::read::GzDecoder::new(file);
            let reader = BufReader::new(decoder);
            return Ok(TableReader::new(Box::new(reader)));
        }
    }
    let reader = BufReader::new(file);
    Ok(TableReader::new(Box::new(reader)))
}

fn estimate_lines_in_file(path: &str) -> Result<usize> {
    // In the gzip case, we assume a 4:1 compression ratio
    let metadata = std::fs::metadata(path)?;
    let file_size = if path.ends_with(".gz") {
        (metadata.len() as usize) * 4
    } else {
        metadata.len() as usize
    };
    let file = File::open(path)?;
    let reader: Box<dyn BufRead> = if path.ends_with(".gz") {
        let decoder = flate2::read::GzDecoder::new(file);
        Box::new(BufReader::new(decoder))
    } else {
        Box::new(BufReader::new(file))
    };
    let mut lines = reader.lines();
    let first_line = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("File is empty"))??;
    let line_length = first_line.len() + 1; // +1 for newline character
    let estimated_lines = file_size / line_length;
    Ok(estimated_lines)
}

pub async fn process_ascii<T>(
    mongodb_uri: String,
    db_name: String,
    collection_name: String,
    ascii_path: String,
    num_workers: usize,
    batch_size: usize,
    channel_capacity: usize,
    drop_existing_collection: bool,
    init_indexes: bool,
) -> Result<()>
where
    T: Serialize + Send + 'static + for<'de> Deserialize<'de> + FromAsciiRow + HasCoordinates,
{
    // Check that the ASCII path exists
    if !Path::new(&ascii_path).exists() {
        anyhow::bail!("ASCII file does not exist: {}", ascii_path);
    }

    // Collect files to process
    let files: Vec<String> = if Path::new(&ascii_path).is_dir() {
        // If it's a directory, collect all valid files
        fs::read_dir(&ascii_path)?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                path.is_file() && {
                    let name = path.to_string_lossy();
                    name.ends_with(".dat")
                        || name.ends_with(".ascii")
                        || name.ends_with(".dat.gz")
                        || name.ends_with(".ascii.gz")
                        || name.ends_with(".gz") // Accept any .gz file
                }
            })
            .map(|path| path.to_string_lossy().to_string())
            .collect()
    } else {
        // Single file - validate extension
        let valid = ascii_path.ends_with(".dat")
            || ascii_path.ends_with(".ascii")
            || ascii_path.ends_with(".dat.gz")
            || ascii_path.ends_with(".ascii.gz")
            || ascii_path.ends_with(".gz");

        if !valid {
            anyhow::bail!(
                "ASCII file must have .dat, .ascii, .gz extension: {}",
                ascii_path
            );
        }
        vec![ascii_path.clone()]
    };

    if files.is_empty() {
        anyhow::bail!("No valid files found in: {}", ascii_path);
    }

    println!("Found {} files to process", files.len());

    let processor = Processor::new::<T>(
        mongodb_uri,
        db_name,
        collection_name,
        num_workers,
        batch_size,
        channel_capacity,
        drop_existing_collection,
        init_indexes,
    )
    .await?;
    let (s, workers) = processor.init_workers::<T>();

    // Estimate total lines across all files
    let num_rows: usize = files
        .iter()
        .filter_map(|f| estimate_lines_in_file(f).ok())
        .sum();

    let progress_bar = ProgressBar::new(num_rows as u64)
        .with_message("Processing ASCII files")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );

    let mut success_count = 0;
    let mut error_count = 0;

    for (file_idx, file_path) in files.iter().enumerate() {
        println!(
            "Processing file {}/{}: {}",
            file_idx + 1,
            files.len(),
            file_path
        );

        let reader = open_table(file_path)?;

        for result in reader.rows() {
            match result {
                Ok(obj) => {
                    match s.send(obj).await {
                        Ok(_) => {
                            progress_bar.inc(1);
                        }
                        Err(e) => {
                            eprintln!("Failed to send record to workers: {}", e);
                            break;
                        }
                    }
                    success_count += 1;
                }
                Err(e) => {
                    error_count += 1;
                    eprintln!("❌ Error in {}: {}", file_path, e);
                    break;
                }
            }
        }
    }

    println!("✅ Successfully parsed: {} rows", success_count);
    if error_count > 0 {
        println!("❌ Failed to parse: {} rows", error_count);
    }

    // Close the sender to signal workers to finish
    drop(s);
    // Wait for all workers to complete
    let _ = processor.close_workers(workers).await;

    Ok(())
}
