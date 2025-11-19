use crate::processor::Processor;
use crate::types::HasCoordinates;
use anyhow::Result;
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
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

fn open_table<P: AsRef<Path>>(path: P) -> std::io::Result<TableReader<BufReader<File>>> {
    let file = File::open(path)?;
    Ok(TableReader::new(BufReader::new(file)))
}

// fn main() -> std::io::Result<()> {
//     let reader = open_table("/home/theodlz/work/zvar/data/vsx.dat")?;

//     println!("Parsing data rows...\n");

//     let mut success_count = 0;
//     let mut error_count = 0;

//     for result in reader.rows() {
//         match result {
//             Ok(obj) => {
//                 success_count += 1;
//                 println!("✅ Parsed: {:?}", obj);
//                 println!();
//             }
//             Err(e) => {
//                 error_count += 1;
//                 eprintln!("❌ Error: {}", e);
//                 eprintln!();
//                 break;
//             }
//         }
//     }

//     println!("✅ Successfully parsed: {} rows", success_count);
//     if error_count > 0 {
//         println!("❌ Failed to parse: {} rows", error_count);
//     }

//     Ok(())
// }

fn estimate_lines_in_file(path: &str) -> Result<usize> {
    // get the metadata to find the file size
    let metadata = std::fs::metadata(path)?;
    let file_size = metadata.len() as usize;
    // just read the first line (all lines are the exact same length in this file)
    let file = File::open(path)?;
    let reader = BufReader::new(file);
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
    // Check that the ASCII file has a .dat or .ascii extension
    if !ascii_path.ends_with(".dat") && !ascii_path.ends_with(".ascii") {
        anyhow::bail!(
            "ASCII file must have .dat or .ascii extension: {}",
            ascii_path
        );
    }

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

    let num_rows = estimate_lines_in_file(&ascii_path)?;

    let reader = open_table(ascii_path)?;

    let progress_bar = ProgressBar::new(num_rows as u64)
        .with_message("Processing ASCII file")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );

    // for chunk_start in (0..num_rows).step_by(batch_size) {
    //     let chunk_end = (chunk_start + batch_size).min(num_rows);
    //     let range = chunk_start..chunk_end;
    //     let rows = T::read_batch(&tlb_hdu, &mut fptr, range)?;
    //     for row in rows {
    //         match s.send(row).await {
    //             Ok(_) => {
    //                 progress_bar.inc(1);
    //             }
    //             Err(e) => {
    //                 eprintln!("Failed to send record to workers: {}", e);
    //                 break;
    //             }
    //         }
    //     }
    // }
    let mut success_count = 0;
    let mut error_count = 0;

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
                eprintln!("❌ Error: {}", e);
                eprintln!();
                break;
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
