use crate::{processor::Processor, types::HasCoordinates};
use anyhow::Result;
use fitsio::{FitsFile, hdu::HduInfo};
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub trait FitsRowBatch {
    fn read_batch(
        hdu: &fitsio::hdu::FitsHdu,
        fptr: &mut FitsFile,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<Self>>
    where
        Self: Sized;
}

pub async fn process_fits<T>(
    mongodb_uri: String,
    db_name: String,
    collection_name: String,
    fits_path: String,
    num_workers: usize,
    batch_size: usize,
    channel_capacity: usize,
    drop_existing_collection: bool,
    init_indexes: bool,
) -> Result<()>
where
    T: Serialize + Send + 'static + for<'de> Deserialize<'de> + FitsRowBatch + HasCoordinates,
{
    // Check that the FITS path exists
    if !Path::new(&fits_path).exists() {
        anyhow::bail!("FITS file does not exist: {}", fits_path);
    }
    // Check that the FITS file has a .fits or .fit extension
    if !fits_path.ends_with(".fits") && !fits_path.ends_with(".fit") {
        anyhow::bail!("FITS file must have .fits or .fit extension: {}", fits_path);
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
    let (s, workers) = processor.init_workers();

    let mut fptr = FitsFile::open(fits_path)?;
    let tlb_hdu = fptr.hdu(1)?;

    let num_rows = if let HduInfo::TableInfo { num_rows, .. } = tlb_hdu.info {
        num_rows
    } else {
        0
    };

    let progress_bar = ProgressBar::new(num_rows as u64)
        .with_message("Processing FITS file")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );

    for chunk_start in (0..num_rows).step_by(batch_size) {
        let chunk_end = (chunk_start + batch_size).min(num_rows);
        let range = chunk_start..chunk_end;
        let rows = T::read_batch(&tlb_hdu, &mut fptr, range)?;
        for row in rows {
            match s.send(row).await {
                Ok(_) => {
                    progress_bar.inc(1);
                }
                Err(e) => {
                    eprintln!("Failed to send record to workers: {}", e);
                    break;
                }
            }
        }
    }

    // Close the sender to signal workers to finish
    drop(s);
    // Wait for all workers to complete
    let _ = processor.close_workers(workers).await;

    Ok(())
}
