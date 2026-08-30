//! Attach PS1-STRM classifications and photo-z onto existing PanSTARRS documents,
//! matched by objID.
//!
//! PS1-STRM's `objID` is explicitly NOT unique (see the catalog README), so several
//! rows can target the same document. `uniquePspsOBid` breaks those ties: the row
//! with the highest value wins. That is enforced in the update filter rather than by
//! de-duplicating in memory -- a 2.9-billion-row hash set will not fit, and a filter
//! makes the outcome independent of the order rows happen to arrive in, so reruns and
//! interrupted runs converge on the same result.
use anyhow::{Context, Result};
use clap::Parser;
use flate2::read::MultiGzDecoder;
use mongodb::{
    Client, Namespace,
    bson::{Bson, doc},
    options::{UpdateOneModel, WriteModel},
};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Column offsets in the headerless `_cat.csv.gz` files, per the PS1-STRM README.
const COL_OBJID: usize = 0;
const COL_UNIQUE_PSPS_OBID: usize = 1;
const COL_CLASS: usize = 6;
const COL_PROB_GALAXY: usize = 7;
const COL_PROB_STAR: usize = 8;
const COL_PROB_QSO: usize = 9;
const COL_Z_PHOT: usize = 13;
const COL_Z_PHOT_ERR: usize = 14;
const NUM_COLS: usize = 19;

/// PS1-STRM writes -999 for "not applicable" -- photo-z is only estimated for
/// galaxies, so every star/QSO/unsure row carries the sentinel. Store null instead,
/// otherwise a redshift cut sees 1.6 billion sources sitting at z = -999.
const SENTINEL: f64 = -900.0;

#[derive(Parser)]
struct Cli {
    #[arg(
        help = "MongoDB collection holding the PanSTARRS documents.",
        env = "MONGODB_COLLECTION"
    )]
    collection: String,
    #[arg(help = "Path to a PS1-STRM _cat.csv.gz file, or a directory of them.")]
    path: String,
    #[arg(long, env = "MONGODB_URI", default_value = "mongodb://localhost:27017")]
    uri: String,
    #[arg(long, env = "MONGODB_DB", default_value = "boom")]
    db: String,
    #[arg(long, help = "MongoDB writer tasks.", default_value_t = 8)]
    num_workers: usize,
    #[arg(
        long,
        help = "Files decompressed concurrently. Gzip decoding is CPU-bound.",
        default_value_t = 4
    )]
    reader_threads: usize,
    #[arg(long, help = "Updates per bulk_write call.", default_value_t = 20000)]
    batch_size: usize,
    #[arg(long, default_value_t = 100000)]
    channel_capacity: usize,
}

#[derive(Debug)]
struct Record {
    obj_id: i64,
    unique_psps_obid: i64,
    class: String,
    prob_galaxy: Option<f64>,
    prob_star: Option<f64>,
    prob_qso: Option<f64>,
    z_phot: Option<f64>,
    z_phot_err: Option<f64>,
}

fn parse_f64(field: &str) -> Option<f64> {
    match field.trim().parse::<f64>() {
        Ok(v) if v > SENTINEL => Some(v),
        _ => None,
    }
}

fn parse_line(line: &str) -> Option<Record> {
    let fields: Vec<&str> = line.trim_end().split(',').collect();
    if fields.len() != NUM_COLS {
        return None;
    }
    Some(Record {
        obj_id: fields[COL_OBJID].trim().parse().ok()?,
        unique_psps_obid: fields[COL_UNIQUE_PSPS_OBID].trim().parse().ok()?,
        class: fields[COL_CLASS].trim().to_string(),
        prob_galaxy: parse_f64(fields[COL_PROB_GALAXY]),
        prob_star: parse_f64(fields[COL_PROB_STAR]),
        prob_qso: parse_f64(fields[COL_PROB_QSO]),
        z_phot: parse_f64(fields[COL_Z_PHOT]),
        z_phot_err: parse_f64(fields[COL_Z_PHOT_ERR]),
    })
}

fn opt(v: Option<f64>) -> Bson {
    v.map(Bson::Double).unwrap_or(Bson::Null)
}

async fn flush(client: &Client, models: &mut Vec<WriteModel>, worker_id: usize) -> u64 {
    if models.is_empty() {
        return 0;
    }
    let batch = std::mem::take(models);
    match client.bulk_write(batch).ordered(false).await {
        Ok(r) => r.modified_count.max(0) as u64,
        Err(e) => {
            eprintln!("Worker {}: bulk_write error: {}", worker_id, e);
            0
        }
    }
}

async fn worker(
    worker_id: usize,
    receiver: async_channel::Receiver<Record>,
    uri: String,
    db: String,
    collection: String,
    batch_size: usize,
) -> Result<u64> {
    let client = Client::with_uri_str(&uri).await?;
    let namespace = Namespace::new(db, collection);
    let mut models: Vec<WriteModel> = Vec::with_capacity(batch_size);
    let mut modified = 0u64;

    while let Ok(r) = receiver.recv().await {
        // Only overwrite when this row wins the tiebreak: either nothing has been
        // written yet, or our uniquePspsOBid is higher than what is already there.
        let filter = doc! {
            "_id": r.obj_id,
            "$or": [
                { "strm_uid": { "$exists": false } },
                { "strm_uid": { "$lt": r.unique_psps_obid } },
            ],
        };
        let update = doc! { "$set": {
            "strm_uid": r.unique_psps_obid,
            "strm_class": r.class,
            "strm_prob_galaxy": opt(r.prob_galaxy),
            "strm_prob_star": opt(r.prob_star),
            "strm_prob_qso": opt(r.prob_qso),
            "strm_z_phot": opt(r.z_phot),
            "strm_z_phot_err": opt(r.z_phot_err),
        }};
        models.push(
            UpdateOneModel::builder()
                .namespace(namespace.clone())
                .filter(filter)
                .update(update)
                .build()
                .into(),
        );
        if models.len() >= batch_size {
            modified += flush(&client, &mut models, worker_id).await;
        }
    }
    modified += flush(&client, &mut models, worker_id).await;
    Ok(modified)
}

/// Decompress one file and push every parsed row into the channel.
fn stream_file(
    path: &str,
    sender: &async_channel::Sender<Record>,
    sent: &AtomicU64,
    skipped: &AtomicU64,
) -> Result<()> {
    let file = File::open(path).with_context(|| format!("opening {}", path))?;
    // MultiGzDecoder, not GzDecoder: a concatenated-member archive would otherwise
    // stop silently at the first member boundary and drop the rest of the file.
    let reader = BufReader::with_capacity(1 << 20, MultiGzDecoder::new(file));
    for line in reader.lines() {
        let line = line.with_context(|| format!("reading {}", path))?;
        if line.trim().is_empty() {
            continue;
        }
        match parse_line(&line) {
            Some(record) => {
                sender.send_blocking(record)?;
                sent.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                skipped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    // Enumerate first: never touch the database before we know there is work to do.
    let paths: Vec<String> = if std::fs::metadata(&args.path)?.is_dir() {
        let mut v: Vec<String> = walkdir::WalkDir::new(&args.path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .filter(|e| e.path().to_string_lossy().ends_with("_cat.csv.gz"))
            .map(|e| e.path().to_string_lossy().to_string())
            .collect();
        v.sort();
        v
    } else {
        vec![args.path.clone()]
    };
    anyhow::ensure!(!paths.is_empty(), "no _cat.csv.gz files found in {}", args.path);
    println!("Found {} PS1-STRM file(s) to apply.", paths.len());

    let (sender, receiver) = async_channel::bounded::<Record>(args.channel_capacity);
    let mut handles = Vec::with_capacity(args.num_workers);
    for worker_id in 0..args.num_workers {
        let rx = receiver.clone();
        let (uri, db, coll) = (args.uri.clone(), args.db.clone(), args.collection.clone());
        let batch_size = args.batch_size;
        handles.push(tokio::spawn(async move {
            worker(worker_id, rx, uri, db, coll, batch_size).await
        }));
    }
    drop(receiver);

    let sent = Arc::new(AtomicU64::new(0));
    let skipped = Arc::new(AtomicU64::new(0));
    let queue = Arc::new(std::sync::Mutex::new(paths.clone()));
    let total_files = paths.len();

    // Gzip decoding is CPU-bound, so readers are OS threads pulling from a shared
    // work queue rather than tokio tasks.
    let mut readers = Vec::with_capacity(args.reader_threads);
    for _ in 0..args.reader_threads.min(total_files) {
        let (queue, sender) = (Arc::clone(&queue), sender.clone());
        let (sent, skipped) = (Arc::clone(&sent), Arc::clone(&skipped));
        readers.push(std::thread::spawn(move || -> Result<()> {
            loop {
                let path = {
                    let mut q = queue.lock().unwrap();
                    match q.pop() {
                        Some(p) => p,
                        None => return Ok(()),
                    }
                };
                let left = queue.lock().unwrap().len();
                println!("Applying {} ({} remaining)", path, left);
                stream_file(&path, &sender, &sent, &skipped)?;
            }
        }));
    }
    drop(sender);

    for r in readers {
        r.join().map_err(|_| anyhow::anyhow!("reader thread panicked"))??;
    }

    let mut modified = 0u64;
    for h in handles {
        modified += h.await??;
    }

    let sent = sent.load(Ordering::Relaxed);
    let skipped = skipped.load(Ordering::Relaxed);
    println!("Sent {} rows; {} documents modified.", sent, modified);
    if skipped > 0 {
        println!("warning: {} line(s) skipped as unparseable", skipped);
    }
    if modified < sent {
        println!(
            "note: {} row(s) changed nothing -- either the objID is absent from this \
             collection, or the row lost the uniquePspsOBid tiebreak",
            sent - modified
        );
    }
    Ok(())
}
