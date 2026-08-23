//! Attach PS1 PSC `ps_score` values onto existing PanSTARRS documents, matched by objID.
use anyhow::Result;
use clap::Parser;
use fitsio::{FitsFile, hdu::HduInfo};
use indicatif::ProgressBar;
use mongodb::{
    Client, Namespace,
    bson::doc,
    options::{UpdateOneModel, WriteModel},
};

#[derive(Parser)]
struct Cli {
    #[arg(help = "MongoDB collection holding the PanSTARRS documents.", env = "MONGODB_COLLECTION")]
    collection: String,
    #[arg(help = "Path to a PS1 PSC .fits file, or a directory of them.")]
    path: String,
    #[arg(long, env = "MONGODB_URI", default_value = "mongodb://localhost:27017")]
    uri: String,
    #[arg(long, env = "MONGODB_DB", default_value = "boom")]
    db: String,
    #[arg(long, default_value_t = 8)]
    num_workers: usize,
    #[arg(long, help = "Updates per bulk_write call.", default_value_t = 20000)]
    batch_size: usize,
    #[arg(long, default_value_t = 100000)]
    channel_capacity: usize,
}

#[derive(Debug, Clone, Copy)]
struct Score {
    obj_id: i64,
    ps_score: f64,
}

async fn flush(client: &Client, models: &mut Vec<WriteModel>, worker_id: usize) -> (u64, u64) {
    if models.is_empty() {
        return (0, 0);
    }
    let batch = std::mem::take(models);
    match client.bulk_write(batch).ordered(false).await {
        Ok(r) => (
            r.matched_count.max(0) as u64,
            r.modified_count.max(0) as u64,
        ),
        Err(e) => {
            eprintln!("Worker {}: bulk_write error: {}", worker_id, e);
            (0, 0)
        }
    }
}

async fn worker(
    worker_id: usize,
    receiver: async_channel::Receiver<Score>,
    uri: String,
    db: String,
    collection: String,
    batch_size: usize,
) -> Result<(u64, u64)> {
    let client = Client::with_uri_str(&uri).await?;
    let namespace = Namespace::new(db, collection);
    let mut models: Vec<WriteModel> = Vec::with_capacity(batch_size);
    let (mut matched, mut modified) = (0u64, 0u64);

    while let Ok(score) = receiver.recv().await {
        models.push(
            UpdateOneModel::builder()
                .namespace(namespace.clone())
                .filter(doc! { "_id": score.obj_id })
                .update(doc! { "$set": { "ps_score": score.ps_score } })
                .build()
                .into(),
        );
        if models.len() >= batch_size {
            let (m, u) = flush(&client, &mut models, worker_id).await;
            matched += m;
            modified += u;
        }
    }
    let (m, u) = flush(&client, &mut models, worker_id).await;
    matched += m;
    modified += u;
    Ok((matched, modified))
}

/// Read one PSC file and push every (objid, ps_score) pair into the channel.
async fn stream_file(path: &str, sender: &async_channel::Sender<Score>, chunk: usize) -> Result<u64> {
    let mut fptr = FitsFile::open(path)?;
    let hdu = fptr.hdu(1)?;
    let num_rows = match hdu.info {
        HduInfo::TableInfo { num_rows, .. } => num_rows,
        _ => anyhow::bail!("{}: HDU 1 is not a table", path),
    };

    let bar = ProgressBar::new(num_rows as u64).with_style(
        indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
            .unwrap(),
    );

    let mut sent = 0u64;
    for start in (0..num_rows).step_by(chunk) {
        let range = start..(start + chunk).min(num_rows);
        let ids: Vec<i64> = hdu.read_col_range(&mut fptr, "objid", &range)?;
        let scores: Vec<f32> = hdu.read_col_range(&mut fptr, "ps_score", &range)?;
        for (obj_id, ps_score) in ids.into_iter().zip(scores) {
            sender
                .send(Score { obj_id, ps_score: ps_score as f64 })
                .await?;
            sent += 1;
        }
        bar.set_position(range.end as u64);
    }
    bar.finish_and_clear();
    Ok(sent)
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
            .filter(|e| e.path().extension().is_some_and(|x| x == "fits"))
            .map(|e| e.path().to_string_lossy().to_string())
            .collect();
        v.sort();
        v
    } else {
        vec![args.path.clone()]
    };
    anyhow::ensure!(!paths.is_empty(), "no .fits files found in {}", args.path);
    println!("Found {} PSC file(s) to apply.", paths.len());

    let (sender, receiver) = async_channel::bounded::<Score>(args.channel_capacity);
    let mut handles = Vec::with_capacity(args.num_workers);
    for worker_id in 0..args.num_workers {
        let rx = receiver.clone();
        let (uri, db, coll) = (args.uri.clone(), args.db.clone(), args.collection.clone());
        handles.push(tokio::spawn(async move {
            worker(worker_id, rx, uri, db, coll, args.batch_size).await
        }));
    }
    drop(receiver);

    let mut sent = 0u64;
    for (i, path) in paths.iter().enumerate() {
        println!("Applying {} ({} of {})", path, i + 1, paths.len());
        sent += stream_file(path, &sender, args.batch_size).await?;
    }
    drop(sender);

    let mut modified = 0u64;
    for h in handles {
        modified += h.await??;
    }
    println!("Sent {} scores; {} documents modified.", sent, modified);
    if modified < sent {
        println!(
            "note: {} scores matched no document (expected if the PSC covers objects absent from this collection)",
            sent - modified
        );
    }
    Ok(())
}
