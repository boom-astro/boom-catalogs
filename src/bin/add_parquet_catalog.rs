use anyhow::Result;
use boom_catalogs::db::from_uri;
use boom_catalogs::parquet::process_parquet;
use boom_catalogs::types::{AllWISE, CatWISE2020, GaiaPS1Xmatch, PanSTARRS, ParquetCatalogs};
use clap::Parser;
use mongodb::bson::Document;

#[derive(Parser)]
struct Cli {
    #[arg(help = "Type name of the struct to deserialize each value into")]
    type_name: ParquetCatalogs,
    #[arg(help = "MongoDB collection name.", env = "MONGODB_COLLECTION")]
    collection: String,
    #[arg(help = "Path to the CSV file to ingest.")]
    path: String,
    #[arg(
        long,
        help = "MongoDB connection string.",
        env = "MONGODB_URI",
        default_value = "mongodb://localhost:27017"
    )]
    uri: String,
    #[arg(
        long,
        help = "MongoDB database name.",
        env = "MONGODB_DB",
        default_value = "boom"
    )]
    db: String,
    #[arg(long, help = "Number of worker tasks to spawn.", default_value_t = 4)]
    num_workers: usize,
    #[arg(long, help = "Batch size for inserts.", default_value_t = 10000)]
    batch_size: usize,
    #[arg(
        long,
        help = "Channel capacity for buffering.",
        default_value_t = 100000
    )]
    channel_capacity: usize,
    #[arg(
        long,
        help = "Drop existing collection before inserting.",
        default_value_t = false
    )]
    drop_existing_collection: bool,
    #[arg(
        long,
        help = "Initialize indexes after inserting.",
        default_value_t = false
    )]
    init_indexes: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    // Drop the collection once before processing any files, not per-file
    if args.drop_existing_collection {
        let db = from_uri(&args.uri, &args.db).await?;
        let collection = db.collection::<Document>(&args.collection);
        collection.drop().await?;
        println!("Dropped existing collection: {}", args.collection);
    }

    // path could be a dir or a file
    let paths = if std::fs::metadata(&args.path)?.is_dir() {
        // we need to look recusively for files, as the parquet files could be in subdirs
        let mut dir_paths = Vec::new();
        for entry in walkdir::WalkDir::new(&args.path) {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "parquet" {
                        dir_paths.push(path.to_string_lossy().to_string());
                    }
                }
            }
        }
        dir_paths
    } else {
        vec![args.path.clone()]
    };

    println!("Found {} files to process.", paths.len());
    for (i, path) in paths.iter().enumerate() {
        println!("Processing file: {} ({} of {})", path, i + 1, paths.len());
        let uri = args.uri.clone();
        let db = args.db.clone();
        let collection = args.collection.clone();
        let path = path.clone();
        let result = match args.type_name {
            ParquetCatalogs::GaiaPS1Xmatch => {
                process_parquet::<GaiaPS1Xmatch>(
                    uri,
                    db,
                    collection,
                    path.clone(),
                    args.num_workers,
                    args.batch_size,
                    args.channel_capacity,
                    false,
                    args.init_indexes,
                )
                .await
            }
            ParquetCatalogs::CatWISE2020 => {
                process_parquet::<CatWISE2020>(
                    uri,
                    db,
                    collection,
                    path.clone(),
                    args.num_workers,
                    args.batch_size,
                    args.channel_capacity,
                    false,
                    args.init_indexes,
                )
                .await
            }
            ParquetCatalogs::AllWISE => {
                process_parquet::<AllWISE>(
                    uri,
                    db,
                    collection,
                    path.clone(),
                    args.num_workers,
                    args.batch_size,
                    args.channel_capacity,
                    false,
                    args.init_indexes,
                )
                .await
            }
            ParquetCatalogs::PanSTARRS => {
                process_parquet::<PanSTARRS>(
                    uri,
                    db,
                    collection,
                    path.clone(),
                    args.num_workers,
                    args.batch_size,
                    args.channel_capacity,
                    false,
                    args.init_indexes,
                )
                .await
            }
        };

        match result {
            Ok(_) => {
                println!("Finished processing file: {}", path);
            }
            Err(e) => {
                eprintln!("Error processing file {}: {}", path, e);
            }
        }
    }

    Ok(())
}
