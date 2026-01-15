use anyhow::Result;
use boom_catalogs::csv::process_csv;
use boom_catalogs::types::{CsvCatalogs, Gaia, LSSG, Ned, Galex};
use clap::Parser;

#[derive(Parser)]
struct Cli {
    #[arg(help = "Type name of the struct to deserialize each value into")]
    type_name: CsvCatalogs,
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
        default_value_t = 1000000
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

    // path could be a dir or a file
    let paths = if std::fs::metadata(&args.path)?.is_dir() {
        let mut dir_paths = Vec::new();
        for entry in std::fs::read_dir(&args.path)? {
            let entry = entry?;
            let path = entry.path();
            // if the file path ends with .csv or .csv.gz, add it to the list
            if let Some(ext) = path.extension() {
                if ext == "csv" || ext == "gz" {
                    dir_paths.push(path.to_string_lossy().to_string());
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
            CsvCatalogs::Ned => {
                process_csv::<Ned>(
                    uri,
                    db,
                    collection,
                    path.clone(),
                    args.num_workers,
                    args.batch_size,
                    args.channel_capacity,
                    args.drop_existing_collection,
                    args.init_indexes,
                )
                .await
            }
            CsvCatalogs::LSSG => {
                process_csv::<LSSG>(
                    uri,
                    db,
                    collection,
                    path.clone(),
                    args.num_workers,
                    args.batch_size,
                    args.channel_capacity,
                    args.drop_existing_collection,
                    args.init_indexes,
                )
                .await
            }
            CsvCatalogs::Gaia => {
                process_csv::<Gaia>(
                    uri,
                    db,
                    collection,
                    path.clone(),
                    args.num_workers,
                    args.batch_size,
                    args.channel_capacity,
                    args.drop_existing_collection,
                    args.init_indexes,
                )
                .await
            }
            CsvCatalogs::Galex => {
                process_csv::<Galex>(
                    uri,
                    db,
                    collection,
                    path.clone(),
                    args.num_workers,
                    args.batch_size,
                    args.channel_capacity,
                    args.drop_existing_collection,
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
