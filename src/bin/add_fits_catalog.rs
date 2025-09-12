use anyhow::Result;
use boom_catalogs::fits::process_fits;
use boom_catalogs::types::{FitsCatalogs, Ned};
use clap::Parser;

#[derive(Parser)]
struct Cli {
    #[arg(help = "Type name of the struct to deserialize each value into")]
    type_name: FitsCatalogs,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    match args.type_name {
        FitsCatalogs::Ned => {
            process_fits::<Ned>(
                args.uri,
                args.db,
                args.collection,
                args.path,
                args.num_workers,
                args.batch_size,
                args.channel_capacity,
            )
            .await?;
        }
    }

    Ok(())
}
