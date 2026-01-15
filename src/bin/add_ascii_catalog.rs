use anyhow::Result;
use boom_catalogs::ascii::process_ascii;
use boom_catalogs::types::{AsciiCatalogs, TwoMass, VSX};
use clap::Parser;

#[derive(Parser)]
struct Cli {
    #[arg(help = "Type name of the struct to deserialize each value into")]
    type_name: AsciiCatalogs,
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

    match args.type_name {
        AsciiCatalogs::VSX => {
            process_ascii::<VSX>(
                args.uri,
                args.db,
                args.collection,
                args.path,
                args.num_workers,
                args.batch_size,
                args.channel_capacity,
                args.drop_existing_collection,
                args.init_indexes,
            )
            .await?;
        }
        AsciiCatalogs::TwoMass => {
            process_ascii::<TwoMass>(
                args.uri,
                args.db,
                args.collection,
                args.path,
                args.num_workers,
                args.batch_size,
                args.channel_capacity,
                args.drop_existing_collection,
                args.init_indexes,
            )
            .await?;
        }
    }

    Ok(())
}
