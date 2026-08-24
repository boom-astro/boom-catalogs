use anyhow::Result;
use boom_catalogs::db::from_uri;
use boom_catalogs::fits::process_fits;
use boom_catalogs::types::{DesiDr1, ERosita, FitsCatalogs, Milliquas, Ned, UVGapsXGaia};
use clap::Parser;
use mongodb::bson::Document;

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

    // Drop the collection once up front, before any file is processed.
    if args.drop_existing_collection {
        let db = from_uri(&args.uri, &args.db).await?;
        let collection = db.collection::<Document>(&args.collection);
        collection.drop().await?;
        println!("Dropped existing collection: {}", args.collection);
    }

    match args.type_name {
        FitsCatalogs::Ned => {
            process_fits::<Ned>(
                args.uri.clone(),
                args.db.clone(),
                args.collection.clone(),
                args.path,
                args.num_workers,
                args.batch_size,
                args.channel_capacity,
                args.init_indexes,
            )
            .await?;
        }
        FitsCatalogs::Milliquas => {
            process_fits::<Milliquas>(
                args.uri.clone(),
                args.db.clone(),
                args.collection.clone(),
                args.path,
                args.num_workers,
                args.batch_size,
                args.channel_capacity,
                args.init_indexes,
            )
            .await?;
        }
        FitsCatalogs::ERosita => {
            process_fits::<ERosita>(
                args.uri.clone(),
                args.db.clone(),
                args.collection.clone(),
                args.path,
                args.num_workers,
                args.batch_size,
                args.channel_capacity,
                args.init_indexes,
            )
            .await?;
        }
        FitsCatalogs::UVGapsXGaia => {
            process_fits::<UVGapsXGaia>(
                args.uri.clone(),
                args.db.clone(),
                args.collection.clone(),
                args.path,
                args.num_workers,
                args.batch_size,
                args.channel_capacity,
                args.init_indexes,
            )
            .await?;
        }
        FitsCatalogs::DesiDr1 => {
            process_fits::<DesiDr1>(
                args.uri.clone(),
                args.db.clone(),
                args.collection.clone(),
                args.path,
                args.num_workers,
                args.batch_size,
                args.channel_capacity,
                args.init_indexes,
            )
            .await?;
        }
    }

    Ok(())
}
