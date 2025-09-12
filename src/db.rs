use anyhow::Result;
use mongodb::{
    Client, Collection, Database, IndexModel,
    bson::{Document, doc},
    options::IndexOptions,
};

pub async fn create_index(
    collection: &Collection<Document>,
    index: Document,
    unique: bool,
) -> Result<()> {
    let index_model = IndexModel::builder()
        .keys(index)
        .options(IndexOptions::builder().unique(unique).build())
        .build();
    collection.create_index(index_model).await?;
    Ok(())
}

pub async fn from_uri(uri: &str, name: &str) -> Result<Database> {
    let client = Client::with_uri_str(&uri).await?;
    let db = client.database(&name);
    // verify that the connection is valid
    db.run_command(doc! {"ping": 1}).await?;
    Ok(db)
}
