use crate::{
    db::{create_index, from_uri},
    types::HasCoordinates,
};
use anyhow::Result;
use mongodb::bson::{Document, doc};
use serde::Serialize;

fn to_document<T: Serialize>(source: T, with_coordinates: bool) -> Result<Document> {
    let mut doc = mongodb::bson::to_document(&source).unwrap();
    if with_coordinates {
        if let (Some(ra), Some(dec)) = (doc.get_f64("ra").ok(), doc.get_f64("dec").ok()) {
            doc.insert(
                "coordinates",
                doc! {
                    "radec_geojson": {
                        "type": "Point",
                        "coordinates": [ra - 180.0, dec],
                    }
                },
            );
        }
    }
    Ok(doc)
}

async fn worker<T>(
    worker_id: usize,
    receiver: async_channel::Receiver<T>,
    mongodb_uri: &str,
    db_name: &str,
    collection_name: &str,
    batch_size: usize,
    with_coordinates: bool,
) -> Result<usize>
where
    T: Serialize,
{
    let db = from_uri(mongodb_uri, db_name).await?;
    let collection = db.collection::<Document>(collection_name);

    let mut docs = Vec::with_capacity(batch_size);
    let mut total_processed = 0;

    while let Ok(record) = receiver.recv().await {
        let doc = to_document(record, with_coordinates)?;
        docs.push(doc);

        if docs.len() >= batch_size {
            let opts = mongodb::options::InsertManyOptions::builder()
                .ordered(false)
                .build();
            match collection.insert_many(&docs).with_options(opts).await {
                Ok(_) => {
                    total_processed += docs.len();
                }
                Err(e) => {
                    eprintln!("Worker {}: error in batch insert: {}", worker_id, e);
                }
            }
            docs.clear();
        }
    }

    if !docs.is_empty() {
        let opts = mongodb::options::InsertManyOptions::builder()
            .ordered(false)
            .build();
        match collection.insert_many(&docs).with_options(opts).await {
            Ok(_) => {
                total_processed += docs.len();
            }
            Err(e) => {
                eprintln!("Worker {}: error in final insert: {}", worker_id, e);
            }
        }
    }
    Ok(total_processed)
}

pub struct Processor {
    mongodb_uri: String,
    db_name: String,
    collection_name: String,
    num_workers: usize,
    batch_size: usize,
    channel_capacity: usize,
}

impl Processor {
    pub async fn new(
        mongodb_uri: String,
        db_name: String,
        collection_name: String,
        num_workers: usize,
        batch_size: usize,
        channel_capacity: usize,
    ) -> Result<Self> {
        // fail fast on a bad URI or unreachable server, rather than in every worker
        from_uri(&mongodb_uri, &db_name).await?;

        Ok(Self {
            mongodb_uri,
            db_name,
            collection_name,
            num_workers,
            batch_size,
            channel_capacity,
        })
    }

    /// Builds the coordinate index. Call once, after every insert has finished:
    /// an index that exists during the load has to be maintained on each write.
    pub async fn init_indexes<T>(&self) -> Result<()>
    where
        T: HasCoordinates,
    {
        if !T::has_coordinates() {
            return Ok(());
        }
        let db = from_uri(&self.mongodb_uri, &self.db_name).await?;
        let collection = db.collection::<Document>(&self.collection_name);
        create_index(
            &collection,
            doc! {"coordinates.radec_geojson": "2dsphere"},
            false,
        )
        .await
    }

    /// Initializes the workers and returns the sender and the vector of worker handles
    pub fn init_workers<T>(
        &self,
    ) -> (
        async_channel::Sender<T>,
        Vec<tokio::task::JoinHandle<Result<usize>>>,
    )
    where
        T: Serialize + Send + 'static + HasCoordinates,
    {
        let (s, r) = async_channel::bounded(self.channel_capacity);
        let mut workers = Vec::new();
        for worker_id in 0..self.num_workers {
            let worker_receiver = r.clone();
            let mongodb_uri = self.mongodb_uri.clone();
            let db_name = self.db_name.clone();
            let collection_name = self.collection_name.clone();
            let batch_size = self.batch_size;
            let worker_handle = tokio::spawn(async move {
                worker(
                    worker_id,
                    worker_receiver,
                    &mongodb_uri,
                    &db_name,
                    &collection_name,
                    batch_size,
                    T::has_coordinates(),
                )
                .await
            });
            workers.push(worker_handle);
        }
        (s, workers)
    }

    /// Waits for all workers to complete and returns the total inserted count
    pub async fn close_workers(
        &self,
        workers: Vec<tokio::task::JoinHandle<Result<usize>>>,
    ) -> usize {
        let mut total_inserted = 0;
        for (worker_id, worker_handle) in workers.into_iter().enumerate() {
            match worker_handle.await {
                Ok(Ok(processed)) => {
                    total_inserted += processed;
                }
                Ok(Err(e)) => {
                    eprintln!("Worker {} completed with error: {}", worker_id, e);
                }
                Err(e) => {
                    eprintln!("Worker {} panicked: {}", worker_id, e);
                }
            }
        }
        total_inserted
    }
}
