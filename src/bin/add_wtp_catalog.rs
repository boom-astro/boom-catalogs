//! Turn WTP (WISE Time-domain Project) alert packets into one catalog document
//! per object, holding the merged W1/W2 light curve and forced photometry, so
//! boom can cross-match WINTER alerts against them.
//!
//! The packets are an alert stream in shape only: the data set is static, and each
//! packet repeats its object's whole history, on top of a cutout triplet that is
//! ~71% of its bytes. Both are dropped here, which is what turns terabytes of Avro
//! into tens of gigabytes of catalog.
//!
//! The collection name must carry boom's `watchlist_` prefix: that prefix is the
//! only thing that keeps a catalog out of reach of users without explicit access,
//! and it also keeps the matches off the world-readable alert documents.
//!
//! Points are kept in one array per band. W1 and W2 come out of the same exposure
//! and share an `mjd`, so a single array de-duplicated on time silently drops one
//! of the two: measured on a sample field, 1% of the light curve and 39% of the
//! forced photometry.
//!
//! ```bash
//! for archive in /path/to/wtp/*.zip; do
//!   unzip -qo "$archive" -d /tmp/wtp \
//!     && add_wtp_catalog watchlist_wtp /tmp/wtp --num-workers 8 \
//!     && rm -rf /tmp/wtp
//! done
//! add_wtp_catalog watchlist_wtp /tmp/last-archive --init-indexes
//! ```
//!
//! Re-running over the same packets is a no-op, so an interrupted load is resumed
//! by replaying the archive it died on. `--init-indexes` is meant for the final
//! run only: a 2dsphere index that exists during the load has to be maintained on
//! every write.
use anyhow::Result;
use apache_avro::{Reader, from_value};
use boom_catalogs::db::{create_index, from_uri};
use clap::Parser;
use indicatif::ProgressBar;
use mongodb::{
    Client, Namespace,
    bson::{Bson, Document, doc},
    options::{UpdateModifications, UpdateOneModel, WriteModel},
};
use serde::Deserialize;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;

const MJD_TO_JD: f64 = 2400000.5;
const WATCHLIST_PREFIX: &str = "watchlist_";
/// WTP writes -99.0 in place of a magnitude it could not measure.
const MAG_SENTINEL: f64 = -90.0;

#[derive(Parser)]
struct Cli {
    #[arg(
        help = "Destination collection. Must carry the watchlist_ prefix.",
        env = "MONGODB_COLLECTION"
    )]
    collection: String,
    #[arg(help = "Path to a WTP .avro packet, or a directory of them.")]
    path: String,
    #[arg(long, env = "MONGODB_URI", default_value = "mongodb://localhost:27017")]
    uri: String,
    #[arg(long, env = "MONGODB_DB", default_value = "boom")]
    db: String,
    #[arg(long, help = "Packet decoding and writing tasks.", default_value_t = 8)]
    num_workers: usize,
    #[arg(
        long,
        help = "Objects per bulk_write call, per worker.",
        default_value_t = 500
    )]
    batch_size: usize,
    #[arg(
        long,
        help = "Packet paths buffered ahead of the workers.",
        default_value_t = 10000
    )]
    channel_capacity: usize,
    #[arg(
        long,
        help = "Build the 2dsphere index once the load is done. Final run only.",
        default_value_t = false
    )]
    init_indexes: bool,
    #[arg(
        long,
        help = "Read and report without writing anything.",
        default_value_t = false
    )]
    dry_run: bool,
}

#[derive(Deserialize)]
struct RawAlert {
    #[serde(rename = "objectId")]
    object_id: String,
    candidate: RawCandidate,
    prv_candidates: Option<Vec<RawPrvCandidate>>,
    fp_records: Option<Vec<RawFpRecord>>,
}

#[derive(Deserialize)]
struct RawCandidate {
    mjd: f64,
    bandid: Option<i32>,
    ra: f64,
    dec: f64,
    magpsf: f32,
    sigmapsf: f32,
    diffmaglim: Option<f32>,
    isdiffpos: String,
    drb: Option<f32>,
}

#[derive(Deserialize)]
struct RawPrvCandidate {
    mjd: f64,
    bandid: Option<i32>,
    magpsf: Option<f32>,
    sigmapsf: Option<f32>,
    diffmaglim: Option<f32>,
    isdiffpos: Option<String>,
    drb: Option<f32>,
}

#[derive(Deserialize)]
struct RawFpRecord {
    mjd: f64,
    bandid: Option<i32>,
    forcediffimflux: f32,
    forcediffimfluxunc: f32,
    forcediffimfluxstaterr: f32,
    forcediffmagpsf: f32,
    forcediffsigmapsf: f32,
    forcestackimflux: f32,
    forcestackimfluxunc: f32,
    forcestackimfluxstaterr: f32,
    forcestackmagpsf: f32,
    forcestacksigmapsf: f32,
    diffmaglim: Option<f32>,
}

fn band(bandid: Option<i32>) -> Option<&'static str> {
    match bandid {
        Some(1) => Some("w1"),
        Some(2) => Some("w2"),
        Some(3) => Some("w3"),
        Some(4) => Some("w4"),
        _ => None,
    }
}

fn insert_finite(point: &mut Document, key: &str, value: Option<f32>) {
    if let Some(value) = value.map(f64::from).filter(|v| v.is_finite()) {
        point.insert(key, value);
    }
}

fn insert_mag(point: &mut Document, key: &str, value: Option<f32>) {
    if let Some(value) = value
        .map(f64::from)
        .filter(|v| v.is_finite() && *v > MAG_SENTINEL)
    {
        point.insert(key, value);
    }
}

/// Two records can share an epoch with the same flux but different uncertainties,
/// so the winner has to come from the point itself, never from its arrival order.
fn rank(point: &Document) -> (bool, f64, f64) {
    let tightest = |key: &str| -point.get_f64(key).unwrap_or(f64::INFINITY);
    (
        point.get_f64("magpsf").is_ok(),
        tightest("sigmapsf"),
        tightest("flux_err"),
    )
}

fn rank_expr(point: &str) -> Bson {
    let tightest = |key: &str| {
        doc! { "$multiply": [-1.0, { "$ifNull": [format!("{}.{}", point, key), f64::INFINITY] }] }
    };
    Bson::Array(vec![
        doc! { "$isNumber": format!("{}.magpsf", point) }.into(),
        tightest("sigmapsf").into(),
        tightest("flux_err").into(),
    ])
}

fn is_positive(isdiffpos: &str) -> bool {
    matches!(isdiffpos, "1" | "t" | "true")
}

fn light_point(
    jd: f64,
    magpsf: Option<f32>,
    sigmapsf: Option<f32>,
    diffmaglim: Option<f32>,
    isdiffpos: Option<&str>,
    drb: Option<f32>,
) -> Document {
    let mut point = doc! { "jd": jd };
    insert_mag(&mut point, "magpsf", magpsf);
    insert_mag(&mut point, "sigmapsf", sigmapsf);
    insert_mag(&mut point, "diffmaglim", diffmaglim);
    insert_finite(&mut point, "drb", drb);
    if let Some(isdiffpos) = isdiffpos {
        point.insert("is_positive", is_positive(isdiffpos));
    }
    point
}

fn forced_point(record: &RawFpRecord) -> Document {
    let mut point = doc! { "jd": record.mjd + MJD_TO_JD };
    insert_finite(&mut point, "flux", Some(record.forcediffimflux));
    insert_finite(&mut point, "flux_err", Some(record.forcediffimfluxunc));
    insert_finite(
        &mut point,
        "flux_staterr",
        Some(record.forcediffimfluxstaterr),
    );
    insert_mag(&mut point, "magpsf", Some(record.forcediffmagpsf));
    insert_mag(&mut point, "sigmapsf", Some(record.forcediffsigmapsf));
    insert_finite(&mut point, "stack_flux", Some(record.forcestackimflux));
    insert_finite(
        &mut point,
        "stack_flux_err",
        Some(record.forcestackimfluxunc),
    );
    insert_finite(
        &mut point,
        "stack_flux_staterr",
        Some(record.forcestackimfluxstaterr),
    );
    insert_mag(&mut point, "stack_magpsf", Some(record.forcestackmagpsf));
    insert_mag(
        &mut point,
        "stack_sigmapsf",
        Some(record.forcestacksigmapsf),
    );
    insert_mag(&mut point, "diffmaglim", record.diffmaglim);
    point
}

#[derive(Clone, Copy)]
enum Series {
    Lightcurve,
    Forced,
}

impl Series {
    fn field(&self, band: &str) -> String {
        match self {
            Series::Lightcurve => format!("lightcurve.{}", band),
            Series::Forced => format!("forced.{}", band),
        }
    }
}

#[derive(Default)]
struct Object {
    ra: f64,
    dec: f64,
    jd_position: f64,
    lightcurve: HashMap<&'static str, HashMap<i64, Document>>,
    forced: HashMap<&'static str, HashMap<i64, Document>>,
}

impl Object {
    fn set_position(&mut self, jd: f64, ra: f64, dec: f64) {
        if (jd, ra, dec) > (self.jd_position, self.ra, self.dec) {
            self.jd_position = jd;
            self.ra = ra;
            self.dec = dec;
        }
    }

    fn has_valid_position(&self) -> bool {
        (0.0..=360.0).contains(&self.ra) && (-90.0..=90.0).contains(&self.dec)
    }

    fn is_empty(&self) -> bool {
        self.lightcurve.is_empty() && self.forced.is_empty()
    }

    fn add(&mut self, series: Series, band: &'static str, jd: f64, point: Document) {
        let series = match series {
            Series::Lightcurve => &mut self.lightcurve,
            Series::Forced => &mut self.forced,
        };
        let epoch = (jd * 86400.0).floor() as i64;
        match series.entry(band).or_default().entry(epoch) {
            Entry::Vacant(slot) => {
                slot.insert(point);
            }
            Entry::Occupied(mut slot) => {
                if rank(&point) > rank(slot.get()) {
                    slot.insert(point);
                }
            }
        }
    }
}

#[derive(Default)]
struct Report {
    unreadable: u64,
    alerts: u64,
    points: u64,
    forced_points: u64,
    inserted: u64,
    modified: u64,
    unknown_band: u64,
    bad_position: u64,
    no_points: u64,
}

impl Report {
    fn merge(&mut self, other: Report) {
        self.unreadable += other.unreadable;
        self.alerts += other.alerts;
        self.points += other.points;
        self.forced_points += other.forced_points;
        self.inserted += other.inserted;
        self.modified += other.modified;
        self.unknown_band += other.unknown_band;
        self.bad_position += other.bad_position;
        self.no_points += other.no_points;
    }
}

fn read_alerts(bytes: &[u8]) -> Result<Vec<RawAlert>> {
    let mut alerts = Vec::new();
    for value in Reader::new(bytes)? {
        alerts.push(from_value::<RawAlert>(&value?)?);
    }
    anyhow::ensure!(!alerts.is_empty(), "no record in the avro container");
    Ok(alerts)
}

fn accumulate(alert: RawAlert, objects: &mut HashMap<String, Object>, report: &mut Report) {
    let candidate = alert.candidate;
    let jd = candidate.mjd + MJD_TO_JD;
    if !jd.is_finite() {
        return;
    }
    let object = objects.entry(alert.object_id).or_default();
    object.set_position(jd, candidate.ra, candidate.dec);

    match band(candidate.bandid) {
        Some(band) => {
            object.add(
                Series::Lightcurve,
                band,
                jd,
                light_point(
                    jd,
                    Some(candidate.magpsf),
                    Some(candidate.sigmapsf),
                    candidate.diffmaglim,
                    Some(&candidate.isdiffpos),
                    candidate.drb,
                ),
            );
            report.points += 1;
        }
        None => report.unknown_band += 1,
    }

    for previous in alert.prv_candidates.unwrap_or_default() {
        let jd = previous.mjd + MJD_TO_JD;
        let Some(band) = band(previous.bandid) else {
            report.unknown_band += 1;
            continue;
        };
        if !jd.is_finite() {
            continue;
        }
        object.add(
            Series::Lightcurve,
            band,
            jd,
            light_point(
                jd,
                previous.magpsf,
                previous.sigmapsf,
                previous.diffmaglim,
                previous.isdiffpos.as_deref(),
                previous.drb,
            ),
        );
        report.points += 1;
    }

    for record in alert.fp_records.unwrap_or_default() {
        let jd = record.mjd + MJD_TO_JD;
        let Some(band) = band(record.bandid) else {
            report.unknown_band += 1;
            continue;
        };
        if !jd.is_finite() {
            continue;
        }
        object.add(Series::Forced, band, jd, forced_point(&record));
        report.forced_points += 1;
    }
}

fn epoch_expr(jd: &str) -> Document {
    doc! { "$floor": { "$multiply": [jd, 86400.0] } }
}

fn merge_points(field: &str, points: Vec<Document>) -> Document {
    doc! {
        "$sortArray": {
            "input": {
                "$reduce": {
                    "input": points,
                    "initialValue": { "$ifNull": [format!("${}", field), []] },
                    "in": {
                        "$let": {
                            "vars": { "point": "$$this", "kept": "$$value" },
                            "in": {
                                "$cond": {
                                    "if": {
                                        "$in": [
                                            epoch_expr("$$point.jd"),
                                            { "$map": {
                                                "input": "$$kept",
                                                "in": epoch_expr("$$this.jd"),
                                            }},
                                        ]
                                    },
                                    "then": { "$map": {
                                        "input": "$$kept",
                                        "in": {
                                            "$cond": {
                                                "if": { "$and": [
                                                    { "$eq": [
                                                        epoch_expr("$$this.jd"),
                                                        epoch_expr("$$point.jd"),
                                                    ]},
                                                    { "$gt": [
                                                        rank_expr("$$point"),
                                                        rank_expr("$$this"),
                                                    ]},
                                                ]},
                                                "then": "$$point",
                                                "else": "$$this",
                                            }
                                        },
                                    }},
                                    "else": { "$concatArrays": ["$$kept", ["$$point"]] },
                                }
                            },
                        }
                    },
                }
            },
            "sortBy": { "jd": 1 },
        }
    }
}

fn build_write(namespace: &Namespace, object_id: &str, object: &Object) -> WriteModel {
    let newer = doc! { "$gt": [
        [object.jd_position, object.ra, object.dec],
        [
            { "$ifNull": ["$jd_position", 0.0] },
            { "$ifNull": ["$ra", 0.0] },
            { "$ifNull": ["$dec", 0.0] },
        ],
    ]};
    let mut set = doc! {
        "jd_position": { "$cond": [&newer, object.jd_position, "$jd_position"] },
        "ra": { "$cond": [&newer, object.ra, "$ra"] },
        "dec": { "$cond": [&newer, object.dec, "$dec"] },
        "coordinates": {
            "$cond": [&newer, { "$literal": doc! {
                "radec_geojson": { "type": "Point", "coordinates": [object.ra - 180.0, object.dec] }
            }}, "$coordinates"]
        },
    };
    for (series, bands) in [
        (Series::Lightcurve, &object.lightcurve),
        (Series::Forced, &object.forced),
    ] {
        for (band, points) in bands {
            let field = series.field(band);
            let points: Vec<Document> = points.values().cloned().collect();
            set.insert(&field, merge_points(&field, points));
        }
    }

    let pipeline = vec![
        doc! { "$set": set },
        doc! { "$set": {
            "_points": {
                "$reduce": {
                    "input": { "$objectToArray": { "$ifNull": ["$lightcurve", { "$literal": {} }] } },
                    "initialValue": [],
                    "in": { "$concatArrays": ["$$value", "$$this.v"] },
                }
            }
        }},
        doc! { "$set": {
            "n_obs": { "$size": "$_points" },
            "n_det": {
                "$size": {
                    "$filter": { "input": "$_points", "cond": { "$isNumber": "$$this.magpsf" } }
                }
            },
            "jd_first": { "$min": "$_points.jd" },
            "jd_last": { "$max": "$_points.jd" },
        }},
        doc! { "$unset": "_points" },
    ];

    UpdateOneModel::builder()
        .namespace(namespace.clone())
        .filter(doc! { "_id": object_id })
        .update(UpdateModifications::Pipeline(pipeline))
        .upsert(true)
        .build()
        .into()
}

async fn flush(
    client: &Client,
    namespace: &Namespace,
    objects: &mut HashMap<String, Object>,
    dry_run: bool,
    report: &mut Report,
    worker_id: usize,
) {
    let mut writes = Vec::with_capacity(objects.len());
    for (object_id, object) in objects.drain() {
        if object.is_empty() {
            report.no_points += 1;
            continue;
        }
        if !object.has_valid_position() {
            report.bad_position += 1;
            eprintln!(
                "Worker {}: {} has ra={} dec={} out of range, skipped",
                worker_id, object_id, object.ra, object.dec
            );
            continue;
        }
        writes.push(build_write(namespace, &object_id, &object));
    }
    if writes.is_empty() {
        return;
    }
    if dry_run {
        report.modified += writes.len() as u64;
        return;
    }
    match client.bulk_write(writes).ordered(false).await {
        Ok(result) => {
            report.inserted += result.upserted_count.max(0) as u64;
            report.modified += result.modified_count.max(0) as u64;
        }
        Err(e) => eprintln!("Worker {}: bulk_write error: {}", worker_id, e),
    }
}

#[derive(Clone)]
struct Job {
    uri: String,
    db: String,
    collection: String,
    batch_size: usize,
    dry_run: bool,
}

async fn worker(
    worker_id: usize,
    receiver: async_channel::Receiver<PathBuf>,
    job: Job,
    bar: ProgressBar,
) -> Result<Report> {
    let client = Client::with_uri_str(&job.uri).await?;
    let namespace = Namespace::new(job.db, job.collection);
    let mut report = Report::default();
    let mut objects: HashMap<String, Object> = HashMap::new();

    while let Ok(path) = receiver.recv().await {
        bar.inc(1);

        let bytes = match tokio::fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(e) => {
                report.unreadable += 1;
                eprintln!("Worker {}: {}: {}", worker_id, path.display(), e);
                continue;
            }
        };
        let alerts = match read_alerts(&bytes) {
            Ok(alerts) => alerts,
            Err(e) => {
                report.unreadable += 1;
                eprintln!("Worker {}: {}: {}", worker_id, path.display(), e);
                continue;
            }
        };
        for alert in alerts {
            report.alerts += 1;
            accumulate(alert, &mut objects, &mut report);
        }

        if objects.len() >= job.batch_size {
            flush(
                &client,
                &namespace,
                &mut objects,
                job.dry_run,
                &mut report,
                worker_id,
            )
            .await;
        }
    }

    flush(
        &client,
        &namespace,
        &mut objects,
        job.dry_run,
        &mut report,
        worker_id,
    )
    .await;
    Ok(report)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    anyhow::ensure!(
        args.collection.starts_with(WATCHLIST_PREFIX),
        "collection must start with '{}': that prefix is what keeps the catalog \
         private in boom, and what keeps its matches off the public alert documents",
        WATCHLIST_PREFIX
    );

    let paths: Vec<PathBuf> = if std::fs::metadata(&args.path)?.is_dir() {
        let mut paths: Vec<PathBuf> = walkdir::WalkDir::new(&args.path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .filter(|e| e.path().extension().is_some_and(|x| x == "avro"))
            .map(|e| e.path().to_path_buf())
            .collect();
        paths.sort();
        paths
    } else {
        vec![PathBuf::from(&args.path)]
    };
    anyhow::ensure!(!paths.is_empty(), "no .avro packet found in {}", args.path);
    println!(
        "Found {} packet(s) to read into {}.",
        paths.len(),
        args.collection
    );

    let bar = ProgressBar::new(paths.len() as u64)
        .with_message("Reading WTP packets")
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );

    let job = Job {
        uri: args.uri.clone(),
        db: args.db.clone(),
        collection: args.collection.clone(),
        batch_size: args.batch_size,
        dry_run: args.dry_run,
    };
    let (sender, receiver) = async_channel::bounded::<PathBuf>(args.channel_capacity);
    let mut handles = Vec::with_capacity(args.num_workers);
    for worker_id in 0..args.num_workers {
        handles.push(tokio::spawn(worker(
            worker_id,
            receiver.clone(),
            job.clone(),
            bar.clone(),
        )));
    }
    drop(receiver);

    for path in paths {
        sender.send(path).await?;
    }
    drop(sender);

    let mut report = Report::default();
    for (worker_id, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(worker_report)) => report.merge(worker_report),
            Ok(Err(e)) => eprintln!("Worker {} completed with error: {}", worker_id, e),
            Err(e) => eprintln!("Worker {} panicked: {}", worker_id, e),
        }
    }
    bar.finish_and_clear();

    let outcome = if args.dry_run {
        format!("{} write(s) planned", report.modified)
    } else {
        format!(
            "{} object(s) created, {} updated",
            report.inserted, report.modified
        )
    };
    println!(
        "Read {} alert(s) holding {} light curve point(s) and {} forced point(s) before de-duplication: {}.",
        report.alerts, report.points, report.forced_points, outcome
    );
    if report.unreadable > 0 {
        println!(
            "note: {} packet(s) skipped as unreadable",
            report.unreadable
        );
    }
    if report.unknown_band > 0 {
        println!(
            "note: {} point(s) skipped, bandid outside W1-W4",
            report.unknown_band
        );
    }
    if report.bad_position > 0 {
        println!(
            "note: {} object(s) skipped, ra or dec out of range",
            report.bad_position
        );
    }
    if report.no_points > 0 {
        println!(
            "note: {} object(s) skipped, no point in any band",
            report.no_points
        );
    }

    if args.init_indexes && !args.dry_run {
        let db = from_uri(&args.uri, &args.db).await?;
        let collection = db.collection::<Document>(&args.collection);
        create_index(
            &collection,
            doc! { "coordinates.radec_geojson": "2dsphere" },
            false,
        )
        .await?;
        println!("2dsphere index on coordinates.radec_geojson ready");
    }

    println!(
        "Declare {} under crossmatch.winter in boom's config.yaml, backfill with \
         reprocess_crossmatch, and grant access with PATCH /users/{{id}}/watchlist_access.",
        args.collection
    );
    Ok(())
}
