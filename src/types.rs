use crate::parquet::ParquetRowBatch;
use crate::{ascii::FromAsciiRow, fits::FitsRowBatch};
use anyhow::Result;
use fitsio::FitsFile;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};

// Helper to parse optional fields
fn parse_optional<T: std::str::FromStr>(s: &str) -> Option<T> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        trimmed.parse().ok()
    }
}

fn parse_optional_string(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_boolean(s: &str) -> bool {
    let trimmed = s.trim().to_lowercase();
    if trimmed.is_empty() {
        false
    } else {
        match trimmed.as_str() {
            "1" | "true" | "yes" | "y" => true,
            _ => false,
        }
    }
}

// Extract substring safely, handling cases where line is shorter than expected
fn extract_range(line: &str, start: usize, end: usize) -> &str {
    let line_len = line.len();
    if start >= line_len {
        return "";
    }
    let actual_end = end.min(line_len);
    &line[start..actual_end]
}

pub trait HasCoordinates {
    fn has_coordinates() -> bool {
        true
    }
}

fn nullable<'de, D, T, E>(de: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    Option<T>: serde::Deserialize<'de>,
    T: std::str::FromStr<Err = E>,
    E: std::error::Error,
{
    use serde::de::Error;

    let val = String::deserialize(de)?;
    if val.is_empty() || val == "null" {
        Ok(None)
    } else {
        val.parse()
            .map(Some)
            .map_err(|e: E| D::Error::custom(e.to_string()))
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LSSG {
    // use the serde rename attribute so that when converted to a bson document
    // the ls_id becomes _id
    #[serde(rename(serialize = "_id"))]
    ls_id: i64,
    ra: f32,
    dec: f32,
    mag_white: f32,
    score: f32,
}

impl HasCoordinates for LSSG {
    fn has_coordinates() -> bool {
        true
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GaiaPS1Xmatch {
    #[serde(rename(serialize = "_id"))]
    pub source_id: i64, // this is the Gaia source_id
    pub original_ext_source_id: i64, // this is the original PanSTARRs psid
    pub score: f64,
}

// for GaiaPS1Xmatch let's implement a function that takes a polars DataFrame
// gets all the columns that are needed, and returns an iterator over GaiaPS1Xmatch structs
// we can't create a WHOLE VEC AT ONCE, SO WE NEED AN ITERATOR
impl ParquetRowBatch for GaiaPS1Xmatch {
    fn from_dataframe(df: &polars::prelude::DataFrame) -> Result<Vec<GaiaPS1Xmatch>> {
        let source_id_series = df.column("source_id")?;
        let original_ext_source_id_series = df.column("original_ext_source_id")?;
        let score_series = df.column("score")?;

        let mut results = Vec::with_capacity(df.height());
        for i in 0..df.height() {
            let source_id = source_id_series
                .i64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing source_id at row {}", i))?;
            let original_ext_source_id = original_ext_source_id_series
                .i64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing original_ext_source_id at row {}", i))?;
            let score = score_series
                .f64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing score at row {}", i))?;

            results.push(GaiaPS1Xmatch {
                source_id,
                original_ext_source_id,
                score,
            });
        }
        Ok(results)
    }
}

impl HasCoordinates for GaiaPS1Xmatch {
    fn has_coordinates() -> bool {
        false
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Gaia {
    // use the serde rename attribute so that when converted to a bson document
    // the source_id becomes _id
    #[serde(rename(serialize = "_id"))]
    source_id: i64,
    ra: f32,
    ra_error: f32,
    dec: f32,
    dec_error: f32,
    #[serde(deserialize_with = "nullable")]
    parallax: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    parallax_error: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    pm: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    pmra: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    pmra_error: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    pmdec: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    pmdec_error: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    phot_g_mean_mag: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    phot_bp_mean_mag: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    phot_rp_mean_mag: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    phot_g_n_obs: Option<i32>,
    #[serde(deserialize_with = "nullable")]
    phot_bp_n_obs: Option<i32>,
    #[serde(deserialize_with = "nullable")]
    phot_rp_n_obs: Option<i32>,
    #[serde(deserialize_with = "nullable")]
    ruwe: Option<f32>,
    #[serde(deserialize_with = "nullable")]
    phot_bp_rp_excess_factor: Option<f32>,
}

impl HasCoordinates for Gaia {
    fn has_coordinates() -> bool {
        true
    }
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct Ned {
    #[serde(rename(serialize = "_id"))]
    objname: String,
    ra: f64,
    dec: f64,
    objtype: String,
    z: f64,
    z_unc: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VSX {
    #[serde(rename = "_id")]
    oid: i64,
    name: String,
    var_flag: i32,
    ra: f64,
    dec: f64,
    types: Vec<String>,
    max: Option<f64>,
    max_band: Option<String>,
    min_is_amplitude: bool,
    min: Option<f64>,
    min_band: Option<String>,
    epoch: Option<f64>,
    period: Option<f64>,
    spectral_type: Option<String>,
}

impl HasCoordinates for VSX {
    fn has_coordinates() -> bool {
        true
    }
}

impl FromAsciiRow for VSX {
    // Hardcoded column positions based on your data format
    // Adjust these indices based on your actual column positions
    fn from_line(line: &str) -> Result<Self, String> {
        // Define column start and end positions (adjust these to match your data!)
        // You can determine these by looking at your data file and counting character positions

        let oid_str = extract_range(line, 0, 8).trim();
        let name_str = extract_range(line, 8, 40).trim();
        let var_flag_str = extract_range(line, 40, 42).trim();
        let ra_str = extract_range(line, 42, 52).trim();
        let dec_str = extract_range(line, 52, 62).trim();
        let type_str = extract_range(line, 62, 96).trim();
        let max_str = extract_range(line, 96, 105).trim();
        let max_band_str = extract_range(line, 105, 110).trim();
        let min_is_amplitude_str = extract_range(line, 110, 118).trim();
        let min_str = extract_range(line, 118, 127).trim();
        let min_band_str = extract_range(line, 127, 137).trim();
        let epoch_str = extract_range(line, 137, 156).trim();
        let period_str = extract_range(line, 156, 174).trim();
        let spectral_type_str = extract_range(line, 174, 204).trim();

        // Parse required fields with better error messages
        let oid = oid_str.parse().map_err(|e| {
            format!(
                "Field 'oid' at [0:8]: failed to parse '{}' - {}",
                oid_str, e
            )
        })?;

        let name = if name_str.is_empty() {
            return Err("Field 'name' at [8:40]: cannot be empty".to_string());
        } else {
            name_str.to_string()
        };

        let var_flag = var_flag_str.parse().map_err(|e| {
            format!(
                "Field 'var_flag' at [40:44]: failed to parse '{}' - {}",
                var_flag_str, e
            )
        })?;

        let ra = ra_str.parse().map_err(|e| {
            format!(
                "Field 'ra' at [44:54]: failed to parse '{}' - {}",
                ra_str, e
            )
        })?;

        let dec = dec_str.parse().map_err(|e| {
            format!(
                "Field 'dec' at [54:64]: failed to parse '{}' - {}",
                dec_str, e
            )
        })?;

        // Parse optional fields
        let types = parse_optional_string(type_str);
        // types could be just one, the separator is |
        let types = if let Some(t) = types {
            t.split('|').map(|s| s.to_string()).collect()
        } else {
            Vec::new()
        };
        let max = parse_optional(max_str);
        let max_band = parse_optional_string(max_band_str);
        let min_is_amplitude = parse_boolean(min_is_amplitude_str);
        let min = parse_optional(min_str);
        let min_band = parse_optional_string(min_band_str);
        let epoch = parse_optional(epoch_str);
        let period = parse_optional(period_str);
        let spectral_type = parse_optional_string(spectral_type_str);

        Ok(VSX {
            oid,
            name,
            var_flag,
            ra,
            dec,
            types,
            max,
            max_band,
            min_is_amplitude,
            min,
            min_band,
            epoch,
            period,
            spectral_type,
        })
    }
}

impl FitsRowBatch for Ned {
    fn read_batch(
        hdu: &fitsio::hdu::FitsHdu,
        fptr: &mut FitsFile,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<Ned>> {
        let objname_col: Vec<String> = hdu.read_col_range(fptr, "objname", &range)?;
        let ra_col: Vec<f64> = hdu.read_col_range(fptr, "ra", &range)?;
        let dec_col: Vec<f64> = hdu.read_col_range(fptr, "dec", &range)?;
        let objtype_col: Vec<String> = hdu.read_col_range(fptr, "objtype", &range)?;
        let z_col: Vec<f64> = hdu.read_col_range(fptr, "z", &range)?;
        let z_unc_col: Vec<f64> = hdu.read_col_range(fptr, "z_unc", &range)?;

        // Combine the columns into a Vec<Row>
        let mut rows = Vec::with_capacity(objname_col.len());
        for i in 0..objname_col.len() {
            rows.push(Ned {
                objname: objname_col[i].clone(),
                ra: ra_col[i],
                dec: dec_col[i],
                objtype: objtype_col[i].clone(),
                z: z_col[i],
                z_unc: z_unc_col[i],
            });
        }
        Ok(rows)
    }
}

impl HasCoordinates for Ned {
    fn has_coordinates() -> bool {
        true
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CatWISE2020 {
    #[serde(rename(serialize = "_id"))]
    pub source_id: String,
    pub source_name: String,
    pub ra: f64,
    pub dec: f64,
    pub sigra: f64,
    pub sigdec: f64,
    pub w1mpro: Option<f64>,
    pub w2mpro: Option<f64>,
    pub w1sigmpro: Option<f64>,
    pub w2sigmpro: Option<f64>,
    pub w1rchi2: Option<f64>,
    pub w2rchi2: Option<f64>,
    pub pmra: f64,
    pub pmdec: f64,
    pub sigpmra: f64,
    pub sigpmdec: f64,
    pub unwise_objid: String,
}

impl ParquetRowBatch for CatWISE2020 {
    fn from_dataframe(df: &polars::prelude::DataFrame) -> Result<Vec<CatWISE2020>> {
        let source_id_series = df.column("source_id")?;
        let source_name_series = df.column("source_name")?;
        let ra_series = df.column("ra")?;
        let dec_series = df.column("dec")?;
        let sigra_series = df.column("sigra")?;
        let sigdec_series = df.column("sigdec")?;
        let w1mpro_series = df.column("w1mpro")?;
        let w2mpro_series = df.column("w2mpro")?;
        let w1sigmpro_series = df.column("w1sigmpro")?;
        let w2sigmpro_series = df.column("w2sigmpro")?;
        let w1rchi2_series = df.column("w1rchi2")?;
        let w2rchi2_series = df.column("w2rchi2")?;
        let pmra_series = df.column("PMRA")?;
        let pmdec_series = df.column("PMDec")?;
        let sigpmra_series = df.column("sigPMRA")?;
        let sigpmdec_series = df.column("sigPMDec")?;
        let unwise_objid_series = df.column("unwise_objid")?;

        let mut results = Vec::with_capacity(df.height());
        for i in 0..df.height() {
            let source_id = source_id_series
                .str()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing source_id at row {}", i))?
                .to_string();
            let source_name = source_name_series
                .str()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing source_name at row {}", i))?
                .to_string();
            let ra = ra_series
                .f64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing ra at row {}", i))?;
            let dec = dec_series
                .f64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing dec at row {}", i))?;
            let sigra = sigra_series
                .f64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing sigra at row {}", i))?;
            let sigdec = sigdec_series
                .f64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing sigdec at row {}", i))?;
            let w1mpro = match w1mpro_series.f64()?.get(i) {
                Some(v) => Some(v),
                None => None,
            };
            let w2mpro = match w2mpro_series.f64()?.get(i) {
                Some(v) => Some(v),
                None => None,
            };
            let w1sigmpro = match w1sigmpro_series.f64()?.get(i) {
                Some(v) => Some(v),
                None => None,
            };
            let w2sigmpro = match w2sigmpro_series.f64()?.get(i) {
                Some(v) => Some(v),
                None => None,
            };
            let w1rchi2 = match w1rchi2_series.f64()?.get(i) {
                Some(v) => Some(v),
                None => None,
            };
            let w2rchi2 = match w2rchi2_series.f64()?.get(i) {
                Some(v) => Some(v),
                None => None,
            };
            let pmra = pmra_series
                .f64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing pmra at row {}", i))?;
            let pmdec = pmdec_series
                .f64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing pmdec at row {}", i))?;
            let sigpmra = sigpmra_series
                .f64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing sigpmra at row {}", i))?;
            let sigpmdec = sigpmdec_series
                .f64()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing sigpmdec at row {}", i))?;
            let unwise_objid = unwise_objid_series
                .str()?
                .get(i)
                .ok_or_else(|| anyhow::anyhow!("Missing unwise_objid at row {}", i))?
                .to_string();
            results.push(CatWISE2020 {
                source_id,
                source_name,
                ra,
                dec,
                sigra,
                sigdec,
                w1mpro,
                w2mpro,
                w1sigmpro,
                w2sigmpro,
                w1rchi2,
                w2rchi2,
                pmra,
                pmdec,
                sigpmra,
                sigpmdec,
                unwise_objid,
            });
        }
        Ok(results)
    }
}

impl HasCoordinates for CatWISE2020 {
    fn has_coordinates() -> bool {
        true
    }
}

#[derive(clap::ValueEnum, Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum FitsCatalogs {
    Ned,
}

#[derive(clap::ValueEnum, Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum CsvCatalogs {
    LSSG,
    Ned,
    Gaia,
}

#[derive(clap::ValueEnum, Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ParquetCatalogs {
    GaiaPS1Xmatch,
    CatWISE2020,
}

#[derive(clap::ValueEnum, Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum AsciiCatalogs {
    VSX,
}
