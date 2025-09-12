use crate::fits::FitsRowBatch;
use anyhow::Result;
use fitsio::FitsFile;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};

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

#[derive(Default, Serialize, Deserialize)]
pub struct Ned {
    #[serde(rename(serialize = "_id"))]
    objname: String,
    ra: f64,
    dec: f64,
    objtype: String,
    z: f64,
    z_unc: f64,
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
}
