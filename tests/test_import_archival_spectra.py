import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from downloaders import import_archival_spectra as importer


class FootprintTests(unittest.TestCase):
    def test_cone_crossing_right_ascension_zero(self):
        cone = importer.Cone(359.95, 1, 0.1)
        self.assertTrue(cone.contains(0.02, 1))
        self.assertFalse(cone.contains(0.08, 1))

    def test_tap_overflow_is_an_error(self):
        raw = b"""<VOTABLE xmlns="http://www.ivoa.net/xml/VOTable/v1.3">
            <RESOURCE><INFO name="QUERY_STATUS" value="OK"/>
            <TABLE><FIELD name="obsid"/><DATA><TABLEDATA/></DATA></TABLE>
            <INFO name="QUERY_STATUS" value="OVERFLOW"/></RESOURCE></VOTABLE>"""
        with self.assertRaisesRegex(ValueError, "OVERFLOW"):
            importer.votable_rows(raw)

    def test_mrs_arms_share_an_epoch_but_keep_both_locators(self):
        shared = {
            "obsid": "1227101003", "ra": "233.4", "dec": "38", "mjd": "60389",
            "snr": "8", "rv_br1": "-9999", "rv_br1_err": "-9999",
            "rv_br_flag": "-9999", "fibermask": "0", "bad_b": "0", "bad_r": "0",
            "gaia_source_id": "1375790522988340352",
        }
        native = [
            {**shared, "band": band, "coadd": coadd, "lmjm": minute}
            for band in "BR" for coadd, minute in [("0", "86960260"), ("1", "")]
        ]
        with patch.object(importer, "tap", side_effect=[[], [], native]):
            rows = list(importer.lamost(None, importer.Cone(233.4, 38, 0.1)))
        self.assertEqual(len({row["_id"] for row in rows}), 4)
        self.assertEqual(len({row["epoch_id"] for row in rows}), 2)
        self.assertEqual({row["spectrum"]["extensions"][0] for row in rows},
                         {"B-86960260", "R-86960260", "COADD_B", "COADD_R"})
        self.assertTrue(all(row["rv"] is None for row in rows))

    def test_boss_locator_is_scoped_to_supported_daily_reduction(self):
        row = {"run2d": "v6_2_1", "field": "102314", "mjd": "60108",
               "catalogid": "27021598726644162"}
        self.assertTrue(importer.boss_locator(row)["url"].endswith(
            "102XXX/102314/60108/spec-102314-60108-27021598726644162.fits"
        ))
        self.assertIsNone(importer.boss_locator({**row, "run2d": "unknown"}))

    def test_catalog_is_deterministic_and_preserves_native_id_precision(self):
        native_id = "39627848784286649"
        row = importer.record("desi", "dr1", "test", native_id, {"ra": "0.01", "dec": "4"},
                              "exposure", archive_source_id=native_id)
        rows = {row["_id"]: row}
        with tempfile.TemporaryDirectory() as directory:
            left, right = (Path(directory) / name for name in ("first.gz", "second.gz"))
            importer.write_catalog(left, rows)
            importer.write_catalog(right, rows)
            self.assertEqual(left.read_bytes(), right.read_bytes())
            with gzip.open(left, "rt") as stream:
                restored = json.loads(stream.readline())
        self.assertEqual(restored["archive_source_id"], native_id)
        self.assertEqual(restored["coordinates"]["radec_geojson"]["coordinates"], [-179.99, 4.0])


if __name__ == "__main__":
    unittest.main()
