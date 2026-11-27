import gzip
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from downloaders.search_archival_rv import search
from test_search_archival_rv import mrs


LAMOST = "lamost:dr12-v2.0:mrs:LAMOST_MRS_BR1"
SDSS = "sdss:dr20:boss:BOSSNet"


def pair(gaia, base, delta, n=2, frame="survey_native"):
    rows = []
    for index in range(n):
        a = mrs(f"{gaia}-a{index}", 83556052 + index * 1440, "R", base + index, gaia=gaia)
        a["archive_source_id"] = gaia
        a["rv"]["frame"] = frame
        b = {
            "_id": f"{gaia}-b{index}", "survey": "sdss", "release": "dr20",
            "product": "boss", "granularity": "visit_coadd", "ra": 11.0, "dec": 22.0,
            "gaia_dr3_source_id": gaia, "mjd": 58000.0 + index,
            "snr": 25.0, "quality": {"flag_bad": False, "flag_warn": False},
            "rv": {"value_km_s": base + index + delta, "error_km_s": 1.0,
                   "method": "BOSSNet", "frame": "survey_native"},
        }
        rows.extend((a, b))
    return rows


class CalibratedPairsTests(unittest.TestCase):
    def test_fit_then_recover_single_visits_only_across_calibrated_streams(self):
        rows = [row for i in range(6) for row in pair(str(100 + i), 100 - i, 10)]
        rows.extend(pair("200", 20, 120, n=1))
        rows.extend(pair("201", 20, 120, n=1, frame="heliocentric"))
        rows.extend(pair("202", 20, 120, n=1))
        rows[-1]["snr"] = 6.0
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "test.jsonl.gz"
            with gzip.open(path, "wt") as stream:
                for row in rows:
                    stream.write(json.dumps(row) + "\n")
            fit = root / "calibration.json"
            args = SimpleNamespace(
                manifest=None, footprint=path, output=fit, scratch_dir=root,
                survey=None, min_snr=10, min_baseline_days=0, min_delta_km_s=30, min_sigma=5,
                fit_cross_pipeline=(LAMOST, SDSS), rv_frames=("survey_native", "survey_native"),
                min_training_controls=3, min_holdout_controls=3, control_max_span_km_s=20,
                control_ids=None, cross_pipeline_calibration=None,
            )
            report = search(args)
            self.assertEqual((report["training_sources"], report["holdout_sources"]), (3, 3))
            self.assertEqual(report["holdout_rms_km_s"], 0)
            args.fit_cross_pipeline = None
            args.output = root / "ranked.jsonl.gz"
            self.assertEqual(search(args)["candidates"], 0)
            args.cross_pipeline_calibration = fit
            args.min_snr = 5
            self.assertEqual(search(args)["cross_pipeline_candidates"], 1)
            with gzip.open(args.output, "rt") as stream:
                candidate = json.loads(stream.readline())
            self.assertEqual(candidate["source_id"], "gaia-dr3:200")
            self.assertEqual(candidate["comparison"], "cross_pipeline")
            self.assertEqual(candidate["delta_km_s"], 110)
            self.assertEqual(candidate["cross_pipeline"]["raw_b_minus_a_km_s"], 120)
            self.assertEqual([visit["pipeline"] for visit in candidate["best_pair"]], [LAMOST, SDSS])
