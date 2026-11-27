import gzip
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from downloaders.search_archival_rv import search


def mrs(obs, minute, band, rv, *, gaia="9007199254740999", warn=0, error=1):
    coadd = minute is None
    epoch = f"{obs}:" + ("coadd" if coadd else str(minute))
    return {
        "_id": f"{epoch}:{band}", "survey": "lamost", "product": "mrs", "release": "dr12-v2.0",
        "ra": 11.0, "dec": 22.0, "mjd": 57990.0, "snr": 25.0,
        "archive_source_id": "native7", "gaia_dr3_source_id": gaia,
        "visit_id": obs, "epoch_id": epoch, "granularity": "visit_coadd" if coadd else "exposure",
        "rv": {"value_km_s": rv, "error_km_s": error, "method": "LAMOST_MRS_BR1"},
        "quality": {"rv_br_flag": warn, "bad_b": 0, "bad_r": 0, "fibermask": 0},
        "spectrum": {"format": "lamost", "url": "https://example.com/spectrum"},
    }


class RepeatRvTests(unittest.TestCase):
    def test_scan_manifest_handles_arms_coadds_and_identity_aliases(self):
        rows = [
            mrs("one", 83556052, "B", 10.0),
            mrs("one", 83556052, "R", 200.0, error=2),
            mrs("one", None, "B", 500.0),
            mrs("two", 83556082, "B", 70.0, gaia=None),
            mrs("three", 83556092, "B", 600.0, warn=1),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shard = root / "visits.jsonl.gz"
            with gzip.open(shard, "wt") as stream:
                for row in rows:
                    stream.write(json.dumps(row) + "\n")
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "format_version": 1, "records": len(rows),
                "jobs": [{"shards": [{"path": shard.name, "bytes": shard.stat().st_size, "records": len(rows)}]}],
            }))
            args = SimpleNamespace(manifest=manifest, footprint=None, output=root / "ranked.jsonl.gz",
                                   scratch_dir=root, survey=None, min_snr=5, min_baseline_days=0,
                                   min_delta_km_s=30, min_sigma=5)
            stats = search(args)
            with gzip.open(args.output, "rt") as stream:
                ranked = list(map(json.loads, stream))
            self.assertEqual(stats["usable_rv_rows"], 4)
            self.assertEqual(len(ranked), 1)
            first = ranked[0]
            self.assertEqual(first["source_id"], "gaia-dr3:9007199254740999")
            self.assertEqual(first["n_visits"], 2)
            self.assertEqual(first["delta_km_s"], 60)
            self.assertAlmostEqual(first["baseline_days"], 30 / 1440)
            self.assertEqual([v["id"] for v in first["best_pair"]],
                             ["one:83556052:B", "two:83556082:B"])
            args.min_baseline_days = 0.1
            self.assertEqual(search(args)["candidates"], 0)
