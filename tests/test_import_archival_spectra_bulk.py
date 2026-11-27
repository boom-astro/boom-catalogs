import gzip
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from downloaders import import_archival_spectra_bulk as bulk


class BulkTests(unittest.TestCase):
    def test_bulk_shard_checkpoint_and_missing_shard_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            rows = [{"_id": str(number)} for number in range(5)]
            with patch.object(bulk, "SHARD_SIZE", 2):
                result = bulk.write_job(path, "sdss/boss", iter(rows))
                self.assertEqual([part["records"] for part in result["shards"]], [2, 2, 1])
                self.assertEqual(bulk.write_job(path, "sdss/boss", iter(())), result)
                (path / result["shards"][0]["path"]).unlink()
                self.assertEqual(bulk.write_job(path, "sdss/boss", iter(rows)), result)
                with gzip.open(path / result["shards"][0]["path"], "rt") as handle:
                    self.assertEqual([json.loads(line) for line in handle], rows[:2])

    def test_incomplete_partition_does_not_get_checkpoint(self):
        def stopped():
            yield {"_id": "one"}
            raise OSError("archive interrupted")

        with tempfile.TemporaryDirectory() as directory, patch.object(bulk, "SHARD_SIZE", 1):
            path = Path(directory)
            with self.assertRaisesRegex(OSError, "archive interrupted"):
                bulk.write_job(path, "lamost/mrs", stopped())
            self.assertFalse((path / "lamost/mrs/done.json").exists())
            result = bulk.write_job(path, "lamost/mrs", iter([{"_id": "one"}, {"_id": "two"}]))
            self.assertEqual(result["records"], 2)

    def test_resume_download_appends_validated_http_range(self):
        client = MagicMock()
        response = client.get.return_value.__enter__.return_value
        response.status_code = 206
        response.headers = {"Content-Range": "bytes 4-7/8"}
        response.iter_content.return_value = iter([b"tail"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "release.gz.part").write_bytes(b"head")
            complete = bulk.download(client, "https://example.com/archive?name=release.gz", path)
            self.assertEqual(complete.read_bytes(), b"headtail")
            self.assertEqual(client.get.call_args.kwargs["headers"]["Range"], "bytes=4-")
            self.assertFalse((path / "release.gz.part").exists())


if __name__ == "__main__":
    unittest.main()
