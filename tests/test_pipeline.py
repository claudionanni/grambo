"""Regression tests for the grambo v3 pipeline.

Run from the repo root:

    .venv/bin/python -m unittest discover -s tests -v

The pipeline tests run grax3 on sample logs and compare a compact summary of
the output against tests/golden/<name>.json. The sample logs (cl407/,
unittest/) are not in the repository, so those tests are skipped when the
logs are missing.

After an intentional change in parsing behaviour, regenerate the summaries
and review the diff before committing:

    GRAMBO_UPDATE_GOLDEN=1 .venv/bin/python -m unittest discover -s tests
    git diff tests/golden
"""

import collections
import importlib.machinery
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GOLDEN_DIR = Path(__file__).resolve().parent / "golden"
UPDATE = os.environ.get("GRAMBO_UPDATE_GOLDEN") == "1"

# name -> log files, relative to the repo root
FIXTURES = {
    "cl407": [
        "cl407/error.11407.log",
        "cl407/error.21407.log",
        "cl407/error.31407.log",
    ],
    "unittest_run1": [
        "unittest/error.11407.log",
        "unittest/error.21407.log",
        "unittest/error.31407.log",
    ],
    "unittest_run2": [
        "unittest/error.11407-02.log",
        "unittest/error.21407-02.log",
        "unittest/error.31407-02.log",
    ],
}


def run_grax(out_dir, logs=(), stdin=None):
    """Run the full pipeline without the web UI; return the completed process."""
    cmd = [sys.executable, str(ROOT / "grax3"), "--python", sys.executable,
           "--no-serve", "--output-dir", str(out_dir), *logs]
    return subprocess.run(cmd, cwd=ROOT, stdin=stdin, capture_output=True, text=True)


def summarize(out_dir):
    """Reduce the pipeline artifacts to a small, stable, reviewable summary."""
    out_dir = Path(out_dir)
    sst = json.loads((out_dir / "sst_sessions.json").read_text())
    grap = json.loads((out_dir / "grap_output.json").read_text())
    with (out_dir / "graf_frames.ndjson").open() as fh:
        frames = [json.loads(line) for line in fh if line.strip()]

    last = frames[-1] if frames else {}
    return {
        "sst": {
            "total_sessions": sst.get("total_sessions"),
            "sessions": [
                {
                    "status": s.get("status"),
                    "start": s.get("start_time"),
                    "end": s.get("end_time"),
                    "duration_seconds": s.get("duration_seconds"),
                    "joiner": s.get("joiner_node"),
                    "donor": s.get("donor_node"),
                    "method": s.get("transfer_method"),
                    "events": len(s.get("events", [])),
                }
                for s in sst.get("sessions", [])
            ],
        },
        "entities": {
            "total": grap.get("total_entities"),
            "by_type": dict(sorted(collections.Counter(
                e.get("entity_type") for e in grap.get("entities", [])).items())),
        },
        "frames": {
            "total": len(frames),
            "first_timestamp": frames[0]["event"].get("timestamp") if frames else None,
            "last_timestamp": last.get("event", {}).get("timestamp"),
            "by_event_type": dict(sorted(collections.Counter(
                f.get("event", {}).get("entity_type") for f in frames).items())),
            "final_node_states": {
                name: node.get("node_state")
                for name, node in sorted(last.get("nodes", {}).items())
            },
        },
    }


class PipelineGoldenTest(unittest.TestCase):
    maxDiff = None

    def check_fixture(self, name):
        logs = FIXTURES[name]
        missing = [p for p in logs if not (ROOT / p).exists()]
        if missing:
            self.skipTest(f"sample logs not present: {', '.join(missing)}")

        with tempfile.TemporaryDirectory() as tmp:
            proc = run_grax(tmp, logs)
            self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
            actual = summarize(tmp)

        golden_path = GOLDEN_DIR / f"{name}.json"
        if UPDATE:
            GOLDEN_DIR.mkdir(exist_ok=True)
            golden_path.write_text(json.dumps(actual, indent=2) + "\n")
            return
        self.assertTrue(golden_path.exists(),
                        f"{golden_path} missing; run with GRAMBO_UPDATE_GOLDEN=1")
        self.assertEqual(json.loads(golden_path.read_text()), actual)

    def test_cl407(self):
        self.check_fixture("cl407")

    def test_unittest_run1(self):
        self.check_fixture("unittest_run1")

    def test_unittest_run2(self):
        self.check_fixture("unittest_run2")


class GrafDeterminismTest(unittest.TestCase):
    LOGS = FIXTURES["cl407"]

    def test_frames_are_byte_identical_across_hash_seeds(self):
        if any(not (ROOT / p).exists() for p in self.LOGS):
            self.skipTest("sample logs not present")
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_grax(tmp, self.LOGS)
            self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
            outputs = set()
            for seed in ("1", "2", "3"):
                proc = subprocess.run(
                    [sys.executable, str(ROOT / "graf3"),
                     str(Path(tmp) / "grap_output.json"), "--ndjson"],
                    capture_output=True, cwd=ROOT,
                    env={**os.environ, "PYTHONHASHSEED": seed})
                self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
                outputs.add(proc.stdout)
            self.assertEqual(len(outputs), 1)


class StdinTest(unittest.TestCase):
    LOG = "cl407/error.31407.log"

    def test_stdin_matches_file_argument(self):
        if not (ROOT / self.LOG).exists():
            self.skipTest(f"sample log not present: {self.LOG}")
        with tempfile.TemporaryDirectory() as from_file, \
                tempfile.TemporaryDirectory() as from_stdin:
            proc = run_grax(from_file, [self.LOG])
            self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
            with (ROOT / self.LOG).open() as fh:
                proc = run_grax(from_stdin, stdin=fh)
            self.assertEqual(proc.returncode, 0, proc.stderr[-2000:])
            self.assertEqual(summarize(from_file), summarize(from_stdin))

    def test_empty_stdin_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            proc = run_grax(tmp, stdin=subprocess.DEVNULL)
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("stdin was empty", proc.stderr)


class GralcTest(unittest.TestCase):
    CANONICAL = "2025-09-09 07:41:41 0 [Note] WSREP: Shifting SYNCED -> OPEN (TO: 10344464)\n"

    def convert(self, fmt, text):
        proc = subprocess.run(
            [sys.executable, str(ROOT / "gralc"), "-f", str(fmt)],
            input=text, capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        return proc.stdout

    def test_format_1_mysql8_with_wsrep_tag(self):
        line = ("2025-09-09T07:41:41.832615+02:00 0 [System] [MY-000000] [WSREP] "
                "P: Shifting SYNCED -> OPEN (TO: 10344464)\n")
        self.assertEqual(self.convert(1, line), self.CANONICAL)

    def test_format_2_mysql8_without_wsrep_tag(self):
        line = ("2025-09-09T07:41:41.832615+02:00 0 [System] [MY-000000] "
                "P: Shifting SYNCED -> OPEN (TO: 10344464)\n")
        self.assertEqual(self.convert(2, line), self.CANONICAL)

    def test_auto_detects_each_line(self):
        mixed = (
            "2025-09-09T07:41:41.832615+02:00 0 [System] [MY-000000] [WSREP] "
            "P: Shifting SYNCED -> OPEN (TO: 10344464)\n"
            "2025-09-09T07:41:41.832615Z 0 [System] [MY-000000] "
            "P: Shifting SYNCED -> OPEN (TO: 10344464)\n"
            "2025-09-22 20:46:30 0 [Warning] WSREP: Failed to prepare for incremental state transfer\n"
            "not a log line\n"
        )
        expected = (self.CANONICAL * 2 +
                    "2025-09-22 20:46:30 0 [Warning] WSREP: Failed to prepare for incremental state transfer\n"
                    "not a log line\n")
        self.assertEqual(self.convert("auto", mixed), expected)
        # auto is also the default when input is piped
        proc = subprocess.run([sys.executable, str(ROOT / "gralc")],
                              input=mixed, capture_output=True, text=True, cwd=ROOT)
        self.assertEqual(proc.stdout, expected)

    def test_invalid_format_is_rejected(self):
        proc = subprocess.run([sys.executable, str(ROOT / "gralc"), "-f", "9"],
                              input="x\n", capture_output=True, text=True, cwd=ROOT)
        self.assertNotEqual(proc.returncode, 0)

    def test_format_3_mariadb_is_unchanged(self):
        line = "2025-09-22 20:46:30 0 [Warning] WSREP: Failed to prepare for incremental state transfer\n"
        self.assertEqual(self.convert(3, line), line)


class GravSmokeTest(unittest.TestCase):
    """Load grav3 in-process and hit its routes with Flask's test client."""

    LOGS = FIXTURES["cl407"]

    @classmethod
    def setUpClass(cls):
        try:
            import flask  # noqa: F401
        except ImportError:
            raise unittest.SkipTest("flask not installed")
        if any(not (ROOT / p).exists() for p in cls.LOGS):
            raise unittest.SkipTest("sample logs not present")

        cls.tmp = tempfile.TemporaryDirectory()
        proc = run_grax(cls.tmp.name, cls.LOGS)
        if proc.returncode != 0:
            cls.tmp.cleanup()
            raise AssertionError(proc.stderr[-2000:])

        loader = importlib.machinery.SourceFileLoader("grav3", str(ROOT / "grav3"))
        spec = importlib.util.spec_from_loader("grav3", loader)
        cls.grav = importlib.util.module_from_spec(spec)
        loader.exec_module(cls.grav)

        out = Path(cls.tmp.name)
        cls.grav.SST_REPORT_PATH = str(out / "graa3_sst.txt")
        cls.grav.SST_JSON_PATH = str(out / "sst_sessions.json")
        cls.grav.FRAMES = cls.grav.load_frames_from_file(str(out / "graf_frames.ndjson"))
        cls.grav.refresh_hotspot_cache()
        cls.client = cls.grav.app.test_client()

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_pages_render(self):
        for path in ("/", "/sst", "/docs"):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 200)

    def test_frames_are_served(self):
        total = len(self.grav.FRAMES)
        self.assertGreater(total, 0)
        for idx in (0, total - 1):
            with self.subTest(frame=idx):
                resp = self.client.get(f"/frame/{idx}")
                self.assertEqual(resp.status_code, 200)
                self.assertIsInstance(resp.get_json(), dict)

    def test_sst_sessions_api(self):
        data = self.client.get("/api/sst-sessions").get_json()
        self.assertNotIn("error", data)
        self.assertEqual(data["total_sessions"], len(data["sessions"]))
        self.assertGreater(data["total_sessions"], 0)

    def test_empty_frames_file_loads_as_no_frames(self):
        with tempfile.NamedTemporaryFile("w", suffix=".ndjson") as fh:
            self.assertEqual(self.grav.load_frames_from_file(fh.name), [])


if __name__ == "__main__":
    unittest.main()
