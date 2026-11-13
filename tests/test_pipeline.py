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
            "changes_by_variable": dict(sorted(collections.Counter(
                c["variable"].split(":")[-1]
                for f in frames for c in f.get("changes", [])).items())),
            "final_node_states": {
                name: [node.get("to_state"), node.get("component"), node.get("member_count")]
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


def load_tool(name):
    """Import one of the extension-less tool scripts as a module."""
    loader = importlib.machinery.SourceFileLoader(name, str(ROOT / name))
    spec = importlib.util.spec_from_loader(name, loader)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses look the module up while it loads
    loader.exec_module(module)
    return module


# A small hand-written two-node log pair. It needs no sample data, so these
# tests also run on a fresh clone.
NODE_A_LOG = """\
250513  1:00:00 mysqld_safe WSREP: Running position recovery with --disable-log-error  --pid-file='/tmp/a.pid'
2025-05-13  1:00:01 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)
2025-05-13  1:00:02 0 [Note] WSREP: New COMPONENT: primary = yes, bootstrap = no, my_idx = 0, memb_num = 1
2025-05-13  1:00:02 0 [Note] WSREP: Quorum results:
\tversion    = 6,
\tcomponent  = PRIMARY,
\tconf_id    = 0,
\tmembers    = 1/1 (joined/total),
\tact_id     = 0,
\tlast_appl. = 0,
\tprotocols  = 5/11/4 (gcs/repl/appl),
\tvote policy= 0,
\tgroup UUID = 11111111-2222-3333-4444-555555555555
2025-05-13  1:00:02 0 [Note] WSREP: Flow-control interval: [16, 16]
2025-05-13  1:00:02 0 [Note] WSREP: Restored state OPEN -> JOINED (1)
2025-05-13  1:00:02 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 1)
2025-05-13  1:00:03 2 [Note] WSREP: Server node-a synced with group
2025-05-13  1:00:04 0 [Warning] Access denied for user 'x'@'y' (using password: YES)
2025-05-13  1:00:05 0 [Warning] WSREP: last inactive check more than PT1.5S ago (PT2S), skipping check
2025-05-13  1:00:06 0 [Note] WSREP: Flow-control interval: [16, 16]
2025-05-13  1:00:07 0 [Note] WSREP: view(view_id(PRIM,aaaaaaaa-0001,2) memb {
2025-05-13  1:00:08 0 [Note] WSREP: New COMPONENT: primary = yes, bootstrap = no, my_idx = 0, memb_num = 2
2025-05-13  1:00:08 0 [Note] WSREP: Flow-control interval: [23, 23]
2025-05-13  1:00:09 0 [Note] WSREP: Member 1.0 (node-b) requested state transfer from '*any*'. Selected 0.0 (node-a)(SYNCED) as donor.
2025-05-13  1:00:09 0 [Note] WSREP: Shifting SYNCED -> DONOR/DESYNCED (TO: 5)
WSREP_SST: [ERROR] mariadb-backup finished with error: 1. Check syslog for details (20250513 01:00:10.123)
2025-05-13  1:00:11 0 [Warning] WSREP: 0.0 (node-a): State transfer to 1.0 (node-b) failed: Invalid argument
2025-05-13  1:00:11 0 [Note] WSREP: Shifting DONOR/DESYNCED -> JOINED (TO: 5)
2025-05-13  1:00:12 0 [Note] WSREP: Member 0.0 (node-a) synced with group.
2025-05-13  1:00:12 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 5)
2025-05-13  1:00:20 0 [Note] WSREP: New COMPONENT: primary = no, bootstrap = no, my_idx = 0, memb_num = 1
2025-05-13  1:00:20 0 [Note] WSREP: Flow-control interval: [16, 16]
2025-05-13  1:00:20 0 [Note] WSREP: Received NON-PRIMARY.
2025-05-13  1:00:20 0 [Note] WSREP: Shifting SYNCED -> OPEN (TO: 5)
"""

NODE_B_LOG = """\
2025-05-13  1:00:08 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)
2025-05-13  1:00:08 0 [Note] WSREP: New COMPONENT: primary = yes, bootstrap = no, my_idx = 1, memb_num = 2
2025-05-13  1:00:08 0 [Note] WSREP: Shifting OPEN -> PRIMARY (TO: 5)
2025-05-13  1:00:09 0 [Note] WSREP: Member 1.0 (node-b) requested state transfer from '*any*'. Selected 0.0 (node-a)(SYNCED) as donor.
2025-05-13  1:00:09 0 [Note] WSREP: Shifting PRIMARY -> JOINER (TO: 5)
2025-05-13  1:00:30 2 [Note] WSREP: Server node-b synced with group
"""


class SyntheticLogTest(unittest.TestCase):
    """Parsing and state-machine rules, checked on a hand-written log pair."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        a, b = Path(cls.tmp.name) / "a.log", Path(cls.tmp.name) / "b.log"
        a.write_text(NODE_A_LOG)
        b.write_text(NODE_B_LOG)
        extractor = load_tool("grap3").EntityExtractor()
        cls.grap = extractor.process_files([a, b])
        cls.entities = cls.grap["entities"]
        graf = load_tool("graf3")
        cls.frames = graf.build_frames(cls.grap)
        cls.event_frames = graf.build_frames(cls.grap, granularity="event")

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def by_pattern(self, name):
        return [e for e in self.entities if e.get("pattern_name") == name]

    def test_single_digit_hours_are_parsed(self):
        shifts = self.by_pattern("node_state_shifting")
        self.assertEqual(len(shifts), 9)
        self.assertEqual(shifts[0]["timestamp"], "2025-05-13 01:00:01")

    def test_entities_belong_to_the_node_that_logged_them(self):
        # node-b only names itself in the last line of its log
        for e in self.entities:
            if e.get("pattern_name") in ("node_state_shifting", "component_change"):
                expected = "node-a" if e["log_source"].endswith("a.log") else "node-b"
                self.assertEqual(e.get("node_name"), expected, e["raw_line"])

    def test_headless_view_block_does_not_swallow_lines(self):
        # the view( header at 01:00:07 has no body; the lines after it must survive
        self.assertEqual(len(self.by_pattern("component_change")), 4)
        self.assertEqual([e for e in self.entities if e["entity_type"] == "view"], [])

    def test_quorum_comes_from_component_messages(self):
        components = [(e["node_name"], e["component"], e["member_count"])
                      for e in self.by_pattern("component_change")]
        self.assertEqual(components, [
            ("node-a", "PRIMARY", 1), ("node-a", "PRIMARY", 2),
            ("node-b", "PRIMARY", 2), ("node-a", "NON_PRIMARY", 1)])
        result = self.by_pattern("quorum_results_multiline")
        self.assertEqual(len(result), 1)
        self.assertEqual((result[0]["component"], result[0]["conf_id"],
                          result[0]["members_joined"], result[0]["member_count"]),
                         ("PRIMARY", 0, 1, 1))

    def test_mysqld_safe_option_name_is_not_an_error(self):
        self.assertEqual(self.by_pattern("error_safe_log"), [])

    def test_sst_script_errors_are_captured_with_their_own_time(self):
        errors = self.by_pattern("error_sst_script")
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["timestamp"], "2025-05-13 01:00:10")
        self.assertIn("mariadb-backup finished with error", errors[0]["message"])

    def test_sst_failure_is_an_sst_entity(self):
        failed = self.by_pattern("sst_transfer_failed")
        self.assertEqual(len(failed), 1)
        self.assertEqual((failed[0]["donor_name"], failed[0]["joiner_name"],
                          failed[0]["failure_reason"]),
                         ("node-a", "node-b", "Invalid argument"))

    def test_every_frame_is_a_state_change(self):
        self.assertGreater(len(self.frames), 0)
        for frame in self.frames[1:]:
            self.assertTrue(frame["changes"], frame["event"])

    def test_non_state_events_do_not_create_frames(self):
        types = {f["event"]["entity_type"] for f in self.frames}
        self.assertEqual(types - {"node_state", "quorum", "flow_control", "wsrep_view", "cluster",
                                  "sst", "ist", "log_coverage"}, set())
        # "Access denied", the WSREP warning and the repeated [16, 16]
        # flow-control line at 01:00:06 change nothing
        stamps = [f["event"]["timestamp"] for f in self.frames]
        for quiet in ("2025-05-13T01:00:04", "2025-05-13T01:00:05", "2025-05-13T01:00:06"):
            self.assertNotIn(quiet, stamps)

    def test_state_is_tracked_per_node(self):
        last = self.frames[-1]["nodes"]
        self.assertEqual((last["node-a"]["to_state"], last["node-a"]["component"],
                          last["node-a"]["member_count"], last["node-a"]["flow_control"]),
                         ("OPEN", "NON_PRIMARY", 1, [16, 16]))
        self.assertEqual((last["node-b"]["to_state"], last["node-b"]["component"],
                          last["node-b"]["member_count"]),
                         ("JOINER", "PRIMARY", 2))

    def test_changes_name_the_variable_that_changed(self):
        changes = [(c["variable"], c["from"], c["to"])
                   for f in self.frames for c in f["changes"]]
        self.assertIn(("node:node-a:to_state", "SYNCED", "DONOR/DESYNCED"), changes)
        self.assertIn(("node:node-a:component", "PRIMARY", "NON_PRIMARY"), changes)
        self.assertIn(("node:node-a:flow_control", [16, 16], [23, 23]), changes)

    def frame_at(self, stamp):
        return next(f for f in self.frames if f["event"]["timestamp"] == stamp)

    def test_one_frame_per_second(self):
        stamps = [f["event"]["timestamp"] for f in self.frames]
        self.assertEqual(len(stamps), len(set(stamps)))
        self.assertEqual([f["index"] for f in self.frames], list(range(len(self.frames))))
        # --granularity event keeps one frame per change
        self.assertGreater(len(self.event_frames), len(self.frames))
        self.assertEqual(sum(f["event_count"] for f in self.frames), len(self.event_frames))

    def test_steps_within_a_second_are_kept_in_order(self):
        # node-a goes OPEN -> JOINED -> SYNCED within 01:00:02
        frame = self.frame_at("2025-05-13T01:00:02")
        steps = [(c["from"], c["to"]) for c in frame["changes"]
                 if c["variable"] == "node:node-a:to_state"]
        self.assertEqual(steps, [("OPEN", "JOINED"), ("JOINED", "SYNCED")])
        self.assertEqual(frame["nodes"]["node-a"]["to_state"], "SYNCED")
        # 01:00:08 has changes on both nodes in one frame
        nodes = {c["variable"].split(":")[1] for c in self.frame_at("2025-05-13T01:00:08")["changes"]}
        self.assertEqual(nodes, {"node-a", "node-b"})

    def test_state_outside_a_nodes_log_is_marked(self):
        # node-b's log starts at 01:00:08; node-a's ends at 01:00:20, before node-b's
        self.assertEqual(self.frames[0]["nodes"]["node-b"]["log_status"], "NO_LOG")
        self.assertEqual(self.frame_at("2025-05-13T01:00:08")["nodes"]["node-b"]["log_status"], "ACTIVE")
        last = self.frames[-1]["nodes"]
        self.assertEqual((last["node-a"]["log_status"], last["node-a"]["log_ended_at"]),
                         ("ENDED", "2025-05-13T01:00:20"))
        # the end of the last log is the end of the analysis, not a state change
        self.assertEqual(last["node-b"]["log_status"], "ACTIVE")

    def test_state_transfer_is_a_state_variable(self):
        def transfer(stamp, node):
            t = self.frame_at(stamp)["nodes"][node].get("state_transfer")
            return t and (t["role"], t["peer"], t["status"])
        self.assertEqual(transfer("2025-05-13T01:00:09", "node-a"), ("donor", "node-b", "IN_PROGRESS"))
        self.assertEqual(transfer("2025-05-13T01:00:09", "node-b"), ("joiner", "node-a", "IN_PROGRESS"))
        failed = self.frame_at("2025-05-13T01:00:11")["nodes"]["node-a"]["state_transfer"]
        self.assertEqual((failed["status"], failed["reason"]), ("FAILED", "Invalid argument"))
        # cleared once the donor is SYNCED again
        self.assertIsNone(transfer("2025-05-13T01:00:12", "node-a"))
        # node-b never logged the failure: its own view of the transfer stays open
        self.assertEqual(transfer("2025-05-13T01:00:20", "node-b"), ("joiner", "node-a", "IN_PROGRESS"))


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
