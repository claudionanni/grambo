# Gramboo Future Todos & Enhancements

This file captures improvement ideas and feature suggestions collected during development.

## Parsing & Detection
- Multi-line join: pre-process soft-wrapped lines (e.g. "Server <name> connected..." split over lines) before regex parsing.
- Truncation detection: flag likely truncated logs (no membership view but JOINER → PRIMARY transitions present).
- Add detection for cluster bootstrap vs regular restart (look for wsrep_start_position vs safe_to_bootstrap).
- Distinguish first startup UUID vs subsequent restarts via earliest timestamp identity evidence.
- Recognize garbd (arbitrator) logs and optionally exclude or summarize separately.

## UUID & Identity
- Label candidate UUID sets explicitly with a suffix ("(candidates)") when derived only from listening lines.
- Annotate startup phase histories with tag "(startup/truncated phase)" when only partial evidence collected.
- Persist alias map to optional cache file for cross-log correlation (improve matching across separate log captures).
- Add confidence scoring to each displayed UUID (full=100, composite=70, partial listening=60, heuristic=40).
- Add optional (NDI) marker (Not Deterministically Inferred):
	- Applied in CLUSTER NODES when final UUID unknown (<NDI> instead of full text)
	- Applied per-fragment in UUID HISTORY for partial/candidate-only entries
	- Controlled by --mark-ndi / --no-mark-ndi flag (default on)
	- Heuristic: mark if length < 36 OR absent from any membership view OR sourced solely from listening candidates

## Output & Reporting
- JSON schema versioning and emit --schema-version flag.
- Optional --quiet and --sections flags to limit output sections.
- Table formatting for UUID HISTORY with columns: name | final uuid | prior uuids | candidate uuid fragments.
- Add --color flag and ANSI coloring (warnings, errors, instability) when stdout is a TTY.
- Summarize time gaps or downtime periods between views / transitions.

## Metrics & Analysis
- Compute join latency (time from JOINER to SYNCED per node) and average across nodes.
- Compute SST duration and success/failure rates; correlate with donor node.
- Flow control ratio metrics: (pause duration / total runtime) if timestamps granular enough.
- Cluster churn index: (UUID changes per physical node) / observation window length.
- Stability trend sparkline (ASCII) based on view changes over time buckets.

## SST / IST Deep Dive
- Link each SST request to the donor and final completion with duration.
- Identify IST fallback to SST scenarios (when IST incomplete -> triggers SST).
- Track IST byte throughput if log lines contain size info (extend regex set).

## Heuristics & Intelligence
- Autodetect probable local node when multiple candidates exist (score by earliest listen + most transitions referencing its name).
- Implement pluggable heuristics module with toggles (--strict, --aggressive) adjusting evidence thresholds.
- Add anomaly detection: sudden surge in state transitions or repeated failed IST ranges.

## Performance & Code Quality
- Split `gramboo.py` into modules: parsing.py, model.py, output.py, metrics.py for maintainability.
- Add basic unit tests using pytest with fixture logs.
- Introduce logging (debug/info) behind --debug flag instead of ad-hoc prints (currently none, but future dev ease).
- Profile large logs and optimize hot regex paths (precompile more patterns, maybe Aho-Corasick for keywords).

## CLI & UX
- Provide `--format=yaml` option.
- Add `--filter-node=<name>` to restrict events/sections to a single physical node.
- Add `--since <timestamp>` and `--until <timestamp>` to slice log timeframe.
- Interactive TUI mode (curses) to navigate sections and expand/collapse histories.
- Remove deprecated flags (`--dialect`, `--galera-version`) in a future major (leave warnings for two minor versions first).

## Extensibility
- Plugin system to allow custom event extractors (entry points or dynamic module loading).
- Export timeline as CSV or NDJSON for downstream analytics.
- REST API mode (run as service: POST log chunk, receive JSON summary incrementally).

## Reliability & Edge Cases
- Gracefully handle logs with mixed time zones or missing dates (infer year/day from first line).
- Detect and mark clock skew (non-monotonic timestamps).
- Provide warning when multiple distinct IPs map to same UUID with equal top scores (possible reuse scenario).

## Documentation
- Expand README_python with examples for each event type and JSON schema snippet.
- Add architecture diagram (parsing pipeline, identity resolution, output assembly).
- Provide troubleshooting guide (empty nodes, no views, truncated log indicators).

## Nice-to-Have Future Ideas
- Machine learning suggestion: classify cause of instability (network vs node churn) based on pattern frequencies.
- Correlate with MySQL error log if provided (multi-file input option).
- Generate Mermaid diagrams for view evolution timeline.

---
Add new ideas below this line:
