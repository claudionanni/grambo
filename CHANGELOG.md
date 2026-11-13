# Changelog

All notable changes to Grambo will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Frames are state changes.** `graf3` now models the cluster as a state
  machine with a defined set of variables per node (Galera state, node UUID,
  component status and size, flow-control interval, wsrep view). A frame is
  emitted only when one of them changes, and each frame lists what changed in
  a new `changes` field. Errors, warnings and other events that change no
  variable no longer create frames.
- **One frame per second.** All state changes that carry the same timestamp
  are one frame. Log timestamps have one-second resolution and node clocks are
  not synchronised, so the order of changes on different nodes within a second
  was arbitrary. The frame shows the state at the end of the second; `changes`
  keeps every step in log order. `graf3 --granularity event` restores one
  frame per change.
- **A node's state comes from its own log.** What peers log about a node
  (`Member X synced with group`, donor selection, transfer result) is used
  only while that node has no log covering the moment.
- **Quorum comes from gcs.** Component status is taken from
  `New COMPONENT: primary = yes|no ... memb_num = N` and the `Quorum results:`
  block, per node. The old `quorum_lost` / `quorum_regained` patterns are gone.
- `graf3` is much faster on large inputs and frame files are much smaller; at
  most the 10 most recent UUIDs of a node are repeated in each frame.

### Added
- **State transfer is a state variable.** Each node carries `state_transfer`
  (role, peer, type SST / IST / SST+IST, status IN_PROGRESS / COMPLETED /
  FAILED / INTERRUPTED) from donor selection until it is SYNCED again. The
  cluster graph draws its donor → joiner arrow from it.
- **Log coverage is a state variable.** `log_status` is NO_LOG before a node's
  log starts, ACTIVE while it runs and ENDED after its last line; from then on
  the UI marks the node's values as last known.
- "Changes in this frame" card in the UI.
- Patterns for `State transfer from X complete` (joiner side) and
  `async IST sender starting to serve` (donor side).
- Patterns for `SST received`, `SST sent`, `SST sending failed`,
  `State transfer to X failed/complete`, the IST range, `IST received`,
  `Member N.N (name) synced with group`, and `WSREP_SST: [ERROR]` script
  errors.

### Fixed
- **State transfer sessions were misclassified** by `graa3`. With several
  logs, a failed transfer was never reported as FAILED;
  `SST succeeded for position <uuid>:-1`, which Galera logs for a failed
  transfer, counted as COMPLETED; an IST-only transfer was INCOMPLETE; and
  events of all nodes were fed into one session at a time. Sessions are now
  read per joiner from its own log, end on the joiner's actual result, carry
  the transfer type and the failure reason, and transfers to a node whose log
  is missing are rebuilt from a peer's log.
- **Lines logged between 00:00 and 09:59 were ignored** by every pattern,
  because MariaDB pads the hour with a space. About 15% of all lines.
- **Events were attributed to the wrong node.** The node name was taken from
  the first "synced with group" line and carried over from the previous file,
  so the start of each log belonged to another node or to none.
- A view header without its body no longer swallows the lines that follow.
- `mysqld_safe ... --disable-log-error` is no longer reported as an error.

### Removed
- Patterns that never matched any real log: `sst_completed`, `ist_started`,
  `ist_completed`, `cluster_uuid`, `view_members_line`, `member_state_in_view`.

### In Progress
- Flow control health status refinement based on actual cluster impact
- Connectivity events (connection refused, timeouts, suspected nodes)

## [3.0.0-alpha.2] - 2026-11-13

### Added
- **gralc**: log canonicalizer that converts MySQL 8.0-style Galera logs to the
  format expected by the v3 tools. The format is detected line by line, so no
  format ID is needed; `--format=N` forces a single format. See `GRALC.md`.
- **grax3 reads stdin**: `./gralc error.log | ./grax3` now works. Piped input
  is saved to `<output-dir>/stdin.log`.
- **Regression tests** in `tests/`, run with
  `python -m unittest discover -s tests`.

### Changed
- `grav3` no longer runs Flask in debug mode by default; pass `--debug` to
  enable it (local use only).
- `graf3` output is byte-identical between runs on the same input.

### Fixed
- `grav3` crashed when the frames file was empty.

### Removed
- Unused v2 code (`lib/`, `patterns/`, `schema/`), which the v3 tools never
  loaded.
- Obsolete development status reports from `docs/`, and editor/assistant
  configuration that referred to files that no longer exist.

## [3.0.0-alpha.1] - 2025-01-06

### Added
- **v3 Tool Suite**: Complete rewrite with `graa3`, `grap3`, `graf3`, `grav3` tools
- **grax3 Orchestrator**: One-command pipeline execution wrapper
- **Flow Control Monitoring**: New entity type for tracking Galera flow control intervals
  - Flow control intervals parsing from logs
  - Timeline markers for flow control events
  - Dedicated flow control card in UI
  - Source code analysis integration for theoretical understanding
- **Natural Timeline**: Visual timeline bar showing actual time gaps between frames
  - Vertical markers for state change events
  - Clickable markers to jump to specific times
  - Differentiation between frame sequence and actual time progression
- **Enhanced UI**:
  - Draggable frame timeline with "drag to reposition" instruction
  - Documentation links in cards for contextual help
  - Improved card organization with proper sectioning
  - SST operation tracking with correct group assignment
  - View change counting improvements
- **Documentation System**:
  - In-card help links explaining metrics
  - Theory documentation for flow control
  - Frame navigation explanation
  - SST impact notes (joiner log reset warnings)

### Changed
- Renamed all tools with `3` suffix to indicate v3 generation:
  - `graa` → `graa3`
  - `grap` → `grap3`
  - `graf` → `graf3`
  - `grav` → `grav3`
  - `grax` → `grax3`
- Output file names updated to reflect tool versions (`graa3_sst.txt`)
- UI layout optimized for multiple analysis cards
- Section headers properly implemented for card organization
- Node data now correctly contained within cards

### Fixed
- **Critical**: SST operations counting in cluster details card
  - Now correctly sums SST operations per group
  - Assigns SST operations to correct group based on lifetime
  - Group lifetime defined as time from group first_seen to next group first_seen or now()
  - Fixed issue where all SST operations were assigned to first group
- **Critical**: View changes counting in cluster details card
  - Corrected excessive view change counts
  - Accurate per-group view change tracking
- SST Sessions viewer now points to correct file (`grax_output/sst_sessions.json`)
- SST Sessions link moved to more visible location in SST card
- Timeline markers now properly offset to avoid overlap
- Cards properly contained within their sections
- Node data display boundaries fixed

### Technical
- Entity extraction enhanced for flow control patterns
- Frame generation includes flow control state
- Group lifetime calculation for temporal entity assignment
- Improved entity-to-group correlation logic

## [2.0.0] - 2024 (v2-rewrite branch)

### Added
- Python rewrite of original bash script
- Entity-based architecture
- JSON/YAML output formats
- Pattern matching system
- SST/IST session tracking
- Web visualization with `grav`

### Changed
- Complete architectural redesign
- From line-based parsing to entity extraction
- Modular, maintainable code structure

## [1.0.0] - Original

### Added
- Original bash implementation (`grambo`)
- Basic Galera log parsing
- State transition tracking
- SST event detection
- Text-based output

---

## Version Naming Convention

- **v1.x**: Original bash implementation
- **v2.x**: Python rewrite with entity architecture (deprecated)
- **v3.x**: Next-generation tools with flow control, enhanced UI, and comprehensive analysis

## Links

- **Repository**: https://github.com/claudionanni/grambo
- **Issues**: https://github.com/claudionanni/grambo/issues
- **Releases**: https://github.com/claudionanni/grambo/releases

[Unreleased]: https://github.com/claudionanni/grambo/compare/v3.0.0-alpha.2...HEAD
[3.0.0-alpha.2]: https://github.com/claudionanni/grambo/releases/tag/v3.0.0-alpha.2
[3.0.0-alpha.1]: https://github.com/claudionanni/grambo/releases/tag/v3.0.0-alpha.1
[2.0.0]: https://github.com/claudionanni/grambo/tree/v2-rewrite
[1.0.0]: https://github.com/claudionanni/grambo/tree/master
