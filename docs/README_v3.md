# Grambo v3 - Galera Cluster Log Analysis Suite

A comprehensive suite of tools for analyzing MySQL/MariaDB Galera cluster log files, featuring a complete **4-tool pipeline** for entity extraction, frame generation, and interactive web visualization with SST analysis.

## 🚀 Quick Start - GRAX3 (Recommended)

Use the new `grax3` helper to run the complete analysis pipeline in a single command:

```bash
# Run graa3 → grap3 → graf3 → grav3 in sequence
./grax3 /path/to/galera/error.log

# Skip launching the web UI (artifacts only)
./grax3 /path/to/galera/error.log --no-serve

# Customize output directory and port
./grax3 /path/to/galera/error.log --output-dir=out --host=0.0.0.0 --port=5050
```

**GRAX3** automatically uses the repository's `.venv` interpreter when available, ensuring Flask and other Python dependencies are loaded correctly. You can override this with `--python=/path/to/python` if needed.

###Output Artifacts (`grax_output/` by default):
- `graa3_sst.txt` – hierarchical SST/IST report displayed in the SST viewer
- `grap_output.json` – structured entity stream  
- `graf_frames.ndjson` – timeline frames loaded by `grav3`
- `sst_sessions.json` – structured SST session data

The web UI includes a **"View SST Sessions"** link when the SST report is available, opening a styled page with download option.

## 🔧 Manual Pipeline (Advanced Users)

If you need fine-grained control over each step:

```bash
# 1. Build the SST/IST summary (optional but recommended)
.venv/bin/python3 graa3 --sst node.log > graa3_sst.txt

# 2. Extract entities
.venv/bin/python3 grap3 --no-cache --format=json node.log > grap_output.json

# 3. Generate timeline frames
.venv/bin/python3 graf3 grap_output.json --ndjson -o graf_frames.ndjson

# 4. Launch the visualizer
.venv/bin/python3 grav3 --frames=graf_frames.ndjson --sst-report=graa3_sst.txt
```

## Tool Overview

| Tool | Purpose | Typical Output |
| --- | --- | --- |
| `graa3` | Summarise logs and build SST/IST tree (`--sst`) | Plain-text report |
| `grap3` | Parse logs into structured entities | `grap_output.json` |
| `graf3` | Turn entities into timeline frames | `graf_frames.ndjson` |
| `grav3` | Flask viewer for the frames + SST page | Web UI on localhost |
| `grax3` | Runs the full chain and launches `grav3` | Artifacts under `grax_output/` |

## 🔧 GRAA3 - SST/IST Analysis Tool

**GRAA3** (Galera Real-time Analysis Analyzer) provides hierarchical SST and IST relationship analysis:

```bash
# Generate SST/IST hierarchical report
./graa3 --sst 11407/error.11407.log

# Basic log summary
./graa3 11407/error.11407.log
```

## 🔧 GRAP3 - Enhanced Entity Extraction

**GRAP3** (Galera Real-time Analysis Parser) extracts structured entities from Galera logs:

### Features:
- **🎯 Entity-Based Architecture** - Extracts structured entities (SST, IST, VIEW, NODE, ERROR, FLOW_CONTROL)
- **🔄 IST Workflow Tracking** - Complete Incremental State Transfer lifecycle monitoring
- **📊 Multi-Format Output** - Text, JSON, and YAML output formats
- **💾 Intelligent Caching** - Cache analysis results for faster re-processing
- **🌐 Multi-Node Analysis** - Analyze multiple log files with cluster correlation
- **⚡ Advanced Filtering** - Filter by entity types and confidence thresholds

### Usage:
```bash
# Basic single node analysis
./grap3 galera-node.log

# Multi-node cluster analysis
./grap3 node1.log node2.log --multi

# JSON output with entity filtering
./grap3 --format=json --entities=SST,IST galera-node.log

# Force fresh parse (no cache)
./grap3 --no-cache --format=json galera-node.log
```

### Key Options:
- `--no-cache` — force a fresh parse when logs change.
- `--format=json` — required for the `graf3` pipeline.
- `--filter` — optional comma-separated entity types.

## 🔧 GRAF3 - Timeline Frame Generator

**GRAF3** converts structured entities into timeline frames for visualization:

```bash
# Generate timeline frames from entity data
./graf3 grap_output.json --ndjson -o graf_frames.ndjson
```

## 🔧 GRAV3 - Interactive Web Visualizer

**GRAV3** provides a Flask-based web interface for timeline visualization:

```bash
# Launch web visualizer with timeline and SST report
./grav3 --frames=graf_frames.ndjson --sst-report=graa3_sst.txt

# Custom host and port
./grav3 --frames=graf_frames.ndjson --host=0.0.0.0 --port=5050
```

### Features:
- **Frame Timeline Navigation** - Navigate through cluster state frames
- **Natural Timeline Bar** - Vertical markers showing actual time gaps between state changes
- **Node State Visualization** - Color-coded node states (SYNCED, DONOR, JOINER, etc.)
- **Cluster Details** - Current cluster configuration, view changes, SST operations
- **SST Sessions** - Detailed SST session information with drag-to-reposition timeline
- **Flow Control Monitoring** - Track flow control intervals across the cluster
- **Interactive Cards** - Click cards for detailed information
- **Documentation Links** - Each card has contextual help explaining the metrics

### Understanding the Timeline:
- **Frame Timeline**: Each frame represents a new state where at least one property of the cluster changed. Frames are sequential but may have time gaps between them.
- **Natural Timeline**: Shows the actual time progression with vertical markers indicating when state changes occurred. Click markers to jump to specific times.

## Understanding Galera Events

### State Transitions
Galera nodes go through various states:
- **JOINING** → **JOINED** → **SYNCED** → **DONOR** (normal flow)
- **SYNCED** is the healthy operational state
- **DONOR** means the node is providing SST/IST to other nodes

### SST vs IST
- **IST (Incremental State Transfer)**: First attempt when a node needs to resync; donor serves missing write sets from gcache. If gcache doesn't contain the full required range or IST isn't possible, it falls back to SST.
- **SST (State Snapshot Transfer)**: Full resync via wsrep_sst_mariabackup (default). Donor runs mariabackup and streams to the joiner on port 4568; the joiner wipes datadir, restores and prepares the backup, then starts MariaDB. After SST, a short IST catch-up typically follows.

**Important Note**: SST resets the joiner's error log, so some local node information is lost during the SST process.

### Cluster Views
Track which nodes are members of the cluster at any given time, including:
- Nodes that joined the cluster
- Nodes that left gracefully  
- Nodes that were partitioned (network split)

### Flow Control
Flow control is Galera's mechanism to prevent fast nodes from overwhelming slower nodes:
- **Intervals**: `[lower, upper]` bounds indicating the acceptable range of uncommitted write-sets
- **FC_STOP**: Sent when a node needs to pause incoming write-sets
- **FC_CONT**: Sent when a node is ready to receive write-sets again
- **Impact**: When active, cluster write performance is throttled to match the slowest node

When flow control is frequently active, it indicates performance bottlenecks in the cluster that need investigation.

## Requirements

### Core Analysis Tools
- Python 3.7 or higher
- Flask for web visualization

```bash
# Install web dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install flask
```

## Legacy Compatibility

The original bash grambo is still available as `grambo` (without .py extension). The v2 Python tools (`graa`, `grap`, `graf`, `grav`) are deprecated in favor of the v3 tools (`graa3`, `grap3`, `graf3`, `grav3`) which provide enhanced analysis with entity extraction, timeline visualization, flow control monitoring, and SST relationship tracking.

## Installation

```bash
# Clone the repository
git clone https://github.com/claudionanni/grambo.git
cd grambo

# The tools are ready to use
chmod +x graa3 grap3 graf3 grav3 grax3

# Install dependencies for web visualization
python3 -m venv .venv
source .venv/bin/activate
pip install flask
```

## License

Same license as the original grambo project.
