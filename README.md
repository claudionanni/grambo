# Grambo v3 Alpha - Galera Cluster Log Analysis Suite

**⚠️ Alpha Version**: This is a pre-release version under active development. For production use with the stable bash version, please checkout the [master branch](https://github.com/claudionanni/grambo/tree/master).

> Transform unstructured Galera logs into comprehensive, interactive cluster analysis with frame-by-frame timeline visualization.

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](CHANGELOG.md)

## What's New in v3

- 🎯 **Complete Tool Rewrite**: `graa3`, `grap3`, `graf3`, `grav3` with enhanced capabilities
- 🔄 **Flow Control Monitoring**: Track and visualize Galera flow control intervals
- 📊 **Natural Timeline**: See actual time gaps between cluster state changes
- 🎨 **Enhanced UI**: Improved visualization with contextual documentation
- 🚀 **One-Command Pipeline**: `grax3` orchestrates the entire analysis workflow
- 📚 **Inline Documentation**: Each card has help links explaining the metrics

## Quick Start

```bash
# Clone the repository
git clone -b v3-alpha https://github.com/claudionanni/grambo.git
cd grambo

# Install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install flask

# Run complete analysis pipeline
./grax3 /path/to/galera/error.log

# Browser opens automatically to http://localhost:5000
```

That's it! The `grax3` command runs the complete pipeline:
1. Extracts SST/IST sessions (`graa3`)
2. Parses entities from logs (`grap3`)
3. Generates timeline frames (`graf3`)
4. Launches interactive visualizer (`grav3`)

## What Grambo Does

Grambo transforms this:
```
2025-09-23 17:56:51 WSREP: Member 0.0 (node-1) requested state transfer
2025-09-23 17:56:51 WSREP: Prepared SST request: mariabackup|10.0.0.1:4568
2025-09-23 17:57:07 WSREP: SST complete, seqno: 17
2025-09-23 17:57:12 WSREP: Receiving IST: 11 writesets, seqnos 7-17
2025-09-23 17:57:12 WSREP: IST received: seqno 17
```

Into this:

```
🔍 Cluster Analysis Summary
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 SST Session #1
   Time: 2025-09-23 17:56:51 → 17:57:07 (16 seconds)
   Joiner: node-1 (10.0.0.1)
   Method: mariabackup
   Status: ✅ COMPLETED
   
   Related IST:
   ├─ Prepared: 11 writesets (seqno 7→17)
   ├─ Received: 100% complete
   └─ Duration: 5 seconds

🎯 Cluster State: HEALTHY
   ├─ Nodes: 3 synced
   ├─ SST Operations: 1 successful
   └─ Flow Control: Inactive
```

Plus an interactive web UI with:
- Frame-by-frame timeline navigation
- Natural timeline showing actual time gaps
- Node state visualization
- Flow control tracking
- SST session details
- Cluster health metrics

## Tools Overview

| Tool | Purpose | Output |
|------|---------|--------|
| `grax3` | **One-command pipeline** | Runs all tools + launches UI |
| `graa3` | SST/IST session analysis | `graa3_sst.txt` |
| `grap3` | Entity extraction | `grap_output.json` |
| `graf3` | Frame generation | `graf_frames.ndjson` |
| `grav3` | Web visualization | Interactive UI |

### Individual Tool Usage

```bash
# SST/IST hierarchical analysis
./graa3 --sst /path/to/error.log > graa3_sst.txt

# Extract entities from logs
./grap3 --format=json --no-cache /path/to/error.log > grap_output.json

# Generate timeline frames
./graf3 grap_output.json --ndjson -o graf_frames.ndjson

# Launch visualizer
./grav3 --frames=graf_frames.ndjson --sst-report=graa3_sst.txt
```

### Multi-Node Analysis

```bash
# Analyze multiple nodes together
./grax3 node1/error.log node2/error.log node3/error.log
```

## Key Features

### 🎯 Comprehensive Entity Extraction
- **Cluster Views**: Track membership changes, quorum events
- **Node States**: JOINING → JOINED → SYNCED → DONOR lifecycle
- **SST Sessions**: Complete state snapshot transfer tracking
- **IST Operations**: Incremental state transfer monitoring
- **Flow Control**: Identify cluster throttling events
- **Errors & Warnings**: Categorized operational issues

### 📊 Interactive Visualization
- **Frame Timeline**: Navigate cluster state changes frame-by-frame
- **Natural Timeline**: See real-time gaps between events
- **Node Cards**: Current state, UUID, address for each node
- **Cluster Details**: View changes, SST operations, group info
- **SST Sessions**: Detailed transfer information with drag timeline
- **Flow Control**: Track when and why cluster throttling occurred

### 🔍 Advanced Analysis
- **Group Assignment**: SST operations correctly assigned to cluster groups
- **Temporal Correlation**: Events linked across multiple nodes
- **Session Relationships**: SST completion triggers IST operations
- **Performance Metrics**: Duration tracking, success rates

### 📚 Built-in Documentation
- **Inline Help**: Click (?) icons in cards for explanations
- **Theory Integration**: Flow control mechanics from source code
- **Contextual Warnings**: SST resets joiner logs, etc.
- **Navigation Guide**: Frame vs. natural timeline explanation

## Architecture

Grambo uses a sophisticated multi-stage pipeline:

```
┌─────────────────────────────────────────────────────────────┐
│                                                               │
│  grax3: One-Command Orchestrator                              │
│  ├─ Detects .venv for Flask dependencies                     │
│  ├─ Runs pipeline in sequence                                │
│  └─ Launches UI with correct artifact paths                  │
│                                                               │
└────────────┬──────────────────────────────────────────────────┘
             │
             ├─► graa3: Domain-Specific Analysis
             │   ├─ SST session identification (joiner/donor pairs)
             │   ├─ IST relationship tracking
             │   └─ Output: graa3_sst.txt
             │
             ├─► grap3: Entity Extraction
             │   ├─ Pattern-based log parsing
             │   ├─ Entity creation (nodes, views, SST, IST, flow control)
             │   └─ Output: grap_output.json
             │
             ├─► graf3: Frame Generation
             │   ├─ Temporal entity correlation
             │   ├─ Group lifetime calculation
             │   ├─ Frame-by-frame state reconstruction
             │   └─ Output: graf_frames.ndjson
             │
             └─► grav3: Web Visualization
                 ├─ Flask-based interactive UI
                 ├─ Timeline navigation
                 ├─ Flow control markers
                 └─ SST session viewer
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design philosophy.

## Understanding Galera Concepts

### State Transitions
- **JOINING** → **JOINED** → **SYNCED**: Normal node startup
- **SYNCED** → **DONOR** → **SYNCED**: Providing SST to another node
- **DESYNCED**: Temporary state during heavy write load

### SST (State Snapshot Transfer)
Full database backup transfer from donor to joiner:
- Triggered when node is too far behind for IST
- Uses `mariabackup` (default), `rsync`, or `xtrabackup`
- **Important**: Resets joiner's error log (local history lost)
- Typically followed by short IST catch-up

### IST (Incremental State Transfer)
Incremental catch-up using gcache:
- First attempt when node needs synchronization
- Faster than SST, no datadir wipe
- Falls back to SST if gcache insufficient

### Flow Control
Galera's mechanism to prevent fast nodes overwhelming slow nodes:
- `FC_STOP`: Node requests pause in write-sets
- `FC_CONT`: Node ready to receive again
- **Impact**: Cluster writes throttled to slowest node
- **Intervals**: `[lower, upper]` bounds for uncommitted write-sets

When flow control is frequently active, investigate:
- Slow disk I/O on affected node
- Network bandwidth constraints
- Query performance issues
- `wsrep_slave_threads` configuration

## Requirements

- **Python**: 3.7 or higher
- **Flask**: For web visualization
- **OS**: Linux/Unix (bash scripts)
- **Logs**: MariaDB/MySQL Galera error logs

## Installation

```bash
# Clone repository
git clone -b v3-alpha https://github.com/claudionanni/grambo.git
cd grambo

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install flask

# Make tools executable
chmod +x graa3 grap3 graf3 grav3 grax3

# Test installation
./grax3 --help
```

## Example Workflow

```bash
# 1. Collect Galera logs from your cluster nodes
scp db1:/var/log/mysql/error.log node1.log
scp db2:/var/log/mysql/error.log node2.log
scp db3:/var/log/mysql/error.log node3.log

# 2. Run complete analysis
./grax3 node1.log node2.log node3.log

# 3. Explore in browser (auto-opens)
# - Navigate frames with timeline slider
# - Click natural timeline markers to jump to events
# - View SST sessions details
# - Check flow control activity
# - Examine node states and cluster health

# 4. Artifacts saved to grax_output/
ls -la grax_output/
# graa3_sst.txt - SST session report
# grap_output.json - Extracted entities
# graf_frames.ndjson - Timeline frames
# sst_sessions.json - Structured SST data
```

## Documentation

- **[CHANGELOG.md](CHANGELOG.md)**: Version history and changes
- **[ARCHITECTURE.md](ARCHITECTURE.md)**: Design philosophy and technical architecture
- **[RELEASE_STRATEGY.md](RELEASE_STRATEGY.md)**: Alpha/beta release approach
- **[docs/README_v3.md](docs/README_v3.md)**: Detailed v3 documentation
- **[docs/README_grap.md](docs/README_grap.md)**: Entity extraction details
- **[docs/README_graa.md](docs/README_graa.md)**: SST/IST analysis details

## Troubleshooting

### UI Not Loading
```bash
# Check Flask is installed
python3 -c "import flask; print(flask.__version__)"

# If missing:
pip install flask
```

### Port Already in Use
```bash
# Use different port
./grax3 /path/to/log --port=5050
```

### Empty Flow Control Card
```bash
# Flow control only shows when present in logs
# Check if logs contain WSREP flow control messages:
grep -i "flow-control" /path/to/error.log
```

### SST Sessions Link Not Working
```bash
# Ensure grax3 generated all files:
ls -la grax_output/
# Should contain: graa3_sst.txt, grap_output.json, graf_frames.ndjson, sst_sessions.json
```

## Known Issues (Alpha)

- Flow control health status needs refinement based on cluster impact analysis
- Some edge cases in multi-node SST tracking under heavy load
- Documentation viewer in UI (coming soon)

See [GitHub Issues](https://github.com/claudionanni/grambo/issues) for current bug reports.

## Feedback & Contributing

This is an **alpha release** — your feedback is invaluable!

### Report Issues
- [GitHub Issues](https://github.com/claudionanni/grambo/issues)
- Tag with `v3-alpha` label
- Include log samples if possible (sanitize sensitive data)

### Feature Requests
- [GitHub Discussions](https://github.com/claudionanni/grambo/discussions)
- Describe use case and expected behavior

### Contributing
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Test with sample logs
5. Submit a pull request

## Roadmap

### Alpha Phase (Current)
- ✅ Core v3 pipeline (graa3, grap3, graf3, grav3)
- ✅ Flow control monitoring
- ✅ Enhanced UI with documentation
- 🔄 Flow control health refinement
- 🔄 Additional temporal entities

### Beta Phase (Planned)
- Performance optimization for large logs
- Advanced multi-node correlation
- Cluster health scoring
- Export reports (PDF, HTML)
- Real-time log streaming

### GA Release (Planned)
- Production-ready stability
- Comprehensive test coverage
- Full documentation
- Migration guides from v1/v2

## License

Same license as the original grambo project.

## Credits

- **Original Grambo**: Bash-based Galera log analyzer
- **v3 Rewrite**: Entity-based architecture with advanced visualization
- **Flow Control Analysis**: Based on Galera source code study

## Support

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/claudionanni/grambo/issues)
- **Discussions**: [GitHub Discussions](https://github.com/claudionanni/grambo/discussions)

---

**Note**: For production environments, consider using the stable bash version from the [master branch](https://github.com/claudionanni/grambo/tree/master) until v3 reaches general availability.
