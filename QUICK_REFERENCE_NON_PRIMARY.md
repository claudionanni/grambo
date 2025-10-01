# NON-PRIMARY Markers - Quick Reference

## What Was Added

### 1. Timeline Purple Markers (Track 4)
**What**: Purple markers appear on timeline when nodes see NON-PRIMARY views  
**When**: During split-brain, network partitions, or cluster issues  
**How to use**: Hover to see node name and state details

### 2. View Card Visual Indicators
**What**: Red border (3px) + warning badge "⚠ NON-PRIMARY"  
**Where**: Wsrep Layer View card  
**What it means**: Node cannot reach quorum and is in non-primary state

### 3. Quorum Panel
**What**: Shows quorum loss/gain events with timestamps  
**Fields**: event_type, quorum_status, severity, event_text  
**What it means**: Tracks when cluster loses/regains quorum

## Quick Start

```bash
# Generate frames with NON-PRIMARY detection
./grap3 cl407/error.*.log --format=json | ./graf --ndjson > frames.ndjson

# Start visualization
./grav --frames=frames.ndjson --host=127.0.0.1 --port=5002 --logs cl407/error.*.log

# Open browser
http://127.0.0.1:5002
```

## What to Look For

### Healthy Cluster
- **Timeline**: Mostly SST markers (orange)
- **View Cards**: Gray borders, "primary" status
- **Quorum Panel**: Empty or shows "quorum_status: true"

### Problem Cluster
- **Timeline**: Purple markers (component NON-PRIMARY)
- **View Cards**: Red borders with ⚠ NON-PRIMARY badges
- **Quorum Panel**: Shows QUORUM_LOST events with CRITICAL severity

## Timeline Legend

```
Track 1: 🟧 SST Operations (orange)
Track 2: 🟥 Cluster NON-PRIMARY (red)
Track 3: 🟧 Node Issues (orange)
Track 4: 🟪 Component NON-PRIMARY (purple) ← NEW!
```

## Troubleshooting

### No purple markers visible
- Check if logs contain NON-PRIMARY events: `grep -i "non-primary" *.log`
- Verify grap3 extracts wsrep_view entities: `./grap3 *.log --format=json | jq '.entities[] | select(.entity_type=="wsrep_view")' | head`

### Quorum panel empty
- Check quorum entities: `./grap3 *.log --format=json | jq '.entities[] | select(.entity_type=="quorum")'`
- Verify frames have quorum: `cat frames.ndjson | jq 'select(.quorum != null)' | head`

### View cards not showing NON-PRIMARY
- Check node_views in frame: `cat frames.ndjson | jq '.node_views' | head`
- Verify status field: `cat frames.ndjson | jq '.node_views[] | select(.status)' | head`

## Color Reference

| Element | Normal | Problem |
|---------|--------|---------|
| View Border | Gray 2px (#444) | Red 3px (#e53935) |
| Timeline Marker | N/A | Purple (#8e24aa) |
| Quorum Status | true (green) | false (red) |

## Data Flow

```
Galera Logs
    ↓
grap3 (extracts wsrep_view + quorum entities)
    ↓
graf (builds frames + detects NON-PRIMARY)
    ↓
grav (visualizes with markers + badges)
```

## Testing Commands

```bash
# Count NON-PRIMARY frames
cat frames.ndjson | jq -s '[.[] | select(.hotspots.node_component_non_primary)] | length'

# List NON-PRIMARY events
cat frames.ndjson | jq -s '.[] | select(.hotspots.node_component_non_primary) | {timestamp: .timestamp, nodes: .hotspots.node_component_non_primary}'

# Check quorum coverage
cat frames.ndjson | jq -s '[.[] | select(.quorum != null)] | length'
```

## Related Documentation

- `NON_PRIMARY_MARKERS_AND_QUORUM.md` - Detailed technical documentation
- `VISUAL_SUMMARY.txt` - ASCII art visual reference
- `V3_IMPROVEMENTS_SUMMARY.md` - Complete v3-alpha feature list
