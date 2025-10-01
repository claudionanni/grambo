# NON-PRIMARY Markers and Quorum Panel Improvements

## Summary
Added visual indicators for NON-PRIMARY cluster states and improved quorum panel display in the grav visualization tool.

## Changes Made

### 1. Graf Improvements (Frame Builder)

#### NON-PRIMARY Detection from Wsrep Views
- **File**: `graf`
- **Change**: Added detection of NON-PRIMARY status from node-level `wsrep_view` entities
- **Location**: Lines 821-851 (hotspot detection section)
- **Logic**:
  - Checks each `frame_node_views` entry for NON-PRIMARY status
  - Examines `status`, `cluster_state`, or `view_status` fields
  - Creates hotspot entries with `layer: "wsrep_view"` for timeline markers
  - Maintains backward compatibility with gcomm views

#### Quorum Lookup Improvements
- **File**: `graf`
- **Change**: Enhanced quorum entity lookup to handle entities without cluster_ref
- **Location**: Lines 749-773 (quorum section building)
- **Logic**:
  - Tries to match quorum to cluster by `group_uuid`, `cluster_uuid`, or `cluster_ref`
  - Falls back to `_default` key when no cluster match is found
  - Ensures quorum entities are properly timestamp-filtered for each frame
  - Only includes quorum events at or before the frame's timestamp

### 2. Grav UI Improvements (Web Visualization)

#### View Card NON-PRIMARY Indicators
- **File**: `templates/index.html`
- **Change**: Added visual indicators for NON-PRIMARY views
- **Location**: Lines 1706-1730 (view rendering section)
- **Features**:
  - **Red Border**: 3px solid red border for NON-PRIMARY views (vs. 2px gray for normal)
  - **Badge**: "⚠ NON-PRIMARY" badge with red background next to node name
  - **Detection**: Checks `status`, `cluster_state`, or `view_status` fields for NON keywords

#### Quorum Panel Field Updates
- **File**: `templates/index.html`
- **Change**: Updated quorum fields to show relevant event information
- **Location**: Lines 1737-1753 (quorum card rendering)
- **Fields Displayed**:
  - `timestamp`: When the quorum event occurred
  - `event_type`: Type of event (e.g., QUORUM_LOST)
  - `event_text`: Human-readable description
  - `quorum_status`: Boolean indicating if quorum is present
  - `severity`: Event severity level (CRITICAL, etc.)
- **Before**: Showed technical fields (component, conf_id, protocols) that weren't populated
- **After**: Shows actual quorum event information with timestamps

### 3. Timeline Marker Support

#### Hotspot Categories
The timeline now displays markers for different types of issues:
- **Track 1 (Top)**: SST operations (orange) - clickable
- **Track 2**: Cluster NON-PRIMARY states (red)
- **Track 3**: Node non-member issues (orange)
- **Track 4 (Bottom)**: Component NON-PRIMARY states (purple) - **NEW: includes wsrep_view NON-PRIMARY**

#### Marker Colors and Styles
- **SST**: `#ff5722` (bright orange), 8px × 14px
- **Cluster**: `#e53935` (red), 6px × 12px
- **Node**: `#fb8c00` (orange), 6px × 12px
- **Component**: `#8e24aa` (purple), 6px × 12px - **Enhanced with wsrep_view detection**

## Data Flow

```
GRAP3 Output
├─ wsrep_view entities
│  ├─ status: "NON-PRIMARY" or "primary"
│  ├─ cluster_state: "NON_PRIM" or "PRIMARY"  
│  └─ view_status: "NON_PRIM" or "PRIM"
│
└─ quorum entities
   ├─ event_type: "QUORUM_LOST", etc.
   ├─ quorum_status: true/false
   └─ severity: "CRITICAL", etc.

         ↓

GRAF Processing
├─ Detects NON-PRIMARY in node_views
│  └─ Creates hotspot: node_component_non_primary[]
│
├─ Looks up quorum by cluster/group UUID
│  └─ Falls back to _default key
│
└─ Outputs frames with:
   ├─ node_views: { status, cluster_state, ... }
   ├─ quorum: { event_type, quorum_status, ... }
   └─ hotspots: { node_component_non_primary: [...] }

         ↓

GRAV Visualization
├─ Timeline: Purple markers for NON-PRIMARY components
├─ View Card: Red border + badge for NON-PRIMARY nodes
└─ Quorum Panel: Event details with timestamps
```

## Testing

### Test Command
```bash
./grap3 cl407/error.*.log --format=json | ./graf --ndjson > frames.ndjson
```

### Expected Results
1. **Frames with NON-PRIMARY hotspots**: Check for `hotspots.node_component_non_primary[]` entries
2. **Quorum data**: 900+ frames should have `quorum` section populated
3. **Node views**: 910+ frames should have `node_views` populated

### Verification
```bash
# Count frames with quorum
cat frames.ndjson | jq -s 'map(select(.quorum != null)) | length'
# Expected: ~904

# Count frames with node_views
cat frames.ndjson | jq -s 'map(select(.node_views != null and (.node_views | length) > 0)) | length'
# Expected: ~914

# Check NON-PRIMARY hotspots
cat frames.ndjson | jq -s 'map(select(.hotspots.node_component_non_primary != null)) | length'
# Expected: Multiple frames with NON-PRIMARY detections
```

## Example Output

### Hotspot Entry
```json
{
  "hotspots": {
    "node_component_non_primary": [
      {
        "node": "NODE_11407",
        "component_state": "NON_PRIM",
        "view_id": "d9c70dcb-97e3-11f0-b2ad-4f637476a656:2",
        "view_seq": 2,
        "layer": "wsrep_view"
      }
    ]
  }
}
```

### Quorum Entry
```json
{
  "quorum": {
    "timestamp": "2025-09-22 20:46:30",
    "event_text": "Non-primary view",
    "event_type": "QUORUM_LOST",
    "quorum_status": false,
    "severity": "CRITICAL"
  }
}
```

## UI Screenshots

### View Card with NON-PRIMARY Indicator
- Red left border (3px solid #e53935)
- Red badge: "⚠ NON-PRIMARY" next to node name
- Status field shows "NON-PRIMARY"

### Quorum Panel
- Shows timestamp of quorum events
- Displays event type (QUORUM_LOST)
- Shows quorum status (true/false)
- Displays severity level

### Timeline Markers
- Purple markers on Track 4 for component NON-PRIMARY states
- Hoverable with tooltip showing node and state information

## Future Enhancements

1. **Clickable NON-PRIMARY markers**: Make component markers clickable like SST markers
2. **Quorum timeline track**: Add dedicated track for quorum events
3. **View transition animations**: Highlight view changes in timeline
4. **NON-PRIMARY duration**: Calculate and display how long cluster stayed in NON-PRIMARY state

## Related Files
- `graf`: Frame builder with hotspot detection
- `templates/index.html`: Web UI with view cards and timeline
- `grav`: Flask server serving the visualization
- `grap3`: Entity extractor producing wsrep_view and quorum entities
