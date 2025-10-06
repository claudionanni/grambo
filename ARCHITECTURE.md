# Grambo Architecture & Concepts

## Overview

Grambo is an advanced analysis platform for Galera Cluster logs that transforms unstructured log data into a temporal, entity-based understanding of cluster behavior. It reconstructs the complete lifecycle of cluster operations, making complex distributed system interactions visible and comprehensible.

## Core Philosophy

### From Logs to Understanding

Traditional log analysis treats logs as independent lines of text. Grambo takes a fundamentally different approach:

**Traditional Approach:**
```
Line 1: Node state changed
Line 2: SST started
Line 3: Cluster view updated
→ Disconnected observations
```

**Grambo Approach:**
```
Event → Entity → Relationship → Timeline → Understanding
SST Event → Joiner Node + Donor Node → State Transfer Session → Frame-by-Frame Evolution
→ Complete operational narrative
```

### Key Insights

1. **Cluster operations are stories, not logs**
   - SST operations span dozens of log lines across multiple nodes
   - State transitions form causal chains
   - Time relationships matter as much as the events themselves

2. **Context is everything**
   - A "SYNCED" state means different things at different cluster moments
   - Node behavior depends on cluster view, quorum status, and peer states
   - Individual events gain meaning through their temporal relationships

3. **Visibility enables diagnosis**
   - See what happened, when it happened, and on which nodes
   - Trace cause-and-effect across the distributed system
   - Identify patterns that indicate problems

## Technical Architecture

### Multi-Stage Pipeline

Grambo uses a sophisticated four-stage pipeline, each building on the previous:

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│ graa3   │ →  │ grap3   │ →  │ graf3   │ →  │ grav3   │
│ SST/IST │    │ Entities│    │ Frames  │    │  Web    │
│ Analysis│    │ Extract │    │ Timeline│    │  View   │
└─────────┘    └─────────┘    └─────────┘    └─────────┘

Orchestrated by grax3 for one-command execution
```

#### Stage 1: graa3 - Domain-Specific Analysis
**Purpose:** Extract high-level operational insights

Focuses on complete operational sessions:
- **SST Sessions:** Identifies joiner/donor pairs, tracks transfer lifecycle
- **IST Sessions:** Identifies incremental state transfers
- **Session Relationships:** Links SST completion to subsequent IST operations

**Output:** Structured JSON with complete session metadata
- Start/end times
- Participant nodes
- Transfer methods (mariabackup, rsync, xtrabackup)
- Status (COMPLETED, FAILED, IN_PROGRESS)
- Event timeline within session

**Why Separate:** Some operational patterns require multi-line, multi-node correlation that benefits from dedicated analysis before entity extraction.

#### Stage 2: grap3 - Entity Extraction
**Purpose:** Parse logs into structured entities with rich metadata

Extracts discrete operational entities:
- **Nodes:** Cluster members with UUIDs, states, addresses
- **Clusters:** Cluster configurations and memberships
- **Views:** Cluster view changes and compositions
- **State Transitions:** Node state changes with causality
- **SST/IST Events:** State transfer events
- **Errors & Warnings:** Operational issues

**Key Innovation - Pattern Matching with Metadata:**

```yaml
pattern:
  regex: 'WSREP: Member (\S+) \(([^)]+)\) synced with group'
  entity_type: node
  properties:
    node_uuid: 1      # Capture group 1
    node_name: 2      # Capture group 2
    to_state: SYNCED  # Inferred property
  metadata:
    confidence: 0.95
    pattern_name: "member_synced"
    extraction_method: "regex"
```

Each pattern carries:
- **Confidence score:** How reliable is this extraction?
- **Pattern identifier:** Which pattern matched?
- **Extraction method:** Regex, heuristic, or correlation?
- **Validation status:** Has this been verified?

**Why Metadata Matters:**
- Enables quality assessment of extracted data
- Allows filtering by confidence in visualization
- Facilitates pattern debugging and refinement
- Provides audit trail for entity relationships

#### Stage 3: graf3 - Frame Generation
**Purpose:** Build temporal timeline from entities

Reconstructs cluster state evolution:
- **Frame concept:** A snapshot of cluster state at a specific moment
- **Event-driven:** Each significant event creates a new frame
- **State propagation:** Carries forward unchanged state
- **Multi-node synthesis:** Combines events from all nodes

**Frame Structure:**
```json
{
  "index": 42,
  "timestamp": "2025-09-25T17:22:20",
  "event": {
    "entity_type": "sst_event",
    "joiner_node": "NODE_31407",
    "donor_node": "NODE_11407"
  },
  "nodes": {
    "NODE_11407": {
      "node_state": "DONOR/DESYNCED",
      "cluster_ref": "cluster_uuid",
      "confidence": 0.95
    },
    "NODE_31407": {
      "node_state": "JOINER",
      "confidence": 0.95
    }
  },
  "clusters": {...},
  "views": {...},
  "quorum": {...}
}
```

**Timeline Construction:**
- Events become frames in chronological order
- Each frame captures complete cluster state at that moment
- State changes propagate through subsequent frames
- Enables "time travel" through cluster history

#### Stage 4: grav3 - Web Visualization
**Purpose:** Interactive exploration of cluster timeline

Provides multi-dimensional views:
- **Dual timeline navigation:** Frame-by-frame + natural time
- **Entity panels:** Nodes, clusters, views, quorum state
- **Raw log context:** See actual log lines around events
- **SST session tracking:** Visualize state transfer operations
- **Hotspot markers:** Quick navigation to issues

### Pattern Matching Architecture

#### Why Pattern-Based Extraction?

Galera logs are semi-structured:
```
2025-09-25 17:22:20 [Note] WSREP: State transfer required
2025-09-25 17:22:20 [Note] WSREP: Member 2.0 (NODE_31407) requested state transfer
```

Characteristics:
- Consistent message formats within versions
- Rich information embedded in log text
- No formal schema or API
- Version variations require flexible patterns

#### Pattern Structure

Patterns are defined in YAML for maintainability:

```yaml
patterns:
  - name: "sst_request"
    regex: 'Member (\d+\.\d+) \(([^)]+)\) requested state transfer from'
    entity_type: sst_event
    category: sst_request
    properties:
      member_index: 1
      joiner_node: 2
      event_category: "sst_request"
    metadata:
      confidence: 0.9
      galera_versions: ["4.x", "26.x"]
      critical: true
```

#### Metadata-Enriched Entities

Every extracted entity includes:

**Core Properties:**
- Entity type (node, cluster, view, sst_event)
- Entity ID (unique identifier)
- Timestamp (when it occurred)
- Source properties (extracted from log line)

**Metadata:**
- `confidence`: 0.0-1.0 reliability score
- `pattern_name`: Which pattern matched
- `extraction_method`: regex, heuristic, correlation
- `validated`: Boolean verification status
- `validation_notes`: Human review comments

**Benefits:**
1. **Quality tracking:** Know which data is reliable
2. **Pattern debugging:** Identify problematic patterns
3. **Incremental improvement:** Refine low-confidence patterns
4. **Research documentation:** Understand extraction decisions

## What Grambo Analyzes

### Cluster Lifecycle Events

**Node States:**
- `JOINING` → `JOINER` → `JOINED` → `SYNCED`
- `DONOR/DESYNCED` → `JOINED` → `SYNCED`
- State transitions reveal node health and cluster stability

**Cluster Views:**
- View changes indicate membership modifications
- Primary component vs non-primary states
- Quorum establishment and loss
- Network partition detection

**State Transfer Operations:**
- **SST (State Snapshot Transfer):** Full data synchronization
  - Joiner/donor node identification
  - Transfer method (mariabackup, rsync, xtrabackup)
  - Duration and completion status
  - Failure detection and retry logic
  
- **IST (Incremental State Transfer):** Catch-up synchronization
  - Write-set application
  - Sequence number ranges
  - Post-SST recovery

### Operational Patterns

**Healthy Patterns:**
- Smooth SST completion followed by IST
- All nodes in SYNCED state
- Primary component maintained
- No view changes

**Problem Indicators:**
- SST failures or timeouts
- Repeated state transitions
- Non-primary component states
- Frequent view changes (network instability)
- Nodes stuck in JOINER state

### Multi-Node Correlation

Grambo synthesizes information across nodes:

**Single-Node View (Traditional):**
```
Node1: SST completed
Node2: Became SYNCED
→ Disconnected events
```

**Multi-Node Synthesis (Grambo):**
```
Frame 310: Node2 (JOINER) ← Node1 (DONOR)
Frame 334: Node2 SST received, Node1 returned SYNCED
→ Complete operational picture
```

## What Grambo Displays

### Timeline Visualization

**Dual Timeline System:**
1. **Frame Navigation:** Step through cluster state changes
2. **Natural Timeline:** Events positioned at actual timestamps

**Event Markers:**
- 🟠 Orange: SST operations
- 🔴 Red: Cluster-level issues
- 🟡 Yellow: Node-level issues  
- 🟣 Purple: Component state issues

### Entity Panels

**Current State Display:**
- **Nodes:** States, roles, UUIDs, addresses
- **Clusters:** Configuration, UUID, member count
- **Views:** Current membership composition
- **Quorum:** Status and primary component indicator
- **SST Sessions:** Active transfers with progress

### Contextual Information

**Raw Log Access:**
- ~121 lines of log context around each event
- Target line highlighted
- Smart centering (10 lines before, 110 after)
- Direct visibility into actual log messages

**Session Details:**
- SST session summaries
- Start/end timestamps
- Duration calculations
- Status indicators

### Interactive Navigation

**Exploration Features:**
- Click timeline markers to jump to events
- Navigate frame-by-frame with timeline slider
- View state evolution over time
- Correlate events across nodes

## Key Technical Decisions

### Why Entity-Based?

**Alternative:** Line-by-line log viewing
**Problem:** Context lost, relationships unclear

**Entity-Based Approach:**
- Preserves semantic meaning
- Enables relationship tracking
- Supports temporal reasoning
- Facilitates pattern recognition

### Why Metadata-Enriched?

**Alternative:** Extract data without quality indicators
**Problem:** Can't assess reliability, difficult to improve

**Metadata Benefits:**
- Quality assessment built-in
- Pattern refinement enabled
- Research documentation preserved
- Gradual improvement path

### Why Frame-Based Timeline?

**Alternative:** Event stream or aggregated views
**Problem:** Hard to see state evolution or causality

**Frame-Based Benefits:**
- Clear temporal progression
- State at any moment visible
- Cause-effect relationships clear
- Time travel through cluster history

### Why Multi-Stage Pipeline?

**Alternative:** Single monolithic analyzer
**Problem:** Complex, difficult to maintain, hard to extend

**Pipeline Benefits:**
- Separation of concerns
- Each stage testable independently
- Easy to add new analyses
- Progressive refinement of understanding

## Current Capabilities

### Analysis Coverage

✅ **Fully Supported:**
- SST session detection and tracking
- Node state transitions
- Cluster view changes
- Primary component status
- Multi-node timeline synthesis
- Timestamp-based navigation

🔨 **In Development:**
- IST session correlation with SST
- Performance metrics extraction
- Network partition detection
- Split-brain scenario identification
- Capacity planning indicators

### Visualization Features

✅ **Current:**
- Dual timeline navigation
- Entity state panels
- Raw log context viewer
- SST operation markers
- Interactive frame exploration

🔨 **Planned:**
- Network topology visualization
- State transition graphs
- Performance charts
- Comparative analysis across time periods
- Export and reporting

## Future Directions

### Enhanced Analysis

- **Predictive indicators:** Detect patterns leading to failures
- **Performance profiling:** Identify bottlenecks and slow operations
- **Capacity planning:** Growth trend analysis
- **Automatic root cause:** AI-assisted diagnosis

### Extended Coverage

- **More entity types:** Replication events, conflicts, deadlocks
- **Deeper correlation:** Cross-reference with system metrics
- **Multi-cluster:** Analyze multiple clusters simultaneously
- **Historical comparison:** Compare behavior across time periods

### Improved Visualization

- **3D topology:** Spatial representation of cluster
- **Animation:** Replay cluster evolution
- **Comparative views:** Side-by-side timeline comparison
- **Custom dashboards:** User-configurable layouts

## Conclusion

Grambo transforms Galera log analysis from a tedious line-by-line investigation into an intuitive exploration of cluster behavior. By extracting entities, preserving metadata, building timelines, and providing rich visualization, it makes distributed system operations comprehensible.

The architecture's key strengths—entity-based modeling, metadata enrichment, temporal reasoning, and progressive refinement—enable both immediate troubleshooting and long-term pattern discovery. As the platform evolves, these foundations support increasingly sophisticated analysis while maintaining clarity and usability.

**Core Insight:** Understanding distributed systems requires seeing them as living, evolving organisms. Grambo provides the microscope to observe that evolution at every level—from individual log lines to complete operational narratives.
