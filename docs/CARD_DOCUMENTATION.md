# Card Documentation

This document explains each card in the Galera Cluster Timeline visualization.

## ⚠️ Important: SST Impact on Log Analysis

**SST (State Snapshot Transfer) resets the joiner node's error log**, which means:
- All local historical data on the joiner node is lost
- Timeline gaps may appear for nodes that received SST
- Some events may be missing from the analysis for joiner nodes
- Cross-node correlation becomes critical for complete analysis

This affects all cards displaying node-level information.

---

## Cluster Graph

**Purpose:** Visual representation of cluster topology and node states.

**Key Features:**
- Shows all nodes in the cluster and their current state
- Displays connections between nodes
- Highlights active SST operations with directional arrows (donor → joiner)
- Color-coded node states (SYNCED=green, JOINER=yellow, DONOR=blue, etc.)

**Important Notes:**
- Node positions are automatically arranged for clarity
- During SST, joiner node logs are reset (see warning above)
- Graph updates with each frame change

---

## SST Sessions

**Purpose:** Displays SST/IST sessions active at the current frame timestamp.

**Key Features:**
- Shows donor → joiner relationship for active transfers
- Duration, method, and timeline information
- Event progression within each session
- Status indicators (COMPLETED, ONGOING, FAILED)

**Important Notes:**
- **SST resets the joiner's error log** - historical data is lost on the joiner
- Only shows sessions active at the current frame time
- Click "View Full Report" for complete hierarchical analysis
- IST events are shown under related SST sessions

**Color Coding:**
- Green border: Completed successfully
- Orange border: Ongoing/In progress
- Red border: Failed transfer

---

## Cluster Details

**Purpose:** High-level cluster statistics and group information.

**Key Features:**
- Cluster UUID and bootstrap session information
- Total SST operations per cluster group
- Total view changes (unique cluster-level views)
- Error and warning counts
- Split-brain detection status
- Member count and list

**Important Notes:**
- Multiple cluster groups indicate cluster restarts/rebootstraps
- Each group represents a distinct cluster lifetime (first_seen → next_group or now)
- SST counts are time-based: assigned to the group active when SST occurred
- View changes count unique cluster-level views (not per-node duplicates)

**About Cluster Groups:**
- When a cluster is completely restarted, a new cluster UUID is generated
- Each UUID represents a distinct "cluster group" or bootstrap session
- Metrics are tracked per group for accurate historical analysis

---

## Wsrep Layer View

**Purpose:** Shows cluster view information from the wsrep protocol layer.

**Key Features:**
- Group UUID and view sequence numbers
- Primary/Non-Primary status
- Member list and counts
- Protocol version and capabilities
- Own index (observing node's position)

**Important Notes:**
- Wsrep views are node-level observations of cluster state
- Each node reports its own view (context: LOCAL)
- View changes indicate cluster membership or configuration changes
- Non-Primary state means loss of quorum

**View Status:**
- PRIMARY: Cluster has quorum, can process writes
- NON_PRIM: No quorum, read-only mode

---

## Quorum State

**Purpose:** Tracks cluster quorum status and component state over time.

**Key Features:**
- Current quorum status (True/False)
- Event type (QUORUM_LOST, QUORUM_REGAINED)
- Severity indicators
- Timestamp of last quorum change

**Important Notes:**
- Quorum loss triggers read-only mode
- All nodes must agree on cluster state for quorum
- Split-brain can cause multiple components to lose quorum
- Quorum regained requires proper cluster rejoining

**Critical Events:**
- QUORUM_LOST: Immediate attention required
- QUORUM_REGAINED: Cluster recovered, but verify all nodes

---

## Nodes

**Purpose:** Detailed per-node state information and lifecycle tracking.

**Key Features:**
- Node name and UUID
- Current state (SYNCED, JOINER, JOINED, DONOR, etc.)
- State transitions with timestamps
- Node-specific error and warning indicators
- UUID history (tracks node restarts)

**Important Notes:**
- **When a node receives SST, its error log is wiped** - prior events are lost
- Node UUIDs change on restart
- UUID history shows all UUIDs the node has had (physical node tracking)
- State transitions show lifecycle: CLOSED → JOINER → JOINED → SYNCED

**Canonical States:**
- CLOSED: Node offline or shutting down
- OPEN: Node starting up
- JOINER: Requesting state transfer
- JOINED: State transfer complete, catching up
- SYNCED: Fully synchronized with cluster
- DONOR: Providing state to another node

**State Transitions:**
- Normal join: CLOSED → OPEN → JOINER → JOINED → SYNCED
- SST donor: SYNCED → DONOR → SYNCED
- Node restart: Any state → CLOSED

---

## Flow Control

**Purpose:** Monitor cluster capacity and flow control mechanism status.

**Key Features:**
- Real-time cluster size inference from flow control intervals
- Visual health indicators (color-coded status)
- Flow control threshold display ([lower, upper])
- Active/Disabled status tracking
- Timeline markers showing cluster capacity over time

**Important Notes:**
- **Flow control only active when node is SYNCED** - disabled during SST/IST
- Flow control protects cluster from replication lag and memory exhaustion
- Interval changes indicate cluster membership changes (nodes joining/leaving)
- Each node reports its own flow control status independently

**Status Indicators:**
- 🔴 **CRITICAL (1 node)**: Cluster running on single node - zero redundancy
- 🟡 **DEGRADED (2 nodes)**: Reduced capacity - limited fault tolerance
- 🟢 **HEALTHY (3+ nodes)**: Full cluster capacity with proper redundancy
- ⚫ **DISABLED (0 nodes)**: Node catching up (SST/IST) - not yet synchronized

### Understanding Flow Control Intervals

The interval format is `[lower_limit, upper_limit]`, representing thresholds for the **slave queue** (writesets waiting to be applied):

**How It Works:**
```
Queue Depth:

   20 ────────────── FC_STOP ────── Node overwhelmed, signals others to pause
   16 ══════════════ upper_limit
                     Normal zone
   16 ══════════════ lower_limit
   10 ────────────── FC_CONT ─────── Node caught up, signals others to resume
    0
```

**Flow Control Actions:**
- **FC_STOP**: Sent when queue exceeds upper_limit → "I'm falling behind, please slow down"
- **FC_CONT**: Sent when queue drops below lower_limit → "I've caught up, you can send more"

### Inferring Cluster Size

Galera calculates flow control limits based on cluster size:

**Formula (default configuration):**
```
upper_limit = base_limit × √(cluster_size)
lower_limit = upper_limit × resume_factor

Where:
  base_limit = 16 (default, configurable via gcs.fc_limit)
  resume_factor = 1.0 (default, configurable via gcs.fc_factor)
```

**Common Intervals:**
- `[16, 16]` → 1 node (or fc_single_primary=yes)
- `[23, 23]` → 2 nodes (√2 × 16 ≈ 23)
- `[28, 28]` → 3 nodes (√3 × 16 ≈ 28)
- `[32, 32]` → 4 nodes (√4 × 16 = 32)

**Note:** When lower = upper (e.g., `[16, 16]`), there's minimal hysteresis, resulting in aggressive flow control.

### Why Flow Control Matters

**Purpose:**
1. **Prevents memory exhaustion** from unbounded queue growth
2. **Limits replication lag** to keep nodes synchronized
3. **Ensures cluster coherency** by pacing fast nodes
4. **Protects slow nodes** from being overwhelmed

**Cluster operates at the speed of the slowest node** - this is by design to maintain data consistency.

### Performance Implications

**Tight Intervals [16, 16]:**
- ✅ Quick detection of slow nodes
- ✅ Lower memory usage
- ✅ Tight control over lag
- ⚠️ May reduce throughput
- ⚠️ Sensitive to temporary slowdowns

**Looser Intervals [22, 28]:**
- ✅ Higher throughput potential
- ✅ Less sensitive to bursts
- ✅ Smoother operation (hysteresis)
- ⚠️ Larger potential lag
- ⚠️ More memory usage

### Timeline Visualization

Flow control markers on the timeline show cluster capacity over time:
- **Marker height** indicates severity (taller = more critical)
- **Marker color** shows health status
- **Marker position** shows when interval changed
- **Click marker** to jump to that frame and see details

**Pattern Recognition:**
- **Many red markers**: Cluster frequently at single-node capacity
- **Orange clusters**: Extended periods of reduced redundancy
- **Green sections**: Healthy operation with full cluster
- **Gray markers**: Nodes catching up (SST/IST in progress)

### Typical Scenarios

**Scenario 1: Cluster Bootstrap**
```
Timeline: 🔴────🟡────🟢────────────🟢
          [16,16] [23,23] [28,28]    [28,28]
Time:     10:00   10:05   10:10      onwards
Nodes:    1 node  2 nodes 3 nodes    stable

Story: Started with 1 node, others joined via SST over 10 minutes
```

**Scenario 2: Node Failure & Recovery**
```
Timeline: 🟢──────🟡──────🔴────🟡────🟢
          [28,28] [23,23] [16,16] [23,23] [28,28]
Event:    Normal  Node3   Node2   Node3   Full
                  crash   crash   rejoins cluster

Story: Sequential failures reduced to 1 node, then gradual recovery
```

**Scenario 3: SST in Progress**
```
Timeline: 🟢──⚫────🟡──────🟢
          [28,28] disabled [23,23] [28,28]
Event:    3 nodes Node3    2 nodes 3 nodes
                  SST      active  SST done

Story: Node3 performing SST (FC disabled), rejoins as SYNCED
```

### Troubleshooting with Flow Control

**Q: Why is my cluster slow?**
- Check timeline for red/orange markers at the time of slowness
- Reduced cluster capacity = reduced throughput
- Identify which node(s) were missing/catching up

**Q: High flow control paused time (`wsrep_flow_control_paused`)?**
- One or more nodes can't keep up with replication
- Check for slow disk I/O, CPU bottlenecks, or heavy queries
- Consider optimizing the slow node or increasing fc_limit

**Q: Frequent interval changes?**
- Indicates cluster membership instability
- Nodes frequently joining/leaving
- Investigate node health and network issues

### Configuration

Flow control behavior is tunable via `wsrep_provider_options`:

```ini
# Base limit for slave queue depth (default: 16)
wsrep_provider_options="gcs.fc_limit=16"

# Resume factor for lower_limit (default: 1.0, range: 0.0-1.0)
wsrep_provider_options="gcs.fc_factor=1.0"

# Use flat profile instead of scaling (default: NO)
wsrep_provider_options="gcs.fc_single_primary=NO"
```

**Example: Create hysteresis to reduce FC oscillation:**
```ini
wsrep_provider_options="gcs.fc_limit=32;gcs.fc_factor=0.8"

Result for 3-node cluster:
  upper_limit = 32 × √3 ≈ 55
  lower_limit = 55 × 0.8 = 44
  Interval: [44, 55]  # 11-writeset hysteresis
```

### Monitoring

Check flow control status in MySQL:
```sql
-- See how much time cluster is paused by FC
SHOW STATUS LIKE 'wsrep_flow_control_paused';
-- 0.0 = never paused, 1.0 = always paused, 0.3 = 30% of time

-- Count FC messages sent by this node
SHOW STATUS LIKE 'wsrep_flow_control_sent';

-- Count FC messages received from other nodes  
SHOW STATUS LIKE 'wsrep_flow_control_recv';
```

### Technical Deep Dive

For complete source code analysis and technical details, see:
- [`FLOW_CONTROL_INTERVAL.md`](FLOW_CONTROL_INTERVAL.md) - Complete technical documentation
- Source: `galera-4-26.4.23/gcs/src/gcs.cpp` (lines 919-955)

**Key Implementation Details:**
- Flow control enabled when node state transitions to SYNCED
- Limits recalculated on every view change (membership change)
- Node sends FC_STOP when queue > upper_limit
- Node sends FC_CONT when queue < lower_limit
- All decisions local to each node (distributed mechanism)

### Official Documentation

- [Galera Flow Control](https://galeracluster.com/library/documentation/flow-control.html)
- [Flow Control Parameters](https://galeracluster.com/library/documentation/galera-parameters.html#gcs-fc-limit)
- [Monitoring Flow Control](https://galeracluster.com/library/documentation/monitoring-cluster.html#checking-flow-control)

---

## General Tips

### Understanding Timeline Gaps
If you see gaps or missing data for a node:
1. Check if the node received SST (its log was reset)
2. Look for node restart events (new UUID)
3. Verify log file coverage (are all nodes' logs included?)

### Cross-Node Analysis
For complete cluster understanding:
- Always analyze logs from all cluster nodes together
- Use `grax3` with all log files: `./grax3 node1.log node2.log node3.log`
- Single-node analysis will miss critical relationships

### Cluster Group Lifetimes
Each cluster bootstrap creates a new group:
- Group 1: First bootstrap
- Group 2+: Subsequent restarts/rebootstraps
- All metrics (SST, views, errors) are tracked per group
- This enables accurate time-based analysis

### Frame Navigation

The timeline interface provides two complementary navigation systems:

#### 1. Frame Navigation (Discrete Events)
- **Frame-by-frame stepping**: Use Prev/Next buttons or frame number input
- **Frame definition**: Each frame represents the cluster state after applying one event
- **Event-driven**: A new frame is generated when at least one property of the system changes
- **Time gaps**: There can be significant time gaps between adjacent frames
  - Example: Frame 100 at 10:30:00, Frame 101 at 15:45:00 (5-hour gap of no events)
- **Useful for**: Analyzing specific state changes and event sequences

#### 2. Natural Timeline (Continuous Time)
- **Visual bar**: Horizontal timeline bar showing the full time range of analysis
- **Vertical markers**: Clickable markers indicate when system status changes occurred
- **Time-proportional**: The distance between markers reflects actual elapsed time
- **Hotspots**: Color-coded markers show event importance:
  - Top track (SST): SST operations
  - CLU track: Cluster-level issues
  - NOD track: Node-level issues  
  - COM track: Component issues
- **Useful for**: Understanding timing, identifying quiet periods, and spotting event clusters

#### Key Differences

**Frame Navigation:**
- Treats all events equally (Frame N → Frame N+1)
- Ignores time gaps between events
- Best for: Step-by-step analysis, debugging specific transitions

**Natural Timeline:**
- Shows true temporal distribution of events
- Reveals periods of stability vs. chaos
- Best for: Understanding cluster behavior over time, correlating external events

#### Usage Tips
- Click on **natural timeline markers** to jump to that moment in time
- The natural timeline helps identify:
  - Quiet periods (long gaps between markers)
  - Event storms (dense clusters of markers)
  - Correlation with time-based patterns (daily schedules, backups, etc.)
- Use frame navigation to examine each state change in detail
- Switch between both views for comprehensive analysis

**Example Scenario:**
```
Natural Timeline:    |----*--*--*-----------*-----------------*---|
Frame Numbers:       1    2  3  4           5                 6
Time:               10:00              13:00              18:00

Frames 2-4: Three quick events within minutes (burst activity)
Gap 4→5: 3-hour quiet period with no state changes
Gap 5→6: 5-hour quiet period
```

This dual navigation allows you to:
1. See the big picture (natural timeline)
2. Analyze details (frame navigation)
3. Understand both "what changed" and "when it changed"

---

## Need More Help?

For detailed technical information:
- See `COMPREHENSIVE_ENTITY_MODEL.md` for entity architecture
- See `ARCHITECTURE.md` for system design
- Run `./graa3 --help` for analysis options
- Run `./grap3 --help` for parser options
