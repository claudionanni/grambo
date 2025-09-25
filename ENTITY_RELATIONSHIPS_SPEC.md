# Galera Cluster Entity Model & State Tracking Specification
**Document Version**: 2.0  
**Date**: September 25, 2025  
**Purpose**: Comprehensive entity model and relationship specification for Grambo Phase 2  
**Status**: SPECIFICATION COMPLETE - Ready for Implementation  
**Parent Document**: GRAMBO_REFACTORING_SPEC.md

---

## 1. EXECUTIVE SUMMARY

This document defines the complete entity model for tracking Galera cluster state, health, and operations. Building upon the successful Phase 1 implementation (Node Name Extraction & Enhanced Analysis), this specification outlines the additional entities and relationships required for comprehensive cluster monitoring and troubleshooting.

### 1.1 Key Objectives
- **Complete State Tracking**: Monitor individual node lifecycle states and transitions
- **Cluster Health Monitoring**: Track consensus state, quorum changes, and cluster topology
- **Operational Context**: Understand the relationships between state changes, SST operations, and cluster events
- **Root Cause Analysis**: Provide causal relationships for troubleshooting cluster issues

### 1.2 Implementation Foundation
This specification extends the current Phase 1 entities:
- ✅ **NODE**: Enhanced with state tracking capabilities
- ✅ **STATE_TRANSFER**: Existing SST/IST temporal entity system
- ✅ **VIEW**: Basic cluster view change detection

And adds new Phase 2 entities:
- 🆕 **NODE_STATE**: Individual node state transition tracking
- 🆕 **QUORUM**: Multi-line cluster consensus state snapshots
- 🆕 **CLUSTER**: Root aggregate entity for cluster-wide state

### 1.3 Enhanced VIEW Entity Architecture

**Critical Discovery**: Galera cluster views are logged in **two distinct multi-line formats** that must be parsed together for complete membership understanding:

#### 1.3.1 **Compact View Format** (Complete Membership + Short UUIDs)
```
2025-09-16 20:47:25 0 [Note] WSREP: view(view_id(NON_PRIM,71b98d8f-ad01,8) memb {
	71b98d8f-ad01,0
} joined {
} left {
} partitioned {
	afb71d41-8233,0
})
```

**Characteristics**:
- Shows **all membership types**: `memb`, `joined`, `left`, `partitioned`
- Uses **short UUID format**: `71b98d8f-ad01` (first + fourth UUID segments)
- Essential for understanding **who left/joined** and **partition events**
- Critical for split-brain and network partition analysis

#### 1.3.2 **Detailed View Format** (Metadata + Long UUIDs) - Multiple Variants

**Variant A: Standard Detailed View**
```
2025-09-15 13:48:12 2 [Note] WSREP: ================================================
View:
  id: 378cdc73-9236-11f0-a8d4-426872f4d003:1646
  status: primary
  protocol_version: 4
  capabilities: MULTI-MASTER, CERTIFICATION, PARALLEL_APPLYING, REPLAY, ISOLATION, PAUSE, CAUSAL_READ, INCREMENTAL_WS, UNORDERED, PREORDERED, STREAMING, NBO
  final: no
  own_index: 0
  members(1):
	0: 378c0ec7-9236-11f0-a3db-f6fdc24ecc7d, UAT-DB-03
=================================================
```

**Variant B: SST Recovered View** ⭐ **NEW DISCOVERY**
```
2025-09-19 11:10:19 0 [Note] WSREP: Recovered view from SST:
  id: 670ce4a0-9538-11f0-aa1f-168ebbd1d7d1:2
  status: primary
  protocol_version: 4
  capabilities: MULTI-MASTER, CERTIFICATION, PARALLEL_APPLYING, REPLAY, ISOLATION, PAUSE, CAUSAL_READ, INCREMENTAL_WS, UNORDERED, PREORDERED, STREAMING, NBO
  final: no
  own_index: 0
  members(2):
	0: 71b98d8f-930b-11f0-ad03-aa72265b12f3, NODE_54320
	1: afb71d41-9267-11f0-8236-56b2b7c8eac9, NODE_54321
```

**Characteristics**:
- Shows **rich metadata**: protocol_version, capabilities, own_index
- Uses **long UUID format**: `378c0ec7-9236-11f0-a3db-f6fdc24ecc7d`
- Includes **node names**: `UAT-DB-03`
- Only shows **synced members** (members in SYNCED state)
- Critical for understanding cluster capabilities and node identity

#### 1.3.3 **Critical Parsing Issues Discovered** ⚠️

**Issue #1: Compact View ID Ambiguity**
```
2025-09-15 13:48:12 0 [Note] WSREP: view(view_id(PRIM,378c0ec7-a3db,17) memb {
	378c0ec7-a3db,1
} joined {
} left {
} partitioned {
	4d1157d4-8693,1
})
```
**CRITICAL PROBLEM**: The `378c0ec7-a3db` in `view_id(PRIM,378c0ec7-a3db,17)` is **NOT the actual view ID** - it's a **node UUID**. This is a Galera logging inconsistency.

**Issue #2: True View ID Structure**
The **actual view ID** follows the format: `Group UUID:sequence_number`

**Evidence from Quorum Results**:
```
2025-09-15 13:53:05 0 [Note] WSREP: Quorum results:
    group UUID = 378cdc73-9236-11f0-a8d4-426872f4d003

2025-09-15 13:53:05 2 [Note] WSREP: ================================================
View:
  id: 378cdc73-9236-11f0-a8d4-426872f4d003:1810  ← TRUE VIEW ID
```

**Issue #3: View Format Detection Complexity**
Three distinct view log patterns identified:
1. **Compact Format**: `view(view_id(PRIM,node_uuid,seq)` - **UNRELIABLE for view ID**
2. **Standard Detailed**: `WSREP: ==================... View: id: group_uuid:seq`
3. **SST Recovered**: `WSREP: Recovered view from SST: id: group_uuid:seq`

#### 1.3.4 **Parsing Strategy Implications** 

**Current Assessment**: 
- ❌ **Cannot reliably correlate compact views to true view IDs**
- ✅ **Can parse detailed views (both variants) for accurate view tracking**
- ⚠️ **Compact views useful only for membership change events, not view identification**

**Recommended Approach**:
```python
class ViewParsingStrategy:
    def parse_view_block(self, log_lines: List[str]) -> Optional[ViewEntity]:
        """Parse only reliable detailed view formats"""
        # Parse detailed views: standard + SST recovered variants
        # Skip compact views for view ID correlation (document limitation)
        
    def extract_membership_changes(self, compact_view_lines: List[str]) -> MembershipChangeEvent:
        """Extract membership changes without view ID correlation"""
        # Use compact views only for join/leave/partition analysis
        # Cannot link to specific view IDs due to logging ambiguity
```

---

## 2. GALERA CLUSTER STATE MODEL

### 2.1 Node Lifecycle State Systems

Galera nodes operate with **two parallel state systems** that must be tracked independently:

#### 2.1.1 **WSREP States** (Cluster Membership)
```
CLOSED → OPEN → PRIMARY → JOINED → SYNCED → DONOR/DESYNCED → JOINED → SYNCED
         ↓       ↓         ↓        ↓              ↓
      JOINER  JOINER   JOINER   DESYNCED      DESYNCED
```

**State Definitions**:
- **CLOSED**: Node not connected to cluster
- **OPEN**: Node connected but not in primary component
- **PRIMARY**: Node in primary component (can accept writes)
- **JOINED**: Node joined cluster but not synchronized
- **SYNCED**: Node fully synchronized and operational
- **JOINER**: Node receiving state transfer
- **DONOR/DESYNCED**: Node providing state transfer (temporarily desynced)
- **DESYNCED**: Node temporarily out of sync

#### 2.1.2 **Server States** (Application Level)
```
disconnected → connected → joiner → initializing → initialized → joined → synced → donor
                           ↑                                      ↓        ↑       ↓
                           └──────────────────────────────────────┘        └───────┘
```

**State Definitions**:
- **disconnected**: No cluster connectivity
- **connected**: Connected to cluster
- **joiner**: Preparing to receive state transfer
- **initializing**: Receiving state transfer (SST/IST)
- **initialized**: State transfer complete, applying changes
- **joined**: Fully joined cluster
- **synced**: Synchronized and operational
- **donor**: Providing state transfer to other nodes

### 2.2 State Transition Patterns

#### 2.2.1 **Node Bootstrap Sequence** (New Node Joining)
```bash
# WSREP States
CLOSED → OPEN → JOINER → JOINED → SYNCED

# Server States (parallel)
disconnected → connected → joiner → initializing → initialized → joined → synced
```

#### 2.2.2 **SST Donor Sequence** (Node Providing State Transfer)
```bash
# WSREP States
SYNCED → DONOR/DESYNCED → JOINED → SYNCED

# Server States (parallel)  
synced → donor → joined → synced
```

#### 2.2.3 **SST Joiner Sequence** (Node Receiving State Transfer)
```bash
# Server States
connected → joiner → initializing → initialized → joined → synced

# WSREP States (overlapping)
OPEN → JOINER → JOINED → SYNCED
```

#### 2.2.4 **Cluster Formation Sequence**
```bash
# Bootstrap Node (first node)
CLOSED → OPEN → PRIMARY

# Subsequent Nodes (joining existing cluster)
CLOSED → OPEN → PRIMARY → JOINER → JOINED → SYNCED
```

---

## 3. ENTITY MODEL SPECIFICATION

### 3.1 Enhanced and New Entity Types

#### 3.1.0 **Revised VIEW Entity** (Reliable View Parsing Strategy)

**Purpose**: Parse reliable Galera view formats while documenting limitations of compact format correlation

**Core Properties**:
```yaml
entity_type: VIEW
entity_id: "view_{group_uuid}_{sequence_number}"  # Only for reliable detailed views
timestamp: datetime
group_uuid: str             # True cluster group UUID (from detailed views only)
view_sequence: int          # Sequential view number  
cluster_state: str          # "primary" | "non-primary"
format_type: str            # "detailed_standard" | "detailed_sst" 

# Detailed Format Properties (reliable data)
protocol_version: int       # Galera protocol version
capabilities: List[str]     # Cluster capabilities list
own_index: int             # Local node index in view
final_view: bool           # Whether this is final view
member_details: List[dict] # [{index, long_uuid, node_name}]

# Metadata
view_source: str           # "standard" | "sst_recovered"
confidence: float          # Parsing confidence (always high for detailed views)
raw_lines: List[str]      # All related log lines
```

**Separate Entity for Membership Changes**:
```yaml
entity_type: MEMBERSHIP_CHANGE
entity_id: "membership_{timestamp}_{hash}"
timestamp: datetime
node_uuid_in_log: str      # ⚠️ Node UUID from compact format (NOT group UUID)
sequence_number: int       # Sequence from compact format
cluster_state: str         # "PRIM" | "NON_PRIM"

# Membership tracking (from compact format)
members_active: List[str]     # Short UUIDs of active members
members_joined: List[str]     # Recently joined members
members_left: List[str]       # Recently departed members
members_partitioned: List[str] # Partitioned members

# Limitations
correlation_status: str    # "uncorrelated" (cannot link to true view ID)
notes: str                # "Compact format uses node UUID, not group UUID"
```

**Multi-Line Parsing Patterns** (Revised):
```python
# Reliable Detailed Format Detection (Two Variants)
DETAILED_VIEW_STANDARD = r'WSREP:\s*={40,}\s*\nView:\s*\n\s*id:\s*(?P<group_uuid>[^:]+):(?P<seq>\d+)'
DETAILED_VIEW_SST = r'WSREP:\s*Recovered view from SST:\s*\n\s*id:\s*(?P<group_uuid>[^:]+):(?P<seq>\d+)'

# Compact Format Detection (Membership Changes Only - NO VIEW ID CORRELATION)  
COMPACT_VIEW_START = r'view\(view_id\((?P<state>PRIM|NON_PRIM),(?P<node_uuid>[^,]+),(?P<seq>\d+)\)\s+memb\s+\{'
# ⚠️ WARNING: node_uuid ≠ group_uuid (cannot correlate to true view ID)

# Member Extraction Patterns
DETAILED_MEMBER = r'^\s*(?P<index>\d+):\s*(?P<long_uuid>[^,]+),\s*(?P<node_name>.+)$'
COMPACT_MEMBER = r'^\s*(?P<short_uuid>[a-f0-9-]+),(?P<index>\d+)\s*$'

# Quorum Pattern for Group UUID Discovery
QUORUM_GROUP_UUID = r'group UUID = (?P<group_uuid>[a-f0-9-]+)'
```

**Revised Parsing Logic** (Based on Discovery):
```python
class RevisedViewParser:
    """
    Revised parsing strategy addressing Galera logging inconsistencies
    """
    
    def parse_reliable_view(self, log_lines: List[str]) -> Optional[ViewEntity]:
        """Parse only detailed view formats with reliable view IDs"""
        # Parse standard detailed view: ================= View: id: group_uuid:seq
        standard_view = self.parse_standard_detailed_view(log_lines)
        if standard_view:
            return standard_view
            
        # Parse SST recovered view: Recovered view from SST: id: group_uuid:seq  
        sst_view = self.parse_sst_recovered_view(log_lines)
        if sst_view:
            return sst_view
            
        return None
    
    def extract_membership_events(self, log_lines: List[str]) -> Optional[MembershipChangeEvent]:
        """Extract membership changes from compact format (no view ID correlation)"""
        # Parse: view(view_id(PRIM,node_uuid,seq) memb { ... joined { ... left { ... partitioned {
        # ⚠️ LIMITATION: Cannot correlate to true view ID due to node_uuid != group_uuid
        
    def correlate_with_quorum_results(self, quorum_lines: List[str]) -> Optional[str]:
        """Extract group UUID from quorum results for view correlation"""
        # Parse: group UUID = 378cdc73-9236-11f0-a8d4-426872f4d003
        # Use for correlating subsequent detailed views
        
    def generate_view_timeline(self, entities: List[ViewEntity]) -> List[ViewTimelineEvent]:
        """Generate timeline using only reliable detailed views"""
        # Build chronological sequence of confirmed view changes
        # Document gaps where compact views cannot be correlated
```

**Enhanced Membership Analysis**:
```python
def analyze_cluster_membership_changes(view_entities: List[ViewEntity]) -> Dict[str, Any]:
    """Comprehensive membership change analysis"""
    return {
        'membership_transitions': analyze_join_leave_patterns(view_entities),
        'partition_events': detect_network_partitions(view_entities), 
        'split_brain_scenarios': detect_split_brain_events(view_entities),
        'uuid_correlation_accuracy': calculate_uuid_mapping_confidence(view_entities),
        'node_identity_resolution': resolve_node_names_from_uuids(view_entities)
    }
```

#### 3.1.1 **NODE_STATE** Entity (Critical Addition)
**Purpose**: Track individual node state transitions within the cluster lifecycle

```python
@dataclass
class NodeStateEntity(Entity):
    entity_type: EntityType = EntityType.NODE_STATE
    entity_id: str = ""                    # Generated: "nodestate_{timestamp}_{node_name}_{transition}"
    
    # Core transition data
    node_name: str = ""                    # Node identifier (e.g., "vinfr-db-d-l05")
    previous_state: str = ""               # Previous state in transition
    current_state: str = ""                # Current state after transition
    state_type: str = ""                   # "WSREP" or "Server" state system
    transaction_order: Optional[int] = None # TO: value for WSREP states
    
    # Temporal data
    timestamp: datetime                    # When transition occurred
    transition_duration: Optional[timedelta] = None  # Time in previous state
    
    # Validation enums
    WSREP_STATES = [
        "CLOSED", "OPEN", "PRIMARY", "JOINED", "SYNCED", 
        "DONOR/DESYNCED", "JOINER", "DONOR", "DESYNCED"
    ]
    
    SERVER_STATES = [
        "disconnected", "connected", "joiner", "initializing", 
        "initialized", "joined", "synced", "donor"
    ]
    
    def validate_transition(self) -> bool:
        """Validate if this state transition is valid according to Galera state machine"""
        if self.state_type == "WSREP":
            return self._validate_wsrep_transition()
        elif self.state_type == "Server":
            return self._validate_server_transition()
        return False
```

**Extraction Patterns**:
```yaml
# WSREP state transitions
wsrep_state_shift:
  pattern_id: "wsrep_state_transition_v1"
  entity_type: "NODE_STATE"
  regex: "WSREP: Shifting (?P<previous_state>\\w+/?.\\w*) -> (?P<current_state>\\w+/?.\\w*) \\(TO: (?P<transaction_order>\\d+)\\)"
  properties:
    state_type: "WSREP"
    node_name: "{extract_from_context}"
  confidence: 0.95

# Server state transitions
server_state_change:
  pattern_id: "server_state_transition_v1" 
  entity_type: "NODE_STATE"
  regex: "WSREP: Server status change (?P<previous_state>\\w+) -> (?P<current_state>\\w+)"
  properties:
    state_type: "Server"
    node_name: "{extract_from_context}"
  confidence: 0.95

# State restoration (recovery scenarios)
wsrep_state_restore:
  pattern_id: "wsrep_state_restore_v1"
  entity_type: "NODE_STATE" 
  regex: "WSREP: Restored state (?P<previous_state>\\w+) -> (?P<current_state>\\w+) \\((?P<transaction_order>\\d+)\\)"
  properties:
    state_type: "WSREP"
    node_name: "{extract_from_context}"
  confidence: 0.90
```

#### 3.1.2 **QUORUM** Entity (Cluster Health Snapshots)
**Purpose**: Multi-line structured entity representing cluster consensus state

```python
@dataclass
class QuorumEntity(Entity):
    entity_type: EntityType = EntityType.QUORUM
    entity_id: str = ""                    # Generated: "quorum_{timestamp}_{conf_id}"
    
    # Quorum configuration
    version: int = 0                       # Quorum version number
    component: str = ""                    # PRIMARY, NON_PRIMARY, etc.
    conf_id: int = 0                       # Configuration ID
    
    # Membership data
    members_joined: int = 0                # Active/joined members
    members_total: int = 0                 # Total cluster members configured
    membership_ratio: float = 0.0          # joined/total ratio
    
    # Transaction tracking
    act_id: int = 0                        # Action ID (current transaction)
    last_appl: int = 0                     # Last applied transaction
    transaction_lag: int = 0               # act_id - last_appl
    
    # Protocol versions
    protocols_gcs: int = 0                 # GCS protocol version
    protocols_repl: int = 0                # Replication protocol version
    protocols_appl: int = 0                # Application protocol version
    
    # Cluster identity
    vote_policy: int = 0                   # Voting policy
    group_uuid: str = ""                   # Cluster group UUID
    
    # Temporal data
    timestamp: datetime                    # Quorum calculation time
    
    # Derived properties
    is_primary_component: bool = field(init=False)
    has_quorum: bool = field(init=False)
    cluster_health_score: float = field(init=False)
    
    def __post_init__(self):
        """Calculate derived properties after initialization"""
        self.is_primary_component = (self.component == "PRIMARY")
        self.membership_ratio = self.members_joined / max(self.members_total, 1)
        self.has_quorum = self.membership_ratio > 0.5
        self.transaction_lag = self.act_id - self.last_appl
        
        # Health score: primary component + membership ratio + transaction currency
        health_components = [
            1.0 if self.is_primary_component else 0.0,
            self.membership_ratio,
            1.0 if self.transaction_lag < 100 else max(0.0, 1.0 - (self.transaction_lag / 1000))
        ]
        self.cluster_health_score = sum(health_components) / len(health_components)
```

---

## 4. ENTITY RELATIONSHIP MATRIX

| Source Entity | Target Entity | Relationship Type | Cardinality | Description |
|---------------|---------------|-------------------|-------------|-------------|
| **NODE** | **NODE_STATE** | has_state_transition | 1:N | Node lifecycle through state transitions |
| **NODE_STATE** | **STATE_TRANSFER** | triggers/triggered_by | 1:1 | State changes trigger SST operations |
| **NODE_STATE** | **QUORUM** | coincides_with | N:1 | State changes correlate with quorum changes |
| **QUORUM** | **CLUSTER** | represents_state_of | N:1 | Quorum snapshots represent cluster state |
| **STATE_TRANSFER** | **NODE** | involves_donor/joiner | 1:1 | SST operations involve specific nodes |
| **CLUSTER** | **NODE** | includes_member | 1:N | Cluster contains member nodes |
| **VIEW** | **QUORUM** | triggers_recalculation | 1:1 | View changes trigger quorum recalculation |
| **NODE** | **VIEW** | triggers_change | 1:1 | Node join/leave triggers view change |
| **VIEW** | **VIEW** | format_correlation | 1:1 | Compact and detailed formats of same view |
| **NODE** | **VIEW** | uuid_mapping | M:N | UUID correlation between view formats |
| **ERROR** | **NODE_STATE** | relates_to | N:1 | Errors relate to state transitions |
| **ERROR** | **STATE_TRANSFER** | caused_by | N:1 | Errors caused by SST failures |

## 5. KEY RELATIONSHIP PATTERNS

### 5.1 **NODE ↔ NODE_STATE** (One-to-Many Lifecycle)
**Purpose**: Track complete lifecycle of each node through state transitions

```python
class NodeStateRelationship:
    relationship_type = "has_state_transition"
    source_entity: NodeEntity              # The node
    target_entity: NodeStateEntity         # The state transition
    
    # Relationship properties
    transition_sequence: int               # Order in node's state sequence (1, 2, 3...)
    is_current_state: bool                 # Is this the node's current state
    state_duration: Optional[timedelta]    # How long node was in previous state
    is_valid_transition: bool              # Does this follow valid state machine
```

### 5.2 **NODE_STATE ↔ STATE_TRANSFER** (Causal Relationships)
**Purpose**: Link state transitions to SST operations with causal understanding

```python
CAUSALITY_PATTERNS = {
    "sst_donation_trigger": "SYNCED → DONOR/DESYNCED triggers SST as donor",
    "sst_reception_trigger": "joiner → initializing indicates SST reception start", 
    "sst_donation_complete": "donor → joined indicates SST donation complete",
    "sst_reception_complete": "initialized → joined indicates SST reception complete"
}
```

### 5.3 **QUORUM ↔ CLUSTER** (State Representation)
**Purpose**: Each quorum represents a snapshot of cluster state at a specific time

```python
class QuorumClusterRelationship:
    relationship_type = "represents_state_of"
    source_entity: QuorumEntity            # The quorum snapshot
    target_entity: ClusterEntity           # The cluster
    
    # Change tracking
    state_change_trigger: str              # What triggered this quorum recalculation
    membership_delta: int                  # Change in member count from previous
    configuration_change: bool             # Did cluster configuration change
}
```

## 6. STATE MACHINE DEFINITIONS

### 6.1 WSREP State Machine Validation
```python
VALID_WSREP_TRANSITIONS = {
    "CLOSED": ["OPEN"],
    "OPEN": ["PRIMARY", "CLOSED"],
    "PRIMARY": ["JOINER", "JOINED", "CLOSED"],
    "JOINER": ["JOINED", "CLOSED"],
    "JOINED": ["SYNCED", "CLOSED"],
    "SYNCED": ["DONOR/DESYNCED", "DESYNCED", "JOINED", "CLOSED"],
    "DONOR/DESYNCED": ["JOINED", "CLOSED"],
    "DESYNCED": ["SYNCED", "JOINED", "CLOSED"]
}
```

### 6.2 Server State Machine Validation
```python
VALID_SERVER_TRANSITIONS = {
    "disconnected": ["connected"],
    "connected": ["joiner", "disconnected"],
    "joiner": ["initializing", "disconnected"],
    "initializing": ["initialized", "disconnected"],
    "initialized": ["joined", "disconnected"],
    "joined": ["synced", "disconnected"],
    "synced": ["donor", "joined", "disconnected"],
    "donor": ["joined", "synced", "disconnected"]
}
```

## 7. MULTI-LINE ENTITY EXTRACTION

### 7.1 QUORUM Pattern (Multi-line)
```yaml
# Example quorum extraction
quorum_results:
  pattern_id: "quorum_results_v1"
  entity_type: "QUORUM"
  multiline: true
  start_pattern: "WSREP: Quorum results:"
  property_patterns:
    - regex: "\\s+version\\s+=\\s+(?P<version>\\d+),"
    - regex: "\\s+component\\s+=\\s+(?P<component>\\w+),"
    - regex: "\\s+conf_id\\s+=\\s+(?P<conf_id>\\d+),"
    - regex: "\\s+members\\s+=\\s+(?P<members_joined>\\d+)/(?P<members_total>\\d+)"
    - regex: "\\s+group UUID\\s+=\\s+(?P<group_uuid>[a-f0-9-]+)"
  confidence: 0.95
```

### 7.2 State Transition Patterns
```bash
# Example log lines from user's specification:
2025-09-24 11:03:42 0 [Note] WSREP: Shifting CLOSED -> OPEN (TO: 0)
2025-09-24 11:03:42 0 [Note] WSREP: Restored state OPEN -> JOINED (67473643)
2025-09-24 11:03:42 0 [Note] WSREP: Shifting JOINED -> SYNCED (TO: 67473643)
2025-09-24 11:03:42 2 [Note] WSREP: Server status change disconnected -> connected
2025-09-24 11:03:42 2 [Note] WSREP: Server status change connected -> joiner
```

## 8. ENHANCED ANALYSIS OUTPUT

### 8.1 Complete Node Lifecycle Analysis
```bash
🖥️ NODE LIFECYCLE ANALYSIS:
├─ vinfr-db-d-l05 (9dbff1f8-a3ef):
│  ├─ Current State: WSREP=SYNCED, Server=synced
│  ├─ Lifecycle: CLOSED→OPEN→JOINER→JOINED→SYNCED (4 transitions, 2m 15s)
│  ├─ SST Operations: 1 joiner (received 2.3GB in 45s)
│  ├─ Stability Score: 0.85 (stable)
│  └─ Anomalies: None detected
```

### 8.2 Cluster Health Dashboard  
```bash
🏥 CLUSTER HEALTH ANALYSIS:
├─ Overall State: OPERATIONAL (3/3 nodes active)
├─ Cluster UUID: 6a6ef0ea-979d-11f0-95e4-77c6b991cf0b
├─ Membership Evolution: 1→2→3→2→3 nodes (5 reconfigurations)
├─ Health Score: 85% (good - minor instability from node cycling)
├─ Quorum History: 8 reconfigurations in 6h window
└─ Current Quorum: PRIMARY component, 3/3 joined, lag=12 transactions
```

### 8.3 Root Cause Analysis
```bash
🔍 ROOT CAUSE ANALYSIS:
├─ SST #1: Triggered by node join (vinfr-db-d-l05 CLOSED→OPEN)
├─ SST #2: Triggered by donor promotion (vinfr-db-d-d01 SYNCED→DONOR/DESYNCED)  
├─ State Anomalies: 1 invalid transition detected
└─ Cluster Events: Bootstrap→Expansion→Stabilization (6h 23m total)
```

---

## 9. IMPLEMENTATION ROADMAP

### 9.1 Phase 2A: Enhanced View Parsing & State Tracking (Priority 1)
**Timeline**: 3-4 weeks
**Deliverables**:
- [ ] Implement enhanced `VIEW` entity with multi-format parsing
- [ ] Add UUID correlation system for compact/detailed view formats
- [ ] Implement multi-line parsing for membership tracking
- [ ] Create comprehensive membership change analysis
- [ ] Implement `NODE_STATE` entity class with validation
- [ ] Add state transition pattern extraction (WSREP + Server states)
- [ ] Implement state machine validation (anomaly detection)
- [ ] Create `NODE_STATE ↔ NODE` and `VIEW ↔ NODE` relationships
- [ ] Add complete view and state transition analysis to graa.py output

**Success Criteria**:
- Parse 100% of compact and detailed view formats with UUID correlation
- Track complete membership changes: joins, leaves, partitions
- Detect split-brain scenarios and network partitions with 95% accuracy  
- Track complete node lifecycle from bootstrap to operational
- Detect 95% of state transitions in test logs
- Identify state machine violations and anomalies
- Correlate view changes with state transitions
- Provide comprehensive cluster membership and state analysis in graa.py

### 9.2 Phase 2B: Cluster Health Monitoring (Priority 2)
**Timeline**: 2-3 weeks  
**Deliverables**:
- [ ] Implement `QUORUM` entity with multi-line parsing
- [ ] Implement `CLUSTER` entity as aggregate root
- [ ] Add cluster health state machine and scoring
- [ ] Create `QUORUM ↔ CLUSTER` relationships
- [ ] Add cluster health dashboard to graa.py

**Success Criteria**:
- Parse 100% of quorum results in test logs
- Track cluster membership changes and health evolution
- Detect cluster degradation and partition events
- Provide cluster health scoring and recommendations

### 9.3 Phase 2C: Causal Relationship Detection (Priority 3)
**Timeline**: 3-4 weeks
**Deliverables**:
- [ ] Implement causal relationship detection engine
- [ ] Add `NODE_STATE ↔ STATE_TRANSFER` causal relationships
- [ ] Implement temporal correlation analysis
- [ ] Create root cause analysis algorithms
- [ ] Add causality timeline to graa.py output

**Success Criteria**:
- Automatically detect "why" SSTs were triggered (90% accuracy)
- Correlate node state changes with cluster events
- Provide root cause analysis for cluster issues
- Generate actionable troubleshooting recommendations

## 10. EXPECTED OUTCOMES

### 10.1 Enhanced Troubleshooting Capabilities
With complete entity model implementation, cluster troubleshooting becomes:

**Before (Phase 1)**:
- Basic SST operation detection
- Node name identification  
- Simple cluster view tracking

**After (Phase 2)**:
- **Complete Node Lifecycle Tracking**: See exactly how each node progressed through states
- **Root Cause Analysis**: Understand why SSTs were triggered and what caused failures
- **Cluster Health Monitoring**: Track cluster health evolution and detect degradation
- **Predictive Analysis**: Identify patterns that lead to cluster issues
- **Anomaly Detection**: Automatic detection of state machine violations and unusual patterns

### 10.2 Operational Intelligence Benefits
- **Enhanced gras.py**: Feed rich entity data to visualization
- **Automated Alerting**: Trigger alerts on cluster health degradation  
- **Capacity Planning**: Historical analysis of cluster growth patterns
- **Compliance Monitoring**: Validate cluster operations against SLAs

## 11. RELATIONSHIP DISCOVERY RULES

### 11.1 Direct Field Correlation (Confidence: 0.95)
1. **Node Name Matching**:
   - `node_name` exact match across entities
   - `donor_node`/`joiner_node` matches node entities
   - UUID correlation (long ↔ short formats)

2. **State Transition Sequencing**:
   - Same node, sequential timestamps → lifecycle progression
   - Same transaction_order (TO) values → related WSREP events
   - State machine validation → valid transition detection

3. **SST Session Correlation**:
   - Same donor + joiner + timeframe → session grouping
   - Method extraction from WSREP_SST script lines → transfer_method

### 11.2 Temporal Correlation (Confidence: 0.70-0.85)
1. **Causal Event Windows**:
   - State transitions within 1-5 seconds → potential causality
   - SST start → node state changes → completion sequence
   - Quorum recalculations → membership changes

2. **Timeline Analysis**:
   - Node join → SST trigger → cluster stabilization
   - Error events → preceding operations correlation
   - View changes → quorum recalculations

### 11.3 Multi-line Pattern Correlation (Confidence: 0.85-0.95)
1. **Quorum Results**:
   - Multi-line structured parsing with property validation
   - Membership count changes → cluster size evolution
   - Component status → cluster health assessment

2. **State Machine Validation**:
   - WSREP state transitions → validate against state machine
   - Server state transitions → application-level validation
   - Anomaly detection → invalid transition identification

## 12. CONFIGURATION & TUNING

### 12.1 Relationship Detection Configuration
```yaml
# ~/.grambo/config.yml - Phase 2 additions
relationships:
  state_transitions:
    enabled: true
    validation_strict: true           # Enforce state machine rules
    anomaly_detection: true          # Detect invalid transitions
    temporal_window_ms: 2000         # Max time between related events
    
  causal_detection:
    enabled: true
    confidence_threshold: 0.7        # Minimum confidence for causality
    sst_trigger_patterns: true       # Detect SST causality patterns
    max_time_delta_ms: 5000         # Max time for causal correlation
    
  quorum_tracking:
    enabled: true
    multiline_timeout_ms: 5000      # Max time to collect quorum data
    health_scoring: true            # Calculate cluster health scores
    membership_change_detection: true # Track membership evolution

entities:
  node_state:
    lifecycle_validation: true       # Validate complete node lifecycles
    state_duration_tracking: true    # Track time in each state
    stability_scoring: true         # Calculate node stability metrics
```

## 13. TECHNICAL IMPLEMENTATION NOTES

### 13.1 Memory-Efficient Processing
```python
class EntityProcessor:
    def __init__(self):
        self.state_machines = {}         # Per-node state validation
        self.pending_quorums = {}        # Multi-line quorum collection
        self.relationship_buffer = []    # Batch relationship processing
        
    def process_streaming(self, log_lines: Iterator[str]):
        """Process entities in streaming fashion for large logs"""
        for line_num, line in enumerate(log_lines):
            # Process single-line patterns (most common)
            entity = self._process_single_line(line, line_num)
            if entity:
                yield entity
                
            # Process multi-line patterns (quorum)
            entity = self._process_multiline(line, line_num)
            if entity:
                yield entity
```

### 13.2 Performance Optimization
- **Streaming Processing**: Handle large log files without memory overflow
- **Indexed Relationships**: Efficient querying with O(1) entity lookup
- **Batch Processing**: Group relationship detection for performance
- **State Machine Caching**: Reuse validation logic per node

## 14. VALIDATION & TESTING

### 14.1 Test Coverage Requirements
```bash
# Test scenarios for Phase 2 entities
TEST_SCENARIOS = [
    "3node_bootstrap_complete",      # Clean cluster formation
    "node_join_with_sst",           # New node joining via SST
    "rolling_restart_sequence",     # Planned maintenance restart
    "network_partition_recovery",   # Split-brain and recovery
    "sst_failure_and_retry",       # Failed SST with recovery
    "state_machine_violations",     # Invalid state transitions
    "large_cluster_operations"      # 10+ node cluster behavior
]

# Validation targets
ACCURACY_TARGETS = {
    "state_transition_detection": ">98%",  # Detect all state changes
    "quorum_parsing_accuracy": ">95%",     # Parse quorum results
    "causal_relationship_detection": ">85%", # Detect SST causality
    "anomaly_detection_rate": ">80%",      # Detect state violations
    "cluster_health_accuracy": ">90%"     # Accurate health scoring
}
```

## 15. CONCLUSION

This comprehensive specification provides the foundation for Phase 2 of the Grambo entity-based refactoring, enabling:

### 15.1 Complete Cluster Understanding
- **Individual Node Lifecycles**: Track every state transition for each node
- **Cluster Health Evolution**: Monitor cluster-wide health and stability  
- **Causal Relationships**: Understand why events occurred, not just that they occurred
- **Operational Intelligence**: Data-driven insights for cluster management

### 15.2 Enhanced Troubleshooting
- **Root Cause Analysis**: Automatically identify why SSTs were triggered
- **Anomaly Detection**: Detect state machine violations and unusual patterns
- **Predictive Analysis**: Identify patterns leading to cluster issues
- **Timeline Reconstruction**: Complete chronological analysis with causal context

### 15.3 Production Benefits
- **Faster Issue Resolution**: Rich diagnostic information for rapid troubleshooting
- **Proactive Monitoring**: Early detection of cluster health degradation
- **Capacity Planning**: Historical analysis for growth and performance planning
- **Operational Excellence**: Data-driven decisions for cluster optimization

---

## 16. ENHANCED VIEW PARSING EXAMPLES

### 16.1 Revised View Parsing Strategy Example

**Input Log Sequence**:
```
2025-09-16 20:47:25 0 [Note] WSREP: view(view_id(NON_PRIM,71b98d8f-ad01,8) memb {
	71b98d8f-ad01,0
} joined {
} left {
} partitioned {
	afb71d41-8233,0
})

2025-09-16 20:47:25 2 [Note] WSREP: ================================================
View:
  id: 71b98d8f-9236-11f0-ad01-f6fdc24ecc7d:8
  status: non-primary  
  protocol_version: 4
  capabilities: MULTI-MASTER, CERTIFICATION
  final: no
  own_index: 0
  members(1):
	0: 71b98d8f-9236-11f0-ad01-f6fdc24ecc7d, UAT-DB-01
=================================================
```

**Revised Analysis Output**:
```
🔗 CLUSTER VIEWS: Reliable View Tracking + Membership Events

📊 RELIABLE VIEW TIMELINE:
   View ID: 670ce4a0-9538-11f0-aa1f-168ebbd1d7d1:2
   Timestamp: 2025-09-19 11:10:19
   Source: SST Recovery
   Status: Primary ✓
   Members: 2 nodes
   • NODE_54320 (71b98d8f-930b-11f0-ad03-aa72265b12f3)
   • NODE_54321 (afb71d41-9267-11f0-8236-56b2b7c8eac9)

⚠️  MEMBERSHIP EVENTS (Uncorrelated):
   Event: 2025-09-16 20:47:25
   Source: Compact format (view sequence 8)
   Status: NON-PRIMARY partition detected
   
   ❌ Cannot correlate to true view ID (Galera logging limitation)
   📊 Membership Changes Detected:
   • Active: 1 node (71b98d8f-ad01)
   • Partitioned: 1 node (afb71d41-8233)
   
   💡 Analysis: Network partition event detected but cannot link
      to specific group UUID due to compact format ambiguity
```

### 16.2 Membership Change Tracking Example

**Log Sequence Showing Node Join**:
```
# Before: 1-node cluster
2025-09-15 13:45:55 0 [Note] WSREP: view(view_id(PRIM,378c0ec7-a3db,16) memb {
	378c0ec7-a3db,0
}

# After: 2-node cluster  
2025-09-15 13:48:12 0 [Note] WSREP: view(view_id(PRIM,378c0ec7-a3db,17) memb {
	378c0ec7-a3db,0  
	4d1157d4-8693,1
} joined {
	4d1157d4-8693,1
}
```

**Enhanced Analysis Output**:
```
🔄 MEMBERSHIP TRANSITIONS: Complete Join/Leave Analysis

📈 VIEW CHANGE #16→17: 2025-09-15 13:48:12
   Change Type: Node Addition
   Cluster Size: 1 → 2 nodes (+100% growth)
   
   Joined Nodes:
   • 4d1157d4-8693 (index:1) ← NEW NODE
     └─ Identity: [Resolving from detailed view...]
   
   Stable Members:  
   • 378c0ec7-a3db (index:0) ← UAT-DB-03 [CONFIRMED]
   
   Cluster Health: PRIMARY ✓ (quorum maintained)
```

**Benefits of Revised VIEW Parsing Strategy**:
1. **Reliable View Tracking**: Only parse views with verifiable group UUIDs (detailed formats)
2. **Accurate Timeline**: Build chronological sequence using confirmed view IDs
3. **Membership Event Detection**: Capture join/leave/partition events from compact format
4. **Documented Limitations**: Clear documentation of Galera logging inconsistencies
5. **Future-Proof Design**: Framework ready for improved correlation methods

**Known Limitations**:
1. **Compact Format Correlation**: Cannot reliably map compact views to true view IDs
2. **Timeline Gaps**: Some membership events cannot be linked to specific views
3. **Forensic Constraints**: Limited correlation capability for troubleshooting certain scenarios
4. **Galera Logging Issue**: Dependency on upstream fix for compact format view ID accuracy

---

## 17. GALERA VIEW PARSING RESEARCH FINDINGS

### 17.1 Critical Discovery: View ID Logging Inconsistency

**Research Date**: September 25, 2025  
**Finding**: Galera compact view format contains **node UUID** instead of **group UUID** in view identifier

**Evidence Analysis**:
```bash
# Compact Format (MISLEADING VIEW ID)
2025-09-15 13:48:12 0 [Note] WSREP: view(view_id(PRIM,378c0ec7-a3db,17) 
#                                                      ↑
#                                               NODE UUID (NOT VIEW ID!)

# Quorum Results (TRUE GROUP UUID)  
2025-09-15 13:53:05 0 [Note] WSREP: Quorum results:
    group UUID = 378cdc73-9236-11f0-a8d4-426872f4d003
#                ↑
#         TRUE GROUP UUID FOR VIEWS

# Detailed View (CORRECT VIEW ID)
View:
  id: 378cdc73-9236-11f0-a8d4-426872f4d003:1810
#     ↑
#     MATCHES GROUP UUID FROM QUORUM
```

**Impact Assessment**:
- ❌ **Cannot correlate compact views to true view IDs**
- ✅ **Detailed views provide accurate view identification**
- ⚠️ **Membership events detectable but not linkable to specific views**

### 17.2 View Format Taxonomy

**Format Classification**:
```
1. COMPACT FORMAT (Membership Rich, ID Unreliable)
   Pattern: view(view_id(PRIM,node_uuid,seq) memb {...} joined {...} left {...} partitioned {...})
   UUID: Node UUID (unreliable for view correlation)
   Data: Complete membership state changes
   
2. DETAILED STANDARD (Reliable ID, Limited Membership)  
   Pattern: ==================== View: id: group_uuid:seq
   UUID: Group UUID (reliable view identification)
   Data: Metadata + synced members only
   
3. DETAILED SST (Reliable ID, Full Context)
   Pattern: Recovered view from SST: id: group_uuid:seq  
   UUID: Group UUID (reliable view identification)
   Data: Metadata + all recovered members
```

### 17.3 Implementation Strategy Decision Matrix

| **Parsing Goal** | **Compact Format** | **Detailed Standard** | **Detailed SST** |
|---|---|---|---|
| **View ID Correlation** | ❌ Unreliable | ✅ Reliable | ✅ Reliable |
| **Membership Changes** | ✅ Complete | ❌ Partial | ✅ Complete |
| **Node Identity** | ⚠️ Short UUID only | ✅ Long UUID + Name | ✅ Long UUID + Name |
| **Timeline Building** | ❌ Cannot sequence | ✅ Accurate | ✅ Accurate |
| **Split-Brain Detection** | ✅ Partition events | ❌ Limited | ✅ Full context |

**Recommended Strategy**:
1. **Primary Parsing**: Focus on detailed formats for reliable view tracking
2. **Secondary Analysis**: Extract membership events from compact format (uncorrelated)
3. **Documentation**: Clearly document correlation limitations
4. **Future Enhancement**: Framework ready for improved correlation methods

### 17.4 Technical Debt & Future Work

**Current Limitations**:
- Galera logging inconsistency prevents perfect view correlation
- Timeline gaps where membership events cannot be linked to views
- Reduced forensic capability for certain troubleshooting scenarios

**Potential Solutions** (Future Research):
1. **Temporal Correlation**: Use timestamps to approximate compact→detailed view linking
2. **Pattern Analysis**: Statistical correlation based on membership patterns  
3. **Multi-Log Analysis**: Cross-reference multiple node logs for correlation
4. **Upstream Fix**: Advocate for Galera logging consistency improvement

**Documentation Status**: **COMPREHENSIVE ANALYSIS COMPLETE**

---

**Implementation Status**: ✅ **SPECIFICATION COMPLETE WITH REALISTIC VIEW PARSING STRATEGY**  
**Ready for**: Phase 2A Implementation (Enhanced View Parsing & State Tracking)  
**Timeline**: 12-14 weeks for complete Phase 2 implementation  
**Dependencies**: Phase 1 (✅ COMPLETE) + Technical Architecture Review

---

**Document Status**: APPROVED FOR IMPLEMENTATION - REALISTIC VIEW PARSING STRATEGY  
**Last Updated**: September 25, 2025 - Critical View Parsing Research & Strategy Revision  
**Implementation Start**: September 23, 2025  
**Research Phase**: September 25, 2025 - Galera View Logging Analysis Complete  
**Owner**: Grambo Development Team