# CLUSTER_ENTITY Reference Documentation

**Entity Type**: CLUSTER  
**Purpose**: Root entity representing a complete Galera cluster with all hierarchical components  
**Relationship**: Parent entity containing all other entities as properties with proper hierarchy  
**Data Source**: Comprehensive cluster analysis integrating all entity patterns and split-brain scenarios  

---

## 1. ENTITY OVERVIEW

### 1.1 Core Definition
- **CLUSTER** is the root entity representing a complete Galera cluster instance
- **Hierarchical Container**: All other entities exist as properties within the cluster
- **Multi-Perspective Reality**: Cluster state may be perceived differently by different nodes
- **Split-Brain Awareness**: Multiple simultaneous cluster views possible during network partitions

### 1.2 Key Architectural Concepts

#### 1.2.1 Cluster as Hierarchical Root
```
CLUSTER {
  ├── cluster_uuid: <group_uuid>
  ├── cluster_name: <string>
  ├── views: Collection<VIEW> 
  ├── nodes: Collection<NODE>
  ├── members: Collection<MEMBER>
  ├── state_transfers: Collection<STATE_TRANSFER>
  ├── cluster_states: Collection<CLUSTER_STATE>
  └── timeline: OrderedCollection<EVENT>
}
```

#### 1.2.2 VIEW Perspective Concept - CRITICAL DISCOVERY
**FUNDAMENTAL PRINCIPLE**: A VIEW is always from a specific node's perspective

```log
# Same cluster moment, different node perspectives:
Node UAT-DB-01 perspective:
  View: id: 378cdc73-9236-11f0-a8d4-426872f4d003:1778
  own_index: 1
  members(2): [0: UAT-DB-03, 1: UAT-DB-01]  # This node sees itself at index 1

Node UAT-DB-03 perspective:  
  View: id: 378cdc73-9236-11f0-a8d4-426872f4d003:1778
  own_index: 0
  members(2): [0: UAT-DB-03, 1: UAT-DB-01]  # This node sees itself at index 0
```

#### 1.2.3 Split-Brain Multi-View Reality
**Split-Brain Scenario**: Two nodes, network partition
```
PARTITION A (Node1 perspective):
  View: id: 378cdc73-9236-11f0-a8d4-426872f4d003:1778
  status: non-primary
  members(1): [0: node1-uuid, node1-hostname]

PARTITION B (Node2 perspective):
  View: id: 378cdc73-9236-11f0-a8d4-426872f4d003:1778  # Same sequence!
  status: non-primary  
  members(1): [0: node2-uuid, node2-hostname]
```

---

## 2. CLUSTER ENTITY STRUCTURE

### 2.1 Core Cluster Properties
```python
class ClusterEntity:
    def __init__(self):
        # Core Identity
        self.cluster_uuid = None           # Primary cluster identifier (group UUID)
        self.cluster_name = None           # Human-readable cluster name
        self.cluster_size = None           # Expected/configured cluster size
        
        # Operational Properties  
        self.bootstrap_timestamp = None    # Cluster creation time
        self.last_activity_timestamp = None # Most recent cluster activity
        self.current_status = None         # HEALTHY, DEGRADED, SPLIT_BRAIN, DOWN
        self.quorum_status = None          # PRIMARY, NON_PRIMARY, PARTITIONED
        
        # Configuration Properties
        self.wsrep_protocol_version = None # WSREP protocol version
        self.galera_version = None         # Galera version string
        self.mariadb_version = None        # MariaDB/MySQL version
        
        # Hierarchical Entity Collections
        self.views = ViewCollection()           # All cluster views from all perspectives
        self.nodes = NodeCollection()           # All nodes that have participated
        self.members = MemberCollection()       # All member instances over time
        self.state_transfers = StateTransferCollection()  # All SST/IST operations
        self.cluster_states = ClusterStateCollection()    # All cluster state changes
        
        # Timeline and Correlation
        self.timeline = TimelineCollection()    # Chronological event sequence
        self.correlation_matrix = {}            # Entity cross-references
```

### 2.2 VIEW Collection with Perspective Awareness
```python
class ViewCollection:
    def __init__(self):
        self.views_by_node = {}        # {node_uuid: [ViewEntity]}
        self.views_by_sequence = {}    # {sequence_number: [ViewEntity]}  
        self.consensus_views = {}      # {sequence: ViewEntity} - agreed views
        self.split_brain_views = {}    # {sequence: [ViewEntity]} - conflicting views
        
    def add_view(self, view_entity, observer_node_uuid):
        """Add view from specific node's perspective"""
        pass
        
    def detect_split_brain(self, sequence_number):
        """Identify when multiple nodes see different views for same sequence"""
        pass
        
    def get_consensus_view(self, sequence_number):
        """Return agreed-upon view if consensus exists"""
        pass
```

### 2.3 Node-Centric View Analysis
**Critical Implementation**: Each VIEW must track its observer node

```python
class ViewEntity:
    def __init__(self):
        # Existing properties...
        self.observer_node_uuid = None     # CRITICAL: Which node reported this view
        self.observer_hostname = None      # Human-readable node identification
        self.perspective_timestamp = None  # When this node observed this view
        
    def is_split_brain_indicator(self, other_view):
        """Compare with other view to detect split-brain conditions"""
        return (self.sequence == other_view.sequence and 
                self.observer_node_uuid != other_view.observer_node_uuid and
                self.members != other_view.members)
```

---

## 3. CLUSTER HIERARCHY RELATIONSHIPS

### 3.1 Parent-Child Entity Relationships
```
CLUSTER (Root)
├── VIEW Collection
│   ├── VIEW[node1_perspective] 
│   │   ├── MEMBER[0] → NODE[uuid1]
│   │   └── MEMBER[1] → NODE[uuid2]
│   └── VIEW[node2_perspective]
│       ├── MEMBER[0] → NODE[uuid2]  # Different perspective!
│       └── MEMBER[1] → NODE[uuid1]
├── NODE Collection
│   ├── NODE[uuid1] → {hostname, state_history, view_participations}
│   └── NODE[uuid2] → {hostname, state_history, view_participations}
├── MEMBER Collection  
│   ├── MEMBER[temporal_instance_1] → NODE[uuid1]
│   └── MEMBER[temporal_instance_2] → NODE[uuid2]
└── STATE_TRANSFER Collection
    ├── SST[donor: NODE[uuid1], joiner: NODE[uuid2]]
    └── IST[donor: NODE[uuid2], joiner: NODE[uuid1]]
```

### 3.2 Cross-Entity Correlation Matrix
**Cluster-Level Correlation Registry**:
```python
class ClusterCorrelationMatrix:
    def __init__(self):
        self.uuid_to_node = {}         # {node_uuid: NodeEntity}
        self.hostname_to_node = {}     # {hostname: NodeEntity}  
        self.member_index_to_node = {} # {(view_sequence, index): NodeEntity}
        self.view_perspectives = {}    # {(sequence, node_uuid): ViewEntity}
        
    def correlate_member_to_node(self, member_op, timestamp):
        """Link member operation to node via view perspective correlation"""
        pass
        
    def resolve_split_brain_reality(self, sequence):
        """Determine actual cluster state during split-brain"""
        pass
```

---

## 4. CLUSTER STATE DETECTION

### 4.1 Cluster Health Status Classifications
```python
CLUSTER_STATUS = {
    'HEALTHY': {
        'criteria': 'All nodes agree on view, primary status',
        'indicators': ['primary status', 'consensus views', 'all members synced']
    },
    'DEGRADED': {
        'criteria': 'Some nodes offline but quorum maintained',
        'indicators': ['primary status', 'reduced member count', 'recent departures']
    },
    'SPLIT_BRAIN': {
        'criteria': 'Multiple conflicting views for same sequence',
        'indicators': ['non-primary status', 'multiple perspectives', 'partition evidence']
    },
    'PARTITIONED': {
        'criteria': 'Network partition with unclear winner',
        'indicators': ['multiple non-primary views', 'timestamp gaps', 'member subsets']
    },
    'DOWN': {
        'criteria': 'No recent activity or all nodes offline',
        'indicators': ['old timestamps', 'no recent views', 'member departures only']
    }
}
```

### 4.2 Split-Brain Detection Algorithm
```python
def detect_split_brain_conditions(cluster):
    """Analyze cluster for split-brain scenarios"""
    split_brain_sequences = []
    
    for sequence in cluster.views.views_by_sequence:
        views_for_sequence = cluster.views.views_by_sequence[sequence]
        
        if len(views_for_sequence) > 1:
            # Multiple views for same sequence - potential split-brain
            unique_memberships = set()
            for view in views_for_sequence:
                membership_signature = frozenset(view.members.items())
                unique_memberships.add(membership_signature)
                
            if len(unique_memberships) > 1:
                # Different membership = confirmed split-brain
                split_brain_sequences.append(sequence)
                
    return split_brain_sequences
```

---

## 5. CLUSTER TIMELINE RECONSTRUCTION

### 5.1 Multi-Perspective Timeline Integration
**Challenge**: Reconcile different node perspectives into unified timeline

```python
class ClusterTimelineReconstructor:
    def integrate_node_perspectives(self, node_logs):
        """Combine multiple node perspectives into coherent cluster timeline"""
        
        # Step 1: Collect all events with timestamps and observer nodes
        all_events = []
        for node_uuid, events in node_logs.items():
            for event in events:
                event.observer_node = node_uuid
                all_events.append(event)
                
        # Step 2: Sort by timestamp
        all_events.sort(key=lambda e: e.timestamp)
        
        # Step 3: Detect and resolve perspective conflicts
        consensus_timeline = []
        conflicting_events = []
        
        for event in all_events:
            if self.has_perspective_conflict(event, consensus_timeline):
                conflicting_events.append(event)
            else:
                consensus_timeline.append(event)
                
        return consensus_timeline, conflicting_events
```

### 5.2 Cluster Bootstrap Detection
**Cluster Initialization Patterns**:
```log
# Bootstrap sequence:
first view: 378cdc73-9236-11f0-a8d4-426872f4d003 my  # Initial node
members(1): [0: bootstrap_node_uuid, hostname]

# Cluster growth:  
members(2): [0: original_node, 1: joining_node]      # Second node joins
members(3): [0: node1, 1: node2, 2: node3]          # Third node joins
```

---

## 6. CLUSTER PROPERTIES DISCOVERY

### 6.1 Derived Cluster Properties
**Properties Calculated from Entity Analysis**:

```python
class ClusterAnalytics:
    def calculate_cluster_metrics(self, cluster):
        return {
            # Size and Growth
            'current_size': len(cluster.get_current_nodes()),
            'max_size_reached': max(view.member_count for view in cluster.views),
            'growth_timeline': self.calculate_size_changes_over_time(),
            
            # Stability Metrics
            'view_change_frequency': len(cluster.views) / cluster.uptime_hours,
            'split_brain_episodes': len(cluster.detect_split_brain_periods()),
            'member_churn_rate': self.calculate_member_turnover(),
            
            # Performance Indicators  
            'sst_frequency': len(cluster.state_transfers.sst_operations),
            'ist_frequency': len(cluster.state_transfers.ist_operations),
            'avg_sync_time': self.calculate_average_sync_duration(),
            
            # Health Indicators
            'quorum_loss_episodes': self.count_non_primary_periods(),
            'consensus_agreement_rate': self.calculate_view_consensus_rate(),
            'network_partition_resilience': self.assess_partition_handling()
        }
```

### 6.2 Cluster Configuration Properties  
**Properties Extracted from Log Analysis**:

```python
# From capability analysis in views:
capabilities = [
    'MULTI-MASTER', 'CERTIFICATION', 'PARALLEL_APPLYING', 
    'REPLAY', 'ISOLATION', 'PAUSE', 'CAUSAL_READ', 
    'INCREMENTAL_WS', 'UNORDERED', 'PREORDERED', 'STREAMING', 'NBO'
]

# From protocol version analysis:
protocol_versions = [4]  # Most common WSREP protocol version

# From UUID pattern analysis:
cluster_naming_patterns = [
    'environment_based',    # UAT-DB-01, UAT-DB-03
    'port_based',          # NODE_54321, NODE_54320  
    'infrastructure_based' # vinfr-db-d-l05, vinfr-db-d-d01
]
```

---

## 7. IMPLEMENTATION ARCHITECTURE

### 7.1 Cluster Factory Pattern
```python
class ClusterEntityFactory:
    def create_from_logs(self, log_files):
        """Create cluster entity from multiple node log files"""
        cluster = ClusterEntity()
        
        # Step 1: Extract individual node perspectives
        node_perspectives = {}
        for log_file in log_files:
            node_uuid = self.extract_node_identity(log_file)
            perspectives = self.extract_node_perspective(log_file, node_uuid)
            node_perspectives[node_uuid] = perspectives
            
        # Step 2: Build cluster from perspectives
        cluster = self.build_cluster_from_perspectives(node_perspectives)
        
        # Step 3: Resolve conflicts and detect split-brains
        cluster.resolve_perspective_conflicts()
        cluster.detect_split_brain_periods()
        
        return cluster
```

### 7.2 Perspective Resolution Engine
```python
class PerspectiveResolutionEngine:
    def resolve_view_conflicts(self, conflicting_views):
        """Determine authoritative view from conflicting node perspectives"""
        
        # Resolution strategies:
        # 1. Majority consensus (most nodes agree)
        # 2. Quorum-based (primary status nodes take precedence)  
        # 3. Temporal proximity (closest timestamps)
        # 4. Configuration authority (bootstrap node precedence)
        
        pass
```

---

## 8. IMPLEMENTATION PRIORITIES

### 8.1 Phase 1: Core Cluster Architecture (IMMEDIATE)
- [ ] Implement ClusterEntity as root hierarchical container
- [ ] Build ViewCollection with perspective awareness
- [ ] Create node-perspective view correlation system  
- [ ] Establish split-brain detection algorithms

### 8.2 Phase 2: Multi-Perspective Integration (SHORT-TERM)
- [ ] Implement perspective resolution engine for conflicting views
- [ ] Build cluster timeline reconstruction from multiple node logs
- [ ] Create cluster health status detection and monitoring
- [ ] Develop cluster analytics and metrics calculation

### 8.3 Phase 3: Advanced Cluster Analysis (LONG-TERM)  
- [ ] Implement cluster optimization recommendations
- [ ] Build predictive cluster failure analysis
- [ ] Create cluster topology visualization with perspective awareness
- [ ] Develop cluster benchmarking and performance correlation

---

## 9. CRITICAL IMPLEMENTATION NOTES

### 9.1 View Perspective Requirements
**MANDATORY**: Every VIEW entity MUST include:
```python
view.observer_node_uuid = "uuid-of-node-that-reported-this-view"
view.observer_hostname = "hostname-of-observer-node"  
view.perspective_timestamp = timestamp_when_observed
```

### 9.2 Split-Brain Handling Strategy
**ESSENTIAL**: Cluster must handle multiple concurrent realities:
- **Store ALL perspectives** (don't choose winners prematurely)
- **Detect conflicts** via sequence+membership analysis  
- **Resolve when possible** via consensus algorithms
- **Flag unresolvable** conflicts for human analysis

### 9.3 Entity Ownership Hierarchy
**STRICT HIERARCHY**: All entities belong to cluster:
```python
# CORRECT:
cluster.nodes["uuid"].member_operations  
cluster.views[sequence].membership[index]

# INCORRECT: 
standalone_node.operations  # Nodes don't exist independently
```

---

## 10. RESEARCH CONCLUSIONS

### 10.1 Critical Architectural Discoveries
1. **CLUSTER is the authoritative root entity** - all other entities are properties
2. **VIEW perspective is fundamental** - each view represents one node's reality  
3. **Split-brain requires multi-perspective storage** - single truth assumption fails
4. **Hierarchy enables correlation** - parent-child relationships solve identity problems
5. **Timeline reconstruction needs conflict resolution** - multiple realities must be reconciled

### 10.2 Implementation Readiness
- ✅ **Hierarchical architecture defined** with proper parent-child relationships
- ✅ **View perspective framework** established for split-brain handling
- ✅ **Cluster properties identified** from comprehensive entity analysis  
- ✅ **Multi-perspective integration strategy** designed for conflict resolution

### 10.3 Next Steps
1. **Implement ClusterEntity as hierarchical root** with all entity collections as properties
2. **Build perspective-aware ViewCollection** with split-brain detection capabilities  
3. **Create cluster timeline reconstruction** engine integrating multiple node perspectives
4. **Develop cluster health analytics** using derived properties from entity relationships

---

**Document Status**: ✅ **ARCHITECTURAL DESIGN COMPLETE - READY FOR IMPLEMENTATION**  
**Evidence Base**: Integration of all entity analysis (UUID, MEMBER, VIEW) + split-brain scenario analysis  
**Primary Use Case**: Root entity architecture for comprehensive Galera cluster analysis  
**Confidence Level**: HIGH - Based on hierarchical entity relationship analysis and split-brain reality recognition  
**Last Updated**: September 25, 2025