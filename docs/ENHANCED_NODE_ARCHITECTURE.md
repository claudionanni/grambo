# Enhanced Node Entity Architecture Implementation

## Overview

This implementation provides a proper hierarchical entity architecture for NODE, NODE_STATE, and CLUSTER entities as requested. The current NODE entity was indeed acting as a NODE_STATE entity, capturing state transitions rather than the physical nodes themselves.

## New Entity Architecture

### 1. CLUSTER Entity (`EntityType.CLUSTER`)
- **Purpose**: Root entity representing the Galera cluster itself
- **Attributes**:
  - `cluster_uuid`: 8-character cluster UUID
  - `cluster_name`: wsrep_cluster_name 
  - `provider_version`: Galera version
  - `known_nodes`: Set of node UUIDs that have been part of cluster
  - `active_nodes`: Currently active node UUIDs
  - `current_seqno`: Latest sequence number
  - `is_primary`: Whether cluster is in PRIMARY state

### 2. NODE Entity (`EntityType.NODE`) 
- **Purpose**: Physical cluster node with persistent attributes
- **Attributes**:
  - `node_uuid`: Full 36-character UUID
  - `short_uuid`: 8-4 character format (auto-generated)
  - `node_name`: Human-readable name (from logs)
  - `node_ip`: Primary IP address
  - `mariadb_port`: MariaDB service port (default 3306)
  - `wsrep_port_out`: Galera outgoing port (default 4567)
  - `wsrep_port_in`: Galera incoming port (usually out+1)
  - `sst_port`: SST port (default 4444)
  - `server_version`: MariaDB version
  - `cluster_uuid`: Foreign key to CLUSTER entity
  - `last_known_state`: Current state (for compatibility)

### 3. NODE_STATE Entity (`EntityType.NODE_STATE`)
- **Purpose**: State changes with timestamps and foreign key to NODE
- **Attributes**:
  - `node_uuid`: Foreign key reference to NODE entity
  - `from_state`: Previous state
  - `to_state`: New state  
  - `total_order`: TO value at state change
  - `sequence_number`: Seqno at state change
  - `view_id`: View ID when state changed
  - `transition_reason`: Why state changed
  - `transition_duration`: Time taken for transition

## Implementation Files

### Core Files Created/Modified

1. **`lib/entities/enhanced_nodes.py`** - New entity classes
2. **`patterns/enhanced_node_patterns.yaml`** - New patterns for proper entity extraction
3. **`lib/entities/base.py`** - Added new EntityType enum values
4. **`lib/entities/core.py`** - Updated registration to use enhanced entities
5. **`lib/entities/__init__.py`** - Updated imports and exports

### Pattern Structure

The new patterns properly separate:
- **CLUSTER patterns**: Extract cluster-wide information (UUID, name, provider info)
- **NODE patterns**: Extract persistent node attributes (UUID, IP, ports, name)
- **NODE_STATE patterns**: Extract state transitions with foreign key references

## Integration Status

### ✅ Completed
- New entity classes implemented with proper validation
- Pattern definitions created for all three entity types
- Entity type enum updated with new types
- Registration system updated to use enhanced entities
- Backward compatibility maintained for existing SST+IST functionality

### 🔄 Integration Required
To activate the enhanced entity extraction, the following integration steps are needed:

1. **Pattern Loading**: Configure grap to load `enhanced_node_patterns.yaml`
2. **Entity Factory**: Update entity creation to use new enhanced classes
3. **Foreign Key Resolution**: Implement NODE_UUID → NODE_STATE relationship resolution
4. **Migration Path**: Provide compatibility layer for existing tools

### 🎯 Benefits Once Integrated

1. **Proper Node Tracking**: Each physical node tracked once with persistent attributes
2. **State History**: Complete state evolution timeline for each node
3. **Relationship Queries**: Query node states at any timestamp
4. **Cluster Overview**: Full cluster topology with member tracking
5. **Better Analytics**: Proper foreign key relationships enable complex queries

## Compatibility

### Existing Functionality Preserved
- ✅ SST+IST tree visualization continues to work
- ✅ All existing graa functionality maintained
- ✅ JSON output format compatible
- ✅ Entity filtering still works

### Migration Strategy
The implementation uses a fallback approach:
1. Try to load enhanced entities first
2. Fall back to legacy NODE entity if enhanced entities unavailable
3. Gradual migration path without breaking existing tools

## Usage Examples (Once Integrated)

```bash
# Extract all entity types including enhanced nodes
./grap logfile.log --entities=CLUSTER,NODE,NODE_STATE,SST,IST --format=json

# Show cluster topology
./grap logfile.log --entities=CLUSTER,NODE --format=json | jq '.entities[] | select(.entity_type == "CLUSTER" or .entity_type == "NODE")'

# Track state evolution for specific node
./grap logfile.log --entities=NODE_STATE --format=json | jq '.entities[] | select(.node_uuid == "specific-uuid")'

# Show node states at specific timestamp
./grap logfile.log --entities=NODE_STATE --format=json | jq '.entities[] | select(.timestamp >= "2025-09-25T10:00:00")'
```

## Next Steps

To complete the integration:

1. **Activate Patterns**: Configure pattern loader to include enhanced_node_patterns.yaml
2. **Test Extraction**: Verify entities are extracted correctly from real logs
3. **Update graa**: Enhance graa to leverage the new entity relationships
4. **Add Queries**: Implement relationship queries (node states at timestamp, etc.)
5. **Documentation**: Update user documentation with new entity types

The foundation is complete and backward-compatible. The enhanced architecture will provide much richer analysis capabilities once the pattern loading is configured.