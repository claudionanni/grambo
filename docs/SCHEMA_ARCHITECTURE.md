# Schema-Driven Entity Extraction Architecture

## Overview

This document describes the new deterministic pattern matching and entity extraction architecture for Grambo. The architecture is based on clearly defined schemas that separate concerns between:

1. **Entity definitions** (what we extract)
2. **Pattern definitions** (how we extract it)
3. **Extraction engine** (the implementation)

## Architecture Principles

### Deterministic Extraction
- Every pattern has a clear, unambiguous definition
- Extraction logic is data-driven, not code-driven
- Changes to patterns don't require code changes
- Testable and reproducible results

### Entity Model: CORE vs TEMPORAL

#### CORE Entities
**Immutable base entities** that represent the foundational structure of the cluster. These are created once and persist throughout the timeline.

- **Cluster**: Represents the Galera cluster itself
- **Node**: Represents individual servers in the cluster

CORE entities:
- Have unique identifiers (UUIDs)
- Are idempotent (creating multiple times has same effect)
- Can be enriched with additional data over time
- Form the relational backbone

#### TEMPORAL Entities
**Time-based events** that reference CORE entities. These track the evolution and state changes over time.

- **NodeStateChange**: State transitions for nodes
- **ClusterView**: Membership changes
- **StateTransfer**: SST/IST operations
- **QuorumEvent**: Quorum loss/restoration
- **ErrorEvent**: Error occurrences
- **FlowControlEvent**: Flow control events

TEMPORAL entities:
- Always have a timestamp
- Are not idempotent (each occurrence creates a new entry)
- Reference CORE entities via foreign keys
- Form the timeline

### Relationships

```
Cluster (CORE)
  ├─> has_many: Node (CORE)
  ├─> has_many: ClusterView (TEMPORAL)
  └─> has_many: QuorumEvent (TEMPORAL)

Node (CORE)
  ├─> belongs_to: Cluster
  ├─> has_many: NodeStateChange (TEMPORAL)
  ├─> has_many: StateTransfer as joiner (TEMPORAL)
  ├─> has_many: StateTransfer as donor (TEMPORAL)
  └─> has_many: ErrorEvent (TEMPORAL)

StateTransfer (TEMPORAL)
  ├─> belongs_to: Node (joiner)
  └─> references: Node (donor)
```

## Schema Files

### 1. Entity Schema (`schema/entity_schema.yaml`)

Defines all entity types with:
- Attributes and their types
- Validation rules
- Relationships
- Unique keys for deduplication
- Lifecycle rules

Example:
```yaml
core_entities:
  Node:
    description: "Represents a cluster node"
    immutable: true
    unique_keys:
      - node_uuid
      - [node_name, cluster_uuid]
    attributes:
      node_uuid:
        type: string
        required: true
        pattern: "^[a-f0-9-]+$"
      node_name:
        type: string
        required: false
    relationships:
      - type: belongs_to
        target: Cluster
        foreign_key: cluster_uuid
```

### 2. Pattern Schema (`schema/pattern_schema.yaml`)

Defines the structure of pattern definitions:
- Pattern structure requirements
- Context types (LOCAL, GLOBAL, PEER)
- Extraction functions
- Entity actions
- Validation rules

### 3. Pattern Definitions (`schema/patterns.yaml`)

Contains actual patterns with:

```yaml
- pattern_id: "node_state_transition"
  entity_target: "NodeStateChange"
  regex: '(?P<timestamp>...) ... state change: (?P<from_state>\\w+) -> (?P<to_state>\\w+)'
  context: LOCAL  # This is a local node event
  confidence: 0.95
  
  extraction_mapping:
    - match_group: "timestamp"
      extraction_fn: "transform"
      transform: "parse_datetime"
      target_field: "timestamp"
    
    - match_group: "to_state"
      extraction_fn: "transform"
      transform: "normalize_state"
      target_field: "to_state"
  
  entity_action:
    action_type: "CREATE_TEMPORAL"
    entity_type: "NodeStateChange"
    parent_refs:
      - entity_type: "Node"
        foreign_key: "node_uuid"
        auto_create_parent: true
  
  validation_rules:
    - rule: "required_fields"
      fields: ["timestamp", "node_uuid", "to_state"]
```

## Pattern Context Types

### LOCAL Context
- Pattern extracts data specific to the local node (the node generating this log)
- Example: "Server X synced with group" - X is always the local node
- Entities created are associated with the log source node

### GLOBAL Context
- Pattern extracts cluster-wide information visible from any node
- Example: Cluster UUID, cluster views
- Information is the same regardless of which log it appears in

### PEER Context
- Pattern extracts information about another node seen from local perspective
- Example: Donor node information during SST
- Creates references between local and remote nodes

## Extraction Process

### 1. Pattern Matching
```
Log Line → Try Patterns (by confidence) → First Match → Extract Data
```

Patterns are tried in confidence order (highest first). First match wins.

### 2. Data Extraction

For each matched pattern:
1. Extract regex groups
2. Apply transformation functions
3. Compute derived fields
4. Validate extracted data

### 3. Entity Actions

Based on pattern's `entity_action`:

#### CREATE_CORE
- Create new CORE entity if doesn't exist (based on unique keys)
- If exists, enrich with additional data
- Update context (e.g., local_node_uuid, cluster_uuid)

#### UPDATE_CORE
- Find existing CORE entity by lookup keys
- Update specified attributes
- Fail if entity doesn't exist

#### CREATE_TEMPORAL
- Always create new TEMPORAL entity
- Resolve parent references
- Auto-create parent CORE entities if specified
- Link to parent entities

#### UPDATE_TEMPORAL
- Find recent TEMPORAL entity by lookup keys
- Update attributes (e.g., SST progress)
- Used for multi-line events

## Transformation Functions

Built-in transformations for data extraction:

- `parse_datetime`: Convert timestamp strings to datetime
- `parse_int`, `parse_float`: Numeric conversions
- `parse_boolean`: Boolean conversion
- `to_uppercase`, `to_lowercase`: Case normalization
- `normalize_state`: Normalize Galera state names
- `extract_uuid`: Extract UUID from text
- `extract_seqno_from_view_id`: Parse view_id format

## Usage

### Basic Usage

```python
from pathlib import Path
from lib.schema_engine import SchemaBasedExtractor

# Initialize extractor with schema directory
schema_dir = Path("schema")
extractor = SchemaBasedExtractor(schema_dir)

# Process log file
log_file = Path("galera-node.log")
entities = extractor.process_log_file(log_file)

# Export to JSON
import json
with open("output.json", 'w') as f:
    json.dump(entities, f, indent=2, default=str)
```

### Output Format

```json
{
  "core_entities": {
    "Cluster": {
      "Cluster_b2c3d4e5-f6a7-8901-bcde-234567890123": {
        "entity_id": "Cluster_b2c3d4e5-f6a7-8901-bcde-234567890123",
        "entity_type": "Cluster",
        "cluster_uuid": "b2c3d4e5-f6a7-8901-bcde-234567890123",
        "first_seen": "2024-09-15T10:25:28"
      }
    },
    "Node": {
      "Node_a1b2c3d4-e5f6-7890-abcd-123456789012": {
        "entity_id": "Node_a1b2c3d4-e5f6-7890-abcd-123456789012",
        "entity_type": "Node",
        "node_uuid": "a1b2c3d4-e5f6-7890-abcd-123456789012",
        "node_name": "db-node-01",
        "first_seen": "2024-09-15T10:25:30"
      }
    }
  },
  "temporal_entities": {
    "NodeStateChange": [
      {
        "entity_id": "NodeStateChange_abc123",
        "entity_type": "NodeStateChange",
        "timestamp": "2024-09-15T10:30:45",
        "node_uuid": "a1b2c3d4-e5f6-7890-abcd-123456789012",
        "from_state": "SYNCED",
        "to_state": "DONOR",
        "log_line": "2024-09-15 10:30:45 ... state change: Synced -> Donor",
        "line_number": 12345
      }
    ]
  }
}
```

## Adding New Patterns

To add a new pattern:

1. **Define the entity** (if new) in `schema/entity_schema.yaml`:
   - Choose CORE or TEMPORAL
   - Define attributes with types
   - Define relationships
   - Set validation rules

2. **Add pattern** to `schema/patterns.yaml`:
   - Write regex with named groups
   - Set context (LOCAL/GLOBAL/PEER)
   - Define extraction mapping
   - Define entity action
   - Add validation rules
   - Include examples

3. **Test pattern**:
   - Use examples to verify regex
   - Validate against real log lines
   - Check entity creation/updates

No code changes required!

## Advantages

### 1. Maintainability
- Clear separation of concerns
- Pattern changes don't require code changes
- Easy to understand and modify

### 2. Testability
- Patterns include examples
- Validation rules are explicit
- Deterministic behavior

### 3. Extensibility
- Add new entity types via schema
- Add new patterns via YAML
- Add new transforms as needed

### 4. Documentation
- Schemas serve as documentation
- Self-describing patterns
- Clear relationships

### 5. Robustness
- Validation at multiple levels
- Type checking
- Relationship integrity

## Migration Path

To migrate from current pattern system:

1. **Audit existing patterns**: Review current YAML patterns
2. **Map to entities**: Identify CORE vs TEMPORAL entities
3. **Define schemas**: Create entity schemas
4. **Convert patterns**: Migrate patterns to new format
5. **Test thoroughly**: Validate against test logs
6. **Gradual rollout**: Can coexist with old system

## Future Enhancements

### 1. Pattern Learning
- Suggest patterns from unmatched lines
- Confidence adjustment based on validation

### 2. Multi-line Patterns
- State machines for complex events
- Look-ahead/look-behind for context

### 3. Relationship Inference
- Auto-discover relationships
- Validate relationship integrity

### 4. Timeline Analysis
- Query temporal entities by time range
- Reconstruct cluster state at any point

### 5. Pattern Optimization
- Pattern ordering optimization
- Regex compilation caching
- Parallel processing

## Files Created

```
schema/
  ├── entity_schema.yaml      # Entity definitions (CORE + TEMPORAL)
  ├── pattern_schema.yaml     # Pattern structure definition
  └── patterns.yaml           # Actual pattern implementations

lib/
  └── schema_engine.py        # Schema-driven extraction engine
```

## Next Steps

1. **Review schemas**: Validate entity and pattern definitions
2. **Add patterns**: Convert existing patterns to new format
3. **Integration**: Integrate with existing codebase
4. **Testing**: Create comprehensive test suite
5. **Documentation**: Add usage examples and tutorials

---

**Note**: This architecture provides a solid foundation for deterministic, maintainable, and extensible log analysis. The clear separation between schema and implementation enables rapid iteration and easy maintenance.
