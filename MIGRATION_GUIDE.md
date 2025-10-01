# Migration Guide: Old Pattern System → Schema-Based Architecture

## Overview

This guide helps migrate from the current pattern matching system to the new schema-based architecture.

## Key Differences

### Old System
```yaml
# patterns/node_patterns.yaml
patterns:
  NODE:
    - name: "node_state_change"
      regex: '...'
      field_mappings:
        node_uuid: "node_id"
```

**Issues:**
- Mixed entity definition and pattern matching
- No clear separation between CORE and TEMPORAL entities
- Pattern context not explicit
- Entity actions implicit in code
- Relationship management in code

### New System
```yaml
# schema/entity_schema.yaml - Entities defined separately
core_entities:
  Node:
    attributes:
      node_uuid: ...

# schema/patterns.yaml - Patterns reference entities
- pattern_id: "node_state_change"
  entity_target: "NodeStateChange"
  context: LOCAL
  entity_action:
    action_type: "CREATE_TEMPORAL"
```

**Benefits:**
- Clear separation of concerns
- Explicit CORE vs TEMPORAL entities
- Context (LOCAL/GLOBAL/PEER) defined
- Entity actions declarative
- Relationships in schema

## Migration Steps

### Step 1: Identify Entity Types

Review your current entities and classify them:

**CORE Entities** (Immutable base):
- Cluster
- Node
- (Any other entities that represent base objects)

**TEMPORAL Entities** (Time-based events):
- NodeStateChange
- ClusterView
- StateTransfer
- QuorumEvent
- ErrorEvent
- FlowControlEvent
- (Any events that occur over time)

### Step 2: Define Entity Schemas

For each entity, create schema definition:

```yaml
# Example: Node (CORE entity)
core_entities:
  Node:
    description: "Represents a cluster node"
    immutable: true
    unique_keys:
      - node_uuid
      - [node_name, cluster_uuid]  # Composite key
    attributes:
      node_uuid:
        type: string
        required: true
        pattern: "^[a-f0-9-]+$"
      node_name:
        type: string
        required: false
      # ... other attributes
    relationships:
      - type: belongs_to
        target: Cluster
      - type: has_many
        target: NodeStateChange

# Example: NodeStateChange (TEMPORAL entity)
temporal_entities:
  NodeStateChange:
    description: "Records a node state transition"
    entity_type: EVENT
    time_series: true
    attributes:
      timestamp:
        type: datetime
        required: true
      from_state:
        type: enum
        values: [OPEN, PRIMARY, JOINER, ...]
      to_state:
        type: enum
        required: true
        values: [OPEN, PRIMARY, JOINER, ...]
    relationships:
      - type: belongs_to
        target: Node
        foreign_key: node_uuid
```

### Step 3: Convert Patterns

For each pattern in your old system:

**Old Pattern:**
```yaml
- name: "node_state_change"
  regex: '(?P<timestamp>...) (?P<node_uuid>...) state change: (?P<from_state>...) -> (?P<to_state>...)'
  field_mappings:
    node_uuid: "node_id"
    from_state: "previous_state"
```

**New Pattern:**
```yaml
- pattern_id: "node_state_transition"
  entity_target: "NodeStateChange"
  description: "Captures node state transitions"
  
  regex: '(?P<timestamp>...) (?P<node_uuid>...) state change: (?P<from_state>...) -> (?P<to_state>...)'
  
  context: LOCAL  # Important: specify context
  confidence: 0.95
  
  extraction_mapping:
    - match_group: "timestamp"
      extraction_fn: "transform"
      transform: "parse_datetime"
      target_field: "timestamp"
    
    - match_group: "node_uuid"
      extraction_fn: "direct_mapping"
      target_field: "node_uuid"
    
    - match_group: "from_state"
      extraction_fn: "transform"
      transform: "normalize_state"
      target_field: "from_state"
    
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
        source_field: "node_uuid"
        auto_create_parent: true  # Create Node if doesn't exist
```

### Step 4: Determine Pattern Context

For each pattern, determine its context:

**LOCAL Context:**
- Information about the local node (log source)
- Examples: "Server X synced", "Local state change"
- Rule: If it says "Server" or uses "My", it's LOCAL

**GLOBAL Context:**
- Cluster-wide information
- Examples: "Cluster UUID", "View change"
- Rule: If visible from all nodes, it's GLOBAL

**PEER Context:**
- Information about other nodes
- Examples: "Donor node X", "Member disconnected"
- Rule: If it references another node, it's PEER

### Step 5: Map Entity Actions

Determine what action each pattern should trigger:

| Pattern Type | Entity Action | When to Use |
|-------------|---------------|-------------|
| First node detection | CREATE_CORE (Node) | When node UUID or name first seen |
| Node enrichment | UPDATE_CORE (Node) | Adding address, version info |
| State change | CREATE_TEMPORAL (NodeStateChange) | Every state transition |
| SST start | CREATE_TEMPORAL (StateTransfer) | Start of SST |
| SST progress | UPDATE_TEMPORAL (StateTransfer) | Progress updates |
| Error event | CREATE_TEMPORAL (ErrorEvent) | Each error occurrence |

### Step 6: Add Validation Rules

Add validation rules to patterns:

```yaml
validation_rules:
  - rule: "required_fields"
    fields: ["timestamp", "node_uuid", "to_state"]
  
  - rule: "enum_check"
    field: "to_state"
    allowed_values: ["OPEN", "PRIMARY", "JOINER", "JOINED", "SYNCED"]
  
  - rule: "pattern_match"
    field: "node_uuid"
    pattern: "^[a-f0-9-]+$"
```

### Step 7: Test Migration

1. **Create test log snippets** for each pattern
2. **Run extraction** on test logs
3. **Verify entities** are created correctly
4. **Check relationships** are established
5. **Validate output** JSON structure

```bash
# Run test extraction
python3 test_schema_extraction.py

# Process actual log file
python3 -c "
from pathlib import Path
from lib.schema_engine import SchemaBasedExtractor
import json

extractor = SchemaBasedExtractor(Path('schema'))
entities = extractor.process_log_file(Path('your_log.log'))

with open('output.json', 'w') as f:
    json.dump(entities, f, indent=2, default=str)
"
```

## Common Migration Patterns

### Pattern 1: Simple Event Extraction

**Old:**
```python
def extract_state_change(line):
    match = re.match(pattern, line)
    if match:
        return {
            'timestamp': match.group('timestamp'),
            'state': match.group('state')
        }
```

**New:**
```yaml
- pattern_id: "state_change"
  regex: '(?P<timestamp>...) (?P<state>...)'
  extraction_mapping:
    - match_group: "timestamp"
      extraction_fn: "transform"
      transform: "parse_datetime"
      target_field: "timestamp"
    - match_group: "state"
      extraction_fn: "direct_mapping"
      target_field: "state"
```

### Pattern 2: Multi-field Entity

**Old:**
```python
node = Node(
    uuid=extract_uuid(line),
    name=extract_name(line),
    address=extract_address(line)
)
registry.add(node)
```

**New:**
```yaml
- pattern_id: "node_full_info"
  entity_action:
    action_type: "CREATE_CORE"
    entity_type: "Node"
    unique_keys: ["node_uuid"]
  extraction_mapping:
    - match_group: "uuid"
      target_field: "node_uuid"
    - match_group: "name"
      target_field: "node_name"
    - match_group: "address"
      target_field: "node_address"
```

### Pattern 3: Entity with Parent Reference

**Old:**
```python
state_change = StateChange(
    timestamp=ts,
    node_id=current_node.id
)
```

**New:**
```yaml
- pattern_id: "state_change"
  entity_action:
    action_type: "CREATE_TEMPORAL"
    entity_type: "NodeStateChange"
    parent_refs:
      - entity_type: "Node"
        foreign_key: "node_uuid"
        source_field: "node_uuid"
```

## Backward Compatibility

To maintain backward compatibility during migration:

1. **Keep old system running** alongside new system
2. **Compare outputs** to ensure equivalence
3. **Gradual migration** of patterns
4. **Fallback mechanism** for unmatched patterns

Example wrapper:

```python
class HybridExtractor:
    def __init__(self):
        self.old_extractor = OldPatternMatcher()
        self.new_extractor = SchemaBasedExtractor(Path('schema'))
    
    def extract(self, log_file):
        # Try new system first
        try:
            return self.new_extractor.process_log_file(log_file)
        except Exception as e:
            logging.warning(f"New extractor failed: {e}, falling back")
            return self.old_extractor.extract(log_file)
```

## Troubleshooting

### Issue: Pattern not matching

**Check:**
1. Regex is correct
2. Pattern is compiled
3. Pattern is in patterns list
4. Line format matches expected

**Debug:**
```python
pattern = extractor.schema_loader.patterns[0]
match = pattern.match(your_line)
if not match:
    print("No match")
else:
    print(f"Matched: {match.groups()}")
```

### Issue: Entity not created

**Check:**
1. Extraction mapping is correct
2. Required fields are extracted
3. Validation rules pass
4. Parent entities exist (for TEMPORAL)

**Debug:**
```python
# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
```

### Issue: Wrong entity type

**Check:**
1. Entity is CORE or TEMPORAL as expected
2. `entity_action.action_type` is correct
3. Schema definition matches pattern

### Issue: Relationships not working

**Check:**
1. Foreign key fields are extracted
2. Parent entity exists
3. Relationship defined in schema
4. `parent_refs` configured correctly

## Performance Optimization

### 1. Pattern Ordering
Sort patterns by:
- Confidence (highest first)
- Frequency (most common first)
- Specificity (most specific first)

### 2. Regex Optimization
- Use non-capturing groups: `(?:...)` instead of `(...)`
- Anchor patterns: `^pattern$` when possible
- Compile once, reuse many times

### 3. Batch Processing
```python
# Process multiple files
for log_file in log_files:
    entities = extractor.process_log_file(log_file)
    # Store incrementally
```

### 4. Incremental Output
```python
# Stream results instead of loading all in memory
def process_streaming(log_file, output_stream):
    for entity in extractor.extract_streaming(log_file):
        json.dump(entity, output_stream)
        output_stream.write('\n')
```

## Best Practices

1. **Start simple**: Migrate high-confidence patterns first
2. **Test thoroughly**: Use real log samples
3. **Document patterns**: Add examples and descriptions
4. **Version control**: Track schema changes
5. **Monitor performance**: Profile extraction time
6. **Validate output**: Check entity counts and relationships
7. **Iterate**: Refine patterns based on results

## Next Steps

1. ✅ Define entity schemas
2. ✅ Convert existing patterns
3. ✅ Add validation rules
4. ✅ Test with sample logs
5. ⬜ Process production logs
6. ⬜ Compare with old system
7. ⬜ Tune performance
8. ⬜ Document learnings
9. ⬜ Deprecate old system

## Resources

- **Entity Schema**: `schema/entity_schema.yaml`
- **Pattern Schema**: `schema/pattern_schema.yaml`
- **Pattern Definitions**: `schema/patterns.yaml`
- **Implementation**: `lib/schema_engine.py`
- **Test Suite**: `test_schema_extraction.py`
- **Architecture Doc**: `SCHEMA_ARCHITECTURE.md`
