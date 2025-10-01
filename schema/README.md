# Schema Directory

This directory contains the schema definitions for the Grambo schema-based entity extraction system.

## Files

### `entity_schema.yaml`
**Complete entity model definition**

Defines all entity types with:
- Attributes and their types
- Validation rules  
- Relationships between entities
- Unique keys for deduplication
- Lifecycle rules

**Entity Categories:**
- **CORE Entities**: Immutable base entities (Cluster, Node)
- **TEMPORAL Entities**: Time-based events (NodeStateChange, ClusterView, StateTransfer, etc.)

### `pattern_schema.yaml`
**Pattern structure specification**

Documents how patterns should be structured:
- Required and optional fields
- Context types (LOCAL, GLOBAL, PEER)
- Extraction functions available
- Entity action types
- Validation rule types
- Dialect variations

This serves as the "meta-schema" - the schema for writing patterns.

### `patterns.yaml`
**Actual pattern implementations**

Contains all pattern definitions for extracting entities from Galera logs:
- 5 CORE entity patterns
- 10 TEMPORAL entity patterns

Each pattern includes:
- Unique pattern ID
- Target entity type
- Regex with named groups
- Context (LOCAL/GLOBAL/PEER)
- Extraction mappings
- Entity actions
- Validation rules
- Examples

## Understanding the Schema System

### Three Layers

```
┌─────────────────────────────────────────────┐
│  pattern_schema.yaml                        │
│  (Meta-schema: How to write patterns)       │
└────────────────┬────────────────────────────┘
                 │ defines structure of
                 ▼
┌─────────────────────────────────────────────┐
│  patterns.yaml                              │
│  (Pattern definitions: What to extract)     │
└────────────────┬────────────────────────────┘
                 │ references
                 ▼
┌─────────────────────────────────────────────┐
│  entity_schema.yaml                         │
│  (Entity definitions: What we create)       │
└─────────────────────────────────────────────┘
```

### Entity Types

#### CORE Entities (Immutable)
Created once, enriched over time, never deleted.

**Cluster**
```yaml
core_entities:
  Cluster:
    unique_keys: [cluster_uuid]
    attributes:
      cluster_uuid: {type: string, required: true}
      cluster_name: {type: string, required: false}
      # ...
```

**Node**
```yaml
core_entities:
  Node:
    unique_keys: [node_uuid, [node_name, cluster_uuid]]
    attributes:
      node_uuid: {type: string, required: true}
      node_name: {type: string, required: false}
      # ...
```

#### TEMPORAL Entities (Time-based)
Created for each occurrence, form the timeline.

**NodeStateChange**
```yaml
temporal_entities:
  NodeStateChange:
    time_series: true
    attributes:
      timestamp: {type: datetime, required: true}
      from_state: {type: enum, values: [SYNCED, DONOR, ...]}
      to_state: {type: enum, required: true}
    relationships:
      - {type: belongs_to, target: Node, foreign_key: node_uuid}
```

### Pattern Structure

A pattern defines how to extract an entity from a log line:

```yaml
- pattern_id: "unique_identifier"
  entity_target: "EntityName"          # Which entity to create
  description: "What this extracts"
  
  regex: '(?P<field1>...) (?P<field2>...)'  # Regex with named groups
  
  context: LOCAL                       # LOCAL, GLOBAL, or PEER
  confidence: 0.95                     # 0.0 to 1.0
  
  extraction_mapping:                  # How to extract data
    - match_group: "field1"
      extraction_fn: "transform"
      transform: "parse_datetime"
      target_field: "timestamp"
    
    - match_group: "field2"
      extraction_fn: "direct_mapping"
      target_field: "node_uuid"
  
  entity_action:                       # What to do with data
    action_type: "CREATE_TEMPORAL"
    entity_type: "NodeStateChange"
    parent_refs:
      - entity_type: "Node"
        foreign_key: "node_uuid"
  
  validation_rules:                    # Validation
    - rule: "required_fields"
      fields: ["timestamp", "node_uuid"]
  
  examples:                            # Test cases
    - input: "2024-09-15 10:30:45 ..."
      expected_extraction: {...}
```

## Context Types

### LOCAL Context
Information about the local node (the node generating this log).

**Example:** "Server X synced with group" - X is always the local node

**Use when:** The log message describes something about the local node itself.

### GLOBAL Context  
Cluster-wide information that's the same regardless of which log it appears in.

**Example:** Cluster UUID, cluster views

**Use when:** The information is global to the cluster.

### PEER Context
Information about another node seen from the local node's perspective.

**Example:** Donor node information during SST

**Use when:** The log message describes another node's action or state.

## Extraction Functions

Built-in transformation functions:

| Function | Description | Example |
|----------|-------------|---------|
| `direct_mapping` | 1:1 field mapping | `match_group → target_field` |
| `parse_datetime` | Parse timestamp | `"2024-09-15 10:30:45" → datetime` |
| `parse_int` | Parse integer | `"42" → 42` |
| `parse_float` | Parse float | `"3.14" → 3.14` |
| `to_uppercase` | Convert to uppercase | `"synced" → "SYNCED"` |
| `to_lowercase` | Convert to lowercase | `"SYNCED" → "synced"` |
| `normalize_state` | Normalize state names | `"Donor/Desynced" → "DONOR"` |
| `extract_uuid` | Extract UUID | `"...abc123..." → "abc123"` |

## Entity Actions

| Action | Description | When to Use |
|--------|-------------|-------------|
| `CREATE_CORE` | Create or enrich CORE entity | First time seeing node/cluster |
| `UPDATE_CORE` | Update existing CORE entity | Adding details to existing entity |
| `CREATE_TEMPORAL` | Create TEMPORAL entity | Every event occurrence |
| `UPDATE_TEMPORAL` | Update existing TEMPORAL entity | Multi-line events (e.g., SST progress) |

## Adding a New Pattern

### Step 1: Identify the Entity
Is this a new entity type or existing?
- If new, add to `entity_schema.yaml` first
- Determine if CORE or TEMPORAL

### Step 2: Write the Pattern
Add to `patterns.yaml`:

```yaml
- pattern_id: "my_new_pattern"
  entity_target: "MyEntity"
  regex: '(?P<timestamp>...) (?P<data>...)'
  context: LOCAL
  confidence: 0.90
  
  extraction_mapping:
    - match_group: "timestamp"
      extraction_fn: "transform"
      transform: "parse_datetime"
      target_field: "timestamp"
  
  entity_action:
    action_type: "CREATE_TEMPORAL"
    entity_type: "MyEntity"
  
  validation_rules:
    - rule: "required_fields"
      fields: ["timestamp"]
  
  examples:
    - input: "sample log line"
      expected_extraction:
        timestamp: "2024-09-15T10:30:45"
```

### Step 3: Test the Pattern
Run test suite:
```bash
python3 test_schema_extraction.py
```

Or test specific pattern:
```python
from lib.schema_engine import SchemaBasedExtractor
extractor = SchemaBasedExtractor(Path('schema'))
pattern = extractor.schema_loader.patterns[0]  # Your pattern
match = pattern.match("your test line")
```

## Validation

Validation happens at multiple levels:

1. **Schema validation**: Entity data validates against schema
2. **Pattern validation**: Extracted data validates against rules
3. **Relationship validation**: Foreign keys reference valid entities
4. **Type validation**: Data types match schema definitions

Example validation rules:
```yaml
validation_rules:
  - rule: "required_fields"
    fields: ["timestamp", "node_uuid"]
  
  - rule: "pattern_match"
    field: "node_uuid"
    pattern: "^[a-f0-9-]+$"
  
  - rule: "enum_check"
    field: "state"
    allowed_values: ["SYNCED", "DONOR", "JOINER"]
  
  - rule: "relationship_exists"
    entity_type: "Node"
    foreign_key: "node_uuid"
```

## Best Practices

### Pattern Design
1. **Be specific**: More specific patterns have higher confidence
2. **Use named groups**: Makes extraction clear
3. **Include context**: Always specify LOCAL/GLOBAL/PEER
4. **Add examples**: Helps with testing and documentation
5. **Validate thoroughly**: Add validation rules

### Entity Design
1. **Choose CORE carefully**: Only immutable entities should be CORE
2. **Define relationships**: Makes data queryable
3. **Use enums**: Constrain values to valid options
4. **Document attributes**: Clear descriptions help
5. **Plan for growth**: Consider future attributes

### Testing
1. **Test each pattern**: With examples
2. **Test relationships**: Verify foreign keys
3. **Test edge cases**: Unusual log formats
4. **Test validation**: Both pass and fail cases
5. **Test real logs**: Use actual production logs

## Troubleshooting

### Pattern not matching
- Check regex syntax
- Verify named groups
- Test with regex debugger
- Check pattern is loaded

### Entity not created
- Check extraction mapping
- Verify required fields
- Review validation rules
- Check entity action type

### Validation errors
- Review entity schema
- Check field types
- Verify patterns
- Review validation rules

### Relationships not working
- Verify foreign keys
- Check parent entity exists
- Review relationship definition
- Check entity creation order

## Resources

- **Architecture**: `../SCHEMA_ARCHITECTURE.md`
- **Migration Guide**: `../MIGRATION_GUIDE.md`
- **Summary**: `../REFACTORING_SUMMARY.md`
- **Test Suite**: `../test_schema_extraction.py`
- **Quick Start**: `../quickstart.py`
- **Engine**: `../lib/schema_engine.py`

## Version

Schema Version: 1.0.0
