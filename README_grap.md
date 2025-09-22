# GRAP - Galera Regex Analysis Parser (Next Generation)

**Entity-based log parsing system for MariaDB/Galera cluster analysis**

This is the next-generation implementation of grambo, featuring an entity-based architecture with interactive learning capabilities and versioned pattern registries.

## Features

- **Entity-based parsing**: Extract structured entities instead of simple regex matches
- **Interactive learning**: Train new patterns interactively
- **Versioned patterns**: YAML-based pattern registry with version management
- **Multiple output formats**: Text, JSON, YAML with backward compatibility
- **Quality gates**: Comprehensive unit testing and validation
- **Extensible architecture**: Easy to add new entity types and patterns

## Installation

```bash
# Clone the grambo repository
git clone https://github.com/claudionanni/grambo
cd grambo

# Install Python dependencies (PyYAML for pattern files)
pip install PyYAML

# Make CLI executable
chmod +x grap.py
```

## Quick Start

```bash
# Basic analysis
./grap.py test_logs/db3.log

# JSON output for integration
./grap.py --format=json test_logs/db3.log

# Filter specific entity types
./grap.py --entities=NODE,STATE_TRANSFER test_logs/db3.log

# Interactive learning mode
./grap.py --learn --interactive test_logs/db3.log

# Show available patterns
./grap.py --show-patterns
```

## Architecture

### Entity Types

- **NODE**: Galera cluster nodes and their state transitions
- **STATE_TRANSFER**: SST/IST operations between nodes
- **VIEW**: Cluster membership and view changes
- **COMMUNICATION**: Network communication events
- **ERROR/WARNING**: Error conditions and warnings

### Pattern Registry

Patterns are defined in YAML files under the `patterns/` directory:

- `node_patterns.yaml`: Node state and identification patterns
- `sst_patterns.yaml`: State transfer (SST/IST) patterns  
- `view_patterns.yaml`: Cluster view and membership patterns

### Output Formats

#### Text Format (Human-readable)
```
GRAP Entity Extraction Results
========================================
Generated: 2024-09-22 10:30:45
Total entities: 15

NODE (5 entities)
------------------------------------------------------------
  10:25:30 (0.90)
    Node: a1b2c3d4... | State: Synced -> Donor/Desynced | Address: 192.168.1.100:4567
```

#### JSON Format (Machine-readable)
```json
{
  "metadata": {
    "generator": "grap",
    "version": "2.0.0-alpha1",
    "timestamp": "2024-09-22T10:30:45",
    "total_entities": 15
  },
  "entities": [
    {
      "entity_type": "NODE",
      "node_id": "a1b2c3d4-e5f6-7890-abcd-123456789012",
      "current_state": "DONOR",
      "previous_state": "SYNCED",
      "timestamp": "2024-09-15T10:30:45",
      "confidence": 0.9
    }
  ]
}
```

#### Compatible Format (grambo-web integration)
```json
{
  "metadata": {
    "generator": "grap",
    "processing_method": "entity_extraction"
  },
  "detailed_events": [...],
  "cluster_events": [...],
  "summary": {
    "node_count": 3,
    "state_transfers": 2,
    "view_changes": 1
  }
}
```

## Command Line Reference

### Basic Options
```bash
grap.py [options] <logfile>
```

### Output Options
- `--format={text,json,yaml}`: Output format (default: text)
- `--output=FILE`: Write output to file (default: stdout)
- `--stats`: Include parsing statistics

### Entity Options
- `--entities=LIST`: Comma-separated entity types to extract
- `--confidence-threshold=N`: Minimum confidence threshold (0.0-1.0)

### Pattern Options
- `--pattern-version=VER`: Use specific MariaDB version patterns
- `--pattern-dir=DIR`: Custom pattern directory
- `--show-patterns`: Display loaded patterns and exit

### Learning Options
- `--learn`: Enable interactive learning mode
- `--interactive`: Enable interactive pattern validation
- `--save-patterns=FILE`: Save learned patterns to file

### Debug Options
- `--dry-run`: Validate configuration without processing
- `--verbose`: Increase verbosity (-v, -vv, -vvv)

## Pattern Development

### Creating New Patterns

1. **Define Pattern Structure**
```yaml
patterns:
  NODE:
    - name: "my_custom_pattern"
      description: "Captures custom node information"
      confidence: 0.85
      regex: 'Custom (?P<node_id>\\w+) pattern (?P<custom_field>\\w+)'
      field_mappings:
        node_id: "node_id"
        custom_field: "custom_data"
      required_fields: ["node_id"]
      test_cases:
        - input: "Custom node123 pattern data456"
          expected:
            node_id: "node123"
            custom_data: "data456"
```

2. **Test Patterns**
```bash
# Validate all patterns
./grap.py --show-patterns

# Test with specific version
./grap.py --pattern-version=10.6 --show-patterns
```

3. **Learn Interactively**
```bash
# Use learning mode to develop patterns
./grap.py --learn --interactive new_log_file.log
```

### Pattern Best Practices

- Use descriptive pattern names
- Set appropriate confidence levels (0.7-0.95)
- Include comprehensive test cases
- Use field mappings for consistent entity attributes
- Test with multiple MariaDB versions

## Testing

```bash
# Run all unit tests
cd tests
python run_tests.py

# Run specific test module
python -m unittest test_entities
python -m unittest test_patterns
python -m unittest test_output
```

## Integration with Existing Tools

### grambo-web Compatibility

GRAP provides backward compatibility with grambo-web through the compatible output format:

```bash
# Generate compatible output for grambo-web
./grap.py --format=json galera.log | graw
```

### Legacy grambo/gras Integration

```bash
# Compare outputs
./grambo galera.log > legacy_output.txt
./grap.py --format=text galera.log > new_output.txt
diff -u legacy_output.txt new_output.txt
```

## Development Roadmap

### Phase 1 (Completed) ✅
- [x] Basic CLI structure
- [x] Entity base classes (Entity, Event, Pattern)
- [x] Core entity types (NODE, STATE_TRANSFER, VIEW)
- [x] Pattern matching system
- [x] YAML pattern registry
- [x] Log parsing engine
- [x] Output formatting (text, JSON, YAML)
- [x] Unit tests and quality gates

### Phase 2 (Planned)
- [ ] Interactive learning improvements
- [ ] Pattern auto-generation
- [ ] Advanced entity relationships
- [ ] Performance optimization
- [ ] Extended entity types (COMMUNICATION, PERFORMANCE)

### Phase 3 (Planned)
- [ ] Machine learning pattern discovery
- [ ] Real-time log streaming
- [ ] Advanced analytics and insights
- [ ] Web-based pattern editor

## Contributing

1. Follow the technical specification in `GRAMBO_REFACTORING_SPEC.md`
2. Add unit tests for new functionality
3. Use the existing entity and pattern structure
4. Test backward compatibility with grambo-web

## License

Same as original grambo project.

## Support

For questions and issues:
- Check existing grambo documentation
- Review pattern YAML files for examples
- Use `--help` for command-line reference
- Run tests to validate installation