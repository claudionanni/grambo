pip install PyYAML
# GRAP Quick Reference

`grap` converts Galera / MariaDB error logs into a structured entity stream that powers downstream analysis (`graf` and `grav`).

## Usage

```bash
# Single log → JSON entities
.venv/bin/python3 grap --no-cache --format=json error.log > grap_output.json

# Multiple logs (merged chronologically)
.venv/bin/python3 grap --no-cache --format=json node1.log node2.log > grap_output.json

# Only emit certain entity types
.venv/bin/python3 grap --no-cache --format=json --filter=view,node error.log
```

Key flags:

- `--no-cache` — force a fresh parse when logs change.
- `--format=json` — required for the `graf` pipeline.
- `--filter` — optional comma-separated entity types.

## Output

JSON payload shaped as:

```json
{
  "grap_version": "v2.x",
  "extraction_time": "2025-09-29T17:00:00",
  "total_entities": 417,
  "entities": [ ... ]
}
```

`entities` contains dictionaries for each event (views, nodes, sst/ist, errors, etc). Feed this file to `graf`:

```bash
.venv/bin/python3 graf grap_output.json --ndjson -o graf_frames.ndjson
```

## Integration Notes

- Log order matters: pass every relevant log file in one invocation to keep a single timeline.
- `grap` automatically records the version used; bump `GRAP_VERSION` in the script to invalidate caches after structural changes.
- For quick experimentation, pipe output directly: `./grap --format=json log | ./graf --ndjson`.
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