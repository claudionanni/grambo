# GRALC - Grambo Log Canonicalizer

## Purpose

The `gralc` tool converts non-standard Galera log formats into the canonical format expected by Grambo's analysis tools. This allows Grambo to process logs from different MySQL/MariaDB versions and configurations without modification.

## The Problem

Different MySQL and MariaDB versions use different log formats:

**MySQL 8.0 format (non-canonical):**
```
2025-09-09T07:41:41.832615+02:00 0 [System] [MY-000000] [WSREP] P: Shifting SYNCED -> OPEN (TO: 10344464)
```

**Canonical format (expected by Grambo):**
```
2025-09-09 07:41:41 0 [Note] WSREP: Shifting SYNCED -> OPEN (TO: 10344464)
```

Key differences:
- **Timestamp**: ISO8601 (`2025-09-09T07:41:41.832615+02:00`) vs. simple datetime (`2025-09-09 07:41:41`)
- **Log Level**: `[System]` vs. `[Note]`
- **Error Codes**: `[MY-000000]` error codes present vs. absent
- **Prefixes**: `P:` prefix in message vs. clean message

## Usage

### Show Supported Formats

```bash
./gralc
```

Output:
```
GRALC - Grambo Log Canonicalizer
======================================================================

Supported log formats:

Format 1: MySQL 8.0 with Error Codes (ISO8601 timestamp)
  Description: MySQL 8.0 style logs with [MY-XXXXXX] error codes and ISO8601 timestamps
  Example:
    2025-09-09T07:41:41.832615+02:00 0 [System] [MY-000000] [WSREP] P: Shifting SYNCED -> OPEN (TO: 10344464)

Format 2: MySQL 8.0 without WSREP prefix
  Description: MySQL 8.0 style with ISO8601 timestamps, missing [WSREP] tag but WSREP content
  Example:
    2025-09-09T07:41:41.832615+02:00 0 [System] [MY-000000] P: Shifting SYNCED -> OPEN (TO: 10344464)

Format 3: MariaDB style (no error codes)
  Description: MariaDB style logs with space-separated date/time and no error codes
  Example:
    2025-09-22 20:46:30 0 [Warning] WSREP: Failed to prepare for incremental state transfer

Canonical format (target):
  2025-09-22 20:46:30 0 [Note] WSREP: Shifting SYNCED -> CLOSED (TO: 2)
```

### Convert a Log File

By default `gralc` detects the format of each line, so no format ID is needed
and logs that mix tagged and untagged lines convert correctly. Lines that match
no known format are passed through unchanged. Use `--format=N` only to force a
single format.

```bash
# Convert to canonical format (auto-detect)
./gralc mysql8-error.log > canonical.log

# Convert and analyze in one step
./gralc mysql8-error.log | ./grax3

# Force a specific format
./gralc --format=1 mysql8-error.log > canonical.log

# Short form
./gralc -f 1 mysql8-error.log > canonical.log

# Pipe through
cat mysql8-error.log | ./gralc -f 1 > canonical.log
```

### Direct Pipeline to Grambo

```bash
# Convert and analyze in one command
./gralc -f 1 mysql8-error.log | ./grax3
```

### Multi-Node Conversion

```bash
# Convert multiple nodes
mkdir -p canonical_logs
for f in node*.log; do
    ./gralc -f 1 "$f" > "canonical_logs/$f"
done

# Analyze converted logs
./grax3 canonical_logs/*.log
```

## Supported Formats

### Format 1: MySQL 8.0 with Error Codes

**Source:**
```
2025-09-09T07:41:41.832615+02:00 0 [System] [MY-000000] [WSREP] P: Shifting SYNCED -> OPEN (TO: 10344464)
2025-09-09T07:41:43.268971+02:00 0 [System] [MY-000000] [WSREP] P: Failed to establish connection: Operation aborted.
2025-09-09T07:41:45.270790+02:00 16 [System] [MY-000000] [WSREP] L: Non-primary view
2025-09-09T01:23:02.782813+02:00 0 [Note] [MY-011953] [InnoDB] Page cleaner took 24345ms to flush 2019 and evict 2 pages
```

**Converted:**
```
2025-09-09 07:41:41 0 [Note] WSREP: Shifting SYNCED -> OPEN (TO: 10344464)
2025-09-09 07:41:43 0 [Note] WSREP: Failed to establish connection: Operation aborted.
2025-09-09 07:41:45 16 [Note] WSREP: Non-primary view
2025-09-09 01:23:02 0 [Note] InnoDB: Page cleaner took 24345ms to flush 2019 and evict 2 pages
```

Changes:
- ISO8601 timestamp → Simple datetime
- `[System]` → `[Note]`
- `[MY-XXXXXX]` codes removed
- `P:` and `L:` prefixes removed
- Component tag preserved (`[WSREP]`, `[InnoDB]`)

### Format 2: MySQL 8.0 without WSREP prefix

Similar to Format 1, but assumes missing `[WSREP]` component tag for Galera-related messages.

### Format 3: MariaDB style

Already in canonical format or very close to it. Minimal transformation needed.

## Implementation Details

### Conversion Rules

1. **Timestamp Conversion**: ISO8601 timestamps are parsed and converted to `YYYY-MM-DD HH:MM:SS` format
2. **Log Level Mapping**: `[System]` → `[Note]` for consistency
3. **Error Code Removal**: `[MY-XXXXXX]` codes are stripped
4. **Prefix Cleanup**: `P:` and `L:` prefixes are removed from messages
5. **Component Preservation**: WSREP, InnoDB, etc. tags are maintained

### Statistics

After conversion, gralc reports:
```
[gralc] Processed 417 lines, converted 260
```

This helps verify the conversion was applied to the expected number of lines.

## When to Use gralc

Use `gralc` when:
- ✅ You have MySQL 8.0 logs with ISO8601 timestamps
- ✅ Your logs have `[MY-XXXXXX]` error codes
- ✅ You see `[System]` log levels instead of `[Note]`
- ✅ Messages have `P:` prefixes before WSREP content
- ✅ Grambo tools fail to parse your logs correctly

Don't use `gralc` when:
- ❌ Your logs are already in canonical MariaDB format
- ❌ Timestamps are already `YYYY-MM-DD HH:MM:SS` format
- ❌ Grambo tools are working correctly with your logs

## Adding New Formats

To add support for a new log format:

1. Add format definition to `FORMATS` list in `gralc`
2. Add conversion function (e.g., `convert_format_4`)
3. Add converter to `CONVERTERS` dict
4. Test with sample logs

Example format definition:
```python
{
    "id": 4,
    "name": "New Format Name",
    "description": "Description of the format",
    "example": "2025-01-01 12:00:00 Example log line",
    "pattern": r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*$',
}
```

## Troubleshooting

### No Lines Converted

If gralc reports `converted 0`:
- Check you're using the correct format ID
- Verify your log matches one of the example patterns
- Use `./gralc` to see all supported formats

### Partial Conversion

If only some lines are converted:
- This is normal - only lines matching the pattern are converted
- Non-matching lines (like InnoDB messages) pass through unchanged
- Check the statistics to see conversion rate

### Timestamps Look Wrong

If timestamps are incorrect:
- gralc uses Python's `fromisoformat()` to parse timestamps
- Timezone information is handled automatically
- Result is always in simple `YYYY-MM-DD HH:MM:SS` format

## Examples

### Real-World Conversion

**Before:**
```
2025-09-09T07:41:39.268227+02:00 0 [System] [MY-000000] [WSREP] P: (5b45aded-944f, 'tcp://0.0.0.0:4567') turning message relay requesting on
2025-09-09T07:41:40.268192+02:00 0 [System] [MY-000000] [WSREP] P: (5b45aded-944f, 'tcp://0.0.0.0:4567') reconnecting to 77f09c3d-a9d8 (tcp://x.x.x.12:4567), attempt 0
2025-09-09T07:41:45.270436+02:00 0 [System] [MY-000000] [WSREP] P: Shifting SYNCED -> OPEN (TO: 10344464)
2025-09-09T07:41:45.270790+02:00 16 [System] [MY-000000] [WSREP] L: Non-primary view
```

**After:**
```
2025-09-09 07:41:39 0 [Note] WSREP: (5b45aded-944f, 'tcp://0.0.0.0:4567') turning message relay requesting on
2025-09-09 07:41:40 0 [Note] WSREP: (5b45aded-944f, 'tcp://0.0.0.0:4567') reconnecting to 77f09c3d-a9d8 (tcp://x.x.x.12:4567), attempt 0
2025-09-09 07:41:45 0 [Note] WSREP: Shifting SYNCED -> OPEN (TO: 10344464)
2025-09-09 07:41:45 16 [Note] WSREP: Non-primary view
```

### Complete Workflow

```bash
# 1. Check log format
head -5 mysql8-error.log

# 2. Show supported formats
./gralc

# 3. Convert logs
./gralc node1.log > canonical_node1.log
./gralc node2.log > canonical_node2.log
./gralc node3.log > canonical_node3.log

# 4. Analyze with grambo
./grax3 canonical_node*.log

# 5. View results at http://localhost:5000
```

## See Also

- [README.md](README.md) - Main Grambo documentation
- [CHANGELOG.md](CHANGELOG.md) - Version history and updates
- Grambo v3 tools: `grax3`, `graa3`, `grap3`, `graf3`, `grav3`
