# Dialect Registry System

## Overview

The `DialectRegistry` class provides a comprehensive, backward-compatible system for managing dialect-specific log parsing patterns in gramboo. This allows the tool to handle different versions and variants of Galera/MariaDB/PXC log formats without breaking existing functionality.

## Architecture

- **Backward Compatible**: Existing code continues to work unchanged via property-based access
- **Zero Risk**: Unknown dialects fall back to default patterns automatically  
- **Extensible**: New dialects can be added without modifying core code
- **Performant**: Compiled regex patterns are cached for efficiency
- **Comprehensive**: 80+ patterns organized into 8 logical categories

## Pattern Categories

### Current Implementation (80+ patterns)

1. **`ist_patterns`** (20 patterns) - Incremental State Transfer events
2. **`sst_patterns`** (18 patterns) - State Snapshot Transfer events  
3. **`state_transition_patterns`** (6 patterns) - WSREP state transitions
4. **`view_change_patterns`** (8 patterns) - Cluster view changes
5. **`communication_patterns`** (6 patterns) - Network communication events
6. **`server_info_patterns`** (12 patterns) - Server version and configuration
7. **`flow_control_patterns`** (5 patterns) - Flow control and replication
8. **`general_patterns`** (5 patterns) - Timestamp and dialect detection

### API Methods

```python
# Get patterns for a dialect and category
patterns = registry.get_patterns('mariadb-10.6', 'sst_patterns')

# List available pattern categories  
categories = registry.list_pattern_categories('default')

# Create a new dialect based on another
registry.add_dialect_variant('pxc-8.0', base_dialect='default')

# Update specific patterns
registry.update_pattern('mariadb-10.6', 'sst_patterns', 'enhanced_progress',
                       r'SST progress: (\d+)% \((\d+)/(\d+) MB\) transferred')
```

### Integration

The `GaleraLogAnalyzer` class now provides convenient access via properties:

```python
# Access pattern categories through properties
analyzer._ist_patterns           # IST patterns (backward compatible)
analyzer._sst_patterns          # SST patterns  
analyzer._state_transition_patterns  # State transition patterns
analyzer._view_change_patterns   # View change patterns
analyzer._communication_patterns # Communication patterns
analyzer._server_info_patterns  # Server info patterns
analyzer._flow_control_patterns # Flow control patterns
analyzer._general_patterns      # General patterns
```

## Benefits Over Previous System

### ✅ **Pattern Organization**
- **Before**: 60+ regex patterns scattered across parsing methods
- **After**: 80+ patterns organized into 8 logical categories

### ✅ **Maintainability** 
- **Before**: Patterns hard-coded in multiple methods  
- **After**: Centralized pattern management with clear separation

### ✅ **Extensibility**
- **Before**: Adding patterns required code changes
- **After**: New dialects and patterns added without touching core code

### ✅ **Backward Compatibility**
- **Before**: N/A (new system)
- **After**: Existing `_ist_patterns` property works unchanged

## Example Usage

### Basic Pattern Access
```python
analyzer = GaleraLogAnalyzer()

# Get all SST patterns for current dialect
sst_patterns = analyzer._sst_patterns

# Check for SST request pattern
if sst_patterns['request_with_donor'].search(line):
    # Handle SST request...
```

### Adding Dialect Variants
```python
# Create MariaDB 10.6 variant
analyzer.dialect_registry.add_dialect_variant('mariadb-10.6')

# Add MariaDB 10.6 specific SST pattern
analyzer.dialect_registry.update_pattern(
    'mariadb-10.6', 'sst_patterns', 'enhanced_progress',
    r'SST progress: (\d+)% \((\d+)/(\d+) MB\) transferred'
)

# Pattern counts: default=18, mariadb-10.6=19 SST patterns
```

### Dialect-Specific Processing
```python
# Automatically uses the right patterns based on detected dialect
if analyzer.resolved_dialect == 'mariadb-10.6':
    patterns = analyzer._sst_patterns  # Gets MariaDB 10.6 patterns
else:
    patterns = analyzer._sst_patterns  # Gets default patterns
```

## Future Extensions

The system is designed to easily accommodate:

1. **Version-Specific Patterns**: MariaDB 10.6 vs 11.0 vs PXC 8.0
2. **Vendor Variants**: Codership Galera vs MariaDB Enterprise  
3. **New Categories**: Error patterns, performance patterns, security patterns
4. **Pattern Evolution**: Updates for new log message formats

## Migration from Old System

**No migration needed!** The system is fully backward compatible:

- Existing `analyzer._ist_patterns` continues to work
- All existing parsing logic unchanged  
- New categories available via new properties
- Future dialect variants can be added gradually

## Community Contributions

The registry system makes it easy for the community to contribute:

1. **Identify dialect-specific patterns** in different MariaDB/Galera versions
2. **Submit pattern additions** for specific dialects  
3. **Test with real log files** from different environments
4. **Improve accuracy** without affecting existing installations

## Performance Impact

- **Pattern Compilation**: Done once at startup, cached for reuse
- **Memory Usage**: Minimal increase (~80 compiled regex objects)
- **Lookup Speed**: O(1) hash table lookup by dialect+category
- **Runtime Impact**: Zero - same performance as before
