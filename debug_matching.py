#!/usr/bin/env python3
"""
Test actual pattern matching order and results
"""

import logging
from pathlib import Path
from lib.patterns.matcher import PatternMatcher
from lib.entities import EntityType

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(name)s - %(message)s')

# Test lines
test_lines = [
    "2024-08-22 16:50:06 0 [Note] WSREP: Node 2ce78b90-5ed1-11ef-ac0a-7e6eaae74be8 state connection_established",
    "2024-08-22 16:52:08 0 [Warning] Aborted connection 80 to db: 'database3' user: 'application' host: '172.20.0.10' (Got an error reading communication packets)"
]

# Initialize PatternMatcher and detect dialect
patterns_dir = Path("patterns")
matcher = PatternMatcher(patterns_dir)

# Detect dialect from log file
log_file = Path("10.6-db3.log")
if log_file.exists():
    dialect_info = matcher.detect_dialect_from_file(log_file)
    print(f"Detected dialect: {dialect_info.dialect_type.value}")

print("\n=== Pattern Matching Test ===")

for line in test_lines:
    print(f"\nTesting line: {line[:80]}...")
    
    # Match the line
    entities = matcher.match_line(line)
    print(f"Extracted {len(entities)} entities:")
    
    for entity in entities:
        print(f"  Entity Type: {entity.entity_type.value}")
        print(f"  Pattern: {entity.pattern_name}")
        print(f"  Confidence: {entity.confidence}")
        print(f"  Entity ID: {entity.entity_id}")
        
        # Print entity-specific fields
        if hasattr(entity, 'current_state'):
            print(f"  Node State: {entity.current_state}")
        if hasattr(entity, 'warning_type'):
            print(f"  Warning Type: {entity.warning_type}")
        if hasattr(entity, 'abort_reason'):
            print(f"  Abort Reason: {entity.abort_reason}")

# Check pattern order for NODE type specifically
print(f"\n=== NODE Pattern Order ===")
node_patterns = matcher.get_patterns(EntityType.NODE)
for i, pattern in enumerate(node_patterns, 1):
    print(f"{i:2d}. {pattern.name} (confidence: {pattern.confidence})")

print(f"\n=== WARNING Pattern Order ===")
warning_patterns = matcher.get_patterns(EntityType.WARNING)
for i, pattern in enumerate(warning_patterns, 1):
    print(f"{i:2d}. {pattern.name} (confidence: {pattern.confidence})")

# Test individual patterns manually
print(f"\n=== Manual Pattern Testing ===")
node_line = test_lines[0]
warning_line = test_lines[1]

print(f"Testing NODE line against mariadb_10_6_node_state pattern:")
for pattern in node_patterns:
    if pattern.name == "mariadb_10_6_node_state":
        match = pattern.compiled_regex.search(node_line)
        print(f"  Pattern: {pattern.name}")
        print(f"  Regex: {pattern.regex}")
        print(f"  Match: {match is not None}")
        if match:
            print(f"  Groups: {match.groupdict()}")
        break

print(f"\nTesting WARNING line against mariadb_10_6_aborted_connection pattern:")
for pattern in warning_patterns:
    if pattern.name == "mariadb_10_6_aborted_connection":
        match = pattern.compiled_regex.search(warning_line)
        print(f"  Pattern: {pattern.name}")
        print(f"  Regex: {pattern.regex}")
        print(f"  Match: {match is not None}")
        if match:
            print(f"  Groups: {match.groupdict()}")
        break