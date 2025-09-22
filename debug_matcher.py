#!/usr/bin/env python3
"""
Debug PatternMatcher pattern loading specifically
"""

import logging
from pathlib import Path
from lib.patterns.matcher import PatternMatcher
from lib.entities import EntityType

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(name)s - %(message)s')

# Initialize PatternMatcher
patterns_dir = Path("patterns")
print("=== Debug PatternMatcher Pattern Loading ===")

# Create PatternMatcher
matcher = PatternMatcher(patterns_dir)
print(f"PatternMatcher initialized for dialect: {matcher.current_dialect}")

# Check what pattern files the dialect manager found
print(f"Dialect manager pattern files: {matcher.dialect_manager.get_pattern_files(matcher.current_dialect)}")

# Check loaded patterns
print("\n=== Loaded Patterns in PatternMatcher ===")
all_patterns = matcher.get_all_patterns()

for entity_type, patterns in all_patterns.items():
    print(f"\n{entity_type.value} patterns: {len(patterns)}")
    for i, pattern in enumerate(patterns, 1):
        print(f"  {i}. {pattern.name}")
        print(f"     Regex: {pattern.regex[:100]}...")
        print(f"     Confidence: {pattern.confidence}")

# Check specifically for NODE and WARNING patterns
print("\n=== NODE Pattern Details ===")
node_patterns = all_patterns.get(EntityType.NODE, [])
for pattern in node_patterns:
    print(f"Pattern: {pattern.name}")
    print(f"Regex: {pattern.regex}")
    print()

print("\n=== WARNING Pattern Details ===")
warning_patterns = all_patterns.get(EntityType.WARNING, [])
for pattern in warning_patterns:
    print(f"Pattern: {pattern.name}")
    print(f"Regex: {pattern.regex}")
    print()

# Check entity registry
print("\n=== Entity Registry Status ===")
registry = matcher.entity_registry
print(f"Registry has entity classes for: {list(registry._entity_classes.keys())}")

# Test dialect detection on sample log
print("\n=== Dialect Detection Test ===")
log_file = Path("10.6-db3.log")
if log_file.exists():
    dialect_info = matcher.detect_dialect_from_file(log_file)
    print(f"Detected dialect: {dialect_info.dialect_type}")
    print(f"Confidence: {dialect_info.confidence}")
    print(f"Method: {dialect_info.detection_method}")
    
    # Check patterns after dialect detection
    print(f"\nPatterns after dialect detection:")
    updated_patterns = matcher.get_all_patterns()
    print(f"NODE patterns: {len(updated_patterns.get(EntityType.NODE, []))}")
    print(f"WARNING patterns: {len(updated_patterns.get(EntityType.WARNING, []))}")
    
    for pattern in updated_patterns.get(EntityType.NODE, []):
        print(f"  NODE: {pattern.name}")
    for pattern in updated_patterns.get(EntityType.WARNING, []):
        print(f"  WARNING: {pattern.name}")