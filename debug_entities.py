#!/usr/bin/env python3
"""
Debug entity creation step-by-step
"""

import logging
from pathlib import Path
from lib.patterns.matcher import PatternMatcher
from lib.entities import EntityType

# Set up detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(name)s - %(message)s')

# Test lines
test_lines = [
    "2024-08-22 16:50:06 0 [Note] WSREP: Node 2ce78b90-5ed1-11ef-ac0a-7e6eaae74be8 state connection_established",
    "2024-08-22 16:52:08 0 [Warning] Aborted connection 80 to db: 'database3' user: 'application' host: '172.20.0.10' (Got an error reading communication packets)"
]

# Initialize PatternMatcher and detect dialect
patterns_dir = Path("patterns")

# Create matcher with lower confidence threshold
print("=== Creating PatternMatcher with confidence_threshold=0.5 ===")
matcher = PatternMatcher(patterns_dir, confidence_threshold=0.5)

# Detect dialect from log file
log_file = Path("10.6-db3.log")
if log_file.exists():
    dialect_info = matcher.detect_dialect_from_file(log_file)
    print(f"Detected dialect: {dialect_info.dialect_type.value}")

print(f"PatternMatcher confidence threshold: {matcher.confidence_threshold}")

print("\n=== Testing First Line (NODE) ===")
line = test_lines[0]
print(f"Line: {line}")

# Get NODE patterns and test each one
node_patterns = matcher.get_patterns(EntityType.NODE)
print(f"Found {len(node_patterns)} NODE patterns")

for i, pattern in enumerate(node_patterns):
    print(f"\n--- Pattern {i+1}: {pattern.name} (confidence: {pattern.confidence}) ---")
    
    # Skip if below threshold
    if pattern.confidence < matcher.confidence_threshold:
        print(f"SKIPPED: Below confidence threshold ({pattern.confidence} < {matcher.confidence_threshold})")
        continue
    
    # Test regex match
    if pattern.compiled_regex:
        match = pattern.compiled_regex.search(line)
        print(f"Regex match: {match is not None}")
        
        if match:
            print(f"Match groups: {match.groupdict()}")
            
            # Try to create entity manually to see what happens
            try:
                print("Attempting entity creation...")
                entity = matcher._create_entity_from_match(pattern, match, line, EntityType.NODE)
                if entity:
                    print(f"SUCCESS: Created entity {entity.entity_type.value} with ID {entity.entity_id}")
                    print(f"Pattern name: {entity.pattern_name}")
                    print(f"Confidence: {entity.confidence}")
                else:
                    print("FAILED: _create_entity_from_match returned None")
            except Exception as e:
                print(f"ERROR in entity creation: {e}")
                import traceback
                traceback.print_exc()
    else:
        print("No compiled regex available")

print("\n=== Testing Second Line (WARNING) ===")
line = test_lines[1]
print(f"Line: {line}")

# Get WARNING patterns and test each one
warning_patterns = matcher.get_patterns(EntityType.WARNING)
print(f"Found {len(warning_patterns)} WARNING patterns")

for i, pattern in enumerate(warning_patterns):
    print(f"\n--- Pattern {i+1}: {pattern.name} (confidence: {pattern.confidence}) ---")
    
    # Skip if below threshold
    if pattern.confidence < matcher.confidence_threshold:
        print(f"SKIPPED: Below confidence threshold ({pattern.confidence} < {matcher.confidence_threshold})")
        continue
    
    # Test regex match
    if pattern.compiled_regex:
        match = pattern.compiled_regex.search(line)
        print(f"Regex match: {match is not None}")
        
        if match:
            print(f"Match groups: {match.groupdict()}")
            
            # Try to create entity manually to see what happens
            try:
                print("Attempting entity creation...")
                entity = matcher._create_entity_from_match(pattern, match, line, EntityType.WARNING)
                if entity:
                    print(f"SUCCESS: Created entity {entity.entity_type.value} with ID {entity.entity_id}")
                    print(f"Pattern name: {entity.pattern_name}")
                    print(f"Confidence: {entity.confidence}")
                else:
                    print("FAILED: _create_entity_from_match returned None")
            except Exception as e:
                print(f"ERROR in entity creation: {e}")
                import traceback
                traceback.print_exc()
    else:
        print("No compiled regex available")

# Test match_line method with debugging
print("\n=== Testing match_line Method ===")
for line in test_lines:
    print(f"\nTesting: {line[:80]}...")
    try:
        entities = matcher.match_line(line)
        print(f"match_line returned {len(entities)} entities")
        for entity in entities:
            print(f"  - {entity.entity_type.value}: {entity.pattern_name}")
    except Exception as e:
        print(f"ERROR in match_line: {e}")
        import traceback
        traceback.print_exc()