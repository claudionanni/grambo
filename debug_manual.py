#!/usr/bin/env python3
"""Manual debug script to test pattern matching"""

import re
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib.patterns.loader import PatternMatcher
from lib.entities.registry import EntityType

# Initialize pattern matcher
pattern_matcher = PatternMatcher()
pattern_matcher.load_patterns('patterns')

# Test line from the log
test_line = "2025-09-15 13:45:55 0 [Note] WSREP: Node 378c0ec7-a3db state prim"

print(f"Testing line: {test_line}")
print()

# Test NODE patterns specifically
node_patterns = pattern_matcher.patterns.get(EntityType.NODE, [])
print(f"Found {len(node_patterns)} NODE patterns")

for pattern in node_patterns:
    print(f"\nTesting pattern: {pattern.name}")
    print(f"Regex: {pattern.regex.pattern}")
    
    match = pattern.regex.search(test_line)
    if match:
        print(f"✓ MATCH! Groups: {match.groups()}")
        print(f"  Captured fields: {match.groupdict()}")
        
        # Try to create entity
        entity = pattern_matcher.create_entity_from_match(pattern, match, test_line)
        if entity:
            print(f"  ✓ Entity created: {entity}")
        else:
            print(f"  ✗ Entity creation failed")
    else:
        print("✗ No match")

print("\n" + "="*50)

# Also test VIEW patterns for the view line
test_view_line = "2025-09-15 13:45:55 0 [Note] WSREP: view(view_id(PRIM,378c0ec7-a3db,16) memb {"
print(f"Testing view line: {test_view_line}")

view_patterns = pattern_matcher.patterns.get(EntityType.VIEW, [])
print(f"Found {len(view_patterns)} VIEW patterns")

for pattern in view_patterns:
    match = pattern.regex.search(test_view_line)
    if match:
        print(f"✓ Pattern '{pattern.name}' matches!")
        print(f"  Groups: {match.groupdict()}")
        entity = pattern_matcher.create_entity_from_match(pattern, match, test_view_line)
        if entity:
            print(f"  ✓ Entity created: {entity}")