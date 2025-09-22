#!/usr/bin/env python3
"""
Debug script to trace pattern matching for NODE and WARNING entities
"""

import re
import yaml
from pathlib import Path
from lib.patterns.dialect import DialectDetector, DialectPatternManager
from lib.patterns.matcher import PatternMatcher
from lib.entities.core import NodeEntity, WarningEntity

def test_pattern_matching():
    print("=== Pattern Matching Debug ===")
    
    # 1. Test dialect detection
    detector = DialectDetector()
    dialect_info = detector.detect_from_file(Path('10.6-db3.log'))
    print(f"Detected dialect: {dialect_info.dialect_type}")
    
    # 2. Test pattern file loading
    manager = DialectPatternManager(Path('/home/claudio/Projects/GITHUB/grambo/patterns'))
    pattern_files = manager.get_pattern_files(dialect_info.dialect_type)
    print(f"Pattern files: {[f.name for f in pattern_files]}")
    
    # 3. Load our specific pattern file
    with open('patterns/node_patterns_mariadb_10_6.yaml') as f:
        patterns = yaml.safe_load(f)
    
    print(f"\nLoaded patterns from node_patterns_mariadb_10_6.yaml:")
    for entity_type, pattern_list in patterns['patterns'].items():
        print(f"  {entity_type}: {len(pattern_list)} patterns")
        for pattern in pattern_list:
            print(f"    - {pattern['name']}: {pattern['regex'][:50]}...")
    
    # 4. Test direct regex matching
    print(f"\n=== Direct Regex Testing ===")
    
    # Test NODE pattern
    node_pattern = patterns['patterns']['NODE'][0]['regex']
    node_line = "2025-09-15 13:45:55 0 [Note] WSREP: Node 378c0ec7-a3db state prim"
    print(f"Testing NODE pattern against: {node_line}")
    node_match = re.search(node_pattern, node_line)
    if node_match:
        print(f"  ✓ NODE pattern matches: {node_match.groupdict()}")
    else:
        print(f"  ✗ NODE pattern does not match")
    
    # Test WARNING pattern  
    warning_pattern = patterns['patterns']['WARNING'][0]['regex']
    warning_line = "2025-09-15 13:46:24 4935 [Warning] Aborted connection 4935 to db: 'denovosystem_portal_dev' user: 'portal' host: 'ip-10-0-21-10.ec2.internal' (Got timeout reading communication packets)"
    print(f"Testing WARNING pattern against: {warning_line[:80]}...")
    warning_match = re.search(warning_pattern, warning_line)
    if warning_match:
        print(f"  ✓ WARNING pattern matches: {warning_match.groupdict()}")
    else:
        print(f"  ✗ WARNING pattern does not match")
    
    # 5. Test PatternMatcher
    print(f"\n=== PatternMatcher Testing ===")
    
    matcher = PatternMatcher(Path('/home/claudio/Projects/GITHUB/grambo/patterns'))
    
    # Check what patterns the matcher has loaded
    print(f"Patterns loaded in matcher:")
    for entity_type, pattern_list in matcher._patterns.items():
        print(f"  {entity_type}: {len(pattern_list)} patterns")
        for pattern in pattern_list:
            print(f"    - {pattern.name}: confidence={pattern.confidence}")
    
    # Test NODE entity creation
    print("Testing NODE entity creation...")
    try:
        from lib.entities.base import EntityType
        node_entities = matcher.match_line(node_line, [EntityType.NODE])
        print(f"  NODE entities created: {len(node_entities)}")
        for entity in node_entities:
            print(f"    - {entity.entity_type}: {type(entity).__name__}")
    except Exception as e:
        print(f"  ✗ NODE entity creation failed: {e}")
        import traceback
        traceback.print_exc()
    
    # Test WARNING entity creation
    print("Testing WARNING entity creation...")
    try:
        warning_entities = matcher.match_line(warning_line, [EntityType.WARNING])
        print(f"  WARNING entities created: {len(warning_entities)}")
        for entity in warning_entities:
            print(f"    - {entity.entity_type}: {type(entity).__name__}")
    except Exception as e:
        print(f"  ✗ WARNING entity creation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pattern_matching()