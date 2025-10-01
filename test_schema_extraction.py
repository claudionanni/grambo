#!/usr/bin/env python3
"""
Test script for schema-based entity extraction

This script demonstrates the new pattern matching architecture with
sample log lines and validates the extraction process.
"""

import sys
import json
import logging
from pathlib import Path
from io import StringIO

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from lib.schema_engine import SchemaBasedExtractor


# Sample Galera log lines for testing
SAMPLE_LOG_LINES = """
2024-09-15 10:25:28 12339 [Note] WSREP: wsrep_cluster_state_uuid: b2c3d4e5-f6a7-8901-bcde-234567890123
2024-09-15 10:25:30 12340 [Note] WSREP: ####### My UUID: a1b2c3d4-e5f6-7890-abcd-123456789012
2024-09-15 10:25:32 12341 [Note] WSREP: Server db-node-01 synced with group
2024-09-15 10:25:35 12342 [Note] WSREP: Member 0.a1b2c3d4 at 192.168.1.100:4567
2024-09-15 10:30:45 12345 [Note] WSREP: a1b2c3d4-e5f6-7890-abcd-123456789012 state change: Synced -> Donor/Desynced
2024-09-15 10:31:02 12346 [Note] WSREP: a1b2c3d4-e5f6-7890-abcd-123456789012 state change: Donor/Desynced -> Synced
2024-09-15 10:32:15 12350 [Note] WSREP: Server db-node-02 synced with group
2024-09-15 10:35:00 12355 [Note] WSREP: Requesting state transfer: mariabackup
2024-09-15 10:40:30 12400 [ERROR] WSREP: Failed to establish connection with donor node
"""


def create_test_log_file():
    """Create a temporary test log file"""
    test_log = Path("test_extraction.log")
    with open(test_log, 'w') as f:
        f.write(SAMPLE_LOG_LINES)
    return test_log


def test_schema_loading():
    """Test schema loading"""
    print("=" * 80)
    print("TEST 1: Schema Loading")
    print("=" * 80)
    
    schema_dir = Path(__file__).parent / "schema"
    
    try:
        extractor = SchemaBasedExtractor(schema_dir)
        
        # Check entity schemas
        print(f"\n✓ Loaded {len(extractor.schema_loader.entity_schemas)} entity schemas:")
        for name, schema in extractor.schema_loader.entity_schemas.items():
            print(f"  - {name} ({schema.category.value})")
        
        # Check patterns
        print(f"\n✓ Loaded {len(extractor.schema_loader.patterns)} patterns:")
        for pattern in extractor.schema_loader.patterns[:5]:  # Show first 5
            print(f"  - {pattern.pattern_id} (confidence: {pattern.confidence})")
        if len(extractor.schema_loader.patterns) > 5:
            print(f"  ... and {len(extractor.schema_loader.patterns) - 5} more")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Failed to load schemas: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_pattern_matching():
    """Test pattern matching against sample lines"""
    print("\n" + "=" * 80)
    print("TEST 2: Pattern Matching")
    print("=" * 80)
    
    schema_dir = Path(__file__).parent / "schema"
    extractor = SchemaBasedExtractor(schema_dir)
    
    test_lines = [
        ("Cluster UUID", "2024-09-15 10:25:28 12339 [Note] WSREP: wsrep_cluster_state_uuid: b2c3d4e5-f6a7-8901-bcde-234567890123"),
        ("Node UUID", "2024-09-15 10:25:30 12340 [Note] WSREP: ####### My UUID: a1b2c3d4-e5f6-7890-abcd-123456789012"),
        ("Server Synced", "2024-09-15 10:25:32 12341 [Note] WSREP: Server db-node-01 synced with group"),
        ("State Change", "2024-09-15 10:30:45 12345 [Note] WSREP: a1b2c3d4-e5f6-7890-abcd-123456789012 state change: Synced -> Donor/Desynced"),
    ]
    
    matched = 0
    for name, line in test_lines:
        print(f"\n{name}:")
        print(f"  Line: {line[:80]}...")
        
        # Try to match
        for pattern in extractor.schema_loader.patterns:
            match = pattern.match(line)
            if match:
                print(f"  ✓ Matched pattern: {pattern.pattern_id}")
                print(f"    Entity target: {pattern.entity_target}")
                print(f"    Context: {pattern.context.value}")
                print(f"    Confidence: {pattern.confidence}")
                matched += 1
                break
        else:
            print(f"  ✗ No pattern matched")
    
    print(f"\n{'✓' if matched == len(test_lines) else '✗'} Matched {matched}/{len(test_lines)} test lines")
    return matched == len(test_lines)


def test_entity_extraction():
    """Test full entity extraction from log file"""
    print("\n" + "=" * 80)
    print("TEST 3: Entity Extraction")
    print("=" * 80)
    
    schema_dir = Path(__file__).parent / "schema"
    extractor = SchemaBasedExtractor(schema_dir)
    
    # Create test log file
    test_log = create_test_log_file()
    print(f"\n✓ Created test log: {test_log}")
    
    try:
        # Process log file
        entities = extractor.process_log_file(test_log)
        
        # Display results
        print("\n--- Extracted Entities ---\n")
        
        # CORE entities
        print("CORE Entities:")
        for entity_type, entities_dict in entities.get('core_entities', {}).items():
            print(f"\n  {entity_type} ({len(entities_dict)} entities):")
            for entity_id, entity_data in entities_dict.items():
                print(f"    - {entity_id}")
                for key, value in entity_data.items():
                    if key not in ['entity_id', 'entity_type']:
                        print(f"      {key}: {value}")
        
        # TEMPORAL entities
        print("\nTEMPORAL Entities:")
        for entity_type, entities_list in entities.get('temporal_entities', {}).items():
            print(f"\n  {entity_type} ({len(entities_list)} events):")
            for entity in entities_list:
                timestamp = entity.get('timestamp', 'N/A')
                print(f"    - {timestamp}")
                for key, value in entity.items():
                    if key not in ['entity_id', 'entity_type', 'timestamp', 'log_line']:
                        print(f"      {key}: {value}")
        
        # Save to JSON
        output_file = Path("test_extraction_output.json")
        with open(output_file, 'w') as f:
            json.dump(entities, f, indent=2, default=str)
        print(f"\n✓ Saved output to: {output_file}")
        
        # Statistics
        core_count = sum(len(e) for e in entities.get('core_entities', {}).values())
        temporal_count = sum(len(e) for e in entities.get('temporal_entities', {}).values())
        
        print(f"\n--- Statistics ---")
        print(f"CORE entities: {core_count}")
        print(f"TEMPORAL entities: {temporal_count}")
        print(f"Total entities: {core_count + temporal_count}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        if test_log.exists():
            test_log.unlink()


def test_entity_validation():
    """Test entity validation"""
    print("\n" + "=" * 80)
    print("TEST 4: Entity Validation")
    print("=" * 80)
    
    schema_dir = Path(__file__).parent / "schema"
    extractor = SchemaBasedExtractor(schema_dir)
    
    # Test valid entity
    print("\nTest 4.1: Valid Node entity")
    valid_node = {
        'node_uuid': 'a1b2c3d4-e5f6-7890-abcd-123456789012',
        'node_name': 'test-node',
        'first_seen': '2024-09-15T10:25:30',
        'log_source': '/var/log/mysql/error.log'
    }
    
    schema = extractor.schema_loader.get_entity_schema('Node')
    valid, errors = schema.validate_data(valid_node)
    
    if valid:
        print("  ✓ Validation passed")
    else:
        print(f"  ✗ Validation failed: {errors}")
    
    # Test invalid entity (missing required field)
    print("\nTest 4.2: Invalid Node entity (missing node_uuid)")
    invalid_node = {
        'node_name': 'test-node',
        'first_seen': '2024-09-15T10:25:30'
    }
    
    valid, errors = schema.validate_data(invalid_node)
    
    if not valid:
        print(f"  ✓ Correctly rejected: {errors}")
    else:
        print("  ✗ Should have failed validation")
    
    # Test pattern validation
    print("\nTest 4.3: Invalid pattern (wrong UUID format)")
    invalid_pattern = {
        'node_uuid': 'not-a-valid-uuid',
        'node_name': 'test-node',
        'first_seen': '2024-09-15T10:25:30',
        'log_source': '/var/log/mysql/error.log'
    }
    
    valid, errors = schema.validate_data(invalid_pattern)
    
    if not valid:
        print(f"  ✓ Correctly rejected: {errors}")
    else:
        print("  ✗ Should have failed pattern validation")
    
    return True


def main():
    """Run all tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s: %(message)s'
    )
    
    print("\n" + "=" * 80)
    print("GRAMBO SCHEMA-BASED ENTITY EXTRACTION TEST SUITE")
    print("=" * 80)
    
    tests = [
        ("Schema Loading", test_schema_loading),
        ("Pattern Matching", test_pattern_matching),
        ("Entity Extraction", test_entity_extraction),
        ("Entity Validation", test_entity_validation),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n✗ Test '{name}' crashed: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
    
    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status}: {name}")
    
    print(f"\nResult: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
