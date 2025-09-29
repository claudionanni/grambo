#!/usr/bin/env python3
"""
Entity Coverage Validation
Validates that GRAP extracts expected entity types with appropriate coverage
"""

import json
import sys
from collections import defaultdict, Counter

def load_grap_output():
    """Load GRAP output"""
    try:
        with open('/home/claudio/Projects/GITHUB/grambo/grax_output/grap_output.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("❌ ERROR: grax_output/grap_output.json not found")
        sys.exit(1)

def analyze_entity_coverage():
    """Analyze entity type coverage and distribution"""
    print("=== ENTITY COVERAGE ANALYSIS ===")
    
    grap_data = load_grap_output()
    
    # Count entities by type
    entity_counts = Counter()
    entity_details = defaultdict(list)
    
    for entity in grap_data['entities']:
        entity_type = entity.get('entity_type', 'unknown')
        entity_counts[entity_type] += 1
        entity_details[entity_type].append(entity)
    
    total_entities = sum(entity_counts.values())
    
    print(f"Total entities extracted: {total_entities}")
    print(f"Entity types found: {len(entity_counts)}")
    print()
    
    # Display distribution
    print("Entity type distribution:")
    for entity_type, count in entity_counts.most_common():
        percentage = (count / total_entities) * 100
        print(f"  {entity_type:20}: {count:4d} ({percentage:5.1f}%)")
    
    return entity_counts, entity_details

def validate_expected_entities(entity_counts, entity_details):
    """Validate that expected entity types are present"""
    print(f"\n=== EXPECTED ENTITY VALIDATION ===")
    
    # Define expected entities for a typical Galera cluster analysis
    expected_entities = {
        'cluster': {'min': 1, 'description': 'Cluster identification'},
        'node_state': {'min': 10, 'description': 'Node state transitions'},
        'view': {'min': 5, 'description': 'Cluster membership views'},
        'sst_event': {'min': 0, 'description': 'State Snapshot Transfer events'},
        'ist_event': {'min': 0, 'description': 'Incremental State Transfer events'},
        'error': {'min': 0, 'description': 'Error conditions'},
        'warning': {'min': 0, 'description': 'Warning messages'}
    }
    
    validation_results = {}
    
    for entity_type, requirements in expected_entities.items():
        count = entity_counts.get(entity_type, 0)
        min_expected = requirements['min']
        description = requirements['description']
        
        if count >= min_expected:
            status = "✅ PASS"
        elif count == 0 and min_expected == 0:
            status = "ℹ️ OPTIONAL"
        else:
            status = "❌ FAIL"
        
        print(f"  {entity_type:15}: {count:4d} (min: {min_expected:2d}) {status} - {description}")
        
        validation_results[entity_type] = {
            'count': count,
            'min_expected': min_expected,
            'status': 'pass' if count >= min_expected else 'fail',
            'description': description
        }
    
    # Check for unexpected entity types
    unexpected = set(entity_counts.keys()) - set(expected_entities.keys())
    if unexpected:
        print(f"\nUnexpected entity types found:")
        for entity_type in unexpected:
            count = entity_counts[entity_type]
            print(f"  {entity_type:15}: {count:4d} - May indicate new patterns or extraction issues")
    
    return validation_results

def analyze_entity_quality(entity_details):
    """Analyze quality metrics for each entity type"""
    print(f"\n=== ENTITY QUALITY ANALYSIS ===")
    
    quality_metrics = {}
    
    for entity_type, entities in entity_details.items():
        if not entities:
            continue
            
        # Analyze common quality indicators
        confidence_scores = [e.get('confidence', 0) for e in entities if 'confidence' in e]
        has_timestamps = sum(1 for e in entities if e.get('timestamp'))
        has_raw_lines = sum(1 for e in entities if e.get('raw_line'))
        has_line_numbers = sum(1 for e in entities if e.get('line_number'))
        
        avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
        timestamp_coverage = (has_timestamps / len(entities)) * 100
        raw_line_coverage = (has_raw_lines / len(entities)) * 100
        line_number_coverage = (has_line_numbers / len(entities)) * 100
        
        quality_metrics[entity_type] = {
            'count': len(entities),
            'avg_confidence': avg_confidence,
            'timestamp_coverage': timestamp_coverage,
            'raw_line_coverage': raw_line_coverage,
            'line_number_coverage': line_number_coverage
        }
        
        print(f"\n{entity_type} ({len(entities)} entities):")
        print(f"  Average confidence: {avg_confidence:.2f}")
        print(f"  Timestamp coverage: {timestamp_coverage:.1f}%")
        print(f"  Raw line coverage: {raw_line_coverage:.1f}%")
        print(f"  Line number coverage: {line_number_coverage:.1f}%")
        
        # Quality assessment
        quality_issues = []
        if avg_confidence < 0.8:
            quality_issues.append("Low confidence scores")
        if timestamp_coverage < 90:
            quality_issues.append("Missing timestamps")
        if raw_line_coverage < 80:
            quality_issues.append("Missing raw lines")
        
        if quality_issues:
            print(f"  ⚠️ Quality issues: {', '.join(quality_issues)}")
        else:
            print(f"  ✅ Good quality metrics")
    
    return quality_metrics

def analyze_temporal_coverage(entity_details):
    """Analyze temporal distribution of entities"""
    print(f"\n=== TEMPORAL COVERAGE ANALYSIS ===")
    
    all_timestamps = []
    
    for entity_type, entities in entity_details.items():
        for entity in entities:
            timestamp = entity.get('timestamp')
            if timestamp:
                all_timestamps.append(timestamp)
    
    if not all_timestamps:
        print("⚠️ No timestamps found in entities")
        return None
    
    all_timestamps.sort()
    
    print(f"Temporal span: {all_timestamps[0]} to {all_timestamps[-1]}")
    print(f"Total timestamped events: {len(all_timestamps)}")
    
    # Analyze distribution by hour/day
    time_distribution = defaultdict(int)
    for timestamp in all_timestamps:
        # Extract date part (YYYY-MM-DD)
        date_part = timestamp.split()[0] if ' ' in timestamp else timestamp[:10]
        time_distribution[date_part] += 1
    
    print(f"Events distributed across {len(time_distribution)} days:")
    for date, count in sorted(time_distribution.items()):
        print(f"  {date}: {count} events")
    
    return time_distribution

def main():
    print("Entity Coverage Validation")
    print("=" * 50)
    
    # Analyze coverage
    entity_counts, entity_details = analyze_entity_coverage()
    
    # Validate expected entities
    validation_results = validate_expected_entities(entity_counts, entity_details)
    
    # Analyze quality
    quality_metrics = analyze_entity_quality(entity_details)
    
    # Analyze temporal coverage
    temporal_coverage = analyze_temporal_coverage(entity_details)
    
    # Overall assessment
    print(f"\n=== OVERALL ASSESSMENT ===")
    
    failed_validations = sum(1 for result in validation_results.values() if result['status'] == 'fail')
    total_validations = len(validation_results)
    
    print(f"Entity type validations: {total_validations - failed_validations}/{total_validations} passed")
    
    if failed_validations == 0:
        print("✅ Entity coverage validation PASSED")
        return 0
    else:
        print(f"❌ Entity coverage validation FAILED ({failed_validations} issues)")
        return 1

if __name__ == "__main__":
    sys.exit(main())