#!/usr/bin/env python3
"""
Quick Start Example for Schema-Based Entity Extraction

This script demonstrates how to use the new schema-based extractor
with a real Galera log file.
"""

import sys
import json
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from lib.schema_engine import SchemaBasedExtractor


def print_banner(text):
    """Print a formatted banner"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80 + "\n")


def format_entity_summary(entities):
    """Format entity summary for display"""
    summary = []
    
    # CORE entities
    core = entities.get('core_entities', {})
    if core:
        summary.append("CORE Entities (Immutable Base Objects):")
        for entity_type, entity_dict in core.items():
            count = len(entity_dict)
            summary.append(f"  • {entity_type}: {count} {'entity' if count == 1 else 'entities'}")
    
    # TEMPORAL entities
    temporal = entities.get('temporal_entities', {})
    if temporal:
        summary.append("\nTEMPORAL Entities (Time-Based Events):")
        for entity_type, entity_list in temporal.items():
            count = len(entity_list)
            summary.append(f"  • {entity_type}: {count} {'event' if count == 1 else 'events'}")
    
    return "\n".join(summary)


def show_sample_entities(entities, max_samples=3):
    """Show sample entities for each type"""
    print("\n--- Sample Entities ---\n")
    
    # Show CORE entities
    print("CORE Entities:\n")
    for entity_type, entity_dict in entities.get('core_entities', {}).items():
        print(f"{entity_type}:")
        for i, (entity_id, entity_data) in enumerate(entity_dict.items()):
            if i >= max_samples:
                print(f"  ... and {len(entity_dict) - max_samples} more")
                break
            print(f"  • {entity_id}")
            for key, value in entity_data.items():
                if key not in ['entity_id', 'entity_type', 'log_line']:
                    value_str = str(value)[:60]
                    print(f"    {key}: {value_str}")
        print()
    
    # Show TEMPORAL entities
    print("TEMPORAL Entities:\n")
    for entity_type, entity_list in entities.get('temporal_entities', {}).items():
        print(f"{entity_type}:")
        for i, entity in enumerate(entity_list):
            if i >= max_samples:
                print(f"  ... and {len(entity_list) - max_samples} more")
                break
            timestamp = entity.get('timestamp', 'N/A')
            print(f"  • {timestamp}")
            for key, value in entity.items():
                if key not in ['entity_id', 'entity_type', 'timestamp', 'log_line', 'line_number']:
                    value_str = str(value)[:60]
                    print(f"    {key}: {value_str}")
        print()


def main():
    """Main function"""
    print_banner("Grambo Schema-Based Entity Extraction - Quick Start")
    
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python3 quickstart.py <galera-log-file>")
        print("\nExample:")
        print("  python3 quickstart.py /var/log/mysql/error.log")
        print("\nIf no log file is provided, will use test data.")
        log_file = None
    else:
        log_file = Path(sys.argv[1])
        if not log_file.exists():
            print(f"Error: Log file not found: {log_file}")
            return 1
    
    # Initialize extractor
    print("Initializing schema-based extractor...")
    schema_dir = Path(__file__).parent / "schema"
    
    if not schema_dir.exists():
        print(f"Error: Schema directory not found: {schema_dir}")
        return 1
    
    try:
        extractor = SchemaBasedExtractor(schema_dir)
        print(f"✓ Loaded {len(extractor.schema_loader.entity_schemas)} entity schemas")
        print(f"✓ Loaded {len(extractor.schema_loader.patterns)} patterns")
    except Exception as e:
        print(f"✗ Failed to initialize extractor: {e}")
        return 1
    
    # Process log file or run test
    if log_file:
        print_banner(f"Processing Log File: {log_file}")
        
        start_time = datetime.now()
        
        try:
            entities = extractor.process_log_file(log_file)
            
            duration = (datetime.now() - start_time).total_seconds()
            
            print(f"\n✓ Processing completed in {duration:.2f} seconds")
            
        except Exception as e:
            print(f"\n✗ Processing failed: {e}")
            import traceback
            traceback.print_exc()
            return 1
    else:
        # Run test with sample data
        print_banner("Running Test with Sample Data")
        print("(Provide log file path to process real data)")
        
        from test_schema_extraction import create_test_log_file
        test_log = create_test_log_file()
        
        try:
            entities = extractor.process_log_file(test_log)
            print("✓ Test data processed successfully")
        finally:
            if test_log.exists():
                test_log.unlink()
    
    # Display summary
    print_banner("Extraction Results")
    
    summary = format_entity_summary(entities)
    print(summary)
    
    # Calculate totals
    core_count = sum(len(e) for e in entities.get('core_entities', {}).values())
    temporal_count = sum(len(e) for e in entities.get('temporal_entities', {}).values())
    total_count = core_count + temporal_count
    
    print(f"\nTotal Entities: {total_count}")
    
    # Show samples
    if total_count > 0:
        show_sample_entities(entities)
    
    # Save output
    output_file = Path("entities_output.json")
    print_banner("Saving Results")
    
    try:
        with open(output_file, 'w') as f:
            json.dump(entities, f, indent=2, default=str)
        print(f"✓ Saved to: {output_file}")
        print(f"  File size: {output_file.stat().st_size:,} bytes")
    except Exception as e:
        print(f"✗ Failed to save output: {e}")
        return 1
    
    # Pretty print option
    print("\nTo view formatted output:")
    print(f"  cat {output_file} | jq .")
    print("\nTo query specific entities:")
    print(f"  cat {output_file} | jq '.core_entities.Node'")
    print(f"  cat {output_file} | jq '.temporal_entities.NodeStateChange'")
    
    # Next steps
    print_banner("Next Steps")
    print("""
1. Review entity output in entities_output.json
2. Check entity relationships and data quality
3. Add more patterns to schema/patterns.yaml
4. Customize entity schema in schema/entity_schema.yaml
5. Process multiple log files to build complete timeline

Documentation:
  • SCHEMA_ARCHITECTURE.md - Architecture overview
  • MIGRATION_GUIDE.md - Pattern migration guide
  • REFACTORING_SUMMARY.md - Summary of changes

Test Suite:
  • python3 test_schema_extraction.py
""")
    
    print("✓ Quick start completed successfully!\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
