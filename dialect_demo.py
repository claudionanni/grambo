#!/usr/bin/env python3
"""
Demonstration of the Dialect Registry System
============================================

This script shows how the new dialect registry can be used to handle
different versions and variants of Galera/MariaDB/PXC log formats
without breaking existing functionality.
"""

import sys
import os

# Add current directory to path to import gramboo
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from gramboo import DialectRegistry

def demonstrate_dialect_system():
    """Demonstrate the flexible dialect pattern system"""
    
    print("=== Dialect Registry System Demonstration ===\n")
    
    # Create a registry
    registry = DialectRegistry()
    
    print("1. Default dialect patterns:")
    default_categories = registry.list_pattern_categories('default')
    print(f"   Available categories: {default_categories}")
    
    # Show some default IST patterns
    ist_patterns = registry.get_patterns('default', 'ist_patterns')
    print(f"   Default IST patterns count: {len(ist_patterns)}")
    if ist_patterns:
        print(f"   Sample pattern: {list(ist_patterns.keys())[0]}")
    else:
        print("   Note: Default IST patterns dictionary is empty (patterns in _ist_patterns property)")
    
    print("\n2. Creating MariaDB 10.6 dialect variant:")
    registry.add_dialect_variant('mariadb-10.6')
    
    # Modify specific patterns for MariaDB 10.6
    registry.update_pattern('mariadb-10.6', 'ist_patterns', 'start_enhanced', 
                          r'.*Incremental state transfer required from.*')
    
    # Add a new pattern specific to newer MariaDB
    registry.update_pattern('mariadb-10.6', 'ist_patterns', 'progress_enhanced',
                          r'.*IST progress: (\d+)% completed.*')
    
    maria_patterns = registry.get_patterns('mariadb-10.6', 'ist_patterns')
    print(f"   MariaDB 10.6 IST patterns count: {len(maria_patterns)}")
    print(f"   New patterns added: start_enhanced, progress_enhanced")
    
    print("\n3. Creating Percona XtraDB Cluster 8.0 dialect:")
    registry.add_dialect_variant('pxc-8.0')
    
    # PXC might have different terminology
    registry.update_pattern('pxc-8.0', 'ist_patterns', 'pxc_start',
                          r'.*Starting incremental state transfer.*')
    registry.update_pattern('pxc-8.0', 'sst_patterns', 'pxc_xtrabackup',
                          r'.*XtraBackup SST method selected.*')
    
    pxc_patterns = registry.get_patterns('pxc-8.0', 'ist_patterns')
    print(f"   PXC 8.0 IST patterns count: {len(pxc_patterns)}")
    
    print("\n4. Fallback mechanism:")
    # Try to get patterns for a non-existent dialect
    unknown_patterns = registry.get_patterns('unknown-dialect', 'ist_patterns')
    print(f"   Unknown dialect falls back to default: {len(unknown_patterns)} patterns")
    
    print("\n5. Pattern caching:")
    # Show that patterns are cached
    print(f"   Cache entries: {len(registry._pattern_cache)}")
    
    print("\n=== Integration Benefits ===")
    print("✓ Backward compatible: existing code unchanged")
    print("✓ Zero risk: fallback to default dialect")
    print("✓ Extensible: add new dialects without code changes")
    print("✓ Performance: compiled patterns cached")
    print("✓ Maintainable: centralized pattern management")
    
    print("\n=== Usage in GaleraLogAnalyzer ===")
    print("The analyzer now uses dialect_registry._ist_patterns property")
    print("which transparently provides the right patterns based on detected dialect.")

if __name__ == '__main__':
    demonstrate_dialect_system()
