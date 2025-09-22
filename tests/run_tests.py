"""
Test runner for GRAP unit tests

Run all unit tests for the GRAP entity-based parsing system.
"""

import unittest
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import test modules
from test_entities import TestEntityBase, TestPatternMatching
from test_patterns import TestPatternMatcher
from test_output import TestOutputFormatter


def create_test_suite():
    """Create a comprehensive test suite"""
    suite = unittest.TestSuite()
    
    # Add entity tests
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestEntityBase))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPatternMatching))
    
    # Add pattern matcher tests
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestPatternMatcher))
    
    # Add output formatter tests
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestOutputFormatter))
    
    return suite


def run_tests():
    """Run all tests and return results"""
    suite = create_test_suite()
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    print("Running GRAP Unit Tests")
    print("=" * 40)
    
    success = run_tests()
    
    if success:
        print("\nAll tests passed! ✅")
        sys.exit(0)
    else:
        print("\nSome tests failed! ❌")
        sys.exit(1)