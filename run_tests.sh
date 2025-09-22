#!/bin/bash

# GRAP Unit Test Runner
# Runs comprehensive tests for GRAP log processing functionality

echo "=================================="
echo "GRAP Unit Test Suite"
echo "=================================="
echo ""

# Change to project directory
cd "$(dirname "$0")" || exit 1

echo "📋 Test Summary:"
echo "• Regression tests for all training logs (CS/ES prefixes)"
echo "• Entity extraction validation"  
echo "• Pattern loading verification"
echo "• Timestamp comparison fix validation"
echo "• JSON output format testing"
echo "• Empty/malformed log handling"
echo "• Performance testing with large logs"
echo ""

echo "🚀 Running tests..."
echo ""

# Run the tests with detailed output
python3 tests/test_log_processing.py

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ All tests passed successfully!"
    echo ""
    echo "📊 Test Coverage:"
    echo "• CS (Community Server) logs: unittest/CS-*.err"
    echo "• ES (Enterprise Server) logs: unittest/ES-*.err" 
    echo "• Pattern files: patterns/*.yaml"
    echo "• Core functionality: temporal properties, entity extraction, formatting"
    echo ""
    echo "🛡️  Your changes are safe to deploy!"
else
    echo ""
    echo "❌ Some tests failed. Please review the output above."
    echo ""
    echo "💡 Common issues:"
    echo "• Missing training log files in unittest/ directory"
    echo "• Pattern file syntax errors"
    echo "• API changes breaking existing functionality"
    exit 1
fi