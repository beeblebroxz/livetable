#!/bin/bash
# Run all tests - Rust and Python

set -e  # Exit on first error

echo "🧪 LiveTable Test Suite Runner"
echo "======================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Track results
RUST_LINT_PASSED=0
RUST_PASSED=0
PYTHON_PASSED=0
INTEGRATION_PASSED=0
FRONTEND_PASSED=0

# Function to print status
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
    fi
}

# 1. Run Rust lints
echo "🧹 Running Rust Lints..."
echo "--------------------------------------"
cd ../impl
if cargo clippy --all-targets -- -D warnings \
    && cargo clippy --all-targets --features server -- -D warnings \
    && env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo clippy --all-targets --features python -- -D warnings; then
    RUST_LINT_PASSED=1
    echo ""
    print_status 0 "Rust clippy checks passed"
else
    print_status 1 "Rust clippy checks failed"
fi
echo ""

# 2. Run Rust tests
echo "📦 Running Rust Unit Tests..."
echo "--------------------------------------"
cd ../impl
if env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 cargo test --lib --features server 2>&1 | tee /tmp/rust_tests.log | grep -q "test result: ok"; then
    RUST_PASSED=1
    RUST_COUNT=$(grep "passed" /tmp/rust_tests.log | grep -o "[0-9]* passed" | head -1 | awk '{print $1}')
    echo ""
    print_status 0 "Rust tests passed ($RUST_COUNT tests)"
else
    print_status 1 "Rust tests failed"
fi
echo ""

# 3. Check if Python package is installed
echo "🐍 Checking Python package..."
echo "--------------------------------------"
if ! python3 -c "import livetable" 2>/dev/null; then
    echo -e "${YELLOW}⚠️  livetable not installed. Installing...${NC}"
    cd ../impl
    ./install.sh > /dev/null 2>&1
    echo -e "${GREEN}✅ Package installed${NC}"
fi
echo ""

# 4. Run Python unit tests
echo "🧪 Running Python Unit Tests..."
echo "--------------------------------------"
cd ../tests
if pytest python/ -v --tb=short 2>&1 | tee /tmp/python_tests.log; then
    PYTHON_PASSED=1
    PYTHON_COUNT=$(grep "passed" /tmp/python_tests.log | grep -o "[0-9]* passed" | tail -1 | awk '{print $1}')
    echo ""
    print_status 0 "Python unit tests passed ($PYTHON_COUNT tests)"
else
    print_status 1 "Python unit tests failed"
fi
echo ""

# 5. Run integration tests
echo "🔗 Running Integration Tests..."
echo "--------------------------------------"
if pytest integration/ -v --tb=short 2>&1 | tee /tmp/integration_tests.log; then
    INTEGRATION_PASSED=1
    INTEGRATION_COUNT=$(grep "passed" /tmp/integration_tests.log | grep -o "[0-9]* passed" | tail -1 | awk '{print $1}')
    echo ""
    print_status 0 "Integration tests passed ($INTEGRATION_COUNT tests)"
else
    print_status 1 "Integration tests failed"
fi
echo ""

# 6. Run frontend checks
echo "🌐 Running Frontend Checks..."
echo "--------------------------------------"
cd ../frontend
if npm run lint && npm run test && npm run build; then
    FRONTEND_PASSED=1
    echo ""
    print_status 0 "Frontend lint, tests, and build passed"
else
    print_status 1 "Frontend checks failed"
fi
echo ""

# Summary
echo "======================================"
echo "📊 Test Summary"
echo "======================================"
print_status $((1 - RUST_LINT_PASSED)) "Rust clippy"
print_status $((1 - RUST_PASSED)) "Rust unit tests ($RUST_COUNT tests)"
print_status $((1 - PYTHON_PASSED)) "Python unit tests ($PYTHON_COUNT tests)"
print_status $((1 - INTEGRATION_PASSED)) "Integration tests ($INTEGRATION_COUNT tests)"
print_status $((1 - FRONTEND_PASSED)) "Frontend checks"
echo ""

# Calculate total
TOTAL_PASSED=$((RUST_LINT_PASSED + RUST_PASSED + PYTHON_PASSED + INTEGRATION_PASSED + FRONTEND_PASSED))

if [ $TOTAL_PASSED -eq 5 ]; then
    echo -e "${GREEN}✨ All test suites passed! ✨${NC}"
    echo ""
    exit 0
else
    echo -e "${RED}❌ Some tests failed${NC}"
    echo ""
    exit 1
fi
