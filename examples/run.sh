#!/bin/bash
# LiveTable - Quick Launcher
# Run different Python examples easily

# Change to the examples directory
cd "$(dirname "$0")"

echo "🚀 LiveTable Python Examples"
echo "=============================="
echo ""
echo "Choose what to run:"
echo ""
echo "  1. Quick Start (quickstart.py)"
echo "     → Learn the basics in 5 minutes"
echo ""
echo "  2. Playground (playground.py)"
echo "     → Interactive examples and challenges"
echo ""
echo "  3. Full Test Suite (test_python_bindings.py)"
echo "     → Comprehensive feature demonstration"
echo ""
echo "  4. Python REPL with livetable imported"
echo "     → Free-form experimentation"
echo ""
echo -n "Enter choice (1-4): "
read choice

case $choice in
  1)
    echo ""
    echo "Running Quick Start..."
    echo "----------------------"
    python3 quickstart.py
    ;;
  2)
    echo ""
    echo "Opening Playground..."
    echo "---------------------"
    python3 playground.py
    ;;
  3)
    echo ""
    echo "Running Full Test Suite..."
    echo "--------------------------"
    python3 test_python_bindings.py
    ;;
  4)
    echo ""
    echo "Starting Python REPL with livetable..."
    echo "--------------------------------------"
    echo ">>> import livetable"
    echo ">>> # Try: livetable.Table, livetable.Schema, etc."
    echo ""
    python3 -i -c "import livetable; print('livetable module loaded. Type help(livetable) for info.')"
    ;;
  *)
    echo ""
    echo "Invalid choice. Please run again and choose 1-4."
    exit 1
    ;;
esac

echo ""
echo "=============================="
echo "Done! 🎉"
