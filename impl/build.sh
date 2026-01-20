#!/bin/bash
# Build the Rust implementation with Python bindings

echo "🔧 Building LiveTable Rust Implementation"
echo "===================================="
echo ""

# Check if maturin is installed
if ! command -v maturin &> /dev/null; then
    echo "❌ maturin not found. Installing..."
    pip3 install maturin --break-system-packages
    echo ""
fi

echo "📦 Building Python wheel..."
echo ""

# Build the wheel
env PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Build successful!"
    echo ""
    echo "📍 Wheel location: target/wheels/"
    echo ""
    echo "To install:"
    echo "  pip3 install target/wheels/livetable-*.whl --force-reinstall --break-system-packages"
    echo ""
    echo "Or run: ./install.sh"
else
    echo ""
    echo "❌ Build failed!"
    exit 1
fi
