#!/bin/bash
# Build and install the Rust implementation

echo "🚀 Build and Install LiveTable"
echo "=============================="
echo ""

# Build first
./build.sh

if [ $? -eq 0 ]; then
    echo ""
    echo "📥 Installing Python package..."
    echo ""

    # Find the wheel file
    WHEEL=$(ls -t target/wheels/livetable-*.whl 2>/dev/null | head -1)

    if [ -z "$WHEEL" ]; then
        echo "❌ No wheel file found in target/wheels/"
        exit 1
    fi

    pip3 install "$WHEEL" --force-reinstall --break-system-packages

    if [ $? -eq 0 ]; then
        echo ""
        echo "✅ Installation complete!"
        echo ""
        echo "Test it:"
        echo "  python3 -c 'import livetable; print(livetable.ColumnType.INT32)'"
        echo ""
        echo "Try examples:"
        echo "  cd ../examples"
        echo "  python3 quickstart.py"
    else
        echo "❌ Installation failed!"
        exit 1
    fi
else
    echo "❌ Build failed, cannot install"
    exit 1
fi
