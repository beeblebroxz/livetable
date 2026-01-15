#!/bin/bash

# Load NVM
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

echo "🚀 Setting up LiveTable React Frontend..."
echo ""

# Install dependencies
echo "📦 Installing dependencies..."
npm install

# Initialize Tailwind
echo "🎨 Configuring Tailwind CSS..."
npx tailwindcss init -p

echo ""
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. npm run dev"
echo "2. Open http://localhost:5173"
echo ""
