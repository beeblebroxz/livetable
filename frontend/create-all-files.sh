#!/bin/bash

# Load NVM
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"

echo "📝 Creating all frontend files..."

# 1. index.html
cat > index.html << 'HTML_EOF'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <link rel="icon" type="image/svg+xml" href="/vite.svg" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>LiveTable Editor</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
HTML_EOF

# 2. vite.config.ts
cat > vite.config.ts << 'VITE_EOF'
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
})
VITE_EOF

# 3. tsconfig.json
cat > tsconfig.json << 'TS_EOF'
{
  "compilerOptions": {
    "target": "ES2020",
    "useDefineForClassFields": true,
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true
  },
  "include": ["src"],
  "references": [{ "path": "./tsconfig.node.json" }]
}
TS_EOF

# 4. tsconfig.node.json
cat > tsconfig.node.json << 'TSNODE_EOF'
{
  "compilerOptions": {
    "composite": true,
    "skipLibCheck": true,
    "module": "ESNext",
    "moduleResolution": "bundler",
    "allowSyntheticDefaultImports": true
  },
  "include": ["vite.config.ts"]
}
TSNODE_EOF

# 5. tailwind.config.js
cat > tailwind.config.js << 'TAILWIND_EOF'
/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {},
  },
  plugins: [],
}
TAILWIND_EOF

# 6. postcss.config.js
cat > postcss.config.js << 'POSTCSS_EOF'
export default {
  plugins: {
    tailwindcss: {},
    autoprefixer: {},
  },
}
POSTCSS_EOF

# 7. src/index.css
cat > src/index.css << 'CSS_EOF'
@tailwind base;
@tailwind components;
@tailwind utilities;

body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen',
    'Ubuntu', 'Cantarell', 'Fira Sans', 'Droid Sans', 'Helvetica Neue',
    sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}
CSS_EOF

# 8. src/main.tsx
cat > src/main.tsx << 'MAIN_EOF'
import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import './index.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
MAIN_EOF

# 9. src/vite-env.d.ts
cat > src/vite-env.d.ts << 'ENV_EOF'
/// <reference types="vite/client" />
ENV_EOF

echo "✅ All configuration files created!"
echo ""
echo "Now run: ./setup-frontend.sh"
