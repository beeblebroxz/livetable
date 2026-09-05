import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  timeout: 60000,
  expect: { timeout: 15000 },
  use: {
    baseURL: 'http://127.0.0.1:5180',
    channel: 'chrome',
    viewport: { width: 1440, height: 1100 },
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
  },
  webServer: [
    {
      command: 'cargo build --release --manifest-path ../impl/Cargo.toml --features server --bin livetable-server && PORT=8087 ../impl/target/release/livetable-server --lab',
      url: 'http://127.0.0.1:8087/health',
      reuseExistingServer: false,
      timeout: 180000,
    },
    {
      command: 'VITE_LIVETABLE_WS_URL=ws://127.0.0.1:8087/ws npm run dev -- --host 127.0.0.1 --port 5180 --strictPort',
      url: 'http://127.0.0.1:5180',
      reuseExistingServer: false,
      timeout: 30000,
    },
  ],
});
