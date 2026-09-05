// One local launcher; owns and cleans up only the child processes it starts.
import { spawn } from 'node:child_process';
import { createServer } from 'node:net';
import { fileURLToPath, URL } from 'node:url';
import console from 'node:console';
import process from 'node:process';

const frontendDir = fileURLToPath(new URL('../', import.meta.url));
const rootDir = fileURLToPath(new URL('../../', import.meta.url));
const children = new Set();
let stopping = false;

function port(name, fallback) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < 1024 || value > 65535) throw new Error(`${name} must be a port between 1024 and 65535`);
  return value;
}

async function checkPort(value) {
  await new Promise((resolve, reject) => {
    const probe = createServer();
    probe.once('error', reject);
    probe.listen(value, '127.0.0.1', () => probe.close(resolve));
  });
}

function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  process.exitCode = code;
  for (const child of children) child.kill('SIGTERM');
}

function start(command, args, cwd, env = process.env) {
  const child = spawn(command, args, { cwd, env, stdio: 'inherit' });
  children.add(child);
  child.on('error', error => { console.error(error.message); stop(1); });
  child.on('exit', () => children.delete(child));
  return child;
}

process.on('SIGINT', () => stop());
process.on('SIGTERM', () => stop());

try {
  const backendPort = port('LAB_PORT', 8080);
  const uiPort = port('LAB_UI_PORT', 5173);
  if (backendPort === uiPort) throw new Error('The backend and frontend need different ports');
  await checkPort(backendPort);
  await checkPort(uiPort);
  const build = start('cargo', ['build', '--release', '--manifest-path', 'impl/Cargo.toml', '--features', 'server', '--bin', 'livetable-server'], rootDir);
  const buildCode = await new Promise(resolve => { build.on('exit', resolve); build.on('error', () => resolve(1)); });
  if (buildCode !== 0 || stopping) stop(1);
  else {
    const backend = start(fileURLToPath(new URL('../../impl/target/release/livetable-server', import.meta.url)), ['--lab'], rootDir,
      { ...process.env, HOST: '127.0.0.1', PORT: String(backendPort) });
    const frontend = start(process.execPath, ['node_modules/vite/bin/vite.js', '--host', '127.0.0.1', '--port', String(uiPort), '--strictPort'], frontendDir,
      { ...process.env, VITE_LIVETABLE_WS_URL: `ws://127.0.0.1:${backendPort}/ws` });
    for (const child of [backend, frontend]) child.on('exit', code => stop(code ?? 1));
    console.log(`\nLiveTable Lab: http://127.0.0.1:${uiPort}\nCtrl+C stops both local servers.\n`);
  }
} catch (error) {
  console.error(`Cannot start the lab: ${error.message}`);
  stop(1);
}
