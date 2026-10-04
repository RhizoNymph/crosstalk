// Headless-Chrome smoke test of demo/index.html: every element renders
// without console errors, interaction sets `value` and fires `change`, the
// error and empty states show, and screenshots of each element are written
// in light and dark mode.
//
//   pnpm build && node scripts/smoke.mjs [screenshot-dir]
//
// Needs google-chrome on PATH. Exits non-zero on the first failed check.
import { spawn } from 'node:child_process';
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import * as esbuild from 'esbuild';

const outDir = process.argv[2] ?? join(process.env.TMPDIR ?? tmpdir(), 'ct-smoke');
mkdirSync(outDir, { recursive: true });
const DEVTOOLS_PORT = 9334;

const server = await esbuild.context({});
const { port } = await server.serve({ servedir: '.', host: '127.0.0.1', port: 8738 });
const base = `http://127.0.0.1:${port}`;

const profile = mkdtempSync(join(process.env.TMPDIR ?? tmpdir(), 'ct-chrome-'));
const chrome = spawn(
  'google-chrome',
  [
    '--headless=new',
    `--remote-debugging-port=${DEVTOOLS_PORT}`,
    '--no-first-run',
    '--no-default-browser-check',
    '--use-angle=swiftshader',
    '--enable-unsafe-swiftshader',
    '--window-size=1400,1200',
    `--user-data-dir=${profile}`,
    'about:blank',
  ],
  { stdio: 'ignore' },
);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const failures = [];
const check = (ok, message) => {
  console.log(`${ok ? 'ok  ' : 'FAIL'} ${message}`);
  if (!ok) failures.push(message);
};

async function finish(code) {
  chrome.kill();
  await server.dispose();
  process.exit(code);
}

let targets;
for (let i = 0; i < 50 && targets === undefined; i++) {
  try {
    targets = await (await fetch(`http://127.0.0.1:${DEVTOOLS_PORT}/json`)).json();
  } catch {
    await sleep(200);
  }
}
const page = targets?.find((t) => t.type === 'page');
if (page === undefined) {
  console.error('could not reach headless chrome');
  await finish(2);
}

const ws = new WebSocket(page.webSocketDebuggerUrl);
await new Promise((resolve) => ws.addEventListener('open', resolve));
let nextId = 0;
const pending = new Map();
const consoleErrors = [];
ws.addEventListener('message', (message) => {
  const data = JSON.parse(message.data);
  if (data.method === 'Runtime.exceptionThrown') {
    consoleErrors.push(data.params.exceptionDetails?.exception?.description ?? 'exception');
  }
  if (data.method === 'Runtime.consoleAPICalled' && data.params.type === 'error') {
    consoleErrors.push(data.params.args.map((a) => a.value ?? a.description).join(' '));
  }
  if (data.method === 'Log.entryAdded' && data.params.entry.level === 'error') {
    const { text, url } = data.params.entry;
    // The demo's HTTP-error panel fetches a missing file on purpose.
    if (!url?.endsWith('/missing.json')) consoleErrors.push(`${text} ${url ?? ''}`);
  }
  const resolve = pending.get(data.id);
  if (resolve !== undefined) {
    pending.delete(data.id);
    resolve(data);
  }
});
const send = (method, params = {}) =>
  new Promise((resolve) => {
    const id = ++nextId;
    pending.set(id, resolve);
    ws.send(JSON.stringify({ id, method, params }));
  });
const evaluate = async (expression) => {
  const reply = await send('Runtime.evaluate', {
    expression,
    awaitPromise: true,
    returnByValue: true,
  });
  if (reply.result?.exceptionDetails) {
    throw new Error(reply.result.exceptionDetails.exception?.description ?? expression);
  }
  return reply.result?.result?.value;
};
const waitFor = async (expression, timeout = 15000) => {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    if (await evaluate(expression)) return true;
    await sleep(100);
  }
  return false;
};
const mouse = (type, x, y, extra = {}) =>
  send('Input.dispatchMouseEvent', { type, x, y, button: 'left', clickCount: 1, ...extra });
const click = async (x, y) => {
  await mouse('mouseMoved', x, y, { button: 'none' });
  await mouse('mousePressed', x, y, { buttons: 1 });
  await mouse('mouseReleased', x, y, { buttons: 0 });
};
const rectOf = (id) =>
  evaluate(
    `(() => { const r = document.getElementById(${JSON.stringify(id)}).getBoundingClientRect(); return { x: r.x, y: r.y, width: r.width, height: r.height }; })()`,
  );
const elementValue = (id) => evaluate(`document.getElementById(${JSON.stringify(id)}).value`);
const changesOf = (id) =>
  evaluate(`window.ctChanges.filter((c) => c.id === ${JSON.stringify(id)}).length`);
const statusOf = (id) =>
  evaluate(
    `(() => { const s = document.getElementById(${JSON.stringify(id)}).shadowRoot.querySelector('.status'); return s.hidden ? null : { kind: s.dataset.kind, text: s.textContent }; })()`,
  );

await send('Runtime.enable');
await send('Log.enable');
await send('Page.enable');
await send('Emulation.setDeviceMetricsOverride', {
  width: 1400,
  height: 1200,
  deviceScaleFactor: 1,
  mobile: false,
});

async function scheme(name) {
  await send('Emulation.setEmulatedMedia', {
    features: [{ name: 'prefers-color-scheme', value: name }],
  });
  await sleep(600);
}

async function shot(id, name) {
  const r = await rectOf(id);
  const image = await send('Page.captureScreenshot', {
    format: 'png',
    clip: { x: r.x, y: r.y, width: r.width, height: r.height, scale: 1 },
  });
  writeFileSync(join(outDir, `${name}.png`), Buffer.from(image.result.data, 'base64'));
}

async function screenshots(name) {
  for (const id of ['brush', 'projection', 'empty', 'missing', 'invalid']) {
    await shot(id, `${id}-${name}`);
  }
  const full = await send('Page.captureScreenshot', { format: 'png' });
  writeFileSync(join(outDir, `page-${name}.png`), Buffer.from(full.result.data, 'base64'));
}

const topologyMode = async (mode, collapse) => {
  await evaluate(`(() => {
    document.querySelector('input[name=mode][value=${mode}]').click();
    const c = document.getElementById('collapse');
    if (c.checked !== ${collapse}) c.click();
  })()`);
  await waitFor(
    `document.getElementById('topology').shadowRoot.querySelector('.status').hidden && document.getElementById('topology').shadowRoot.querySelector('.meta').textContent.startsWith('${mode}')`,
  );
  await sleep(400);
};

/** Moves over a grid until the tooltip shows, then clicks there once. */
async function scanClick(id, rect, accept) {
  for (let gy = 0.1; gy < 0.92; gy += 0.02) {
    for (let gx = 0.04; gx < 0.97; gx += 0.012) {
      const x = rect.x + rect.width * gx;
      const y = rect.y + rect.height * gy;
      await mouse('mouseMoved', x, y, { button: 'none' });
      const hovering = await evaluate(
        `!document.getElementById(${JSON.stringify(id)}).shadowRoot.querySelector('.tooltip').hidden`,
      );
      if (!hovering) continue;
      await mouse('mousePressed', x, y, { buttons: 1 });
      await mouse('mouseReleased', x, y, { buttons: 0 });
      // Sigma counts two clicks within its timeout as a double click.
      await sleep(350);
      const value = await elementValue(id);
      if (accept(value)) return value;
    }
  }
  return null;
}

await send('Page.navigate', { url: `${base}/demo/` });
const ready = await waitFor(`(() => {
  const ok = (id, sel) => { const e = document.getElementById(id); return e?.shadowRoot?.querySelector(sel) && e.shadowRoot.querySelector('.status').hidden; };
  return ok('topology', 'canvas') && ok('projection', 'canvas') && ok('brush', 'svg rect');
})()`);
check(ready, 'all three elements render');
await sleep(800);

const empty = await statusOf('empty');
check(
  empty?.kind === 'empty' && /No transmissions/.test(empty.text),
  `empty state: ${empty?.text}`,
);
const missing = await statusOf('missing');
check(
  missing?.kind === 'error' && /HTTP 404/.test(missing.text),
  `HTTP error state: ${missing?.text}`,
);
const invalid = await statusOf('invalid');
check(
  invalid?.kind === 'error' && /unexpected payload/.test(invalid.text),
  `invalid payload state: ${invalid?.text?.split('\n')[0]}`,
);

await scheme('light');
await shot('topology', 'topology-agents-light');
await screenshots('light');

// Topology: hover-scan for a node and an edge; then clear by clicking empty space.
{
  const r = await rectOf('topology');
  const agent = await scanClick('topology', r, (v) => v.startsWith('agent:'));
  check(/^agent:[0-7][0-9A-HJKMNP-TV-Z]{25}$/.test(agent ?? ''), `topology node click → ${agent}`);
  await shot('topology', 'topology-agent-selected-light');
  const edge = await scanClick('topology', r, (v) => v.startsWith('edge:'));
  check(
    /^edge:[0-7][0-9A-HJKMNP-TV-Z]{25}:[0-7][0-9A-HJKMNP-TV-Z]{25}:(ch\.[0-9A-Z]{26}|dl\.p2c|dl\.c2p|dr\.user|dr\.sys|dr\.tool\..+|un)$/.test(
      edge ?? '',
    ),
    `topology edge click → ${edge}`,
  );
  const highlight = await evaluate(`document.getElementById('topology').dataset.highlight`);
  check(highlight === (await elementValue('topology')), 'page bound data-highlight to the value');
  await sleep(350);
  await click(r.x + 4, r.y + r.height - 30);
  await sleep(350);
  check((await elementValue('topology')) === '', 'clicking empty space clears the selection');
  check((await changesOf('topology')) >= 3, 'topology fires change for every selection');
}

// Projection: lasso with shift-drag, then click a point.
{
  const r = await rectOf('projection');
  const cx = r.x + r.width * 0.4;
  const cy = r.y + r.height * 0.5;
  const radius = Math.min(r.width, r.height) * 0.22;
  await mouse('mouseMoved', cx + radius, cy, { button: 'none', modifiers: 8 });
  await mouse('mousePressed', cx + radius, cy, { buttons: 1, modifiers: 8 });
  for (let step = 1; step <= 40; step++) {
    const angle = (step / 40) * 2 * Math.PI;
    await mouse('mouseMoved', cx + radius * Math.cos(angle), cy + radius * Math.sin(angle), {
      buttons: 1,
      modifiers: 8,
    });
    await sleep(12);
  }
  await mouse('mouseReleased', cx + radius, cy, { buttons: 0, modifiers: 8 });
  await sleep(300);
  const lasso = await elementValue('projection');
  const vertices = lasso.startsWith('lasso:') ? lasso.slice(6).split(';') : [];
  check(
    vertices.length >= 3 &&
      vertices.length <= 48 &&
      vertices.every((v) => /^-?\d+(\.\d{1,4})?,-?\d+(\.\d{1,4})?$/.test(v)),
    `projection lasso → ${vertices.length} vertices (${lasso.slice(0, 60)}…)`,
  );
  await screenshotLasso();
  let point = null;
  for (let gy = 0.15; gy < 0.9 && point === null; gy += 0.03) {
    for (let gx = 0.1; gx < 0.6; gx += 0.015) {
      await click(r.x + r.width * gx, r.y + r.height * gy);
      const value = await elementValue('projection');
      if (value.startsWith('point:')) {
        point = value;
        break;
      }
    }
  }
  check(
    /^point:[0-7][0-9A-HJKMNP-TV-Z]{25}$/.test(point ?? ''),
    `projection point click → ${point}`,
  );
  check((await changesOf('projection')) >= 2, 'projection fires change');
  await evaluate(
    `(() => { const p = document.getElementById('projection'); p.dataset.colorBy = 'route'; })()`,
  );
  await sleep(300);
  const legend = await evaluate(
    `document.getElementById('projection').shadowRoot.querySelector('.legend').textContent`,
  );
  check(/channel/.test(legend) && /delegation/.test(legend), 'colour by route updates the legend');
  await evaluate(`document.getElementById('hits').click()`);
  await sleep(300);
}

async function screenshotLasso() {
  const r = await rectOf('projection');
  const shot = await send('Page.captureScreenshot', {
    format: 'png',
    clip: { x: r.x, y: r.y, width: r.width, height: r.height, scale: 1 },
  });
  writeFileSync(join(outDir, 'projection-lasso.png'), Buffer.from(shot.result.data, 'base64'));
}

// Time brush: drag across the bars; the value snaps to 15-minute edges.
{
  const r = await rectOf('brush');
  const y = r.y + r.height * 0.55;
  await mouse('mouseMoved', r.x + r.width * 0.2, y, { button: 'none' });
  await mouse('mousePressed', r.x + r.width * 0.2, y, { buttons: 1 });
  for (let step = 1; step <= 10; step++) {
    await mouse('mouseMoved', r.x + r.width * (0.2 + 0.025 * step), y, { buttons: 1 });
  }
  await mouse('mouseReleased', r.x + r.width * 0.45, y, { buttons: 0 });
  await sleep(200);
  const value = await elementValue('brush');
  const match =
    /^(\d{4}-\d{2}-\d{2}T\d{2}:(\d{2}):00Z)\/(\d{4}-\d{2}-\d{2}T\d{2}:(\d{2}):00Z)$/.exec(value);
  check(
    match !== null &&
      Number(match[2]) % 15 === 0 &&
      Number(match[4]) % 15 === 0 &&
      match[1] < match[3],
    `time brush drag → ${value}`,
  );
  check((await changesOf('brush')) === 1, 'time brush fires one change, on pointer-up');
  const attrs = await evaluate(
    `(() => { const b = document.getElementById('brush'); return b.dataset.from + '/' + b.dataset.to; })()`,
  );
  check(attrs === value, 'page fed the window back as data-from/data-to');
}

// Mode switch: data-src changes reload; collapse redraws.
{
  await topologyMode('channels', false);
  const legend = await evaluate(
    `document.getElementById('topology').shadowRoot.querySelector('.legend').textContent`,
  );
  check(/channel policy/.test(legend), 'switching data-src loads the channels graph');
  await shot('topology', 'topology-channels-light');
  await topologyMode('agents', true);
  const label = await evaluate(
    `document.getElementById('topology').shadowRoot.querySelector('.meta').textContent`,
  );
  check(label.startsWith('agents'), 'collapse redraws the agents graph');
  await shot('topology', 'topology-collapsed-light');
}

await scheme('dark');
await screenshots('dark');
await shot('topology', 'topology-collapsed-dark');
await topologyMode('agents', false);
await shot('topology', 'topology-agents-dark');
await topologyMode('channels', false);
await shot('topology', 'topology-channels-dark');
await scheme('light');

// Abort: two quick data-src changes end on the second.
{
  await evaluate(`(() => {
    const t = document.getElementById('topology');
    t.dataset.src = '../test/fixtures/topology-agents.json';
    t.dataset.src = '../test/fixtures/topology-empty.json';
  })()`);
  await sleep(800);
  const status = await statusOf('topology');
  check(status?.kind === 'empty', 'a superseded data-src never renders');
}

// Detach and re-attach: WebGL is released and drawing resumes.
{
  await evaluate(`(() => {
    const p = document.getElementById('projection');
    const parent = p.parentElement;
    p.remove();
    parent.append(p);
  })()`);
  const back = await waitFor(
    `document.getElementById('projection').shadowRoot.querySelector('canvas') !== null && document.getElementById('projection').shadowRoot.querySelector('.status').hidden`,
  );
  check(back, 're-attached projection draws again');
}

check(
  consoleErrors.length === 0,
  `no console errors${consoleErrors.length ? `: ${consoleErrors.join(' | ')}` : ''}`,
);
console.log(`screenshots in ${outDir}`);
ws.close();
await finish(failures.length === 0 ? 0 : 1);
