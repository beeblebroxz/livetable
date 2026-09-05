import { memo, useEffect, useRef, useState } from 'react';
import { useLab, type LabStats, type NodeDelivery } from '../hooks/useLab';
import { formatBytes, formatCell, NODE_LABELS, SCENARIOS, scenarioAction } from '../lib/lab';
import type { LabAction, PipelineSnapshot, WireViewRecord } from '../types';
import './OrdersLab.css';

function Glyph({ name, size = 16 }: { name: 'arrow' | 'play' | 'pause' | 'reset' | 'external' | 'check'; size?: number }) {
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    {name === 'arrow' ? <path d="M5 12h14m-5-5 5 5-5 5" />
      : name === 'play' ? <path d="m9 5 11 7-11 7V5Z" />
      : name === 'pause' ? <path d="M8 5v14M16 5v14" />
      : name === 'reset' ? <><path d="M4 10a8 8 0 1 1 1 7M4 4v6h6" /></>
      : name === 'external' ? <><path d="M14 4h6v6m0-6L10 14M10 4H4v16h16v-6" /></>
      : <path d="m5 12 4 4L19 6" />}
  </svg>;
}

const DataViewport = memo(function DataViewport({ snapshot, delivery, onSelect, selectedId }: {
  snapshot?: PipelineSnapshot; delivery?: NodeDelivery; selectedId: number | null;
  onSelect: (record: WireViewRecord) => void;
}) {
  const [scrollTop, setScrollTop] = useState(0);
  const rows = snapshot?.rows ?? [];
  const columns = snapshot?.columns ?? ['order', 'region', 'product', 'quantity', 'amount'];
  const start = Math.min(Math.max(0, Math.floor(scrollTop / 40) - 4), Math.max(0, rows.length - 22));
  const end = Math.min(rows.length, start + 22);
  return <div className="lab-table-viewport" onScroll={event => setScrollTop(event.currentTarget.scrollTop)} tabIndex={0} aria-label="Scrollable results table">
    <table className="lab-table" aria-rowcount={rows.length + 1}>
      <thead><tr>{columns.map(column => <th key={column} scope="col" className={['amount', 'quantity', 'total', 'orders', 'average'].includes(column) ? 'numeric' : ''}>{column === 'order' ? 'Order ID' : column === 'amount' ? 'Order value' : column}</th>)}</tr></thead>
      <tbody>
        {start > 0 && <tr aria-hidden="true"><td colSpan={columns.length} style={{ height: start * 40, padding: 0 }} /></tr>}
        {rows.slice(start, end).map((record, offset) => {
          const index = start + offset;
          const highlighted = delivery?.indices.includes(index);
          return <tr key={record.row_id ?? index} aria-rowindex={index + 2}
            className={`${highlighted ? 'row-changed' : ''} ${record.row_id !== null && record.row_id === selectedId ? 'row-selected' : ''}`}>
            {columns.map(column => <td key={column} className={['amount', 'quantity', 'total', 'orders', 'average'].includes(column) ? 'numeric' : ''}>
              {column === 'order' && record.row_id !== null ? <button className="order-link" onClick={() => onSelect(record)}>{formatCell(column, record.row[column])}</button>
                : column === 'region' ? <span className="region-label"><i className={`region-dot region-${String(record.row[column]).replace(/ /g, '-').toLowerCase()}`} />{formatCell(column, record.row[column])}</span>
                  : formatCell(column, record.row[column])}
            </td>)}
          </tr>;
        })}
        {end < rows.length && <tr aria-hidden="true"><td colSpan={columns.length} style={{ height: (rows.length - end) * 40, padding: 0 }} /></tr>}
        {!rows.length && <tr><td colSpan={columns.length} className="table-empty">{snapshot ? 'No orders match this view.' : 'Waiting for a server snapshot…'}</td></tr>}
      </tbody>
    </table>
  </div>;
});

function PipelineGraph({ snapshots, stats, selected, onSelect, threshold }: {
  snapshots: Record<string, PipelineSnapshot>; stats: LabStats; selected: string; onSelect: (node: string) => void; threshold: number;
}) {
  const node = (id: string, number: string, subtitle: string) => <button key={id} className={`pipeline-node ${selected === id ? 'node-selected' : ''}`} onClick={() => onSelect(id)} aria-pressed={selected === id}>
    <span className="node-top"><span className="node-number">{number}</span><span className={`delivery-tag ${stats.nodes[id]?.kind ?? ''}`}>{stats.nodes[id]?.kind ?? (snapshots[id] ? 'no delivery' : 'waiting')}</span></span>
    <strong>{NODE_LABELS[id]}</strong><span className="node-subtitle">{subtitle}</span>
    <span className="node-bottom"><b>{snapshots[id]?.rows.length.toLocaleString('en-US') ?? '—'}</b> rows <span>seq {snapshots[id]?.seq ?? '—'}</span></span>
  </button>;
  return <section className="pipeline-section" aria-label="Server-side dependency graph">
    <div className="section-heading"><h2>Follow the change</h2><span>SERVER-SIDE PIPELINE <span className="small-dot" /> SELECT A VIEW TO INSPECT</span></div>
    <div className="pipeline-graph">
      {node('base', '01', 'Shared source · native typed columns')}
      <div className="graph-link" aria-hidden="true"><Glyph name="arrow" /></div>
      {node('high-value', '02', `FILTER amount ≥ ${formatCell('amount', threshold)}`)}
      <div className="graph-branch" aria-hidden="true"><span /><span /></div>
      <div className="branch-nodes">{node('ranked', '03', 'SORT amount descending')}{node('regions', '04', 'GROUP BY region · sum / count / avg')}</div>
    </div>
    <p className="graph-caption">One source, two downstream perspectives. Filter and sort receive bounded deltas; regional totals receive snapshots.</p>
  </section>;
}

function DeliveryInspector({ stats, busy }: { stats: LabStats; busy: boolean }) {
  const [selected, setSelected] = useState<number | null>(null);
  const entry = stats.events.find(event => event.id === selected);
  return <aside className="delivery-inspector" aria-label="Delivery inspector">
    <div className="inspector-heading"><span className={`activity-dot ${busy ? 'is-active' : ''}`} /><h2>On the wire</h2><span className="mono">LIVE</span></div>
    <p className="inspector-description">Actual deliveries to this client.<br />Select an event to inspect it.</p>
    <div className="trace-legend"><span><i className="legend-delta" /> delta</span><span><i className="legend-snapshot" /> snapshot</span><span><i className="legend-repair" /> recovery</span></div>
    <div className="trace-list" tabIndex={0} aria-label="Recent delivery events">
      {stats.events.length ? stats.events.map(event => <button key={event.id} className={`trace-entry ${event.kind} ${selected === event.id ? 'trace-selected' : ''}`} onClick={() => setSelected(event.id)}>
        <span className="trace-mark" /><span className="trace-body"><span><strong>{NODE_LABELS[event.node] ?? event.node}</strong><b>{event.bytes ? formatBytes(event.bytes) : '—'}</b></span><span className="trace-kind">{event.kind}<span>#{event.id}</span></span></span>
      </button>) : <p className="trace-empty">Run a scenario to see exactly what travels over the connection.</p>}
    </div>
    {entry && <div className="trace-detail"><span className="eyebrow">EVENT #{entry.id} / {entry.kind}</span><p>{entry.detail}</p><span className="mono">{formatBytes(entry.bytes)} received JSON</span></div>}
    <div className="inspector-footnote">Last 80 events retained. Byte totals also include flat-table echoes and command replies. UTF-8 JSON only; excludes WebSocket framing and outbound requests.</div>
  </aside>;
}

export function OrdersLab() {
  const clientB = new URLSearchParams(window.location.search).get('client') === 'b';
  const [threshold, setThreshold] = useState(clientB ? 2500 : 1000);
  const [scenarioIndex, setScenarioIndex] = useState(0);
  const [mode, setMode] = useState<'guided' | 'explore'>('guided');
  const [selectedNode, setSelectedNode] = useState('ranked');
  const [size, setSize] = useState(1000);
  const [rate, setRate] = useState(2);
  const [streaming, setStreaming] = useState(false);
  const [resetConfirm, setResetConfirm] = useState(false);
  const [localError, setLocalError] = useState('');
  const [edit, setEdit] = useState<{ id: number; order: string; amount: string } | null>(null);
  const [showStorage, setShowStorage] = useState(false);
  const lab = useLab(threshold);
  const labRef = useRef(lab);
  labRef.current = lab;
  const scenario = SCENARIOS[scenarioIndex];
  const ready = lab.connected && Object.keys(lab.snapshots).length === 4 && !Object.keys(lab.errors).length;
  const disabled = !ready || lab.busy || streaming;
  const mountedRef = useRef(true);

  useEffect(() => { mountedRef.current = true; return () => { mountedRef.current = false; }; }, []);
  useEffect(() => {
    if (!lab.connected) setStreaming(false);
  }, [lab.connected]);
  useEffect(() => {
    if (!streaming) return;
    let cancelled = false;
    let timer: number | undefined;
    const next = async () => {
      const started = performance.now();
      try { await labRef.current.run({ kind: 'step' }, 'Mixed batch'); }
      catch (error) {
        if (!cancelled && mountedRef.current) { setStreaming(false); setLocalError(String(error instanceof Error ? error.message : error)); }
        return;
      }
      // One command in flight; target rate is a ceiling, never an unbounded queue.
      if (!cancelled) timer = window.setTimeout(next, Math.max(0, 1000 / rate - (performance.now() - started)));
    };
    timer = window.setTimeout(next, 0);
    return () => { cancelled = true; window.clearTimeout(timer); };
  }, [rate, streaming]);

  const execute = async (action: LabAction, label: string, recovery = false) => {
    setLocalError('');
    try { await lab.run(action, label, recovery); }
    catch (error) { if (mountedRef.current) setLocalError(error instanceof Error ? error.message : String(error)); }
  };
  const runScenario = () => {
    if (!lab.snapshots.base) return;
    try {
      const action = scenarioAction(scenario.id, lab.snapshots.base, threshold, lab.snapshots.ranked);
      lab.clear();
      setSelectedNode(scenario.id === 'excluded' ? 'base' : scenario.id === 'recovery' ? 'high-value' : 'ranked');
      void execute(action, scenario.title, scenario.id === 'recovery');
    } catch (error) { setLocalError(error instanceof Error ? error.message : String(error)); }
  };
  const selectNode = (id: string) => { setSelectedNode(id); setEdit(null); };
  const error = localError || lab.problem || Object.values(lab.errors)[0];
  const peerUrl = `${window.location.pathname}?client=${clientB ? 'a' : 'b'}#lab`;

  return <div className="orders-lab">
    <header className="lab-header"><a className="lab-brand" href="#lab" aria-label="LiveTable Lab home"><span className="brand-symbol"><i /><i /><i /></span>livetable<span className="brand-divider" /> <span className="brand-lab">LAB</span></a>
      <nav aria-label="Main navigation"><span className="nav-current">Orders playground</span><a href="#editor">Table editor <Glyph name="arrow" /></a><a href={peerUrl} target="_blank" rel="noreferrer">Open client {clientB ? 'A' : 'B'} <Glyph name="external" /></a></nav>
      <span className={`connection-pill ${lab.connected ? 'connected' : ''}`}><i />{lab.connected ? 'Local server connected' : 'Connecting to local server'}</span>
    </header>

    <main className="lab-main">
      <section className="lab-hero"><div><div className="eyebrow"><span className="small-cross">+</span> A LIVE DATA ENGINE, UNDER THE MICROSCOPE</div><h1>Small changes.<br /><em>Live consequences.</em></h1><p>A working order stream. A branching pipeline. Only the changes that matter.<br className="desktop-break" /> See how LiveTable keeps every view in sync.</p></div>
        <div className="hero-note"><span className="note-index">/ 001</span><span>THE ORDERS LAB</span><p>Synthetic commerce.<br />Real Rust. Real WebSockets.<br />Nothing simulated on the wire.</p><div className="protocol-note">PROTOCOL V3 <span>·</span> CLIENT {clientB ? 'B' : 'A'}</div></div>
      </section>

      {!ready && <div className="server-notice" role="status"><strong>{lab.connected ? 'Waiting for the Orders lab' : 'Start the local lab server'}</strong><code>cargo run --release --manifest-path impl/Cargo.toml --features server --bin livetable-server -- --lab</code><span>The lab is opt-in and loopback-only. Existing “demo” editor data is untouched.</span></div>}
      {error && <div className="lab-error" role="alert">{error}</div>}

      <section className="lab-toolbar" aria-label="Lab controls"><div className="mode-switch" aria-label="Experience mode"><button aria-pressed={mode === 'guided'} onClick={() => { setMode('guided'); setStreaming(false); }}>Guided tour <span>05</span></button><button aria-pressed={mode === 'explore'} onClick={() => setMode('explore')}>Explore & stream</button></div>
        <div className="dataset-controls"><label>DATASET<select aria-label="Dataset size" value={size} disabled={lab.busy || streaming} onChange={event => setSize(Number(event.target.value))}><option value={1000}>1,000 orders</option><option value={10000}>10,000 orders</option><option value={100000}>100,000 orders</option></select></label><button className="quiet-button" disabled={disabled} onClick={() => setResetConfirm(true)}><Glyph name="reset" /> Reset / load</button></div>
      </section>

      {resetConfirm && <div className="reset-confirm" role="alertdialog" aria-label="Reset synthetic orders"><div><strong>Reset the shared lab to {size.toLocaleString()} seeded orders?</strong><p>This replaces synthetic “lab” data for every connected client. The “demo” editor table is not affected.</p></div><button className="quiet-button" onClick={() => setResetConfirm(false)}>Cancel</button><button className="primary-button" disabled={disabled} onClick={() => { setResetConfirm(false); setEdit(null); lab.clear(); void execute({ kind: 'reset', rows: size }, 'Dataset reset'); }}>Reset shared lab</button></div>}

      {mode === 'guided' ? <>
        <div className="tour-steps" aria-label="Guided scenarios">{SCENARIOS.map((item, index) => <button key={item.id} disabled={lab.busy} aria-current={scenarioIndex === index ? 'step' : undefined} onClick={() => { setScenarioIndex(index); setLocalError(''); }}><span>{item.number}</span>{item.tag}<Glyph name="arrow" /></button>)}</div>
        <section className="scenario-card"><div className="scenario-number">{scenario.number}<span>/ 05</span></div><div className="scenario-copy"><span className="eyebrow">{scenario.tag}</span><h2>{scenario.title}</h2><p>{scenario.description}</p><div className="expected-path">{scenario.expected}</div></div><div className="scenario-actions">{scenario.id === 'clients' && <a className="secondary-button" href={peerUrl} target="_blank" rel="noreferrer">Open the other client <Glyph name="external" /></a>}<button className="primary-button" disabled={disabled} onClick={runScenario}>{lab.busy ? 'Following the change…' : scenario.action}<Glyph name="arrow" /></button><span>Live server mutation · repeatable after reset</span></div></section>
      </> : <section className="explore-controls"><div><span className="eyebrow">BOUNDED MIXED WORKLOAD</span><h2>Make it move.</h2><p>Each batch updates, deletes and inserts. Row count stays steady.</p></div><label>Target batches / sec<select aria-label="Target batches per second" value={rate} disabled={streaming || lab.busy} onChange={event => setRate(Number(event.target.value))}><option value={1}>1 / sec</option><option value={2}>2 / sec</option><option value={5}>5 / sec</option><option value={10}>10 / sec</option></select></label><button className="secondary-button" disabled={disabled} onClick={() => void execute({ kind: 'step' }, 'Single mixed batch')}>Single step <Glyph name="arrow" /></button><button className="primary-button" disabled={!ready || (!streaming && lab.busy)} onClick={() => setStreaming(!streaming)}><Glyph name={streaming ? 'pause' : 'play'} />{streaming ? 'Pause stream' : 'Start stream'}</button><span className="flow-control-note">One batch in flight. Slows down automatically when delivery takes longer.</span></section>}

      <section className="metrics-strip" aria-label="Measured delivery metrics"><div><span>RECEIVED JSON</span><strong>{formatBytes(lab.stats.bytes)}</strong><small>Since trace start / clear</small></div><div><span>DELTA DELIVERIES</span><strong>{lab.stats.deltas.toLocaleString()}<i className="metric-accent">↗</i></strong><small>Compact row operations</small></div><div><span>SNAPSHOT DELIVERIES</span><strong>{lab.stats.snapshots.toLocaleString()}</strong><small>Initial, group, fallback or repair</small></div><div><span>RECOVERIES</span><strong>{lab.stats.recovered}<small className="inline-small"> / {lab.stats.repairs} requests</small></strong><small>{lab.stats.statuses} watermarks received</small></div><button className="clear-trace" disabled={lab.busy || streaming} onClick={lab.clear} title="Clear client counters and trace; keep server data">Clear trace <Glyph name="reset" /></button></section>

      <div className="lab-workspace"><div className="workspace-main"><PipelineGraph snapshots={lab.snapshots} stats={lab.stats} selected={selectedNode} onSelect={selectNode} threshold={threshold} />
        <section className="results-section"><div className="results-heading"><div><span className="eyebrow">MATERIALIZED CLIENT STATE</span><h2>{NODE_LABELS[selectedNode]} <span>{lab.snapshots[selectedNode]?.rows.length.toLocaleString('en-US') ?? '—'}</span></h2></div><label className="threshold-control">My threshold<select aria-label="Client filter threshold" value={threshold} disabled={lab.busy || streaming} onChange={event => { setThreshold(Number(event.target.value)); setEdit(null); }}><option value={500}>$500</option><option value={1000}>$1,000</option><option value={2500}>$2,500</option><option value={4000}>$4,000</option></select></label></div>
          <DataViewport key={selectedNode} snapshot={lab.snapshots[selectedNode]} delivery={lab.stats.nodes[selectedNode]} selectedId={edit?.id ?? null} onSelect={record => setEdit({ id: record.row_id!, order: formatCell('order', record.row.order), amount: String(record.row.amount) })} />
          <div className="table-footer"><span><span className="small-dot" /> {selectedNode === 'base' ? 'Select an order ID to edit its value' : 'Derived view · edit the source to change it'}</span><span>Virtualized viewport · all rows scrollable</span></div>
          {edit && <form className="order-editor" onSubmit={event => { event.preventDefault(); const amount = Number(edit.amount); if (!edit.amount.trim() || !Number.isFinite(amount) || amount < 0 || amount > 1_000_000) { setLocalError('Enter an order value between 0 and 1,000,000.'); return; } void execute({ kind: 'update', row_id: edit.id, amount }, `Edited ${edit.order}`); }}><strong>{edit.order}</strong><label>Order value ($)<input aria-label="Selected order value" inputMode="decimal" value={edit.amount} onChange={event => setEdit({ ...edit, amount: event.target.value })} /></label><button className="primary-button" disabled={disabled} type="submit">Apply change</button><button className="quiet-button" type="button" onClick={() => setEdit(null)}>Close</button></form>}
        </section>
        <div className="lab-result" role="status">{lab.busy ? <><span className="activity-dot is-active" /> Waiting for deliveries{scenario.id === 'recovery' ? ' and recovery' : ''}…</> : lab.result ? <><Glyph name="check" /><strong>{lab.result.label}</strong><span>{lab.result.ms.toFixed(1)} ms send → client-state completion</span></> : <><span className="small-cross">+</span><span>Ready when you are. Run a scenario and follow its deliveries.</span></>}</div>
        <p className="measurement-note">Completion is the command reply after queued deliveries, plus repair when injected. Includes client parsing and reconciliation; excludes React commit and browser paint. Concurrent clients can contribute traffic. Not an engine-only benchmark.</p>
      </div><DeliveryInspector stats={lab.stats} busy={lab.busy} /></div>

      <section className="under-the-hood"><button onClick={() => setShowStorage(!showStorage)} aria-expanded={showStorage}><span><span className="small-cross">+</span> Under the hood <small>Typed storage & honest boundaries</small></span><span>{showStorage ? '−' : '+'}</span></button>{showStorage && <div className="storage-details"><div><h3>Native-width columns</h3><p>Order and quantity use INT32 buffers. Amount uses FLOAT64. Repeated region and product strings use interned IDs. These are server storage choices, not a zero-copy JSON transport.</p></div><div><h3>Measure memory separately</h3><p>No estimated “memory saved” gauge here. Run the repository’s column-layout benchmark for allocator-counted storage comparisons. Client row arrays and view caches have their own costs.</p><code>cargo run --release --manifest-path impl/Cargo.toml --example column_layout_benchmark -- 10000 100000</code></div><div><h3>Know the limits</h3><p>Initial and repair snapshots are unchunked. Client delta application still copies row-array references. Group results are snapshots. The lab has no persistence or authentication and is for local synthetic data only.</p></div></div>}</section>
      <footer className="lab-footer"><span>livetable <span className="small-cross">+</span> LITTLE CHANGES. LIVE VIEWS.</span><span>Synthetic data · in-memory server · protocol v3</span></footer>
    </main>
  </div>;
}
