/**
 * `<ct-topology>`: the communication graph, drawn with sigma (WebGL).
 *
 * Inputs: `data-src` (a `/data/topology?…` URL), `data-highlight` (a
 * selection value to light up), `data-collapse` (`"true"` draws sub-agents
 * as their parent). Output: `value` in the topology selection grammar
 * (`shared/selection.ts`), announced with `change`.
 *
 * With `data-live` (the feed URL), a `watermark` event refetches `data-src`
 * and merges it into the drawn graph (`merge.ts`) rather than rebuilding:
 * the sigma instance, camera, positions, selection and hover are kept.
 * Edges whose traffic rose, and new nodes, pulse (`flash.ts`). The page's
 * `[data-topology-stat]` elements (agents, edges, transmissions, watermark)
 * are kept current from the same payload.
 */

import { createEdgeCurveProgram } from '@sigma/edge-curve';
import Graph from 'graphology';
import Sigma from 'sigma';
import {
  DEFAULT_EDGE_ARROW_HEAD_PROGRAM_OPTIONS,
  EdgeArrowProgram,
  NodeCircleProgram,
} from 'sigma/rendering';
import type { Settings } from 'sigma/settings';
import type { EdgeDisplayData, NodeDisplayData, PartialButFor } from 'sigma/types';
import { type TopologyPayload, topologyPayload } from '../payloads/topology.ts';
import { mix, parseColor, toCss, withAlpha } from '../shared/color.ts';
import { PayloadElement } from '../shared/element.ts';
import { type LoadError, loadJson } from '../shared/fetch.ts';
import { formatUtc } from '../shared/format.ts';
import { LiveRefetch } from '../shared/live-refetch.ts';
import type { Result } from '../shared/result.ts';
import { ROUTE_KINDS } from '../shared/route.ts';
import {
  decodeTopologySelection,
  encodeTopologySelection,
  type TopologySelection,
} from '../shared/selection.ts';
import type { Ulid } from '../shared/ulid.ts';
import { releaseWebGL } from '../shared/webgl.ts';
import { curvatures } from './curvature.ts';
import { NodeDiamondProgram } from './diamond-program.ts';
import { edgeCounts, type Flashes, prune, risenEdges, strength } from './flash.ts';
import { layout } from './layout.ts';
import { planMerge } from './merge.ts';
import {
  buildModel,
  edgeSelection,
  type GraphEdge,
  type GraphModel,
  type GraphNode,
  type Highlight,
  highlightOf,
  nodeSelection,
} from './model.ts';
import {
  DIAMOND_SCALE,
  dimmedEdge,
  dimmedNode,
  edgeColor,
  nodeColor,
  shortLabel,
} from './style.ts';
import { edgeTooltip, nodeTooltip, type TooltipLine } from './tooltip.ts';

const STYLES = `
:host { height: 480px; }
.stage { cursor: default; }
.legend {
  position: absolute;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  flex-wrap: wrap;
  gap: 2px 10px;
  padding: 3px 8px 5px;
  color: var(--ct-muted);
  font-size: 11px;
  pointer-events: none;
}
.legend b { font-weight: 500; color: var(--ct-text); }
.legend b:not(:first-child) { margin-left: 6px; }
.legend span { display: inline-flex; align-items: center; gap: 4px; white-space: nowrap; }
.legend i { display: inline-block; width: 12px; height: 3px; border-radius: 2px; }
.legend i.disc { width: 8px; height: 8px; border-radius: 50%; }
.legend i.diamond { width: 7px; height: 7px; border-radius: 1px; transform: rotate(45deg); }
.meta {
  position: absolute;
  right: 8px;
  top: 6px;
  color: var(--ct-muted);
  font-size: 11px;
  pointer-events: none;
}
`;

type NodeAttributes = {
  x: number;
  y: number;
  size: number;
  label: string;
  color: string;
  type: 'circle' | 'diamond';
  zIndex: number;
};
type EdgeAttributes = {
  size: number;
  color: string;
  type: 'arrow' | 'curved';
  curvature: number;
  zIndex: number;
};

/** The colour new traffic flashes towards. */
const FLASH_COLOR = parseColor('#f59e0b') ?? ([245, 158, 11, 1] as const);

/** Arrows that bend, for edges sharing a pair of nodes (see `curvature.ts`). */
const EdgeCurvedArrowProgram = createEdgeCurveProgram<NodeAttributes, EdgeAttributes>({
  arrowHead: DEFAULT_EDGE_ARROW_HEAD_PROGRAM_OPTIONS,
});

export class TopologyElement extends PayloadElement<TopologyPayload> {
  static observedAttributes = ['data-src', 'data-highlight', 'data-collapse', 'data-live'];

  #payload: TopologyPayload | null = null;
  #model: GraphModel | null = null;
  #renderer: Sigma<NodeAttributes, EdgeAttributes> | null = null;
  #resize: ResizeObserver | null = null;
  #nodes = new Map<string, GraphNode>();
  #edges = new Map<string, GraphEdge>();
  #names = new Map<Ulid, string>();
  #highlight: Highlight | null = null;
  #hoveredNode: string | null = null;
  #graph: Graph<NodeAttributes, EdgeAttributes> | null = null;
  #counts = new Map<string, number>();
  readonly #edgeFlashes: Flashes = new Map();
  readonly #nodeFlashes: Flashes = new Map();
  #frame: number | null = null;
  #refetch: AbortController | null = null;
  readonly #live = new LiveRefetch(() => void this.#liveTick());
  /** Live merges done and edges flashed, for diagnostics. */
  readonly liveStats = { merges: 0, flashedEdges: 0, addedNodes: 0 };

  /** The camera's state (zoom ratio, pan), for diagnostics; `null` before drawing. */
  get cameraState(): { x: number; y: number; ratio: number; angle: number } | null {
    return this.#renderer?.getCamera().getState() ?? null;
  }
  readonly #tooltip: HTMLDivElement;
  readonly #legend: HTMLDivElement;
  readonly #meta: HTMLDivElement;

  constructor() {
    super(STYLES);
    this.#tooltip = document.createElement('div');
    this.#tooltip.className = 'tooltip';
    this.#tooltip.hidden = true;
    this.#legend = document.createElement('div');
    this.#legend.className = 'legend';
    this.#meta = document.createElement('div');
    this.#meta.className = 'meta';
    this.frame.append(this.#legend, this.#meta, this.#tooltip);
  }

  protected load(url: string, signal: AbortSignal): Promise<Result<TopologyPayload, LoadError>> {
    return loadJson(url, signal, topologyPayload);
  }

  protected emptyMessage(payload: TopologyPayload): string | null {
    if (payload.nodes.length > 0) return null;
    return `No ${payload.mode === 'agents' ? 'transmissions' : 'agents or channels'} in this window and filter.`;
  }

  protected mount(payload: TopologyPayload): void {
    this.#payload = payload;
    this.#draw();
  }

  override connectedCallback(): void {
    super.connectedCallback();
    this.#live.start(this.dataset.live ?? '');
  }

  override disconnectedCallback(): void {
    this.#live.stop();
    this.#refetch?.abort();
    this.#refetch = null;
    super.disconnectedCallback();
  }

  async #liveTick(): Promise<void> {
    const src = this.dataset.src?.trim() ?? '';
    if (src === '') return;
    this.#refetch?.abort();
    const controller = new AbortController();
    this.#refetch = controller;
    const result = await this.load(src, controller.signal);
    if (this.#refetch !== controller) return;
    this.#refetch = null;
    if ((this.dataset.src?.trim() ?? '') !== src) return;
    if (!result.ok) {
      if (result.error.kind !== 'aborted') {
        console.warn('ct-topology: live refetch failed', { error: result.error.kind });
      }
      return;
    }
    const payload = result.value;
    if (this.emptyMessage(payload) !== null) return;
    if (this.#renderer === null || this.#graph === null) {
      this.hideStatus();
      this.mount(payload);
      return;
    }
    this.#merge(payload);
  }

  /** Merges a refetched payload into the drawn graph, keeping the renderer. */
  #merge(payload: TopologyPayload): void {
    const graph = this.#graph;
    if (graph === null) return;
    const model = buildModel(payload, this.dataset.collapse === 'true');
    const drawnNodes = new Map(
      graph.mapNodes((id, a) => [id, { x: a.x, y: a.y }] as [string, { x: number; y: number }]),
    );
    const plan = planMerge(drawnNodes, graph.edges(), model);
    const counts = edgeCounts(model.edges);
    const risen = risenEdges(this.#counts, counts);

    for (const key of plan.removedEdges) {
      if (graph.hasEdge(key)) graph.dropEdge(key);
      this.#edges.delete(key);
    }
    for (const id of plan.removedNodes) {
      if (graph.hasNode(id)) graph.dropNode(id);
      this.#nodes.delete(id);
      this.#names.delete(id as Ulid);
      if (this.#hoveredNode === id) this.#hoveredNode = null;
    }
    for (const node of model.nodes) {
      const at = plan.positions.get(node.id) ?? { x: 0, y: 0 };
      this.#nodes.set(node.id, node);
      this.#names.set(
        node.id,
        node.kind === 'agent' ? (node.members[0]?.name ?? node.label) : node.label,
      );
      const attributes = {
        size: node.kind === 'channel' ? node.size * DIAMOND_SCALE : node.size,
        label: shortLabel(node.label),
        color: toCss(nodeColor(node, this.theme)),
        type: node.kind === 'channel' ? ('diamond' as const) : ('circle' as const),
      };
      if (graph.hasNode(node.id)) graph.mergeNodeAttributes(node.id, attributes);
      else graph.addNode(node.id, { ...attributes, x: at.x, y: at.y, zIndex: 1 });
    }
    const drawn = model.edges.filter((e) => graph.hasNode(e.source) && graph.hasNode(e.target));
    const bends = curvatures(drawn);
    for (const edge of drawn) {
      this.#edges.set(edge.key, edge);
      const curvature = bends.get(edge.key) ?? 0;
      const attributes = {
        size: edge.width,
        color: toCss(edgeColor(edge, this.theme)),
        type: curvature === 0 ? ('arrow' as const) : ('curved' as const),
        curvature,
      };
      if (graph.hasEdge(edge.key)) graph.mergeEdgeAttributes(edge.key, attributes);
      else
        graph.addDirectedEdgeWithKey(edge.key, edge.source, edge.target, {
          ...attributes,
          zIndex: 0,
        });
    }

    this.#payload = payload;
    this.#model = model;
    this.#counts = counts;
    this.#highlight = highlightOf(model, this.#currentSelection());
    this.#renderLegend();
    this.#renderMeta(payload);

    const now = performance.now();
    for (const key of risen) if (graph.hasEdge(key)) this.#edgeFlashes.set(key, now);
    for (const id of plan.addedNodes) this.#nodeFlashes.set(id, now);
    this.liveStats.merges += 1;
    this.liveStats.flashedEdges += risen.size;
    this.liveStats.addedNodes += plan.addedNodes.size;
    console.info('ct-topology: live merge', {
      flashedEdges: risen.size,
      addedNodes: plan.addedNodes.size,
      removedNodes: plan.removedNodes.length,
      removedEdges: plan.removedEdges.length,
    });
    this.#animate();
  }

  #animate(): void {
    if (this.#frame !== null) return;
    const step = (): void => {
      this.#frame = null;
      const renderer = this.#renderer;
      if (renderer === null) return;
      const now = performance.now();
      const edges = prune(this.#edgeFlashes, now);
      const nodes = prune(this.#nodeFlashes, now);
      renderer.refresh();
      if (edges || nodes) this.#frame = requestAnimationFrame(step);
    };
    this.#frame = requestAnimationFrame(step);
  }

  /** Header numbers on the page, from the payload. */
  #publishStats(payload: TopologyPayload): void {
    const values: Record<string, string> = {
      watermark: `final up to ${formatUtc(payload.watermark)} UTC`,
    };
    if (payload.mode === 'agents') {
      values.agents = String(payload.nodes.filter((n) => n.kind === 'agent').length);
      values.edges = String(payload.edges.length);
      values.transmissions = String(
        payload.edges.reduce(
          (sum, e) => sum + (e.kind === 'transmission' ? e.transmissions : 0),
          0,
        ),
      );
    }
    for (const target of document.querySelectorAll<HTMLElement>('[data-topology-stat]')) {
      const value = values[target.dataset.topologyStat ?? ''];
      if (value !== undefined && target.textContent !== value) target.textContent = value;
    }
  }

  #renderMeta(payload: TopologyPayload): void {
    this.#meta.textContent = `${payload.mode === 'agents' ? 'agents' : 'channels'} · final up to ${formatUtc(payload.watermark)} UTC`;
    if (this.dataset.live !== undefined) this.#publishStats(payload);
  }

  protected unmount(): void {
    this.#kill();
    this.#graph = null;
    this.#counts = new Map();
    this.#payload = null;
    this.#model = null;
    this.#legend.replaceChildren();
    this.stage.style.bottom = '0';
    this.#meta.textContent = '';
  }

  protected inputChanged(name: string): void {
    if (name === 'data-highlight') {
      this.#applySelection(this.#selectionFromAttribute());
    } else if (name === 'data-collapse' && this.#payload !== null) {
      this.#draw();
    } else if (name === 'data-live') {
      this.#live.start(this.dataset.live ?? '');
    }
  }

  protected override valueChanged(value: string): void {
    const parsed = decodeTopologySelection(value);
    if (parsed.ok) this.#applySelection(parsed.value);
  }

  protected themeChanged(): void {
    if (this.#renderer === null) return;
    this.#renderer.setSetting('labelColor', { color: toCss(this.theme.text) });
    this.#renderer.setSetting('labelFont', this.theme.font);
    this.#renderLegend();
    this.#renderer.refresh();
  }

  #selectionFromAttribute(): TopologySelection {
    const parsed = decodeTopologySelection(this.dataset.highlight ?? '');
    return parsed.ok ? parsed.value : { kind: 'none' };
  }

  #applySelection(selection: TopologySelection): void {
    this.#highlight = this.#model === null ? null : highlightOf(this.#model, selection);
    this.#renderer?.refresh({ skipIndexation: true });
  }

  #kill(): void {
    if (this.#frame !== null) cancelAnimationFrame(this.#frame);
    this.#frame = null;
    this.#edgeFlashes.clear();
    this.#nodeFlashes.clear();
    this.#resize?.disconnect();
    this.#resize = null;
    if (this.#renderer === null) return;
    const canvases = Object.values(this.#renderer.getCanvases());
    this.#renderer.kill();
    for (const canvas of canvases) releaseWebGL(canvas);
    this.#renderer = null;
    this.#tooltip.hidden = true;
  }

  #draw(): void {
    const payload = this.#payload;
    if (payload === null) return;
    this.#kill();
    const model = buildModel(payload, this.dataset.collapse === 'true');
    this.#model = model;
    const positions = layout(model);
    // Before sigma measures the stage, which the legend strip shortens.
    this.#renderLegend();

    const graph = new Graph<NodeAttributes, EdgeAttributes>({ type: 'directed', multi: true });
    this.#graph = graph;
    this.#nodes.clear();
    this.#edges.clear();
    this.#names.clear();
    for (const node of model.nodes) {
      const position = positions.get(node.id) ?? { x: 0, y: 0 };
      this.#nodes.set(node.id, node);
      this.#names.set(
        node.id,
        node.kind === 'agent' ? (node.members[0]?.name ?? node.label) : node.label,
      );
      graph.addNode(node.id, {
        x: position.x,
        y: position.y,
        size: node.kind === 'channel' ? node.size * DIAMOND_SCALE : node.size,
        label: shortLabel(node.label),
        color: toCss(nodeColor(node, this.theme)),
        type: node.kind === 'channel' ? 'diamond' : 'circle',
        zIndex: 1,
      });
    }
    const drawn = model.edges.filter((e) => graph.hasNode(e.source) && graph.hasNode(e.target));
    const bends = curvatures(drawn);
    for (const edge of drawn) {
      this.#edges.set(edge.key, edge);
      const curvature = bends.get(edge.key) ?? 0;
      graph.addDirectedEdgeWithKey(edge.key, edge.source, edge.target, {
        size: edge.width,
        color: toCss(edgeColor(edge, this.theme)),
        type: curvature === 0 ? 'arrow' : 'curved',
        curvature,
        zIndex: 0,
      });
    }

    const settings: Partial<Settings<NodeAttributes, EdgeAttributes>> = {
      allowInvalidContainer: true,
      enableEdgeEvents: true,
      renderEdgeLabels: false,
      defaultEdgeType: 'arrow',
      nodeProgramClasses: { circle: NodeCircleProgram, diamond: NodeDiamondProgram },
      edgeProgramClasses: { arrow: EdgeArrowProgram, curved: EdgeCurvedArrowProgram },
      labelFont: this.theme.font,
      labelSize: 11,
      labelWeight: '500',
      labelColor: { color: toCss(this.theme.text) },
      labelDensity: 1.2,
      labelGridCellSize: 60,
      labelRenderedSizeThreshold: 4,
      stagePadding: 36,
      zIndex: true,
      minCameraRatio: 0.08,
      maxCameraRatio: 4,
      defaultDrawNodeLabel: (context, data, s) => this.#drawLabel(context, data, s.labelSize),
      defaultDrawNodeHover: (context, data, s) => this.#drawHover(context, data, s.labelSize),
      nodeReducer: (key, data) => this.#reduceNode(key, data),
      edgeReducer: (key, data) => this.#reduceEdge(key, data),
    };
    this.#counts = edgeCounts(drawn);
    const renderer = new Sigma<NodeAttributes, EdgeAttributes>(graph, this.stage, settings);
    this.#renderer = renderer;
    // Sigma only watches the window; the stage also changes with the legend
    // strip and the page layout.
    this.#resize = new ResizeObserver(() => this.#renderer?.refresh());
    this.#resize.observe(this.stage);
    this.#highlight = highlightOf(model, this.#currentSelection());

    renderer.on('clickNode', ({ node }) => {
      const drawn = this.#nodes.get(node);
      if (drawn !== undefined) this.#choose(nodeSelection(drawn));
    });
    renderer.on('clickEdge', ({ edge }) => {
      const drawn = this.#edges.get(edge);
      if (drawn !== undefined) this.#choose(edgeSelection(drawn));
    });
    renderer.on('clickStage', () => this.#choose({ kind: 'none' }));
    renderer.on('enterNode', ({ node, event }) => {
      this.#hoveredNode = node;
      const drawn = this.#nodes.get(node);
      if (drawn !== undefined) this.#showTooltip(nodeTooltip(drawn), event.x, event.y);
      this.stage.style.cursor = 'pointer';
    });
    renderer.on('leaveNode', () => {
      this.#hoveredNode = null;
      this.#hideTooltip();
    });
    renderer.on('enterEdge', ({ edge, event }) => {
      const drawn = this.#edges.get(edge);
      if (drawn !== undefined) this.#showTooltip(edgeTooltip(drawn, this.#names), event.x, event.y);
      this.stage.style.cursor = 'pointer';
    });
    renderer.on('leaveEdge', () => this.#hideTooltip());

    this.#renderMeta(payload);
  }

  /** The highlight source: the page's `data-highlight`, else our own value. */
  #currentSelection(): TopologySelection {
    const fromAttribute = this.#selectionFromAttribute();
    if (fromAttribute.kind !== 'none') return fromAttribute;
    const own = decodeTopologySelection(this.value);
    return own.ok ? own.value : { kind: 'none' };
  }

  #choose(selection: TopologySelection): void {
    this.select(encodeTopologySelection(selection));
    this.#applySelection(selection);
  }

  // Colours are computed here, from the current theme, rather than stored on
  // the graph, so a colour-scheme change only needs a refresh.
  #reduceNode(key: string, data: NodeAttributes): Partial<NodeDisplayData> {
    const node = this.#nodes.get(key);
    const color = node === undefined ? this.theme.faint : nodeColor(node, this.theme);
    const highlight = this.#highlight;
    const flash = strength(this.#nodeFlashes, key, performance.now());
    if (flash > 0) {
      return {
        ...data,
        size: data.size * (1 + 0.9 * flash),
        color: toCss(withAlpha(mix(color, FLASH_COLOR, 0.7 * flash), 1)),
        zIndex: 3,
        forceLabel: true,
      };
    }
    if (highlight === null || highlight.nodes.has(key as Ulid)) {
      return {
        ...data,
        color: toCss(color),
        zIndex: highlight === null ? 1 : 2,
        forceLabel: highlight !== null,
      };
    }
    return { ...data, color: toCss(dimmedNode(color, this.theme)), label: null, zIndex: 0 };
  }

  #reduceEdge(key: string, data: EdgeAttributes): Partial<EdgeDisplayData> {
    const edge = this.#edges.get(key);
    if (edge === undefined) return data;
    const color = edgeColor(edge, this.theme);
    const highlight = this.#highlight;
    const hovered = this.#hoveredNode;
    const touchesHover = hovered !== null && (edge.source === hovered || edge.target === hovered);
    const flash = strength(this.#edgeFlashes, key, performance.now());
    if (flash > 0) {
      return {
        ...data,
        size: data.size * (1 + 1.5 * flash) + 2 * flash,
        color: toCss(withAlpha(mix(color, FLASH_COLOR, 0.85 * flash), 1)),
        zIndex: 3,
      };
    }
    if (highlight === null) return { ...data, color: toCss(color), zIndex: touchesHover ? 1 : 0 };
    if (highlight.edges.has(key)) return { ...data, color: toCss(color), zIndex: 2 };
    return { ...data, color: toCss(dimmedEdge(color, this.theme)), zIndex: 0 };
  }

  #drawLabel(
    context: CanvasRenderingContext2D,
    data: PartialButFor<NodeDisplayData, 'x' | 'y' | 'size' | 'label' | 'color'>,
    size: number,
  ): void {
    if (!data.label) return;
    context.font = `500 ${size}px ${this.theme.font}`;
    const x = data.x + data.size + 4;
    const y = data.y + size / 3;
    context.lineJoin = 'round';
    context.lineWidth = 3;
    context.strokeStyle = toCss(withAlpha(this.theme.surface, 0.85));
    context.strokeText(data.label, x, y);
    context.fillStyle = toCss(this.theme.text);
    context.fillText(data.label, x, y);
  }

  #drawHover(
    context: CanvasRenderingContext2D,
    data: PartialButFor<NodeDisplayData, 'x' | 'y' | 'size' | 'label' | 'color'>,
    size: number,
  ): void {
    const label = data.label ?? '';
    context.font = `600 ${size}px ${this.theme.font}`;
    const width = context.measureText(label).width;
    const pad = 3;
    const x = data.x + data.size + 2;
    context.fillStyle = toCss(this.theme.surface);
    context.strokeStyle = toCss(this.theme.faint);
    context.lineWidth = 1;
    context.beginPath();
    context.roundRect(x, data.y - size / 2 - pad, width + 2 * pad + 2, size + 2 * pad, 3);
    context.fill();
    context.stroke();
    context.beginPath();
    context.arc(data.x, data.y, data.size + 2, 0, 2 * Math.PI);
    context.strokeStyle = toCss(this.theme.text);
    context.lineWidth = 1.5;
    context.stroke();
    context.fillStyle = toCss(this.theme.text);
    context.fillText(label, x + pad + 1, data.y + size / 3);
  }

  #showTooltip(lines: readonly TooltipLine[], x: number, y: number): void {
    const rows = lines.map((l) => {
      const row = document.createElement('div');
      if (l.style === 'claim') {
        const badge = document.createElement('span');
        badge.className = 'claim';
        badge.textContent = l.text;
        row.append(badge);
      } else {
        row.textContent = l.text;
        if (l.style !== 'plain') row.className = l.style === 'title' ? 'title' : 'dim';
      }
      if (l.detail !== undefined) row.title = l.detail;
      return row;
    });
    this.#tooltip.replaceChildren(...rows);
    this.#tooltip.hidden = false;
    const bounds = this.getBoundingClientRect();
    const tip = this.#tooltip.getBoundingClientRect();
    const left = x + 14 + tip.width > bounds.width ? x - 14 - tip.width : x + 14;
    const top = Math.min(Math.max(4, y + 12), Math.max(4, bounds.height - tip.height - 4));
    this.#tooltip.style.left = `${Math.max(4, left)}px`;
    this.#tooltip.style.top = `${top}px`;
  }

  #hideTooltip(): void {
    this.#tooltip.hidden = true;
    this.stage.style.cursor = 'default';
  }

  #renderLegend(): void {
    const payload = this.#payload;
    if (payload === null) return;
    const present = new Set(this.#model?.edges.map((e) => e.routeKind) ?? []);
    const items: HTMLElement[] = [];
    const swatch = (color: string, shape: '' | 'disc' | 'diamond', text: string) => {
      const item = document.createElement('span');
      const mark = document.createElement('i');
      if (shape !== '') mark.className = shape;
      mark.style.background = color;
      item.append(mark, text);
      items.push(item);
    };
    const group = (text: string) => {
      const label = document.createElement('b');
      label.textContent = text;
      items.push(label);
    };
    group('route');
    for (const kind of ROUTE_KINDS) {
      if (present.has(kind)) swatch(toCss(this.theme.route[kind]), '', kind);
    }
    group('node');
    swatch(toCss(this.theme.agent), 'disc', 'agent');
    if (payload.mode === 'channels') {
      const policies = new Set(
        payload.nodes.flatMap((n) => (n.kind === 'channel' ? [n.policy] : [])),
      );
      group('channel policy');
      for (const policy of ['unreviewed', 'sanctioned', 'unsanctioned'] as const) {
        if (policies.has(policy)) swatch(toCss(this.theme.policy[policy]), 'diamond', policy);
      }
    }
    this.#legend.replaceChildren(...items);
    // The legend is a strip below the graph, never over it.
    this.stage.style.bottom = `${this.#legend.offsetHeight}px`;
  }
}
