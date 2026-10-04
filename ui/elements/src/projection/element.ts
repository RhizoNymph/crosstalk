/**
 * `<ct-projection>`: a stored UMAP projection, drawn with regl-scatterplot
 * (WebGL).
 *
 * Inputs: `data-src` (a `/data/projection/{id}` URL), `data-color-by`
 * (`topic | sender | reader | route | channel`), `data-highlight`
 * (comma-separated transmission ULIDs; the rest dim). Output: `value` in
 * the projection selection grammar (`shared/selection.ts`): a lasso polygon
 * in projection coordinates, or one point.
 */

import createScatterplot from 'regl-scatterplot';
import { decodeProjection, NONE, type Projection } from '../payloads/projection.ts';
import { mix, type Rgba, toCss } from '../shared/color.ts';
import { PayloadElement } from '../shared/element.ts';
import { type LoadError, loadBinary } from '../shared/fetch.ts';
import { formatCount, formatUtc } from '../shared/format.ts';
import type { Result } from '../shared/result.ts';
import { ROUTE_KINDS } from '../shared/route.ts';
import { decodeProjectionSelection, encodeProjectionSelection } from '../shared/selection.ts';
import { releaseWebGL } from '../shared/webgl.ts';
import { type ColorBy, type Coloring, colorPoints, highlightMask, parseColorBy } from './colors.ts';
import { lassoPolygon, pointsInPolygon, vertexList } from './lasso.ts';
import { fitTransform, normalizeColumns, type Transform } from './transform.ts';

type Scatterplot = ReturnType<typeof createScatterplot>;

const STYLES = `
:host { height: 420px; }
canvas { position: absolute; inset: 0; width: 100%; height: 100%; display: block; }
.meta, .hint {
  position: absolute;
  left: 8px;
  color: var(--ct-muted);
  font-size: 11px;
  pointer-events: none;
}
.meta { top: 6px; }
.hint { bottom: 6px; }
.legend {
  position: absolute;
  top: 6px;
  right: 8px;
  max-width: 45%;
  max-height: calc(100% - 12px);
  overflow: auto;
  padding: 4px 6px;
  border-radius: 4px;
  background: var(--ct-veil);
  font-size: 11px;
}
.legend .head { color: var(--ct-muted); margin-bottom: 2px; }
.legend .row { display: flex; align-items: center; gap: 6px; white-space: nowrap; }
.legend .row i { flex: none; width: 8px; height: 8px; border-radius: 50%; }
.legend .row .label { overflow: hidden; text-overflow: ellipsis; max-width: 22em; }
.legend .row .label.hidden { font-style: italic; color: var(--ct-muted); }
.legend .row .num { margin-left: auto; padding-left: 8px; color: var(--ct-muted); }
`;

const toUnit = (c: Rgba): [number, number, number, number] => [
  c[0] / 255,
  c[1] / 255,
  c[2] / 255,
  c[3],
];

function pointSizeFor(count: number): number {
  return Math.max(2, Math.min(6, 5 * Math.sqrt(2000 / Math.max(1, count))));
}

export class ProjectionElement extends PayloadElement<Projection> {
  static observedAttributes = ['data-src', 'data-color-by', 'data-highlight'];

  #projection: Projection | null = null;
  #transform: Transform = { cx: 0, cy: 0, scale: 1 };
  #coloring: Coloring | null = null;
  #plot: Scatterplot | null = null;
  #canvas: HTMLCanvasElement | null = null;
  #resize: ResizeObserver | null = null;
  #normalized: { x: Float32Array; y: Float32Array } | null = null;
  #lassoing = false;
  readonly #legend: HTMLDivElement;
  readonly #meta: HTMLDivElement;
  readonly #hint: HTMLDivElement;
  readonly #tooltip: HTMLDivElement;

  constructor() {
    super(STYLES);
    this.#legend = document.createElement('div');
    this.#legend.className = 'legend';
    this.#legend.hidden = true;
    this.#meta = document.createElement('div');
    this.#meta.className = 'meta';
    this.#hint = document.createElement('div');
    this.#hint.className = 'hint';
    this.#tooltip = document.createElement('div');
    this.#tooltip.className = 'tooltip';
    this.#tooltip.hidden = true;
    this.frame.append(this.#meta, this.#hint, this.#legend, this.#tooltip);
  }

  /** The drawn points' transmission ids, in payload order (empty until loaded). */
  get transmissionIds(): readonly string[] {
    return this.#projection?.ids ?? [];
  }

  protected load(url: string, signal: AbortSignal): Promise<Result<Projection, LoadError>> {
    return loadBinary(url, signal, decodeProjection);
  }

  protected emptyMessage(projection: Projection): string | null {
    return projection.ids.length > 0 ? null : 'This projection has no points.';
  }

  protected mount(projection: Projection): void {
    this.#destroyPlot();
    this.#projection = projection;
    this.#transform = fitTransform(projection.xs, projection.ys);
    this.#normalized = normalizeColumns(this.#transform, projection.xs, projection.ys);

    const canvas = document.createElement('canvas');
    this.stage.replaceChildren(canvas);
    this.#canvas = canvas;
    const { width, height } = this.stage.getBoundingClientRect();
    const theme = this.theme;
    const plot = createScatterplot({
      canvas,
      width: Math.max(1, Math.round(width)),
      height: Math.max(1, Math.round(height)),
      backgroundColor: toUnit(theme.surface),
      pointSize: pointSizeFor(projection.ids.length),
      pointSizeSelected: 2,
      pointOutlineWidth: 2,
      pointColorActive: toUnit(theme.text),
      pointColorHover: toUnit(theme.text),
      lassoColor: toUnit(mix(theme.text, theme.surface, 0.3)),
      lassoInitiator: true,
      lassoInitiatorParentElement: this.frame,
      lassoLongPressIndicatorParentElement: this.frame,
      lassoMinDelay: 10,
      lassoMinDist: 2,
      keyMap: { shift: 'lasso', alt: 'rotate', ctrl: 'merge', cmd: 'merge', meta: 'merge' },
      deselectOnDblClick: true,
      deselectOnEscape: true,
      showReticle: false,
      opacityInactiveScale: 1,
    });
    if (!plot.isSupported) throw new Error('WebGL is not available');
    this.#plot = plot;

    plot.subscribe('lassoStart', () => {
      this.#lassoing = true;
    });
    plot.subscribe('lassoEnd', ({ coordinates }) => this.#onLasso(coordinates));
    plot.subscribe('select', ({ points }) => {
      if (this.#lassoing) return;
      const [index] = points;
      const id = index === undefined ? undefined : projection.ids[index];
      if (points.length === 1 && id !== undefined) {
        this.select(encodeProjectionSelection({ kind: 'point', id }));
      }
    });
    plot.subscribe('deselect', () => {
      if (!this.#lassoing) this.select('');
    });
    plot.subscribe('pointOver', (index) => this.#showTooltip(index));
    plot.subscribe('pointOut', () => {
      this.#tooltip.hidden = true;
    });

    this.#resize = new ResizeObserver(([entry]) => {
      if (entry === undefined || this.#plot === null) return;
      const box = entry.contentRect;
      void this.#plot.set({
        width: Math.max(1, Math.round(box.width)),
        height: Math.max(1, Math.round(box.height)),
      });
    });
    this.#resize.observe(this.stage);

    this.#meta.textContent = `${formatCount(projection.ids.length)} transmissions · ${projection.header.embeddingModel.name} · fitted ${formatUtc(projection.header.fittedAt)} UTC`;
    this.#hint.textContent = 'drag to pan · scroll to zoom · shift-drag to lasso · click a point';
    this.#recolor();
    void this.#draw();
  }

  protected unmount(): void {
    this.#destroyPlot();
    this.#projection = null;
    this.#coloring = null;
    this.#legend.hidden = true;
    this.#meta.textContent = '';
    this.#hint.textContent = '';
  }

  protected inputChanged(name: string): void {
    if (this.#plot === null) return;
    if (name === 'data-color-by') this.#recolor();
    void this.#draw();
  }

  protected themeChanged(): void {
    if (this.#plot === null) return;
    const theme = this.theme;
    void this.#plot.set({
      backgroundColor: toUnit(theme.surface),
      pointColorActive: toUnit(theme.text),
      pointColorHover: toUnit(theme.text),
      lassoColor: toUnit(mix(theme.text, theme.surface, 0.3)),
    });
    this.#recolor();
    void this.#draw();
  }

  protected override valueChanged(value: string): void {
    const projection = this.#projection;
    const plot = this.#plot;
    const parsed = decodeProjectionSelection(value);
    if (projection === null || plot === null || !parsed.ok) return;
    const selection = parsed.value;
    if (selection.kind === 'none') plot.deselect({ preventEvent: true });
    else if (selection.kind === 'point') {
      const index = projection.ids.indexOf(selection.id);
      if (index >= 0) plot.select([index], { preventEvent: true });
    } else {
      plot.select(pointsInPolygon(projection.xs, projection.ys, selection.polygon), {
        preventEvent: true,
      });
    }
  }

  #destroyPlot(): void {
    this.#resize?.disconnect();
    this.#resize = null;
    this.#plot?.destroy();
    this.#plot = null;
    if (this.#canvas !== null) releaseWebGL(this.#canvas);
    this.#canvas = null;
    this.stage.replaceChildren();
    this.#tooltip.hidden = true;
  }

  #colorBy(): ColorBy {
    return parseColorBy(this.dataset.colorBy);
  }

  #recolor(): void {
    const projection = this.#projection;
    if (projection === null || this.#plot === null) return;
    const coloring = colorPoints(projection, this.#colorBy(), this.theme);
    this.#coloring = coloring;
    void this.#plot.set({ pointColor: coloring.palette.map(toUnit), colorBy: 'valueA' });
    this.#renderLegend(coloring);
  }

  async #draw(): Promise<void> {
    const plot = this.#plot;
    const projection = this.#projection;
    const coloring = this.#coloring;
    const normalized = this.#normalized;
    if (plot === null || projection === null || coloring === null || normalized === null) return;
    const mask = highlightMask(projection.ids, this.dataset.highlight ?? '');
    const valueB =
      mask === null ? new Float32Array(projection.ids.length) : Float32Array.from(mask);
    await plot.set(
      mask === null
        ? { opacityBy: null, opacity: 0.8 }
        : { opacityBy: 'valueB', opacity: [0.12, 0.95] },
    );
    await plot.draw(
      { x: normalized.x, y: normalized.y, valueA: coloring.categories, valueB },
      { zDataType: 'categorical', wDataType: 'continuous' },
    );
    this.valueChanged(this.value);
  }

  #onLasso(coordinates: unknown): void {
    this.#lassoing = false;
    const projection = this.#projection;
    const plot = this.#plot;
    if (projection === null || plot === null) return;
    const polygon = lassoPolygon(vertexList(coordinates), this.#transform);
    if (polygon === null) return;
    // Select exactly what the value selects, so the view matches the URL.
    plot.select(pointsInPolygon(projection.xs, projection.ys, polygon), { preventEvent: true });
    this.select(encodeProjectionSelection({ kind: 'lasso', polygon }));
  }

  #showTooltip(index: number): void {
    const projection = this.#projection;
    const plot = this.#plot;
    if (projection === null || plot === null) return;
    const { header } = projection;
    const topic = projection.topic[index] ?? NONE;
    const channel = projection.channel[index] ?? NONE;
    const sender = header.agents[projection.sender[index] ?? 0]?.name ?? '?';
    const reader = header.agents[projection.reader[index] ?? 0]?.name ?? '?';
    const route = ROUTE_KINDS[projection.route[index] ?? 0] ?? 'unobserved';
    const topicEntry = topic === NONE ? null : header.topics[topic];
    const rows: [string, string][] = [
      [
        topicEntry === null || topicEntry === undefined
          ? 'outlier'
          : (topicEntry.label ?? 'topic (content hidden)'),
        'title',
      ],
      [`${sender} → ${reader}`, ''],
      [channel === NONE ? route : `${route} · ${header.channels[channel]?.name ?? '?'}`, 'dim'],
      [projection.ids[index] ?? '', 'dim'],
    ];
    this.#tooltip.replaceChildren(
      ...rows.map(([text, cls]) => {
        const row = document.createElement('div');
        row.textContent = text;
        if (cls !== '') row.className = cls;
        return row;
      }),
    );
    const position = plot.getScreenPosition(index);
    if (position === undefined) return;
    this.#tooltip.hidden = false;
    const bounds = this.stage.getBoundingClientRect();
    const tip = this.#tooltip.getBoundingClientRect();
    const [x, y] = position;
    const left = x + 12 + tip.width > bounds.width ? x - 12 - tip.width : x + 12;
    this.#tooltip.style.left = `${Math.max(4, left)}px`;
    this.#tooltip.style.top = `${Math.min(Math.max(4, y + 10), bounds.height - tip.height - 4)}px`;
  }

  #renderLegend(coloring: Coloring): void {
    const head = document.createElement('div');
    head.className = 'head';
    head.textContent = `colour: ${this.#colorBy()}`;
    const rows = coloring.legend.map((entry) => {
      const row = document.createElement('div');
      row.className = 'row';
      const swatch = document.createElement('i');
      swatch.style.background = toCss(entry.color);
      const label = document.createElement('span');
      label.className = entry.hidden === true ? 'label hidden' : 'label';
      label.textContent = entry.label;
      label.title = entry.label;
      const count = document.createElement('span');
      count.className = 'num';
      count.textContent = formatCount(entry.count);
      row.append(swatch, label, count);
      return row;
    });
    this.#legend.replaceChildren(head, ...rows);
    this.#legend.hidden = false;
  }
}
