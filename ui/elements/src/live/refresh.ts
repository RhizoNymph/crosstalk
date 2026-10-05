/**
 * Refreshing the page's live regions in place.
 *
 * Topcoat's runtime (0.9) re-renders a page with the signal values the
 * browser holds when a window event `topcoat:dev-runtime:v1` asks it to:
 * the listener fills `detail.runtime` with `request(signal)` (the page
 * rendered again, as HTML) and `replace(update)` (release the bindings,
 * run `update`, hydrate the document again). The runtime registers that
 * listener on every page, not only under the dev server, so a refresh
 * that swaps `[data-live-region]` elements inside `replace` keeps the
 * page's signals and re-binds whatever the new markup holds.
 *
 * A refresh is skipped (and the element asks the user to reload instead)
 * when the runtime does not answer, the response is not the page, a region
 * is missing from it, or a region holds input the user is editing.
 */

const RUNTIME_EVENT = 'topcoat:dev-runtime:v1';
export const REGION_ATTRIBUTE = 'data-live-region';

interface PageRuntime {
  request(signal: AbortSignal): Promise<Response>;
  replace(update: () => void): void;
}

interface RuntimeDetail {
  runtime?: PageRuntime;
}

export type RefreshOutcome =
  | { readonly kind: 'refreshed' }
  | { readonly kind: 'aborted' }
  | { readonly kind: 'needs-reload'; readonly why: string };

const needsReload = (why: string): RefreshOutcome => ({ kind: 'needs-reload', why });

/** Whether a form control in `region` holds a value the user changed or is focused. */
export function isEditing(region: Element): boolean {
  const active = region.ownerDocument.activeElement;
  if (
    active !== null &&
    region.contains(active) &&
    (active instanceof HTMLInputElement ||
      active instanceof HTMLTextAreaElement ||
      active instanceof HTMLSelectElement)
  ) {
    return true;
  }
  for (const input of region.querySelectorAll('input, textarea')) {
    if (input instanceof HTMLInputElement) {
      if (input.type === 'checkbox' || input.type === 'radio') {
        if (input.checked !== input.defaultChecked) return true;
      } else if (input.type !== 'hidden' && input.value !== input.defaultValue) {
        return true;
      }
    } else if (input instanceof HTMLTextAreaElement && input.value !== input.defaultValue) {
      return true;
    }
  }
  for (const select of region.querySelectorAll('select')) {
    for (const option of select.options) {
      if (option.selected !== option.defaultSelected) return true;
    }
  }
  return false;
}

function pageRuntime(): PageRuntime | null {
  const detail: RuntimeDetail = {};
  window.dispatchEvent(new CustomEvent(RUNTIME_EVENT, { detail }));
  return detail.runtime ?? null;
}

/** Renders the page again and swaps every live region of the document. */
export async function refreshRegions(signal: AbortSignal): Promise<RefreshOutcome> {
  const regions = [...document.querySelectorAll(`[${REGION_ATTRIBUTE}]`)];
  if (regions.length === 0) return needsReload('the page has no live region');
  if (regions.some(isEditing)) return needsReload('you are editing a form on this page');
  const runtime = pageRuntime();
  if (runtime === null) return needsReload('the page runtime did not answer');
  let response: Response;
  try {
    response = await runtime.request(signal);
  } catch (error) {
    if (signal.aborted) return { kind: 'aborted' };
    if (error instanceof TypeError) return needsReload(`the page did not load: ${error.message}`);
    throw error;
  }
  if (signal.aborted) return { kind: 'aborted' };
  const type = response.headers.get('Content-Type')?.split(';')[0]?.trim();
  if (!response.ok || response.redirected || type !== 'text/html') {
    return needsReload(`the page answered ${response.status}`);
  }
  const html = await response.text();
  if (signal.aborted) return { kind: 'aborted' };
  const next = new DOMParser().parseFromString(html, 'text/html');
  const swaps: [Element, Element][] = [];
  for (const region of regions) {
    const name = region.getAttribute(REGION_ATTRIBUTE);
    const fresh = next.querySelector(`[${REGION_ATTRIBUTE}="${CSS.escape(name ?? '')}"]`);
    if (fresh === null) return needsReload(`the page no longer has region ${name}`);
    swaps.push([region, document.importNode(fresh, true)]);
  }
  // Re-checked after the request: the user may have started typing.
  if (regions.some(isEditing)) return needsReload('you are editing a form on this page');
  runtime.replace(() => {
    for (const [region, fresh] of swaps) region.replaceWith(fresh);
  });
  return { kind: 'refreshed' };
}
