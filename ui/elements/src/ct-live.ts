import { LiveElement } from './live/element.ts';

// Defined here rather than through `shared/element.ts`, whose payload
// machinery (zod, themes) this element does not use: it loads on every page.
if (customElements.get('ct-live') === undefined) customElements.define('ct-live', LiveElement);
