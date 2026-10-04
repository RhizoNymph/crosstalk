// Bundles each element into dist/<name>.js (ESM, minified, external source
// map). With --serve, rebuilds on change and serves this package directory
// so demo/index.html can load dist/ and test/fixtures/.
import * as esbuild from 'esbuild';

const ELEMENTS = ['ct-topology', 'ct-projection', 'ct-timebrush'];
const serve = process.argv.includes('--serve');

/** @type {import('esbuild').BuildOptions} */
const options = {
  entryPoints: ELEMENTS.map((name) => ({ in: `src/${name}.ts`, out: name })),
  outdir: 'dist',
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: 'es2022',
  minify: true,
  // External: the asset bundle renames files with a content hash, so a
  // sourceMappingURL comment would point at a missing file. The demo links it.
  sourcemap: serve ? 'linked' : 'external',
  legalComments: 'eof',
  logLevel: 'info',
};

if (serve) {
  const context = await esbuild.context(options);
  await context.watch();
  const { port } = await context.serve({ servedir: '.', host: '127.0.0.1', port: 8737 });
  console.log(`demo: http://127.0.0.1:${port}/demo/`);
} else {
  await esbuild.build(options);
}
