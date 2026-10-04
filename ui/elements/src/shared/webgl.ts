/**
 * Releases a canvas's WebGL context now instead of at garbage collection, so
 * re-rendering elements never exhausts the browser's context limit.
 * `getContext` returns the existing context for the type it was created
 * with and `null` otherwise, so this never creates one.
 */
export function releaseWebGL(canvas: HTMLCanvasElement): void {
  const gl = canvas.getContext('webgl2') ?? canvas.getContext('webgl');
  gl?.getExtension('WEBGL_lose_context')?.loseContext();
}
