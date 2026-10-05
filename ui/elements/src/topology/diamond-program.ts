/**
 * A sigma node program drawing diamonds, for channel nodes. It is sigma's
 * circle program (one triangle around the node) with an L1 distance in the
 * fragment shader instead of the Euclidean one.
 */

import type { Attributes } from 'graphology-types';
import { NodeProgram, type ProgramInfo } from 'sigma/rendering';
import type { NodeDisplayData, RenderParams } from 'sigma/types';
import { floatColor } from 'sigma/utils';

const VERTEX_SHADER = /* glsl */ `
attribute vec4 a_id;
attribute vec4 a_color;
attribute vec2 a_position;
attribute float a_size;
attribute float a_angle;

uniform mat3 u_matrix;
uniform float u_sizeRatio;
uniform float u_correctionRatio;

varying vec4 v_color;
varying vec2 v_diffVector;
varying float v_radius;

const float bias = 255.0 / 254.0;

void main() {
  float size = a_size * u_correctionRatio / u_sizeRatio * 4.0;
  vec2 diffVector = size * vec2(cos(a_angle), sin(a_angle));
  vec2 position = a_position + diffVector;
  gl_Position = vec4((u_matrix * vec3(position, 1)).xy, 0, 1);
  v_diffVector = diffVector;
  // The triangle's inscribed circle has radius size / 2: the largest diamond
  // that fits has that half-diagonal. Callers enlarge diamonds to balance
  // their smaller area against discs.
  v_radius = size / 2.0;
  #ifdef PICKING_MODE
  v_color = a_id;
  #else
  v_color = a_color;
  #endif
  v_color.a *= bias;
}
`;

const FRAGMENT_SHADER = /* glsl */ `
precision highp float;

varying vec4 v_color;
varying vec2 v_diffVector;
varying float v_radius;

uniform float u_correctionRatio;

const vec4 transparent = vec4(0.0, 0.0, 0.0, 0.0);

void main(void) {
  float border = u_correctionRatio * 2.0;
  float dist = abs(v_diffVector.x) + abs(v_diffVector.y) - v_radius + border;
  #ifdef PICKING_MODE
  gl_FragColor = dist > border ? transparent : v_color;
  #else
  float t = 0.0;
  if (dist > border) t = 1.0;
  else if (dist > 0.0) t = dist / border;
  gl_FragColor = mix(v_color, transparent, t);
  #endif
}
`;

const { UNSIGNED_BYTE, FLOAT, TRIANGLES } = WebGLRenderingContext;
const UNIFORMS = ['u_sizeRatio', 'u_correctionRatio', 'u_matrix'] as const;
type Uniform = (typeof UNIFORMS)[number];

export class NodeDiamondProgram<
  N extends Attributes = Attributes,
  E extends Attributes = Attributes,
  G extends Attributes = Attributes,
> extends NodeProgram<Uniform, N, E, G> {
  getDefinition() {
    return {
      VERTICES: 3,
      VERTEX_SHADER_SOURCE: VERTEX_SHADER,
      FRAGMENT_SHADER_SOURCE: FRAGMENT_SHADER,
      METHOD: TRIANGLES,
      UNIFORMS,
      ATTRIBUTES: [
        { name: 'a_position', size: 2, type: FLOAT },
        { name: 'a_size', size: 1, type: FLOAT },
        { name: 'a_color', size: 4, type: UNSIGNED_BYTE, normalized: true },
        { name: 'a_id', size: 4, type: UNSIGNED_BYTE, normalized: true },
      ],
      CONSTANT_ATTRIBUTES: [{ name: 'a_angle', size: 1, type: FLOAT }],
      CONSTANT_DATA: [[0], [(2 * Math.PI) / 3], [(4 * Math.PI) / 3]],
    };
  }

  processVisibleItem(nodeIndex: number, startIndex: number, data: NodeDisplayData): void {
    const array = this.array;
    let i = startIndex;
    array[i++] = data.x;
    array[i++] = data.y;
    array[i++] = data.size;
    array[i++] = floatColor(data.color);
    array[i++] = nodeIndex;
  }

  setUniforms(params: RenderParams, { gl, uniformLocations }: ProgramInfo<Uniform>): void {
    gl.uniform1f(uniformLocations.u_correctionRatio, params.correctionRatio);
    gl.uniform1f(uniformLocations.u_sizeRatio, params.sizeRatio);
    gl.uniformMatrix3fv(uniformLocations.u_matrix, false, params.matrix);
  }
}
