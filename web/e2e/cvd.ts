/**
 * Colour-vision-deficiency simulation, for issue 3.8's criterion "choropleth
 * palette passes a deuteranopia simulation".
 *
 * The simulation is Machado, Oliveira and Fernandes (2009), the matrix set most
 * colour tools use, applied at severity 1.0 in LINEAR RGB. Applying it to
 * gamma-encoded sRGB : the common shortcut : exaggerates separation in the
 * shadows and understates it in the highlights, which for a sequential ramp is
 * precisely the region the answer depends on.
 *
 * Differences are reported as CIE76 ΔE in L*a*b*. CIE76 is the crude one; it is
 * used here because the thresholds are coarse (is this ramp still a ramp?) and
 * because a reader can check the arithmetic. Nothing in this file is a
 * substitute for testing with people who have the condition.
 */

export interface Rgb {
  r: number;
  g: number;
  b: number;
}

/** Parse the `rgb(…)` / `rgba(…)` string getComputedStyle returns. */
export function parseRgb(value: string): Rgb {
  const nums = value.match(/-?\d+(\.\d+)?/g);
  if (!nums || nums.length < 3) throw new Error(`Not an rgb() colour: '${value}'`);
  return { r: Number(nums[0]), g: Number(nums[1]), b: Number(nums[2]) };
}

const toLinear = (c: number) => {
  const s = c / 255;
  return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
};

const toSrgb = (c: number) => {
  const v = c <= 0.0031308 ? c * 12.92 : 1.055 * c ** (1 / 2.4) - 0.055;
  return Math.min(255, Math.max(0, Math.round(v * 255)));
};

/** Machado et al. (2009), deuteranomaly severity 1.0 : i.e. deuteranopia. */
const DEUTERANOPIA = [
  [0.367322, 0.860646, -0.227968],
  [0.280085, 0.672501, 0.047413],
  [-0.011820, 0.04294, 0.968881],
] as const;

export function simulateDeuteranopia(colour: Rgb): Rgb {
  const [lr, lg, lb] = [toLinear(colour.r), toLinear(colour.g), toLinear(colour.b)];
  const [m0, m1, m2] = DEUTERANOPIA;
  return {
    r: toSrgb(m0[0] * lr + m0[1] * lg + m0[2] * lb),
    g: toSrgb(m1[0] * lr + m1[1] * lg + m1[2] * lb),
    b: toSrgb(m2[0] * lr + m2[1] * lg + m2[2] * lb),
  };
}

interface Lab {
  L: number;
  a: number;
  b: number;
}

/** sRGB → CIE L*a*b* through XYZ, D65. */
export function toLab(colour: Rgb): Lab {
  const [r, g, b] = [toLinear(colour.r), toLinear(colour.g), toLinear(colour.b)];
  const x = (r * 0.4124564 + g * 0.3575761 + b * 0.1804375) / 0.95047;
  const y = r * 0.2126729 + g * 0.7151522 + b * 0.072175;
  const z = (r * 0.0193339 + g * 0.119192 + b * 0.9503041) / 1.08883;

  const f = (t: number) => (t > 216 / 24389 ? Math.cbrt(t) : (841 / 108) * t + 4 / 29);
  const [fx, fy, fz] = [f(x), f(y), f(z)];
  return { L: 116 * fy - 16, a: 500 * (fx - fy), b: 200 * (fy - fz) };
}

/** CIE76 colour difference. Roughly: <2 invisible, ~10 clearly different. */
export function deltaE(a: Rgb, b: Rgb): number {
  const la = toLab(a);
  const lb = toLab(b);
  return Math.hypot(la.L - lb.L, la.a - lb.a, la.b - lb.b);
}

/** Perceptual lightness, 0–100. */
export function lightness(colour: Rgb): number {
  return toLab(colour).L;
}
