/**
 * src/processing/windowStats.ts
 *
 * Sliding window statistical calculators operating over a fixed-size
 * circular sample buffer. All operations are O(1) or O(window) — no
 * sorting, no heap allocation per sample beyond the initial buffer.
 *
 * Used by the stream processor to maintain rolling metrics over the
 * most recent N packets for a given sensor field without re-scanning
 * the full history on every update.
 */

export interface WindowStats {
  readonly mean: number;
  readonly min: number;
  readonly max: number;
  readonly variance: number;
  readonly stdDev: number;
  readonly ema: number;       // exponential moving average
  readonly count: number;     // samples currently in the window
}

export class SlidingWindow {
  private readonly buffer: Float64Array;
  private readonly capacity: number;
  private head = 0;
  private count = 0;
  private sum = 0;
  private sumSq = 0;
  private _min = Infinity;
  private _max = -Infinity;
  private _ema = 0;
  private readonly alpha: number; // EMA smoothing factor

  /**
   * @param windowSize  Number of samples to keep. Older samples are
   *                    evicted once the window is full.
   * @param emaAlpha    EMA smoothing factor, 0 < alpha <= 1.
   *                    Defaults to 2 / (windowSize + 1) — the standard
   *                    choice for a windowSize-period EMA.
   */
  public constructor(windowSize: number, emaAlpha?: number) {
    if (!Number.isInteger(windowSize) || windowSize < 1) {
      throw new RangeError("SlidingWindow windowSize must be an integer >= 1");
    }
    this.capacity = windowSize;
    this.buffer = new Float64Array(windowSize);
    this.alpha = emaAlpha ?? 2 / (windowSize + 1);
  }

  public get windowSize(): number {
    return this.capacity;
  }

  public get sampleCount(): number {
    return this.count;
  }

  public get isFull(): boolean {
    return this.count === this.capacity;
  }

  /** Add a new sample, evicting the oldest if the window is full. */
  public push(value: number): void {
    if (this.count === this.capacity) {
      // Evict the oldest sample before writing the new one.
      const evicted = this.buffer[this.head] as number;
      this.sum -= evicted;
      this.sumSq -= evicted * evicted;
      // Min/max can't be cheaply updated on eviction — recompute lazily
      // via getStats() when needed. Mark as dirty by resetting.
      if (evicted === this._min || evicted === this._max) {
        this._min = Infinity;
        this._max = -Infinity;
        for (let i = 0; i < this.capacity; i++) {
          const v = this.buffer[i] as number;
          if (i !== this.head) {
            if (v < this._min) this._min = v;
            if (v > this._max) this._max = v;
          }
        }
      }
    } else {
      this.count++;
    }

    this.buffer[this.head] = value;
    this.head = (this.head + 1) % this.capacity;
    this.sum += value;
    this.sumSq += value * value;
    if (value < this._min) this._min = value;
    if (value > this._max) this._max = value;

    // EMA: seed with first value, then apply standard formula.
    this._ema = this.count === 1 ? value : this.alpha * value + (1 - this.alpha) * this._ema;
  }

  /** Compute and return current window statistics. Returns zeros when empty. */
  public getStats(): WindowStats {
    if (this.count === 0) {
      return { mean: 0, min: 0, max: 0, variance: 0, stdDev: 0, ema: 0, count: 0 };
    }
    const mean = this.sum / this.count;
    // Population variance over the window samples.
    const variance = Math.max(0, this.sumSq / this.count - mean * mean);
    return {
      mean,
      min: this._min,
      max: this._max,
      variance,
      stdDev: Math.sqrt(variance),
      ema: this._ema,
      count: this.count,
    };
  }

  /** Reset all samples and accumulators. */
  public reset(): void {
    this.head = 0;
    this.count = 0;
    this.sum = 0;
    this.sumSq = 0;
    this._min = Infinity;
    this._max = -Infinity;
    this._ema = 0;
    this.buffer.fill(0);
  }
}