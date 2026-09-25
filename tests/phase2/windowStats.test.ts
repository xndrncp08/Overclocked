import { describe, expect, it } from "vitest";
import { SlidingWindow } from "../../src/processing/windowStats.js";

describe("SlidingWindow: construction", () => {
  it("rejects invalid window sizes", () => {
    expect(() => new SlidingWindow(0)).toThrow(RangeError);
    expect(() => new SlidingWindow(-1)).toThrow(RangeError);
    expect(() => new SlidingWindow(1.5)).toThrow(RangeError);
  });

  it("starts empty with zero stats", () => {
    const w = new SlidingWindow(10);
    expect(w.sampleCount).toBe(0);
    expect(w.isFull).toBe(false);
    const stats = w.getStats();
    expect(stats.count).toBe(0);
    expect(stats.mean).toBe(0);
  });
});

describe("SlidingWindow: statistical accuracy", () => {
  it("computes correct mean, min, max for a known dataset", () => {
    const w = new SlidingWindow(5);
    [10, 20, 30, 40, 50].forEach((v) => w.push(v));
    const s = w.getStats();
    expect(s.mean).toBeCloseTo(30, 5);
    expect(s.min).toBe(10);
    expect(s.max).toBe(50);
    expect(s.count).toBe(5);
  });

  it("computes correct variance and stdDev", () => {
    const w = new SlidingWindow(4);
    [2, 4, 4, 4].forEach((v) => w.push(v));
    const s = w.getStats();
    expect(s.mean).toBeCloseTo(3.5, 5);
    expect(s.variance).toBeCloseTo(0.75, 5);
    expect(s.stdDev).toBeCloseTo(Math.sqrt(0.75), 5);
  });

  it("EMA seeds on the first value and converges toward recent values", () => {
    const w = new SlidingWindow(4);
    w.push(100);
    expect(w.getStats().ema).toBeCloseTo(100, 5);
    w.push(0);
    // alpha = 2/(4+1) = 0.4 → ema = 0.4*0 + 0.6*100 = 60
    expect(w.getStats().ema).toBeCloseTo(60, 5);
    w.push(0);
    // ema = 0.4*0 + 0.6*60 = 36
    expect(w.getStats().ema).toBeCloseTo(36, 5);
  });
});

describe("SlidingWindow: eviction and wrap-around", () => {
  it("evicts oldest samples when full and updates stats correctly", () => {
    const w = new SlidingWindow(3);
    w.push(10);
    w.push(20);
    w.push(30);
    expect(w.isFull).toBe(true);
    // Evict 10, add 40 → window is [20, 30, 40]
    w.push(40);
    const s = w.getStats();
    expect(s.mean).toBeCloseTo(30, 5);
    expect(s.min).toBe(20);
    expect(s.max).toBe(40);
    expect(s.count).toBe(3);
  });

  it("correctly updates min/max after evicting the current min or max", () => {
    const w = new SlidingWindow(3);
    w.push(1);   // will be min
    w.push(50);  // will be max
    w.push(25);
    expect(w.getStats().min).toBe(1);
    expect(w.getStats().max).toBe(50);

    w.push(30); // evicts 1 (the min)
    expect(w.getStats().min).toBe(25);

    w.push(10); // evicts 50 (the max)
    expect(w.getStats().max).toBe(30);
  });

  it("maintains accuracy over 10,000 push/evict cycles", () => {
    const w = new SlidingWindow(100);
    for (let i = 1; i <= 10_000; i++) {
      w.push(i);
    }
    // Window holds samples 9901..10000 → mean = 9950.5
    const s = w.getStats();
    expect(s.count).toBe(100);
    expect(s.mean).toBeCloseTo(9950.5, 3);
    expect(s.min).toBe(9901);
    expect(s.max).toBe(10000);
  });

  it("reset() clears all state", () => {
    const w = new SlidingWindow(5);
    [1, 2, 3, 4, 5].forEach((v) => w.push(v));
    w.reset();
    expect(w.sampleCount).toBe(0);
    expect(w.isFull).toBe(false);
    expect(w.getStats().count).toBe(0);
  });
});