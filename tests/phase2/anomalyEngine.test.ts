import { describe, expect, it, beforeEach } from "vitest";
import { AnomalyEngine } from "../../src/processing/anomalyEngine.js";
import { SlidingWindow } from "../../src/processing/windowStats.js";
import type { TelemetryPacket } from "../../src/types/telemetry.js";

function makePacket(overrides: Partial<TelemetryPacket> = {}): TelemetryPacket {
  return {
    timestamp: Date.now(),
    sequenceId: 0,
    carId: 1,
    gear: 5,
    speed: 200,
    rpm: 10_000,
    throttle: 0.8,
    brake: 0,
    tireTempFL: 90,
    tireTempFR: 90,
    tireTempRL: 88,
    tireTempRR: 88,
    ...overrides,
  };
}

function emptyStats() {
  return new SlidingWindow(10).getStats();
}

function statsWithStdDev(stdDev: number) {
  // Push values that produce the desired stdDev. For a window of values
  // [mean-d, mean+d, mean-d, mean+d...] population stdDev = d.
  const w = new SlidingWindow(20);
  const mean = 90;
  for (let i = 0; i < 20; i++) {
    w.push(i % 2 === 0 ? mean - stdDev : mean + stdDev);
  }
  return w.getStats();
}

describe("AnomalyEngine: threshold breaches", () => {
  let engine: AnomalyEngine;
  beforeEach(() => { engine = new AnomalyEngine(); });

  it("fires threshold_breach/critical for speed above maxSpeed", () => {
    const events = engine.evaluate(makePacket({ speed: 380 }), emptyStats(), emptyStats(), emptyStats());
    const breach = events.find((e) => e.kind === "threshold_breach" && e.field === "speed");
    expect(breach).toBeDefined();
    expect(breach?.severity).toBe("critical");
    expect(breach?.value).toBe(380);
  });

  it("does not fire speed breach when speed is below the limit", () => {
    const events = engine.evaluate(makePacket({ speed: 200 }), emptyStats(), emptyStats(), emptyStats());
    expect(events.find((e) => e.field === "speed")).toBeUndefined();
  });

  it("fires over_rev/critical when RPM exceeds maxRpm", () => {
    const events = engine.evaluate(makePacket({ rpm: 14_000 }), emptyStats(), emptyStats(), emptyStats());
    const ev = events.find((e) => e.kind === "over_rev");
    expect(ev).toBeDefined();
    expect(ev?.severity).toBe("critical");
  });

  it("fires threshold_breach/critical for overheating tire", () => {
    const events = engine.evaluate(makePacket({ tireTempFL: 140 }), emptyStats(), emptyStats(), emptyStats());
    const ev = events.find((e) => e.field === "tireTempFL" && e.kind === "threshold_breach");
    expect(ev?.severity).toBe("critical");
  });

  it("fires threshold_breach/warning for cold tire", () => {
    const events = engine.evaluate(makePacket({ tireTempFL: 50 }), emptyStats(), emptyStats(), emptyStats());
    const ev = events.find((e) => e.field === "tireTempFL" && e.kind === "threshold_breach");
    expect(ev?.severity).toBe("warning");
  });
});

describe("AnomalyEngine: rate-of-change", () => {
  it("fires rapid_drop/warning when speed falls sharply between packets", () => {
    const engine = new AnomalyEngine();
    engine.evaluate(makePacket({ speed: 250, sequenceId: 0 }), emptyStats(), emptyStats(), emptyStats());
    const events = engine.evaluate(makePacket({ speed: 100, sequenceId: 1 }), emptyStats(), emptyStats(), emptyStats());
    expect(events.find((e) => e.kind === "rapid_drop" && e.field === "speed")).toBeDefined();
  });

  it("does not fire rapid_drop when speed decreases within normal bounds", () => {
    const engine = new AnomalyEngine();
    engine.evaluate(makePacket({ speed: 200, sequenceId: 0 }), emptyStats(), emptyStats(), emptyStats());
    const events = engine.evaluate(makePacket({ speed: 195, sequenceId: 1 }), emptyStats(), emptyStats(), emptyStats());
    expect(events.find((e) => e.kind === "rapid_drop" && e.field === "speed")).toBeUndefined();
  });

  it("fires rapid_drop for RPM", () => {
    const engine = new AnomalyEngine();
    engine.evaluate(makePacket({ rpm: 12_000, sequenceId: 0 }), emptyStats(), emptyStats(), emptyStats());
    const events = engine.evaluate(makePacket({ rpm: 7_000, sequenceId: 1 }), emptyStats(), emptyStats(), emptyStats());
    expect(events.find((e) => e.kind === "rapid_drop" && e.field === "rpm")).toBeDefined();
  });
});

describe("AnomalyEngine: window-based detection", () => {
  it("fires sensor_noise when tire temp std deviation exceeds threshold", () => {
    const engine = new AnomalyEngine();
    const noisyStats = statsWithStdDev(10); // > default threshold of 8
    const events = engine.evaluate(makePacket(), emptyStats(), emptyStats(), noisyStats);
    expect(events.find((e) => e.kind === "sensor_noise")).toBeDefined();
  });

  it("does not fire sensor_noise when std deviation is within tolerance", () => {
    const engine = new AnomalyEngine();
    const quietStats = statsWithStdDev(2); // < default threshold of 8
    const events = engine.evaluate(makePacket(), emptyStats(), emptyStats(), quietStats);
    expect(events.find((e) => e.kind === "sensor_noise")).toBeUndefined();
  });

  it("fires stall_detected when RPM is near zero but car is moving", () => {
    const engine = new AnomalyEngine();
    const events = engine.evaluate(makePacket({ rpm: 200, speed: 80 }), emptyStats(), emptyStats(), emptyStats());
    expect(events.find((e) => e.kind === "stall_detected")).toBeDefined();
  });

  it("does not fire stall_detected when car is stationary", () => {
    const engine = new AnomalyEngine();
    const events = engine.evaluate(makePacket({ rpm: 200, speed: 5 }), emptyStats(), emptyStats(), emptyStats());
    expect(events.find((e) => e.kind === "stall_detected")).toBeUndefined();
  });
});

describe("AnomalyEngine: custom thresholds", () => {
  it("respects overridden thresholds", () => {
    const engine = new AnomalyEngine({ maxSpeed: 200, maxRpm: 8000 });
    const events = engine.evaluate(makePacket({ speed: 210, rpm: 8500 }), emptyStats(), emptyStats(), emptyStats());
    expect(events.find((e) => e.field === "speed" && e.kind === "threshold_breach")).toBeDefined();
    expect(events.find((e) => e.kind === "over_rev")).toBeDefined();
  });
});

describe("AnomalyEngine: anomalyCount", () => {
  it("accumulates anomaly count across packets", () => {
    const engine = new AnomalyEngine();
    engine.evaluate(makePacket({ speed: 380, rpm: 14_000 }), emptyStats(), emptyStats(), emptyStats());
    expect(engine.anomalyCount).toBe(2);
    engine.evaluate(makePacket({ speed: 380 }), emptyStats(), emptyStats(), emptyStats());
    expect(engine.anomalyCount).toBe(3);
    engine.reset();
    expect(engine.anomalyCount).toBe(0);
  });
});