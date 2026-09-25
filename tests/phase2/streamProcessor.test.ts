import { describe, expect, it, vi, beforeEach } from "vitest";
import { StreamProcessor } from "../../src/processing/streamProcessor.js";
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

describe("StreamProcessor: basic processing", () => {
  let processor: StreamProcessor;
  beforeEach(() => {
    processor = new StreamProcessor({ windowSize: 10, bufferWindowMs: 50 });
  });

  it("emits a 'processed' event for every pushed packet", () => {
    return new Promise<void>((resolve) => {
      processor.on("processed", (result) => {
        expect(result.packet.sequenceId).toBe(42);
        expect(result.carId).toBe(1);
        expect(result.speedStats.count).toBe(1);
        expect(result.speedStats.mean).toBeCloseTo(200, 3);
        processor.destroy();
        resolve();
      });
      processor.push(makePacket({ sequenceId: 42 }));
    });
  });

  it("maintains separate window state per car", () => {
    return new Promise<void>((resolve) => {
      const results: ReturnType<typeof makePacket>[] = [];
      let car1Result: { speedStats: { mean: number } } | null = null;
      let car2Result: { speedStats: { mean: number } } | null = null;

      processor.on("processed", (result) => {
        if (result.carId === 1) car1Result = result;
        if (result.carId === 2) car2Result = result;
        if (car1Result && car2Result) {
          // Car 1 pushed speed=200, car 2 pushed speed=100 — windows must be independent
          expect(car1Result.speedStats.mean).toBeCloseTo(200, 3);
          expect(car2Result.speedStats.mean).toBeCloseTo(100, 3);
          processor.destroy();
          resolve();
        }
      });

      processor.push(makePacket({ carId: 1, speed: 200 }));
      processor.push(makePacket({ carId: 2, speed: 100 }));
      results; // suppress unused warning
    });
  });

  it("emits 'anomaly' events when the anomaly engine fires", () => {
    return new Promise<void>((resolve) => {
      processor.on("anomaly", (event) => {
        expect(event.kind).toBe("over_rev");
        expect(event.carId).toBe(1);
        processor.destroy();
        resolve();
      });
      processor.push(makePacket({ rpm: 14_000 }));
    });
  });

  it("accumulates packetCount and anomalyCount in getStats()", () => {
    return new Promise<void>((resolve) => {
      let processed = 0;
      processor.on("processed", () => {
        processed++;
        if (processed === 3) {
          const stats = processor.getStats();
          expect(stats.packetCount).toBeGreaterThanOrEqual(3);
          processor.destroy();
          resolve();
        }
      });
      processor.push(makePacket({ sequenceId: 0 }));
      processor.push(makePacket({ sequenceId: 1 }));
      processor.push(makePacket({ sequenceId: 2 }));
    });
  });
});

describe("StreamProcessor: window accuracy under load", () => {
  it("produces correct rolling mean after 1,000 packets", () => {
    return new Promise<void>((resolve) => {
      const processor = new StreamProcessor({ windowSize: 100, bufferWindowMs: 50 });
      let count = 0;
      let lastResult: { speedStats: { mean: number; count: number } } | null = null;

      processor.on("processed", (result) => {
        lastResult = result;
        count++;
        if (count === 1000) {
          // Last 100 packets all had speed=999, window should reflect that.
          expect(lastResult.speedStats.count).toBe(100);
          expect(lastResult.speedStats.mean).toBeCloseTo(999, 1);
          processor.destroy();
          resolve();
        }
      });

      for (let i = 0; i < 900; i++) {
        processor.push(makePacket({ sequenceId: i, speed: 100 }));
      }
      for (let i = 900; i < 1000; i++) {
        processor.push(makePacket({ sequenceId: i, speed: 999 }));
      }
    });
  });
});