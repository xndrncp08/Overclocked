import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { BatchWriter } from "../../src/storage/batchWriter.js";
import type { TelemetryPacket } from "../../src/types/telemetry.js";
import type { AnomalyEvent } from "../../src/processing/anomalyEngine.js";

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

function makeAnomaly(overrides: Partial<AnomalyEvent> = {}): AnomalyEvent {
  return {
    timestamp: Date.now(),
    sequenceId: 0,
    carId: 1,
    kind: "over_rev",
    severity: "critical",
    field: "rpm",
    value: 14_000,
    threshold: 13_000,
    message: "Car 1 over-rev: 14000 RPM",
    ...overrides,
  };
}

describe("BatchWriter: open and schema setup", () => {
  it("opens an in-memory database and emits ready", async () => {
    const writer = new BatchWriter({ dbPath: ":memory:" });
    await new Promise<void>((resolve) => {
      writer.on("ready", () => resolve());
      void writer.open();
    });
    await writer.close();
  });
});

describe("BatchWriter: flush", () => {
  let writer: BatchWriter;

  beforeEach(async () => {
    writer = new BatchWriter({
      dbPath: ":memory:",
      flushRecordThreshold: 5_000,
      flushIntervalMs: 60_000, // effectively disable auto-flush during tests
    });
    await writer.open();
  });

  afterEach(async () => {
    await writer.close();
  });

  it("returns a zero-count result when buffers are empty", async () => {
    const result = await writer.flush();
    expect(result?.telemetryFlushed).toBe(0);
    expect(result?.anomaliesFlushed).toBe(0);
  });

  it("flushes buffered telemetry and reports correct count", async () => {
    for (let i = 0; i < 100; i++) {
      writer.pushTelemetry(makePacket({ sequenceId: i }));
    }
    const result = await writer.flush();
    expect(result?.telemetryFlushed).toBe(100);
    expect(writer.stats.totalTelemetryWritten).toBe(100);
    expect(writer.stats.bufferedTelemetry).toBe(0);
  });

  it("flushes buffered anomalies and reports correct count", async () => {
    for (let i = 0; i < 20; i++) {
      writer.pushAnomaly(makeAnomaly({ sequenceId: i }));
    }
    const result = await writer.flush();
    expect(result?.anomaliesFlushed).toBe(20);
    expect(writer.stats.totalAnomaliesWritten).toBe(20);
  });

  it("flushes both tables in the same call", async () => {
    writer.pushTelemetry(makePacket({ sequenceId: 1 }));
    writer.pushAnomaly(makeAnomaly({ sequenceId: 1 }));
    const result = await writer.flush();
    expect(result?.telemetryFlushed).toBe(1);
    expect(result?.anomaliesFlushed).toBe(1);
  });

  it("does not double-flush records already written", async () => {
    for (let i = 0; i < 50; i++) writer.pushTelemetry(makePacket({ sequenceId: i }));
    await writer.flush();
    const result2 = await writer.flush();
    expect(result2?.telemetryFlushed).toBe(0);
    expect(writer.stats.totalTelemetryWritten).toBe(50);
  });

  it("emits a flushed event with timing metadata", async () => {
    writer.pushTelemetry(makePacket());
    await new Promise<void>((resolve) => {
      writer.on("flushed", (result) => {
        expect(result.telemetryFlushed).toBe(1);
        expect(result.durationMs).toBeGreaterThanOrEqual(0);
        resolve();
      });
      void writer.flush();
    });
  });
});

describe("BatchWriter: threshold-triggered flush", () => {
  it("automatically flushes when the record threshold is crossed", async () => {
    const writer = new BatchWriter({
      dbPath: ":memory:",
      flushRecordThreshold: 10,
      flushIntervalMs: 60_000,
    });
    await writer.open();

    const flushed = new Promise<void>((resolve) => {
      writer.on("flushed", () => resolve());
    });

    for (let i = 0; i < 10; i++) {
      writer.pushTelemetry(makePacket({ sequenceId: i }));
    }

    await flushed;
    expect(writer.stats.totalTelemetryWritten).toBe(10);
    await writer.close();
  });
});

describe("BatchWriter: throughput", () => {
  it("inserts 5,000 telemetry records in under 2 seconds", async () => {
    const writer = new BatchWriter({
      dbPath: ":memory:",
      flushRecordThreshold: 100_000,
      flushIntervalMs: 60_000,
    });
    await writer.open();

    for (let i = 0; i < 5_000; i++) {
      writer.pushTelemetry(makePacket({ sequenceId: i }));
    }

    const start = performance.now();
    const result = await writer.flush();
    const elapsed = performance.now() - start;

    expect(result?.telemetryFlushed).toBe(5_000);
    expect(elapsed).toBeLessThan(2_000);
    await writer.close();
  }, 10_000);
});