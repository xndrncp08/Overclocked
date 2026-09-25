import { afterAll, beforeAll, describe, expect, it } from "vitest";
import duckdb from "duckdb";
import { BatchWriter } from "../../src/storage/batchWriter.js";
import { QueryEngine } from "../../src/storage/queryEngine.js";
import type { TelemetryPacket } from "../../src/types/telemetry.js";
import type { AnomalyEvent } from "../../src/processing/anomalyEngine.js";

let writer: BatchWriter;
let queryEngine: QueryEngine;
let db: duckdb.Database;
let conn: duckdb.Connection;

function makePacket(overrides: Partial<TelemetryPacket> = {}): TelemetryPacket {
  return {
    timestamp: 1_700_000_000_000,
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
    timestamp: 1_700_000_000_000,
    sequenceId: 0,
    carId: 1,
    kind: "over_rev",
    severity: "critical",
    field: "rpm",
    value: 14_000,
    threshold: 13_000,
    message: "over-rev",
    ...overrides,
  };
}

beforeAll(async () => {
  writer = new BatchWriter({ dbPath: ":memory:", flushIntervalMs: 60_000 });
  await writer.open();

  // Insert known data for deterministic query assertions.
  // Car 1: 100 packets, speed 100..199, rpm 8000..8099
  for (let i = 0; i < 100; i++) {
    writer.pushTelemetry(
      makePacket({
        sequenceId: i,
        carId: 1,
        speed: 100 + i,
        rpm: 8_000 + i,
        timestamp: 1_700_000_000_000 + i * 1_000,
      }),
    );
  }
  // Car 2: 50 packets, speed all 250, rpm all 12000
  for (let i = 0; i < 50; i++) {
    writer.pushTelemetry(
      makePacket({ sequenceId: 100 + i, carId: 2, speed: 250, rpm: 12_000 }),
    );
  }

  writer.pushAnomaly(makeAnomaly({ carId: 1, kind: "over_rev", severity: "critical" }));
  writer.pushAnomaly(makeAnomaly({ carId: 1, kind: "over_rev", severity: "critical" }));
  writer.pushAnomaly(makeAnomaly({ carId: 2, kind: "stall_detected", severity: "critical" }));

  await writer.flush();

  // Open a second connection to the same in-memory DB for the query engine.
  // DuckDB in-memory DBs are single-process only, so we reach into the
  // writer's private db field through casting for test purposes.
  // In production the query engine would be passed the same connection.
  const writerWithDb = writer as unknown as { db: duckdb.Database };
  db = writerWithDb.db;
  conn = db.connect();
  queryEngine = new QueryEngine(conn);
});

afterAll(async () => {
  await writer.close();
});

describe("QueryEngine: percentile metrics", () => {
  it("returns p50/p95/p99 for speed and rpm per car", async () => {
    const metrics = await queryEngine.getPercentileMetrics();
    expect(metrics.length).toBeGreaterThan(0);

    const car1Speed = metrics.find((m) => m.carId === 1 && m.field === "speed");
    expect(car1Speed).toBeDefined();
    // speed 100..199 → median ≈ 149.5
    expect(car1Speed?.p50).toBeCloseTo(149.5, 0);
    expect(car1Speed?.p99).toBeGreaterThan(196);
    expect(car1Speed?.sampleCount).toBe(100);

    const car2Speed = metrics.find((m) => m.carId === 2 && m.field === "speed");
    // All car 2 speeds are 250, so all percentiles should equal 250.
    expect(car2Speed?.p50).toBeCloseTo(250, 1);
    expect(car2Speed?.p99).toBeCloseTo(250, 1);
  });

  it("filters by carId correctly", async () => {
    const metrics = await queryEngine.getPercentileMetrics(2);
    expect(metrics.every((m) => m.carId === 2)).toBe(true);
  });
});

describe("QueryEngine: anomaly summary", () => {
  it("groups anomalies by car, kind, and severity with correct counts", async () => {
    const summary = await queryEngine.getAnomalySummary();
    const car1OverRev = summary.find(
      (s) => s.carId === 1 && s.kind === "over_rev",
    );
    expect(car1OverRev?.count).toBe(2);

    const car2Stall = summary.find(
      (s) => s.carId === 2 && s.kind === "stall_detected",
    );
    expect(car2Stall?.count).toBe(1);
  });

  it("filters by carId", async () => {
    const summary = await queryEngine.getAnomalySummary(1);
    expect(summary.every((s) => s.carId === 1)).toBe(true);
    expect(summary.find((s) => s.carId === 2)).toBeUndefined();
  });
});

describe("QueryEngine: historical windows", () => {
  it("buckets packets into time windows and computes per-window averages", async () => {
    // Car 1 packets span 100s (1 per second). A 10,000ms window should
    // give ~10 buckets of ~10 packets each.
    const windows = await queryEngine.getHistoricalWindows(10_000, 1);
    expect(windows.length).toBeGreaterThan(0);
    for (const w of windows) {
      expect(w.carId).toBe(1);
      expect(w.packetCount).toBeGreaterThan(0);
      expect(w.avgSpeed).toBeGreaterThan(0);
      expect(w.windowEnd - w.windowStart).toBe(10_000);
    }
  });
});