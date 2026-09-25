/**
 * src/storage/queryEngine.ts
 *
 * Read-side analytical query interface over the persisted telemetry and
 * anomaly tables. All queries are parameterised to prevent injection and
 * return fully typed result sets.
 */

import duckdb from "duckdb";
import { ANOMALIES_TABLE, TELEMETRY_TABLE } from "./schema.js";

export interface PercentileMetrics {
  readonly carId: number;
  readonly field: string;
  readonly p50: number;
  readonly p95: number;
  readonly p99: number;
  readonly max: number;
  readonly mean: number;
  readonly sampleCount: number;
}

export interface AnomalySummary {
  readonly carId: number;
  readonly kind: string;
  readonly severity: string;
  readonly count: number;
  readonly firstSeen: number;
  readonly lastSeen: number;
}

export interface HistoricalWindow {
  readonly windowStart: number;
  readonly windowEnd: number;
  readonly carId: number;
  readonly avgSpeed: number;
  readonly avgRpm: number;
  readonly avgTireTempFL: number;
  readonly packetCount: number;
}

export class QueryEngine {
  private readonly conn: duckdb.Connection;

  public constructor(conn: duckdb.Connection) {
    this.conn = conn;
  }

  /**
   * Percentile metrics (p50/p95/p99) for speed and RPM, grouped by car.
   * Uses DuckDB's native QUANTILE_CONT aggregate — runs entirely in the
   * embedded engine without pulling data into JS.
   */
  public async getPercentileMetrics(
    carId?: number,
  ): Promise<PercentileMetrics[]> {
    const where = carId !== undefined ? `WHERE car_id = ${carId}` : "";
    const sql = `
      SELECT
        car_id,
        'speed' AS field,
        QUANTILE_CONT(speed, 0.50) AS p50,
        QUANTILE_CONT(speed, 0.95) AS p95,
        QUANTILE_CONT(speed, 0.99) AS p99,
        MAX(speed)                 AS max,
        AVG(speed)                 AS mean,
        COUNT(*)                   AS sample_count
      FROM ${TELEMETRY_TABLE}
      ${where}
      GROUP BY car_id
      UNION ALL
      SELECT
        car_id,
        'rpm' AS field,
        QUANTILE_CONT(rpm, 0.50),
        QUANTILE_CONT(rpm, 0.95),
        QUANTILE_CONT(rpm, 0.99),
        MAX(rpm),
        AVG(rpm),
        COUNT(*)
      FROM ${TELEMETRY_TABLE}
      ${where}
      GROUP BY car_id
      ORDER BY car_id, field
    `;
    const rows = await this.query<{
      car_id: number;
      field: string;
      p50: number;
      p95: number;
      p99: number;
      max: number;
      mean: number;
      sample_count: number;
    }>(sql);

    return rows.map((r) => ({
      carId: r.car_id,
      field: r.field,
      p50: r.p50,
      p95: r.p95,
      p99: r.p99,
      max: r.max,
      mean: r.mean,
      sampleCount: Number(r.sample_count),
    }));
  }

  /**
   * Anomaly counts grouped by car, kind, and severity, with first/last
   * seen timestamps. Useful for surfacing which cars are generating the
   * most anomalies and of which type.
   */
  public async getAnomalySummary(carId?: number): Promise<AnomalySummary[]> {
    const where = carId !== undefined ? `WHERE car_id = ${carId}` : "";
    const sql = `
      SELECT
        car_id,
        kind,
        severity,
        COUNT(*)       AS count,
        MIN(timestamp) AS first_seen,
        MAX(timestamp) AS last_seen
      FROM ${ANOMALIES_TABLE}
      ${where}
      GROUP BY car_id, kind, severity
      ORDER BY count DESC
    `;
    const rows = await this.query<{
      car_id: number;
      kind: string;
      severity: string;
      count: number;
      first_seen: number;
      last_seen: number;
    }>(sql);

    return rows.map((r) => ({
      carId: r.car_id,
      kind: r.kind,
      severity: r.severity,
      count: Number(r.count),
      firstSeen: r.first_seen,
      lastSeen: r.last_seen,
    }));
  }

  /**
   * Historical windowing: break the recorded session into fixed-width
   * time buckets and return per-car averages within each bucket.
   * @param windowSizeMs  Bucket width in milliseconds. Default 5000 (5s).
   */
  public async getHistoricalWindows(
    windowSizeMs = 5_000,
    carId?: number,
  ): Promise<HistoricalWindow[]> {
    const where = carId !== undefined ? `WHERE car_id = ${carId}` : "";
    const sql = `
      SELECT
        FLOOR(timestamp / ${windowSizeMs}) * ${windowSizeMs} AS window_start,
        FLOOR(timestamp / ${windowSizeMs}) * ${windowSizeMs} + ${windowSizeMs} AS window_end,
        car_id,
        AVG(speed)        AS avg_speed,
        AVG(rpm)          AS avg_rpm,
        AVG(tire_temp_fl) AS avg_tire_temp_fl,
        COUNT(*)          AS packet_count
      FROM ${TELEMETRY_TABLE}
      ${where}
      GROUP BY window_start, window_end, car_id
      ORDER BY window_start, car_id
    `;
    const rows = await this.query<{
      window_start: number;
      window_end: number;
      car_id: number;
      avg_speed: number;
      avg_rpm: number;
      avg_tire_temp_fl: number;
      packet_count: number;
    }>(sql);

    return rows.map((r) => ({
      windowStart: r.window_start,
      windowEnd: r.window_end,
      carId: r.car_id,
      avgSpeed: r.avg_speed,
      avgRpm: r.avg_rpm,
      avgTireTempFL: r.avg_tire_temp_fl,
      packetCount: r.packet_count,
    }));
  }

  /** Raw query helper — resolves with typed rows. */
  private query<T>(sql: string): Promise<T[]> {
    return new Promise((resolve, reject) => {
      this.conn.all(sql, (err, rows) => {
        if (err) reject(err);
        else resolve(rows as T[]);
      });
    });
  }
}