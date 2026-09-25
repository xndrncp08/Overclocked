/**
 * src/storage/batchWriter.ts
 */

import duckdb from "duckdb";
import { EventEmitter } from "node:events";
import type { TelemetryPacket } from "../types/telemetry.js";
import type { AnomalyEvent } from "../processing/anomalyEngine.js";
import {
  ANOMALIES_TABLE,
  CREATE_ANOMALY_INDEXES,
  CREATE_ANOMALIES_TABLE,
  CREATE_TELEMETRY_INDEXES,
  CREATE_TELEMETRY_TABLE,
  TELEMETRY_TABLE,
} from "./schema.js";

export interface BatchWriterOptions {
  readonly dbPath?: string;
  readonly flushRecordThreshold?: number;
  readonly flushIntervalMs?: number;
}

export interface FlushResult {
  readonly telemetryFlushed: number;
  readonly anomaliesFlushed: number;
  readonly durationMs: number;
}

export interface BatchWriterEvents {
  ready: [];
  flushed: [result: FlushResult];
  error: [error: Error];
}

const DEFAULTS: Required<BatchWriterOptions> = {
  dbPath: ":memory:",
  flushRecordThreshold: 5_000,
  flushIntervalMs: 500,
};

export class BatchWriter extends EventEmitter {
  private readonly options: Required<BatchWriterOptions>;
  private db: duckdb.Database | null = null;
  private conn: duckdb.Connection | null = null;

  private telemetryBuffer: TelemetryPacket[] = [];
  private anomalyBuffer: AnomalyEvent[] = [];

  private flushTimer: NodeJS.Timeout | null = null;
  private flushing = false;
  private totalTelemetryWritten = 0;
  private totalAnomaliesWritten = 0;

  public constructor(options: BatchWriterOptions = {}) {
    super();
    this.options = { ...DEFAULTS, ...options };
  }

  public override on<K extends keyof BatchWriterEvents>(
    event: K,
    listener: (...args: BatchWriterEvents[K]) => void,
  ): this {
    return super.on(event, listener as (...args: unknown[]) => void);
  }

  public override emit<K extends keyof BatchWriterEvents>(
    event: K,
    ...args: BatchWriterEvents[K]
  ): boolean {
    return super.emit(event, ...args);
  }

  public async open(): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      this.db = new duckdb.Database(
        this.options.dbPath,
        (err: Error | null) => {
          if (err) reject(err);
          else resolve();
        },
      );
    });

    this.conn = (this.db as duckdb.Database).connect();

    await this.exec(CREATE_TELEMETRY_TABLE);
    await this.exec(CREATE_ANOMALIES_TABLE);
    for (const idx of CREATE_TELEMETRY_INDEXES) await this.exec(idx);
    for (const idx of CREATE_ANOMALY_INDEXES) await this.exec(idx);

    this.flushTimer = setInterval(() => {
      void this.flush();
    }, this.options.flushIntervalMs);

    this.emit("ready");
  }

  public pushTelemetry(packet: TelemetryPacket): void {
    this.telemetryBuffer.push(packet);
    if (this.telemetryBuffer.length >= this.options.flushRecordThreshold) {
      void this.flush();
    }
  }

  public pushAnomaly(event: AnomalyEvent): void {
    this.anomalyBuffer.push(event);
  }

  public async flush(): Promise<FlushResult | null> {
    if (this.flushing || !this.conn) return null;
    if (this.telemetryBuffer.length === 0 && this.anomalyBuffer.length === 0) {
      return { telemetryFlushed: 0, anomaliesFlushed: 0, durationMs: 0 };
    }

    this.flushing = true;
    const telemetryBatch = this.telemetryBuffer.splice(0);
    const anomalyBatch = this.anomalyBuffer.splice(0);
    const start = performance.now();

    try {
      if (telemetryBatch.length > 0) {
        await this.insertTelemetry(telemetryBatch);
        this.totalTelemetryWritten += telemetryBatch.length;
      }
      if (anomalyBatch.length > 0) {
        await this.insertAnomalies(anomalyBatch);
        this.totalAnomaliesWritten += anomalyBatch.length;
      }

      const result: FlushResult = {
        telemetryFlushed: telemetryBatch.length,
        anomaliesFlushed: anomalyBatch.length,
        durationMs: performance.now() - start,
      };
      this.emit("flushed", result);
      return result;
    } catch (err) {
      this.telemetryBuffer.unshift(...telemetryBatch);
      this.anomalyBuffer.unshift(...anomalyBatch);
      this.emit("error", err instanceof Error ? err : new Error(String(err)));
      return null;
    } finally {
      this.flushing = false;
    }
  }

  public get stats(): {
    totalTelemetryWritten: number;
    totalAnomaliesWritten: number;
    bufferedTelemetry: number;
    bufferedAnomalies: number;
  } {
    return {
      totalTelemetryWritten: this.totalTelemetryWritten,
      totalAnomaliesWritten: this.totalAnomaliesWritten,
      bufferedTelemetry: this.telemetryBuffer.length,
      bufferedAnomalies: this.anomalyBuffer.length,
    };
  }

  public async close(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flush();
    await new Promise<void>((resolve) => {
      this.db?.close(() => resolve());
    });
    this.db = null;
    this.conn = null;
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private exec(sql: string): Promise<void> {
    return new Promise((resolve, reject) => {
      (this.conn as duckdb.Connection).exec(sql, (err: Error | null) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private async insertTelemetry(records: TelemetryPacket[]): Promise<void> {
    if (records.length === 0) return;
    const CHUNK = 500;
    for (let i = 0; i < records.length; i += CHUNK) {
      const chunk = records.slice(i, i + CHUNK);
      const placeholders = chunk
        .map(() => "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .join(", ");
      const sql = `INSERT INTO ${TELEMETRY_TABLE} VALUES ${placeholders}`;
      const params: unknown[] = [];
      for (const r of chunk) {
        params.push(
          r.sequenceId,
          r.timestamp,
          r.carId,
          r.gear,
          r.speed,
          r.rpm,
          r.throttle,
          r.brake,
          r.tireTempFL,
          r.tireTempFR,
          r.tireTempRL,
          r.tireTempRR,
        );
      }
      await new Promise<void>((resolve, reject) => {
        (this.conn as duckdb.Connection).run(
          sql,
          ...params,
          (err: Error | null) => {
            if (err) reject(err);
            else resolve();
          },
        );
      });
    }
  }

  private async insertAnomalies(records: AnomalyEvent[]): Promise<void> {
    if (records.length === 0) return;
    const CHUNK = 500;
    for (let i = 0; i < records.length; i += CHUNK) {
      const chunk = records.slice(i, i + CHUNK);
      const placeholders = chunk
        .map(() => "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .join(", ");
      const sql = `INSERT INTO ${ANOMALIES_TABLE} VALUES ${placeholders}`;
      const params: unknown[] = [];
      for (const r of chunk) {
        params.push(
          r.sequenceId,
          r.timestamp,
          r.carId,
          r.kind,
          r.severity,
          r.field,
          r.value,
          r.threshold,
          r.message,
        );
      }
      await new Promise<void>((resolve, reject) => {
        (this.conn as duckdb.Connection).run(
          sql,
          ...params,
          (err: Error | null) => {
            if (err) reject(err);
            else resolve();
          },
        );
      });
    }
  }
}
