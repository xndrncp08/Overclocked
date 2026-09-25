/**
 * src/processing/streamProcessor.ts
 *
 * RxJS-based stream processor. Subscribes to the TelemetryReceiver's
 * 'batch' events, fans each packet out into a per-car processing lane,
 * maintains sliding window stats per car per sensor field, runs anomaly
 * detection on every packet, and emits typed output events downstream.
 *
 * Per-car isolation means one car's burst or anomaly pattern can't
 * pollute another car's statistics.
 */

import { EventEmitter } from "node:events";
import { Subject, bufferTime, filter } from "rxjs";
import type { TelemetryPacket } from "../types/telemetry.js";
import type { TelemetryReceiver } from "../ingestion/receiver.js";
import { SlidingWindow } from "./windowStats.js";
import { AnomalyEngine, type AnomalyEvent } from "./anomalyEngine.js";

export interface ProcessedPacket {
  readonly packet: TelemetryPacket;
  readonly carId: number;
  readonly speedStats: ReturnType<SlidingWindow["getStats"]>;
  readonly rpmStats: ReturnType<SlidingWindow["getStats"]>;
  readonly tireTempFLStats: ReturnType<SlidingWindow["getStats"]>;
  readonly anomalies: AnomalyEvent[];
  readonly processedAt: number;
}

export interface StreamProcessorOptions {
  /**
   * Size of the sliding window in packets per car.
   * At 10,000 pkts/sec with 4 cars, 2,500 packets/car/sec means
   * a window of 12,500 covers roughly 5 seconds. Default: 12500.
   */
  readonly windowSize?: number;
  /**
   * RxJS bufferTime window in ms. Packets are batched into this window
   * before being emitted as a group on the 'processed_batch' event.
   * Default: 100ms.
   */
  readonly bufferWindowMs?: number;
}

interface CarState {
  readonly speedWindow: SlidingWindow;
  readonly rpmWindow: SlidingWindow;
  readonly tireTempFLWindow: SlidingWindow;
  readonly anomalyEngine: AnomalyEngine;
}

export interface StreamProcessorEvents {
  processed: [result: ProcessedPacket];
  processed_batch: [results: ProcessedPacket[]];
  anomaly: [event: AnomalyEvent];
  error: [error: Error];
}

const DEFAULT_OPTIONS: Required<StreamProcessorOptions> = {
  windowSize: 12_500,
  bufferWindowMs: 100,
};

export class StreamProcessor extends EventEmitter {
  private readonly options: Required<StreamProcessorOptions>;
  private readonly subject = new Subject<TelemetryPacket>();
  private readonly carStates = new Map<number, CarState>();
  private packetCount = 0;
  private anomalyCount = 0;

  public constructor(options: StreamProcessorOptions = {}) {
    super();
    this.options = { ...DEFAULT_OPTIONS, ...options };
    this.setupPipeline();
  }

  public override on<K extends keyof StreamProcessorEvents>(
    event: K,
    listener: (...args: StreamProcessorEvents[K]) => void,
  ): this {
    return super.on(event, listener as (...args: unknown[]) => void);
  }

  public override emit<K extends keyof StreamProcessorEvents>(
    event: K,
    ...args: StreamProcessorEvents[K]
  ): boolean {
    return super.emit(event, ...args);
  }

  private getOrCreateCarState(carId: number): CarState {
    const existing = this.carStates.get(carId);
    if (existing) return existing;
    const state: CarState = {
      speedWindow: new SlidingWindow(this.options.windowSize),
      rpmWindow: new SlidingWindow(this.options.windowSize),
      tireTempFLWindow: new SlidingWindow(this.options.windowSize),
      anomalyEngine: new AnomalyEngine(),
    };
    this.carStates.set(carId, state);
    return state;
  }

  private processPacket(packet: TelemetryPacket): ProcessedPacket {
    const state = this.getOrCreateCarState(packet.carId);

    state.speedWindow.push(packet.speed);
    state.rpmWindow.push(packet.rpm);
    state.tireTempFLWindow.push(packet.tireTempFL);

    const speedStats = state.speedWindow.getStats();
    const rpmStats = state.rpmWindow.getStats();
    const tireTempFLStats = state.tireTempFLWindow.getStats();

    const anomalies = state.anomalyEngine.evaluate(
      packet,
      speedStats,
      rpmStats,
      tireTempFLStats,
    );

    this.packetCount++;
    this.anomalyCount += anomalies.length;

    return {
      packet,
      carId: packet.carId,
      speedStats,
      rpmStats,
      tireTempFLStats,
      anomalies,
      processedAt: performance.now(),
    };
  }

  private setupPipeline(): void {
    // Individual processed packets.
    this.subject.subscribe({
      next: (packet) => {
        try {
          const result = this.processPacket(packet);
          this.emit("processed", result);
          for (const anomaly of result.anomalies) {
            this.emit("anomaly", anomaly);
          }
        } catch (err) {
          this.emit("error", err instanceof Error ? err : new Error(String(err)));
        }
      },
      error: (err: unknown) => {
        this.emit("error", err instanceof Error ? err : new Error(String(err)));
      },
    });

    // Batched output every bufferWindowMs for downstream consumers that
    // prefer processing in groups (e.g. the DuckDB writer in Phase 3).
    this.subject.pipe(
      bufferTime(this.options.bufferWindowMs),
      filter((packets) => packets.length > 0),
    ).subscribe({
      next: (packets) => {
        const results = packets.map((p) => this.processPacket(p));
        this.emit("processed_batch", results);
      },
      error: (err: unknown) => {
        this.emit("error", err instanceof Error ? err : new Error(String(err)));
      },
    });
  }

  /** Feed a single packet into the processing pipeline. */
  public push(packet: TelemetryPacket): void {
    this.subject.next(packet);
  }

  /** Wire directly to a TelemetryReceiver — subscribes to its 'packet' events. */
  public attachReceiver(receiver: TelemetryReceiver): void {
    receiver.on("packet", (packet) => this.push(packet));
  }

  public getStats(): { packetCount: number; anomalyCount: number; carCount: number } {
    return {
      packetCount: this.packetCount,
      anomalyCount: this.anomalyCount,
      carCount: this.carStates.size,
    };
  }

  public destroy(): void {
    this.subject.complete();
    this.carStates.clear();
  }
}