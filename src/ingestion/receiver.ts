/**
 * src/ingestion/receiver.ts
 *
 * Main-thread orchestrator for Phase 1 ingestion:
 *  - Allocates the SharedArrayBuffer-backed ring buffer.
 *  - Spawns the UDP listener on a worker_thread and hands it the buffer.
 *  - Drains the ring buffer on a tight poll loop and re-emits packets as
 *    Node events for downstream consumers (Phase 2's stream processor
 *    will subscribe to `'packet'`/`'batch'`).
 *  - Owns graceful shutdown: SIGINT/SIGTERM stop the poll loop, tell the
 *    worker to close its socket, and terminate the worker thread.
 *
 * Run directly (`npm run receive`) it also prints live throughput stats
 * to stdout, which is useful on its own for smoke-testing Phase 1 before
 * Phase 2's TUI dashboard exists.
 */

import { EventEmitter } from "node:events";
import { Worker } from "node:worker_threads";
import { fileURLToPath } from "node:url";
import type { TelemetryPacket } from "../types/telemetry.js";
import { SharedRingBuffer } from "./ringBuffer.js";
import { resolveWorkerEntryUrl } from "./resolveWorkerEntry.js";
import type { UdpWorkerData, UdpWorkerMessage } from "./udpWorker.js";

export interface ReceiverOptions {
  /** UDP bind host. Defaults to "0.0.0.0". */
  readonly host?: string;
  /** UDP bind port. Defaults to 41234. */
  readonly port?: number;
  /** Ring buffer capacity in packet slots. Defaults to 65536. */
  readonly ringBufferCapacity?: number;
  /** How often the worker reports stats upstream, in ms. Defaults to 1000. */
  readonly statsIntervalMs?: number;
  /** How often the main thread polls the ring buffer for new packets, in ms. Defaults to 1. */
  readonly pollIntervalMs?: number;
  /** Max packets drained from the ring buffer per poll tick. Defaults to 2048. */
  readonly maxBatchSize?: number;
}

export interface ReceiverStats {
  readonly receivedCount: number;
  readonly processedCount: number;
  readonly malformedCount: number;
  readonly droppedCount: number;
  readonly bufferSize: number;
  readonly bufferCapacity: number;
  readonly lastUpdated: number;
}

const DEFAULT_OPTIONS = {
  host: "0.0.0.0",
  port: 41234,
  ringBufferCapacity: 65536,
  statsIntervalMs: 1000,
  pollIntervalMs: 1,
  maxBatchSize: 2048,
} as const satisfies Required<ReceiverOptions>;

/**
 * Typed event map for `TelemetryReceiver`. Declared separately so
 * `EventEmitter`'s `on`/`emit` overloads stay fully typed at call sites.
 */
export interface ReceiverEvents {
  ready: [address: { host: string; port: number }];
  packet: [packet: TelemetryPacket];
  batch: [packets: TelemetryPacket[]];
  stats: [stats: ReceiverStats];
  error: [error: Error];
  shutdown: [];
}

export class TelemetryReceiver extends EventEmitter {
  private readonly options: Required<ReceiverOptions>;
  private readonly ringBuffer: SharedRingBuffer;
  private worker: Worker | null = null;
  private pollHandle: NodeJS.Timeout | null = null;

  private processedCount = 0;
  private latestWorkerStats: Omit<ReceiverStats, "processedCount"> = {
    receivedCount: 0,
    malformedCount: 0,
    droppedCount: 0,
    bufferSize: 0,
    bufferCapacity: 0,
    lastUpdated: Date.now(),
  };

  public constructor(options: ReceiverOptions = {}) {
    super();
    this.options = { ...DEFAULT_OPTIONS, ...options };
    this.ringBuffer = SharedRingBuffer.create(this.options.ringBufferCapacity);
  }

  public override on<K extends keyof ReceiverEvents>(
    event: K,
    listener: (...args: ReceiverEvents[K]) => void,
  ): this {
    return super.on(event, listener as (...args: unknown[]) => void);
  }

  public override emit<K extends keyof ReceiverEvents>(event: K, ...args: ReceiverEvents[K]): boolean {
    return super.emit(event, ...args);
  }

  /**
   * Spin up the UDP worker and begin draining the ring buffer.
   *
   * Async because in dev mode (running from `.ts` source via `tsx`) it
   * first builds a plain-JS esbuild bundle of udpWorker.ts before spawning
   * the worker. Worker threads load their entry through Node's bare module
   * resolver — not through whatever hooks (e.g. tsx's `.js` -> `.ts`
   * remapping) are active on the main thread — so handing a worker a `.ts`
   * entry would fail without that pre-build step. In the compiled build
   * (`npm run build`) the worker's `.js` file already exists on disk and
   * this returns immediately.
   */
  public async start(): Promise<void> {
    if (this.worker !== null) {
      throw new Error("TelemetryReceiver.start() called while already running");
    }

    const workerUrl = await resolveWorkerEntryUrl();
    const workerData: UdpWorkerData = {
      host: this.options.host,
      port: this.options.port,
      ringBufferHandles: this.ringBuffer.handles,
      statsIntervalMs: this.options.statsIntervalMs,
    };

    this.worker = new Worker(workerUrl, { workerData });

    this.worker.on("message", (message: UdpWorkerMessage) => {
      switch (message.type) {
        case "ready":
          this.emit("ready", { host: message.host, port: message.port });
          break;
        case "stats":
          this.latestWorkerStats = {
            receivedCount: message.receivedCount,
            malformedCount: message.malformedCount,
            droppedCount: message.droppedCount,
            bufferSize: message.bufferSize,
            bufferCapacity: message.bufferCapacity,
            lastUpdated: message.timestamp,
          };
          this.emit("stats", { ...this.latestWorkerStats, processedCount: this.processedCount });
          break;
        case "error":
          this.emit("error", Object.assign(new Error(message.message), { stack: message.stack }));
          break;
      }
    });

    this.worker.on("error", (err: Error) => {
      this.emit("error", err);
    });

    this.pollHandle = setInterval(() => {
      const batch = this.ringBuffer.readBatch(this.options.maxBatchSize);
      if (batch.length === 0) {
        return;
      }
      this.processedCount += batch.length;
      for (const packet of batch) {
        this.emit("packet", packet);
      }
      this.emit("batch", batch);
    }, this.options.pollIntervalMs);
  }

  /** Current snapshot of ingestion/processing statistics. */
  public getStats(): ReceiverStats {
    return { ...this.latestWorkerStats, processedCount: this.processedCount };
  }

  /** Direct access to the ring buffer, e.g. for Phase 2/3 consumers that want to drain it themselves. */
  public getRingBuffer(): SharedRingBuffer {
    return this.ringBuffer;
  }

  /** Gracefully stop polling, ask the worker to close its socket, then terminate it. */
  public async stop(): Promise<void> {
    if (this.pollHandle !== null) {
      clearInterval(this.pollHandle);
      this.pollHandle = null;
    }
    if (this.worker !== null) {
      this.worker.postMessage("shutdown");
      await this.worker.terminate();
      this.worker = null;
    }
    this.emit("shutdown");
  }
}

/* -------------------------------------------------------------------------
 * CLI entry point: `npm run receive`
 * ---------------------------------------------------------------------- */

function isMainModule(): boolean {
  if (!process.argv[1]) return false;
  return fileURLToPath(import.meta.url) === process.argv[1];
}

if (isMainModule()) {
  const receiver = new TelemetryReceiver();
  let shuttingDown = false;

  receiver.on("ready", ({ host, port }) => {
    console.log(`[receiver] listening for UDP telemetry on ${host}:${port}`);
  });

  receiver.on("stats", (stats) => {
    console.log(
      `[receiver] recv=${stats.receivedCount} proc=${stats.processedCount} ` +
        `malformed=${stats.malformedCount} dropped=${stats.droppedCount} ` +
        `buffer=${stats.bufferSize}/${stats.bufferCapacity}`,
    );
  });

  receiver.on("error", (err) => {
    console.error("[receiver] error:", err.message);
  });

  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[receiver] received ${signal}, shutting down gracefully...`);
    await receiver.stop();
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  await receiver.start();
}
