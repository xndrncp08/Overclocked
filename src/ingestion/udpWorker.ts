/**
 * src/ingestion/udpWorker.ts
 *
 * Runs inside a dedicated worker_thread. Binds a UDP socket, decodes each
 * incoming datagram as a TelemetryPacket, and pushes it into the shared
 * ring buffer. Ingestion is kept on its own thread so that a burst of
 * datagrams, or a slow downstream consumer, can never block the event
 * loop that's accepting new packets off the socket.
 *
 * This file is only ever loaded as a worker entry point (via
 * `new Worker(new URL(import.meta.url), ...)` from receiver.ts) — it is
 * not meant to be imported directly.
 */

import dgram from "node:dgram";
import { parentPort, workerData } from "node:worker_threads";
import { decodeBinaryPacket, PACKET_BYTE_LENGTH } from "../types/telemetry.js";
import { SharedRingBuffer, type RingBufferHandles } from "./ringBuffer.js";

export interface UdpWorkerData {
  readonly host: string;
  readonly port: number;
  readonly ringBufferHandles: RingBufferHandles;
  readonly statsIntervalMs: number;
}

export type UdpWorkerMessage =
  | { readonly type: "ready"; readonly host: string; readonly port: number }
  | {
      readonly type: "stats";
      readonly timestamp: number;
      readonly receivedCount: number;
      readonly malformedCount: number;
      readonly droppedCount: number;
      readonly bufferSize: number;
      readonly bufferCapacity: number;
    }
  | { readonly type: "error"; readonly message: string; readonly stack?: string };

function isUdpWorkerData(value: unknown): value is UdpWorkerData {
  if (typeof value !== "object" || value === null) return false;
  const candidate = value as Record<string, unknown>;
  return (
    typeof candidate.host === "string" &&
    typeof candidate.port === "number" &&
    typeof candidate.statsIntervalMs === "number" &&
    typeof candidate.ringBufferHandles === "object" &&
    candidate.ringBufferHandles !== null
  );
}

function postMessage(message: UdpWorkerMessage): void {
  parentPort?.postMessage(message);
}

function main(): void {
  if (!parentPort) {
    throw new Error("udpWorker.ts must be run as a worker_thread, not the main thread");
  }
  if (!isUdpWorkerData(workerData)) {
    throw new TypeError("udpWorker.ts received malformed workerData");
  }

  const { host, port, ringBufferHandles, statsIntervalMs } = workerData;
  const ringBuffer = SharedRingBuffer.attach(ringBufferHandles);
  const socket = dgram.createSocket("udp4");

  let receivedCount = 0;
  let malformedCount = 0;

  socket.on("message", (msg: Buffer) => {
    if (msg.byteLength !== PACKET_BYTE_LENGTH) {
      malformedCount++;
      return;
    }
    try {
      const packet = decodeBinaryPacket(msg);
      receivedCount++;
      ringBuffer.write(packet);
    } catch {
      malformedCount++;
    }
  });

  socket.on("error", (err: Error) => {
    postMessage(
      err.stack === undefined
        ? { type: "error", message: err.message }
        : { type: "error", message: err.message, stack: err.stack },
    );
  });

  socket.on("listening", () => {
    const address = socket.address();
    postMessage({ type: "ready", host: address.address, port: address.port });
  });

  socket.bind(port, host);

  const statsTimer = setInterval(() => {
    postMessage({
      type: "stats",
      timestamp: Date.now(),
      receivedCount,
      malformedCount,
      droppedCount: ringBuffer.droppedCount,
      bufferSize: ringBuffer.size,
      bufferCapacity: ringBuffer.slotCapacity,
    });
  }, statsIntervalMs);
  statsTimer.unref();

  const shutdown = (): void => {
    clearInterval(statsTimer);
    socket.close();
  };

  parentPort.on("message", (message: unknown) => {
    if (message === "shutdown") {
      shutdown();
    }
  });

  parentPort.on("close", shutdown);
}

main();
