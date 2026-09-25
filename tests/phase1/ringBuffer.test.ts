import { describe, expect, it } from "vitest";
import { SharedRingBuffer } from "../../src/ingestion/ringBuffer.js";
import type { TelemetryPacket } from "../../src/types/telemetry.js";

function makePacket(overrides: Partial<TelemetryPacket> = {}): TelemetryPacket {
  return {
    timestamp: 1_700_000_000_000,
    sequenceId: 0,
    carId: 1,
    gear: 4,
    speed: 220.5,
    rpm: 11_400,
    throttle: 0.87,
    brake: 0,
    tireTempFL: 92.1,
    tireTempFR: 91.7,
    tireTempRL: 88.4,
    tireTempRR: 88.9,
    ...overrides,
  };
}

describe("SharedRingBuffer: construction", () => {
  it("creates a buffer with the requested capacity", () => {
    const buffer = SharedRingBuffer.create(16);
    expect(buffer.slotCapacity).toBe(16);
    expect(buffer.size).toBe(0);
    expect(buffer.isEmpty).toBe(true);
    expect(buffer.isFull).toBe(false);
    expect(buffer.droppedCount).toBe(0);
  });

  it("rejects a non-integer or too-small capacity", () => {
    expect(() => SharedRingBuffer.create(1)).toThrow(RangeError);
    expect(() => SharedRingBuffer.create(0)).toThrow(RangeError);
    expect(() => SharedRingBuffer.create(-4)).toThrow(RangeError);
    expect(() => SharedRingBuffer.create(3.5)).toThrow(RangeError);
  });

  it("attach() produces a view over the exact same underlying memory", () => {
    const producer = SharedRingBuffer.create(8);
    const consumer = SharedRingBuffer.attach(producer.handles);

    expect(producer.write(makePacket({ sequenceId: 1 }))).toBe(true);

    // Written via `producer`, visible to `consumer` because both wrap the
    // same SharedArrayBuffer.
    expect(consumer.size).toBe(1);
    const read = consumer.tryRead();
    expect(read?.sequenceId).toBe(1);
    expect(producer.size).toBe(0);
  });

  it("attach() rejects a handle whose declared capacity does not match the buffer", () => {
    const producer = SharedRingBuffer.create(8);
    const handles = producer.handles;
    expect(() => SharedRingBuffer.attach({ ...handles, capacity: 999 })).toThrow(RangeError);
  });
});

describe("SharedRingBuffer: write/read round trip", () => {
  it("preserves every field through a write/read cycle", () => {
    const buffer = SharedRingBuffer.create(4);
    const packet = makePacket({
      sequenceId: 42,
      carId: 7,
      gear: -1,
      speed: 0.001,
      rpm: 900.25,
      throttle: 1,
      brake: 0.333,
      tireTempFL: 39.99,
      tireTempFR: 140,
      tireTempRL: 40,
      tireTempRR: 139.999,
    });

    expect(buffer.write(packet)).toBe(true);
    const result = buffer.tryRead();

    expect(result).not.toBeNull();
    expect(result).toEqual(packet);
  });

  it("returns null when reading from an empty buffer", () => {
    const buffer = SharedRingBuffer.create(4);
    expect(buffer.tryRead()).toBeNull();
  });

  it("maintains FIFO order across interleaved writes and reads", () => {
    const buffer = SharedRingBuffer.create(4);

    buffer.write(makePacket({ sequenceId: 1 }));
    buffer.write(makePacket({ sequenceId: 2 }));
    expect(buffer.tryRead()?.sequenceId).toBe(1);

    buffer.write(makePacket({ sequenceId: 3 }));
    expect(buffer.tryRead()?.sequenceId).toBe(2);
    expect(buffer.tryRead()?.sequenceId).toBe(3);
    expect(buffer.tryRead()).toBeNull();
  });
});

describe("SharedRingBuffer: overflow and underflow", () => {
  it("reports full one slot before nominal capacity and drops writes past that point", () => {
    // Capacity 4 usable slots = 3, by the head/tail full-detection design.
    const buffer = SharedRingBuffer.create(4);

    expect(buffer.write(makePacket({ sequenceId: 1 }))).toBe(true);
    expect(buffer.write(makePacket({ sequenceId: 2 }))).toBe(true);
    expect(buffer.write(makePacket({ sequenceId: 3 }))).toBe(true);
    expect(buffer.isFull).toBe(true);

    expect(buffer.write(makePacket({ sequenceId: 4 }))).toBe(false);
    expect(buffer.droppedCount).toBe(1);

    expect(buffer.write(makePacket({ sequenceId: 5 }))).toBe(false);
    expect(buffer.droppedCount).toBe(2);

    // The three successful writes are still intact and in order.
    expect(buffer.tryRead()?.sequenceId).toBe(1);
    expect(buffer.tryRead()?.sequenceId).toBe(2);
    expect(buffer.tryRead()?.sequenceId).toBe(3);
    expect(buffer.tryRead()).toBeNull();
  });

  it("recovers correctly after wrapping around the buffer multiple times", () => {
    const buffer = SharedRingBuffer.create(4); // 3 usable slots
    let nextExpectedRead = 0;

    for (let sequenceId = 0; sequenceId < 1000; sequenceId++) {
      expect(buffer.write(makePacket({ sequenceId }))).toBe(true);
      const read = buffer.tryRead();
      expect(read?.sequenceId).toBe(nextExpectedRead);
      nextExpectedRead++;
    }
    expect(buffer.droppedCount).toBe(0);
    expect(buffer.isEmpty).toBe(true);
  });
});

describe("SharedRingBuffer: readBatch", () => {
  it("drains up to maxCount packets in FIFO order", () => {
    const buffer = SharedRingBuffer.create(16);
    for (let i = 0; i < 10; i++) {
      buffer.write(makePacket({ sequenceId: i }));
    }

    const firstBatch = buffer.readBatch(4);
    expect(firstBatch.map((p) => p.sequenceId)).toEqual([0, 1, 2, 3]);
    expect(buffer.size).toBe(6);

    const secondBatch = buffer.readBatch(100);
    expect(secondBatch.map((p) => p.sequenceId)).toEqual([4, 5, 6, 7, 8, 9]);
    expect(buffer.isEmpty).toBe(true);
  });

  it("returns an empty array when there is nothing to read", () => {
    const buffer = SharedRingBuffer.create(8);
    expect(buffer.readBatch(10)).toEqual([]);
  });
});

describe("SharedRingBuffer: throughput", () => {
  it("sustains 100,000 write/read cycles with zero data loss and full sequence integrity", () => {
    const buffer = SharedRingBuffer.create(2048);
    const totalPackets = 100_000;
    let written = 0;
    let readCount = 0;
    let nextExpectedSequence = 0;

    const start = performance.now();

    while (readCount < totalPackets) {
      // Fill as much as possible. We check `isFull` up front rather than
      // relying on `write()`'s return value to detect "no room left",
      // because calling `write()` on a full buffer is a *real* drop
      // (it increments the buffer's own dropped-packet counter) — this
      // test is about sustained throughput with a consumer keeping pace,
      // not about overflow behavior (covered separately below).
      while (written < totalPackets && !buffer.isFull) {
        buffer.write(makePacket({ sequenceId: written }));
        written++;
      }
      // Drain a batch.
      const batch = buffer.readBatch(512);
      for (const packet of batch) {
        expect(packet.sequenceId).toBe(nextExpectedSequence);
        nextExpectedSequence++;
        readCount++;
      }
      if (batch.length === 0 && written >= totalPackets) {
        break;
      }
    }

    const elapsedMs = performance.now() - start;

    expect(readCount).toBe(totalPackets);
    expect(buffer.droppedCount).toBe(0);
    // Generous ceiling — this is a correctness/regression guard, not a
    // tuned micro-benchmark, but a gross performance cliff should fail it.
    expect(elapsedMs).toBeLessThan(5000);
  });
});
