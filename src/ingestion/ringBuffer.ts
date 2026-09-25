/**
 * src/ingestion/ringBuffer.ts
 *
 * A fixed-capacity, single-producer/single-consumer (SPSC) circular buffer
 * backed by SharedArrayBuffer, so the UDP ingestion worker thread (producer)
 * and the main thread (consumer) can hand off telemetry packets without
 * copying between thread heaps and without a JS-level lock.
 *
 * Design notes:
 *  - Two SharedArrayBuffers are used: `data` (packet payloads, Float64) and
 *    `control` (head/tail/dropped counters, Int32, accessed via Atomics).
 *  - Because there is exactly one writer and one reader, we don't need a
 *    CAS retry loop to claim slots — we only need Atomics.load/store so
 *    that writes on one thread become visible to reads on the other
 *    (SharedArrayBuffer + Atomics is what gives that cross-thread memory
 *    visibility guarantee; plain non-atomic reads/writes are not
 *    guaranteed to be observed promptly, or at all, on another thread).
 *  - The buffer is considered full when advancing `head` would make it
 *    equal to `tail` — this sacrifices exactly one slot of capacity in
 *    exchange for a branch-free full/empty test, a standard ring-buffer
 *    trade-off.
 */

import { FIELDS_PER_PACKET, decodeRingPacket, encodeRingPacket, type TelemetryPacket } from "../types/telemetry.js";

/** Index layout inside the Int32Array control block. */
const enum ControlIndex {
  HEAD = 0, // next slot the producer will write to
  TAIL = 1, // next slot the consumer will read from
  DROPPED = 2, // count of packets dropped because the buffer was full
  CAPACITY = 3, // slot capacity, stored for attach()-side validation
  CONTROL_LENGTH = 4,
}

/** The two SharedArrayBuffers that make up one ring buffer's storage. */
export interface RingBufferHandles {
  readonly data: SharedArrayBuffer;
  readonly control: SharedArrayBuffer;
  readonly capacity: number;
}

export class SharedRingBuffer {
  private readonly capacity: number;
  private readonly data: Float64Array;
  private readonly control: Int32Array;

  private constructor(capacity: number, data: SharedArrayBuffer, control: SharedArrayBuffer) {
    this.capacity = capacity;
    this.data = new Float64Array(data);
    this.control = new Int32Array(control);
  }

  /**
   * Allocate fresh backing SharedArrayBuffers for a ring buffer of the
   * given slot capacity, and return a ready-to-use instance. Pass the
   * returned `.handles` to `SharedRingBuffer.attach()` in another thread
   * (e.g. via `workerData`) to get a view over the exact same memory.
   */
  public static create(capacity: number): SharedRingBuffer {
    if (!Number.isInteger(capacity) || capacity < 2) {
      throw new RangeError("SharedRingBuffer capacity must be an integer >= 2");
    }
    const dataBytes = capacity * FIELDS_PER_PACKET * Float64Array.BYTES_PER_ELEMENT;
    const dataBuffer = new SharedArrayBuffer(dataBytes);
    const controlBuffer = new SharedArrayBuffer(ControlIndex.CONTROL_LENGTH * Int32Array.BYTES_PER_ELEMENT);

    const instance = new SharedRingBuffer(capacity, dataBuffer, controlBuffer);
    instance.control[ControlIndex.CAPACITY] = capacity;
    return instance;
  }

  /** Attach to an existing ring buffer's memory from another thread. */
  public static attach(handles: RingBufferHandles): SharedRingBuffer {
    const instance = new SharedRingBuffer(handles.capacity, handles.data, handles.control);
    const storedCapacity = Atomics.load(instance.control, ControlIndex.CAPACITY);
    if (storedCapacity !== handles.capacity) {
      throw new RangeError(
        `SharedRingBuffer.attach: capacity mismatch (handle says ${handles.capacity}, buffer says ${storedCapacity})`,
      );
    }
    return instance;
  }

  /** The raw handles to hand to another thread (e.g. `workerData`). */
  public get handles(): RingBufferHandles {
    return {
      data: this.data.buffer as SharedArrayBuffer,
      control: this.control.buffer as SharedArrayBuffer,
      capacity: this.capacity,
    };
  }

  /** Total number of packet slots. */
  public get slotCapacity(): number {
    return this.capacity;
  }

  /** Number of packets currently buffered and unread. */
  public get size(): number {
    const head = Atomics.load(this.control, ControlIndex.HEAD);
    const tail = Atomics.load(this.control, ControlIndex.TAIL);
    return (head - tail + this.capacity) % this.capacity;
  }

  /** True when the buffer holds no unread packets. */
  public get isEmpty(): boolean {
    return Atomics.load(this.control, ControlIndex.HEAD) === Atomics.load(this.control, ControlIndex.TAIL);
  }

  /** True when the buffer cannot accept another packet without overwriting unread data. */
  public get isFull(): boolean {
    const head = Atomics.load(this.control, ControlIndex.HEAD);
    const tail = Atomics.load(this.control, ControlIndex.TAIL);
    return (head + 1) % this.capacity === tail;
  }

  /** Total packets dropped over this buffer's lifetime due to overflow. */
  public get droppedCount(): number {
    return Atomics.load(this.control, ControlIndex.DROPPED);
  }

  /**
   * Producer-side: write one packet. Returns `true` if the write
   * succeeded, `false` if the buffer was full (in which case the packet
   * is discarded and the dropped-packet counter is incremented).
   */
  public write(packet: TelemetryPacket): boolean {
    const head = Atomics.load(this.control, ControlIndex.HEAD);
    const tail = Atomics.load(this.control, ControlIndex.TAIL);
    const nextHead = (head + 1) % this.capacity;

    if (nextHead === tail) {
      Atomics.add(this.control, ControlIndex.DROPPED, 1);
      return false;
    }

    encodeRingPacket(packet, this.data, head * FIELDS_PER_PACKET);
    Atomics.store(this.control, ControlIndex.HEAD, nextHead);
    return true;
  }

  /**
   * Consumer-side: read and remove one packet. Returns `null` (rather than
   * throwing) when the buffer is empty, since "nothing to read yet" is an
   * expected steady-state condition in a polling consumer loop, not an
   * error.
   */
  public tryRead(): TelemetryPacket | null {
    const tail = Atomics.load(this.control, ControlIndex.TAIL);
    const head = Atomics.load(this.control, ControlIndex.HEAD);

    if (tail === head) {
      return null;
    }

    const packet = decodeRingPacket(this.data, tail * FIELDS_PER_PACKET);
    Atomics.store(this.control, ControlIndex.TAIL, (tail + 1) % this.capacity);
    return packet;
  }

  /**
   * Consumer-side: drain up to `maxCount` packets in one call. Cheaper
   * than repeated `tryRead()` calls for downstream code that processes in
   * batches (e.g. the DuckDB batch-flush writer in Phase 3).
   */
  public readBatch(maxCount: number): TelemetryPacket[] {
    const results: TelemetryPacket[] = [];
    for (let i = 0; i < maxCount; i++) {
      const packet = this.tryRead();
      if (packet === null) {
        break;
      }
      results.push(packet);
    }
    return results;
  }
}
