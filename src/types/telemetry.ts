/**
 * src/types/telemetry.ts
 *
 * Core domain types for the Overclocked telemetry pipeline, plus the two
 * binary encodings the system relies on:
 *
 *  1. WIRE format  — the compact fixed-width Buffer layout sent over UDP
 *     between the synthetic generator and the ingestion worker.
 *  2. RING format  — the fixed-width Float64 layout used inside the
 *     SharedArrayBuffer-backed ring buffer (uniform slot width keeps the
 *     buffer's index math trivial and avoids mixed-typed-array bookkeeping).
 *
 * Both encodings are defined here, in one place, so producers and
 * consumers can never drift out of sync with each other.
 */

/** A single decoded telemetry sample for one car at one instant. */
export interface TelemetryPacket {
  /** Epoch milliseconds (float for sub-millisecond precision), producer clock. */
  readonly timestamp: number;
  /** Monotonically increasing packet sequence number, wraps at 2^32. */
  readonly sequenceId: number;
  /** Car/session identifier, 0-255. */
  readonly carId: number;
  /** Current gear: -1 = reverse, 0 = neutral, 1-8 = forward gears. */
  readonly gear: number;
  /** Speed in km/h. */
  readonly speed: number;
  /** Engine speed in RPM. */
  readonly rpm: number;
  /** Throttle position, 0.0 (closed) to 1.0 (floored). */
  readonly throttle: number;
  /** Brake pressure, 0.0 (released) to 1.0 (full lock). */
  readonly brake: number;
  /** Front-left tire surface temperature, degrees Celsius. */
  readonly tireTempFL: number;
  /** Front-right tire surface temperature, degrees Celsius. */
  readonly tireTempFR: number;
  /** Rear-left tire surface temperature, degrees Celsius. */
  readonly tireTempRL: number;
  /** Rear-right tire surface temperature, degrees Celsius. */
  readonly tireTempRR: number;
}

/** Ordered field list used for the ring-buffer (uniform float64) encoding. */
export const RING_FIELD_ORDER = [
  "timestamp",
  "sequenceId",
  "carId",
  "gear",
  "speed",
  "rpm",
  "throttle",
  "brake",
  "tireTempFL",
  "tireTempFR",
  "tireTempRL",
  "tireTempRR",
] as const satisfies readonly (keyof TelemetryPacket)[];

/** Number of float64 slots one packet occupies inside the ring buffer. */
export const FIELDS_PER_PACKET = RING_FIELD_ORDER.length;

/**
 * Encode a packet into a Float64Array at the given element offset.
 * Used by the ring buffer's write path (offset = slotIndex * FIELDS_PER_PACKET).
 */
export function encodeRingPacket(
  packet: TelemetryPacket,
  target: Float64Array,
  offset: number,
): void {
  for (let i = 0; i < FIELDS_PER_PACKET; i++) {
    const field = RING_FIELD_ORDER[i] as keyof TelemetryPacket;
    target[offset + i] = packet[field];
  }
}

/**
 * Decode a packet from a Float64Array at the given element offset.
 * Used by the ring buffer's read path.
 */
export function decodeRingPacket(source: Float64Array, offset: number): TelemetryPacket {
  const values: number[] = new Array(FIELDS_PER_PACKET);
  for (let i = 0; i < FIELDS_PER_PACKET; i++) {
    values[i] = source[offset + i] ?? 0;
  }
  return {
    timestamp: values[0] as number,
    sequenceId: values[1] as number,
    carId: values[2] as number,
    gear: values[3] as number,
    speed: values[4] as number,
    rpm: values[5] as number,
    throttle: values[6] as number,
    brake: values[7] as number,
    tireTempFL: values[8] as number,
    tireTempFR: values[9] as number,
    tireTempRL: values[10] as number,
    tireTempRR: values[11] as number,
  };
}

/* -------------------------------------------------------------------------
 * WIRE (UDP) binary schema
 *
 * Fixed 80-byte little-endian layout. Chosen over JSON for throughput: at
 * 10,000 pkts/sec, JSON.parse/stringify overhead and payload size (~250
 * bytes/packet as text vs. 80 bytes binary) become the bottleneck well
 * before the network or the ring buffer do.
 *
 * Offset  Bytes  Field         Type
 * ------  -----  ------------  -------
 *      0      8  timestamp     Float64
 *      8      4  sequenceId    Uint32
 *     12      1  carId         Uint8
 *     13      1  gear          Int8
 *     14      2  (reserved)    padding, always zero
 *     16      8  speed         Float64
 *     24      8  rpm           Float64
 *     32      8  throttle      Float64
 *     40      8  brake         Float64
 *     48      8  tireTempFL    Float64
 *     56      8  tireTempFR    Float64
 *     64      8  tireTempRL    Float64
 *     72      8  tireTempRR    Float64
 *     80  (total length)
 * ---------------------------------------------------------------------- */

export const PACKET_BYTE_LENGTH = 80;

const WIRE_OFFSETS = {
  timestamp: 0,
  sequenceId: 8,
  carId: 12,
  gear: 13,
  speed: 16,
  rpm: 24,
  throttle: 32,
  brake: 40,
  tireTempFL: 48,
  tireTempFR: 56,
  tireTempRL: 64,
  tireTempRR: 72,
} as const;

const WIRE_LITTLE_ENDIAN = true;

/** Encode a TelemetryPacket into a new 80-byte Buffer for UDP transmission. */
export function encodeBinaryPacket(packet: TelemetryPacket): Buffer {
  const buf = Buffer.allocUnsafe(PACKET_BYTE_LENGTH);
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

  view.setFloat64(WIRE_OFFSETS.timestamp, packet.timestamp, WIRE_LITTLE_ENDIAN);
  view.setUint32(WIRE_OFFSETS.sequenceId, packet.sequenceId >>> 0, WIRE_LITTLE_ENDIAN);
  view.setUint8(WIRE_OFFSETS.carId, packet.carId & 0xff);
  view.setInt8(WIRE_OFFSETS.gear, packet.gear);
  view.setUint16(14, 0, WIRE_LITTLE_ENDIAN); // reserved padding
  view.setFloat64(WIRE_OFFSETS.speed, packet.speed, WIRE_LITTLE_ENDIAN);
  view.setFloat64(WIRE_OFFSETS.rpm, packet.rpm, WIRE_LITTLE_ENDIAN);
  view.setFloat64(WIRE_OFFSETS.throttle, packet.throttle, WIRE_LITTLE_ENDIAN);
  view.setFloat64(WIRE_OFFSETS.brake, packet.brake, WIRE_LITTLE_ENDIAN);
  view.setFloat64(WIRE_OFFSETS.tireTempFL, packet.tireTempFL, WIRE_LITTLE_ENDIAN);
  view.setFloat64(WIRE_OFFSETS.tireTempFR, packet.tireTempFR, WIRE_LITTLE_ENDIAN);
  view.setFloat64(WIRE_OFFSETS.tireTempRL, packet.tireTempRL, WIRE_LITTLE_ENDIAN);
  view.setFloat64(WIRE_OFFSETS.tireTempRR, packet.tireTempRR, WIRE_LITTLE_ENDIAN);

  return buf;
}

/**
 * Decode an 80-byte Buffer (as received off a UDP socket) into a
 * TelemetryPacket. Throws a RangeError if the buffer is the wrong length,
 * so malformed/truncated datagrams fail loudly instead of silently
 * corrupting downstream stats.
 */
export function decodeBinaryPacket(buf: Buffer): TelemetryPacket {
  if (buf.byteLength !== PACKET_BYTE_LENGTH) {
    throw new RangeError(
      `decodeBinaryPacket: expected ${PACKET_BYTE_LENGTH} bytes, got ${buf.byteLength}`,
    );
  }
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);

  return {
    timestamp: view.getFloat64(WIRE_OFFSETS.timestamp, WIRE_LITTLE_ENDIAN),
    sequenceId: view.getUint32(WIRE_OFFSETS.sequenceId, WIRE_LITTLE_ENDIAN),
    carId: view.getUint8(WIRE_OFFSETS.carId),
    gear: view.getInt8(WIRE_OFFSETS.gear),
    speed: view.getFloat64(WIRE_OFFSETS.speed, WIRE_LITTLE_ENDIAN),
    rpm: view.getFloat64(WIRE_OFFSETS.rpm, WIRE_LITTLE_ENDIAN),
    throttle: view.getFloat64(WIRE_OFFSETS.throttle, WIRE_LITTLE_ENDIAN),
    brake: view.getFloat64(WIRE_OFFSETS.brake, WIRE_LITTLE_ENDIAN),
    tireTempFL: view.getFloat64(WIRE_OFFSETS.tireTempFL, WIRE_LITTLE_ENDIAN),
    tireTempFR: view.getFloat64(WIRE_OFFSETS.tireTempFR, WIRE_LITTLE_ENDIAN),
    tireTempRL: view.getFloat64(WIRE_OFFSETS.tireTempRL, WIRE_LITTLE_ENDIAN),
    tireTempRR: view.getFloat64(WIRE_OFFSETS.tireTempRR, WIRE_LITTLE_ENDIAN),
  };
}

/** Target sustained ingestion rate used by the generator and benchmarks. */
export const TARGET_PACKETS_PER_SECOND = 10_000;

/** End-to-end processing latency budget, in milliseconds. */
export const MAX_PROCESSING_LATENCY_MS = 20;
