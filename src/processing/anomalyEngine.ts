/**
 * src/processing/anomalyEngine.ts
 *
 * Rule-based anomaly detection engine. Evaluates each incoming packet
 * against a set of configurable thresholds and rate-of-change limits,
 * emitting a typed AnomalyEvent for every rule that fires.
 *
 * Designed to run synchronously inside the RxJS pipeline so detection
 * latency is bounded by the pipeline's own tick rate, not by a separate
 * async loop.
 */

import type { TelemetryPacket } from "../types/telemetry.js";
import type { WindowStats } from "./windowStats.js";

export type AnomalySeverity = "warning" | "critical";

export type AnomalyKind =
  | "threshold_breach"   // value exceeded an absolute limit
  | "rapid_drop"         // value fell faster than the allowed rate
  | "rapid_spike"        // value rose faster than the allowed rate
  | "sensor_noise"       // std deviation exceeds noise tolerance
  | "stall_detected"     // RPM dropped to near-zero while speed is nonzero
  | "over_rev";          // RPM exceeded the engine's rev limit

export interface AnomalyEvent {
  readonly timestamp: number;
  readonly sequenceId: number;
  readonly carId: number;
  readonly kind: AnomalyKind;
  readonly severity: AnomalySeverity;
  readonly field: string;
  readonly value: number;
  readonly threshold: number;
  readonly message: string;
}

export interface AnomalyThresholds {
  /** Maximum speed in km/h before a critical breach is raised. Default 375. */
  readonly maxSpeed?: number;
  /** RPM above which an over-rev warning fires. Default 13000. */
  readonly maxRpm?: number;
  /** Tire temperature above which a critical breach fires. Default 135. */
  readonly maxTireTemp?: number;
  /** Tire temperature below which a warning fires (cold tires = low grip). Default 60. */
  readonly minTireTemp?: number;
  /** Max speed drop (km/h) between consecutive packets before a rapid_drop fires. Default 80. */
  readonly maxSpeedDropPerPacket?: number;
  /** Max RPM drop between consecutive packets before a rapid_drop fires. Default 4000. */
  readonly maxRpmDropPerPacket?: number;
  /** Std deviation of tire temp within the window above which sensor_noise fires. Default 8. */
  readonly tireTempNoiseThreshold?: number;
  /** RPM below which stall_detected fires when speed > 20 km/h. Default 500. */
  readonly stallRpmThreshold?: number;
}

const DEFAULTS: Required<AnomalyThresholds> = {
  maxSpeed: 375,
  maxRpm: 13000,
  maxTireTemp: 135,
  minTireTemp: 60,
  maxSpeedDropPerPacket: 80,
  maxRpmDropPerPacket: 4000,
  tireTempNoiseThreshold: 8,
  stallRpmThreshold: 500,
};

export class AnomalyEngine {
  private readonly thresholds: Required<AnomalyThresholds>;
  private prevPacket: TelemetryPacket | null = null;
  private totalAnomalies = 0;

  public constructor(thresholds: AnomalyThresholds = {}) {
    this.thresholds = { ...DEFAULTS, ...thresholds };
  }

  public get anomalyCount(): number {
    return this.totalAnomalies;
  }

  /**
   * Evaluate a single packet and its window statistics.
   * Returns an array of all anomalies detected (empty = clean packet).
   */
  public evaluate(
    packet: TelemetryPacket,
    speedStats: WindowStats,
    rpmStats: WindowStats,
    tireTempFLStats: WindowStats,
  ): AnomalyEvent[] {
    const events: AnomalyEvent[] = [];
    const t = this.thresholds;

    const emit = (
      kind: AnomalyKind,
      severity: AnomalySeverity,
      field: string,
      value: number,
      threshold: number,
      message: string,
    ): void => {
      events.push({
        timestamp: packet.timestamp,
        sequenceId: packet.sequenceId,
        carId: packet.carId,
        kind,
        severity,
        field,
        value,
        threshold,
        message,
      });
      this.totalAnomalies++;
    };

    // --- Absolute threshold breaches ---

    if (packet.speed > t.maxSpeed) {
      emit("threshold_breach", "critical", "speed", packet.speed, t.maxSpeed,
        `Car ${packet.carId} speed ${packet.speed.toFixed(1)} km/h exceeds limit ${t.maxSpeed} km/h`);
    }

    if (packet.rpm > t.maxRpm) {
      emit("over_rev", "critical", "rpm", packet.rpm, t.maxRpm,
        `Car ${packet.carId} over-rev: ${packet.rpm.toFixed(0)} RPM (limit ${t.maxRpm})`);
    }

    const tireFLTemp = packet.tireTempFL;
    const tireFRTemp = packet.tireTempFR;
    const tireRLTemp = packet.tireTempRL;
    const tireRRTemp = packet.tireTempRR;

    for (const [field, temp] of [
      ["tireTempFL", tireFLTemp],
      ["tireTempFR", tireFRTemp],
      ["tireTempRL", tireRLTemp],
      ["tireTempRR", tireRRTemp],
    ] as [string, number][]) {
      if (temp > t.maxTireTemp) {
        emit("threshold_breach", "critical", field, temp, t.maxTireTemp,
          `Car ${packet.carId} ${field} overheating: ${temp.toFixed(1)}°C`);
      } else if (temp < t.minTireTemp) {
        emit("threshold_breach", "warning", field, temp, t.minTireTemp,
          `Car ${packet.carId} ${field} too cold: ${temp.toFixed(1)}°C (grip risk)`);
      }
    }

    // --- Rate-of-change checks (require a previous packet) ---

    if (this.prevPacket !== null) {
      const prev = this.prevPacket;

      const speedDrop = prev.speed - packet.speed;
      if (speedDrop > t.maxSpeedDropPerPacket) {
        emit("rapid_drop", "warning", "speed", speedDrop, t.maxSpeedDropPerPacket,
          `Car ${packet.carId} rapid speed drop: -${speedDrop.toFixed(1)} km/h in one packet`);
      }

      const rpmDrop = prev.rpm - packet.rpm;
      if (rpmDrop > t.maxRpmDropPerPacket) {
        emit("rapid_drop", "warning", "rpm", rpmDrop, t.maxRpmDropPerPacket,
          `Car ${packet.carId} rapid RPM drop: -${rpmDrop.toFixed(0)} RPM in one packet`);
      }
    }

    // --- Window-based checks ---

    // Sensor noise: high std deviation in tire temps within the window.
    if (tireTempFLStats.count >= 10 && tireTempFLStats.stdDev > t.tireTempNoiseThreshold) {
      emit("sensor_noise", "warning", "tireTempFL", tireTempFLStats.stdDev, t.tireTempNoiseThreshold,
        `Car ${packet.carId} FL tire temp noise: σ=${tireTempFLStats.stdDev.toFixed(2)}°C`);
    }

    // Stall detection: near-zero RPM while the car is still moving.
    if (packet.rpm < t.stallRpmThreshold && packet.speed > 20) {
      emit("stall_detected", "critical", "rpm", packet.rpm, t.stallRpmThreshold,
        `Car ${packet.carId} possible stall: ${packet.rpm.toFixed(0)} RPM at ${packet.speed.toFixed(1)} km/h`);
    }

    this.prevPacket = packet;
    return events;
  }

  public reset(): void {
    this.prevPacket = null;
    this.totalAnomalies = 0;
  }
}