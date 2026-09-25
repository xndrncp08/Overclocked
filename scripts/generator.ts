/**
 * scripts/generator.ts
 *
 * Synthetic UDP telemetry generator. Emits binary-encoded TelemetryPacket
 * datagrams (see src/types/telemetry.ts for the wire format) at a
 * configurable sustained rate, defaulting to 10,000 pkts/sec, matching the
 * pipeline's target load.
 *
 * Pacing strategy: rather than one `setInterval` tick per packet (which is
 * both impossible below ~1ms reliably and wasteful), we tick on a short
 * fixed interval (default 5ms) and, each tick, send however many packets
 * are needed to catch the *actual elapsed wall-clock time* up to the
 * target rate. Driving off elapsed time rather than a per-tick fixed count
 * prevents the steady drift that timer jitter would otherwise cause over
 * a long run.
 *
 * Usage:
 *   npm run generate -- --host 127.0.0.1 --port 41234 --rate 10000 --duration 30 --cars 4
 */

import dgram from "node:dgram";
import { encodeBinaryPacket, type TelemetryPacket } from "../src/types/telemetry.js";

interface GeneratorConfig {
  readonly host: string;
  readonly port: number;
  readonly ratePerSecond: number;
  readonly durationSeconds: number | null; // null = run until interrupted
  readonly carCount: number;
  readonly tickIntervalMs: number;
}

const DEFAULT_CONFIG: GeneratorConfig = {
  host: "127.0.0.1",
  port: 41234,
  ratePerSecond: 10_000,
  durationSeconds: null,
  carCount: 4,
  tickIntervalMs: 5,
};

function parseArgs(argv: readonly string[]): GeneratorConfig {
  const config = { ...DEFAULT_CONFIG };
  const overrides: Record<string, string> = {};

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (!arg?.startsWith("--")) continue;
    const key = arg.slice(2);
    const value = argv[i + 1];
    if (value === undefined || value.startsWith("--")) continue;
    overrides[key] = value;
    i++;
  }

  return {
    host: overrides.host ?? config.host,
    port: overrides.port ? Number.parseInt(overrides.port, 10) : config.port,
    ratePerSecond: overrides.rate ? Number.parseInt(overrides.rate, 10) : config.ratePerSecond,
    durationSeconds: overrides.duration ? Number.parseFloat(overrides.duration) : config.durationSeconds,
    carCount: overrides.cars ? Number.parseInt(overrides.cars, 10) : config.carCount,
    tickIntervalMs: overrides.tick ? Number.parseInt(overrides.tick, 10) : config.tickIntervalMs,
  };
}

/**
 * Deterministic-ish pseudo-random per-car telemetry generator. Each car
 * walks its state forward slightly on every call, giving smooth, plausible
 * sensor traces instead of pure white noise.
 */
class CarSimulator {
  private speed = 60 + Math.random() * 40;
  private rpm = 4000 + Math.random() * 2000;
  private throttle = 0.5;
  private brake = 0;
  private gear = 3;
  private tireTemp: [number, number, number, number] = [80, 80, 78, 78];

  public constructor(public readonly carId: number) {}

  private clamp(value: number, min: number, max: number): number {
    return Math.min(max, Math.max(min, value));
  }

  private step(): void {
    const throttleDelta = (Math.random() - 0.48) * 0.08;
    this.throttle = this.clamp(this.throttle + throttleDelta, 0, 1);

    // Occasional braking event.
    this.brake = Math.random() < 0.03 ? this.clamp(Math.random(), 0, 1) : Math.max(0, this.brake - 0.2);

    const accel = this.throttle * 6 - this.brake * 12 - 0.3;
    this.speed = this.clamp(this.speed + accel, 0, 340);

    this.gear = this.speed < 40 ? 2 : this.speed < 90 ? 3 : this.speed < 140 ? 4 : this.speed < 200 ? 5 : this.speed < 260 ? 6 : this.speed < 300 ? 7 : 8;

    const targetRpm = 3000 + this.throttle * 9000 - this.brake * 2000;
    this.rpm = this.clamp(this.rpm + (targetRpm - this.rpm) * 0.3, 800, 13500);

    this.tireTemp = this.tireTemp.map((t) => {
      const load = 0.02 * (this.speed / 100) + 0.05 * this.brake;
      return this.clamp(t + load + (Math.random() - 0.5) * 0.4, 40, 140);
    }) as [number, number, number, number];
  }

  public next(sequenceId: number): TelemetryPacket {
    this.step();
    return {
      timestamp: Date.now() + performance.now() % 1,
      sequenceId,
      carId: this.carId,
      gear: this.gear,
      speed: this.speed,
      rpm: this.rpm,
      throttle: this.throttle,
      brake: this.brake,
      tireTempFL: this.tireTemp[0],
      tireTempFR: this.tireTemp[1],
      tireTempRL: this.tireTemp[2],
      tireTempRR: this.tireTemp[3],
    };
  }
}

function runGenerator(config: GeneratorConfig): void {
  const socket = dgram.createSocket("udp4");
  const cars = Array.from({ length: config.carCount }, (_, i) => new CarSimulator(i + 1));

  let sequenceId = 0;
  let sentCount = 0;
  let sendErrors = 0;
  const startedAt = process.hrtime.bigint();

  const elapsedSeconds = (): number => Number(process.hrtime.bigint() - startedAt) / 1e9;

  const tick = setInterval(() => {
    const targetSent = Math.floor(elapsedSeconds() * config.ratePerSecond);
    const toSend = targetSent - sentCount;

    for (let i = 0; i < toSend; i++) {
      const car = cars[sentCount % cars.length] as CarSimulator;
      const packet = car.next(sequenceId++);
      const buf = encodeBinaryPacket(packet);
      socket.send(buf, config.port, config.host, (err) => {
        if (err) sendErrors++;
      });
      sentCount++;
    }

    if (config.durationSeconds !== null && elapsedSeconds() >= config.durationSeconds) {
      void stop();
    }
  }, config.tickIntervalMs);

  const statsTimer = setInterval(() => {
    const actualRate = sentCount / elapsedSeconds();
    console.log(
      `[generator] sent=${sentCount} errors=${sendErrors} elapsed=${elapsedSeconds().toFixed(1)}s ` +
        `rate=${actualRate.toFixed(0)}/s (target ${config.ratePerSecond}/s)`,
    );
  }, 1000);

  let stopped = false;
  async function stop(): Promise<void> {
    if (stopped) return;
    stopped = true;
    clearInterval(tick);
    clearInterval(statsTimer);
    await new Promise<void>((resolve) => socket.close(() => resolve()));
    console.log(
      `[generator] stopped. total sent=${sentCount}, errors=${sendErrors}, ` +
        `duration=${elapsedSeconds().toFixed(2)}s`,
    );
    process.exit(0);
  }

  process.on("SIGINT", () => void stop());
  process.on("SIGTERM", () => void stop());

  console.log(
    `[generator] broadcasting to ${config.host}:${config.port} at ~${config.ratePerSecond} pkts/sec ` +
      `across ${config.carCount} car(s)${config.durationSeconds ? ` for ${config.durationSeconds}s` : " (until interrupted)"}`,
  );
}

function isMainModule(): boolean {
  if (!process.argv[1]) return false;
  return import.meta.url === `file://${process.argv[1]}`;
}

if (isMainModule()) {
  const config = parseArgs(process.argv.slice(2));
  runGenerator(config);
}

export { CarSimulator, parseArgs, runGenerator, type GeneratorConfig };
