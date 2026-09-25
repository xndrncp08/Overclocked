# Overclocked

Overclocked

A production-grade, real-time telemetry ingestion and stream processing pipeline designed around the data characteristics of F1 motorsport sensor streams — high frequency, low latency, zero tolerance for packet loss.

What it does

Ingests binary-encoded telemetry (speed, RPM, throttle, brake pressure, gear, all four tire temperatures) broadcast over UDP at 10,000 packets per second, processes it through a reactive stream pipeline with sliding-window analytics and anomaly detection, persists it to an embedded analytical database, and displays everything live in an interactive terminal dashboard — all with sub-20ms end-to-end latency.

How it works

The core architectural challenge is that 10,000 packets/sec leaves 0.1ms per packet, and JavaScript's single-threaded event loop can't reliably receive, process, and store data at that rate without falling behind. Overclocked solves this by separating concerns across threads and using shared memory to hand off data between them at zero cost:

UDP ingestion runs on a dedicated worker thread so that processing slowdowns downstream can never cause the socket listener to miss incoming packets
A lock-free SPSC ring buffer backed by SharedArrayBuffer sits between the worker thread (producer) and the main thread (consumer) — packets are written and read using Atomics for cross-thread memory visibility, with no copying and no JS-level locks
A compact 80-byte binary wire format replaces JSON for UDP transmission, cutting per-packet payload size by ~3× and eliminating serialization overhead at throughput
RxJS sliding window operators compute 5-second rolling averages, exponential smoothing, and peak detection over the live stream
An anomaly detection engine flags threshold breaches, rapid sensor drops, and noise signatures in sub-10ms intervals
DuckDB batch flushing buffers records in memory and writes to the embedded analytical database every 500ms or 5,000 records, whichever comes first, without blocking ingestion
An Ink TUI dashboard renders live metrics — intake rate, processing latency, buffer saturation %, recent anomaly alerts, and a rolling ASCII graph of speed and RPM — directly in the terminal
Stack
Concern	Technology
Language & runtime	TypeScript (strict), Node.js 20+ ESM
UDP ingestion	node:dgram + node:worker_threads
Zero-copy buffer	SharedArrayBuffer + Atomics (Float64Array / Int32Array)
Stream processing	RxJS sliding window pipelines
Persistence	DuckDB (embedded, batch-buffered inserts)
Terminal UI	Ink (React in the CLI)
Testing	Vitest — unit, integration, throughput benchmarks, concurrency
Project structure
src/
  types/        # Telemetry interfaces, binary wire format, encode/decode
  ingestion/    # Ring buffer, UDP worker, receiver orchestrator
  processing/   # Stream pipelines, anomaly engine          (Phase 2)
  storage/      # DuckDB schema, batch flush, query layer   (Phase 3)
  tui/          # Ink dashboard components                  (Phase 4)
scripts/
  generator.ts  # Synthetic 10k/s UDP telemetry emitter
tests/
  phase1/       # Ring buffer correctness, UDP ingestion throughput
  phase2/       # Stream accuracy, anomaly detection
  phase3/       # Batch insert throughput, query execution
  phase4/       # End-to-end pipeline integration
