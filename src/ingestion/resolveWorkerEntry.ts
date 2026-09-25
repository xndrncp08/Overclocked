/**
 * src/ingestion/resolveWorkerEntry.ts
 *
 * Resolves the file the UDP ingestion worker_thread should load as its
 * entry point.
 *
 * Worker threads load their entry file through Node's plain module
 * resolver, not through whatever loader hooks (e.g. tsx's `.js` -> `.ts`
 * remapping) made *this* file loadable in the first place — and those
 * hooks don't reliably propagate into a freshly spawned worker either, so
 * simply pointing a `Worker` at `udpWorker.ts` and hoping the right loader
 * comes along for the ride is not dependable across Node/tsx versions.
 *
 * Instead: when running from TypeScript source (dev, via `tsx`), this
 * bundles `udpWorker.ts` and its local (relative) imports into a single,
 * plain-JavaScript file with esbuild — once per process, then cached — and
 * hands the worker *that*. A worker loading plain `.mjs` needs no
 * TypeScript support of any kind, so this sidesteps the whole
 * loader-propagation problem rather than working around it.
 *
 * When running the compiled build (`npm run build && node dist/...`),
 * `udpWorker.js` already exists right next to this file (emitted by
 * `tsc`), and this returns that directly — no bundling step at all.
 */

import { mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

let cachedDevBundleUrl: URL | null = null;

/** True when this module itself was loaded from a `.ts` file (dev, via `tsx`) rather than a compiled `.js` file. */
function isRunningFromTypeScriptSource(): boolean {
  return import.meta.url.endsWith(".ts");
}

async function bundleWorkerForDev(): Promise<URL> {
  if (cachedDevBundleUrl) {
    return cachedDevBundleUrl;
  }

  // Imported dynamically, and only in the dev path: the compiled build
  // never needs esbuild at runtime, only as a devDependency.
  const { build } = await import("esbuild");

  const entryPath = fileURLToPath(new URL("./udpWorker.ts", import.meta.url));
  const outDir = path.join(tmpdir(), "overclocked-dev-build");
  await mkdir(outDir, { recursive: true });
  // Namespaced by pid so concurrent dev processes (e.g. the test suite
  // running alongside `npm run receive`) never race on the same file.
  const outFile = path.join(outDir, `udpWorker.${process.pid}.mjs`);

  await build({
    entryPoints: [entryPath],
    outfile: outFile,
    bundle: true,
    platform: "node",
    format: "esm",
    target: "node20",
    // Keep npm dependencies external (there are none in udpWorker.ts's
    // graph today, but this keeps the bundle minimal if that changes);
    // only our own relative `../types/telemetry.js` / `./ringBuffer.js`
    // style imports get resolved and inlined.
    packages: "external",
    logLevel: "silent",
  });

  cachedDevBundleUrl = pathToFileURL(outFile);
  return cachedDevBundleUrl;
}

/** Resolve the `file://` URL the UDP worker's `Worker(...)` constructor should load. */
export async function resolveWorkerEntryUrl(): Promise<URL> {
  if (!isRunningFromTypeScriptSource()) {
    return new URL("./udpWorker.js", import.meta.url);
  }
  return bundleWorkerForDev();
}
