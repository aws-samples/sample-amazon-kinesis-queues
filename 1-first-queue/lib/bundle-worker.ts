import * as path from 'node:path';
import { buildSync } from 'esbuild';

export const WORKER_DIR = path.resolve(__dirname, '..', 'worker');

/** Bundles the consumer script and its dependencies into worker/dist/main.js for the Docker image. */
export function bundleWorker(): void {
  buildSync({
    entryPoints: [path.resolve(__dirname, '..', 'src', 'consume.ts')],
    outfile: path.join(WORKER_DIR, 'dist', 'main.js'),
    bundle: true,
    platform: 'node',
    target: 'node22',
    format: 'cjs',
    sourcemap: true,
    sourcesContent: false,
    legalComments: 'none',
    logLevel: 'warning',
  });
}
