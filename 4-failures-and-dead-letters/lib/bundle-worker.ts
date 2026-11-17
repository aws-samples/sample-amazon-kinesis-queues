import * as path from 'node:path';
import { buildSync } from 'esbuild';

export const WORKER_DIR = path.resolve(__dirname, '..', 'worker');

/**
 * Bundles the worker entry points and their dependencies, including the shared
 * queue tools, into worker/dist/. The Docker build context is then just the
 * Dockerfile and those files.
 */
export function bundleWorker(): void {
  buildSync({
    // main.js: the fulfillment worker service. dead-letters.js: the shared dead-letter
    // tool (npm run queue:dead-letters), run as a one-off task under the operator role.
    entryPoints: {
      main: path.join(WORKER_DIR, 'src', 'main.ts'),
      'dead-letters': path.resolve(WORKER_DIR, '..', '..', 'shared', 'src', 'dead-letters.ts'),
    },
    outdir: path.join(WORKER_DIR, 'dist'),
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
