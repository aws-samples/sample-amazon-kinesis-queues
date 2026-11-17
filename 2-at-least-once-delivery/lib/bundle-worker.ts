import * as path from 'node:path';
import { buildSync } from 'esbuild';

export const WORKER_DIR = path.resolve(__dirname, '..', 'worker');

/**
 * Bundles the worker and its dependencies, including the shared order parsing, into
 * worker/dist/. The Docker build context is then just the Dockerfile and that file.
 */
export function bundleWorker(): void {
  buildSync({
    entryPoints: [path.join(WORKER_DIR, 'src', 'main.ts')],
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
