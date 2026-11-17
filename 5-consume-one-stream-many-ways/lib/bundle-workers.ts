import * as path from 'node:path';
import { buildSync } from 'esbuild';

export const WORKERS_DIR = path.resolve(__dirname, '..', 'workers');
export const FRAUD_SIGNALS_DIR = path.resolve(__dirname, '..', 'fraud-signals');

/**
 * Bundles the queue worker and its dependencies, including the shared order
 * parsing, into workers/dist/. The Docker build context is then just the Dockerfile
 * and that file.
 */
export function bundleWorkers(): void {
  buildSync({
    entryPoints: [path.join(WORKERS_DIR, 'src', 'main.ts')],
    outdir: path.join(WORKERS_DIR, 'dist'),
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
