import * as path from 'node:path';
import { buildSync } from 'esbuild';

export const WORKER_DIR = path.resolve(__dirname, '..', 'worker');
export const KCL_CONSUMER_DIR = path.resolve(__dirname, '..', 'kcl-consumer');
/** The sideline Lambda function's code, kept apart from the worker image's files. */
export const SIDELINE_DIR = path.join(WORKER_DIR, 'dist-sideline');

/**
 * Bundles the queue worker and the relay's SQS consumer into worker/dist/ for their
 * Docker image, and the sideline
 * function into worker/dist-sideline/ for Lambda. Both share the fulfillment code.
 */
export function bundleWorker(): void {
  const common = {
    bundle: true,
    platform: 'node',
    target: 'node22',
    format: 'cjs',
    sourcemap: true,
    sourcesContent: false,
    legalComments: 'none',
    logLevel: 'warning',
  } as const;
  // main.js: the queue consumer. sqs-main.js: the relay's SQS consumer, in the same image.
  buildSync({
    ...common,
    entryPoints: [path.join(WORKER_DIR, 'src', 'main.ts'), path.join(WORKER_DIR, 'src', 'sqs-main.ts')],
    outdir: path.join(WORKER_DIR, 'dist'),
  });
  buildSync({ ...common, entryPoints: [path.join(WORKER_DIR, 'src', 'sideline.ts')], outdir: SIDELINE_DIR });
}
