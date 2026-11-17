import { randomUUID } from 'node:crypto';

/** Short ID for this task process, so log lines from several tasks can be told apart. */
export const TASK_ID = randomUUID().slice(0, 8);

/** One JSON object per line, easy to query in CloudWatch Logs Insights. */
export function log(level: 'INFO' | 'WARN' | 'ERROR', event: string, fields: Record<string, unknown> = {}): void {
  const line = JSON.stringify({ time: new Date().toISOString(), level, task: TASK_ID, event, ...fields });
  if (level === 'INFO') console.log(line);
  else console.error(line);
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? `${error.name}: ${error.message}` : String(error);
}
