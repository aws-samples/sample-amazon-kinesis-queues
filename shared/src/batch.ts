// Helpers for the batch calls that take receipt handles: AcknowledgeQueueRecords,
// UpdateQueueRecordsVisibility and the dead-letter operations.
//
// Each entry carries only the record's ReceiptHandle (plus a visibility timeout for
// the visibility calls). Each call returns HTTP 200 even when some entries fail, so
// the caller has to match every entry in Failed back to the record it came from.
// Failed entries carry the ReceiptHandle they were sent with.

/**
 * Pairs each failed entry with the item that has the same ReceiptHandle.
 */
export function matchFailed<T extends { ReceiptHandle?: string }, F extends { ReceiptHandle?: string }>(
  items: T[],
  failed: F[] | undefined,
): { item: T | undefined; failure: F }[] {
  return (failed ?? []).map((failure) => ({
    failure,
    item: items.find((x) => x.ReceiptHandle === failure.ReceiptHandle),
  }));
}
