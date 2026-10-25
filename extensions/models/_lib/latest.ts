// A model name can own data under more than one definition ID: a definition
// that was deleted or migrated (e.g. a persisted model replaced by a workflow's
// direct type execution) leaves its records behind under the same modelName.
// queryData/readModelData return the latest version per data item per
// definition, in no useful order, so `records[0]` can be the stale one and
// `version` is not comparable across definitions. "Latest" means most recently
// written.
type Stamped = { createdAt?: unknown; version?: number };

// deno-lint-ignore no-explicit-any
export function newest<T = any>(records: T[]): T | undefined {
  let best: T | undefined;
  let bestAt = -Infinity;
  for (const r of records) {
    const { createdAt, version } = r as Stamped;
    const at = Date.parse(String(createdAt));
    const t = Number.isNaN(at) ? -Infinity : at;
    if (
      best === undefined || t > bestAt ||
      (t === bestAt && (version ?? 0) > ((best as Stamped).version ?? 0))
    ) {
      best = r;
      bestAt = t;
    }
  }
  return best;
}
