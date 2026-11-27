import { createHash, randomBytes } from 'node:crypto';

/**
 * Recursively sorts object keys for deterministic canonical JSON serialization.
 */
export function sortCanonical(value: unknown): unknown {
  if (value === null || typeof value !== 'object') {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item) => sortCanonical(item));
  }
  const obj = value as Record<string, unknown>;
  const sorted: Record<string, unknown> = {};
  for (const key of Object.keys(obj).sort()) {
    const v = obj[key];
    if (v !== undefined) {
      sorted[key] = sortCanonical(v);
    }
  }
  return sorted;
}

export function canonicalJson(value: unknown): string {
  return JSON.stringify(sortCanonical(value));
}

export function sha256Hex(input: string | Uint8Array): string {
  return createHash('sha256').update(input).digest('hex');
}

export function canonicalSha256(value: unknown): string {
  return sha256Hex(canonicalJson(value));
}

export function newId(prefix: string, byteLen = 12): string {
  return `${prefix}_${randomBytes(byteLen).toString('hex')}`;
}

export function newHighEntropyToken(byteLen = 32): string {
  return randomBytes(byteLen).toString('hex');
}

/**
 * Deterministic fingerprint ID for idempotent ingestion and de-duplication.
 */
export function deterministicEventId(
  streamId: string,
  sourceFingerprint: string,
): string {
  return `evt_${sha256Hex(`${streamId}\n${sourceFingerprint}`).slice(0, 24)}`;
}
