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

export class InvalidIdentifierError extends Error {
  constructor(fieldName: string, value: string) {
    super(
      `Invalid ${fieldName} (${JSON.stringify(value)}): must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ and must not contain '..'`,
    );
    this.name = 'InvalidIdentifierError';
  }
}

const SAFE_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

/**
 * Enforces strict safe identifier syntax across filesystem-mapped and UI-rendered IDs
 * (`project_id`, `thread_id`, `stream_id`), preventing path traversal and DOM injection.
 */
export function assertSafeIdentifier(fieldName: string, value: string): string {
  if (typeof value !== 'string' || !SAFE_ID_RE.test(value) || value.includes('..')) {
    throw new InvalidIdentifierError(fieldName, String(value));
  }
  return value;
}
