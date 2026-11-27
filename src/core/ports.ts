import type {
  BlobManifestEntry,
  JournalEventRecord,
  LineageAnchor,
  StreamShardLine,
  TransactionFinalizationCertificate,
  TransactionManifest,
} from './types.js';

export interface ClockPort {
  clockDomainId(): string;
  bootEpoch(): string;
  monotonicNs(): bigint;
  wallIso(): string;
  /**
   * Returns true if suspend/resume clock continuity cannot be guaranteed
   * and an active lease must conservatively degrade to `unknown`.
   */
  isClockContinuityUncertain?(recordedClockDomainId: string, recordedBootEpoch: string): boolean;
}

export interface RedactionResult {
  sanitized: string;
  redactedCount: number;
  matchedRuleNames: string[];
}

export interface RedactionHook {
  sanitizeText(text: string): RedactionResult;
  scanForSecrets(text: string): string[];
}

export interface StoragePort {
  getStreamHead(streamId: string): string | null;
  getJournalHead(threadId: string): string | null;
  getAnchor(anchorId: string): LineageAnchor | null;
  readShardLines(relativePath: string): StreamShardLine[];
  readBlob(sha256: string): { bytes: Uint8Array; manifest?: BlobManifestEntry } | null;
}

export interface CoordinationPort {
  listJournalEvents(threadId: string): JournalEventRecord[];
}

export interface SyncPackageDescriptor {
  manifests: TransactionManifest[];
  certificates: TransactionFinalizationCertificate[];
  approvedBlobHashes: string[];
  excludedLocalOnlyHashes: string[];
}

export interface SyncTransportPort {
  collectSyncableWhitelist(threadId: string): SyncPackageDescriptor;
}
