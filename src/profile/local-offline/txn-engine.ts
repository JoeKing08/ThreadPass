import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  assertSafeIdentifier,
  canonicalJson,
  canonicalSha256,
  newId,
  sha256Hex,
} from '../../core/canonical.js';
import type {
  ClockPort,
  CoordinationPort,
  StoragePort,
  SyncPackageDescriptor,
  SyncTransportPort,
} from '../../core/ports.js';
import {
  projectJournalState,
  projectMailboxState,
} from '../../core/state-machine.js';
import {
  SCHEMA_VERSION,
  type BlobManifestEntry,
  type CommitIntentProof,
  type ConfirmationProof,
  type ControlRecord,
  type EventRecord,
  type FsckTxnStatus,
  type HostTranscriptLocator,
  type InputDependencyManifest,
  type JournalEventRecord,
  type LineageAnchor,
  type NotifyReceiptRecord,
  type PublicationClosureProof,
  type ShardEntry,
  type StreamProjectionHealth,
  type StreamShardLine,
  type TransactionFinalizationCertificate,
  type TransactionManifest,
  type TrustedDeviceKey,
  type WorkspaceManifest,
} from '../../core/types.js';
import {
  ensureExclusiveAcl,
  linkPreparedImmutable,
  probeHardLinkSupport,
  publishImmutable,
} from './acl-and-fs.js';
import {
  loadOrCreateDeviceIdentity,
  loadTrustedDeviceKeys,
  pinTrustedDeviceKey,
  signCommitIntentProof,
  verifyCommitIntentProofSignature,
  type LocalDeviceIdentity,
} from './crypto-keys.js';
import { isOpaqueBinary, redactRemoteUrlCredentials, SlidingWindowRedactor } from './redaction.js';
import { StateStore, SystemClock } from './state-store.js';

export class FencingViolationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FencingViolationError';
  }
}

export class CommitConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CommitConflictError';
  }
}

export class SimulatedCrashError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SimulatedCrashError';
  }
}

export interface StagedBlobInput {
  bytes: Uint8Array | string;
  sync_policy?: 'approved' | 'local_only' | 'unscannable_binary';
  media_type?: string;
}

export interface StagedStreamInput {
  stream_id: string;
  expected_parent_head: string | null;
  anchor: LineageAnchor;
  events: EventRecord[];
  controls?: ControlRecord[];
  invocations?: StreamShardLine[];
}

export interface TransactionSpec {
  thread_id: string;
  is_new_thread?: boolean;
  main_stream_id?: string;
  affected_streams: StagedStreamInput[];
  expected_journal_head?: string | null;
  journal_events?: JournalEventRecord[];
  blobs?: Record<string, StagedBlobInput>;
  input_dependency_manifests?: InputDependencyManifest[];
  provenance_refs?: string[];
  host_bindings?: Array<{ host_session_id: string; stream_id: string }>;
}

export interface PreparedTxnContext {
  txn_id: string;
  thread_id: string;
  owner_token: string;
  fencing_epoch: number;
  manifest: TransactionManifest;
  staged_stream_files: Array<{ stream_id: string; seq: number; staging_path: string; target_rel: string; sha256: string }>;
  staged_journal_file?: { seq: number; staging_path: string; target_rel: string; sha256: string };
  staged_blobs: Array<{ sha256: string; staging_path: string; target_rel: string; manifest: BlobManifestEntry }>;
  staged_manifest_path: string;
  target_manifest_rel: string;
  commit_intent_proof?: CommitIntentProof;
  is_new_thread?: boolean;
  main_stream_id?: string;
  spec: TransactionSpec;
}

export interface TxnFaultHooks {
  afterPrepareBeforeCommitIntent?: (ctx: PreparedTxnContext) => void;
  afterCommitIntentBeforePublish?: (ctx: PreparedTxnContext) => void;
  afterPublishBeforeFinalize?: (ctx: PreparedTxnContext) => void;
  afterFinalizedLinkBeforeSqliteCommit?: (ctx: PreparedTxnContext) => void;
}

export interface FsckTxnReport {
  txn_id: string;
  status: FsckTxnStatus;
  reason?: string;
  missing_local_only_blobs: string[];
  affected_streams: string[];
}

export interface FsckReport {
  thread_id: string;
  ok: boolean;
  rebuilt: boolean;
  txn_reports: FsckTxnReport[];
  stream_health: Record<string, StreamProjectionHealth>;
  divergent_stream_heads: Record<string, string[]>;
  divergent_journal_heads: string[];
  journal_conflicts: ReturnType<typeof projectJournalState>['conflicts'];
}

export function resolveThreadPassHome(explicitHome?: string): string {
  return (
    explicitHome ??
    process.env.THREADPASS_HOME ??
    path.join(process.env.USERPROFILE || process.env.HOME || '.', '.threadpass')
  );
}

export function manifestDigestPayload(
  manifest: Omit<TransactionManifest, 'manifest_hash'>,
): string {
  return canonicalSha256(manifest);
}

export function isPidAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    return code === 'EPERM';
  }
}

function collectSpecConfirmationProofs(spec: TransactionSpec): ConfirmationProof[] {
  const byNonce = new Map<string, ConfirmationProof>();
  const register = (proof?: ConfirmationProof) => {
    if (!proof?.nonce) return;
    const prev = byNonce.get(proof.nonce);
    if (prev) {
      if (
        prev.content_sha256 !== proof.content_sha256 ||
        prev.purpose !== proof.purpose
      ) {
        throw new CommitConflictError(
          `Conflicting reuse of confirmation proof nonce ${proof.nonce} inside transaction`,
        );
      }
      return;
    }
    byNonce.set(proof.nonce, proof);
  };
  for (const sInput of spec.affected_streams) {
    for (const ev of sInput.events) {
      register(ev.provenance.confirmation_proof);
    }
    for (const ctrl of sInput.controls ?? []) {
      if (ctrl.payload.type === 'reconciliation_record') {
        register(ctrl.payload.record.confirmation_proof);
      }
    }
  }
  return [...byNonce.values()];
}

export class ThreadDirLayout {
  public readonly rootDir: string;

  constructor(
    public readonly homeDir: string,
    public readonly projectId: string,
    public readonly threadId: string,
  ) {
    assertSafeIdentifier('project_id', projectId);
    assertSafeIdentifier('thread_id', threadId);
    this.rootDir = path.join(homeDir, 'projects', projectId, 'threads', threadId);
  }

  get projectDir(): string {
    return path.join(this.homeDir, 'projects', this.projectId);
  }
  get dbPath(): string {
    return path.join(this.rootDir, 'state.sqlite');
  }
  get lineageJsonPath(): string {
    return path.join(this.rootDir, 'lineage.json');
  }
  get transcriptIndexJsonPath(): string {
    return path.join(this.rootDir, 'transcript-index.json');
  }
  get historyStagingDir(): string {
    return path.join(this.rootDir, 'history', 'staging');
  }
  get historyCommittedDir(): string {
    return path.join(this.rootDir, 'history', 'committed');
  }
  get journalStagingDir(): string {
    return path.join(this.rootDir, 'coordination', 'journal', 'staging');
  }
  get journalCommittedDir(): string {
    return path.join(this.rootDir, 'coordination', 'journal', 'committed');
  }
  get blobsStagingDir(): string {
    return path.join(this.rootDir, 'blobs', 'staging');
  }
  get blobsObjectsDir(): string {
    return path.join(this.rootDir, 'blobs', 'objects');
  }
  get txnsStagingDir(): string {
    return path.join(this.rootDir, 'transactions', 'staging');
  }
  get txnsCommittedDir(): string {
    return path.join(this.rootDir, 'transactions', 'committed');
  }
  get bundlesDir(): string {
    return path.join(this.rootDir, 'bundles');
  }
  get reconcilePacksDir(): string {
    return path.join(this.rootDir, 'reconcile-packs');
  }

  ensureDirectories(): void {
    ensureExclusiveAcl(this.homeDir, 'dir');
    ensureExclusiveAcl(this.projectDir, 'dir');
    ensureExclusiveAcl(this.rootDir, 'dir');
    for (const dir of [
      this.historyStagingDir,
      this.historyCommittedDir,
      this.journalStagingDir,
      this.journalCommittedDir,
      this.blobsStagingDir,
      this.blobsObjectsDir,
      this.txnsStagingDir,
      this.txnsCommittedDir,
      this.bundlesDir,
    ]) {
      fs.mkdirSync(dir, { recursive: true });
    }
    probeHardLinkSupport(this.txnsCommittedDir);
  }
}

export class LocalOfflineEngine {
  public readonly homeDir: string;
  public readonly clock: ClockPort;
  public readonly identity: LocalDeviceIdentity;
  public readonly redactor: SlidingWindowRedactor;
  private readonly verifiedAclDirs = new Set<string>();
  private readonly probedHardLinkDirs = new Set<string>();

  constructor(options: {
    homeDir?: string;
    clock?: ClockPort;
    customSecrets?: readonly string[];
  } = {}) {
    this.homeDir = resolveThreadPassHome(options.homeDir);
    this.clock = options.clock ?? new SystemClock();
    this.ensureDirAclCached(this.homeDir);
    this.identity = loadOrCreateDeviceIdentity(this.homeDir);
    this.redactor = new SlidingWindowRedactor(options.customSecrets);
  }

  private ensureDirAclCached(dirPath: string): void {
    if (this.verifiedAclDirs.has(dirPath) && fs.existsSync(dirPath)) {
      return;
    }
    ensureExclusiveAcl(dirPath, 'dir');
    this.verifiedAclDirs.add(dirPath);
  }

  ensureLayoutReady(layout: ThreadDirLayout): void {
    this.ensureDirAclCached(layout.homeDir);
    this.ensureDirAclCached(layout.projectDir);
    this.ensureDirAclCached(layout.rootDir);
    for (const dir of [
      layout.historyStagingDir,
      layout.historyCommittedDir,
      layout.journalStagingDir,
      layout.journalCommittedDir,
      layout.blobsStagingDir,
      layout.blobsObjectsDir,
      layout.txnsStagingDir,
      layout.txnsCommittedDir,
      layout.bundlesDir,
    ]) {
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }
    }
    if (!this.probedHardLinkDirs.has(layout.txnsCommittedDir)) {
      probeHardLinkSupport(layout.txnsCommittedDir);
      this.probedHardLinkDirs.add(layout.txnsCommittedDir);
    }
  }

  getLayout(projectId: string, threadId: string): ThreadDirLayout {
    return new ThreadDirLayout(this.homeDir, projectId, threadId);
  }

  openStore(projectId: string, threadId: string): StateStore {
    const layout = this.getLayout(projectId, threadId);
    this.ensureLayoutReady(layout);
    const projectJson = path.join(layout.projectDir, 'project.json');
    if (!fs.existsSync(projectJson)) {
      fs.writeFileSync(
        projectJson,
        JSON.stringify(
          { schema_version: SCHEMA_VERSION, project_id: projectId, created_at: this.clock.wallIso() },
          null,
          2,
        ),
        'utf8',
      );
    }
    // Automatically pin local device key as active for this project if not yet pinned
    pinTrustedDeviceKey(this.homeDir, {
      device_id: this.identity.device_id,
      project_id: projectId,
      public_key_spki_b64: this.identity.public_key_spki_b64,
      status: 'active',
      note: 'local primary device',
    });
    return new StateStore(layout.dbPath);
  }

  /**
   * Opens an already-initialized thread store without creating empty directories
   * or empty SQLite files for non-existent threads.
   */
  openExistingStore(projectId: string, threadId: string): StateStore {
    const layout = this.getLayout(projectId, threadId);
    if (!fs.existsSync(layout.dbPath)) {
      throw new Error(`Thread ${threadId} is not initialized in project ${projectId}`);
    }
    this.ensureLayoutReady(layout);
    pinTrustedDeviceKey(this.homeDir, {
      device_id: this.identity.device_id,
      project_id: projectId,
      public_key_spki_b64: this.identity.public_key_spki_b64,
      status: 'active',
      note: 'local primary device',
    });
    return new StateStore(layout.dbPath);
  }

  /**
   * Returns an adapter implementing Pure Core's StoragePort, CoordinationPort,
   * and SyncTransportPort over this Local-Offline thread directory.
   */
  createCorePorts(
    projectId: string,
    threadId: string,
  ): StoragePort & CoordinationPort & SyncTransportPort {
    const layout = this.getLayout(projectId, threadId);
    const openReadStore = (): StateStore | null =>
      fs.existsSync(layout.dbPath) ? new StateStore(layout.dbPath) : null;

    return {
      getStreamHead: (streamId: string): string | null => {
        const s = openReadStore();
        if (!s) return null;
        try {
          return s.getStream(streamId)?.head_anchor_id ?? null;
        } finally {
          s.close();
        }
      },
      getJournalHead: (tid: string): string | null => {
        const s = openReadStore();
        if (!s) return null;
        try {
          return s.getThread(tid)?.journal_head_id ?? null;
        } finally {
          s.close();
        }
      },
      getAnchor: (anchorId: string): LineageAnchor | null => {
        const s = openReadStore();
        if (!s) return null;
        try {
          return s.getAnchor(anchorId);
        } finally {
          s.close();
        }
      },
      readShardLines: (relativePath: string): StreamShardLine[] => {
        const fullPath = path.resolve(layout.rootDir, ...relativePath.split('/'));
        const relCheck = path.relative(layout.rootDir, fullPath);
        if (relCheck.startsWith('..') || path.isAbsolute(relCheck) || !fs.existsSync(fullPath)) {
          return [];
        }
        return fs
          .readFileSync(fullPath, 'utf8')
          .split(/\r?\n/)
          .filter((l) => l.trim().length > 0)
          .map((l) => JSON.parse(l) as StreamShardLine);
      },
      readBlob: (sha256: string): { bytes: Uint8Array; manifest?: BlobManifestEntry } | null => {
        assertSafeIdentifier('blob_sha256', sha256);
        const blobPath = path.join(layout.blobsObjectsDir, sha256.slice(0, 2), sha256);
        if (!fs.existsSync(blobPath)) return null;
        const bytes = fs.readFileSync(blobPath);
        if (sha256Hex(bytes) !== sha256) return null;
        let manifest: BlobManifestEntry | undefined;
        const s = openReadStore();
        if (s) {
          try {
            const row = s.db
              .prepare('SELECT manifest_json FROM blobs_index WHERE sha256 = ?')
              .get(sha256) as { manifest_json: string } | undefined;
            if (row) manifest = JSON.parse(row.manifest_json) as BlobManifestEntry;
          } finally {
            s.close();
          }
        }
        return { bytes, manifest };
      },
      listJournalEvents: (tid: string): JournalEventRecord[] => {
        const s = openReadStore();
        if (!s) return [];
        try {
          return s.listJournalEvents(tid);
        } finally {
          s.close();
        }
      },
      collectSyncableWhitelist: (tid: string): SyncPackageDescriptor => {
        const manifests: TransactionManifest[] = [];
        const certificates: TransactionFinalizationCertificate[] = [];
        const approvedBlobHashes = new Set<string>();
        const excludedLocalOnlyHashes = new Set<string>();
        if (tid !== threadId || !fs.existsSync(layout.txnsCommittedDir)) {
          return {
            manifests,
            certificates,
            approvedBlobHashes: [],
            excludedLocalOnlyHashes: [],
          };
        }
        for (const f of fs.readdirSync(layout.txnsCommittedDir).sort()) {
          if (!f.endsWith('.manifest.json')) continue;
          const txnId = f.slice(0, -'.manifest.json'.length);
          const certPath = path.join(layout.txnsCommittedDir, `${txnId}.finalized.json`);
          if (!fs.existsSync(certPath)) continue;
          const manifest = JSON.parse(
            fs.readFileSync(path.join(layout.txnsCommittedDir, f), 'utf8'),
          ) as TransactionManifest;
          const cert = JSON.parse(
            fs.readFileSync(certPath, 'utf8'),
          ) as TransactionFinalizationCertificate;
          manifests.push(manifest);
          certificates.push(cert);
          for (const b of manifest.blobs) {
            if (b.manifest.sync_policy === 'approved') {
              approvedBlobHashes.add(b.manifest.sha256);
            } else {
              excludedLocalOnlyHashes.add(b.manifest.sha256);
            }
          }
        }
        return {
          manifests,
          certificates,
          approvedBlobHashes: [...approvedBlobHashes],
          excludedLocalOnlyHashes: [...excludedLocalOnlyHashes],
        };
      },
    };
  }

  /**
   * Step 1: Reserve streams + write lease + monotonic fencing_epoch.
   */
  reserveTransaction(
    store: StateStore,
    projectId: string,
    spec: TransactionSpec,
    leaseTtlMs = 30000,
  ): PreparedTxnContext {
    const txnId = newId('txn', 10);
    const ownerToken = newId('own', 12);
    const nowIso = this.clock.wallIso();
    const leaseUntilNs = (
      this.clock.monotonicNs() +
      BigInt(leaseTtlMs) * 1_000_000n
    ).toString();
    const sortedStreams = [...spec.affected_streams].sort((a, b) =>
      a.stream_id.localeCompare(b.stream_id),
    );
    const streamIds = sortedStreams.map((s) => assertSafeIdentifier('stream_id', s.stream_id));
    const expectedHeads: Record<string, string | null> = {};
    for (const s of sortedStreams) {
      expectedHeads[s.stream_id] = s.expected_parent_head;
    }

    const fencingEpoch = store.runImmediate(() => {
      const epoch = store.nextFencingEpoch();
      const stalePreparingTxnIds = new Set<string>();
      for (const sid of streamIds) {
        const existingLease = store.db
          .prepare('SELECT * FROM stream_leases WHERE stream_id = ?')
          .get(sid) as
          | {
              txn_id: string;
              lease_until_mono_ns: string;
              clock_domain_id: string;
              boot_epoch: string;
              lease_state: string;
              pid: number;
            }
          | undefined;
        if (existingLease) {
          const existingTxn = store.getTxn(existingLease.txn_id);
          if (existingTxn?.state === 'committed_pending_publish') {
            throw new CommitConflictError(
              `Stream ${sid} is locked by committed_pending_publish txn ${existingLease.txn_id}; run recovery first`,
            );
          }
          const bootChanged = existingLease.boot_epoch !== this.clock.bootEpoch();
          const domainChanged = existingLease.clock_domain_id !== this.clock.clockDomainId();
          const uncertain =
            existingLease.lease_state === 'unknown' ||
            (this.clock.isClockContinuityUncertain?.(
              existingLease.clock_domain_id,
              existingLease.boot_epoch,
            ) ?? false);
          const ownerAlive = isPidAlive(existingLease.pid);
          const expired =
            !uncertain &&
            !bootChanged &&
            !domainChanged &&
            BigInt(existingLease.lease_until_mono_ns) <= this.clock.monotonicNs();

          if (
            (existingLease.lease_state === 'active' && !uncertain && !expired && ownerAlive) ||
            (uncertain && !bootChanged && !domainChanged && ownerAlive)
          ) {
            throw new CommitConflictError(
              `Stream ${sid} is reserved by txn ${existingLease.txn_id} (lease_state=${existingLease.lease_state}, uncertain=${uncertain}, pid=${existingLease.pid})`,
            );
          }
          if (existingTxn && existingTxn.state === 'preparing') {
            stalePreparingTxnIds.add(existingLease.txn_id);
          }
        }
      }

      // Abort any expired/dead/rebooted preparing txns whose leases we are reclaiming so old owners fail CAS
      for (const staleTxnId of stalePreparingTxnIds) {
        const abortEpoch = store.nextFencingEpoch();
        store.db
          .prepare(
            `UPDATE txn_log SET state = 'aborted', fencing_epoch = ?, updated_at = ? WHERE txn_id = ? AND state = 'preparing'`,
          )
          .run(abortEpoch, nowIso, staleTxnId);
        store.db.prepare('DELETE FROM stream_leases WHERE txn_id = ?').run(staleTxnId);
      }

      store.db
        .prepare(
          `INSERT INTO txn_log(
            txn_id, thread_id, state, owner_token, fencing_epoch,
            clock_domain_id, boot_epoch, lease_until_mono_ns, lease_state, pid,
            manifest_hash, affected_streams_json, expected_parent_heads_json,
            expected_journal_head, created_at, updated_at
          ) VALUES (?, ?, 'preparing', ?, ?, ?, ?, ?, 'active', ?, NULL, ?, ?, ?, ?, ?)`,
        )
        .run(
          txnId,
          spec.thread_id,
          ownerToken,
          epoch,
          this.clock.clockDomainId(),
          this.clock.bootEpoch(),
          leaseUntilNs,
          process.pid,
          JSON.stringify(streamIds),
          JSON.stringify(expectedHeads),
          spec.expected_journal_head ?? null,
          nowIso,
          nowIso,
        );

      const upsertLease = store.db.prepare(
        `INSERT INTO stream_leases(
          stream_id, txn_id, owner_token, fencing_epoch, clock_domain_id, boot_epoch,
          lease_until_mono_ns, lease_state, pid
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?)
        ON CONFLICT(stream_id) DO UPDATE SET
          txn_id = excluded.txn_id,
          owner_token = excluded.owner_token,
          fencing_epoch = excluded.fencing_epoch,
          clock_domain_id = excluded.clock_domain_id,
          boot_epoch = excluded.boot_epoch,
          lease_until_mono_ns = excluded.lease_until_mono_ns,
          lease_state = 'active',
          pid = excluded.pid`,
      );
      for (const sid of streamIds) {
        upsertLease.run(
          sid,
          txnId,
          ownerToken,
          epoch,
          this.clock.clockDomainId(),
          this.clock.bootEpoch(),
          leaseUntilNs,
          process.pid,
        );
      }
      return epoch;
    });

    const layout = this.getLayout(projectId, spec.thread_id);
    const placeholderManifest: TransactionManifest = {
      schema_version: SCHEMA_VERSION,
      txn_id: txnId,
      project_id: projectId,
      thread_id: spec.thread_id,
      affected_streams: streamIds,
      expected_parent_heads: expectedHeads,
      expected_journal_head: spec.expected_journal_head ?? null,
      shards: [],
      blobs: [],
      input_dependency_refs: (spec.input_dependency_manifests ?? []).map(
        (m) => m.input_dependency_id,
      ),
      provenance_refs: spec.provenance_refs ?? [],
      manifest_hash: '',
      created_at: nowIso,
    };

    return {
      txn_id: txnId,
      thread_id: spec.thread_id,
      owner_token: ownerToken,
      fencing_epoch: fencingEpoch,
      manifest: placeholderManifest,
      staged_stream_files: [],
      staged_blobs: [],
      staged_manifest_path: path.join(layout.txnsStagingDir, `${txnId}.manifest.json`),
      target_manifest_rel: path.posix.join('transactions', 'committed', `${txnId}.manifest.json`),
      is_new_thread: spec.is_new_thread,
      main_stream_id: spec.main_stream_id,
      spec,
    };
  }

  /**
   * Renews an active or unknown lease iff SQLite CAS confirms owner_token & fencing_epoch still hold.
   */
  renewLease(
    store: StateStore,
    txnId: string,
    ownerToken: string,
    fencingEpoch: number,
    extendMs = 30000,
  ): void {
    const newUntil = (
      this.clock.monotonicNs() +
      BigInt(extendMs) * 1_000_000n
    ).toString();
    const nowIso = this.clock.wallIso();
    store.runImmediate(() => {
      const row = store.getTxn(txnId);
      if (
        !row ||
        row.state !== 'preparing' ||
        row.owner_token !== ownerToken ||
        row.fencing_epoch !== fencingEpoch
      ) {
        throw new FencingViolationError(
          `Cannot renew lease for txn ${txnId}: ownership or fencing_epoch lost`,
        );
      }
      store.db
        .prepare(
          `UPDATE txn_log SET
            clock_domain_id = ?, boot_epoch = ?, lease_until_mono_ns = ?,
            lease_state = 'active', updated_at = ?
          WHERE txn_id = ?`,
        )
        .run(
          this.clock.clockDomainId(),
          this.clock.bootEpoch(),
          newUntil,
          nowIso,
          txnId,
        );
      store.db
        .prepare(
          `UPDATE stream_leases SET
            clock_domain_id = ?, boot_epoch = ?, lease_until_mono_ns = ?,
            lease_state = 'active'
          WHERE txn_id = ?`,
        )
        .run(
          this.clock.clockDomainId(),
          this.clock.bootEpoch(),
          newUntil,
          txnId,
        );
    });
  }

  /**
   * Step 2: Prepare sanitized blobs, stream shards, journal shards, and Transaction Manifest in staging.
   */
  prepareTransaction(store: StateStore, ctx: PreparedTxnContext): void {
    const layout = this.getLayout(ctx.manifest.project_id, ctx.thread_id);
    const txnId = ctx.txn_id;
    const nowIso = this.clock.wallIso();

    // Verify ownership before writing staging
    const curRow = store.getTxn(txnId);
    if (
      !curRow ||
      curRow.state !== 'preparing' ||
      curRow.owner_token !== ctx.owner_token ||
      curRow.fencing_epoch !== ctx.fencing_epoch
    ) {
      throw new FencingViolationError(
        `Prepare aborted for txn ${txnId}: fencing_epoch or owner_token mismatch`,
      );
    }

    const blobDir = path.join(layout.blobsStagingDir, txnId);
    const histDir = path.join(layout.historyStagingDir, txnId);
    fs.mkdirSync(blobDir, { recursive: true });
    fs.mkdirSync(histDir, { recursive: true });
    fs.mkdirSync(layout.journalStagingDir, { recursive: true });
    fs.mkdirSync(layout.txnsStagingDir, { recursive: true });

    const blobManifestEntries: BlobManifestEntry[] = [];
    const stagedBlobs: PreparedTxnContext['staged_blobs'] = [];
    if (ctx.spec.blobs) {
      for (const key of Object.keys(ctx.spec.blobs).sort()) {
        const bInput = ctx.spec.blobs[key];
        const rawBytes =
          typeof bInput.bytes === 'string'
            ? Buffer.from(bInput.bytes, 'utf8')
            : Buffer.from(bInput.bytes);
        const binary = isOpaqueBinary(rawBytes);
        let finalBytes: Uint8Array = rawBytes;
        let redactionStatus: BlobManifestEntry['redaction_status'] = 'clean';
        let syncPolicy = bInput.sync_policy ?? 'approved';
        let isolationLevel: BlobManifestEntry['isolation_level'] = 'normal';

        if (binary) {
          redactionStatus = 'unscannable_binary';
          syncPolicy = 'unscannable_binary';
          isolationLevel = 'local_only';
        } else {
          const text = Buffer.from(rawBytes).toString('utf8');
          const res = this.redactor.sanitizeText(text);
          if (res.redactedCount > 0) {
            finalBytes = Buffer.from(res.sanitized, 'utf8');
            redactionStatus = 'redacted';
          }
          if (syncPolicy === 'local_only') {
            isolationLevel = 'local_only';
          }
        }

        const digest = sha256Hex(finalBytes);
        const stagingPath = path.join(blobDir, digest);
        const fd = fs.openSync(stagingPath, 'w', 0o600);
        try {
          fs.writeSync(fd, finalBytes);
          fs.fsyncSync(fd);
        } finally {
          fs.closeSync(fd);
        }
        const entry: BlobManifestEntry = {
          sha256: digest,
          byte_length: finalBytes.byteLength,
          redaction_status: redactionStatus,
          isolation_level: isolationLevel,
          sync_policy: syncPolicy,
          media_type: bInput.media_type,
        };
        blobManifestEntries.push(entry);
        stagedBlobs.push({
          sha256: digest,
          staging_path: stagingPath,
          target_rel: path.posix.join('blobs', 'objects', digest.slice(0, 2), digest),
          manifest: entry,
        });
      }
    }

    const manifestBlobs = stagedBlobs.map((b) => ({
      relative_path: b.target_rel,
      manifest: b.manifest,
    }));

    const shards: ShardEntry[] = [];
    const stagedStreamFiles: PreparedTxnContext['staged_stream_files'] = [];
    const threadRow = store.getThread(ctx.thread_id);

    let attachedSharedControls = false;
    const sortedStreams = [...ctx.spec.affected_streams].sort((a, b) =>
      a.stream_id.localeCompare(b.stream_id),
    );
    const affectedStreamSet = new Set(sortedStreams.map((s) => s.stream_id));
    for (const sInput of sortedStreams) {
      const existingStream = store.getStream(sInput.stream_id);
      const nextSeq = (existingStream?.last_seq ?? 0) + 1;
      const lines: StreamShardLine[] = [];
      for (const ev of sInput.events) {
        const sanitizedEv: EventRecord = {
          ...ev,
          commit_txn_id: txnId,
          payload: this.sanitizeEventPayload(ev.payload),
        };
        lines.push(sanitizedEv);
      }
      if (sInput.invocations) {
        for (const inv of sInput.invocations) {
          lines.push(inv);
        }
      }
      if (sInput.controls) {
        for (const ctrl of sInput.controls) {
          if (ctrl.payload.type === 'workspace_manifest') {
            lines.push({
              ...ctrl,
              commit_txn_id: txnId,
              payload: {
                type: 'workspace_manifest',
                manifest: this.sanitizeWorkspaceManifest(ctrl.payload.manifest),
              },
            });
          } else if (ctrl.payload.type === 'transcript_locator') {
            lines.push({
              ...ctrl,
              commit_txn_id: txnId,
              payload: {
                type: 'transcript_locator',
                locator: this.sanitizeTranscriptLocator(ctrl.payload.locator),
              },
            });
          } else {
            lines.push({ ...ctrl, commit_txn_id: txnId });
          }
        }
      }
      for (const hb of ctx.spec.host_bindings ?? []) {
        if (
          hb.stream_id === sInput.stream_id ||
          (!affectedStreamSet.has(hb.stream_id) && !attachedSharedControls)
        ) {
          lines.push({
            schema_version: SCHEMA_VERSION,
            kind: 'control',
            control_id: newId('ctrl', 8),
            stream_id: hb.stream_id,
            commit_txn_id: txnId,
            created_at: nowIso,
            payload: {
              type: 'host_binding',
              host_session_id: hb.host_session_id,
              stream_id: hb.stream_id,
            },
          });
        }
      }
      if (!attachedSharedControls) {
        attachedSharedControls = true;
        if (blobManifestEntries.length > 0) {
          lines.push({
            schema_version: SCHEMA_VERSION,
            kind: 'control',
            control_id: newId('ctrl', 8),
            stream_id: sInput.stream_id,
            commit_txn_id: txnId,
            created_at: nowIso,
            payload: { type: 'blob_manifest', entries: blobManifestEntries },
          });
        }
        for (const idm of ctx.spec.input_dependency_manifests ?? []) {
          lines.push({
            schema_version: SCHEMA_VERSION,
            kind: 'control',
            control_id: newId('ctrl', 8),
            stream_id: sInput.stream_id,
            commit_txn_id: txnId,
            created_at: nowIso,
            payload: { type: 'input_dependency_manifest', manifest: idm },
          });
        }
      }
      const safeWs = this.sanitizeWorkspaceManifest(sInput.anchor.workspace_manifest);
      const safeAnchor: LineageAnchor = {
        ...sInput.anchor,
        workspace_manifest: safeWs,
        ...(sInput.anchor.summary
          ? { summary: this.redactor.sanitizeText(sInput.anchor.summary).sanitized }
          : {}),
      };
      lines.push({
        schema_version: SCHEMA_VERSION,
        kind: 'control',
        control_id: newId('ctrl', 8),
        stream_id: sInput.stream_id,
        commit_txn_id: txnId,
        created_at: nowIso,
        payload: { type: 'anchor', anchor: safeAnchor },
      });

      const jsonl = `${lines.map((l) => canonicalJson(l)).join('\n')}\n`;
      const shardSha = sha256Hex(jsonl);
      const stagingPath = path.join(histDir, `${sInput.stream_id}.jsonl`);
      const fd = fs.openSync(stagingPath, 'w', 0o600);
      try {
        fs.writeSync(fd, jsonl, 0, 'utf8');
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      const seqStr = String(nextSeq).padStart(6, '0');
      const targetRel = path.posix.join(
        'history',
        'committed',
        sInput.stream_id,
        `${seqStr}_${txnId}.jsonl`,
      );
      shards.push({
        relative_path: targetRel,
        sha256: shardSha,
        stream_id: sInput.stream_id,
        seq: nextSeq,
      });
      stagedStreamFiles.push({
        stream_id: sInput.stream_id,
        seq: nextSeq,
        staging_path: stagingPath,
        target_rel: targetRel,
        sha256: shardSha,
      });
    }

    let journalShard: ShardEntry | undefined;
    let stagedJournalFile: PreparedTxnContext['staged_journal_file'];
    if (ctx.spec.journal_events && ctx.spec.journal_events.length > 0) {
      const nextJSeq = (threadRow?.journal_last_seq ?? 0) + 1;
      const stampedEvents = ctx.spec.journal_events.map((je) => ({
        ...je,
        commit_txn_id: txnId,
      }));
      const jJsonl = `${stampedEvents.map((je) => canonicalJson(je)).join('\n')}\n`;
      const jSha = sha256Hex(jJsonl);
      const jStagingPath = path.join(layout.journalStagingDir, `${txnId}.jsonl`);
      const fd = fs.openSync(jStagingPath, 'w', 0o600);
      try {
        fs.writeSync(fd, jJsonl, 0, 'utf8');
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
      const seqStr = String(nextJSeq).padStart(6, '0');
      const jTargetRel = path.posix.join(
        'coordination',
        'journal',
        'committed',
        `${seqStr}_${txnId}.jsonl`,
      );
      journalShard = { relative_path: jTargetRel, sha256: jSha, seq: nextJSeq };
      stagedJournalFile = {
        seq: nextJSeq,
        staging_path: jStagingPath,
        target_rel: jTargetRel,
        sha256: jSha,
      };
    }

    const unsignedManifest: Omit<TransactionManifest, 'manifest_hash'> = {
      schema_version: SCHEMA_VERSION,
      txn_id: txnId,
      project_id: ctx.manifest.project_id,
      thread_id: ctx.thread_id,
      affected_streams: ctx.manifest.affected_streams,
      expected_parent_heads: ctx.manifest.expected_parent_heads,
      expected_journal_head: ctx.manifest.expected_journal_head,
      shards,
      journal_shard: journalShard,
      blobs: manifestBlobs,
      input_dependency_refs: ctx.manifest.input_dependency_refs,
      provenance_refs: ctx.manifest.provenance_refs,
      created_at: ctx.manifest.created_at,
    };
    const manifestHash = manifestDigestPayload(unsignedManifest);
    const fullManifest: TransactionManifest = {
      ...unsignedManifest,
      manifest_hash: manifestHash,
    };

    const mFd = fs.openSync(ctx.staged_manifest_path, 'w', 0o600);
    try {
      fs.writeSync(mFd, `${canonicalJson(fullManifest)}\n`, 0, 'utf8');
      fs.fsyncSync(mFd);
    } finally {
      fs.closeSync(mFd);
    }

    ctx.manifest = fullManifest;
    ctx.staged_stream_files = stagedStreamFiles;
    ctx.staged_journal_file = stagedJournalFile;
    ctx.staged_blobs = stagedBlobs;
  }

  private sanitizeWorkspaceManifest(ws: WorkspaceManifest): WorkspaceManifest {
    const scrub = (s: string) => this.redactor.sanitizeText(s).sanitized;
    return {
      ...ws,
      root_hint: ws.root_hint ? scrub(ws.root_hint) : undefined,
      remote_url: ws.remote_url ? scrub(redactRemoteUrlCredentials(ws.remote_url)) : undefined,
      branch: ws.branch ? scrub(ws.branch) : undefined,
      dirty_files: ws.dirty_files.map(scrub),
      untracked_files: ws.untracked_files.map(scrub),
      blind_spots: ws.blind_spots.map(scrub),
    };
  }

  private sanitizeTranscriptLocator(loc: HostTranscriptLocator): HostTranscriptLocator {
    const scrub = (s: string) => this.redactor.sanitizeText(s).sanitized;
    return {
      ...loc,
      source_label: scrub(loc.source_label),
      file_path: loc.file_path ? scrub(loc.file_path) : undefined,
      cloud_provider: loc.cloud_provider ? scrub(loc.cloud_provider) : undefined,
      host_session_id: loc.host_session_id ? scrub(loc.host_session_id) : undefined,
      remote_uri: loc.remote_uri ? scrub(redactRemoteUrlCredentials(loc.remote_uri)) : undefined,
      uploaded_filename: loc.uploaded_filename ? scrub(loc.uploaded_filename) : undefined,
      note: loc.note ? scrub(loc.note) : undefined,
    };
  }

  private sanitizeEventPayload(payload: EventRecord['payload']): EventRecord['payload'] {
    if (payload.type === 'human_message') {
      return { ...payload, text: this.redactor.sanitizeText(payload.text).sanitized };
    }
    if (payload.type === 'assistant_part') {
      return {
        ...payload,
        text: payload.text ? this.redactor.sanitizeText(payload.text).sanitized : undefined,
        args_json: payload.args_json
          ? this.redactor.sanitizeText(payload.args_json).sanitized
          : undefined,
      };
    }
    if (payload.type === 'tool_result') {
      return {
        ...payload,
        text: payload.text ? this.redactor.sanitizeText(payload.text).sanitized : undefined,
      };
    }
    if (payload.type === 'coordinator_inject') {
      return {
        ...payload,
        summary_text: this.redactor.sanitizeText(payload.summary_text).sanitized,
      };
    }
    return {
      ...payload,
      text: payload.text ? this.redactor.sanitizeText(payload.text).sanitized : undefined,
    };
  }

  /**
   * Step 3: Commit Intent inside SQLite `BEGIN IMMEDIATE`.
   * Re-checks owner_token, fencing_epoch, expected parent heads & journal head.
   * Registers signed commit_intent_proof and transitions txn_log -> 'committed_pending_publish'.
   * Does NOT advance active stream heads.
   */
  commitIntentTransaction(store: StateStore, ctx: PreparedTxnContext): CommitIntentProof {
    const nowIso = this.clock.wallIso();
    const proof = signCommitIntentProof(this.identity, {
      txn_id: ctx.txn_id,
      project_id: ctx.manifest.project_id,
      thread_id: ctx.thread_id,
      manifest_hash: ctx.manifest.manifest_hash,
      expected_parent_heads: ctx.manifest.expected_parent_heads,
      expected_journal_head: ctx.manifest.expected_journal_head,
      fencing_epoch: ctx.fencing_epoch,
      issued_at: nowIso,
    });

    store.runImmediate(() => {
      const row = store.getTxn(ctx.txn_id);
      if (
        !row ||
        row.state !== 'preparing' ||
        row.owner_token !== ctx.owner_token ||
        row.fencing_epoch !== ctx.fencing_epoch
      ) {
        throw new FencingViolationError(
          `Commit Intent rejected for txn ${ctx.txn_id}: fencing_epoch or owner_token mismatch`,
        );
      }

      // Check expected parent heads
      for (const sInput of ctx.spec.affected_streams) {
        const sRow = store.getStream(sInput.stream_id);
        const actualHead = sRow?.head_anchor_id ?? null;
        if (actualHead !== sInput.expected_parent_head) {
          throw new CommitConflictError(
            `Parent head CAS mismatch on stream ${sInput.stream_id}: expected=${sInput.expected_parent_head} actual=${actualHead}`,
          );
        }
      }

      // Check expected journal head
      const tRow = store.getThread(ctx.thread_id);
      const actualJHead = tRow?.journal_head_id ?? null;
      if (
        ctx.spec.expected_journal_head !== undefined &&
        actualJHead !== ctx.spec.expected_journal_head
      ) {
        throw new CommitConflictError(
          `Journal head CAS mismatch on thread ${ctx.thread_id}: expected=${ctx.spec.expected_journal_head} actual=${actualJHead}`,
        );
      }

      // Atomically claim any single-use confirmation proof nonces inside SQLite BEGIN IMMEDIATE
      // to eliminate concurrent replay TOCTOU races before entering committed_pending_publish.
      const specProofs = collectSpecConfirmationProofs(ctx.spec);
      for (const cp of specProofs) {
        if (store.hasConsumedConfirmationNonce(cp.nonce)) {
          throw new CommitConflictError(
            `Confirmation proof nonce ${cp.nonce} was already consumed (atomic replay guard)`,
          );
        }
        store.consumeConfirmationNonce(cp, nowIso);
      }

      store.db
        .prepare(
          `UPDATE txn_log SET state = 'committed_pending_publish', manifest_hash = ?, updated_at = ? WHERE txn_id = ?`,
        )
        .run(ctx.manifest.manifest_hash, nowIso, ctx.txn_id);

      // Persist commit_intent_proof snapshot in staging so crash recovery can roll forward even without memory
      const layout = this.getLayout(ctx.manifest.project_id, ctx.thread_id);
      const intentPath = path.join(layout.txnsStagingDir, `${ctx.txn_id}.intent.json`);
      const fd = fs.openSync(intentPath, 'w', 0o600);
      try {
        fs.writeSync(fd, `${canonicalJson(proof)}\n`, 0, 'utf8');
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
    });

    ctx.commit_intent_proof = proof;
    return proof;
  }

  /**
   * Step 4: Publish all blobs, stream shards, journal shards, and .manifest.json
   * exclusively via `PublishImmutable` (unique tmp + fsync + no-replace fs.link).
   */
  publishTransaction(ctx: PreparedTxnContext): void {
    const layout = this.getLayout(ctx.manifest.project_id, ctx.thread_id);

    for (const b of ctx.staged_blobs) {
      const target = path.join(layout.rootDir, ...b.target_rel.split('/'));
      const raw = fs.readFileSync(b.staging_path);
      publishImmutable(path.dirname(b.staging_path), target, raw, b.sha256);
    }

    for (const sf of ctx.staged_stream_files) {
      const target = path.join(layout.rootDir, ...sf.target_rel.split('/'));
      const raw = fs.readFileSync(sf.staging_path);
      publishImmutable(path.dirname(sf.staging_path), target, raw, sf.sha256);
    }

    if (ctx.staged_journal_file) {
      const jf = ctx.staged_journal_file;
      const target = path.join(layout.rootDir, ...jf.target_rel.split('/'));
      const raw = fs.readFileSync(jf.staging_path);
      publishImmutable(path.dirname(jf.staging_path), target, raw, jf.sha256);
    }

    const manifestTarget = path.join(layout.rootDir, ...ctx.target_manifest_rel.split('/'));
    const mBytes = fs.readFileSync(ctx.staged_manifest_path);
    publishImmutable(layout.txnsStagingDir, manifestTarget, mBytes, sha256Hex(mBytes));
  }

  /**
   * Step 5: Finalize (Certificate-as-Commit with strict SQLite lock boundary):
   * - OUTSIDE SQLite write lock: verify full SHA-256 closure over committed objects & manifest,
   *   write + fsync `.finalized.tmp` in staging.
   * - INSIDE SQLite `BEGIN IMMEDIATE` write lock: re-verify `txn_log.state === 'committed_pending_publish'`,
   *   `owner_token`, and `fencing_epoch`; perform sub-millisecond `fs.linkSync(tmp, .finalized.json)` +
   *   POSIX parent dir fsync (Certificate-as-Commit commit point); atomically update SQLite stream/journal
   *   projections, mark `txn_log = 'published'`, release stream_leases, and COMMIT.
   * - OUTSIDE write lock: clean empty staging dirs and refresh non-authoritative `lineage.json`.
   */
  finalizeTransaction(
    store: StateStore,
    ctx: PreparedTxnContext,
    faultHooks?: TxnFaultHooks,
  ): TransactionFinalizationCertificate {
    const layout = this.getLayout(ctx.manifest.project_id, ctx.thread_id);
    if (!ctx.commit_intent_proof) {
      throw new Error(`Missing commit_intent_proof for txn ${ctx.txn_id}`);
    }

    // 1) OUTSIDE write lock: verify SHA-256 closure
    let verifiedCount = 0;
    const closureHashes: string[] = [];
    for (const s of ctx.manifest.shards) {
      const p = path.join(layout.rootDir, ...s.relative_path.split('/'));
      const actual = sha256Hex(fs.readFileSync(p));
      if (actual !== s.sha256) {
        throw new Error(`Publication closure check failed for shard ${s.relative_path}`);
      }
      closureHashes.push(actual);
      verifiedCount += 1;
    }
    if (ctx.manifest.journal_shard) {
      const jp = path.join(layout.rootDir, ...ctx.manifest.journal_shard.relative_path.split('/'));
      const actual = sha256Hex(fs.readFileSync(jp));
      if (actual !== ctx.manifest.journal_shard.sha256) {
        throw new Error(`Publication closure check failed for journal shard`);
      }
      closureHashes.push(actual);
      verifiedCount += 1;
    }
    for (const b of ctx.manifest.blobs) {
      const bp = path.join(layout.rootDir, ...b.relative_path.split('/'));
      const actual = sha256Hex(fs.readFileSync(bp));
      if (actual !== b.manifest.sha256) {
        throw new Error(`Publication closure check failed for blob ${b.manifest.sha256}`);
      }
      closureHashes.push(actual);
      verifiedCount += 1;
    }
    const manifestPath = path.join(layout.rootDir, ...ctx.target_manifest_rel.split('/'));
    const manifestBytes = fs.readFileSync(manifestPath);
    const parsedManifest = JSON.parse(manifestBytes.toString('utf8')) as TransactionManifest;
    const { manifest_hash, ...unsigned } = parsedManifest;
    if (manifest_hash !== ctx.manifest.manifest_hash || manifestDigestPayload(unsigned) !== manifest_hash) {
      throw new Error(`Manifest hash check failed during Finalize for txn ${ctx.txn_id}`);
    }

    const nowIso = this.clock.wallIso();
    const closureProof: PublicationClosureProof = {
      txn_id: ctx.txn_id,
      manifest_hash: ctx.manifest.manifest_hash,
      verified_objects_count: verifiedCount,
      verified_at: nowIso,
      closure_digest: sha256Hex(closureHashes.join('\n')),
    };

    const cert: TransactionFinalizationCertificate = {
      schema_version: SCHEMA_VERSION,
      txn_id: ctx.txn_id,
      manifest_hash: ctx.manifest.manifest_hash,
      commit_intent_proof: ctx.commit_intent_proof,
      publication_closure_proof: closureProof,
      finalized_at: nowIso,
    };

    const certBytes = Buffer.from(`${canonicalJson(cert)}\n`, 'utf8');
    const certSha = sha256Hex(certBytes);
    const tmpCertPath = path.join(layout.txnsStagingDir, `.${ctx.txn_id}.finalized.tmp`);
    const finalCertPath = path.join(layout.txnsCommittedDir, `${ctx.txn_id}.finalized.json`);

    // Pre-write & fsync temporary certificate outside SQLite lock
    const fd = fs.openSync(tmpCertPath, 'w', 0o600);
    try {
      fs.writeSync(fd, certBytes);
      fs.fsyncSync(fd);
    } finally {
      fs.closeSync(fd);
    }

    try {
      // 2) INSIDE SQLite `BEGIN IMMEDIATE` write lock
      store.runImmediate(() => {
        const row = store.getTxn(ctx.txn_id);
        if (
          !row ||
          row.state !== 'committed_pending_publish' ||
          row.owner_token !== ctx.owner_token ||
          row.fencing_epoch !== ctx.fencing_epoch
        ) {
          throw new FencingViolationError(
            `Finalize blocked inside SQLite write lock for txn ${ctx.txn_id}: stale fencing_epoch (${ctx.fencing_epoch}) or wrong state (${row?.state})`,
          );
        }

        // Link already-fsynced temporary certificate inside tight exclusive write lock
        linkPreparedImmutable(tmpCertPath, finalCertPath, certSha);

        if (faultHooks?.afterFinalizedLinkBeforeSqliteCommit) {
          faultHooks.afterFinalizedLinkBeforeSqliteCommit(ctx);
        }

        // Atomically update local SQLite projections & release reservations
        this.applyPublishedTxnToSqlite(
          store,
          layout,
          parsedManifest,
          nowIso,
          undefined,
          undefined,
          ctx.spec.host_bindings,
        );
      });
    } finally {
      if (fs.existsSync(tmpCertPath)) {
        try {
          fs.unlinkSync(tmpCertPath);
        } catch {
          // ignore
        }
      }
    }

    // 3) OUTSIDE lock: cleanup txn staging & refresh lineage.json
    this.cleanupTxnStaging(layout, ctx.txn_id);
    this.exportLineageJson(store, layout);
    return cert;
  }

  /**
   * Executes the complete 5-step dual-domain transaction.
   */
  executeTransaction(
    projectId: string,
    spec: TransactionSpec,
    faultHooks?: TxnFaultHooks,
  ): {
    txn_id: string;
    manifest: TransactionManifest;
    certificate: TransactionFinalizationCertificate;
  } {
    const store = this.openStore(projectId, spec.thread_id);
    let ctx: PreparedTxnContext | undefined;
    try {
      // Recover any prior interrupted transactions before starting new work
      this.recoverPendingTransactions(projectId, spec.thread_id, { store });
      ctx = this.reserveTransaction(store, projectId, spec);
      this.prepareTransaction(store, ctx);
      if (faultHooks?.afterPrepareBeforeCommitIntent) {
        faultHooks.afterPrepareBeforeCommitIntent(ctx);
      }
      this.commitIntentTransaction(store, ctx);
      if (faultHooks?.afterCommitIntentBeforePublish) {
        faultHooks.afterCommitIntentBeforePublish(ctx);
      }
      this.publishTransaction(ctx);
      if (faultHooks?.afterPublishBeforeFinalize) {
        faultHooks.afterPublishBeforeFinalize(ctx);
      }
      const cert = this.finalizeTransaction(store, ctx, faultHooks);
      return { txn_id: ctx.txn_id, manifest: ctx.manifest, certificate: cert };
    } catch (err) {
      if (ctx && !ctx.commit_intent_proof && !(err instanceof SimulatedCrashError)) {
        const abortCtx = ctx;
        try {
          store.runImmediate(() => {
            store.db
              .prepare(
                `UPDATE txn_log SET state = 'aborted', updated_at = ?
                 WHERE txn_id = ? AND state = 'preparing' AND owner_token = ? AND fencing_epoch = ?`,
              )
              .run(this.clock.wallIso(), abortCtx.txn_id, abortCtx.owner_token, abortCtx.fencing_epoch);
            store.db
              .prepare(`DELETE FROM stream_leases WHERE txn_id = ? AND owner_token = ?`)
              .run(abortCtx.txn_id, abortCtx.owner_token);
          });
          this.cleanupTxnStaging(this.getLayout(projectId, spec.thread_id), abortCtx.txn_id);
        } catch {
          // Best-effort abort cleanup before rethrowing original error
        }
      }
      throw err;
    } finally {
      store.close();
    }
  }

  private applyPublishedTxnToSqlite(
    store: StateStore,
    layout: ThreadDirLayout,
    manifest: TransactionManifest,
    nowIso: string,
    extraHealthByStream?: Map<string, StreamProjectionHealth>,
    divergentHeadsByStream?: Map<string, string[]>,
    hostBindings?: Array<{ host_session_id: string; stream_id: string }>,
  ): void {
    const existingThread = store.getThread(manifest.thread_id);
    let mainStreamId = existingThread?.main_stream_id ?? '';
    let maxCoordEpoch = existingThread?.coordinator_epoch ?? 1;
    let journalHeadId = existingThread?.journal_head_id ?? null;
    let journalSeq = existingThread?.journal_last_seq ?? 0;

    // Parse journal shard first if present (to discover main_designated)
    const parsedJournalEvents: JournalEventRecord[] = [];
    if (manifest.journal_shard) {
      const jp = path.join(layout.rootDir, ...manifest.journal_shard.relative_path.split('/'));
      const jText = fs.readFileSync(jp, 'utf8');
      for (const line of jText.split(/\r?\n/)) {
        if (!line.trim()) continue;
        const je = JSON.parse(line) as JournalEventRecord;
        parsedJournalEvents.push(je);
        if (je.event_type === 'main_designated' && !mainStreamId) {
          mainStreamId = String(je.data.main_stream_id ?? '');
        }
        if (je.coordinator_epoch > maxCoordEpoch) {
          maxCoordEpoch = je.coordinator_epoch;
        }
        journalHeadId = je.journal_event_id;
      }
      journalSeq = manifest.journal_shard.seq ?? journalSeq + 1;
    }

    if (!mainStreamId && manifest.affected_streams.length > 0) {
      mainStreamId = manifest.affected_streams[0];
    }

    store.db
      .prepare(
        `INSERT INTO threads(thread_id, project_id, main_stream_id, coordinator_epoch, journal_head_id, journal_last_seq, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(thread_id) DO UPDATE SET
           coordinator_epoch = MAX(threads.coordinator_epoch, excluded.coordinator_epoch),
           journal_head_id = COALESCE(excluded.journal_head_id, threads.journal_head_id),
           journal_last_seq = MAX(threads.journal_last_seq, excluded.journal_last_seq)`,
      )
      .run(
        manifest.thread_id,
        manifest.project_id,
        mainStreamId,
        maxCoordEpoch,
        journalHeadId,
        journalSeq,
        manifest.created_at,
      );

    // Register journal events with strictly monotonic seq in projection to avoid collisions across synced devices
    const insertJe = store.db.prepare(
      `INSERT OR IGNORE INTO journal_events_proj(
        journal_event_id, thread_id, main_stream_id, event_type, actor_stream_id,
        commit_txn_id, coordinator_epoch, seq, event_json, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    );
    const maxSeqRow = store.db
      .prepare('SELECT COALESCE(MAX(seq), 0) AS max_seq FROM journal_events_proj WHERE thread_id = ?')
      .get(manifest.thread_id) as { max_seq: number } | undefined;
    let seqCursor = maxSeqRow?.max_seq ?? 0;
    for (const je of parsedJournalEvents) {
      seqCursor += 1;
      insertJe.run(
        je.journal_event_id,
        je.thread_id,
        je.main_stream_id,
        je.event_type,
        je.actor_stream_id,
        manifest.txn_id,
        je.coordinator_epoch,
        seqCursor,
        canonicalJson(je),
        je.created_at,
      );
    }

    // Register blobs
    const upsertBlob = store.db.prepare(
      `INSERT INTO blobs_index(sha256, byte_length, sync_policy, manifest_json, ref_count, orphan_since)
       VALUES (?, ?, ?, ?, 1, NULL)
       ON CONFLICT(sha256) DO UPDATE SET ref_count = blobs_index.ref_count + 1, orphan_since = NULL`,
    );
    for (const b of manifest.blobs) {
      upsertBlob.run(
        b.manifest.sha256,
        b.manifest.byte_length,
        b.manifest.sync_policy,
        canonicalJson(b.manifest),
      );
    }

    // Process each stream shard
    for (const shard of manifest.shards) {
      const sp = path.join(layout.rootDir, ...shard.relative_path.split('/'));
      const lines = fs
        .readFileSync(sp, 'utf8')
        .split(/\r?\n/)
        .filter((l) => l.trim().length > 0)
        .map((l) => JSON.parse(l) as StreamShardLine);

      let shardAnchor: LineageAnchor | null = null;
      const streamId = shard.stream_id ?? '';
      for (const rec of lines) {
        if (rec.kind === 'event') {
          if (rec.provenance.confirmation_proof?.nonce) {
            store.consumeConfirmationNonce(rec.provenance.confirmation_proof, rec.created_at);
          }
          store.db
            .prepare(
              `INSERT OR IGNORE INTO event_fingerprints(event_id, project_id, thread_id, stream_id)
               VALUES (?, ?, ?, ?)`,
            )
            .run(rec.event_id, manifest.project_id, manifest.thread_id, rec.stream_id);
          if (rec.payload.type === 'cross_stream_message') {
            store.db
              .prepare(
                `INSERT OR IGNORE INTO mailbox_proj(
                  message_id, from_stream_id, target_stream_id, status, batch_id,
                  lease_until_mono_ns, payload_json, updated_at
                ) VALUES (?, ?, ?, 'pending', NULL, NULL, ?, ?)`,
              )
              .run(
                rec.payload.message_id,
                rec.payload.from_stream_id,
                rec.payload.target_stream_id,
                canonicalJson(rec.payload),
                rec.created_at,
              );
          }
        } else if (rec.kind === 'control') {
          if (rec.payload.type === 'anchor') {
            shardAnchor = rec.payload.anchor;
          } else if (rec.payload.type === 'reconciliation_record') {
            if (rec.payload.record.confirmation_proof?.nonce) {
              store.consumeConfirmationNonce(
                rec.payload.record.confirmation_proof,
                rec.created_at,
              );
            }
          } else if (rec.payload.type === 'notify_receipt') {
            this.applyNotifyReceiptProj(store, rec.payload.receipt);
          } else if (rec.payload.type === 'transcript_locator') {
            store.upsertTranscriptLocator(manifest.thread_id, rec.payload.locator);
          } else if (rec.payload.type === 'host_binding') {
            store.db
              .prepare(
                `INSERT INTO host_bindings(host_session_id, project_id, thread_id, stream_id, bound_at)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(host_session_id) DO NOTHING`,
              )
              .run(
                rec.payload.host_session_id,
                manifest.project_id,
                manifest.thread_id,
                rec.payload.stream_id,
                rec.created_at,
              );
          }
        }
      }

      if (shardAnchor) {
        const isDivergent = divergentHeadsByStream?.has(shardAnchor.stream_id) ?? false;
        const health = extraHealthByStream?.get(shardAnchor.stream_id) ?? 'complete';
        const isMain = shardAnchor.stream_id === mainStreamId ? 1 : 0;
        const effectiveHead = isDivergent ? null : shardAnchor.anchor_id;
        store.db
          .prepare(
            `INSERT INTO streams(stream_id, thread_id, head_anchor_id, last_seq, health, is_main, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(stream_id) DO UPDATE SET
               head_anchor_id = CASE WHEN excluded.health = 'divergent_heads' THEN NULL ELSE excluded.head_anchor_id END,
               last_seq = MAX(streams.last_seq, excluded.last_seq),
               health = excluded.health,
               is_main = CASE WHEN streams.is_main = 1 THEN 1 ELSE excluded.is_main END`,
          )
          .run(
            shardAnchor.stream_id,
            manifest.thread_id,
            effectiveHead,
            shard.seq ?? 1,
            health,
            isMain,
            shardAnchor.created_at,
          );

        store.db
          .prepare(
            `INSERT OR IGNORE INTO anchors(anchor_id, thread_id, stream_id, commit_txn_id, parents_json, anchor_json, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)`,
          )
          .run(
            shardAnchor.anchor_id,
            manifest.thread_id,
            shardAnchor.stream_id,
            manifest.txn_id,
            JSON.stringify(shardAnchor.parents),
            canonicalJson(shardAnchor),
            shardAnchor.created_at,
          );

        for (const parentId of shardAnchor.parents) {
          store.db
            .prepare(
              `INSERT OR IGNORE INTO lineage_edges(child_anchor_id, parent_anchor_id) VALUES (?, ?)`,
            )
            .run(shardAnchor.anchor_id, parentId);
        }
      }
    }

    store.db
      .prepare(
        `INSERT INTO txn_log(
          txn_id, thread_id, state, owner_token, fencing_epoch, clock_domain_id, boot_epoch,
          lease_until_mono_ns, lease_state, pid, manifest_hash, affected_streams_json,
          expected_parent_heads_json, expected_journal_head, created_at, updated_at
        ) VALUES (?, ?, 'published', 'rebuilt', 0, ?, ?, '0', 'expired', ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(txn_id) DO UPDATE SET state = 'published', updated_at = excluded.updated_at`,
      )
      .run(
        manifest.txn_id,
        manifest.thread_id,
        this.clock.clockDomainId(),
        this.clock.bootEpoch(),
        process.pid,
        manifest.manifest_hash,
        JSON.stringify(manifest.affected_streams),
        JSON.stringify(manifest.expected_parent_heads),
        manifest.expected_journal_head,
        manifest.created_at,
        nowIso,
      );

    store.db.prepare('DELETE FROM stream_leases WHERE txn_id = ?').run(manifest.txn_id);

    if (hostBindings && hostBindings.length > 0) {
      const bindStmt = store.db.prepare(
        `INSERT INTO host_bindings(host_session_id, project_id, thread_id, stream_id, bound_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(host_session_id) DO NOTHING`,
      );
      for (const hb of hostBindings) {
        bindStmt.run(hb.host_session_id, manifest.project_id, manifest.thread_id, hb.stream_id, nowIso);
      }
    }
  }

  private applyNotifyReceiptProj(store: StateStore, receipt: NotifyReceiptRecord): void {
    const newStatus = receipt.receipt_type === 'offered' ? 'offered' : 'committed_receipt';
    if (receipt.receipt_type === 'committed_receipt') {
      // Verify offered batch matches target stream in projection
      for (const mid of receipt.message_ids) {
        const row = store.db
          .prepare('SELECT batch_id, target_stream_id FROM mailbox_proj WHERE message_id = ?')
          .get(mid) as { batch_id: string | null; target_stream_id: string } | undefined;
        if (
          row &&
          row.target_stream_id === receipt.target_stream_id &&
          row.batch_id === receipt.batch_id
        ) {
          store.db
            .prepare(
              `UPDATE mailbox_proj SET status = 'committed_receipt', updated_at = ? WHERE message_id = ?`,
            )
            .run(receipt.recorded_at, mid);
        }
      }
      return;
    }

    for (const mid of receipt.message_ids) {
      store.db
        .prepare(
          `UPDATE mailbox_proj
           SET status = CASE WHEN status = 'committed_receipt' THEN 'committed_receipt' ELSE ? END,
               batch_id = CASE WHEN status = 'committed_receipt' THEN batch_id ELSE ? END,
               lease_until_mono_ns = ?,
               updated_at = ?
           WHERE message_id = ? AND target_stream_id = ?`,
        )
        .run(
          newStatus,
          receipt.batch_id,
          receipt.lease_until_mono_ns ?? null,
          receipt.recorded_at,
          mid,
          receipt.target_stream_id,
        );
    }
  }

  /**
   * Deterministic crash recovery:
   * - `preparing`: respects active same-boot live owners; cleans up on boot_epoch change or dead/reclaimed unknown leases.
   * - `committed_pending_publish`: rolls forward using Certificate-as-Commit semantics.
   */
  recoverPendingTransactions(
    projectId: string,
    threadId: string,
    options: {
      store?: StateStore;
      isProcessAlive?: (pid: number) => boolean;
      forceReclaimUnknown?: boolean;
    } = {},
  ): { rolledForward: string[]; aborted: string[]; keptPreparing: string[] } {
    const layout = this.getLayout(projectId, threadId);
    const store = options.store ?? this.openStore(projectId, threadId);
    const ownStore = !options.store;
    const aliveCheck = options.isProcessAlive ?? isPidAlive;
    const rolledForward: string[] = [];
    const aborted: string[] = [];
    const keptPreparing: string[] = [];

    try {
      const pending = store.listPendingTxns();
      for (const row of pending) {
        const nowIso = this.clock.wallIso();
        if (row.state === 'preparing') {
          const bootChanged = row.boot_epoch !== this.clock.bootEpoch();
          const domainChanged = row.clock_domain_id !== this.clock.clockDomainId();
          const uncertain =
            row.lease_state === 'unknown' ||
            (this.clock.isClockContinuityUncertain?.(
              row.clock_domain_id,
              row.boot_epoch,
            ) ?? false);
          const ownerAlive = aliveCheck(row.pid);
          const expired =
            !uncertain &&
            !bootChanged &&
            !domainChanged &&
            BigInt(row.lease_until_mono_ns) <= this.clock.monotonicNs();

          const shouldAbort =
            bootChanged ||
            domainChanged ||
            expired ||
            !ownerAlive ||
            (uncertain && options.forceReclaimUnknown === true);

          if (!shouldAbort) {
            keptPreparing.push(row.txn_id);
            continue;
          }

          store.runImmediate(() => {
            const latest = store.getTxn(row.txn_id);
            if (!latest || latest.state !== 'preparing' || latest.fencing_epoch !== row.fencing_epoch) {
              return;
            }
            // Bump fencing_epoch so stale owner process will fail SQLite CAS if it wakes up
            const newEpoch = store.nextFencingEpoch();
            store.db
              .prepare(
                `UPDATE txn_log SET state = 'aborted', fencing_epoch = ?, updated_at = ? WHERE txn_id = ?`,
              )
              .run(newEpoch, nowIso, row.txn_id);
            store.db.prepare('DELETE FROM stream_leases WHERE txn_id = ?').run(row.txn_id);
          });
          this.cleanupTxnStaging(layout, row.txn_id);
          aborted.push(row.txn_id);
          continue;
        }

        if (row.state === 'committed_pending_publish') {
          const finalCertPath = path.join(layout.txnsCommittedDir, `${row.txn_id}.finalized.json`);
          const committedManifestPath = path.join(layout.txnsCommittedDir, `${row.txn_id}.manifest.json`);

          if (fs.existsSync(finalCertPath) && fs.existsSync(committedManifestPath)) {
            // Certificate already exists (Certificate-as-Commit): strictly verify proof, signature,
            // manifest hash, and object SHA-256 closure BEFORE rolling forward SQLite projection!
            const manifest = JSON.parse(
              fs.readFileSync(committedManifestPath, 'utf8'),
            ) as TransactionManifest;
            const cert = JSON.parse(
              fs.readFileSync(finalCertPath, 'utf8'),
            ) as TransactionFinalizationCertificate;
            const { manifest_hash, ...unsignedManifest } = manifest;
            if (
              cert.txn_id !== row.txn_id ||
              manifest.txn_id !== row.txn_id ||
              manifestDigestPayload(unsignedManifest) !== manifest_hash ||
              cert.manifest_hash !== manifest_hash ||
              cert.commit_intent_proof.manifest_hash !== manifest_hash
            ) {
              throw new Error(
                `Fail-closed recovery abort: corrupted manifest or certificate digest for txn ${row.txn_id}`,
              );
            }
            const trustedKeys = loadTrustedDeviceKeys(this.homeDir).keys;
            const sigCheck = verifyCommitIntentProofSignature(
              cert.commit_intent_proof,
              trustedKeys,
              row.txn_id,
              manifest.project_id,
              manifest.thread_id,
            );
            if (!sigCheck.valid) {
              throw new Error(
                `Fail-closed recovery abort: invalid commit_intent_proof signature for txn ${row.txn_id}: ${sigCheck.reason ?? 'untrusted'}`,
              );
            }
            for (const s of manifest.shards) {
              const sp = path.join(layout.rootDir, ...s.relative_path.split('/'));
              if (!fs.existsSync(sp) || sha256Hex(fs.readFileSync(sp)) !== s.sha256) {
                throw new Error(`Fail-closed recovery abort: missing or corrupted shard ${s.relative_path}`);
              }
            }
            if (manifest.journal_shard) {
              const jp = path.join(layout.rootDir, ...manifest.journal_shard.relative_path.split('/'));
              if (
                !fs.existsSync(jp) ||
                sha256Hex(fs.readFileSync(jp)) !== manifest.journal_shard.sha256
              ) {
                throw new Error(`Fail-closed recovery abort: missing or corrupted journal shard`);
              }
            }
            for (const b of manifest.blobs) {
              if (b.manifest.sync_policy !== 'approved') continue;
              const bp = path.join(layout.rootDir, ...b.relative_path.split('/'));
              if (!fs.existsSync(bp) || sha256Hex(fs.readFileSync(bp)) !== b.manifest.sha256) {
                throw new Error(`Fail-closed recovery abort: missing or corrupted approved blob ${b.manifest.sha256}`);
              }
            }
            store.runImmediate(() => {
              this.applyPublishedTxnToSqlite(store, layout, manifest, nowIso);
            });
            this.cleanupTxnStaging(layout, row.txn_id);
            this.exportLineageJson(store, layout);
            rolledForward.push(row.txn_id);
            continue;
          }

          // Reclaim fencing_epoch & publish remaining staging objects + finalize
          const stagedManifestPath = path.join(layout.txnsStagingDir, `${row.txn_id}.manifest.json`);
          const stagedIntentPath = path.join(layout.txnsStagingDir, `${row.txn_id}.intent.json`);
          if (!fs.existsSync(stagedManifestPath) || !fs.existsSync(stagedIntentPath)) {
            continue;
          }

          const manifest = JSON.parse(
            fs.readFileSync(stagedManifestPath, 'utf8'),
          ) as TransactionManifest;
          const intentProof = JSON.parse(
            fs.readFileSync(stagedIntentPath, 'utf8'),
          ) as CommitIntentProof;

          // Bump epoch atomically before resuming publication/finalization
          const claimed = store.runImmediate(() => {
            const latest = store.getTxn(row.txn_id);
            if (!latest || latest.state !== 'committed_pending_publish') return null;
            const newEpoch = store.nextFencingEpoch();
            store.db
              .prepare(
                `UPDATE txn_log SET fencing_epoch = ?, owner_token = ?, pid = ?, updated_at = ? WHERE txn_id = ?`,
              )
              .run(newEpoch, row.owner_token, process.pid, nowIso, row.txn_id);
            return newEpoch;
          });
          if (claimed === null) continue;

          // Roll forward any missing blobs/shards from staging
          for (const b of manifest.blobs) {
            const st = path.join(layout.blobsStagingDir, row.txn_id, b.manifest.sha256);
            const tgt = path.join(layout.rootDir, ...b.relative_path.split('/'));
            if (fs.existsSync(st) && !fs.existsSync(tgt)) {
              publishImmutable(path.dirname(st), tgt, fs.readFileSync(st), b.manifest.sha256);
            }
          }
          for (const s of manifest.shards) {
            const st = path.join(layout.historyStagingDir, row.txn_id, `${s.stream_id}.jsonl`);
            const tgt = path.join(layout.rootDir, ...s.relative_path.split('/'));
            if (fs.existsSync(st) && !fs.existsSync(tgt)) {
              publishImmutable(path.dirname(st), tgt, fs.readFileSync(st), s.sha256);
            }
          }
          if (manifest.journal_shard) {
            const st = path.join(layout.journalStagingDir, `${row.txn_id}.jsonl`);
            const tgt = path.join(layout.rootDir, ...manifest.journal_shard.relative_path.split('/'));
            if (fs.existsSync(st) && !fs.existsSync(tgt)) {
              publishImmutable(path.dirname(st), tgt, fs.readFileSync(st), manifest.journal_shard.sha256);
            }
          }
          if (!fs.existsSync(committedManifestPath)) {
            publishImmutable(
              layout.txnsStagingDir,
              committedManifestPath,
              fs.readFileSync(stagedManifestPath),
              sha256Hex(fs.readFileSync(stagedManifestPath)),
            );
          }

          const fakeCtx: PreparedTxnContext = {
            txn_id: row.txn_id,
            thread_id: row.thread_id,
            owner_token: row.owner_token,
            fencing_epoch: claimed,
            manifest,
            staged_stream_files: [],
            staged_blobs: [],
            staged_manifest_path: stagedManifestPath,
            target_manifest_rel: path.posix.join(
              'transactions',
              'committed',
              `${row.txn_id}.manifest.json`,
            ),
            commit_intent_proof: intentProof,
            spec: { thread_id: row.thread_id, affected_streams: [] },
          };
          this.finalizeTransaction(store, fakeCtx);
          rolledForward.push(row.txn_id);
        }
      }
      return { rolledForward, aborted, keptPreparing };
    } finally {
      if (ownStore) store.close();
    }
  }

  private cleanupTxnStaging(layout: ThreadDirLayout, txnId: string): void {
    const dirs = [
      path.join(layout.blobsStagingDir, txnId),
      path.join(layout.historyStagingDir, txnId),
    ];
    for (const d of dirs) {
      if (fs.existsSync(d)) {
        fs.rmSync(d, { recursive: true, force: true });
      }
    }
    const files = [
      path.join(layout.journalStagingDir, `${txnId}.jsonl`),
      path.join(layout.txnsStagingDir, `${txnId}.manifest.json`),
      path.join(layout.txnsStagingDir, `${txnId}.intent.json`),
      path.join(layout.txnsStagingDir, `.${txnId}.finalized.tmp`),
    ];
    for (const f of files) {
      if (fs.existsSync(f)) {
        try {
          fs.unlinkSync(f);
        } catch {
          // ignore
        }
      }
    }
  }

  exportLineageJson(store: StateStore, layout: ThreadDirLayout): void {
    const generatedAt = this.clock.wallIso();
    const data = {
      schema_version: SCHEMA_VERSION,
      generated_at: generatedAt,
      threads: store.listThreads(),
      streams: store.listStreams(),
      anchors: store.listAnchors(),
    };
    const tmp = path.join(layout.rootDir, `.lineage.${newId('tmp', 6)}.tmp`);
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2), 'utf8');
    fs.renameSync(tmp, layout.lineageJsonPath);

    const locators = store.listTranscriptLocators();
    const indexData = {
      schema_version: SCHEMA_VERSION,
      generated_at: generatedAt,
      current_machine: {
        hostname: os.hostname(),
        device_id: this.identity.device_id,
      },
      total_locators: locators.length,
      locators,
    };
    const idxTmp = path.join(layout.rootDir, `.transcript-index.${newId('tmp', 6)}.tmp`);
    fs.writeFileSync(idxTmp, JSON.stringify(indexData, null, 2), 'utf8');
    fs.renameSync(idxTmp, layout.transcriptIndexJsonPath);
  }

  /**
   * 4-step `thp fsck` verification and optional full SQLite rebuild from immutable committed files:
   * 1) `commit_intent_proof` signature vs `trusted_device_keys`
   * 2) `.manifest.json` digest match
   * 3) Shards & `approved` blobs SHA-256 closure (marks missing `local_only`/`unscannable_binary` separately)
   * 4) Cross-transaction hard dependency closure (`anchor.parents[]`, Journal predecessors,
   *    dispatch capsules, adopted snapshots, required `input_dependency_manifest` items)
   */
  runFsck(
    projectId: string,
    threadId: string,
    options: {
      rebuildSqlite?: boolean;
      requireTrustedSignature?: boolean;
    } = {},
  ): FsckReport {
    const layout = this.getLayout(projectId, threadId);
    if (!fs.existsSync(layout.rootDir)) {
      throw new Error(`Thread ${threadId} is not initialized in project ${projectId}`);
    }
    this.ensureLayoutReady(layout);
    const trustedRegistry = loadTrustedDeviceKeys(this.homeDir);
    const trustedKeys: TrustedDeviceKey[] = trustedRegistry.keys;
    const requireSig = options.requireTrustedSignature ?? true;

    const committedDir = layout.txnsCommittedDir;
    const entries = fs.existsSync(committedDir) ? fs.readdirSync(committedDir) : [];
    const txnIds = new Set<string>();
    for (const name of entries) {
      if (name.endsWith('.finalized.json')) {
        txnIds.add(name.slice(0, -'.finalized.json'.length));
      } else if (name.endsWith('.manifest.json')) {
        txnIds.add(name.slice(0, -'.manifest.json'.length));
      }
    }

    const txnData = new Map<
      string,
      {
        manifest?: TransactionManifest;
        cert?: TransactionFinalizationCertificate;
        report: FsckTxnReport;
        anchors: LineageAnchor[];
        journalEvents: JournalEventRecord[];
        requiredInputDeps: InputDependencyManifest[];
        snapshotBlobRefs: string[];
      }
    >();

    for (const txnId of [...txnIds].sort()) {
      const certPath = path.join(committedDir, `${txnId}.finalized.json`);
      const manifestPath = path.join(committedDir, `${txnId}.manifest.json`);
      const report: FsckTxnReport = {
        txn_id: txnId,
        status: 'verified',
        missing_local_only_blobs: [],
        affected_streams: [],
      };
      const entry = {
        report,
        anchors: [] as LineageAnchor[],
        journalEvents: [] as JournalEventRecord[],
        requiredInputDeps: [] as InputDependencyManifest[],
        snapshotBlobRefs: [] as string[],
      };

      if (!fs.existsSync(certPath) || !fs.existsSync(manifestPath)) {
        report.status = 'incomplete_unverified_txn';
        report.reason = 'Missing .finalized.json certificate or .manifest.json';
        txnData.set(txnId, entry);
        continue;
      }

      let cert: TransactionFinalizationCertificate;
      let manifest: TransactionManifest;
      try {
        cert = JSON.parse(
          fs.readFileSync(certPath, 'utf8'),
        ) as TransactionFinalizationCertificate;
        manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8')) as TransactionManifest;
      } catch (err) {
        report.status = 'incomplete_unverified_txn';
        report.reason = `Malformed JSON in transaction proof files: ${(err as Error).message}`;
        txnData.set(txnId, entry);
        continue;
      }

      report.affected_streams = [...manifest.affected_streams];

      // Step 1: Verify commit_intent_proof signature against trusted_device_keys
      if (requireSig) {
        const sigCheck = verifyCommitIntentProofSignature(
          cert.commit_intent_proof,
          trustedKeys,
          txnId,
          projectId,
          threadId,
        );
        if (!sigCheck.valid) {
          report.status = 'untrusted_unverified_txn';
          report.reason = sigCheck.reason ?? 'Untrusted or invalid commit_intent_proof';
          txnData.set(txnId, { ...entry, cert, manifest });
          continue;
        }
      }

      // Step 2: Verify manifest_hash binds cert and manifest content
      const { manifest_hash, ...unsignedManifest } = manifest;
      if (
        manifestDigestPayload(unsignedManifest) !== manifest_hash ||
        cert.manifest_hash !== manifest_hash ||
        cert.commit_intent_proof.manifest_hash !== manifest_hash
      ) {
        report.status = 'incomplete_unverified_txn';
        report.reason = 'Manifest digest mismatch against Finalization Certificate';
        txnData.set(txnId, { ...entry, cert, manifest });
        continue;
      }

      // Step 3: Verify shards, journal shard, and blobs
      let incompleteReason: string | undefined;
      for (const s of manifest.shards) {
        const sp = path.join(layout.rootDir, ...s.relative_path.split('/'));
        if (!fs.existsSync(sp)) {
          incompleteReason = `Missing shard ${s.relative_path}`;
          break;
        }
        const bytes = fs.readFileSync(sp);
        if (sha256Hex(bytes) !== s.sha256) {
          incompleteReason = `SHA-256 mismatch on shard ${s.relative_path}`;
          break;
        }
        const lines = bytes
          .toString('utf8')
          .split(/\r?\n/)
          .filter((l) => l.trim().length > 0)
          .map((l) => JSON.parse(l) as StreamShardLine);
        for (const rec of lines) {
          if (rec.kind === 'control') {
            if (rec.payload.type === 'anchor') {
              entry.anchors.push(rec.payload.anchor);
            } else if (rec.payload.type === 'input_dependency_manifest') {
              entry.requiredInputDeps.push(rec.payload.manifest);
            } else if (rec.payload.type === 'dispatch_recovery_capsule') {
              entry.snapshotBlobRefs.push(rec.payload.capsule.coordinator_snapshot_ref);
            } else if (rec.payload.type === 'adopted_source_manifest') {
              for (const src of rec.payload.manifest.sources) {
                entry.snapshotBlobRefs.push(src.snapshot_blob_ref);
              }
            }
          }
        }
      }

      if (!incompleteReason && manifest.journal_shard) {
        const jp = path.join(layout.rootDir, ...manifest.journal_shard.relative_path.split('/'));
        if (!fs.existsSync(jp)) {
          incompleteReason = `Missing journal shard ${manifest.journal_shard.relative_path}`;
        } else {
          const jBytes = fs.readFileSync(jp);
          if (sha256Hex(jBytes) !== manifest.journal_shard.sha256) {
            incompleteReason = `SHA-256 mismatch on journal shard`;
          } else {
            for (const l of jBytes.toString('utf8').split(/\r?\n/)) {
              if (!l.trim()) continue;
              entry.journalEvents.push(JSON.parse(l) as JournalEventRecord);
            }
          }
        }
      }

      if (!incompleteReason) {
        for (const b of manifest.blobs) {
          const bp = path.join(layout.rootDir, ...b.relative_path.split('/'));
          if (!fs.existsSync(bp)) {
            if (
              b.manifest.sync_policy === 'local_only' ||
              b.manifest.sync_policy === 'unscannable_binary'
            ) {
              report.missing_local_only_blobs.push(b.manifest.sha256);
            } else {
              incompleteReason = `Missing approved blob ${b.manifest.sha256}`;
              break;
            }
          } else if (sha256Hex(fs.readFileSync(bp)) !== b.manifest.sha256) {
            incompleteReason = `SHA-256 mismatch on blob ${b.manifest.sha256}`;
            break;
          }
        }
      }

      if (incompleteReason) {
        report.status = 'incomplete_unverified_txn';
        report.reason = incompleteReason;
      }
      txnData.set(txnId, { ...entry, cert, manifest });
    }

    // Step 4: Cross-transaction dependency closure check over Step 1-3 verified txns
    const verifiedAnchorToTxn = new Map<string, string>();
    const verifiedJournalToTxn = new Map<string, string>();
    const verifiedBlobSet = new Set<string>();
    const localOnlyBlobSet = new Set<string>();
    const verifiedAnchorById = new Map<string, LineageAnchor>();

    for (const [txnId, data] of txnData.entries()) {
      if (data.report.status !== 'verified' || !data.manifest) continue;
      for (const a of data.anchors) {
        verifiedAnchorToTxn.set(a.anchor_id, txnId);
        verifiedAnchorById.set(a.anchor_id, a);
      }
      for (const je of data.journalEvents) {
        verifiedJournalToTxn.set(je.journal_event_id, txnId);
      }
      for (const b of data.manifest.blobs) {
        const bp = path.join(layout.rootDir, ...b.relative_path.split('/'));
        if (fs.existsSync(bp)) {
          verifiedBlobSet.add(b.manifest.sha256);
        } else if (
          b.manifest.sync_policy === 'local_only' ||
          b.manifest.sync_policy === 'unscannable_binary'
        ) {
          localOnlyBlobSet.add(b.manifest.sha256);
        }
      }
    }
    // Also scan physical blobs/objects directory for pre-existing verified content-addressed objects
    if (fs.existsSync(layout.blobsObjectsDir)) {
      for (const prefix of fs.readdirSync(layout.blobsObjectsDir)) {
        const sub = path.join(layout.blobsObjectsDir, prefix);
        if (!fs.statSync(sub).isDirectory()) continue;
        for (const hashFile of fs.readdirSync(sub)) {
          const full = path.join(sub, hashFile);
          if (sha256Hex(fs.readFileSync(full)) === hashFile) {
            verifiedBlobSet.add(hashFile);
          }
        }
      }
    }

    // Iteratively propagate closure failures
    let closureChanged = true;
    while (closureChanged) {
      closureChanged = false;
      for (const [txnId, data] of txnData.entries()) {
        if (data.report.status !== 'verified') continue;
        let brokenReason: string | undefined;

        for (const a of data.anchors) {
          for (const parentId of a.parents) {
            const pTxn = verifiedAnchorToTxn.get(parentId);
            if (!pTxn || txnData.get(pTxn)?.report.status !== 'verified') {
              brokenReason = `Missing or unverified parent anchor ${parentId} required by ${a.anchor_id}`;
              break;
            }
          }
          if (brokenReason) break;
        }

        if (!brokenReason) {
          for (const je of data.journalEvents) {
            for (const predId of je.causal_predecessors) {
              const pTxn = verifiedJournalToTxn.get(predId);
              if (!pTxn || txnData.get(pTxn)?.report.status !== 'verified') {
                brokenReason = `Missing or unverified Journal predecessor ${predId}`;
                break;
              }
            }
            if (brokenReason) break;
          }
        }

        if (!brokenReason) {
          for (const snapSha of data.snapshotBlobRefs) {
            if (!verifiedBlobSet.has(snapSha) && !localOnlyBlobSet.has(snapSha)) {
              brokenReason = `Missing required snapshot/capsule blob ${snapSha}`;
              break;
            }
          }
        }

        if (!brokenReason) {
          for (const idm of data.requiredInputDeps) {
            for (const item of idm.items) {
              if (!item.required_for_recovery) continue;
              if (item.ref_type === 'anchor') {
                const pTxn = verifiedAnchorToTxn.get(item.target_id);
                if (!pTxn || txnData.get(pTxn)?.report.status !== 'verified') {
                  brokenReason = `Missing required input_dependency anchor ${item.target_id}`;
                  break;
                }
              } else if (item.ref_type === 'journal_event') {
                const pTxn = verifiedJournalToTxn.get(item.target_id);
                if (!pTxn || txnData.get(pTxn)?.report.status !== 'verified') {
                  brokenReason = `Missing required input_dependency journal event ${item.target_id}`;
                  break;
                }
              } else if (item.ref_type === 'blob' || item.ref_type === 'artifact') {
                if (
                  !verifiedBlobSet.has(item.sha256) &&
                  !localOnlyBlobSet.has(item.sha256)
                ) {
                  brokenReason = `Missing required input_dependency ${item.ref_type} (${item.sha256})`;
                  break;
                }
              }
            }
            if (brokenReason) break;
          }
        }

        if (brokenReason) {
          data.report.status = 'broken_dependency_closure';
          data.report.reason = brokenReason;
          closureChanged = true;
        }
      }
    }

    // Compute causal topological ordering (never rely on wall-clock created_at!) & detect divergent heads
    const streamHealth: Record<string, StreamProjectionHealth> = {};
    const verifiedTxnsRaw = [...txnData.values()]
      .filter((d) => d.report.status === 'verified' && d.manifest)
      .sort((a, b) => a.report.txn_id.localeCompare(b.report.txn_id));

    const orderedTxns: typeof verifiedTxnsRaw = [];
    const placedTxns = new Set<string>();
    const remaining = [...verifiedTxnsRaw];
    while (remaining.length > 0) {
      let progressed = false;
      for (let i = 0; i < remaining.length; i++) {
        const cand = remaining[i];
        const parentDepsReady = cand.anchors.every((a) =>
          a.parents.every((p) => {
            const pt = verifiedAnchorToTxn.get(p);
            return !pt || pt === cand.report.txn_id || placedTxns.has(pt);
          }),
        );
        const journalDepsReady = cand.journalEvents.every((je) =>
          je.causal_predecessors.every((p) => {
            const pt = verifiedJournalToTxn.get(p);
            return !pt || pt === cand.report.txn_id || placedTxns.has(pt);
          }),
        );
        if (parentDepsReady && journalDepsReady) {
          orderedTxns.push(cand);
          placedTxns.add(cand.report.txn_id);
          remaining.splice(i, 1);
          progressed = true;
          break;
        }
      }
      if (!progressed) {
        // Fallback deterministic order if cycle/external edge exists
        const next = remaining.shift()!;
        orderedTxns.push(next);
        placedTxns.add(next.report.txn_id);
      }
    }
    const verifiedTxns = orderedTxns;

    // Detect per-stream divergent heads (leaf anchors in same stream_id)
    const anchorsByStream = new Map<string, LineageAnchor[]>();
    for (const d of verifiedTxns) {
      for (const a of d.anchors) {
        const arr = anchorsByStream.get(a.stream_id) ?? [];
        arr.push(a);
        anchorsByStream.set(a.stream_id, arr);
      }
    }
    const divergentStreamHeads: Record<string, string[]> = {};
    const divergentHeadsMap = new Map<string, string[]>();
    for (const [sid, list] of anchorsByStream.entries()) {
      const sameStreamParents = new Set<string>();
      for (const a of list) {
        for (const p of a.parents) {
          sameStreamParents.add(p);
        }
      }
      const leafHeads = list
        .filter((a) => !sameStreamParents.has(a.anchor_id))
        .map((a) => a.anchor_id)
        .sort();
      if (leafHeads.length > 1) {
        divergentStreamHeads[sid] = leafHeads;
        divergentHeadsMap.set(sid, leafHeads);
      }
    }

    for (const d of txnData.values()) {
      for (const sid of d.report.affected_streams) {
        if (d.report.status !== 'verified') {
          streamHealth[sid] = 'quarantined';
        }
      }
    }
    for (const d of verifiedTxns) {
      const hasLocalOnlyMissing =
        d.report.missing_local_only_blobs.length > 0 ||
        d.requiredInputDeps.some((idm) =>
          idm.items.some((it) => localOnlyBlobSet.has(it.sha256)),
        );
      for (const sid of d.report.affected_streams) {
        if (streamHealth[sid] === 'quarantined') continue;
        if (divergentHeadsMap.has(sid)) {
          streamHealth[sid] = 'divergent_heads';
        } else {
          streamHealth[sid] = hasLocalOnlyMissing
            ? 'partial_blobs_local_only'
            : streamHealth[sid] ?? 'complete';
        }
      }
    }

    // Evaluate Journal CALM conflicts & offline Coordinator Journal DAG divergence
    const allVerifiedJournal = verifiedTxns.flatMap((d) => d.journalEvents);
    const jProj = projectJournalState(threadId, allVerifiedJournal);
    const referencedJournalPreds = new Set<string>();
    for (const je of allVerifiedJournal) {
      for (const predId of je.causal_predecessors) {
        referencedJournalPreds.add(predId);
      }
    }
    const journalLeafHeads = allVerifiedJournal
      .filter((je) => !referencedJournalPreds.has(je.journal_event_id))
      .map((je) => je.journal_event_id)
      .sort();
    const divergentJournalHeads = journalLeafHeads.length > 1 ? journalLeafHeads : [];

    if (options.rebuildSqlite) {
      for (const suffix of ['', '-wal', '-shm']) {
        const f = `${layout.dbPath}${suffix}`;
        if (fs.existsSync(f)) fs.unlinkSync(f);
      }
      const rebuiltStore = this.openStore(projectId, threadId);
      try {
        const healthMap = new Map(Object.entries(streamHealth));
        rebuiltStore.runImmediate(() => {
          const nowIso = this.clock.wallIso();
          for (const d of verifiedTxns) {
            this.applyPublishedTxnToSqlite(
              rebuiltStore,
              layout,
              d.manifest!,
              nowIso,
              healthMap,
              divergentHeadsMap,
            );
          }
          if (divergentJournalHeads.length > 1) {
            rebuiltStore.db
              .prepare(`UPDATE threads SET journal_head_id = NULL WHERE thread_id = ?`)
              .run(threadId);
          }
          for (const d of txnData.values()) {
            if (d.report.status !== 'verified') {
              rebuiltStore.db
                .prepare(
                  `INSERT OR REPLACE INTO fsck_quarantine(txn_id, status, reason, detected_at) VALUES (?, ?, ?, ?)`,
                )
                .run(d.report.txn_id, d.report.status, d.report.reason ?? 'quarantined', nowIso);
            }
          }
        });
        this.exportLineageJson(rebuiltStore, layout);
      } finally {
        rebuiltStore.close();
      }
    }

    const txnReports = [...txnData.values()].map((d) => d.report);
    const hasQuarantine = txnReports.some((r) => r.status !== 'verified');
    const hasDivergentHeads = Object.keys(divergentStreamHeads).length > 0;
    const hasDivergentJournal = divergentJournalHeads.length > 1;
    return {
      thread_id: threadId,
      ok: !hasQuarantine && !hasDivergentHeads && !hasDivergentJournal && !jProj.has_blocking_conflict,
      rebuilt: Boolean(options.rebuildSqlite),
      txn_reports: txnReports,
      stream_health: streamHealth,
      divergent_stream_heads: divergentStreamHeads,
      divergent_journal_heads: divergentJournalHeads,
      journal_conflicts: jProj.conflicts,
    };
  }

  /**
   * Reads all committed immutable shard lines for a stream (or across streams)
   * filtered by verified `.finalized.json` transactions.
   */
  readCommittedStreamLines(
    projectId: string,
    threadId: string,
    streamId?: string,
  ): StreamShardLine[] {
    const layout = this.getLayout(projectId, threadId);
    if (!fs.existsSync(layout.txnsCommittedDir)) return [];
    const entries = fs.readdirSync(layout.txnsCommittedDir);
    const parsedTxns: Array<{
      manifest: TransactionManifest;
      shardLinesByStream: Map<string, StreamShardLine[]>;
      anchors: LineageAnchor[];
    }> = [];
    const anchorToTxn = new Map<string, string>();

    for (const f of entries) {
      if (!f.endsWith('.manifest.json')) continue;
      const txnId = f.slice(0, -'.manifest.json'.length);
      if (!fs.existsSync(path.join(layout.txnsCommittedDir, `${txnId}.finalized.json`))) {
        continue; // Never expose uncommitted/unfinalized shards as active context
      }
      const manifest = JSON.parse(
        fs.readFileSync(path.join(layout.txnsCommittedDir, f), 'utf8'),
      ) as TransactionManifest;
      const shardLinesByStream = new Map<string, StreamShardLine[]>();
      const anchors: LineageAnchor[] = [];
      for (const s of manifest.shards) {
        if (streamId && s.stream_id !== streamId) continue;
        const sp = path.join(layout.rootDir, ...s.relative_path.split('/'));
        if (!fs.existsSync(sp)) continue;
        const shardLines: StreamShardLine[] = [];
        for (const row of fs.readFileSync(sp, 'utf8').split(/\r?\n/)) {
          if (!row.trim()) continue;
          const parsed = JSON.parse(row) as StreamShardLine;
          shardLines.push(parsed);
          if (parsed.kind === 'control' && parsed.payload.type === 'anchor') {
            anchors.push(parsed.payload.anchor);
            anchorToTxn.set(parsed.payload.anchor.anchor_id, manifest.txn_id);
          }
        }
        shardLinesByStream.set(s.stream_id ?? '', shardLines);
      }
      if (shardLinesByStream.size > 0) {
        parsedTxns.push({ manifest, shardLinesByStream, anchors });
      }
    }

    // Order causally by anchor.parents[] first (never blindly trust wall-clock created_at across devices/same-ms runs),
    // breaking incomparable ties deterministically by min shard.seq, created_at, and txn_id.
    const minShardSeq = (m: TransactionManifest) => {
      const seqs = m.shards
        .filter((s) => !streamId || s.stream_id === streamId)
        .map((s) => s.seq ?? Number.MAX_SAFE_INTEGER);
      return seqs.length > 0 ? Math.min(...seqs) : Number.MAX_SAFE_INTEGER;
    };
    parsedTxns.sort((a, b) => {
      const seqDiff = minShardSeq(a.manifest) - minShardSeq(b.manifest);
      if (seqDiff !== 0) return seqDiff;
      const timeDiff = a.manifest.created_at.localeCompare(b.manifest.created_at);
      if (timeDiff !== 0) return timeDiff;
      return a.manifest.txn_id.localeCompare(b.manifest.txn_id);
    });

    const ordered: typeof parsedTxns = [];
    const placed = new Set<string>();
    const remaining = [...parsedTxns];
    while (remaining.length > 0) {
      let progressed = false;
      for (let i = 0; i < remaining.length; i++) {
        const cand = remaining[i];
        const depsReady = cand.anchors.every((a) =>
          a.parents.every((p) => {
            const pt = anchorToTxn.get(p);
            return !pt || pt === cand.manifest.txn_id || placed.has(pt);
          }),
        );
        if (depsReady) {
          ordered.push(cand);
          placed.add(cand.manifest.txn_id);
          remaining.splice(i, 1);
          progressed = true;
          break;
        }
      }
      if (!progressed) {
        const next = remaining.shift()!;
        ordered.push(next);
        placed.add(next.manifest.txn_id);
      }
    }

    const lines: StreamShardLine[] = [];
    for (const item of ordered) {
      for (const s of item.manifest.shards) {
        if (streamId && s.stream_id !== streamId) continue;
        const sl = item.shardLinesByStream.get(s.stream_id ?? '');
        if (sl) lines.push(...sl);
      }
    }
    return lines;
  }

  /**
   * Collects only finalized, sync_policy='approved' objects into an offline sync descriptor.
   */
  collectSyncableWhitelist(projectId: string, threadId: string): {
    files: string[];
    excludedLocalOnlyBlobs: string[];
    excludedUnfinalizedTxns: string[];
  } {
    const layout = this.getLayout(projectId, threadId);
    const files: string[] = [];
    const excludedLocalOnlyBlobs: string[] = [];
    const excludedUnfinalizedTxns: string[] = [];

    if (!fs.existsSync(layout.txnsCommittedDir)) {
      return { files, excludedLocalOnlyBlobs, excludedUnfinalizedTxns };
    }
    for (const f of fs.readdirSync(layout.txnsCommittedDir).sort()) {
      if (!f.endsWith('.manifest.json')) continue;
      const txnId = f.slice(0, -'.manifest.json'.length);
      const certPath = path.join(layout.txnsCommittedDir, `${txnId}.finalized.json`);
      if (!fs.existsSync(certPath)) {
        excludedUnfinalizedTxns.push(txnId);
        continue;
      }
      const manifest = JSON.parse(
        fs.readFileSync(path.join(layout.txnsCommittedDir, f), 'utf8'),
      ) as TransactionManifest;
      files.push(path.posix.join('transactions', 'committed', `${txnId}.manifest.json`));
      files.push(path.posix.join('transactions', 'committed', `${txnId}.finalized.json`));
      for (const s of manifest.shards) files.push(s.relative_path);
      if (manifest.journal_shard) files.push(manifest.journal_shard.relative_path);
      for (const b of manifest.blobs) {
        if (b.manifest.sync_policy === 'approved') {
          files.push(b.relative_path);
        } else {
          excludedLocalOnlyBlobs.push(b.manifest.sha256);
        }
      }
    }
    return {
      files: [...new Set(files)],
      excludedLocalOnlyBlobs: [...new Set(excludedLocalOnlyBlobs)],
      excludedUnfinalizedTxns,
    };
  }

  /**
   * Reference- and lease-protected Blob garbage collector.
   * Never deletes objects referenced by published blobs, `committed_pending_publish` txns,
   * or live `preparing` leases. Never deletes from staging directly.
   */
  gcOrphanBlobs(
    projectId: string,
    threadId: string,
    options: { gracePeriodMs?: number } = {},
  ): { deleted: string[]; retained: string[] } {
    const layout = this.getLayout(projectId, threadId);
    const store = this.openExistingStore(projectId, threadId);
    const deleted: string[] = [];
    const retained: string[] = [];
    const graceMs = options.gracePeriodMs ?? 0;

    try {
      const protectedHashes = new Set<string>();
      store.runImmediate(() => {
        // 1. Committed/published references
        const rows = store.db
          .prepare('SELECT sha256, ref_count FROM blobs_index WHERE ref_count > 0')
          .all() as Array<{ sha256: string; ref_count: number }>;
        for (const r of rows) protectedHashes.add(r.sha256);

        // 2. Pending transactions (committed_pending_publish or live preparing leases)
        const pending = store.listPendingTxns();
        for (const t of pending) {
          const mPath = path.join(layout.txnsStagingDir, `${t.txn_id}.manifest.json`);
          if (fs.existsSync(mPath)) {
            const m = JSON.parse(fs.readFileSync(mPath, 'utf8')) as TransactionManifest;
            for (const b of m.blobs) protectedHashes.add(b.manifest.sha256);
          }
          const bDir = path.join(layout.blobsStagingDir, t.txn_id);
          if (fs.existsSync(bDir)) {
            for (const fname of fs.readdirSync(bDir)) {
              protectedHashes.add(fname);
            }
          }
        }
      });

      if (fs.existsSync(layout.blobsObjectsDir)) {
        const nowMs = Date.now();
        for (const prefix of fs.readdirSync(layout.blobsObjectsDir)) {
          const sub = path.join(layout.blobsObjectsDir, prefix);
          if (!fs.statSync(sub).isDirectory()) continue;
          for (const sha of fs.readdirSync(sub)) {
            const fullPath = path.join(sub, sha);
            if (protectedHashes.has(sha)) {
              retained.push(sha);
              continue;
            }
            const stat = fs.statSync(fullPath);
            if (nowMs - stat.mtimeMs < graceMs) {
              retained.push(sha);
              continue;
            }
            // Double-check inside SQLite before deleting
            const stillReferenced = store.runImmediate(() => {
              const row = store.db
                .prepare('SELECT ref_count FROM blobs_index WHERE sha256 = ?')
                .get(sha) as { ref_count: number } | undefined;
              return (row?.ref_count ?? 0) > 0;
            });
            if (stillReferenced) {
              retained.push(sha);
              continue;
            }
            fs.unlinkSync(fullPath);
            deleted.push(sha);
          }
        }
      }
      return { deleted, retained };
    } finally {
      store.close();
    }
  }
}
