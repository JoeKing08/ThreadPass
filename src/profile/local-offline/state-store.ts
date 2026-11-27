import os from 'node:os';
import { DatabaseSync } from 'node:sqlite';
import { sha256Hex } from '../../core/canonical.js';
import type { ClockPort } from '../../core/ports.js';
import type {
  ConfirmationProof,
  FsckTxnStatus,
  HostTranscriptLocator,
  JournalEventRecord,
  LeaseInfo,
  LeaseState,
  LineageAnchor,
  StreamProjectionHealth,
} from '../../core/types.js';

export function computeTranscriptLocatorKey(loc: HostTranscriptLocator): string {
  const sid = loc.stream_id;
  if (loc.host_kind === 'local_file' && loc.file_path) {
    const normPath = loc.file_path.trim().replace(/\\/g, '/');
    const origin = loc.locator_origin ?? 'operator_verified';
    return sha256Hex(
      `local|${sid}|${origin}|${(loc.machine_device_id ?? '').trim()}|${(loc.machine_hostname ?? '').trim()}|${normPath}`,
    );
  }
  if (loc.host_kind === 'web_uploaded_blob' && loc.uploaded_blob_sha256) {
    return sha256Hex(`blob|${sid}|${loc.uploaded_blob_sha256}`);
  }
  if (loc.host_kind === 'cloud_remote') {
    const origin = loc.locator_origin ?? 'operator_verified';
    const ident = (loc.host_session_id ?? loc.remote_uri ?? loc.stream_id).trim();
    return sha256Hex(`cloud|${sid}|${origin}|${(loc.cloud_provider ?? '').trim()}|${ident}`);
  }
  return sha256Hex(`placeholder|${sid}|${loc.source_label}`);
}

export class SystemClock implements ClockPort {
  private readonly domainId: string;
  private readonly bootEpochId: string;

  constructor(clockDomainId?: string, bootEpoch?: string) {
    this.domainId =
      clockDomainId ??
      `clk_${sha256Hex(`${os.hostname()}|${os.platform()}|${os.arch()}`).slice(0, 12)}`;
    // Quantize approximate OS boot wall-time to 30s bucket so normal jitter keeps same boot_epoch
    // while genuine reboots produce a distinct epoch.
    const approxBootBucket = Math.floor((Date.now() - os.uptime() * 1000) / 30000);
    this.bootEpochId = bootEpoch ?? `boot_${approxBootBucket}`;
  }

  clockDomainId(): string {
    return this.domainId;
  }

  bootEpoch(): string {
    return this.bootEpochId;
  }

  monotonicNs(): bigint {
    return process.hrtime.bigint();
  }

  wallIso(): string {
    return new Date().toISOString();
  }

  isClockContinuityUncertain(
    recordedClockDomainId: string,
    recordedBootEpoch: string,
  ): boolean {
    return (
      recordedClockDomainId !== this.domainId ||
      recordedBootEpoch !== this.bootEpochId
    );
  }
}

export interface StreamRow {
  stream_id: string;
  thread_id: string;
  head_anchor_id: string | null;
  last_seq: number;
  health: StreamProjectionHealth;
  is_main: number;
  created_at: string;
}

export interface TxnLogRow {
  txn_id: string;
  thread_id: string;
  state: 'preparing' | 'committed_pending_publish' | 'published' | 'aborted';
  owner_token: string;
  fencing_epoch: number;
  clock_domain_id: string;
  boot_epoch: string;
  lease_until_mono_ns: string;
  lease_state: LeaseState;
  pid: number;
  manifest_hash: string | null;
  affected_streams_json: string;
  expected_parent_heads_json: string;
  expected_journal_head: string | null;
  created_at: string;
  updated_at: string;
}

export class StateStore {
  public readonly db: DatabaseSync;

  constructor(public readonly dbPath: string) {
    this.db = new DatabaseSync(dbPath);
    this.db.exec('PRAGMA journal_mode = WAL;');
    this.db.exec('PRAGMA synchronous = FULL;');
    this.db.exec('PRAGMA busy_timeout = 5000;');
    this.db.exec('PRAGMA foreign_keys = ON;');
    this.migrate();
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS threads (
        thread_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        main_stream_id TEXT NOT NULL,
        coordinator_epoch INTEGER NOT NULL DEFAULT 1,
        journal_head_id TEXT,
        journal_last_seq INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS streams (
        stream_id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        head_anchor_id TEXT,
        last_seq INTEGER NOT NULL DEFAULT 0,
        health TEXT NOT NULL DEFAULT 'complete',
        is_main INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS anchors (
        anchor_id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        stream_id TEXT NOT NULL,
        commit_txn_id TEXT NOT NULL,
        parents_json TEXT NOT NULL,
        anchor_json TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS lineage_edges (
        child_anchor_id TEXT NOT NULL,
        parent_anchor_id TEXT NOT NULL,
        PRIMARY KEY (child_anchor_id, parent_anchor_id)
      );

      CREATE TABLE IF NOT EXISTS txn_log (
        txn_id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        state TEXT NOT NULL,
        owner_token TEXT NOT NULL,
        fencing_epoch INTEGER NOT NULL,
        clock_domain_id TEXT NOT NULL,
        boot_epoch TEXT NOT NULL,
        lease_until_mono_ns TEXT NOT NULL,
        lease_state TEXT NOT NULL DEFAULT 'active',
        pid INTEGER NOT NULL,
        manifest_hash TEXT,
        affected_streams_json TEXT NOT NULL,
        expected_parent_heads_json TEXT NOT NULL,
        expected_journal_head TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS stream_leases (
        stream_id TEXT PRIMARY KEY,
        txn_id TEXT NOT NULL,
        owner_token TEXT NOT NULL,
        fencing_epoch INTEGER NOT NULL,
        clock_domain_id TEXT NOT NULL,
        boot_epoch TEXT NOT NULL,
        lease_until_mono_ns TEXT NOT NULL,
        lease_state TEXT NOT NULL DEFAULT 'active',
        pid INTEGER NOT NULL
      );

      CREATE TABLE IF NOT EXISTS journal_events_proj (
        journal_event_id TEXT PRIMARY KEY,
        thread_id TEXT NOT NULL,
        main_stream_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        actor_stream_id TEXT NOT NULL,
        commit_txn_id TEXT NOT NULL,
        coordinator_epoch INTEGER NOT NULL,
        seq INTEGER NOT NULL,
        event_json TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS mailbox_proj (
        message_id TEXT PRIMARY KEY,
        from_stream_id TEXT NOT NULL,
        target_stream_id TEXT NOT NULL,
        status TEXT NOT NULL,
        batch_id TEXT,
        lease_until_mono_ns TEXT,
        payload_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS blobs_index (
        sha256 TEXT PRIMARY KEY,
        byte_length INTEGER NOT NULL,
        sync_policy TEXT NOT NULL,
        manifest_json TEXT NOT NULL,
        ref_count INTEGER NOT NULL DEFAULT 1,
        orphan_since TEXT
      );

      CREATE TABLE IF NOT EXISTS host_bindings (
        host_session_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        stream_id TEXT NOT NULL,
        bound_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS event_fingerprints (
        event_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        stream_id TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS fsck_quarantine (
        txn_id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        reason TEXT NOT NULL,
        detected_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS transcript_locators (
        locator_key TEXT PRIMARY KEY,
        locator_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        stream_id TEXT NOT NULL,
        anchor_id TEXT,
        source_label TEXT NOT NULL,
        host_kind TEXT NOT NULL,
        machine_hostname TEXT,
        machine_device_id TEXT,
        file_path TEXT,
        cloud_provider TEXT,
        host_session_id TEXT,
        remote_uri TEXT,
        uploaded_blob_sha256 TEXT,
        uploaded_blob_rel TEXT,
        uploaded_filename TEXT,
        locator_json TEXT NOT NULL,
        recorded_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS consumed_confirmation_nonces (
        nonce TEXT PRIMARY KEY,
        content_sha256 TEXT NOT NULL,
        purpose TEXT NOT NULL,
        channel TEXT NOT NULL,
        consumed_at TEXT NOT NULL
      );
    `);
  }

  close(): void {
    this.db.close();
  }

  runImmediate<T>(fn: () => T): T {
    this.db.exec('BEGIN IMMEDIATE;');
    try {
      const res = fn();
      this.db.exec('COMMIT;');
      return res;
    } catch (err) {
      try {
        this.db.exec('ROLLBACK;');
      } catch {
        // ignore rollback error
      }
      throw err;
    }
  }

  nextFencingEpoch(): number {
    const row = this.db
      .prepare("SELECT value FROM meta WHERE key = 'global_fencing_epoch'")
      .get() as { value: string } | undefined;
    const next = row ? Number(row.value) + 1 : 1;
    this.db
      .prepare(
        "INSERT INTO meta(key, value) VALUES ('global_fencing_epoch', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
      )
      .run(String(next));
    return next;
  }

  getThread(threadId: string): {
    thread_id: string;
    project_id: string;
    main_stream_id: string;
    coordinator_epoch: number;
    journal_head_id: string | null;
    journal_last_seq: number;
  } | null {
    const row = this.db
      .prepare('SELECT * FROM threads WHERE thread_id = ?')
      .get(threadId) as
      | {
          thread_id: string;
          project_id: string;
          main_stream_id: string;
          coordinator_epoch: number;
          journal_head_id: string | null;
          journal_last_seq: number;
        }
      | undefined;
    return row ?? null;
  }

  listThreads(): Array<{
    thread_id: string;
    project_id: string;
    main_stream_id: string;
    coordinator_epoch: number;
    journal_head_id: string | null;
    journal_last_seq: number;
  }> {
    return this.db.prepare('SELECT * FROM threads ORDER BY created_at').all() as Array<{
      thread_id: string;
      project_id: string;
      main_stream_id: string;
      coordinator_epoch: number;
      journal_head_id: string | null;
      journal_last_seq: number;
    }>;
  }

  getStream(streamId: string): StreamRow | null {
    const row = this.db
      .prepare('SELECT * FROM streams WHERE stream_id = ?')
      .get(streamId) as StreamRow | undefined;
    return row ?? null;
  }

  listStreams(threadId?: string): StreamRow[] {
    if (threadId) {
      return this.db
        .prepare('SELECT * FROM streams WHERE thread_id = ? ORDER BY created_at')
        .all(threadId) as unknown as StreamRow[];
    }
    return this.db
      .prepare('SELECT * FROM streams ORDER BY created_at')
      .all() as unknown as StreamRow[];
  }

  getAnchor(anchorId: string): LineageAnchor | null {
    const row = this.db
      .prepare('SELECT anchor_json FROM anchors WHERE anchor_id = ?')
      .get(anchorId) as { anchor_json: string } | undefined;
    return row ? (JSON.parse(row.anchor_json) as LineageAnchor) : null;
  }

  listAnchors(threadId?: string): LineageAnchor[] {
    const rows = (threadId
      ? this.db
          .prepare('SELECT anchor_json FROM anchors WHERE thread_id = ? ORDER BY created_at')
          .all(threadId)
      : this.db.prepare('SELECT anchor_json FROM anchors ORDER BY created_at').all()) as Array<{
      anchor_json: string;
    }>;
    return rows.map((r) => JSON.parse(r.anchor_json) as LineageAnchor);
  }

  getTxn(txnId: string): TxnLogRow | null {
    const row = this.db
      .prepare('SELECT * FROM txn_log WHERE txn_id = ?')
      .get(txnId) as TxnLogRow | undefined;
    return row ?? null;
  }

  listPendingTxns(): TxnLogRow[] {
    return this.db
      .prepare(
        "SELECT * FROM txn_log WHERE state IN ('preparing', 'committed_pending_publish') ORDER BY created_at",
      )
      .all() as unknown as TxnLogRow[];
  }

  listJournalEvents(threadId: string): JournalEventRecord[] {
    const rows = this.db
      .prepare(
        'SELECT event_json FROM journal_events_proj WHERE thread_id = ? ORDER BY seq ASC',
      )
      .all(threadId) as Array<{ event_json: string }>;
    return rows.map((r) => JSON.parse(r.event_json) as JournalEventRecord);
  }

  lookupHostBinding(hostSessionId: string): {
    host_session_id: string;
    project_id: string;
    thread_id: string;
    stream_id: string;
  } | null {
    const row = this.db
      .prepare('SELECT * FROM host_bindings WHERE host_session_id = ?')
      .get(hostSessionId) as
      | {
          host_session_id: string;
          project_id: string;
          thread_id: string;
          stream_id: string;
        }
      | undefined;
    return row ?? null;
  }

  lookupEventFingerprints(eventIds: readonly string[]): Array<{
    event_id: string;
    project_id: string;
    thread_id: string;
    stream_id: string;
  }> {
    if (eventIds.length === 0) return [];
    const results: Array<{
      event_id: string;
      project_id: string;
      thread_id: string;
      stream_id: string;
    }> = [];
    const stmt = this.db.prepare(
      'SELECT event_id, project_id, thread_id, stream_id FROM event_fingerprints WHERE event_id = ?',
    );
    for (const id of eventIds) {
      const row = stmt.get(id) as
        | {
            event_id: string;
            project_id: string;
            thread_id: string;
            stream_id: string;
          }
        | undefined;
      if (row) results.push(row);
    }
    return results;
  }

  markLeaseUnknown(txnId: string, nowIso: string): void {
    this.runImmediate(() => {
      this.db
        .prepare("UPDATE txn_log SET lease_state = 'unknown', updated_at = ? WHERE txn_id = ?")
        .run(nowIso, txnId);
      this.db
        .prepare("UPDATE stream_leases SET lease_state = 'unknown' WHERE txn_id = ?")
        .run(txnId);
    });
  }

  listQuarantined(): Array<{
    txn_id: string;
    status: FsckTxnStatus;
    reason: string;
    detected_at: string;
  }> {
    return this.db.prepare('SELECT * FROM fsck_quarantine').all() as Array<{
      txn_id: string;
      status: FsckTxnStatus;
      reason: string;
      detected_at: string;
    }>;
  }

  getLeaseInfo(txnId: string): LeaseInfo | null {
    const row = this.getTxn(txnId);
    if (!row) return null;
    return {
      txn_id: row.txn_id,
      owner_token: row.owner_token,
      clock_domain_id: row.clock_domain_id,
      boot_epoch: row.boot_epoch,
      lease_until_mono_ns: row.lease_until_mono_ns,
      fencing_epoch: row.fencing_epoch,
      pid: row.pid,
      state: row.lease_state,
    };
  }

  upsertTranscriptLocator(threadId: string, loc: HostTranscriptLocator): void {
    const key = computeTranscriptLocatorKey(loc);
    const existingRow = this.db
      .prepare('SELECT locator_json FROM transcript_locators WHERE locator_key = ?')
      .get(key) as { locator_json: string } | undefined;
    let merged: HostTranscriptLocator = loc;
    if (existingRow) {
      try {
        const existing = JSON.parse(existingRow.locator_json) as HostTranscriptLocator;
        const locMs = Date.parse(loc.recorded_at);
        const exMs = Date.parse(existing.recorded_at);
        const isLocNewerOrEqual =
          Number.isFinite(locMs) && Number.isFinite(exMs)
            ? locMs >= exMs
            : loc.recorded_at >= existing.recorded_at;
        const newer = isLocNewerOrEqual ? loc : existing;
        const older = newer === loc ? existing : loc;
        const resolvedOrigin =
          newer.locator_origin === 'operator_verified' ||
          older.locator_origin === 'operator_verified'
            ? ('operator_verified' as const)
            : (newer.locator_origin ?? older.locator_origin);
        merged = {
          ...older,
          ...newer,
          locator_origin: resolvedOrigin,
          anchor_id: newer.anchor_id ?? older.anchor_id,
          machine_hostname: newer.machine_hostname ?? older.machine_hostname,
          machine_device_id: newer.machine_device_id ?? older.machine_device_id,
          file_path: newer.file_path ?? older.file_path,
          cloud_provider: newer.cloud_provider ?? older.cloud_provider,
          host_session_id: newer.host_session_id ?? older.host_session_id,
          remote_uri: newer.remote_uri ?? older.remote_uri,
          uploaded_blob_sha256: newer.uploaded_blob_sha256 ?? older.uploaded_blob_sha256,
          uploaded_blob_rel: newer.uploaded_blob_rel ?? older.uploaded_blob_rel,
          uploaded_filename: newer.uploaded_filename ?? older.uploaded_filename,
          note: newer.note ?? older.note,
          recorded_at: newer.recorded_at,
        };
      } catch {
        merged = loc;
      }
    }
    this.db
      .prepare(
        `INSERT INTO transcript_locators(
          locator_key, locator_id, thread_id, stream_id, anchor_id, source_label, host_kind,
          machine_hostname, machine_device_id, file_path, cloud_provider, host_session_id,
          remote_uri, uploaded_blob_sha256, uploaded_blob_rel, uploaded_filename,
          locator_json, recorded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(locator_key) DO UPDATE SET
          locator_id = excluded.locator_id,
          stream_id = excluded.stream_id,
          anchor_id = COALESCE(excluded.anchor_id, transcript_locators.anchor_id),
          source_label = excluded.source_label,
          machine_hostname = COALESCE(excluded.machine_hostname, transcript_locators.machine_hostname),
          machine_device_id = COALESCE(excluded.machine_device_id, transcript_locators.machine_device_id),
          file_path = COALESCE(excluded.file_path, transcript_locators.file_path),
          cloud_provider = COALESCE(excluded.cloud_provider, transcript_locators.cloud_provider),
          host_session_id = COALESCE(excluded.host_session_id, transcript_locators.host_session_id),
          remote_uri = COALESCE(excluded.remote_uri, transcript_locators.remote_uri),
          uploaded_blob_sha256 = COALESCE(excluded.uploaded_blob_sha256, transcript_locators.uploaded_blob_sha256),
          uploaded_blob_rel = COALESCE(excluded.uploaded_blob_rel, transcript_locators.uploaded_blob_rel),
          uploaded_filename = COALESCE(excluded.uploaded_filename, transcript_locators.uploaded_filename),
          locator_json = excluded.locator_json,
          recorded_at = excluded.recorded_at`,
      )
      .run(
        key,
        merged.locator_id,
        threadId,
        merged.stream_id,
        merged.anchor_id ?? null,
        merged.source_label,
        merged.host_kind,
        merged.machine_hostname ?? null,
        merged.machine_device_id ?? null,
        merged.file_path ?? null,
        merged.cloud_provider ?? null,
        merged.host_session_id ?? null,
        merged.remote_uri ?? null,
        merged.uploaded_blob_sha256 ?? null,
        merged.uploaded_blob_rel ?? null,
        merged.uploaded_filename ?? null,
        JSON.stringify(merged),
        merged.recorded_at,
      );
  }

  listTranscriptLocators(threadId?: string): HostTranscriptLocator[] {
    const rows = (threadId
      ? this.db
          .prepare(
            'SELECT locator_key, locator_json FROM transcript_locators WHERE thread_id = ?',
          )
          .all(threadId)
      : this.db
          .prepare('SELECT locator_key, locator_json FROM transcript_locators')
          .all()) as Array<{ locator_key: string; locator_json: string }>;
    return rows
      .map((r) => ({
        key: r.locator_key,
        loc: JSON.parse(r.locator_json) as HostTranscriptLocator,
      }))
      .sort((a, b) => {
        const aMs = Date.parse(a.loc.recorded_at);
        const bMs = Date.parse(b.loc.recorded_at);
        if (Number.isFinite(aMs) && Number.isFinite(bMs)) {
          if (bMs !== aMs) return bMs - aMs;
        } else {
          const cmp = b.loc.recorded_at.localeCompare(a.loc.recorded_at);
          if (cmp !== 0) return cmp;
        }
        return a.key.localeCompare(b.key);
      })
      .map((r) => r.loc);
  }

  hasConsumedConfirmationNonce(nonce: string): boolean {
    const row = this.db
      .prepare('SELECT 1 AS found FROM consumed_confirmation_nonces WHERE nonce = ?')
      .get(nonce) as { found: number } | undefined;
    return Boolean(row?.found);
  }

  consumeConfirmationNonce(proof: ConfirmationProof, consumedAt: string): void {
    this.db
      .prepare(
        `INSERT OR IGNORE INTO consumed_confirmation_nonces(
          nonce, content_sha256, purpose, channel, consumed_at
        ) VALUES (?, ?, ?, ?, ?)`,
      )
      .run(proof.nonce, proof.content_sha256, proof.purpose, proof.channel, consumedAt);
  }
}
