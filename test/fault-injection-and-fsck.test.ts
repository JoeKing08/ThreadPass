import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { canonicalSha256, sha256Hex } from '../src/core/canonical.js';
import type { ClockPort } from '../src/core/ports.js';
import { SCHEMA_VERSION, type TransactionFinalizationCertificate, type TransactionManifest } from '../src/core/types.js';
import {
  ImmutableTargetCorruptedError,
  publishImmutable,
} from '../src/profile/local-offline/acl-and-fs.js';
import { loadOrCreateDeviceIdentity, signCommitIntentProof } from '../src/profile/local-offline/crypto-keys.js';
import {
  EgressBlockedError,
  SlidingWindowRedactor,
  assertOutboundEgressSafe,
  redactRemoteUrlCredentials,
} from '../src/profile/local-offline/redaction.js';
import {
  FencingViolationError,
  CommitConflictError,
  LocalOfflineEngine,
  SimulatedCrashError,
} from '../src/profile/local-offline/txn-engine.js';
import { ThreadPassService, ForeignCanonicalDetectedError } from '../src/shell/service.js';

function makeTempHome(label: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), `thp-test-${label}-`));
}

class MockClock implements ClockPort {
  public domain = 'clk_test';
  public epoch = 'boot_1';
  public mono = 10_000_000_000n;
  public uncertain = false;
  clockDomainId() { return this.domain; }
  bootEpoch() { return this.epoch; }
  monotonicNs() { return this.mono; }
  wallIso() { return new Date().toISOString(); }
  isClockContinuityUncertain(dom: string, boot: string) {
    return this.uncertain || dom !== this.domain || boot !== this.epoch;
  }
}

test('PublishImmutable is idempotent on identical EEXIST and fails-closed on corrupted EEXIST', () => {
  const tmp = makeTempHome('pubimm');
  try {
    const staging = path.join(tmp, 'staging');
    const target = path.join(tmp, 'committed', 'obj.json');
    const payload = '{"ok":true}\n';
    const digest = sha256Hex(payload);

    const r1 = publishImmutable(staging, target, payload, digest);
    assert.equal(r1.status, 'created');

    const r2 = publishImmutable(staging, target, payload, digest);
    assert.equal(r2.status, 'idempotent_hit');

    // Simulate corrupted / half-written target file on disk
    fs.writeFileSync(target, '{"ok":false', 'utf8');
    assert.throws(
      () => publishImmutable(staging, target, payload, digest),
      ImmutableTargetCorruptedError,
    );
    // Must NOT overwrite corrupted file
    assert.equal(fs.readFileSync(target, 'utf8'), '{"ok":false');
  } finally {
    fs.rmSync(tmp, { recursive: true, force: true });
  }
});

test('Crash after fs.link(.finalized.json) before SQLite COMMIT rolls forward deterministically', () => {
  const home = makeTempHome('crash-link');
  try {
    const svc = new ThreadPassService({ homeDir: home });
    const init = svc.initThread({ projectId: 'proj_c1', threadId: 'thr_c1' });
    const store = svc.engine.openStore('proj_c1', 'thr_c1');
    const headBefore = store.getStream(init.main_stream_id)!.head_anchor_id!;
    store.close();

    const newAnc = {
      anchor_id: 'anc_crash_fwd',
      thread_id: 'thr_c1',
      stream_id: init.main_stream_id,
      parents: [headBefore],
      workspace_manifest: {
        schema_version: SCHEMA_VERSION,
        workspace_id: 'ws_none',
        workspace_kind: 'none' as const,
        vcs: 'none' as const,
        dirty_files: [],
        untracked_files: [],
        status_hash: sha256Hex('none'),
        completeness: 'complete' as const,
        blind_spots: [],
      },
      summary: 'Crash before sqlite commit',
      head_event_ids: ['evt_crash_1'],
      created_at: new Date().toISOString(),
    };

    assert.throws(
      () =>
        svc.engine.executeTransaction(
          'proj_c1',
          {
            thread_id: 'thr_c1',
            affected_streams: [
              {
                stream_id: init.main_stream_id,
                expected_parent_head: headBefore,
                anchor: newAnc,
                events: [
                  {
                    schema_version: SCHEMA_VERSION,
                    kind: 'event',
                    event_id: 'evt_crash_1',
                    stream_id: init.main_stream_id,
                    seq: 2,
                    commit_txn_id: 'pending',
                    trust_tier: 'human_authority',
                    provenance: {
                      capture_origin: 'test',
                      verification_method: 'local_cli_direct',
                      source_event_ref: '1',
                      adapter_version: '0.1.0',
                    },
                    created_at: new Date().toISOString(),
                    payload: { type: 'human_message', text: 'Crash test message' },
                  },
                ],
              },
            ],
          },
          {
            afterFinalizedLinkBeforeSqliteCommit: () => {
              throw new SimulatedCrashError('Simulated crash right after fs.link(.finalized.json)');
            },
          },
        ),
      SimulatedCrashError,
    );

    // SQLite still points to old head before recovery
    const storeMid = svc.engine.openStore('proj_c1', 'thr_c1');
    assert.equal(storeMid.getStream(init.main_stream_id)!.head_anchor_id, headBefore);
    storeMid.close();

    // Recovery sees `.finalized.json` already persisted (Certificate-as-Commit) and rolls forward
    const rec = svc.engine.recoverPendingTransactions('proj_c1', 'thr_c1');
    assert.equal(rec.rolledForward.length, 1);

    const storeAfter = svc.engine.openStore('proj_c1', 'thr_c1');
    assert.equal(storeAfter.getStream(init.main_stream_id)!.head_anchor_id, 'anc_crash_fwd');
    storeAfter.close();
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('Stale paused process waking after recovery bumps fencing_epoch is blocked inside Finalize write lock', () => {
  const home = makeTempHome('stale-fence');
  const clock = new MockClock();
  try {
    const engine = new LocalOfflineEngine({ homeDir: home, clock });
    const svc = new ThreadPassService({ homeDir: home, clock });
    const init = svc.initThread({ projectId: 'proj_f1', threadId: 'thr_f1' });

    const store = engine.openStore('proj_f1', 'thr_f1');
    const headBefore = store.getStream(init.main_stream_id)!.head_anchor_id!;
    const spec = {
      thread_id: 'thr_f1',
      affected_streams: [
        {
          stream_id: init.main_stream_id,
          expected_parent_head: headBefore,
          anchor: {
            anchor_id: 'anc_stale_attempt',
            thread_id: 'thr_f1',
            stream_id: init.main_stream_id,
            parents: [headBefore],
            workspace_manifest: {
              schema_version: SCHEMA_VERSION,
              workspace_id: 'ws_none',
              workspace_kind: 'none' as const,
              vcs: 'none' as const,
              dirty_files: [],
              untracked_files: [],
              status_hash: sha256Hex('none'),
              completeness: 'complete' as const,
              blind_spots: [],
            },
            summary: 'Stale writer',
            head_event_ids: [],
            created_at: new Date().toISOString(),
          },
          events: [],
        },
      ],
    };

    const ctx = engine.reserveTransaction(store, 'proj_f1', spec, 5000);
    engine.prepareTransaction(store, ctx);
    engine.commitIntentTransaction(store, ctx);
    engine.publishTransaction(ctx);

    // Pause old process right before Finalize; recoverer takes over and finishes with bumped fencing_epoch!
    const rec = engine.recoverPendingTransactions('proj_f1', 'thr_f1', { store });
    assert.deepEqual(rec.rolledForward, [ctx.txn_id]);

    // Old process wakes up with stale ctx.fencing_epoch and attempts finalizeTransaction -> MUST fail!
    assert.throws(
      () => engine.finalizeTransaction(store, ctx),
      FencingViolationError,
    );
    store.close();
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('Lease handles boot_epoch reboot cleanup and suspend unknown arbitration via SQLite CAS', () => {
  const home = makeTempHome('lease-sleep');
  const clock = new MockClock();
  try {
    const engine = new LocalOfflineEngine({ homeDir: home, clock });
    const svc = new ThreadPassService({ homeDir: home, clock });
    const init = svc.initThread({ projectId: 'proj_l1', threadId: 'thr_l1' });
    const store = engine.openStore('proj_l1', 'thr_l1');
    const head = store.getStream(init.main_stream_id)!.head_anchor_id!;

    const ctx1 = engine.reserveTransaction(store, 'proj_l1', {
      thread_id: 'thr_l1',
      affected_streams: [
        {
          stream_id: init.main_stream_id,
          expected_parent_head: head,
          anchor: {
            anchor_id: 'anc_prep_1',
            thread_id: 'thr_l1',
            stream_id: init.main_stream_id,
            parents: [head],
            workspace_manifest: {
              schema_version: SCHEMA_VERSION,
              workspace_id: 'ws_none',
              workspace_kind: 'none',
              vcs: 'none',
              dirty_files: [],
              untracked_files: [],
              status_hash: sha256Hex('none'),
              completeness: 'complete',
              blind_spots: [],
            },
            head_event_ids: [],
            created_at: clock.wallIso(),
          },
          events: [],
        },
      ],
    });
    engine.prepareTransaction(store, ctx1);

    // Suspend/resume makes clock continuity uncertain -> mark unknown; live owner can still re-verify CAS and renew!
    store.markLeaseUnknown(ctx1.txn_id, clock.wallIso());
    clock.uncertain = true;
    const recAlive = engine.recoverPendingTransactions('proj_l1', 'thr_l1', {
      store,
      isProcessAlive: () => true,
      forceReclaimUnknown: false,
    });
    assert.deepEqual(recAlive.keptPreparing, [ctx1.txn_id]);

    engine.renewLease(store, ctx1.txn_id, ctx1.owner_token, ctx1.fencing_epoch);
    assert.equal(store.getLeaseInfo(ctx1.txn_id)?.state, 'active');

    // Genuine reboot changes boot_epoch -> recoverer aborts old preparing txn & bumps fencing_epoch
    clock.epoch = 'boot_2';
    clock.uncertain = false;
    const recReboot = engine.recoverPendingTransactions('proj_l1', 'thr_l1', {
      store,
      isProcessAlive: () => true,
    });
    assert.deepEqual(recReboot.aborted, [ctx1.txn_id]);
    assert.throws(
      () => engine.renewLease(store, ctx1.txn_id, ctx1.owner_token, ctx1.fencing_epoch),
      FencingViolationError,
    );
    store.close();
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('fsck rebuilds deleted SQLite, quarantines untrusted/incomplete/broken closure, and degrades local_only blobs', () => {
  const home = makeTempHome('fsck-full');
  try {
    const svc = new ThreadPassService({ homeDir: home });
    const init = svc.initThread({ projectId: 'proj_k1', threadId: 'thr_k1' });

    const store = svc.engine.openStore('proj_k1', 'thr_k1');
    const head1 = store.getStream(init.main_stream_id)!.head_anchor_id!;
    store.close();

    // Commit a transaction referencing a `local_only` blob
    const localBlobBytes = Buffer.from('local-only diagnostic payload', 'utf8');
    const localBlobSha = sha256Hex(localBlobBytes);
    const anc2Id = 'anc_step2';
    const tx2 = svc.engine.executeTransaction('proj_k1', {
      thread_id: 'thr_k1',
      affected_streams: [
        {
          stream_id: init.main_stream_id,
          expected_parent_head: head1,
          anchor: {
            anchor_id: anc2Id,
            thread_id: 'thr_k1',
            stream_id: init.main_stream_id,
            parents: [head1],
            workspace_manifest: {
              schema_version: SCHEMA_VERSION,
              workspace_id: 'ws_none',
              workspace_kind: 'none',
              vcs: 'none',
              dirty_files: [],
              untracked_files: [],
              status_hash: sha256Hex('none'),
              completeness: 'complete',
              blind_spots: [],
            },
            summary: 'Step 2 with local_only blob',
            head_event_ids: [],
            created_at: new Date().toISOString(),
          },
          events: [],
        },
      ],
      blobs: {
        diag: { bytes: localBlobBytes, sync_policy: 'local_only' },
      },
    });

    const layout = svc.engine.getLayout('proj_k1', 'thr_k1');
    // Delete entire state.sqlite + WAL/SHM and verify full deterministic rebuild!
    for (const suf of ['', '-wal', '-shm']) {
      const f = `${layout.dbPath}${suf}`;
      if (fs.existsSync(f)) fs.unlinkSync(f);
    }
    const rebuilt = svc.runFsck('proj_k1', 'thr_k1', { rebuildSqlite: true });
    assert.equal(rebuilt.ok, true);
    assert.equal(rebuilt.stream_health[init.main_stream_id], 'complete');

    // Simulate cross-device sync where `local_only` blob is excluded from remote replica
    const localBlobPath = path.join(layout.blobsObjectsDir, localBlobSha.slice(0, 2), localBlobSha);
    fs.unlinkSync(localBlobPath);
    const localOnlyDegraded = svc.runFsck('proj_k1', 'thr_k1', { rebuildSqlite: true });
    assert.equal(localOnlyDegraded.ok, true);
    assert.equal(
      localOnlyDegraded.stream_health[init.main_stream_id],
      'partial_blobs_local_only',
    );

    // Add a forged txn from an unpinned device -> untrusted_unverified_txn,
    // plus a dependent txn pointing to that forged txn's anchor -> broken_dependency_closure
    const rogueHome = makeTempHome('rogue-dev');
    const rogueId = loadOrCreateDeviceIdentity(rogueHome);
    const rogueManifestUnsigned: Omit<TransactionManifest, 'manifest_hash'> = {
      schema_version: SCHEMA_VERSION,
      txn_id: 'txn_rogue',
      project_id: 'proj_k1',
      thread_id: 'thr_k1',
      affected_streams: ['strm_rogue'],
      expected_parent_heads: { strm_rogue: null },
      expected_journal_head: null,
      shards: [],
      blobs: [],
      input_dependency_refs: [],
      provenance_refs: [],
      created_at: new Date().toISOString(),
    };
    const rogueManifest: TransactionManifest = {
      ...rogueManifestUnsigned,
      manifest_hash: canonicalSha256(rogueManifestUnsigned),
    };
    const rogueIntent = signCommitIntentProof(rogueId, {
      txn_id: 'txn_rogue',
      project_id: 'proj_k1',
      thread_id: 'thr_k1',
      manifest_hash: rogueManifest.manifest_hash,
      expected_parent_heads: { strm_rogue: null },
      expected_journal_head: null,
      fencing_epoch: 1,
      issued_at: rogueManifest.created_at,
    });
    const rogueCert: TransactionFinalizationCertificate = {
      schema_version: SCHEMA_VERSION,
      txn_id: 'txn_rogue',
      manifest_hash: rogueManifest.manifest_hash,
      commit_intent_proof: rogueIntent,
      publication_closure_proof: {
        txn_id: 'txn_rogue',
        manifest_hash: rogueManifest.manifest_hash,
        verified_objects_count: 0,
        verified_at: rogueManifest.created_at,
        closure_digest: sha256Hex(''),
      },
      finalized_at: rogueManifest.created_at,
    };
    fs.writeFileSync(
      path.join(layout.txnsCommittedDir, 'txn_rogue.manifest.json'),
      JSON.stringify(rogueManifest),
    );
    fs.writeFileSync(
      path.join(layout.txnsCommittedDir, 'txn_rogue.finalized.json'),
      JSON.stringify(rogueCert),
    );

    const fsckQuarantine = svc.runFsck('proj_k1', 'thr_k1', { rebuildSqlite: true });
    assert.equal(fsckQuarantine.ok, false);
    const rogueRep = fsckQuarantine.txn_reports.find((r) => r.txn_id === 'txn_rogue');
    assert.equal(rogueRep?.status, 'untrusted_unverified_txn');
    assert.equal(fsckQuarantine.stream_health['strm_rogue'], 'quarantined');

    // Now test broken_dependency_closure when anc2's parent txn (init.txn_id) cert is missing
    fs.unlinkSync(path.join(layout.txnsCommittedDir, `${init.txn_id}.finalized.json`));
    const fsckBroken = svc.runFsck('proj_k1', 'thr_k1', { rebuildSqlite: true });
    const tx2Rep = fsckBroken.txn_reports.find((r) => r.txn_id === tx2.txn_id);
    assert.equal(tx2Rep?.status, 'broken_dependency_closure');

    fs.rmSync(rogueHome, { recursive: true, force: true });
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('SlidingWindowRedactor catches secrets spanning chunk boundaries & Outbound Egress Gate blocks leaks', () => {
  const redactor = new SlidingWindowRedactor(['MY_CUSTOM_SUPER_SECRET_VALUE']);
  const secretPart1 = 'ghp_1234567890';
  const secretPart2 = 'ABCDEFGHIJKLMNOPQRST';
  // Pad chunk 1 so word-bounded secret spans exact 1024-byte chunk boundary!
  const chunk1 = `${'A'.repeat(1013)} ${secretPart1}`;
  const chunk2 = `${secretPart2} and sk-proj-999999999999999999 and MY_CUSTOM_SUPER_SECRET_VALUE`;

  const res = redactor.sanitizeStreamChunks([chunk1, chunk2], 256);
  assert.ok(!res.sanitized.includes(`${secretPart1}${secretPart2}`));
  assert.ok(!res.sanitized.includes('sk-proj-999999999999999999'));
  assert.ok(!res.sanitized.includes('MY_CUSTOM_SUPER_SECRET_VALUE'));
  assert.equal(res.redactedCount >= 3, true);

  assert.throws(
    () =>
      assertOutboundEgressSafe(
        redactor,
        'Accidental leak: ghp_1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ',
      ),
    EgressBlockedError,
  );
});

test('recoverPendingTransactions fails-closed on tampered .finalized.json before SQLite projection', () => {
  const home = makeTempHome('tampered-cert-rec');
  try {
    const svc = new ThreadPassService({ homeDir: home });
    const init = svc.initThread({ projectId: 'proj_tc1', threadId: 'thr_tc1' });
    const store = svc.engine.openStore('proj_tc1', 'thr_tc1');
    const headBefore = store.getStream(init.main_stream_id)!.head_anchor_id!;
    store.close();

    assert.throws(
      () =>
        svc.engine.executeTransaction(
          'proj_tc1',
          {
            thread_id: 'thr_tc1',
            affected_streams: [
              {
                stream_id: init.main_stream_id,
                expected_parent_head: headBefore,
                anchor: {
                  anchor_id: 'anc_tamper_test',
                  thread_id: 'thr_tc1',
                  stream_id: init.main_stream_id,
                  parents: [headBefore],
                  workspace_manifest: {
                    schema_version: SCHEMA_VERSION,
                    workspace_id: 'ws_none',
                    workspace_kind: 'none',
                    vcs: 'none',
                    dirty_files: [],
                    untracked_files: [],
                    status_hash: sha256Hex('none'),
                    completeness: 'complete',
                    blind_spots: [],
                  },
                  head_event_ids: [],
                  created_at: new Date().toISOString(),
                },
                events: [],
              },
            ],
          },
          {
            afterFinalizedLinkBeforeSqliteCommit: () => {
              throw new SimulatedCrashError('crash right after cert link');
            },
          },
        ),
      SimulatedCrashError,
    );

    const layout = svc.engine.getLayout('proj_tc1', 'thr_tc1');
    const certFile = fs
      .readdirSync(layout.txnsCommittedDir)
      .find((f) => f.endsWith('.finalized.json') && !f.startsWith(init.txn_id));
    assert.ok(certFile);
    const certPath = path.join(layout.txnsCommittedDir, certFile);
    const certObj = JSON.parse(fs.readFileSync(certPath, 'utf8')) as TransactionFinalizationCertificate;
    // Tamper signature inside .finalized.json
    certObj.commit_intent_proof.signature_b64 = Buffer.from('forged-sig').toString('base64');
    fs.writeFileSync(certPath, JSON.stringify(certObj), 'utf8');

    assert.throws(
      () => svc.engine.recoverPendingTransactions('proj_tc1', 'thr_tc1'),
      /Fail-closed recovery abort/,
    );
    const storeAfter = svc.engine.openStore('proj_tc1', 'thr_tc1');
    assert.equal(storeAfter.getStream(init.main_stream_id)!.head_anchor_id, headBefore);
    storeAfter.close();
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('runFsck detects offline divergent heads on same stream_id without silent wall-clock overwrite', () => {
  const homeA = makeTempHome('div-a');
  const homeB = makeTempHome('div-b');
  try {
    const svcA = new ThreadPassService({ homeDir: homeA });
    const init = svcA.initThread({
      projectId: 'proj_div',
      threadId: 'thr_div',
      mainStreamId: 'strm_main',
    });

    // Copy thread directory from Device A to Device B & pin Device A+B keys on Device A
    const layoutA = svcA.engine.getLayout('proj_div', 'thr_div');
    const layoutB = new LocalOfflineEngine({ homeDir: homeB }).getLayout('proj_div', 'thr_div');
    fs.mkdirSync(path.dirname(layoutB.rootDir), { recursive: true });
    fs.cpSync(layoutA.rootDir, layoutB.rootDir, { recursive: true });

    const svcB = new ThreadPassService({ homeDir: homeB });
    svcA.pinDeviceKey('proj_div', svcB.engine.identity.device_id, svcB.engine.identity.public_key_spki_b64);

    // Device A offline commit from initial head
    const repA = svcA.ingestReply({
      projectId: 'proj_div',
      threadId: 'thr_div',
      streamId: 'strm_main',
      rawText: 'assistant: Offline commit from Device A',
    });
    // Device B offline commit from the SAME initial head
    const repB = svcB.ingestReply({
      projectId: 'proj_div',
      threadId: 'thr_div',
      streamId: 'strm_main',
      rawText: 'assistant: Offline commit from Device B (later wall clock)',
    });

    // Sync Device B's whitelist files into Device A
    const wlB = svcB.engine.collectSyncableWhitelist('proj_div', 'thr_div');
    for (const rel of wlB.files) {
      const src = path.join(layoutB.rootDir, ...rel.split('/'));
      const dst = path.join(layoutA.rootDir, ...rel.split('/'));
      fs.mkdirSync(path.dirname(dst), { recursive: true });
      if (!fs.existsSync(dst)) fs.copyFileSync(src, dst);
    }

    const fsck = svcA.runFsck('proj_div', 'thr_div', { rebuildSqlite: true });
    assert.equal(fsck.ok, false);
    assert.equal(fsck.stream_health['strm_main'], 'divergent_heads');
    assert.deepEqual(
      fsck.divergent_stream_heads['strm_main'].sort(),
      [repA.anchor_id, repB.anchor_id].sort(),
    );

    const rebuiltStore = svcA.engine.openStore('proj_div', 'thr_div');
    const strm = rebuiltStore.getStream('strm_main')!;
    assert.equal(strm.health, 'divergent_heads');
    assert.equal(strm.head_anchor_id, null);
    assert.ok(rebuiltStore.getAnchor(repA.anchor_id));
    assert.ok(rebuiltStore.getAnchor(repB.anchor_id));
    rebuiltStore.close();

    // Unreconciled handoff and ingest on divergent stream MUST both block
    assert.throws(
      () => svcA.packHandoff({ projectId: 'proj_div', threadId: 'thr_div', streamId: 'strm_main' }),
      /divergent_heads/,
    );
    assert.throws(
      () =>
        svcA.ingestReply({
          projectId: 'proj_div',
          threadId: 'thr_div',
          streamId: 'strm_main',
          rawText: 'assistant: should fail while divergent',
        }),
      /divergent_heads/,
    );

    // commitReconcile heals both divergent Main stream heads and divergent Journal heads
    svcA.commitReconcile({
      projectId: 'proj_div',
      threadId: 'thr_div',
      sourceStreamIds: [],
      receiptRawText: JSON.stringify({
        reconciliation_receipt: true,
        adopted_items: ['strm_main: joined offline heads'],
        conflicts: [],
        synthesis_summary: 'Unified Device A and Device B commits on Main.',
      }),
    });
    const healed = svcA.runFsck('proj_div', 'thr_div', { rebuildSqlite: true });
    assert.equal(healed.ok, true);
    assert.equal(healed.stream_health['strm_main'], 'complete');
    assert.deepEqual(healed.divergent_stream_heads, {});
    assert.deepEqual(healed.divergent_journal_heads, []);
  } finally {
    fs.rmSync(homeA, { recursive: true, force: true });
    fs.rmSync(homeB, { recursive: true, force: true });
  }
});

test('host_bindings survive fsck --rebuild and Profile Conformance ports work', () => {
  const home = makeTempHome('hb-ports');
  try {
    const svc = new ThreadPassService({ homeDir: home });
    const init = svc.initThread({ projectId: 'proj_hb', threadId: 'thr_hb1' });
    svc.initThread({ projectId: 'proj_hb', threadId: 'thr_hb2' });

    const adopted = svc.adoptExternal({
      projectId: 'proj_hb',
      threadId: 'thr_hb1',
      sourceLabel: 'ext',
      rawText: 'assistant: bound session work',
      hostSessionId: 'HS_REBUILD_42',
    });

    // Delete SQLite and rebuild from immutable shards
    const layout = svc.engine.getLayout('proj_hb', 'thr_hb1');
    for (const suf of ['', '-wal', '-shm']) {
      const f = `${layout.dbPath}${suf}`;
      if (fs.existsSync(f)) fs.unlinkSync(f);
    }
    const rebuilt = svc.runFsck('proj_hb', 'thr_hb1', { rebuildSqlite: true });
    assert.equal(rebuilt.ok, true);

    const storeAfter = svc.engine.openExistingStore('proj_hb', 'thr_hb1');
    assert.equal(storeAfter.lookupHostBinding('HS_REBUILD_42')?.stream_id, adopted.stream_id);
    storeAfter.close();

    // Cross-thread adoption MUST still detect ForeignCanonicalDetectedError after rebuild!
    assert.throws(
      () =>
        svc.adoptExternal({
          projectId: 'proj_hb',
          threadId: 'thr_hb2',
          sourceLabel: 'ext2',
          rawText: 'assistant: duplicate host session',
          hostSessionId: 'HS_REBUILD_42',
        }),
      ForeignCanonicalDetectedError,
    );

    // Profile Conformance: StoragePort / CoordinationPort / SyncTransportPort
    const ports = svc.engine.createCorePorts('proj_hb', 'thr_hb1');
    const headId = ports.getStreamHead(init.main_stream_id);
    assert.ok(headId);
    assert.equal(ports.getAnchor(headId!)?.stream_id, init.main_stream_id);
    assert.ok(ports.getJournalHead('thr_hb1'));
    assert.ok(ports.listJournalEvents('thr_hb1').length >= 2);
    const syncPkg = ports.collectSyncableWhitelist('thr_hb1');
    assert.equal(syncPkg.manifests.length, syncPkg.certificates.length);
    assert.ok(syncPkg.manifests.length >= 2);
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('reserveTransaction refuses to steal an unknown lease of a live owner and cleans up dead/expired preparing leases', () => {
  const home = makeTempHome('lease-steal-guard');
  const clock = new MockClock();
  try {
    const engine = new LocalOfflineEngine({ homeDir: home, clock });
    const svc = new ThreadPassService({ homeDir: home, clock });
    const init = svc.initThread({ projectId: 'proj_ls', threadId: 'thr_ls' });
    const store = engine.openExistingStore('proj_ls', 'thr_ls');
    const head = store.getStream(init.main_stream_id)!.head_anchor_id!;

    const makeSpec = (anchorId: string) => ({
      thread_id: 'thr_ls',
      affected_streams: [
        {
          stream_id: init.main_stream_id,
          expected_parent_head: head,
          anchor: {
            anchor_id: anchorId,
            thread_id: 'thr_ls',
            stream_id: init.main_stream_id,
            parents: [head],
            workspace_manifest: {
              schema_version: SCHEMA_VERSION,
              workspace_id: 'ws_none',
              workspace_kind: 'none' as const,
              vcs: 'none' as const,
              dirty_files: [],
              untracked_files: [],
              status_hash: sha256Hex('none'),
              completeness: 'complete' as const,
              blind_spots: [],
            },
            head_event_ids: [],
            created_at: clock.wallIso(),
          },
          events: [],
        },
      ],
    });

    const ctx1 = engine.reserveTransaction(store, 'proj_ls', makeSpec('anc_1'), 5000);
    store.markLeaseUnknown(ctx1.txn_id, clock.wallIso());

    // Live process with unknown lease MUST block concurrent reserveTransaction
    assert.throws(
      () => engine.reserveTransaction(store, 'proj_ls', makeSpec('anc_2'), 5000),
      CommitConflictError,
    );

    // Advance past reboot -> reserveTransaction reclaims and aborts ctx1 so ctx1 fails CAS
    clock.epoch = 'boot_next';
    const ctx2 = engine.reserveTransaction(store, 'proj_ls', makeSpec('anc_2'), 5000);
    assert.ok(ctx2.txn_id !== ctx1.txn_id);
    assert.equal(store.getTxn(ctx1.txn_id)?.state, 'aborted');
    assert.throws(
      () => engine.commitIntentTransaction(store, ctx1),
      FencingViolationError,
    );
    store.close();
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('Redaction scrubs JSON-quoted kv_secret with special chars and strips Git remote URL credentials', () => {
  const redactor = new SlidingWindowRedactor();
  const input = '{"api_key": "My$ecret!Pass=123", "password": "P@ssw0rd!9876"}';
  const res = redactor.sanitizeText(input);
  assert.ok(!res.sanitized.includes('My$ecret'));
  assert.ok(!res.sanitized.includes('P@ssw0rd'));
  assert.equal(res.redactedCount, 2);
  // Preserves valid JSON syntax when quoted
  const parsed = JSON.parse(res.sanitized) as Record<string, string>;
  assert.equal(parsed.api_key, '[REDACTED:kv_secret]');

  const escapedJson = '{"password": "abc\\"def$1234", "other": "safe"}';
  const escRes = redactor.sanitizeText(escapedJson);
  assert.ok(!escRes.sanitized.includes('def$1234'));
  assert.equal((JSON.parse(escRes.sanitized) as Record<string, string>).password, '[REDACTED:kv_secret]');

  assert.equal(
    redactRemoteUrlCredentials('https://oauth2:glpat-secret123@gitlab.example.com/team/repo.git'),
    'https://REDACTED@gitlab.example.com/team/repo.git',
  );
});

test('runFsck detects offline Coordinator Journal divergence across synced devices and clears journal_head_id on rebuild', () => {
  const homeA = makeTempHome('jdiv-a');
  const homeB = makeTempHome('jdiv-b');
  try {
    const svcA = new ThreadPassService({ homeDir: homeA });
    const svcB = new ThreadPassService({ homeDir: homeB });
    svcA.initThread({ projectId: 'proj_jd', threadId: 'thr_jd', mainStreamId: 'strm_main' });

    const layA = svcA.engine.getLayout('proj_jd', 'thr_jd');
    const layB = svcB.engine.getLayout('proj_jd', 'thr_jd');
    fs.mkdirSync(path.dirname(layB.rootDir), { recursive: true });
    fs.cpSync(layA.rootDir, layB.rootDir, { recursive: true });
    svcA.pinDeviceKey('proj_jd', svcB.engine.identity.device_id, svcB.engine.identity.public_key_spki_b64);
    svcB.pinDeviceKey('proj_jd', svcA.engine.identity.device_id, svcA.engine.identity.public_key_spki_b64);

    // Offline adopt journal divergence on two devices without mutual sync
    svcA.adoptExternal({
      projectId: 'proj_jd',
      threadId: 'thr_jd',
      streamId: 'strm_ext_a',
      rawText: 'assistant: device A adoption',
    });
    svcB.adoptExternal({
      projectId: 'proj_jd',
      threadId: 'thr_jd',
      streamId: 'strm_ext_b',
      rawText: 'assistant: device B adoption',
    });

    // Sync Device B's whitelist files into Device A
    const wlB = svcB.engine.collectSyncableWhitelist('proj_jd', 'thr_jd');
    for (const rel of wlB.files) {
      const src = path.join(layB.rootDir, ...rel.split('/'));
      const dst = path.join(layA.rootDir, ...rel.split('/'));
      fs.mkdirSync(path.dirname(dst), { recursive: true });
      if (!fs.existsSync(dst)) fs.copyFileSync(src, dst);
    }

    const fsck = svcA.runFsck('proj_jd', 'thr_jd', { rebuildSqlite: true });
    assert.equal(fsck.ok, false);
    assert.equal(fsck.divergent_journal_heads.length, 2);

    const stAfter = svcA.engine.openExistingStore('proj_jd', 'thr_jd');
    try {
      assert.equal(stAfter.getThread('thr_jd')?.journal_head_id, null);
    } finally {
      stAfter.close();
    }

    // Divergent journal state must block new forks/adoptions until reconciled, then reconcile heals the DAG
    assert.throws(
      () =>
        svcA.forkDispatch({
          projectId: 'proj_jd',
          threadId: 'thr_jd',
          taskBrief: 'Should block while journal divergent',
        }),
      /unresolved divergent Coordinator Journal heads/,
    );
    svcA.commitReconcile({
      projectId: 'proj_jd',
      threadId: 'thr_jd',
      sourceStreamIds: ['strm_ext_a', 'strm_ext_b'],
      receiptRawText: JSON.stringify({
        reconciliation_receipt: true,
        adopted_items: ['strm_ext_a', 'strm_ext_b'],
        conflicts: [],
        synthesis_summary: 'Resolved offline journal divergence onto Main.',
      }),
    });
    const fsckHealed = svcA.runFsck('proj_jd', 'thr_jd', { rebuildSqlite: true });
    assert.equal(fsckHealed.ok, true);
    assert.deepEqual(fsckHealed.divergent_journal_heads, []);
  } finally {
    fs.rmSync(homeA, { recursive: true, force: true });
    fs.rmSync(homeB, { recursive: true, force: true });
  }
});

test('readCommittedStreamLines follows causal anchor.parents order even under backward wall-clock skew', () => {
  const home = makeTempHome('causal-read');
  let isoNow = '2026-11-27T12:00:10.000Z';
  const skewedClock: ClockPort = {
    clockDomainId: () => 'clk_skew',
    bootEpoch: () => 'boot_skew',
    monotonicNs: () => 10_000_000_000n,
    wallIso: () => isoNow,
  };
  try {
    const svc = new ThreadPassService({ homeDir: home, clock: skewedClock });
    const init = svc.initThread({ projectId: 'proj_skew', threadId: 'thr_skew', mainStreamId: 'strm_main' });
    // Second commit has an EARLIER wall-clock timestamp than genesis!
    isoNow = '2026-11-27T11:59:00.000Z';
    svc.ingestReply({
      projectId: 'proj_skew',
      threadId: 'thr_skew',
      streamId: init.main_stream_id,
      rawText: 'assistant: second event with skewed earlier clock',
    });
    const events = svc
      .peekStream('proj_skew', 'thr_skew', init.main_stream_id)
      .filter((l) => l.kind === 'event');
    assert.equal(events.length, 2);
    assert.ok(events[1].kind === 'event' && events[1].payload.type === 'assistant_part');
    assert.equal(events[1].payload.text, 'second event with skewed earlier clock');
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('Transcript locators cap inline prompt list at 12, export transcript-index.json, and survive fsck --rebuild', () => {
  const home = makeTempHome('tloc-rebuild');
  try {
    const svc = new ThreadPassService({ homeDir: home });
    const init = svc.initThread({ projectId: 'proj_tloc', threadId: 'thr_tloc', mainStreamId: 'strm_main' });

    for (let i = 1; i <= 15; i++) {
      const fpath = path.join(home, `session_${i}.jsonl`);
      fs.writeFileSync(fpath, `line_${i}\n`, 'utf8');
      svc.adoptExternal({
        projectId: 'proj_tloc',
        threadId: 'thr_tloc',
        sourceLabel: `ai_${i}`,
        streamId: `strm_t_${i}`,
        rawText: `assistant: finding number ${i}`,
        transcriptFilePath: fpath,
      });
    }

    // Re-ingesting the SAME path on an existing stream should deduplicate cleanly
    svc.ingestReply({
      projectId: 'proj_tloc',
      threadId: 'thr_tloc',
      streamId: 'strm_t_1',
      rawText: 'assistant: second update same transcript file',
      transcriptFilePath: path.join(home, 'session_1.jsonl'),
    });

    const layout = svc.engine.getLayout('proj_tloc', 'thr_tloc');
    const indexJson = JSON.parse(fs.readFileSync(layout.transcriptIndexJsonPath, 'utf8')) as {
      total_locators: number;
      locators: Array<{ stream_id: string }>;
    };
    assert.equal(indexJson.total_locators, 15);

    const ho = svc.packHandoff({
      projectId: 'proj_tloc',
      threadId: 'thr_tloc',
      streamId: init.main_stream_id,
    });
    assert.ok(ho.full_markdown.includes('(+3 older transcript locator(s) archived in'));

    // Delete SQLite & rebuild from immutable shards -> all 15 locators MUST survive
    for (const suf of ['', '-wal', '-shm']) {
      const f = `${layout.dbPath}${suf}`;
      if (fs.existsSync(f)) fs.unlinkSync(f);
    }
    const rebuilt = svc.runFsck('proj_tloc', 'thr_tloc', { rebuildSqlite: true });
    assert.equal(rebuilt.ok, true);

    const stAfter = svc.getStatus('proj_tloc', 'thr_tloc');
    assert.equal(stAfter.transcript_locators.length, 15);
  } finally {
    fs.rmSync(home, { recursive: true, force: true });
  }
});
