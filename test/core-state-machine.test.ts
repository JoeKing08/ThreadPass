import assert from 'node:assert/strict';
import test from 'node:test';
import {
  analyzeCommonAncestors,
  projectJournalState,
  projectLifecycle,
  projectMailboxState,
  verifyMailboxReceiptAck,
} from '../src/core/state-machine.js';
import {
  SCHEMA_VERSION,
  type JournalEventRecord,
  type LifecycleObservation,
  type NotifyReceiptRecord,
} from '../src/core/types.js';

function makeJournalEvent(
  partial: Partial<JournalEventRecord> & Pick<JournalEventRecord, 'journal_event_id' | 'event_type' | 'data'>,
): JournalEventRecord {
  return {
    schema_version: SCHEMA_VERSION,
    thread_id: 'thr_1',
    main_stream_id: 'strm_main',
    actor_stream_id: 'strm_main',
    causal_predecessors: [],
    commit_txn_id: 'txn_1',
    coordinator_epoch: 1,
    created_at: '2026-11-26T12:00:00Z',
    ...partial,
  };
}

test('CALM monotone journal facts converge while conflicting main_designated blocks', () => {
  const events: JournalEventRecord[] = [
    makeJournalEvent({
      journal_event_id: 'je_m1',
      event_type: 'main_designated',
      data: { main_stream_id: 'strm_main_a' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_m2',
      event_type: 'main_designated',
      data: { main_stream_id: 'strm_main_b' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_r1',
      event_type: 'result_receipt_recorded',
      data: {
        receipt_id: 'rcpt_1',
        target_id: 'task_1',
        source_stream_id: 'strm_w1',
        source_anchor_id: 'anc_1',
        summary: 'done',
        artifact_refs: [],
      },
    }),
  ];

  const proj = projectJournalState('thr_1', events);
  assert.equal(proj.has_blocking_conflict, true);
  assert.equal(proj.conflicts.length, 1);
  assert.equal(proj.conflicts[0].kind, 'main_designation_conflict');
  assert.equal(proj.receipts.get('rcpt_1')?.summary, 'done');
});

test('Non-monotone result_accepted vs result_rejected raises blocking conflict', () => {
  const proj = projectJournalState('thr_1', [
    makeJournalEvent({
      journal_event_id: 'je_m1',
      event_type: 'main_designated',
      data: { main_stream_id: 'strm_main' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_a',
      event_type: 'result_accepted',
      data: { receipt_id: 'rcpt_1', target_id: 'work_9' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_r',
      event_type: 'result_rejected',
      data: { receipt_id: 'rcpt_1', target_id: 'work_9' },
    }),
  ]);
  assert.equal(proj.has_blocking_conflict, true);
  assert.equal(proj.conflicts[0].kind, 'result_decision_conflict');
});

test('Escrow quota split conserves parent budget and irrevocable grant_revoked propagates', () => {
  const events: JournalEventRecord[] = [
    makeJournalEvent({
      journal_event_id: 'je_m',
      event_type: 'main_designated',
      data: { main_stream_id: 'strm_main' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_g1',
      event_type: 'grant_issued',
      data: {
        grant_id: 'grant_root',
        issuer_stream_id: 'strm_main',
        grantee_stream_id: 'strm_a',
        allowed_actions: ['fork'],
        workspace_scope: ['ws_1'],
        max_total_spawns: 3,
        max_depth: 2,
        can_redelegate: true,
      },
    }),
    makeJournalEvent({
      journal_event_id: 'je_g2',
      event_type: 'grant_issued',
      data: {
        grant_id: 'grant_child',
        issuer_stream_id: 'strm_a',
        grantee_stream_id: 'strm_b',
        parent_grant_id: 'grant_root',
        allowed_actions: ['fork'],
        workspace_scope: ['ws_1'],
        max_total_spawns: 2,
        max_depth: 1,
        can_redelegate: false,
      },
    }),
    makeJournalEvent({
      journal_event_id: 'je_rev',
      event_type: 'grant_revoked',
      data: {
        grant_id: 'grant_root',
        revoked_by_stream_id: 'strm_main',
        reason: 'scope closed',
      },
    }),
    makeJournalEvent({
      journal_event_id: 'je_d1',
      event_type: 'dispatch_committed',
      data: {
        dispatch_id: 'disp_after',
        grant_id: 'grant_child',
      },
    }),
  ];

  const proj = projectJournalState('thr_1', events);
  assert.equal(proj.grants.get('grant_root')?.remaining_spawns, 1);
  assert.equal(proj.grants.get('grant_child')?.revoked, true);
  assert.equal(proj.has_blocking_conflict, true);
  assert.ok(proj.conflicts.some((c) => c.kind === 'offline_dispatch_vs_revocation'));
});

test('Concurrent contradictory Lifecycle Observations yield `concurrent` deterministically', () => {
  const obs: LifecycleObservation[] = [
    {
      observation_id: 'obs_root',
      target_stream_id: 'strm_w1',
      state: 'active',
      source: 'dev_a',
      causal_predecessors: [],
      observed_at: '2026-11-26T10:00:00Z',
    },
    {
      observation_id: 'obs_b1',
      target_stream_id: 'strm_w1',
      state: 'active',
      source: 'dev_a',
      causal_predecessors: ['obs_root'],
      observed_at: '2026-11-26T10:05:00Z',
    },
    {
      observation_id: 'obs_b2',
      target_stream_id: 'strm_w1',
      state: 'unavailable',
      source: 'dev_b',
      causal_predecessors: ['obs_root'],
      observed_at: '2026-11-26T10:06:00Z',
    },
  ];
  assert.equal(projectLifecycle(obs), 'concurrent');

  const resolved = [
    ...obs,
    {
      observation_id: 'obs_join',
      target_stream_id: 'strm_w1',
      state: 'recovered' as const,
      source: 'dev_a',
      causal_predecessors: ['obs_b1', 'obs_b2'],
      observed_at: '2026-11-26T10:10:00Z',
    },
  ];
  assert.equal(projectLifecycle(resolved), 'recovered');
});

test('Mailbox rejects forged batch_id echo and accepts valid offered batch', () => {
  const targetReceipts: NotifyReceiptRecord[] = [
    {
      receipt_type: 'offered',
      batch_id: 'batch_real_123',
      target_stream_id: 'strm_target',
      target_base_anchor_id: 'anc_1',
      message_ids: ['msg_1', 'msg_2'],
      recorded_at: '2026-11-26T10:00:00Z',
    },
  ];

  const forged = verifyMailboxReceiptAck(
    'strm_target',
    'batch_forged_999',
    ['msg_1'],
    targetReceipts,
  );
  assert.equal(forged.valid, false);

  const validAck = verifyMailboxReceiptAck(
    'strm_target',
    'batch_real_123',
    ['msg_1'],
    targetReceipts,
  );
  assert.equal(validAck.valid, true);
  assert.deepEqual(validAck.acceptedMessageIds, ['msg_1']);

  const proj = projectMailboxState('strm_target', ['msg_1', 'msg_2'], [
    ...targetReceipts,
    {
      receipt_type: 'committed_receipt',
      batch_id: 'batch_real_123',
      target_stream_id: 'strm_target',
      target_base_anchor_id: 'anc_2',
      message_ids: ['msg_1'],
      recorded_at: '2026-11-26T10:01:00Z',
    },
  ]);
  assert.equal(proj.messages.get('msg_1')?.status, 'committed_receipt');
  assert.equal(proj.messages.get('msg_2')?.status, 'offered');
});

test('Ancestor pre-analysis computes common ancestors without implying semantic merge', () => {
  const parentsMap: Record<string, string[]> = {
    anc_root: [],
    anc_a1: ['anc_root'],
    anc_b1: ['anc_root'],
    anc_c_wild: [],
  };
  const res = analyzeCommonAncestors(
    ['anc_a1', 'anc_b1'],
    (id) => parentsMap[id] ?? null,
  );
  assert.deepEqual(res.maximalCommon, ['anc_root']);

  const wildRes = analyzeCommonAncestors(
    ['anc_a1', 'anc_c_wild'],
    (id) => parentsMap[id] ?? null,
  );
  assert.deepEqual(wildRes.maximalCommon, []);
});
