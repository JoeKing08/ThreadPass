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

test('Causally prior dispatch before grant_revoked succeeds, while post-revocation dispatch fails closed', () => {
  const events: JournalEventRecord[] = [
    makeJournalEvent({
      journal_event_id: 'je_m',
      event_type: 'main_designated',
      data: { main_stream_id: 'strm_main' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_g1',
      event_type: 'grant_issued',
      causal_predecessors: ['je_m'],
      data: {
        grant_id: 'grant_seq',
        issuer_stream_id: 'strm_main',
        grantee_stream_id: 'strm_a',
        allowed_actions: ['fork'],
        workspace_scope: ['ws_1'],
        max_total_spawns: 2,
        max_depth: 1,
        can_redelegate: false,
      },
    }),
    makeJournalEvent({
      journal_event_id: 'je_d_before',
      event_type: 'dispatch_committed',
      causal_predecessors: ['je_g1'],
      data: { dispatch_id: 'disp_before', grant_id: 'grant_seq' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_rev_after',
      event_type: 'grant_revoked',
      causal_predecessors: ['je_d_before'],
      data: {
        grant_id: 'grant_seq',
        revoked_by_stream_id: 'strm_main',
        reason: 'finished',
      },
    }),
  ];

  const validProj = projectJournalState('thr_1', events);
  assert.equal(validProj.has_blocking_conflict, false);
  assert.equal(validProj.grants.get('grant_seq')?.remaining_spawns, 1);

  // Now append a dispatch that causally depends on the revocation -> MUST report revoked_grant_used
  const invalidProj = projectJournalState('thr_1', [
    ...events,
    makeJournalEvent({
      journal_event_id: 'je_d_after',
      event_type: 'dispatch_committed',
      causal_predecessors: ['je_rev_after'],
      data: { dispatch_id: 'disp_after', grant_id: 'grant_seq' },
    }),
  ]);
  assert.equal(invalidProj.has_blocking_conflict, true);
  assert.ok(invalidProj.conflicts.some((c) => c.kind === 'revoked_grant_used'));
});

test('Core DST: seeded permutation fuzzing confirms CALM order-independence and Lifecycle determinism', () => {
  // Deterministic Mulberry32 PRNG
  let seed = 0x5eed1234;
  const nextRand = () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const shuffle = <T>(arr: readonly T[]): T[] => {
    const copy = [...arr];
    for (let i = copy.length - 1; i > 0; i--) {
      const j = Math.floor(nextRand() * (i + 1));
      [copy[i], copy[j]] = [copy[j], copy[i]];
    }
    return copy;
  };

  const baseEvents: JournalEventRecord[] = [
    makeJournalEvent({
      journal_event_id: 'je_main',
      event_type: 'main_designated',
      data: { main_stream_id: 'strm_main' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_grant',
      event_type: 'grant_issued',
      causal_predecessors: ['je_main'],
      data: {
        grant_id: 'g_root',
        issuer_stream_id: 'strm_main',
        grantee_stream_id: 'strm_w',
        allowed_actions: ['fork'],
        workspace_scope: ['*'],
        max_total_spawns: 2,
        max_depth: 1,
        can_redelegate: false,
      },
    }),
    makeJournalEvent({
      journal_event_id: 'je_d1',
      event_type: 'dispatch_committed',
      causal_predecessors: ['je_grant'],
      data: { dispatch_id: 'd1', grant_id: 'g_root' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_rcpt',
      event_type: 'result_receipt_recorded',
      causal_predecessors: ['je_d1'],
      data: {
        receipt_id: 'rcpt_dst',
        target_id: 'task_dst',
        source_stream_id: 'strm_w',
        source_anchor_id: 'anc_w',
        summary: 'ok',
        artifact_refs: [],
      },
    }),
    makeJournalEvent({
      journal_event_id: 'je_rev',
      event_type: 'grant_revoked',
      causal_predecessors: ['je_d1'],
      data: { grant_id: 'g_root', revoked_by_stream_id: 'strm_main', reason: 'done' },
    }),
  ];

  const baseObs: LifecycleObservation[] = [
    {
      observation_id: 'o1',
      target_stream_id: 'strm_w',
      state: 'active',
      source: 'dev_a',
      causal_predecessors: [],
      observed_at: '2026-11-26T01:00:00Z',
    },
    {
      observation_id: 'o2',
      target_stream_id: 'strm_w',
      state: 'unavailable',
      source: 'dev_b',
      causal_predecessors: ['o1'],
      observed_at: '2026-11-26T02:00:00Z',
    },
    {
      observation_id: 'o3',
      target_stream_id: 'strm_w',
      state: 'active',
      source: 'dev_a',
      causal_predecessors: ['o1'],
      observed_at: '2026-11-26T03:00:00Z',
    },
  ];

  for (let trial = 0; trial < 30; trial++) {
    const permEvents = shuffle(baseEvents);
    const p = projectJournalState('thr_1', permEvents);
    assert.equal(p.has_blocking_conflict, false);
    assert.equal(p.grants.get('g_root')?.revoked, true);
    assert.equal(p.grants.get('g_root')?.remaining_spawns, 1);
    assert.equal(p.receipts.get('rcpt_dst')?.summary, 'ok');

    const permObs = shuffle(baseObs);
    assert.equal(projectLifecycle(permObs), 'concurrent');
  }
});

test('Child grant enforces holder ownership, parent revocation causality, wildcard narrowing, and non-negative max_depth', () => {
  const base: JournalEventRecord[] = [
    makeJournalEvent({
      journal_event_id: 'je_m',
      event_type: 'main_designated',
      data: { main_stream_id: 'strm_main' },
    }),
    makeJournalEvent({
      journal_event_id: 'je_g_root',
      event_type: 'grant_issued',
      causal_predecessors: ['je_m'],
      data: {
        grant_id: 'g_root',
        issuer_stream_id: 'strm_main',
        grantee_stream_id: 'strm_a',
        allowed_actions: ['fork'],
        workspace_scope: ['*'],
        max_total_spawns: 4,
        max_depth: 2,
        can_redelegate: true,
      },
    }),
  ];

  // 1. Valid narrowing from ['*'] -> ['ws_1'] by actual holder strm_a succeeds
  const validSub = makeJournalEvent({
    journal_event_id: 'je_g_child_ok',
    event_type: 'grant_issued',
    causal_predecessors: ['je_g_root'],
    data: {
      grant_id: 'g_child_ok',
      issuer_stream_id: 'strm_a',
      grantee_stream_id: 'strm_b',
      parent_grant_id: 'g_root',
      allowed_actions: ['fork'],
      workspace_scope: ['ws_1'],
      max_total_spawns: 1,
      max_depth: 1,
      can_redelegate: false,
    },
  });
  assert.equal(projectJournalState('thr_1', [...base, validSub]).has_blocking_conflict, false);

  // 2. Non-holder strm_c stealing strm_a's grant as parent MUST fail with grant_scope_expanded
  const theft = makeJournalEvent({
    journal_event_id: 'je_g_theft',
    event_type: 'grant_issued',
    causal_predecessors: ['je_g_root'],
    data: {
      grant_id: 'g_theft',
      issuer_stream_id: 'strm_c',
      grantee_stream_id: 'strm_d',
      parent_grant_id: 'g_root',
      allowed_actions: ['fork'],
      workspace_scope: ['ws_1'],
      max_total_spawns: 1,
      max_depth: 1,
      can_redelegate: false,
    },
  });
  const theftProj = projectJournalState('thr_1', [...base, theft]);
  assert.equal(theftProj.has_blocking_conflict, true);
  assert.ok(theftProj.conflicts.some((c) => c.kind === 'grant_scope_expanded'));

  // 3. Negative max_depth MUST fail
  const negDepth = makeJournalEvent({
    journal_event_id: 'je_g_neg',
    event_type: 'grant_issued',
    causal_predecessors: ['je_m'],
    data: {
      grant_id: 'g_neg',
      issuer_stream_id: 'strm_main',
      grantee_stream_id: 'strm_a',
      allowed_actions: ['fork'],
      workspace_scope: ['*'],
      max_total_spawns: 1,
      max_depth: -1,
      can_redelegate: false,
    },
  });
  assert.ok(
    projectJournalState('thr_1', [...base, negDepth]).conflicts.some(
      (c) => c.kind === 'grant_scope_expanded',
    ),
  );

  // 4. Child grant issued causally AFTER parent grant revocation MUST fail with revoked_grant_used
  const revParent = makeJournalEvent({
    journal_event_id: 'je_rev_parent',
    event_type: 'grant_revoked',
    causal_predecessors: ['je_g_root'],
    data: { grant_id: 'g_root', revoked_by_stream_id: 'strm_main', reason: 'revoked' },
  });
  const afterRevChild = makeJournalEvent({
    journal_event_id: 'je_child_after_rev',
    event_type: 'grant_issued',
    causal_predecessors: ['je_rev_parent'],
    data: {
      grant_id: 'g_child_late',
      issuer_stream_id: 'strm_a',
      grantee_stream_id: 'strm_b',
      parent_grant_id: 'g_root',
      allowed_actions: ['fork'],
      workspace_scope: ['ws_1'],
      max_total_spawns: 1,
      max_depth: 1,
      can_redelegate: false,
    },
  });
  const afterRevProj = projectJournalState('thr_1', [...base, revParent, afterRevChild]);
  assert.equal(afterRevProj.has_blocking_conflict, true);
  assert.ok(afterRevProj.conflicts.some((c) => c.kind === 'revoked_grant_used'));
});

test('projectJournalState deduplicates identical journal_event_id and blocks duplicate grant_id collision', () => {
  const g1 = makeJournalEvent({
    journal_event_id: 'je_g1',
    event_type: 'grant_issued',
    data: {
      grant_id: 'grt_dup',
      issuer_stream_id: 'strm_main',
      grantee_stream_id: 'strm_a',
      allowed_actions: ['fork'],
      workspace_scope: ['*'],
      max_total_spawns: 2,
      max_depth: 1,
      can_redelegate: true,
    },
  });
  const d1 = makeJournalEvent({
    journal_event_id: 'je_d1',
    event_type: 'dispatch_committed',
    causal_predecessors: ['je_g1'],
    data: { dispatch_id: 'd1', grant_id: 'grt_dup' },
  });
  const replay = projectJournalState('thr_1', [g1, d1, g1, d1]);
  assert.equal(replay.has_blocking_conflict, false);
  assert.equal(replay.grants.get('grt_dup')?.remaining_spawns, 1);

  const colliding = makeJournalEvent({
    journal_event_id: 'je_g_collide',
    event_type: 'grant_issued',
    data: {
      grant_id: 'grt_dup',
      issuer_stream_id: 'strm_main',
      grantee_stream_id: 'strm_b',
      allowed_actions: ['fork'],
      workspace_scope: ['*'],
      max_total_spawns: 5,
      max_depth: 1,
      can_redelegate: false,
    },
  });
  const collideProj = projectJournalState('thr_1', [g1, colliding]);
  assert.equal(collideProj.has_blocking_conflict, true);
  assert.ok(collideProj.conflicts.some((c) => c.kind === 'grant_scope_expanded'));
});
