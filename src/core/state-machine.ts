import type {
  GrantIssuedData,
  GrantRevokedData,
  JournalEventRecord,
  LifecycleObservation,
  LifecycleProjectionValue,
  MainDesignationData,
  NotifyReceiptRecord,
  ResultDecisionData,
  ResultReceiptData,
} from './types.js';

export interface JournalConflict {
  kind:
    | 'main_designation_conflict'
    | 'result_decision_conflict'
    | 'grant_quota_exceeded'
    | 'grant_scope_expanded'
    | 'revoked_grant_used'
    | 'offline_dispatch_vs_revocation';
  target_id: string;
  event_ids: string[];
  detail: string;
}

export interface EscrowGrantState {
  grant: GrantIssuedData;
  remaining_spawns: number;
  revoked: boolean;
  revoked_by_event_ids: string[];
  children_grant_ids: string[];
  used_dispatch_ids: string[];
}

export interface ProjectedJournalState {
  thread_id: string;
  main_stream_ids: string[];
  has_blocking_conflict: boolean;
  conflicts: JournalConflict[];
  receipts: Map<string, ResultReceiptData>;
  decisions: Map<string, { status: 'accepted' | 'rejected' | 'abandoned'; event_id: string }>;
  grants: Map<string, EscrowGrantState>;
  revoked_grant_ids: Set<string>;
}

/**
 * Pure CALM + I-Confluence Journal projector.
 * Monotone fact sets (receipts, grant_revoked tombstones) converge order-independently.
 * Non-monotone uniqueness/mutex invariants (main_designated, result decisions, Escrow bounds)
 * raise explicit blocking conflicts rather than resolving by wall-clock or epoch ordering.
 */
export function projectJournalState(
  threadId: string,
  events: readonly JournalEventRecord[],
): ProjectedJournalState {
  const conflicts: JournalConflict[] = [];
  const mainCandidates = new Map<string, string[]>();
  const receipts = new Map<string, ResultReceiptData>();
  const decisions = new Map<
    string,
    { status: 'accepted' | 'rejected' | 'abandoned'; event_id: string }
  >();
  const decisionEventsByTarget = new Map<
    string,
    Array<{ status: 'accepted' | 'rejected' | 'abandoned'; event_id: string }>
  >();

  const grants = new Map<string, EscrowGrantState>();
  const grantIssuerEventIds = new Map<string, string>();
  const seenEventIds = new Set<string>();
  const uniqueEvents: JournalEventRecord[] = [];
  const revokedGrantIds = new Set<string>();
  const revocationsByGrant = new Map<string, Array<{ event_id: string; causal: Set<string> }>>();
  const journalPredsById = new Map<string, readonly string[]>();

  const computeCausalAncestors = (startId: string): Set<string> => {
    const visited = new Set<string>();
    const queue = [...(journalPredsById.get(startId) ?? [])];
    while (queue.length > 0) {
      const cur = queue.shift()!;
      if (visited.has(cur)) continue;
      visited.add(cur);
      for (const p of journalPredsById.get(cur) ?? []) {
        if (!visited.has(p)) queue.push(p);
      }
    }
    return visited;
  };

  // Pass 1: Deduplicate by journal_event_id; collect monotone tombstones, main candidates & grants
  for (const ev of events) {
    if (ev.thread_id !== threadId) continue;
    if (seenEventIds.has(ev.journal_event_id)) continue;
    seenEventIds.add(ev.journal_event_id);
    uniqueEvents.push(ev);
    journalPredsById.set(ev.journal_event_id, ev.causal_predecessors);

    if (ev.event_type === 'main_designated') {
      const data = ev.data as unknown as MainDesignationData;
      const list = mainCandidates.get(data.main_stream_id) ?? [];
      list.push(ev.journal_event_id);
      mainCandidates.set(data.main_stream_id, list);
    } else if (ev.event_type === 'result_receipt_recorded') {
      const data = ev.data as unknown as ResultReceiptData;
      receipts.set(data.receipt_id, data);
    } else if (ev.event_type === 'grant_revoked') {
      const data = ev.data as unknown as GrantRevokedData;
      revokedGrantIds.add(data.grant_id);
      const revList = revocationsByGrant.get(data.grant_id) ?? [];
      revList.push({
        event_id: ev.journal_event_id,
        causal: new Set(ev.causal_predecessors),
      });
      revocationsByGrant.set(data.grant_id, revList);
    } else if (ev.event_type === 'grant_issued') {
      const data = ev.data as unknown as GrantIssuedData;
      const prevGrantEvent = grantIssuerEventIds.get(data.grant_id);
      if (prevGrantEvent) {
        conflicts.push({
          kind: 'grant_scope_expanded',
          target_id: data.grant_id,
          event_ids: [prevGrantEvent, ev.journal_event_id],
          detail: `Duplicate grant_issued with colliding grant_id ${data.grant_id}`,
        });
      } else {
        grantIssuerEventIds.set(data.grant_id, ev.journal_event_id);
        grants.set(data.grant_id, {
          grant: data,
          remaining_spawns: data.max_total_spawns,
          revoked: false,
          revoked_by_event_ids: [],
          children_grant_ids: [],
          used_dispatch_ids: [],
        });
      }
    } else if (
      ev.event_type === 'result_accepted' ||
      ev.event_type === 'result_rejected' ||
      ev.event_type === 'branch_abandoned'
    ) {
      const data = ev.data as unknown as ResultDecisionData;
      const targetId = data.target_id || data.receipt_id;
      const status =
        ev.event_type === 'result_accepted'
          ? 'accepted'
          : ev.event_type === 'result_rejected'
            ? 'rejected'
            : 'abandoned';
      const arr = decisionEventsByTarget.get(targetId) ?? [];
      arr.push({ status, event_id: ev.journal_event_id });
      decisionEventsByTarget.set(targetId, arr);
    }
  }

  // Check main_designated uniqueness invariant
  const mainStreamIds = [...mainCandidates.keys()];
  if (mainStreamIds.length > 1) {
    conflicts.push({
      kind: 'main_designation_conflict',
      target_id: threadId,
      event_ids: [...mainCandidates.values()].flat(),
      detail: `Multiple conflicting main_stream_id designated in thread ${threadId}: ${mainStreamIds.join(', ')}`,
    });
  }

  // Check decision mutex invariant
  for (const [targetId, decs] of decisionEventsByTarget.entries()) {
    const distinctStatuses = new Set(decs.map((d) => d.status));
    if (distinctStatuses.size > 1) {
      conflicts.push({
        kind: 'result_decision_conflict',
        target_id: targetId,
        event_ids: decs.map((d) => d.event_id),
        detail: `Conflicting non-monotone decisions (${[...distinctStatuses].join(' vs ')}) on ${targetId}`,
      });
    } else if (decs.length > 0) {
      decisions.set(targetId, decs[0]);
    }
  }

  // Propagate monotone irreversible revocations down delegation tree
  let changed = true;
  while (changed) {
    changed = false;
    for (const [grantId, gState] of grants.entries()) {
      const directRev = revocationsByGrant.get(grantId);
      const parentState =
        gState.grant.parent_grant_id !== undefined
          ? grants.get(gState.grant.parent_grant_id)
          : undefined;
      const parentRevoked =
        gState.grant.parent_grant_id !== undefined &&
        revokedGrantIds.has(gState.grant.parent_grant_id);
      if ((directRev && directRev.length > 0) || parentRevoked) {
        const mergedRevIds = [
          ...new Set([
            ...(directRev ? directRev.map((r) => r.event_id) : []),
            ...(parentState?.revoked_by_event_ids ?? []),
            ...gState.revoked_by_event_ids,
          ]),
        ];
        if (!gState.revoked || mergedRevIds.length !== gState.revoked_by_event_ids.length) {
          gState.revoked = true;
          revokedGrantIds.add(grantId);
          gState.revoked_by_event_ids = mergedRevIds;
          changed = true;
        }
      }
    }
  }

  // Validate Escrow split / delegation conservation and offline dispatch vs revocation
  for (const ev of uniqueEvents) {
    if (ev.event_type === 'grant_issued') {
      if (grantIssuerEventIds.get((ev.data as unknown as GrantIssuedData).grant_id) !== ev.journal_event_id) {
        continue;
      }
      const data = ev.data as unknown as GrantIssuedData;
      if (!Number.isInteger(data.max_total_spawns) || data.max_total_spawns < 0) {
        conflicts.push({
          kind: 'grant_quota_exceeded',
          target_id: data.grant_id,
          event_ids: [ev.journal_event_id],
          detail: `Grant ${data.grant_id} declares invalid max_total_spawns=${data.max_total_spawns}`,
        });
      }
      if (!Number.isInteger(data.max_depth) || data.max_depth < 0) {
        conflicts.push({
          kind: 'grant_scope_expanded',
          target_id: data.grant_id,
          event_ids: [ev.journal_event_id],
          detail: `Grant ${data.grant_id} declares invalid max_depth=${data.max_depth}`,
        });
      }
      if (data.parent_grant_id) {
        const parent = grants.get(data.parent_grant_id);
        if (!parent) {
          conflicts.push({
            kind: 'grant_scope_expanded',
            target_id: data.grant_id,
            event_ids: [ev.journal_event_id],
            detail: `Child grant ${data.grant_id} references unknown parent grant ${data.parent_grant_id}`,
          });
          continue;
        }
        parent.children_grant_ids.push(data.grant_id);
        if (parent.grant.grantee_stream_id !== data.issuer_stream_id) {
          conflicts.push({
            kind: 'grant_scope_expanded',
            target_id: data.grant_id,
            event_ids: [ev.journal_event_id],
            detail: `Child grant ${data.grant_id} issuer ${data.issuer_stream_id} does not hold parent grant ${parent.grant.grant_id}`,
          });
        }
        if (parent.revoked) {
          const grantAncestors = computeCausalAncestors(ev.journal_event_id);
          const explicitlyAfterRevIds: string[] = [];
          const concurrentRevIds: string[] = [];
          for (const revId of parent.revoked_by_event_ids) {
            if (grantAncestors.has(revId)) {
              explicitlyAfterRevIds.push(revId);
            } else {
              const revAncestors = computeCausalAncestors(revId);
              if (!revAncestors.has(ev.journal_event_id)) {
                concurrentRevIds.push(revId);
              }
            }
          }
          if (explicitlyAfterRevIds.length > 0) {
            conflicts.push({
              kind: 'revoked_grant_used',
              target_id: data.grant_id,
              event_ids: [ev.journal_event_id, ...explicitlyAfterRevIds],
              detail: `Child grant ${data.grant_id} redelegated from already-revoked parent grant ${parent.grant.grant_id}`,
            });
          } else if (concurrentRevIds.length > 0 || parent.revoked_by_event_ids.length === 0) {
            conflicts.push({
              kind: 'offline_dispatch_vs_revocation',
              target_id: data.grant_id,
              event_ids: [ev.journal_event_id, ...concurrentRevIds],
              detail: `Child grant ${data.grant_id} redelegated from parent grant ${parent.grant.grant_id} concurrently with revocation; fail-closed reconciliation required`,
            });
          }
        }
        const parentActions = new Set(parent.grant.allowed_actions);
        const invalidActions = data.allowed_actions.filter((a) => !parentActions.has(a));
        const parentWs = new Set(parent.grant.workspace_scope);
        const invalidWs = parentWs.has('*')
          ? []
          : data.workspace_scope.filter((w) => !parentWs.has(w));
        if (
          !parent.grant.can_redelegate ||
          invalidActions.length > 0 ||
          invalidWs.length > 0 ||
          data.max_depth >= parent.grant.max_depth
        ) {
          conflicts.push({
            kind: 'grant_scope_expanded',
            target_id: data.grant_id,
            event_ids: [ev.journal_event_id],
            detail: `Child grant ${data.grant_id} exceeds parent ${parent.grant.grant_id} scope/depth/redelegation`,
          });
        }
        parent.remaining_spawns -= data.max_total_spawns;
        if (parent.remaining_spawns < 0) {
          conflicts.push({
            kind: 'grant_quota_exceeded',
            target_id: parent.grant.grant_id,
            event_ids: [ev.journal_event_id],
            detail: `Escrow quota exceeded on grant ${parent.grant.grant_id} during split to ${data.grant_id}`,
          });
        }
      }
    } else if (ev.event_type === 'dispatch_committed') {
      const data = ev.data as { dispatch_id: string; grant_id?: string };
      if (data.grant_id) {
        const g = grants.get(data.grant_id);
        if (!g) {
          conflicts.push({
            kind: 'revoked_grant_used',
            target_id: data.dispatch_id,
            event_ids: [ev.journal_event_id],
            detail: `Dispatch ${data.dispatch_id} uses unknown grant ${data.grant_id}`,
          });
          continue;
        }
        g.used_dispatch_ids.push(data.dispatch_id);
        g.remaining_spawns -= 1;
        if (g.remaining_spawns < 0) {
          conflicts.push({
            kind: 'grant_quota_exceeded',
            target_id: g.grant.grant_id,
            event_ids: [ev.journal_event_id],
            detail: `Escrow quota exhausted on grant ${g.grant.grant_id} by dispatch ${data.dispatch_id}`,
          });
        }
        if (g.revoked) {
          const dispAncestors = computeCausalAncestors(ev.journal_event_id);
          const explicitlyAfterRevIds: string[] = [];
          const concurrentRevIds: string[] = [];
          for (const revId of g.revoked_by_event_ids) {
            if (dispAncestors.has(revId)) {
              explicitlyAfterRevIds.push(revId);
            } else {
              const revAncestors = computeCausalAncestors(revId);
              if (!revAncestors.has(ev.journal_event_id)) {
                concurrentRevIds.push(revId);
              }
            }
          }
          if (explicitlyAfterRevIds.length > 0) {
            conflicts.push({
              kind: 'revoked_grant_used',
              target_id: data.dispatch_id,
              event_ids: [ev.journal_event_id, ...explicitlyAfterRevIds],
              detail: `Dispatch ${data.dispatch_id} used already-revoked grant ${g.grant.grant_id}`,
            });
          } else if (concurrentRevIds.length > 0 || g.revoked_by_event_ids.length === 0) {
            conflicts.push({
              kind: 'offline_dispatch_vs_revocation',
              target_id: data.dispatch_id,
              event_ids: [ev.journal_event_id, ...concurrentRevIds],
              detail: `Dispatch ${data.dispatch_id} used grant ${g.grant.grant_id} concurrently with revocation; fail-closed reconciliation required`,
            });
          }
        }
      }
    }
  }

  return {
    thread_id: threadId,
    main_stream_ids: mainStreamIds,
    has_blocking_conflict: conflicts.length > 0,
    conflicts,
    receipts,
    decisions,
    grants,
    revoked_grant_ids: revokedGrantIds,
  };
}

/**
 * Computes causal Lifecycle Projection from monotone Lifecycle Observations.
 * Concurrent contradictory observations deterministically yield `'concurrent'`
 * without selecting winners by wall-clock timestamps.
 */
export function projectLifecycle(
  observations: readonly LifecycleObservation[],
): LifecycleProjectionValue {
  if (observations.length === 0) {
    return 'unknown';
  }
  const byId = new Map<string, LifecycleObservation>();
  const referencedAsPred = new Set<string>();
  for (const obs of observations) {
    byId.set(obs.observation_id, obs);
    for (const p of obs.causal_predecessors) {
      referencedAsPred.add(p);
    }
  }
  const heads = observations.filter((o) => !referencedAsPred.has(o.observation_id));
  if (heads.length === 0) {
    return 'unknown';
  }
  const headStates = new Set(heads.map((h) => h.state));
  if (headStates.size === 1) {
    return heads[0].state;
  }
  return 'concurrent';
}

export interface MailboxMessageStatus {
  message_id: string;
  status: 'pending' | 'offered' | 'committed_receipt';
  offered_batch_id?: string;
}

/**
 * Projects three-state asynchronous mailbox state purely from immutable
 * cross_stream_message records and target-stream notify_receipt records.
 */
export function projectMailboxState(
  targetStreamId: string,
  outboundMessageIds: readonly string[],
  targetReceipts: readonly NotifyReceiptRecord[],
): {
  messages: Map<string, MailboxMessageStatus>;
  validOfferedBatches: Map<string, Set<string>>;
} {
  const messages = new Map<string, MailboxMessageStatus>();
  for (const id of outboundMessageIds) {
    messages.set(id, { message_id: id, status: 'pending' });
  }

  const validOfferedBatches = new Map<string, Set<string>>();
  for (const rec of targetReceipts) {
    if (rec.target_stream_id !== targetStreamId) continue;
    if (rec.receipt_type === 'offered') {
      const set = validOfferedBatches.get(rec.batch_id) ?? new Set<string>();
      for (const mid of rec.message_ids) {
        set.add(mid);
        const cur = messages.get(mid);
        if (cur && cur.status === 'pending') {
          cur.status = 'offered';
          cur.offered_batch_id = rec.batch_id;
        }
      }
      validOfferedBatches.set(rec.batch_id, set);
    }
  }

  for (const rec of targetReceipts) {
    if (rec.target_stream_id !== targetStreamId) continue;
    if (rec.receipt_type === 'committed_receipt') {
      const offeredSet = validOfferedBatches.get(rec.batch_id);
      if (!offeredSet) {
        // Forged or unmatched batch_id: ignore safely
        continue;
      }
      for (const mid of rec.message_ids) {
        if (!offeredSet.has(mid)) continue;
        const cur = messages.get(mid);
        if (cur) {
          cur.status = 'committed_receipt';
          cur.offered_batch_id = rec.batch_id;
        }
      }
    }
  }

  return { messages, validOfferedBatches };
}

/**
 * Validates whether a claimed batch receipt matches an offered batch bound to target_stream_id.
 */
export function verifyMailboxReceiptAck(
  targetStreamId: string,
  batchId: string,
  ackedMessageIds: readonly string[],
  targetReceipts: readonly NotifyReceiptRecord[],
): { valid: boolean; acceptedMessageIds: string[]; reason?: string } {
  const { validOfferedBatches } = projectMailboxState(
    targetStreamId,
    ackedMessageIds,
    targetReceipts,
  );
  const offeredSet = validOfferedBatches.get(batchId);
  if (!offeredSet) {
    return {
      valid: false,
      acceptedMessageIds: [],
      reason: `Batch ${batchId} was never offered to target stream ${targetStreamId}`,
    };
  }
  const accepted = ackedMessageIds.filter((id) => offeredSet.has(id));
  if (accepted.length === 0) {
    return {
      valid: false,
      acceptedMessageIds: [],
      reason: `None of the acknowledged message_ids belong to offered batch ${batchId}`,
    };
  }
  return { valid: true, acceptedMessageIds: accepted };
}

/**
 * Topological pre-analysis helper over verified Lineage anchors.
 * Computes ancestor closures A(p), common ancestors, and maximal common ancestors.
 * Does NOT imply semantic mergeability.
 */
export function computeAncestorClosure(
  anchorId: string,
  lookupParents: (id: string) => readonly string[] | null,
): { ancestors: Set<string>; missingParents: string[] } {
  const ancestors = new Set<string>();
  const missingParents: string[] = [];
  const queue: string[] = [anchorId];
  const visited = new Set<string>();

  while (queue.length > 0) {
    const cur = queue.shift()!;
    if (visited.has(cur)) continue;
    visited.add(cur);
    const parents = lookupParents(cur);
    if (parents === null) {
      missingParents.push(cur);
      continue;
    }
    ancestors.add(cur);
    for (const p of parents) {
      if (!visited.has(p)) {
        queue.push(p);
      }
    }
  }
  return { ancestors, missingParents };
}

export function analyzeCommonAncestors(
  anchorIds: readonly string[],
  lookupParents: (id: string) => readonly string[] | null,
): {
  union: Set<string>;
  common: Set<string>;
  maximalCommon: string[];
  perAnchor: Map<string, Set<string>>;
} {
  const perAnchor = new Map<string, Set<string>>();
  const union = new Set<string>();
  for (const id of anchorIds) {
    const { ancestors } = computeAncestorClosure(id, lookupParents);
    perAnchor.set(id, ancestors);
    for (const a of ancestors) union.add(a);
  }
  const common = new Set<string>();
  if (anchorIds.length > 0) {
    const first = perAnchor.get(anchorIds[0])!;
    for (const cand of first) {
      if (anchorIds.every((id) => perAnchor.get(id)!.has(cand))) {
        common.add(cand);
      }
    }
  }
  // An ancestor in `common` is maximal if no other node in `common` has it as a strict parent
  const nonMaximal = new Set<string>();
  for (const c of common) {
    const parents = lookupParents(c) ?? [];
    for (const p of parents) {
      if (common.has(p)) nonMaximal.add(p);
    }
  }
  const maximalCommon = [...common].filter((c) => !nonMaximal.has(c)).sort();
  return { union, common, maximalCommon, perAnchor };
}
