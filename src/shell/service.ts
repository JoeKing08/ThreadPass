import fs from 'node:fs';
import path from 'node:path';
import {
  assertSafeIdentifier,
  canonicalJson,
  canonicalSha256,
  deterministicEventId,
  newId,
  sha256Hex,
} from '../core/canonical.js';
import {
  analyzeCommonAncestors,
  projectJournalState,
  verifyMailboxReceiptAck,
} from '../core/state-machine.js';
import {
  SCHEMA_VERSION,
  type AdoptedSourceManifest,
  type ConfirmationProof,
  type ControlRecord,
  type DispatchRecoveryCapsule,
  type EventRecord,
  type GrantIssuedData,
  type InputDependencyManifest,
  type JournalEventRecord,
  type LineageAnchor,
  type NotifyReceiptRecord,
  type ReconciliationRecord,
  type StreamShardLine,
  type TrustTier,
} from '../core/types.js';
import {
  pinTrustedDeviceKey,
  updateTrustedDeviceKeyStatus,
} from '../profile/local-offline/crypto-keys.js';
import { assertOutboundEgressSafe } from '../profile/local-offline/redaction.js';
import { StateStore } from '../profile/local-offline/state-store.js';
import { LocalOfflineEngine, type FsckReport } from '../profile/local-offline/txn-engine.js';
import { type HostBudgetSpec, type PackedEnvelopeResult } from './budget.js';
import {
  buildEventsFromAdoptedPackage,
  parseExternalConversationInput,
  type ParsedAdoptPackage,
} from './ir.js';
import {
  buildCurrentRoleBriefing,
  generateBatchId,
  packHandoffEnvelope,
  packReconcileEnvelope,
  parseReconcileReceipt,
  renderAdoptExtractPrompt,
  renderCheckpointPrompt,
  type ReconcileSourceInput,
} from './prompts.js';
import {
  computeTrustTier,
  normalizeConfirmationText,
  renderNonInstructionFence,
  TRUST_ADAPTER_VERSION,
} from './trust.js';
import { captureWorkspaceManifest } from './workspace.js';

export class DuplicateAdoptionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'DuplicateAdoptionError';
  }
}

export class ForeignCanonicalDetectedError extends Error {
  constructor(
    message: string,
    public readonly originThreadId: string,
    public readonly originStreamId: string,
  ) {
    super(message);
    this.name = 'ForeignCanonicalDetectedError';
  }
}

export class ReleaseGateBlockedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ReleaseGateBlockedError';
  }
}

export class ThreadPassService {
  public readonly engine: LocalOfflineEngine;

  constructor(engineOptions?: ConstructorParameters<typeof LocalOfflineEngine>[0]) {
    this.engine = new LocalOfflineEngine(engineOptions);
  }

  private sanitizeTextList(items: readonly string[]): { items: string[]; redactedCount: number } {
    let redactedCount = 0;
    const out: string[] = [];
    for (const item of items) {
      const r = this.engine.redactor.sanitizeText(item);
      redactedCount += r.redactedCount;
      out.push(r.sanitized);
    }
    return { items: out, redactedCount };
  }

  private sanitizeAdoptPackage(pkg: ParsedAdoptPackage): {
    pkg: ParsedAdoptPackage;
    redactedCount: number;
  } {
    let total = 0;
    const sumRes = this.engine.redactor.sanitizeText(pkg.summary_text);
    total += sumRes.redactedCount;
    const quotesRes = this.sanitizeTextList(pkg.claimed_user_quotes);
    total += quotesRes.redactedCount;
    const decRes = this.sanitizeTextList(pkg.decisions_made);
    total += decRes.redactedCount;
    const filesRes = this.sanitizeTextList(pkg.files_and_artifacts);
    total += filesRes.redactedCount;
    const tasksRes = this.sanitizeTextList(pkg.pending_tasks);
    total += tasksRes.redactedCount;
    const blockersRes = this.sanitizeTextList(pkg.blockers);
    total += blockersRes.redactedCount;
    const qRes = this.sanitizeTextList(pkg.open_questions);
    total += qRes.redactedCount;
    const errRes = this.sanitizeTextList(pkg.tool_errors);
    total += errRes.redactedCount;
    const turns = pkg.recent_turns.map((t) => {
      const tr = this.engine.redactor.sanitizeText(t.text);
      total += tr.redactedCount;
      return { ...t, text: tr.sanitized };
    });

    return {
      redactedCount: total,
      pkg: {
        ...pkg,
        summary_text: sumRes.sanitized,
        claimed_user_quotes: quotesRes.items,
        decisions_made: decRes.items,
        files_and_artifacts: filesRes.items,
        pending_tasks: tasksRes.items,
        blockers: blockersRes.items,
        open_questions: qRes.items,
        tool_errors: errRes.items,
        recent_turns: turns,
      },
    };
  }

  initThread(params: {
    projectId: string;
    threadId?: string;
    mainStreamId?: string;
    workspaceDir?: string;
    summary?: string;
  }): {
    project_id: string;
    thread_id: string;
    main_stream_id: string;
    anchor_id: string;
    txn_id: string;
  } {
    assertSafeIdentifier('project_id', params.projectId);
    const threadId = assertSafeIdentifier('thread_id', params.threadId ?? newId('thr', 8));
    const mainStreamId = assertSafeIdentifier(
      'main_stream_id',
      params.mainStreamId ?? newId('main', 6),
    );
    const anchorId = newId('anc', 8);
    const nowIso = this.engine.clock.wallIso();
    const ws = captureWorkspaceManifest(params.workspaceDir);
    const initEventId = deterministicEventId(mainStreamId, `init|${threadId}|${anchorId}`);

    const anchor: LineageAnchor = {
      anchor_id: anchorId,
      thread_id: threadId,
      stream_id: mainStreamId,
      parents: [],
      workspace_manifest: ws,
      summary: params.summary ?? 'ThreadPass Main stream initialized',
      head_event_ids: [initEventId],
      created_at: nowIso,
    };

    const initEvent: EventRecord = {
      schema_version: SCHEMA_VERSION,
      kind: 'event',
      event_id: initEventId,
      stream_id: mainStreamId,
      seq: 1,
      commit_txn_id: 'pending',
      trust_tier: 'human_authority',
      provenance: {
        capture_origin: 'thp_cli',
        verification_method: 'local_cli_direct',
        source_event_ref: 'init',
        adapter_version: TRUST_ADAPTER_VERSION,
      },
      created_at: nowIso,
      payload: {
        type: 'human_message',
        text: params.summary ?? 'Initialize ThreadPass Main coordination stream.',
      },
    };

    const mainDesigEvent: JournalEventRecord = {
      schema_version: SCHEMA_VERSION,
      journal_event_id: newId('je', 8),
      thread_id: threadId,
      main_stream_id: mainStreamId,
      event_type: 'main_designated',
      actor_stream_id: mainStreamId,
      causal_predecessors: [],
      commit_txn_id: 'pending',
      coordinator_epoch: 1,
      created_at: nowIso,
      data: { main_stream_id: mainStreamId },
    };

    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: threadId,
      is_new_thread: true,
      main_stream_id: mainStreamId,
      expected_journal_head: null,
      affected_streams: [
        {
          stream_id: mainStreamId,
          expected_parent_head: null,
          anchor,
          events: [initEvent],
        },
      ],
      journal_events: [mainDesigEvent],
    });

    return {
      project_id: params.projectId,
      thread_id: threadId,
      main_stream_id: mainStreamId,
      anchor_id: anchorId,
      txn_id: res.txn_id,
    };
  }

  getStatus(projectId: string, threadId: string) {
    const store = this.engine.openExistingStore(projectId, threadId);
    try {
      const thread = store.getThread(threadId);
      const streams = store.listStreams(threadId);
      const anchors = store.listAnchors(threadId);
      const journal = store.listJournalEvents(threadId);
      const quarantined = store.listQuarantined();
      const pendingMailbox = store.db
        .prepare("SELECT * FROM mailbox_proj WHERE status != 'committed_receipt'")
        .all();
      return {
        device_id: this.engine.identity.device_id,
        project_id: projectId,
        thread,
        streams,
        anchors,
        journal_count: journal.length,
        quarantined,
        pending_mailbox: pendingMailbox,
      };
    } finally {
      store.close();
    }
  }

  getAdoptExtractPrompt(sourceLabel?: string): string {
    return renderAdoptExtractPrompt(sourceLabel);
  }

  getCheckpointPrompt(
    projectId: string,
    threadId: string,
    streamId?: string,
  ): {
    prompt: string;
    thread_id: string;
    stream_id: string;
    anchor_id: string;
    recent_event_ids: string[];
    pending_batch_id?: string;
    pending_message_ids?: string[];
  } {
    const store = this.engine.openExistingStore(projectId, threadId);
    let targetStreamId: string;
    let headAnchorId: string;
    let isMain: boolean;
    let pendingBatchId: string | undefined;
    let pendingMessageIds: string[] | undefined;

    try {
      const threadRow = store.getThread(threadId);
      if (!threadRow) throw new Error(`Thread ${threadId} not found`);
      targetStreamId = streamId ?? threadRow.main_stream_id;
      const sRow = store.getStream(targetStreamId);
      if (!sRow || !sRow.head_anchor_id) {
        throw new Error(`Stream ${targetStreamId} has no published head anchor`);
      }
      headAnchorId = sRow.head_anchor_id;
      isMain = sRow.is_main === 1 || targetStreamId === threadRow.main_stream_id;

      const offeredRows = store.db
        .prepare(
          `SELECT message_id, batch_id FROM mailbox_proj WHERE target_stream_id = ? AND status = 'offered'`,
        )
        .all(targetStreamId) as Array<{ message_id: string; batch_id: string | null }>;
      if (offeredRows.length > 0 && offeredRows[0].batch_id) {
        pendingBatchId = offeredRows[0].batch_id;
        pendingMessageIds = offeredRows
          .filter((r) => r.batch_id === pendingBatchId)
          .map((r) => r.message_id);
      }
    } finally {
      store.close();
    }

    const lines = this.engine.readCommittedStreamLines(projectId, threadId, targetStreamId);
    const recentEventIds = lines
      .filter((l): l is EventRecord => l.kind === 'event')
      .slice(-6)
      .map((e) => e.event_id);

    const prompt = renderCheckpointPrompt({
      threadId,
      streamId: targetStreamId,
      anchorId: headAnchorId,
      role: isMain ? 'main_coordinator' : 'scoped_worker',
      recentEventIds,
      pendingBatchId,
      pendingMessageIds,
    });

    return {
      prompt,
      thread_id: threadId,
      stream_id: targetStreamId,
      anchor_id: headAnchorId,
      recent_event_ids: recentEventIds,
      pending_batch_id: pendingBatchId,
      pending_message_ids: pendingMessageIds,
    };
  }

  /**
   * Adopts an external AI conversation (`unmanaged_external` or explicit `foreign_canonical`),
   * enforcing 3-layer pre-ingestion anti-re-adoption lookup, streaming redaction,
   * strict TrustTier derivation, and atomic immutable transaction publication.
   */
  adoptExternal(params: {
    projectId: string;
    threadId: string;
    rawText: string;
    sourceLabel?: string;
    streamId?: string;
    hostSessionId?: string;
    confirmationProof?: ConfirmationProof;
    allowForeignCanonical?: boolean;
    workspaceDir?: string;
  }): {
    stream_id: string;
    anchor_id: string;
    txn_id: string;
    trust_tier: TrustTier;
    mode: string;
    continuity_level: string;
    warnings: string[];
    redacted_count: number;
  } {
    assertSafeIdentifier('project_id', params.projectId);
    assertSafeIdentifier('thread_id', params.threadId);
    if (!params.rawText.trim()) {
      throw new Error('adoptExternal requires non-empty rawText');
    }
    const warnings: string[] = [];
    const label = params.sourceLabel ?? 'external_ai';
    const pkg = parseExternalConversationInput(params.rawText, label);
    if (pkg.parse_warnings.length > 0) {
      warnings.push(...pkg.parse_warnings);
    }
    const streamId = assertSafeIdentifier(
      'stream_id',
      params.streamId ??
        `adopt_${label.toLowerCase().replace(/[^a-z0-9_-]+/g, '_').slice(0, 20)}_${sha256Hex(params.rawText).slice(0, 8)}`,
    );

    const sanitizedRawRes = this.engine.redactor.sanitizeText(pkg.raw_text);
    const sanitizedPkgText = sanitizedRawRes.sanitized;

    // Verify confirmation proof against exact raw text first, then sanitize all fields persisted at rest
    const exactSha = sha256Hex(normalizeConfirmationText(params.rawText));
    const verificationMethod =
      pkg.mode === 'model_authored' ? 'model_reported' : 'unverified_text_import';
    const provenance = {
      capture_origin: `adopt_paste:${label}`,
      verification_method: verificationMethod as 'model_reported' | 'unverified_text_import',
      source_event_ref: `adopt:${label}`,
      adapter_version: TRUST_ADAPTER_VERSION,
      confirmation_proof: params.confirmationProof,
    };
    const trustTier = computeTrustTier(provenance, exactSha);

    const sanitizedPkgRes = this.sanitizeAdoptPackage(pkg);
    const safePkg = sanitizedPkgRes.pkg;

    const anchorId = newId('anc', 8);
    const nowIso = this.engine.clock.wallIso();
    const { events, invocation } = buildEventsFromAdoptedPackage({
      streamId,
      txnId: 'pending',
      pkg: safePkg,
      trustTier,
      provenance,
      startSeq: 1,
      nowIso,
    });

    // 3-layer pre-ingestion lookup across SQLite
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let threadRow: ReturnType<typeof store.getThread>;
    let existingStream: ReturnType<typeof store.getStream>;
    try {
      threadRow = store.getThread(params.threadId);
      if (!threadRow) {
        throw new Error(`Thread ${params.threadId} is not initialized in project ${params.projectId}`);
      }
      const existingJournal = store.listJournalEvents(params.threadId);
      if (threadRow.journal_head_id === null && existingJournal.length > 0) {
        throw new Error(
          `Thread ${params.threadId} has unresolved divergent Coordinator Journal heads; run reconcile commit before adopting new streams`,
        );
      }
      existingStream = store.getStream(streamId);
      if (existingStream) {
        throw new DuplicateAdoptionError(
          `Stream ${streamId} already exists in thread ${params.threadId}; use save/harvest/reconcile instead of duplicate adopt`,
        );
      }

      const hostId = params.hostSessionId ?? pkg.host_session_id;
      if (hostId) {
        const hb = store.lookupHostBinding(hostId);
        if (hb && hb.thread_id === params.threadId) {
          throw new DuplicateAdoptionError(
            `host_session_id ${hostId} is already bound to stream ${hb.stream_id} in same thread ${params.threadId}`,
          );
        }
      }

      const fpHits = store.lookupEventFingerprints(events.map((e) => e.event_id));
      if (fpHits.length > 0) {
        const same = fpHits.find((h) => h.thread_id === params.threadId);
        if (same) {
          throw new DuplicateAdoptionError(
            `Events already exist in same thread ${params.threadId} (stream ${same.stream_id}); duplicate wild adopt rejected`,
          );
        }
      }

      // Cross-thread lookup across other threads under the same project
      const threadsRoot = path.join(this.engine.homeDir, 'projects', params.projectId, 'threads');
      if (fs.existsSync(threadsRoot)) {
        for (const otherTid of fs.readdirSync(threadsRoot)) {
          if (otherTid === params.threadId) continue;
          const otherDb = path.join(threadsRoot, otherTid, 'state.sqlite');
          if (!fs.existsSync(otherDb)) continue;
          const otherStore = new StateStore(otherDb);
          try {
            if (hostId) {
              const otherHb = otherStore.lookupHostBinding(hostId);
              if (otherHb && !params.allowForeignCanonical) {
                throw new ForeignCanonicalDetectedError(
                  `host_session_id ${hostId} belongs to another canonical thread ${otherHb.thread_id} (${otherHb.stream_id}); re-run as foreign_canonical fixed snapshot without stealing active host binding`,
                  otherHb.thread_id,
                  otherHb.stream_id,
                );
              }
            }
            const otherFpHits = otherStore.lookupEventFingerprints(
              events.map((e) => e.event_id),
            );
            if (otherFpHits.length > 0 && !params.allowForeignCanonical) {
              const foreign = otherFpHits[0];
              throw new ForeignCanonicalDetectedError(
                `Events match existing canonical stream ${foreign.stream_id} in thread ${foreign.thread_id}`,
                foreign.thread_id,
                foreign.stream_id,
              );
            }
          } finally {
            otherStore.close();
          }
        }
      }

      if (pkg.referenced_ids.length > 0) {
        warnings.push(
          `Detected suspected unsynced ThreadPass IDs in pasted text (${pkg.referenced_ids.slice(0, 4).join(', ')}); ignored self-claimed identity and tagged as ${trustTier}`,
        );
      }
    } finally {
      store.close();
    }

    const ws = captureWorkspaceManifest(params.workspaceDir);
    const snapshotSha = sha256Hex(Buffer.from(sanitizedPkgText, 'utf8'));
    const snapshotRel = path.posix.join(
      'blobs',
      'objects',
      snapshotSha.slice(0, 2),
      snapshotSha,
    );

    const adoptedManifest: AdoptedSourceManifest = {
      adopted_manifest_id: newId('adp', 8),
      sources: [
        {
          source_kind: params.allowForeignCanonical ? 'foreign_canonical' : 'unmanaged_external',
          source_project_id: params.projectId,
          source_thread_id: params.threadId,
          source_stream_id: streamId,
          source_anchor_id: anchorId,
          ancestor_ids: [],
          snapshot_blob_ref: snapshotSha,
          manifest_sha256: snapshotSha,
          completeness: pkg.mode === 'evidence_based' ? 'partial' : 'complete',
          trust_tier: trustTier,
        },
      ],
    };

    const anchor: LineageAnchor = {
      anchor_id: anchorId,
      thread_id: params.threadId,
      stream_id: streamId,
      parents: [], // Independent wild root stream
      workspace_manifest: ws,
      summary: safePkg.summary_text || `Adopted ${label} (${safePkg.continuity_level})`,
      head_event_ids: events.map((e) => e.event_id),
      created_at: nowIso,
    };

    const adoptedCtrl: ControlRecord = {
      schema_version: SCHEMA_VERSION,
      kind: 'control',
      control_id: newId('ctrl', 8),
      stream_id: streamId,
      commit_txn_id: 'pending',
      created_at: nowIso,
      payload: {
        type: 'adopted_source_manifest',
        manifest: adoptedManifest,
      },
    };

    const journalEv: JournalEventRecord = {
      schema_version: SCHEMA_VERSION,
      journal_event_id: newId('je', 8),
      thread_id: params.threadId,
      main_stream_id: threadRow!.main_stream_id,
      event_type: 'adoption_committed',
      actor_stream_id: threadRow!.main_stream_id,
      causal_predecessors: threadRow!.journal_head_id ? [threadRow!.journal_head_id] : [],
      commit_txn_id: 'pending',
      coordinator_epoch: threadRow!.coordinator_epoch + 1,
      created_at: nowIso,
      data: {
        adopted_stream_id: streamId,
        adopted_anchor_id: anchorId,
        source_label: label,
        mode: pkg.mode,
        trust_tier: trustTier,
        snapshot_blob_ref: snapshotRel,
      },
    };

    const hostIdToBind = !params.allowForeignCanonical
      ? (params.hostSessionId ?? pkg.host_session_id)
      : undefined;

    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: params.threadId,
      expected_journal_head: threadRow!.journal_head_id,
      affected_streams: [
        {
          stream_id: streamId,
          expected_parent_head: null,
          anchor,
          events,
          invocations: [invocation],
          controls: [adoptedCtrl],
        },
      ],
      journal_events: [journalEv],
      blobs: {
        adopt_snapshot: {
          bytes: sanitizedPkgText,
          sync_policy: 'approved',
          media_type: 'text/plain',
        },
      },
      host_bindings: hostIdToBind
        ? [{ host_session_id: hostIdToBind, stream_id: streamId }]
        : undefined,
    });

    return {
      stream_id: streamId,
      anchor_id: anchorId,
      txn_id: res.txn_id,
      trust_tier: trustTier,
      mode: pkg.mode,
      continuity_level: pkg.continuity_level,
      warnings,
      redacted_count: Math.max(sanitizedRawRes.redactedCount, sanitizedPkgRes.redactedCount),
    };
  }

  /**
   * Builds an explicit multi-source Reconciliation Prompt envelope (e.g. merging 7 adopted AIs into Main),
   * deduplicating shared blocks, enforcing budget ceilings, running the Outbound Egress Gate,
   * and optionally exporting `.md` attachments.
   */
  packReconcile(params: {
    projectId: string;
    threadId: string;
    sourceStreamIds?: string[];
    mainGoal?: string;
    workspaceDir?: string;
    budget?: HostBudgetSpec;
    writeAttachment?: boolean;
  }): PackedEnvelopeResult & {
    common_ancestors: string[];
    source_anchors: Array<{ stream_id: string; anchor_id: string }>;
    attachment_path?: string;
  } {
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let threadRow: ReturnType<typeof store.getThread>;
    let sources: ReconcileSourceInput[];
    let commonAncestors: string[] = [];

    try {
      threadRow = store.getThread(params.threadId);
      if (!threadRow) {
        throw new Error(`Thread ${params.threadId} not found`);
      }
      const allStreams = store.listStreams(params.threadId);
      const selectedIds =
        params.sourceStreamIds && params.sourceStreamIds.length > 0
          ? params.sourceStreamIds
          : allStreams
              .filter((s) => s.stream_id !== threadRow!.main_stream_id)
              .map((s) => s.stream_id);

      if (selectedIds.length === 0) {
        throw new Error('No source streams available to reconcile');
      }

      sources = [];
      for (const sid of selectedIds) {
        const sRow = store.getStream(sid);
        if (!sRow) {
          throw new Error(`Source stream ${sid} not found`);
        }
        if (sRow.health === 'quarantined' || sRow.health === 'divergent_heads') {
          throw new Error(
            `Source stream ${sid} has health=${sRow.health}; resolve fsck/divergent heads before reconciling`,
          );
        }
        if (!sRow.head_anchor_id) {
          throw new Error(`Source stream ${sid} has no published head anchor`);
        }
        const anchor = store.getAnchor(sRow.head_anchor_id);
        if (!anchor) {
          throw new Error(`Head anchor ${sRow.head_anchor_id} not found for ${sid}`);
        }
        const lines = this.engine.readCommittedStreamLines(
          params.projectId,
          params.threadId,
          sid,
        );
        const events = lines.filter((l): l is EventRecord => l.kind === 'event');
        const highestTrust = events.some((e) => e.trust_tier === 'human_authority')
          ? 'human_authority'
          : events[0]?.trust_tier ?? 'untrusted_peer';

        sources.push({
          streamId: sid,
          anchor,
          events,
          trustTier: highestTrust,
          completeness: sRow.health,
          sourceLabel: sid,
        });
      }

      const preAnalysis = analyzeCommonAncestors(
        sources.map((s) => s.anchor.anchor_id),
        (id) => store.getAnchor(id)?.parents ?? null,
      );
      commonAncestors = [...preAnalysis.common];
    } finally {
      store.close();
    }

    const targetWs = captureWorkspaceManifest(params.workspaceDir);
    const packed = packReconcileEnvelope({
      threadId: params.threadId,
      mainStreamId: threadRow.main_stream_id,
      sources,
      mainGoal:
        params.mainGoal ??
        'Synthesize the selected independent AI sessions into a single unified plan and engineering checklist as ThreadPass Main.',
      targetWorkspace: targetWs,
      budget: params.budget,
    });

    // Enforce Outbound Egress Gate
    assertOutboundEgressSafe(this.engine.redactor, packed.full_markdown);

    let attachmentPath: string | undefined;
    if (params.writeAttachment) {
      const layout = this.engine.getLayout(params.projectId, params.threadId);
      fs.mkdirSync(layout.bundlesDir, { recursive: true });
      attachmentPath = path.join(
        layout.bundlesDir,
        `reconcile.${packed.envelope_id}.md`,
      );
      fs.writeFileSync(attachmentPath, packed.full_markdown, 'utf8');
    }

    return {
      ...packed,
      common_ancestors: commonAncestors,
      source_anchors: sources.map((s) => ({
        stream_id: s.streamId,
        anchor_id: s.anchor.anchor_id,
      })),
      attachment_path: attachmentPath,
    };
  }

  /**
   * Commits an explicit ReconciliationReceipt back onto Main, writing an immutable
   * `reconciliation_record`, `input_dependency_manifest`, `coordinator_inject` event,
   * and `reconciliation_committed` Coordinator Journal event.
   */
  commitReconcile(params: {
    projectId: string;
    threadId: string;
    sourceStreamIds: string[];
    receiptRawText: string;
    confirmationProof?: ConfirmationProof;
    workspaceDir?: string;
  }): {
    reconciliation_id: string;
    main_stream_id: string;
    anchor_id: string;
    txn_id: string;
    conflicts_count: number;
    parse_warning?: string;
  } {
    if (!params.receiptRawText.trim()) {
      throw new Error('commitReconcile requires non-empty receiptRawText');
    }
    const parsed = parseReconcileReceipt(params.receiptRawText);
    const exactReceiptSha = sha256Hex(normalizeConfirmationText(params.receiptRawText));
    const sanitizedSummary = this.engine.redactor.sanitizeText(parsed.synthesis_summary).sanitized;
    const sanitizedAdopted = this.sanitizeTextList(parsed.adopted_items).items;
    const sanitizedPartial = this.sanitizeTextList(parsed.partial_items).items;
    const sanitizedExcluded = this.sanitizeTextList(parsed.excluded_items).items;
    const sanitizedProvenanceRefs = this.sanitizeTextList(parsed.provenance_only_refs).items;
    const sanitizedNextActions = this.sanitizeTextList(parsed.next_actions).items;
    const sanitizedConflicts = parsed.conflicts.map((c) => ({
      ...c,
      topic: this.engine.redactor.sanitizeText(c.topic).sanitized,
      positions: this.sanitizeTextList(c.positions).items,
      resolution: this.engine.redactor.sanitizeText(c.resolution).sanitized,
    }));
    const store = this.engine.openExistingStore(params.projectId, params.threadId);

    let threadRow: ReturnType<typeof store.getThread>;
    let mainRow: ReturnType<typeof store.getStream>;
    let inputSources: ReconciliationRecord['input_sources'] = [];
    let depItems: InputDependencyManifest['items'] = [];
    let journalCausalPreds: string[] = [];
    let mainAnchorParents: string[] = [];

    try {
      threadRow = store.getThread(params.threadId);
      if (!threadRow) throw new Error(`Thread ${params.threadId} not found`);
      mainRow = store.getStream(threadRow.main_stream_id);
      if (!mainRow) throw new Error(`Main stream ${threadRow.main_stream_id} not found`);
      if (mainRow.health === 'quarantined') {
        throw new Error(
          `Main stream ${mainRow.stream_id} has health=quarantined; resolve fsck before reconciling`,
        );
      }

      if (mainRow.health === 'divergent_heads') {
        const mainAnchors = store
          .listAnchors(params.threadId)
          .filter((a) => a.stream_id === mainRow!.stream_id);
        const mainPredIds = new Set<string>();
        for (const ma of mainAnchors) {
          for (const p of ma.parents) mainPredIds.add(p);
        }
        mainAnchorParents = mainAnchors
          .filter((ma) => !mainPredIds.has(ma.anchor_id))
          .map((ma) => ma.anchor_id)
          .sort();
      } else if (mainRow.head_anchor_id) {
        mainAnchorParents = [mainRow.head_anchor_id];
      }

      if (threadRow.journal_head_id) {
        journalCausalPreds = [threadRow.journal_head_id];
      } else {
        // When divergent journal heads cleared journal_head_id, join all current leaf heads so reconciliation heals the DAG
        const jEvents = store.listJournalEvents(params.threadId);
        const referencedPreds = new Set<string>();
        for (const je of jEvents) {
          for (const p of je.causal_predecessors) referencedPreds.add(p);
        }
        journalCausalPreds = jEvents
          .filter((je) => !referencedPreds.has(je.journal_event_id))
          .map((je) => je.journal_event_id)
          .sort();
      }

      for (const sid of params.sourceStreamIds) {
        const sRow = store.getStream(sid);
        if (!sRow) {
          throw new Error(`Source stream ${sid} not found`);
        }
        if (sRow.health === 'quarantined' || sRow.health === 'divergent_heads') {
          throw new Error(
            `Reconcile source ${sid} has health=${sRow.health}; resolve fsck/divergent heads before reconciling`,
          );
        }
        if (!sRow.head_anchor_id) {
          throw new Error(`Reconcile source ${sid} missing published head`);
        }
        const a = store.getAnchor(sRow.head_anchor_id);
        if (!a) throw new Error(`Anchor ${sRow.head_anchor_id} missing`);
        inputSources.push({
          stream_id: sid,
          anchor_id: a.anchor_id,
          completeness: sRow.health,
        });
        depItems.push({
          id: newId('dep', 6),
          ref_type: 'anchor',
          target_id: a.anchor_id,
          stream_id: sid,
          thread_id: params.threadId,
          sha256: canonicalSha256(a),
          observability: 'exact',
          required_for_recovery: true,
        });
      }
    } finally {
      store.close();
    }

    const nowIso = this.engine.clock.wallIso();
    const reconciliationId = newId('rec', 8);
    const newAnchorId = newId('anc', 8);
    const ws = captureWorkspaceManifest(params.workspaceDir);
    const depManifest: InputDependencyManifest = {
      input_dependency_id: newId('idm', 8),
      items: depItems,
    };

    const recRecord: ReconciliationRecord = {
      reconciliation_id: reconciliationId,
      thread_id: params.threadId,
      output_stream_id: threadRow.main_stream_id,
      output_anchor_id: newAnchorId,
      input_sources: inputSources,
      adopted_items: sanitizedAdopted,
      partial_items: sanitizedPartial,
      excluded_items: sanitizedExcluded,
      provenance_only_refs: sanitizedProvenanceRefs,
      conflicts: sanitizedConflicts,
      next_actions: sanitizedNextActions,
      strategy_id: 'explicit_human_or_main_reconciliation',
      strategy_version: SCHEMA_VERSION,
      input_manifest_hash: canonicalSha256(depManifest),
      confirmation_proof: params.confirmationProof,
      created_at: nowIso,
    };

    const nextSeq = mainRow.last_seq + 1;
    const injectEventId = deterministicEventId(
      threadRow.main_stream_id,
      `reconcile|${reconciliationId}`,
    );

    const reconcileProvenance = {
      capture_origin: 'thp_reconcile_commit',
      verification_method: 'model_reported' as const,
      source_event_ref: reconciliationId,
      adapter_version: TRUST_ADAPTER_VERSION,
      confirmation_proof: params.confirmationProof,
    };
    const reconcileTrustTier = computeTrustTier(reconcileProvenance, exactReceiptSha);

    const injectEvent: EventRecord = {
      schema_version: SCHEMA_VERSION,
      kind: 'event',
      event_id: injectEventId,
      stream_id: threadRow.main_stream_id,
      seq: nextSeq,
      commit_txn_id: 'pending',
      trust_tier: reconcileTrustTier,
      provenance: reconcileProvenance,
      created_at: nowIso,
      payload: {
        type: 'coordinator_inject',
        handoff_mode: 'model_authored',
        continuity_level: 'Prompt Reconstruction',
        envelope_hash: recRecord.input_manifest_hash,
        summary_text: sanitizedSummary,
        conflict_manifest_ref: reconciliationId,
      },
    };

    const anchor: LineageAnchor = {
      anchor_id: newAnchorId,
      thread_id: params.threadId,
      stream_id: threadRow.main_stream_id,
      parents: mainAnchorParents,
      workspace_manifest: ws,
      reconciliation_record_ref: reconciliationId,
      summary: sanitizedSummary.slice(0, 600),
      head_event_ids: [injectEventId],
      input_dependency_refs: [depManifest.input_dependency_id],
      provenance_refs: sanitizedProvenanceRefs,
      created_at: nowIso,
    };

    const recControl: ControlRecord = {
      schema_version: SCHEMA_VERSION,
      kind: 'control',
      control_id: newId('ctrl', 8),
      stream_id: threadRow.main_stream_id,
      commit_txn_id: 'pending',
      created_at: nowIso,
      payload: { type: 'reconciliation_record', record: recRecord },
    };

    const journalEv: JournalEventRecord = {
      schema_version: SCHEMA_VERSION,
      journal_event_id: newId('je', 8),
      thread_id: params.threadId,
      main_stream_id: threadRow.main_stream_id,
      event_type: 'reconciliation_committed',
      actor_stream_id: threadRow.main_stream_id,
      causal_predecessors: journalCausalPreds,
      commit_txn_id: 'pending',
      coordinator_epoch: threadRow.coordinator_epoch + 1,
      created_at: nowIso,
      data: {
        reconciliation_id: reconciliationId,
        output_anchor_id: newAnchorId,
        input_sources: inputSources,
        conflicts_count: sanitizedConflicts.length,
      },
    };

    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: params.threadId,
      expected_journal_head: threadRow.journal_head_id,
      affected_streams: [
        {
          stream_id: threadRow.main_stream_id,
          expected_parent_head: mainRow.head_anchor_id,
          anchor,
          events: [injectEvent],
          controls: [recControl],
        },
      ],
      journal_events: [journalEv],
      input_dependency_manifests: [depManifest],
      provenance_refs: sanitizedProvenanceRefs,
    });

    return {
      reconciliation_id: reconciliationId,
      main_stream_id: threadRow.main_stream_id,
      anchor_id: newAnchorId,
      txn_id: res.txn_id,
      conflicts_count: sanitizedConflicts.length,
      ...(parsed.parse_warning ? { parse_warning: parsed.parse_warning } : {}),
    };
  }

  /**
   * Packs a handoff prompt for continuing any canonical stream (including Main after reconcile),
   * attaching any pending asynchronous mailbox messages with a committed `offered` batch_id.
   */
  packHandoff(params: {
    projectId: string;
    threadId: string;
    streamId?: string;
    goalOverride?: string;
    workspaceDir?: string;
    budget?: HostBudgetSpec;
    writeAttachment?: boolean;
  }): PackedEnvelopeResult & {
    stream_id: string;
    anchor_id: string;
    offered_batch_id?: string;
    attachment_path?: string;
  } {
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let targetStreamId: string;
    let isMain: boolean;
    let headAnchor: LineageAnchor;
    let pendingMsgs: Array<{
      message_id: string;
      from_stream_id: string;
      status: string;
      batch_id: string | null;
      lease_until_mono_ns: string | null;
      payload_json: string;
    }> = [];

    try {
      const threadRow = store.getThread(params.threadId);
      if (!threadRow) throw new Error(`Thread ${params.threadId} not found`);
      targetStreamId = params.streamId ?? threadRow.main_stream_id;
      const sRow = store.getStream(targetStreamId);
      if (!sRow) {
        throw new Error(`Stream ${targetStreamId} not found`);
      }
      if (sRow.health === 'quarantined' || sRow.health === 'divergent_heads') {
        throw new Error(
          `Stream ${targetStreamId} has health=${sRow.health}; resolve before handoff`,
        );
      }
      if (!sRow.head_anchor_id) {
        throw new Error(`Stream ${targetStreamId} missing head anchor`);
      }
      const anc = store.getAnchor(sRow.head_anchor_id);
      if (!anc) throw new Error(`Anchor ${sRow.head_anchor_id} missing`);
      headAnchor = anc;
      isMain = sRow.is_main === 1 || targetStreamId === threadRow.main_stream_id;

      pendingMsgs = store.db
        .prepare(
          `SELECT message_id, from_stream_id, status, batch_id, lease_until_mono_ns, payload_json FROM mailbox_proj
           WHERE target_stream_id = ? AND status IN ('pending', 'offered')`,
        )
        .all(targetStreamId) as typeof pendingMsgs;
    } finally {
      store.close();
    }

    let offeredBatch:
      | {
          batch_id: string;
          messages: Array<{ message_id: string; from_stream_id: string; text: string }>;
        }
      | undefined;

    if (pendingMsgs.length > 0) {
      const nowMono = this.engine.clock.monotonicNs();
      const distinctExistingBatches = [
        ...new Set(pendingMsgs.map((m) => m.batch_id).filter((b): b is string => Boolean(b))),
      ];
      const allActiveOffered =
        distinctExistingBatches.length === 1 &&
        pendingMsgs.every(
          (m) =>
            m.status === 'offered' &&
            m.batch_id === distinctExistingBatches[0] &&
            m.lease_until_mono_ns !== null &&
            BigInt(m.lease_until_mono_ns) > nowMono,
        );

      if (allActiveOffered) {
        offeredBatch = {
          batch_id: distinctExistingBatches[0],
          messages: pendingMsgs.map((m) => {
            const p = JSON.parse(m.payload_json) as { text?: string };
            return {
              message_id: m.message_id,
              from_stream_id: m.from_stream_id,
              text: p.text ?? '',
            };
          }),
        };
      } else {
        const batchId = generateBatchId();
        const nowIso = this.engine.clock.wallIso();
        const msgIds = pendingMsgs.map((m) => m.message_id);
        const offeredReceipt: NotifyReceiptRecord = {
          receipt_type: 'offered',
          batch_id: batchId,
          target_stream_id: targetStreamId,
          target_base_anchor_id: headAnchor.anchor_id,
          message_ids: msgIds,
          lease_until_mono_ns: (nowMono + 60_000_000_000n).toString(),
          recorded_at: nowIso,
        };
        const newAnchorId = newId('anc', 8);
        const nextAnchor: LineageAnchor = {
          anchor_id: newAnchorId,
          thread_id: params.threadId,
          stream_id: targetStreamId,
          parents: [headAnchor.anchor_id],
          workspace_manifest: headAnchor.workspace_manifest,
          summary: headAnchor.summary,
          head_event_ids: [],
          created_at: nowIso,
        };
        const ctrl: ControlRecord = {
          schema_version: SCHEMA_VERSION,
          kind: 'control',
          control_id: newId('ctrl', 8),
          stream_id: targetStreamId,
          commit_txn_id: 'pending',
          created_at: nowIso,
          payload: { type: 'notify_receipt', receipt: offeredReceipt },
        };
        this.engine.executeTransaction(params.projectId, {
          thread_id: params.threadId,
          affected_streams: [
            {
              stream_id: targetStreamId,
              expected_parent_head: headAnchor.anchor_id,
              anchor: nextAnchor,
              events: [],
              controls: [ctrl],
            },
          ],
        });
        headAnchor = nextAnchor;
        offeredBatch = {
          batch_id: batchId,
          messages: pendingMsgs.map((m) => {
            const p = JSON.parse(m.payload_json) as { text?: string };
            return {
              message_id: m.message_id,
              from_stream_id: m.from_stream_id,
              text: p.text ?? '',
            };
          }),
        };
      }
    }

    const lines = this.engine.readCommittedStreamLines(
      params.projectId,
      params.threadId,
      targetStreamId,
    );
    const ws = captureWorkspaceManifest(params.workspaceDir);
    const packed = packHandoffEnvelope({
      threadId: params.threadId,
      streamId: targetStreamId,
      role: isMain ? 'main_coordinator' : 'scoped_worker',
      anchor: headAnchor,
      lines,
      continuityLevel: 'Prompt Reconstruction',
      workspace: ws,
      mailboxBatch: offeredBatch,
      goalOverride: params.goalOverride,
      budget: params.budget,
    });

    assertOutboundEgressSafe(this.engine.redactor, packed.full_markdown);

    let attachmentPath: string | undefined;
    if (params.writeAttachment) {
      const layout = this.engine.getLayout(params.projectId, params.threadId);
      fs.mkdirSync(layout.bundlesDir, { recursive: true });
      attachmentPath = path.join(
        layout.bundlesDir,
        `${targetStreamId}.${headAnchor.anchor_id}.md`,
      );
      fs.writeFileSync(attachmentPath, packed.full_markdown, 'utf8');
    }

    return {
      ...packed,
      stream_id: targetStreamId,
      anchor_id: headAnchor.anchor_id,
      offered_batch_id: offeredBatch?.batch_id,
      attachment_path: attachmentPath,
    };
  }

  /**
   * Ingests a pasted turn/handoff/mailbox acknowledgement onto an existing stream.
   */
  ingestReply(params: {
    projectId: string;
    threadId: string;
    streamId: string;
    rawText: string;
    verificationMethod?: 'local_cli_direct' | 'unverified_text_import' | 'model_reported';
    confirmationProof?: ConfirmationProof;
    ackBatchId?: string;
    ackMessageIds?: string[];
    workspaceDir?: string;
  }): {
    anchor_id: string;
    txn_id: string;
    accepted_mailbox_ids: string[];
    warnings: string[];
    redacted_count: number;
  } {
    if (!params.rawText.trim()) {
      throw new Error('ingestReply requires non-empty rawText');
    }
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let headAnchorId: string | null = null;
    let lastSeq = 0;
    try {
      const sRow = store.getStream(params.streamId);
      if (!sRow) throw new Error(`Stream ${params.streamId} not found`);
      if (sRow.health === 'quarantined' || sRow.health === 'divergent_heads') {
        throw new Error(
          `Stream ${params.streamId} has health=${sRow.health}; resolve before ingesting`,
        );
      }
      headAnchorId = sRow.head_anchor_id;
      lastSeq = sRow.last_seq;
    } finally {
      store.close();
    }

    const lines = this.engine.readCommittedStreamLines(
      params.projectId,
      params.threadId,
      params.streamId,
    );

    const pkg = parseExternalConversationInput(params.rawText, params.streamId);
    const effectiveAckBatch = params.ackBatchId ?? pkg.mailbox_ack?.batch_id;
    const effectiveAckMsgs =
      params.ackMessageIds && params.ackMessageIds.length > 0
        ? params.ackMessageIds
        : pkg.mailbox_ack?.message_ids;

    const acceptedMailboxIds: string[] = [];
    const controls: ControlRecord[] = [];
    const nowIso = this.engine.clock.wallIso();

    if (effectiveAckBatch && effectiveAckMsgs && effectiveAckMsgs.length > 0) {
      const targetReceipts: NotifyReceiptRecord[] = [];
      for (const l of lines) {
        if (l.kind === 'control' && l.payload.type === 'notify_receipt') {
          targetReceipts.push(l.payload.receipt);
        }
      }
      const ackCheck = verifyMailboxReceiptAck(
        params.streamId,
        effectiveAckBatch,
        effectiveAckMsgs,
        targetReceipts,
      );
      if (!ackCheck.valid) {
        throw new Error(`Invalid mailbox acknowledgement: ${ackCheck.reason}`);
      }
      acceptedMailboxIds.push(...ackCheck.acceptedMessageIds);
      controls.push({
        schema_version: SCHEMA_VERSION,
        kind: 'control',
        control_id: newId('ctrl', 8),
        stream_id: params.streamId,
        commit_txn_id: 'pending',
        created_at: nowIso,
        payload: {
          type: 'notify_receipt',
          receipt: {
            receipt_type: 'committed_receipt',
            batch_id: effectiveAckBatch,
            target_stream_id: params.streamId,
            target_base_anchor_id: headAnchorId ?? '',
            message_ids: ackCheck.acceptedMessageIds,
            recorded_at: nowIso,
          },
        },
      });
    }

    const exactSha = sha256Hex(normalizeConfirmationText(params.rawText));
    const provenance = {
      capture_origin: 'thp_ingest',
      verification_method: params.verificationMethod ?? 'model_reported',
      source_event_ref: `ingest:${params.streamId}`,
      adapter_version: TRUST_ADAPTER_VERSION,
      confirmation_proof: params.confirmationProof,
    };
    const trustTier = computeTrustTier(provenance, exactSha);
    const sanitizedPkgRes = this.sanitizeAdoptPackage(pkg);
    const safePkg = sanitizedPkgRes.pkg;
    const { events, invocation } = buildEventsFromAdoptedPackage({
      streamId: params.streamId,
      txnId: 'pending',
      pkg: safePkg,
      trustTier,
      provenance,
      startSeq: lastSeq + 1,
      nowIso,
      turnScopeSeed: `${headAnchorId ?? 'root'}:${lastSeq}`,
    });

    const sanitizedRawSnapshot = this.engine.redactor.sanitizeText(pkg.raw_text).sanitized;
    const shouldArchiveRawBlob =
      sanitizedRawSnapshot.length > 4096 ||
      pkg.parse_warnings.some((w) => w.includes('truncated'));

    const newAnchorId = newId('anc', 8);
    const anchor: LineageAnchor = {
      anchor_id: newAnchorId,
      thread_id: params.threadId,
      stream_id: params.streamId,
      parents: headAnchorId ? [headAnchorId] : [],
      workspace_manifest: captureWorkspaceManifest(params.workspaceDir),
      summary: safePkg.summary_text || `Ingested reply onto ${params.streamId}`,
      head_event_ids: events.map((e) => e.event_id),
      created_at: nowIso,
    };

    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: params.threadId,
      affected_streams: [
        {
          stream_id: params.streamId,
          expected_parent_head: headAnchorId,
          anchor,
          events,
          invocations: [invocation],
          controls,
        },
      ],
      ...(shouldArchiveRawBlob
        ? {
            blobs: {
              ingest_snapshot: {
                bytes: sanitizedRawSnapshot,
                sync_policy: 'approved' as const,
                media_type: 'text/plain',
              },
            },
          }
        : {}),
    });

    return {
      anchor_id: newAnchorId,
      txn_id: res.txn_id,
      accepted_mailbox_ids: acceptedMailboxIds,
      warnings: pkg.parse_warnings,
      redacted_count: sanitizedPkgRes.redactedCount,
    };
  }

  /**
   * Explicit Main-initiated dispatch creating a child stream with a Dispatch Recovery Capsule
   * and Coordinator Journal `dispatch_committed` in ONE atomic multi-stream transaction.
   * Honors Release Gate when `requireDisasterRedundant` is true.
   */
  forkDispatch(params: {
    projectId: string;
    threadId: string;
    originStreamId?: string;
    sourceAnchorId?: string;
    workerStreamId?: string;
    taskBrief: string;
    workspaceDir?: string;
    permissionBoundary?: string[];
    expectedArtifacts?: string[];
    requireDisasterRedundant?: boolean;
    disasterVerified?: boolean;
    actAsMain?: boolean;
  }): {
    dispatch_id: string;
    worker_stream_id: string;
    worker_anchor_id: string;
    txn_id: string;
    durability_state: 'local_committed' | 'disaster_redundant';
    release_gated: boolean;
    worker_prompt?: string;
  } {
    assertSafeIdentifier('project_id', params.projectId);
    assertSafeIdentifier('thread_id', params.threadId);
    if (!params.taskBrief.trim()) {
      throw new Error('forkDispatch requires non-empty taskBrief');
    }
    const workerStreamId = assertSafeIdentifier(
      'worker_stream_id',
      params.workerStreamId ?? newId('wrk', 6),
    );
    const ws = captureWorkspaceManifest(params.workspaceDir);
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let threadRow: ReturnType<typeof store.getThread>;
    let sourceAnchor: LineageAnchor;
    let coordSnapshotText: string;
    let originSid: string;
    let matchedGrantId: string | undefined;

    try {
      threadRow = store.getThread(params.threadId);
      if (!threadRow) throw new Error(`Thread ${params.threadId} not found`);
      originSid = params.originStreamId ?? threadRow.main_stream_id;
      const originStream = store.getStream(originSid);
      if (
        originStream &&
        (originStream.health === 'quarantined' || originStream.health === 'divergent_heads')
      ) {
        throw new Error(
          `Origin stream ${originSid} has health=${originStream.health}; resolve before dispatching`,
        );
      }
      if (store.getStream(workerStreamId)) {
        throw new Error(`Target worker stream ${workerStreamId} already exists in thread ${params.threadId}`);
      }
      const resolvedAnchorId = params.sourceAnchorId ?? originStream?.head_anchor_id;
      if (!resolvedAnchorId) throw new Error(`Origin stream ${originSid} has no published anchor`);
      const anc = store.getAnchor(resolvedAnchorId);
      if (!anc) throw new Error(`Source anchor ${resolvedAnchorId} not found`);
      sourceAnchor = anc;

      const journalEvents = store.listJournalEvents(params.threadId);
      if (threadRow.journal_head_id === null && journalEvents.length > 0) {
        throw new Error(
          `Cannot dispatch from ${originSid}: thread ${params.threadId} has unresolved divergent Coordinator Journal heads`,
        );
      }
      const jProj = projectJournalState(params.threadId, journalEvents);
      if (jProj.has_blocking_conflict) {
        throw new Error(
          `Cannot dispatch from ${originSid}: thread ${params.threadId} has unresolved journal conflicts`,
        );
      }

      if (originSid !== threadRow.main_stream_id && !params.actAsMain) {
        const nowMs = Date.now();
        const validGrant = [...jProj.grants.values()].find(
          (g) =>
            !g.revoked &&
            g.grant.grantee_stream_id === originSid &&
            g.grant.allowed_actions.includes('fork') &&
            g.remaining_spawns >= 1 &&
            (!g.grant.expires_at || Date.parse(g.grant.expires_at) > nowMs) &&
            (g.grant.workspace_scope.includes('*') ||
              g.grant.workspace_scope.includes(ws.workspace_id)),
        );
        if (!validGrant) {
          throw new Error(
            `Unauthorized nested fork from ${originSid}: no active dispatch_grant with remaining escrow quota`,
          );
        }
        matchedGrantId = validGrant.grant.grant_id;
      }

      const snapshotObj = {
        thread_id: params.threadId,
        main_stream_id: threadRow.main_stream_id,
        coordinator_epoch: threadRow.coordinator_epoch + 1,
        streams: store.listStreams(params.threadId).map((s) => ({
          stream_id: s.stream_id,
          head_anchor_id: s.head_anchor_id,
          health: s.health,
        })),
        recent_journal: store.listJournalEvents(params.threadId).slice(-10),
      };
      coordSnapshotText = this.engine.redactor.sanitizeText(
        canonicalJson(snapshotObj),
      ).sanitized;
    } finally {
      store.close();
    }

    const nowIso = this.engine.clock.wallIso();
    const dispatchId = newId('disp', 8);
    const workerAnchorId = newId('anc', 8);
    const snapshotSha = sha256Hex(Buffer.from(coordSnapshotText, 'utf8'));
    const sanitizedBrief = this.engine.redactor.sanitizeText(params.taskBrief).sanitized;
    const nextCoordEpoch = threadRow.coordinator_epoch + 1;

    const capsule: DispatchRecoveryCapsule = {
      capsule_id: newId('cap', 8),
      origin_main_stream_id: threadRow.main_stream_id,
      origin_thread_id: params.threadId,
      source_anchor_id: sourceAnchor.anchor_id,
      dispatch_id: dispatchId,
      coordinator_epoch: nextCoordEpoch,
      coordinator_snapshot_ref: snapshotSha,
      task_brief: sanitizedBrief,
      source_trust_tier: 'human_authority',
      initial_context_refs: [sourceAnchor.anchor_id],
      target_workspace_id: ws.workspace_id,
      permission_boundary: params.permissionBoundary ?? ['read_workspace', 'propose_patch'],
      expected_artifacts: params.expectedArtifacts ?? ['summary_and_patch_receipt'],
      continuity_level: 'Prompt Reconstruction',
      completeness: 'complete',
    };

    const childEventId = deterministicEventId(workerStreamId, `dispatch|${dispatchId}`);
    const childInjectEvent: EventRecord = {
      schema_version: SCHEMA_VERSION,
      kind: 'event',
      event_id: childEventId,
      stream_id: workerStreamId,
      seq: 1,
      commit_txn_id: 'pending',
      trust_tier: 'human_authority',
      provenance: {
        capture_origin: 'thp_dispatch',
        verification_method: 'local_cli_direct',
        source_event_ref: dispatchId,
        adapter_version: TRUST_ADAPTER_VERSION,
      },
      created_at: nowIso,
      payload: {
        type: 'coordinator_inject',
        handoff_mode: 'model_authored',
        continuity_level: 'Prompt Reconstruction',
        envelope_hash: sha256Hex(sanitizedBrief),
        summary_text: sanitizedBrief,
      },
    };

    const workerAnchor: LineageAnchor = {
      anchor_id: workerAnchorId,
      thread_id: params.threadId,
      stream_id: workerStreamId,
      parents: [sourceAnchor.anchor_id],
      workspace_manifest: ws,
      summary: sanitizedBrief.slice(0, 240),
      head_event_ids: [childEventId],
      created_at: nowIso,
    };

    const capsuleCtrl: ControlRecord = {
      schema_version: SCHEMA_VERSION,
      kind: 'control',
      control_id: newId('ctrl', 8),
      stream_id: workerStreamId,
      commit_txn_id: 'pending',
      created_at: nowIso,
      payload: { type: 'dispatch_recovery_capsule', capsule },
    };

    const journalEv: JournalEventRecord = {
      schema_version: SCHEMA_VERSION,
      journal_event_id: newId('je', 8),
      thread_id: params.threadId,
      main_stream_id: threadRow.main_stream_id,
      event_type: 'dispatch_committed',
      actor_stream_id: params.actAsMain ? threadRow.main_stream_id : originSid,
      causal_predecessors: threadRow.journal_head_id ? [threadRow.journal_head_id] : [],
      commit_txn_id: 'pending',
      coordinator_epoch: nextCoordEpoch,
      created_at: nowIso,
      data: {
        dispatch_id: dispatchId,
        origin_stream_id: sourceAnchor.stream_id,
        source_anchor_id: sourceAnchor.anchor_id,
        target_stream_id: workerStreamId,
        target_workspace_id: ws.workspace_id,
        ...(matchedGrantId ? { grant_id: matchedGrantId } : {}),
        task_brief: sanitizedBrief,
        capsule_id: capsule.capsule_id,
        coordinator_snapshot_ref: snapshotSha,
        expected_artifacts: capsule.expected_artifacts,
      },
    };

    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: params.threadId,
      expected_journal_head: threadRow.journal_head_id,
      affected_streams: [
        {
          stream_id: workerStreamId,
          expected_parent_head: null,
          anchor: workerAnchor,
          events: [childInjectEvent],
          controls: [capsuleCtrl],
        },
      ],
      journal_events: [journalEv],
      blobs: {
        coordinator_snapshot: {
          bytes: coordSnapshotText,
          sync_policy: 'approved',
          media_type: 'application/json',
        },
      },
    });

    const durabilityState: 'local_committed' | 'disaster_redundant' =
      params.disasterVerified ? 'disaster_redundant' : 'local_committed';
    const releaseGated =
      Boolean(params.requireDisasterRedundant) && !params.disasterVerified;

    let workerPrompt: string | undefined;
    if (!releaseGated) {
      // Strictly separate feed surface from global archive capsule snapshot,
      // while including recent fenced events from the origin checkpoint so scoped workers have real context.
      const originLines = this.engine.readCommittedStreamLines(
        params.projectId,
        params.threadId,
        sourceAnchor.stream_id,
      );
      const allOriginEvents = originLines.filter((l): l is EventRecord => l.kind === 'event');
      const omittedOriginCount = Math.max(0, allOriginEvents.length - 4);
      const originEventLines = allOriginEvents
        .slice(-4)
        .map((ev) => {
          const rawBody =
            ev.payload.type === 'human_message'
              ? ev.payload.text
              : ev.payload.type === 'assistant_part'
                ? ev.payload.text ?? ''
                : ev.payload.type === 'coordinator_inject'
                  ? ev.payload.summary_text
                  : ev.payload.type === 'tool_result'
                    ? ev.payload.text ?? ''
                    : ev.payload.text ?? '';
          return `[${ev.event_id}][${ev.payload.type}][tier=${ev.trust_tier}] ${rawBody.slice(0, 800)}`;
        });
      if (omittedOriginCount > 0) {
        originEventLines.unshift(`[... ${omittedOriginCount} earlier parent event(s) omitted ...]`);
      }
      const originEvents = originEventLines.join('\n');

      let latestNextActions: string[] = [];
      for (const line of originLines) {
        if (line.kind === 'control' && line.payload.type === 'reconciliation_record') {
          latestNextActions = line.payload.record.next_actions ?? [];
        }
      }

      const scopedBody = [
        `Origin summary: ${sourceAnchor.summary ?? '(none)'}`,
        latestNextActions.length > 0 ? `Origin next_actions:\n- ${latestNextActions.join('\n- ')}` : '',
        originEvents ? `Recent checkpoint events:\n${originEvents}` : '',
      ]
        .filter(Boolean)
        .join('\n\n');

      const roleHeader = buildCurrentRoleBriefing({
        threadId: params.threadId,
        streamId: workerStreamId,
        role: 'scoped_worker',
        taskGoal: sanitizedBrief,
        canDelegateDispatch: false,
        expectedOutputFormat: `Return concrete results referencing dispatch_id=${dispatchId} and anchor_id=${workerAnchorId}.`,
      });
      const scopedCtx = renderNonInstructionFence({
        sourceLabel: `parent_${sourceAnchor.stream_id}`,
        trustTier: 'untrusted_peer',
        anchorOrRef: sourceAnchor.anchor_id,
        bodyText: scopedBody,
      });
      const returnReminder = [
        `When your subtask finishes, provide your findings/changes AND include ONE \`\`\`json checkpoint block citing:`,
        `- \`threadpass_handoff: true\`, \`source_label: "${workerStreamId}"\`, \`base_anchor_id: "${workerAnchorId}"\`, \`dispatch_id: "${dispatchId}"\`, \`summary_text\`, \`decisions_made\`, \`files_and_artifacts\`, \`pending_tasks\`, \`blockers\`, \`tool_errors\`.`,
      ].join('\n');
      workerPrompt = `${roleHeader}\n\n## Scoped Parent Context\n\n${scopedCtx}\n\n## Permission Boundary\n- ${capsule.permission_boundary.join('\n- ')}\n\n## Expected Artifacts\n- ${capsule.expected_artifacts.join('\n- ')}\n\n## Return Format Reminder\n${returnReminder}`;
      assertOutboundEgressSafe(this.engine.redactor, workerPrompt);
    }

    return {
      dispatch_id: dispatchId,
      worker_stream_id: workerStreamId,
      worker_anchor_id: workerAnchorId,
      txn_id: res.txn_id,
      durability_state: durabilityState,
      release_gated: releaseGated,
      worker_prompt: workerPrompt,
    };
  }

  sendMailboxMessage(params: {
    projectId: string;
    threadId: string;
    fromStreamId: string;
    targetStreamId: string;
    text: string;
  }): { message_id: string; txn_id: string } {
    assertSafeIdentifier('project_id', params.projectId);
    assertSafeIdentifier('thread_id', params.threadId);
    assertSafeIdentifier('from_stream_id', params.fromStreamId);
    assertSafeIdentifier('target_stream_id', params.targetStreamId);
    if (!params.text.trim()) {
      throw new Error('sendMailboxMessage requires non-empty text');
    }
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let headAnchor: LineageAnchor;
    let lastSeq: number;
    try {
      const sRow = store.getStream(params.fromStreamId);
      if (!sRow) {
        throw new Error(`Sender stream ${params.fromStreamId} missing`);
      }
      if (sRow.health === 'quarantined' || sRow.health === 'divergent_heads') {
        throw new Error(
          `Sender stream ${params.fromStreamId} has health=${sRow.health}; resolve before sending mailbox message`,
        );
      }
      if (!sRow.head_anchor_id) {
        throw new Error(`Sender stream ${params.fromStreamId} missing`);
      }
      const anc = store.getAnchor(sRow.head_anchor_id);
      if (!anc) throw new Error(`Sender anchor ${sRow.head_anchor_id} missing`);
      headAnchor = anc;
      lastSeq = sRow.last_seq;
    } finally {
      store.close();
    }

    const messageId = newId('msg', 8);
    const nowIso = this.engine.clock.wallIso();
    const sanitizedText = this.engine.redactor.sanitizeText(params.text).sanitized;
    const ev: EventRecord = {
      schema_version: SCHEMA_VERSION,
      kind: 'event',
      event_id: deterministicEventId(params.fromStreamId, `mailbox|${messageId}`),
      stream_id: params.fromStreamId,
      seq: lastSeq + 1,
      commit_txn_id: 'pending',
      trust_tier: 'human_authority',
      provenance: {
        capture_origin: 'thp_mailbox',
        verification_method: 'local_cli_direct',
        source_event_ref: messageId,
        adapter_version: TRUST_ADAPTER_VERSION,
      },
      created_at: nowIso,
      payload: {
        type: 'cross_stream_message',
        message_id: messageId,
        from_stream_id: params.fromStreamId,
        from_anchor_id: headAnchor.anchor_id,
        target_stream_id: params.targetStreamId,
        text: sanitizedText,
      },
    };
    const nextAnchor: LineageAnchor = {
      anchor_id: newId('anc', 8),
      thread_id: params.threadId,
      stream_id: params.fromStreamId,
      parents: [headAnchor.anchor_id],
      workspace_manifest: headAnchor.workspace_manifest,
      summary: `Sent mailbox message ${messageId} to ${params.targetStreamId}`,
      head_event_ids: [ev.event_id],
      created_at: nowIso,
    };

    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: params.threadId,
      affected_streams: [
        {
          stream_id: params.fromStreamId,
          expected_parent_head: headAnchor.anchor_id,
          anchor: nextAnchor,
          events: [ev],
        },
      ],
    });

    return { message_id: messageId, txn_id: res.txn_id };
  }

  issueDispatchGrant(params: {
    projectId: string;
    threadId: string;
    issuerStreamId?: string;
    granteeStreamId: string;
    parentGrantId?: string;
    allowedActions?: string[];
    workspaceScope?: string[];
    maxTotalSpawns: number;
    maxDepth?: number;
    canRedelegate?: boolean;
    expiresAt?: string;
  }): { grant_id: string; txn_id: string } {
    assertSafeIdentifier('project_id', params.projectId);
    assertSafeIdentifier('thread_id', params.threadId);
    assertSafeIdentifier('grantee_stream_id', params.granteeStreamId);
    if (!Number.isInteger(params.maxTotalSpawns) || params.maxTotalSpawns < 1) {
      throw new Error(`Invalid maxTotalSpawns (${params.maxTotalSpawns}): must be an integer >= 1`);
    }
    if (params.maxDepth !== undefined && (!Number.isInteger(params.maxDepth) || params.maxDepth < 0)) {
      throw new Error(`Invalid maxDepth (${params.maxDepth}): must be an integer >= 0`);
    }
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let threadRow: NonNullable<ReturnType<typeof store.getThread>>;
    let issuerStreamId: string;
    let existingJournal: JournalEventRecord[];
    try {
      const tr = store.getThread(params.threadId);
      if (!tr) throw new Error(`Thread ${params.threadId} not found`);
      threadRow = tr;
      existingJournal = store.listJournalEvents(params.threadId);
      if (threadRow.journal_head_id === null && existingJournal.length > 0) {
        throw new Error(
          `Thread ${params.threadId} has unresolved divergent Coordinator Journal heads; reconcile before issuing grants`,
        );
      }
      issuerStreamId = params.issuerStreamId ?? tr.main_stream_id;
      if (!store.getStream(params.granteeStreamId)) {
        throw new Error(`Grantee stream ${params.granteeStreamId} not found`);
      }
      if (issuerStreamId !== tr.main_stream_id && !params.parentGrantId) {
        throw new Error('Non-Main grant issuer must specify --parent-grant');
      }
      if (params.parentGrantId) {
        const parentProj = projectJournalState(params.threadId, existingJournal).grants.get(
          params.parentGrantId,
        );
        if (
          parentProj?.grant.expires_at &&
          Date.parse(parentProj.grant.expires_at) <= Date.now()
        ) {
          throw new Error(`Parent grant ${params.parentGrantId} has already expired`);
        }
      }
    } finally {
      store.close();
    }

    const grantId = newId('grt', 8);
    const nowIso = this.engine.clock.wallIso();
    const grantData: GrantIssuedData = {
      grant_id: grantId,
      issuer_stream_id: issuerStreamId,
      grantee_stream_id: params.granteeStreamId,
      ...(params.parentGrantId ? { parent_grant_id: params.parentGrantId } : {}),
      allowed_actions: params.allowedActions ?? ['fork'],
      workspace_scope: params.workspaceScope ?? ['*'],
      max_total_spawns: params.maxTotalSpawns,
      max_depth: params.maxDepth ?? 1,
      can_redelegate: Boolean(params.canRedelegate),
      ...(params.expiresAt ? { expires_at: params.expiresAt } : {}),
    };

    const je: JournalEventRecord = {
      schema_version: SCHEMA_VERSION,
      journal_event_id: newId('je', 8),
      thread_id: params.threadId,
      main_stream_id: threadRow.main_stream_id,
      event_type: 'grant_issued',
      actor_stream_id: issuerStreamId,
      causal_predecessors: threadRow.journal_head_id ? [threadRow.journal_head_id] : [],
      commit_txn_id: 'pending',
      coordinator_epoch: threadRow.coordinator_epoch + 1,
      created_at: nowIso,
      data: grantData as unknown as Record<string, unknown>,
    };
    const projected = projectJournalState(params.threadId, [...existingJournal, je]);
    if (projected.has_blocking_conflict) {
      throw new Error(
        `Grant rejected (fail-closed): ${projected.conflicts.map((c) => c.detail).join('; ')}`,
      );
    }

    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: params.threadId,
      expected_journal_head: threadRow.journal_head_id,
      affected_streams: [],
      journal_events: [je],
    });
    return { grant_id: grantId, txn_id: res.txn_id };
  }

  revokeDispatchGrant(params: {
    projectId: string;
    threadId: string;
    grantId: string;
    reason?: string;
  }): { journal_event_id: string; txn_id: string } {
    assertSafeIdentifier('project_id', params.projectId);
    assertSafeIdentifier('thread_id', params.threadId);
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let threadRow: NonNullable<ReturnType<typeof store.getThread>>;
    try {
      const tr = store.getThread(params.threadId);
      if (!tr) throw new Error(`Thread ${params.threadId} not found`);
      threadRow = tr;
      const existingJournal = store.listJournalEvents(params.threadId);
      if (threadRow.journal_head_id === null && existingJournal.length > 0) {
        throw new Error(
          `Thread ${params.threadId} has unresolved divergent Coordinator Journal heads; reconcile before revoking grants`,
        );
      }
      const jProj = projectJournalState(params.threadId, existingJournal);
      if (!jProj.grants.has(params.grantId)) {
        throw new Error(`Grant ${params.grantId} not found in thread ${params.threadId}`);
      }
    } finally {
      store.close();
    }
    const nowIso = this.engine.clock.wallIso();
    const jeId = newId('je', 8);
    const je: JournalEventRecord = {
      schema_version: SCHEMA_VERSION,
      journal_event_id: jeId,
      thread_id: params.threadId,
      main_stream_id: threadRow.main_stream_id,
      event_type: 'grant_revoked',
      actor_stream_id: threadRow.main_stream_id,
      causal_predecessors: threadRow.journal_head_id ? [threadRow.journal_head_id] : [],
      commit_txn_id: 'pending',
      coordinator_epoch: threadRow.coordinator_epoch + 1,
      created_at: nowIso,
      data: {
        grant_id: params.grantId,
        revoked_by_stream_id: threadRow.main_stream_id,
        reason: params.reason ?? 'revoked by coordinator',
      },
    };
    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: params.threadId,
      expected_journal_head: threadRow.journal_head_id,
      affected_streams: [],
      journal_events: [je],
    });
    return { journal_event_id: jeId, txn_id: res.txn_id };
  }

  peekStream(projectId: string, threadId: string, streamId: string): StreamShardLine[] {
    return this.engine.readCommittedStreamLines(projectId, threadId, streamId);
  }

  runFsck(
    projectId: string,
    threadId: string,
    options?: { rebuildSqlite?: boolean; requireTrustedSignature?: boolean },
  ): FsckReport {
    return this.engine.runFsck(projectId, threadId, options);
  }

  pinDeviceKey(
    projectId: string,
    deviceId: string,
    publicKeySpkiB64: string,
    threadId?: string,
  ) {
    return pinTrustedDeviceKey(this.engine.homeDir, {
      device_id: deviceId,
      project_id: projectId,
      thread_id: threadId,
      public_key_spki_b64: publicKeySpkiB64,
      status: 'active',
    });
  }

  revokeOrRetireDeviceKey(
    projectId: string,
    deviceId: string,
    status: 'retired' | 'revoked',
    pinnedTxnIds: readonly string[] = [],
  ) {
    return updateTrustedDeviceKeyStatus(
      this.engine.homeDir,
      deviceId,
      projectId,
      status,
      pinnedTxnIds,
    );
  }
}
