import fs from 'node:fs';
import os from 'node:os';
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
  type ConfirmationPurpose,
  type ControlRecord,
  type DispatchRecoveryCapsule,
  type EventRecord,
  type GrantIssuedData,
  type HostTranscriptLocator,
  type InputDependencyManifest,
  type JournalEventRecord,
  type LineageAnchor,
  type NotifyReceiptRecord,
  type ProvenanceChain,
  type ReconcilePackManifest,
  type ReconcilePackSourceSnapshot,
  type ReconciliationRecord,
  type StreamShardLine,
  type TrustTier,
} from '../core/types.js';
import { ensureExclusiveAcl, publishImmutable } from '../profile/local-offline/acl-and-fs.js';
import {
  loadTrustedDeviceKeys,
  pinTrustedDeviceKey,
  updateTrustedDeviceKeyStatus,
} from '../profile/local-offline/crypto-keys.js';
import {
  assertOutboundEgressSafe,
  redactRemoteUrlCredentials,
} from '../profile/local-offline/redaction.js';
import { StateStore } from '../profile/local-offline/state-store.js';
import {
  LocalOfflineEngine,
  type FsckReport,
  type StagedBlobInput,
} from '../profile/local-offline/txn-engine.js';
import { type HostBudgetSpec, type PackedEnvelopeResult } from './budget.js';
import {
  buildEventsFromAdoptedPackage,
  normalizeHostSessionId,
  normalizeOptionalMetaString,
  parseExternalConversationInput,
  type AssembledMultipartInfo,
  type ParsedAdoptPackage,
} from './ir.js';
import {
  buildCurrentRoleBriefing,
  formatTranscriptLocatorsSection,
  generateBatchId,
  packHandoffEnvelope,
  packReconcileEnvelope,
  parseReconcileReceipt,
  renderAdoptExtractPrompt,
  renderCheckpointPrompt,
  type ReconcileSourceInput,
  type TranscriptLocatorFormatContext,
} from './prompts.js';
import {
  computeTrustTier,
  formatOriginLabel,
  issueConfirmationProof,
  normalizeConfirmationText,
  renderNonInstructionFence,
  TRUST_ADAPTER_VERSION,
  type ConfirmationVerifier,
  type ReferenceOriginSummary,
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

export class ReconcileDriftError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ReconcileDriftError';
  }
}

export class StaleCheckpointBaselineError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'StaleCheckpointBaselineError';
  }
}

export interface TranscriptInputOptions {
  transcriptFilePath?: string;
  machineHostname?: string;
  cloudProvider?: string;
  remoteUri?: string;
  uploadedTranscriptText?: string;
  uploadedTranscriptFilename?: string;
  transcriptNote?: string;
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

  /**
   * Issues an Ed25519 device-signed ConfirmationProof bound to the local operator's device key.
   */
  issueLocalConfirmationProof(
    rawText: string,
    purpose: ConfirmationPurpose,
    channel: 'local_cli' | 'thp_web',
  ): ConfirmationProof {
    return issueConfirmationProof(rawText, purpose, channel, {
      identity: this.engine.identity,
    });
  }

  private evaluateConfirmationProof(
    store: StateStore,
    projectId: string,
    threadId: string,
    proof: ConfirmationProof | undefined,
    provenance: ProvenanceChain,
    exactSha: string,
  ): { trustTier: TrustTier; proofAccepted: boolean; warning?: string } {
    if (!proof) {
      return { trustTier: computeTrustTier(provenance, exactSha), proofAccepted: false };
    }
    if (store.hasConsumedConfirmationNonce(proof.nonce)) {
      const stripped = { ...provenance, confirmation_proof: undefined };
      return {
        trustTier: computeTrustTier(stripped, exactSha),
        proofAccepted: false,
        warning: `Confirmation proof nonce ${proof.nonce} was already consumed (replay rejected); downgraded trust.`,
      };
    }
    const verifier: ConfirmationVerifier = {
      trustedKeys: loadTrustedDeviceKeys(this.engine.homeDir).keys,
      projectId,
      threadId,
      localIdentity: this.engine.identity,
    };
    const trustTier = computeTrustTier(provenance, exactSha, verifier);
    return { trustTier, proofAccepted: trustTier === 'human_authority' };
  }

  private getTranscriptFormatContext(
    projectId: string,
    threadId: string,
  ): TranscriptLocatorFormatContext {
    const layout = this.engine.getLayout(projectId, threadId);
    return {
      currentHostname: os.hostname(),
      currentDeviceId: this.engine.identity.device_id,
      threadRootDir: layout.rootDir,
      transcriptIndexPath: layout.transcriptIndexJsonPath,
      checkFileExists: (p: string) => fs.existsSync(p),
      resolveThreadPath: (rel: string) => path.join(layout.rootDir, ...rel.split('/')),
    };
  }

  private prepareTranscriptArtifacts(params: {
    streamId: string;
    anchorId: string;
    sourceLabel: string;
    nowIso: string;
    operatorOptions?: TranscriptInputOptions & { hostSessionId?: string };
    modelOptions?: {
      transcriptFilePath?: string;
      machineHostname?: string;
      cloudProvider?: string;
      remoteUri?: string;
      hostSessionId?: string;
    };
    requirePlaceholderIfEmpty?: boolean;
  }): {
    controls: ControlRecord[];
    blobs: Record<string, StagedBlobInput>;
    locators: HostTranscriptLocator[];
    redactedCount: number;
  } {
    const controls: ControlRecord[] = [];
    const blobs: Record<string, StagedBlobInput> = {};
    const locators: HostTranscriptLocator[] = [];
    let redactedCount = 0;

    const op = params.operatorOptions ?? {};
    const mod = params.modelOptions ?? {};

    const opFilePath = normalizeOptionalMetaString(op.transcriptFilePath);
    const opHostname = normalizeOptionalMetaString(op.machineHostname) ?? os.hostname();
    const opCloudProvider = normalizeOptionalMetaString(op.cloudProvider);
    const opRemoteUri = normalizeOptionalMetaString(op.remoteUri);
    const opHostSessionId = normalizeOptionalMetaString(op.hostSessionId);
    const opNote = normalizeOptionalMetaString(op.transcriptNote);

    const modFilePath = normalizeOptionalMetaString(mod.transcriptFilePath);
    const modHostname = normalizeOptionalMetaString(mod.machineHostname);
    const modCloudProvider = normalizeOptionalMetaString(mod.cloudProvider);
    const modRemoteUri = normalizeOptionalMetaString(mod.remoteUri);
    const modHostSessionId = normalizeOptionalMetaString(mod.hostSessionId);
    const effectiveHostSessionId = opHostSessionId ?? modHostSessionId;

    const pushLocator = (loc: HostTranscriptLocator) => {
      locators.push(loc);
      controls.push({
        schema_version: SCHEMA_VERSION,
        kind: 'control',
        control_id: newId('ctrl', 8),
        stream_id: params.streamId,
        commit_txn_id: 'pending',
        created_at: params.nowIso,
        payload: { type: 'transcript_locator', locator: loc },
      });
    };

    if (opFilePath) {
      const fpRes = this.engine.redactor.sanitizeText(opFilePath);
      redactedCount += fpRes.redactedCount;
      pushLocator({
        locator_id: newId('tloc', 8),
        stream_id: params.streamId,
        anchor_id: params.anchorId,
        source_label: params.sourceLabel,
        host_kind: 'local_file',
        locator_origin: 'operator_verified',
        machine_hostname: opHostname,
        machine_device_id: this.engine.identity.device_id,
        file_path: fpRes.sanitized,
        ...(opCloudProvider ? { cloud_provider: opCloudProvider } : {}),
        ...(effectiveHostSessionId ? { host_session_id: effectiveHostSessionId } : {}),
        ...(opNote ? { note: opNote } : {}),
        recorded_at: params.nowIso,
      });
    }

    if (op.uploadedTranscriptText && op.uploadedTranscriptText.trim().length > 0) {
      const upRes = this.engine.redactor.sanitizeText(op.uploadedTranscriptText);
      redactedCount += upRes.redactedCount;
      const sanitizedBytes = Buffer.from(upRes.sanitized, 'utf8');
      const blobSha = sha256Hex(sanitizedBytes);
      const blobRel = path.posix.join('blobs', 'objects', blobSha.slice(0, 2), blobSha);
      blobs.uploaded_transcript = {
        bytes: upRes.sanitized,
        sync_policy: 'approved',
        media_type: 'text/plain',
      };
      const filename = normalizeOptionalMetaString(op.uploadedTranscriptFilename);
      pushLocator({
        locator_id: newId('tloc', 8),
        stream_id: params.streamId,
        anchor_id: params.anchorId,
        source_label: params.sourceLabel,
        host_kind: 'web_uploaded_blob',
        locator_origin: 'operator_verified',
        machine_hostname: opHostname,
        machine_device_id: this.engine.identity.device_id,
        uploaded_blob_sha256: blobSha,
        uploaded_blob_rel: blobRel,
        ...(filename ? { uploaded_filename: filename } : {}),
        ...(opCloudProvider ? { cloud_provider: opCloudProvider } : {}),
        ...(effectiveHostSessionId ? { host_session_id: effectiveHostSessionId } : {}),
        ...(opNote ? { note: opNote } : {}),
        recorded_at: params.nowIso,
      });
    } else if (!opFilePath && (opCloudProvider || opRemoteUri)) {
      const safeUri = opRemoteUri
        ? this.engine.redactor.sanitizeText(redactRemoteUrlCredentials(opRemoteUri)).sanitized
        : undefined;
      pushLocator({
        locator_id: newId('tloc', 8),
        stream_id: params.streamId,
        anchor_id: params.anchorId,
        source_label: params.sourceLabel,
        host_kind: 'cloud_remote',
        locator_origin: 'operator_verified',
        ...(opCloudProvider ? { cloud_provider: opCloudProvider } : {}),
        ...(effectiveHostSessionId ? { host_session_id: effectiveHostSessionId } : {}),
        ...(safeUri ? { remote_uri: safeUri } : {}),
        ...(opNote ? { note: opNote } : {}),
        recorded_at: params.nowIso,
      });
    }

    // Model-reported path/cloud hints are stored as unverified metadata and NEVER promoted to local machine bindings
    if (!opFilePath && modFilePath) {
      const mFpRes = this.engine.redactor.sanitizeText(modFilePath);
      redactedCount += mFpRes.redactedCount;
      pushLocator({
        locator_id: newId('tloc', 8),
        stream_id: params.streamId,
        anchor_id: params.anchorId,
        source_label: params.sourceLabel,
        host_kind: 'local_file',
        locator_origin: 'model_reported',
        ...(modHostname ? { machine_hostname: modHostname } : {}),
        file_path: mFpRes.sanitized,
        ...(modCloudProvider ? { cloud_provider: modCloudProvider } : {}),
        ...(modHostSessionId ? { host_session_id: modHostSessionId } : {}),
        ...(opNote ? { note: opNote } : {}),
        recorded_at: params.nowIso,
      });
    } else if (!opFilePath && !opCloudProvider && !opRemoteUri && !modFilePath && (modCloudProvider || modRemoteUri)) {
      const safeModUri = modRemoteUri
        ? this.engine.redactor.sanitizeText(redactRemoteUrlCredentials(modRemoteUri)).sanitized
        : undefined;
      pushLocator({
        locator_id: newId('tloc', 8),
        stream_id: params.streamId,
        anchor_id: params.anchorId,
        source_label: params.sourceLabel,
        host_kind: 'cloud_remote',
        locator_origin: 'model_reported',
        ...(modCloudProvider ? { cloud_provider: modCloudProvider } : {}),
        ...(modHostSessionId ? { host_session_id: modHostSessionId } : {}),
        ...(safeModUri ? { remote_uri: safeModUri } : {}),
        ...(opNote ? { note: opNote } : {}),
        recorded_at: params.nowIso,
      });
    }

    if (params.requirePlaceholderIfEmpty && locators.length === 0) {
      pushLocator({
        locator_id: newId('tloc', 8),
        stream_id: params.streamId,
        anchor_id: params.anchorId,
        source_label: params.sourceLabel,
        host_kind: 'placeholder_unavailable',
        ...(effectiveHostSessionId
          ? {
              locator_origin: opHostSessionId ? 'operator_verified' : 'model_reported',
              host_session_id: effectiveHostSessionId,
            }
          : {}),
        ...(opNote ? { note: opNote } : {}),
        recorded_at: params.nowIso,
      });
    }

    return { controls, blobs, locators, redactedCount };
  }

  initThread(params: {
    projectId: string;
    threadId?: string;
    mainStreamId?: string;
    workspaceDir?: string;
    summary?: string;
    hostSessionId?: string;
  } & TranscriptInputOptions): {
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

    const safeHostSessionId = normalizeHostSessionId(params.hostSessionId);
    const transcriptArtifacts = this.prepareTranscriptArtifacts({
      streamId: mainStreamId,
      anchorId,
      sourceLabel: mainStreamId,
      nowIso,
      operatorOptions: { ...params, hostSessionId: safeHostSessionId },
      requirePlaceholderIfEmpty: false,
    });

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
          ...(transcriptArtifacts.controls.length > 0 ? { controls: transcriptArtifacts.controls } : {}),
        },
      ],
      journal_events: [mainDesigEvent],
      ...(Object.keys(transcriptArtifacts.blobs).length > 0 ? { blobs: transcriptArtifacts.blobs } : {}),
      ...(safeHostSessionId
        ? { host_bindings: [{ host_session_id: safeHostSessionId, stream_id: mainStreamId }] }
        : {}),
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
      const transcriptLocators = store.listTranscriptLocators(threadId);
      const layout = this.engine.getLayout(projectId, threadId);
      return {
        device_id: this.engine.identity.device_id,
        project_id: projectId,
        thread,
        streams,
        anchors,
        journal_count: journal.length,
        quarantined,
        pending_mailbox: pendingMailbox,
        transcript_locators: transcriptLocators,
        transcript_index_path: layout.transcriptIndexJsonPath,
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
  } & TranscriptInputOptions): {
    stream_id: string;
    anchor_id: string;
    txn_id: string;
    trust_tier: TrustTier;
    mode: string;
    continuity_level: string;
    warnings: string[];
    redacted_count: number;
    transcript_locators: HostTranscriptLocator[];
    assembled_parts?: AssembledMultipartInfo;
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

    // Verify confirmation proof against exact raw text + unconsumed nonce + trusted signature
    const exactSha = sha256Hex(normalizeConfirmationText(params.rawText));
    const verificationMethod =
      pkg.mode === 'model_authored' ? 'model_reported' : 'unverified_text_import';
    const rawProvenance: ProvenanceChain = {
      capture_origin: `adopt_paste:${label}`,
      verification_method: verificationMethod as 'model_reported' | 'unverified_text_import',
      source_event_ref: `adopt:${label}`,
      adapter_version: TRUST_ADAPTER_VERSION,
      confirmation_proof: params.confirmationProof,
    };

    const sanitizedPkgRes = this.sanitizeAdoptPackage(pkg);
    const safePkg = sanitizedPkgRes.pkg;

    const anchorId = newId('anc', 8);
    const nowIso = this.engine.clock.wallIso();
    const operatorHostSessionId = normalizeHostSessionId(params.hostSessionId);
    if (!operatorHostSessionId && pkg.host_session_id) {
      warnings.push(
        `Ignored model-reported host_session_id (${pkg.host_session_id}) for active host binding; recorded as unverified locator metadata only.`,
      );
    }

    // 3-layer pre-ingestion lookup across SQLite & confirmation proof replay/signature verification
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let threadRow: ReturnType<typeof store.getThread>;
    let existingStream: ReturnType<typeof store.getStream>;
    let trustTier: TrustTier;
    let effectiveProvenance: ProvenanceChain;
    let events: EventRecord[];
    let invocation: ReturnType<typeof buildEventsFromAdoptedPackage>['invocation'];
    try {
      const proofEval = this.evaluateConfirmationProof(
        store,
        params.projectId,
        params.threadId,
        params.confirmationProof,
        rawProvenance,
        exactSha,
      );
      trustTier = proofEval.trustTier;
      if (proofEval.warning) warnings.push(proofEval.warning);
      effectiveProvenance = {
        ...rawProvenance,
        confirmation_proof: proofEval.proofAccepted ? params.confirmationProof : undefined,
      };
      const built = buildEventsFromAdoptedPackage({
        streamId,
        txnId: 'pending',
        pkg: safePkg,
        trustTier,
        provenance: effectiveProvenance,
        startSeq: 1,
        nowIso,
      });
      events = built.events;
      invocation = built.invocation;

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

      if (operatorHostSessionId) {
        const hb = store.lookupHostBinding(operatorHostSessionId);
        if (hb && hb.thread_id === params.threadId) {
          throw new DuplicateAdoptionError(
            `host_session_id ${operatorHostSessionId} is already bound to stream ${hb.stream_id} in same thread ${params.threadId}`,
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
            if (operatorHostSessionId) {
              const otherHb = otherStore.lookupHostBinding(operatorHostSessionId);
              if (otherHb && !params.allowForeignCanonical) {
                throw new ForeignCanonicalDetectedError(
                  `host_session_id ${operatorHostSessionId} belongs to another canonical thread ${otherHb.thread_id} (${otherHb.stream_id}); re-run as foreign_canonical fixed snapshot without stealing active host binding`,
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

    const hostIdToBind = !params.allowForeignCanonical ? operatorHostSessionId : undefined;

    const transcriptArtifacts = this.prepareTranscriptArtifacts({
      streamId,
      anchorId,
      sourceLabel: label,
      nowIso,
      operatorOptions: params,
      modelOptions: {
        transcriptFilePath: pkg.transcript_file_path,
        machineHostname: pkg.machine_hostname,
        cloudProvider: pkg.cloud_provider,
        remoteUri: pkg.remote_uri,
        hostSessionId: pkg.host_session_id,
      },
      requirePlaceholderIfEmpty: true,
    });

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
          controls: [adoptedCtrl, ...transcriptArtifacts.controls],
        },
      ],
      journal_events: [journalEv],
      blobs: {
        adopt_snapshot: {
          bytes: sanitizedPkgText,
          sync_policy: 'approved',
          media_type: 'text/plain',
        },
        ...transcriptArtifacts.blobs,
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
      redacted_count:
        Math.max(sanitizedRawRes.redactedCount, sanitizedPkgRes.redactedCount) +
        transcriptArtifacts.redactedCount,
      transcript_locators: transcriptArtifacts.locators,
      ...(pkg.assembled_parts ? { assembled_parts: pkg.assembled_parts } : {}),
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
    pack_manifest: ReconcilePackManifest;
    common_ancestors: string[];
    source_anchors: Array<{ stream_id: string; anchor_id: string }>;
    attachment_path?: string;
  } {
    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let threadRow: ReturnType<typeof store.getThread>;
    let mainBaseAnchorId: string | null = null;
    let sources: ReconcileSourceInput[];
    let sourceSnapshots: ReconcilePackSourceSnapshot[] = [];
    let commonAncestors: string[] = [];
    let transcriptLocators: HostTranscriptLocator[] = [];

    try {
      threadRow = store.getThread(params.threadId);
      if (!threadRow) {
        throw new Error(`Thread ${params.threadId} not found`);
      }
      const mainStreamRow = store.getStream(threadRow.main_stream_id);
      if (mainStreamRow?.health === 'quarantined') {
        throw new Error(
          `Main stream ${threadRow.main_stream_id} has health=quarantined; resolve fsck before packing reconcile`,
        );
      }
      mainBaseAnchorId = mainStreamRow?.head_anchor_id ?? null;
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
        const distinctTiers = new Set(events.map((e) => e.trust_tier));
        const originSummary: ReferenceOriginSummary =
          distinctTiers.size === 0
            ? 'untrusted_peer'
            : distinctTiers.size === 1
              ? [...distinctTiers][0]
              : 'mixed_origin';

        sourceSnapshots.push({
          stream_id: sid,
          anchor_id: anchor.anchor_id,
          anchor_sha256: canonicalSha256(anchor),
          completeness: sRow.health,
        });
        sources.push({
          streamId: sid,
          anchor,
          events,
          trustTier: originSummary,
          completeness: sRow.health,
          sourceLabel: sid,
        });
      }

      const preAnalysis = analyzeCommonAncestors(
        sources.map((s) => s.anchor.anchor_id),
        (id) => store.getAnchor(id)?.parents ?? null,
      );
      commonAncestors = [...preAnalysis.common];
      transcriptLocators = store.listTranscriptLocators(params.threadId);
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
      transcriptLocators,
      transcriptContext: this.getTranscriptFormatContext(params.projectId, params.threadId),
      budget: params.budget,
    });

    // Enforce Outbound Egress Gate
    assertOutboundEgressSafe(this.engine.redactor, packed.full_markdown);

    const layout = this.engine.getLayout(params.projectId, params.threadId);
    ensureExclusiveAcl(layout.reconcilePacksDir, 'dir');
    ensureExclusiveAcl(layout.txnsStagingDir, 'dir');
    const unsignedManifest: Omit<ReconcilePackManifest, 'pack_manifest_hash'> = {
      schema_version: SCHEMA_VERSION,
      envelope_id: packed.envelope_id,
      project_id: params.projectId,
      thread_id: params.threadId,
      main_stream_id: threadRow.main_stream_id,
      main_base_anchor_id: mainBaseAnchorId,
      sources: sourceSnapshots,
      prompt_manifest_hash: packed.manifest_hash,
      created_at: this.engine.clock.wallIso(),
    };
    const packManifest: ReconcilePackManifest = {
      ...unsignedManifest,
      pack_manifest_hash: canonicalSha256(unsignedManifest),
    };
    const finalPackPath = path.join(layout.reconcilePacksDir, `${packed.envelope_id}.pack.json`);
    publishImmutable(
      layout.txnsStagingDir,
      finalPackPath,
      Buffer.from(`${canonicalJson(packManifest)}\n`, 'utf8'),
    );
    ensureExclusiveAcl(finalPackPath, 'file');

    let attachmentPath: string | undefined;
    if (params.writeAttachment) {
      fs.mkdirSync(layout.bundlesDir, { recursive: true });
      attachmentPath = path.join(
        layout.bundlesDir,
        `reconcile.${packed.envelope_id}.md`,
      );
      fs.writeFileSync(attachmentPath, packed.full_markdown, 'utf8');
    }

    return {
      ...packed,
      pack_manifest: packManifest,
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
    packEnvelopeId?: string;
    confirmedConflictTopics?: string[];
    confirmAllConflicts?: boolean;
    confirmationProof?: ConfirmationProof;
    workspaceDir?: string;
    hostSessionId?: string;
  } & TranscriptInputOptions): {
    reconciliation_id: string;
    main_stream_id: string;
    anchor_id: string;
    txn_id: string;
    conflicts_count: number;
    parse_warning?: string;
    assembled_parts?: AssembledMultipartInfo;
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

    const layout = this.engine.getLayout(params.projectId, params.threadId);
    const paramPackId = normalizeOptionalMetaString(params.packEnvelopeId);
    if (paramPackId && parsed.pack_envelope_id && paramPackId !== parsed.pack_envelope_id) {
      throw new ReconcileDriftError(
        `Reconcile pack envelope mismatch: caller supplied ${paramPackId}, receipt references ${parsed.pack_envelope_id}`,
      );
    }
    const resolvedPackId = paramPackId ?? parsed.pack_envelope_id;
    let loadedPack: ReconcilePackManifest | undefined;
    if (resolvedPackId) {
      assertSafeIdentifier('pack_envelope_id', resolvedPackId);
      const packPath = path.join(layout.reconcilePacksDir, `${resolvedPackId}.pack.json`);
      if (!fs.existsSync(packPath)) {
        throw new ReconcileDriftError(
          `Reconcile pack manifest ${resolvedPackId} not found; run packReconcile first`,
        );
      }
      loadedPack = JSON.parse(fs.readFileSync(packPath, 'utf8')) as ReconcilePackManifest;
      const { pack_manifest_hash, ...unsignedPack } = loadedPack;
      if (canonicalSha256(unsignedPack) !== pack_manifest_hash) {
        throw new ReconcileDriftError(
          `Reconcile pack manifest ${resolvedPackId} integrity hash mismatch`,
        );
      }
      if (
        loadedPack.project_id !== params.projectId ||
        loadedPack.thread_id !== params.threadId
      ) {
        throw new ReconcileDriftError(
          `Reconcile pack ${resolvedPackId} belongs to ${loadedPack.project_id}/${loadedPack.thread_id}`,
        );
      }
    }

    const store = this.engine.openExistingStore(params.projectId, params.threadId);

    let threadRow: ReturnType<typeof store.getThread>;
    let mainRow: ReturnType<typeof store.getStream>;
    let inputSources: ReconciliationRecord['input_sources'] = [];
    let depItems: InputDependencyManifest['items'] = [];
    let journalCausalPreds: string[] = [];
    let mainAnchorParents: string[] = [];
    let reconcileTrustTier: TrustTier;
    let proofAccepted = false;

    const rawReconcileProvenance: ProvenanceChain = {
      capture_origin: 'thp_reconcile_commit',
      verification_method: 'model_reported',
      source_event_ref: 'reconcile_pending',
      adapter_version: TRUST_ADAPTER_VERSION,
      confirmation_proof: params.confirmationProof,
    };

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

      const isDivergentRecovery =
        mainRow.health === 'divergent_heads' || threadRow.journal_head_id === null;

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

      if (loadedPack) {
        if (loadedPack.main_stream_id !== threadRow.main_stream_id) {
          throw new ReconcileDriftError(
            `Reconcile pack ${loadedPack.envelope_id} targets main=${loadedPack.main_stream_id}, current=${threadRow.main_stream_id}`,
          );
        }
        if (
          mainRow.health !== 'divergent_heads' &&
          mainRow.head_anchor_id !== loadedPack.main_base_anchor_id
        ) {
          throw new ReconcileDriftError(
            `Main stream ${mainRow.stream_id} advanced from ${loadedPack.main_base_anchor_id ?? 'none'} to ${mainRow.head_anchor_id ?? 'none'} since pack ${loadedPack.envelope_id}; re-run packReconcile`,
          );
        }
        if (params.sourceStreamIds.length > 0) {
          const requestedSorted = [...new Set(params.sourceStreamIds)].sort();
          const packSorted = [...new Set(loadedPack.sources.map((s) => s.stream_id))].sort();
          if (
            requestedSorted.length !== packSorted.length ||
            requestedSorted.some((sid, idx) => sid !== packSorted[idx])
          ) {
            throw new ReconcileDriftError(
              `Reconcile source streams drifted vs pack ${loadedPack.envelope_id}: requested=[${requestedSorted.join(',')}] expected=[${packSorted.join(',')}]`,
            );
          }
        }
        for (const ps of loadedPack.sources) {
          const sRow = store.getStream(ps.stream_id);
          if (!sRow) {
            throw new ReconcileDriftError(`Packed source stream ${ps.stream_id} no longer exists`);
          }
          if (sRow.health === 'quarantined' || sRow.health === 'divergent_heads') {
            throw new ReconcileDriftError(
              `Packed source stream ${ps.stream_id} entered health=${sRow.health}; resolve and re-pack`,
            );
          }
          if (sRow.head_anchor_id !== ps.anchor_id) {
            throw new ReconcileDriftError(
              `Source stream ${ps.stream_id} drifted from packed anchor ${ps.anchor_id} to ${sRow.head_anchor_id ?? 'none'}; re-run packReconcile`,
            );
          }
          const a = store.getAnchor(ps.anchor_id);
          if (!a) {
            throw new ReconcileDriftError(`Packed source anchor ${ps.anchor_id} missing`);
          }
          const actualHash = canonicalSha256(a);
          if (actualHash !== ps.anchor_sha256) {
            throw new ReconcileDriftError(
              `Source anchor ${ps.anchor_id} digest changed since pack ${loadedPack.envelope_id}`,
            );
          }
          inputSources.push({
            stream_id: ps.stream_id,
            anchor_id: ps.anchor_id,
            completeness: sRow.health,
          });
          depItems.push({
            id: newId('dep', 6),
            ref_type: 'anchor',
            target_id: ps.anchor_id,
            stream_id: ps.stream_id,
            thread_id: params.threadId,
            sha256: ps.anchor_sha256,
            observability: 'exact',
            required_for_recovery: true,
          });
        }
      } else if (params.sourceStreamIds.length === 0 && isDivergentRecovery) {
        // Explicit Main/Journal divergence healing without external sources is allowed without a pack
      } else {
        throw new ReconcileDriftError(
          'commitReconcile requires a bound packEnvelopeId from packReconcile to verify exact packed snapshots',
        );
      }

      const proofEval = this.evaluateConfirmationProof(
        store,
        params.projectId,
        params.threadId,
        params.confirmationProof,
        rawReconcileProvenance,
        exactReceiptSha,
      );
      reconcileTrustTier = proofEval.trustTier;
      proofAccepted = proofEval.proofAccepted;
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

    const normalizedConfirmTopics = params.confirmedConflictTopics
      ?.map((s) => s.trim())
      .filter(Boolean);
    if (
      params.confirmedConflictTopics &&
      params.confirmedConflictTopics.length > 0 &&
      (!normalizedConfirmTopics || normalizedConfirmTopics.length === 0)
    ) {
      throw new Error('confirmedConflictTopics must contain at least one non-empty topic name');
    }
    if (
      ((normalizedConfirmTopics && normalizedConfirmTopics.length > 0) ||
        params.confirmAllConflicts === true) &&
      (!proofAccepted || reconcileTrustTier !== 'human_authority')
    ) {
      throw new Error(
        'Cannot confirm conflicts (confirmedConflictTopics / confirmAllConflicts) without a valid human_authority ConfirmationProof',
      );
    }

    const sanitizedConflicts = parsed.conflicts.map((c) => {
      const topic = this.engine.redactor.sanitizeText(c.topic).sanitized;
      const confirmedByHuman =
        proofAccepted &&
        reconcileTrustTier === 'human_authority' &&
        (normalizedConfirmTopics
          ? normalizedConfirmTopics.includes(c.topic) ||
            normalizedConfirmTopics.includes(topic)
          : params.confirmAllConflicts !== false);
      return {
        ...c,
        topic,
        positions: this.sanitizeTextList(c.positions).items,
        resolution: this.engine.redactor.sanitizeText(c.resolution).sanitized,
        human_confirmed: confirmedByHuman,
        ...(confirmedByHuman && params.confirmationProof
          ? { confirmation_proof_nonce: params.confirmationProof.nonce }
          : {}),
      };
    });

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
      ...(loadedPack
        ? {
            pack_envelope_id: loadedPack.envelope_id,
            pack_manifest_hash: loadedPack.pack_manifest_hash,
          }
        : {}),
      confirmation_proof: proofAccepted ? params.confirmationProof : undefined,
      created_at: nowIso,
    };

    const nextSeq = mainRow.last_seq + 1;
    const injectEventId = deterministicEventId(
      threadRow.main_stream_id,
      `reconcile|${reconciliationId}`,
    );

    const reconcileProvenance: ProvenanceChain = {
      capture_origin: 'thp_reconcile_commit',
      verification_method: 'model_reported',
      source_event_ref: reconciliationId,
      adapter_version: TRUST_ADAPTER_VERSION,
      confirmation_proof: proofAccepted ? params.confirmationProof : undefined,
    };

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

    const transcriptArtifacts = this.prepareTranscriptArtifacts({
      streamId: threadRow.main_stream_id,
      anchorId: newAnchorId,
      sourceLabel: threadRow.main_stream_id,
      nowIso,
      operatorOptions: params,
      requirePlaceholderIfEmpty: false,
    });

    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: params.threadId,
      expected_journal_head: threadRow.journal_head_id,
      affected_streams: [
        {
          stream_id: threadRow.main_stream_id,
          expected_parent_head: mainRow.head_anchor_id,
          anchor,
          events: [injectEvent],
          controls: [recControl, ...transcriptArtifacts.controls],
        },
      ],
      journal_events: [journalEv],
      input_dependency_manifests: [depManifest],
      provenance_refs: sanitizedProvenanceRefs,
      ...(Object.keys(transcriptArtifacts.blobs).length > 0 ? { blobs: transcriptArtifacts.blobs } : {}),
    });

    return {
      reconciliation_id: reconciliationId,
      main_stream_id: threadRow.main_stream_id,
      anchor_id: newAnchorId,
      txn_id: res.txn_id,
      conflicts_count: sanitizedConflicts.length,
      ...(parsed.parse_warning ? { parse_warning: parsed.parse_warning } : {}),
      ...(parsed.assembled_parts ? { assembled_parts: parsed.assembled_parts } : {}),
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
    let transcriptLocators: HostTranscriptLocator[] = [];
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
      transcriptLocators = store.listTranscriptLocators(params.threadId);
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
      transcriptLocators,
      transcriptContext: this.getTranscriptFormatContext(params.projectId, params.threadId),
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
    expectedBaseAnchorId?: string;
    verificationMethod?: 'local_cli_direct' | 'unverified_text_import' | 'model_reported';
    confirmationProof?: ConfirmationProof;
    ackBatchId?: string;
    ackMessageIds?: string[];
    workspaceDir?: string;
    hostSessionId?: string;
  } & TranscriptInputOptions): {
    anchor_id: string;
    txn_id: string;
    accepted_mailbox_ids: string[];
    warnings: string[];
    redacted_count: number;
    transcript_locators: HostTranscriptLocator[];
    assembled_parts?: AssembledMultipartInfo;
  } {
    if (!params.rawText.trim()) {
      throw new Error('ingestReply requires non-empty rawText');
    }
    const pkg = parseExternalConversationInput(params.rawText, params.streamId);
    const paramBase = normalizeOptionalMetaString(params.expectedBaseAnchorId);
    if (paramBase && pkg.base_anchor_id && paramBase !== pkg.base_anchor_id) {
      throw new StaleCheckpointBaselineError(
        `Checkpoint base_anchor_id conflict: caller supplied ${paramBase}, payload carries ${pkg.base_anchor_id}`,
      );
    }
    const effectiveBaseAnchor = paramBase ?? pkg.base_anchor_id;

    const exactSha = sha256Hex(normalizeConfirmationText(params.rawText));
    const rawProvenance: ProvenanceChain = {
      capture_origin: 'thp_ingest',
      verification_method: params.verificationMethod ?? 'model_reported',
      source_event_ref: `ingest:${params.streamId}`,
      adapter_version: TRUST_ADAPTER_VERSION,
      confirmation_proof: params.confirmationProof,
    };

    const store = this.engine.openExistingStore(params.projectId, params.threadId);
    let headAnchorId: string | null = null;
    let lastSeq = 0;
    let trustTier: TrustTier;
    let effectiveProvenance: ProvenanceChain;
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
      if (effectiveBaseAnchor && effectiveBaseAnchor !== headAnchorId) {
        throw new StaleCheckpointBaselineError(
          `Checkpoint base_anchor_id (${effectiveBaseAnchor}) does not match current stream ${params.streamId} head (${headAnchorId ?? 'none'}); stream has advanced — re-pack handoff or reconcile instead of appending stale context`,
        );
      }

      const proofEval = this.evaluateConfirmationProof(
        store,
        params.projectId,
        params.threadId,
        params.confirmationProof,
        rawProvenance,
        exactSha,
      );
      trustTier = proofEval.trustTier;
      if (proofEval.warning) pkg.parse_warnings.push(proofEval.warning);
      effectiveProvenance = {
        ...rawProvenance,
        confirmation_proof: proofEval.proofAccepted ? params.confirmationProof : undefined,
      };
    } finally {
      store.close();
    }

    const lines = this.engine.readCommittedStreamLines(
      params.projectId,
      params.threadId,
      params.streamId,
    );

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

    const sanitizedPkgRes = this.sanitizeAdoptPackage(pkg);
    const safePkg = sanitizedPkgRes.pkg;
    const { events, invocation } = buildEventsFromAdoptedPackage({
      streamId: params.streamId,
      txnId: 'pending',
      pkg: safePkg,
      trustTier,
      provenance: effectiveProvenance,
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

    const transcriptArtifacts = this.prepareTranscriptArtifacts({
      streamId: params.streamId,
      anchorId: newAnchorId,
      sourceLabel: params.streamId,
      nowIso,
      operatorOptions: params,
      modelOptions: {
        transcriptFilePath: pkg.transcript_file_path,
        machineHostname: pkg.machine_hostname,
        cloudProvider: pkg.cloud_provider,
        remoteUri: pkg.remote_uri,
        hostSessionId: pkg.host_session_id,
      },
      requirePlaceholderIfEmpty: false,
    });

    const combinedBlobs: Record<string, StagedBlobInput> = {
      ...transcriptArtifacts.blobs,
      ...(shouldArchiveRawBlob
        ? {
            ingest_snapshot: {
              bytes: sanitizedRawSnapshot,
              sync_policy: 'approved' as const,
              media_type: 'text/plain',
            },
          }
        : {}),
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
          controls: [...controls, ...transcriptArtifacts.controls],
        },
      ],
      ...(Object.keys(combinedBlobs).length > 0 ? { blobs: combinedBlobs } : {}),
    });

    return {
      anchor_id: newAnchorId,
      txn_id: res.txn_id,
      accepted_mailbox_ids: acceptedMailboxIds,
      warnings: pkg.parse_warnings,
      redacted_count: sanitizedPkgRes.redactedCount + transcriptArtifacts.redactedCount,
      transcript_locators: transcriptArtifacts.locators,
      ...(pkg.assembled_parts ? { assembled_parts: pkg.assembled_parts } : {}),
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
    hostSessionId?: string;
  } & TranscriptInputOptions): {
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
    let existingLocators: HostTranscriptLocator[] = [];

    try {
      threadRow = store.getThread(params.threadId);
      if (!threadRow) throw new Error(`Thread ${params.threadId} not found`);
      originSid = params.originStreamId ?? threadRow.main_stream_id;
      const originStream = store.getStream(originSid);
      if (!originStream) {
        throw new Error(`Origin stream ${originSid} not found in thread ${params.threadId}`);
      }
      if (originStream.health === 'quarantined' || originStream.health === 'divergent_heads') {
        throw new Error(
          `Origin stream ${originSid} has health=${originStream.health}; resolve before dispatching`,
        );
      }
      if (store.getStream(workerStreamId)) {
        throw new Error(`Target worker stream ${workerStreamId} already exists in thread ${params.threadId}`);
      }
      const resolvedAnchorId = params.sourceAnchorId ?? originStream.head_anchor_id;
      if (!resolvedAnchorId) throw new Error(`Origin stream ${originSid} has no published anchor`);
      const anc = store.getAnchor(resolvedAnchorId);
      if (!anc) throw new Error(`Source anchor ${resolvedAnchorId} not found`);
      if (anc.stream_id !== originSid) {
        throw new Error(
          `Source anchor ${resolvedAnchorId} belongs to stream ${anc.stream_id}, not origin stream ${originSid}`,
        );
      }
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
      existingLocators = store.listTranscriptLocators(params.threadId);
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

    const transcriptArtifacts = this.prepareTranscriptArtifacts({
      streamId: workerStreamId,
      anchorId: workerAnchorId,
      sourceLabel: workerStreamId,
      nowIso,
      operatorOptions: params,
      requirePlaceholderIfEmpty: false,
    });

    const res = this.engine.executeTransaction(params.projectId, {
      thread_id: params.threadId,
      expected_journal_head: threadRow.journal_head_id,
      affected_streams: [
        {
          stream_id: workerStreamId,
          expected_parent_head: null,
          anchor: workerAnchor,
          events: [childInjectEvent],
          controls: [capsuleCtrl, ...transcriptArtifacts.controls],
        },
      ],
      journal_events: [journalEv],
      blobs: {
        coordinator_snapshot: {
          bytes: coordSnapshotText,
          sync_policy: 'approved',
          media_type: 'application/json',
        },
        ...transcriptArtifacts.blobs,
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
          const displayKind =
            ev.payload.type === 'coordinator_inject'
              ? 'handoff_note'
              : ev.payload.type === 'assistant_part'
                ? 'prior_agent'
                : ev.payload.type === 'human_message'
                  ? 'archived_user_msg'
                  : ev.payload.type;
          return `[${ev.event_id}][${displayKind}][origin=${formatOriginLabel(ev.trust_tier)}] ${rawBody.slice(0, 800)}`;
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
        `- If the report exceeds your single-turn output limit, split it into standalone valid \`\`\`json objects using \`part_group_id\`, \`part_index\`, \`total_parts\` (or \`null\`), and \`is_final\`.`,
      ].join('\n');
      const allWorkerLocators = [...transcriptArtifacts.locators, ...existingLocators];
      const locatorText =
        allWorkerLocators.length > 0
          ? formatTranscriptLocatorsSection({
              locators: allWorkerLocators,
              context: this.getTranscriptFormatContext(params.projectId, params.threadId),
            })
          : '';
      const locatorBlock = locatorText
        ? `\n\n## Session Transcript Locators (Machine-Scoped Paths & Reachability)\n${locatorText}`
        : '';
      workerPrompt = `${roleHeader}\n\n## Scoped Parent Context\n\n${scopedCtx}${locatorBlock}\n\n## Scope & Working Boundaries\n- ${capsule.permission_boundary.join('\n- ')}\n\n## Expected Artifacts\n- ${capsule.expected_artifacts.join('\n- ')}\n\n## Return Format Reminder\n${returnReminder}`;
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
