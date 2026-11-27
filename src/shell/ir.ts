import { deterministicEventId, newId, sha256Hex } from '../core/canonical.js';
import {
  SCHEMA_VERSION,
  type ContinuityLevel,
  type EventRecord,
  type HandoffMode,
  type InvocationRecord,
  type ProvenanceChain,
  type TrustTier,
} from '../core/types.js';

export interface ParsedTurnMessage {
  role: 'user' | 'assistant' | 'tool';
  text: string;
  source_ref?: string;
}

export interface ParsedAdoptPackage {
  mode: HandoffMode;
  continuity_level: ContinuityLevel;
  source_label: string;
  host_session_id?: string;
  claimed_user_quotes: string[];
  summary_text: string;
  recent_turns: ParsedTurnMessage[];
  pending_tasks: string[];
  blockers: string[];
  tool_errors: string[];
  referenced_ids: string[];
  mailbox_ack?: { batch_id: string; message_ids: string[] };
  raw_text: string;
}

/**
 * Parses either a structured `model_authored` adopt-extract block OR raw pasted conversation text
 * (`evidence_based` fallback). Never elevates self-claimed `user:` labels to `human_authority`.
 */
export function parseExternalConversationInput(
  rawText: string,
  sourceLabel = 'external_ai',
): ParsedAdoptPackage {
  const trimmed = rawText.trim();
  const jsonFenceMatch = trimmed.match(
    /```json\s*(\{[\s\S]*?"(?:threadpass_handoff|handoff_mode|summary_text|recent_turns)"[\s\S]*?\})\s*```/i,
  );
  const candidateJson = jsonFenceMatch
    ? jsonFenceMatch[1]
    : trimmed.startsWith('{') && trimmed.endsWith('}')
      ? trimmed
      : null;

  if (candidateJson) {
    try {
      const obj = JSON.parse(candidateJson) as Record<string, unknown>;
      const mode: HandoffMode =
        obj.handoff_mode === 'evidence_based' ? 'evidence_based' : 'model_authored';
      const turns: ParsedTurnMessage[] = Array.isArray(obj.recent_turns)
        ? (obj.recent_turns as Array<Record<string, unknown>>).map((t, idx) => ({
            role:
              t.role === 'user' || t.role === 'assistant' || t.role === 'tool'
                ? t.role
                : 'assistant',
            text: String(t.text ?? t.content ?? ''),
            source_ref: String(t.source_ref ?? `turn_${idx + 1}`),
          }))
        : [];
      return {
        mode,
        continuity_level:
          mode === 'evidence_based' ? 'Evidence Recovery' : 'Prompt Reconstruction',
        source_label: String(obj.source_label ?? sourceLabel),
        host_session_id:
          typeof obj.host_session_id === 'string' ? obj.host_session_id : undefined,
        claimed_user_quotes: Array.isArray(obj.claimed_user_quotes)
          ? obj.claimed_user_quotes.map(String)
          : [],
        summary_text: String(obj.summary_text ?? obj.summary ?? ''),
        recent_turns: turns,
        pending_tasks: Array.isArray(obj.pending_tasks)
          ? obj.pending_tasks.map(String)
          : [],
        blockers: Array.isArray(obj.blockers) ? obj.blockers.map(String) : [],
        tool_errors: Array.isArray(obj.tool_errors)
          ? obj.tool_errors.map(String)
          : [],
        referenced_ids: Array.isArray(obj.referenced_ids)
          ? obj.referenced_ids.map(String)
          : [],
        mailbox_ack:
          obj.mailbox_ack &&
          typeof obj.mailbox_ack === 'object' &&
          typeof (obj.mailbox_ack as Record<string, unknown>).batch_id === 'string' &&
          Array.isArray((obj.mailbox_ack as Record<string, unknown>).message_ids)
            ? {
                batch_id: String((obj.mailbox_ack as Record<string, unknown>).batch_id),
                message_ids: (
                  (obj.mailbox_ack as Record<string, unknown>).message_ids as unknown[]
                ).map(String),
              }
            : undefined,
        raw_text: trimmed,
      };
    } catch {
      // Fall through to deterministic raw-transcript parser
    }
  }

  // Deterministic raw conversation extraction (`evidence_based` zero-model fallback)
  const lines = trimmed.split(/\r?\n/);
  const turns: ParsedTurnMessage[] = [];
  const toolErrors: string[] = [];
  let curRole: ParsedTurnMessage['role'] = 'assistant';
  let curBuf: string[] = [];

  const flushTurn = () => {
    const body = curBuf.join('\n').trim();
    if (body.length > 0) {
      turns.push({
        role: curRole,
        text: body,
        source_ref: `raw_turn_${turns.length + 1}`,
      });
    }
    curBuf = [];
  };

  for (const line of lines) {
    const userPrefix = line.match(/^(?:user|human|用户|我)\s*[:：]\s*(.*)$/i);
    const asstPrefix = line.match(/^(?:assistant|ai|model|模型)\s*[:：]\s*(.*)$/i);
    const toolPrefix = line.match(/^(?:tool|error|stderr|工具)\s*[:：]\s*(.*)$/i);

    if (userPrefix) {
      flushTurn();
      curRole = 'user';
      if (userPrefix[1]) curBuf.push(userPrefix[1]);
    } else if (asstPrefix) {
      flushTurn();
      curRole = 'assistant';
      if (asstPrefix[1]) curBuf.push(asstPrefix[1]);
    } else if (toolPrefix) {
      flushTurn();
      curRole = 'tool';
      if (toolPrefix[1]) curBuf.push(toolPrefix[1]);
    } else {
      curBuf.push(line);
    }

    if (/\b(?:Error|Exception|ENOENT|EEXIST|FAIL)\b/i.test(line)) {
      toolErrors.push(line.trim());
    }
  }
  flushTurn();

  const recent = turns.slice(-8);
  const summaryFallback = `[UNSUMMARIZED_EVIDENCE_HANDOFF] ${turns.length} raw turn(s) extracted from ${sourceLabel}.`;

  return {
    mode: 'evidence_based',
    continuity_level: 'Evidence Recovery',
    source_label: sourceLabel,
    recent_turns: recent.length > 0 ? recent : [{ role: 'assistant', text: trimmed, source_ref: 'raw_1' }],
    claimed_user_quotes: turns.filter((t) => t.role === 'user').map((t) => t.text.slice(0, 240)),
    summary_text: summaryFallback,
    pending_tasks: [],
    blockers: [],
    tool_errors: toolErrors.slice(-6),
    referenced_ids: extractSuspiciousThreadPassIds(trimmed),
    raw_text: trimmed,
  };
}

export function extractSuspiciousThreadPassIds(text: string): string[] {
  const matches = text.match(
    /\b(?:thp|thr|strm|anc|evt|txn|disp|batch|env)_[a-f0-9]{6,}\b/g,
  );
  return matches ? [...new Set(matches)] : [];
}

/**
 * Converts parsed turns into canonical EventRecords + InvocationRecord.
 * Guarantees deterministic `event_id` for idempotency and assigns conservative `trust_tier`.
 */
export function buildEventsFromAdoptedPackage(params: {
  streamId: string;
  txnId: string;
  pkg: ParsedAdoptPackage;
  trustTier: TrustTier;
  provenance: ProvenanceChain;
  startSeq?: number;
  nowIso?: string;
}): { events: EventRecord[]; invocation: InvocationRecord } {
  const nowIso = params.nowIso ?? new Date().toISOString();
  let seq = params.startSeq ?? 1;
  const events: EventRecord[] = [];
  const inputRefs: string[] = [];
  const outputRefs: string[] = [];

  for (let i = 0; i < params.pkg.recent_turns.length; i++) {
    const t = params.pkg.recent_turns[i];
    const fp = sha256Hex(`${params.pkg.source_label}|${i}|${t.role}|${t.text}`);
    const eventId = deterministicEventId(params.streamId, fp);
    const ev: EventRecord = {
      schema_version: SCHEMA_VERSION,
      kind: 'event',
      event_id: eventId,
      stream_id: params.streamId,
      seq: seq++,
      commit_txn_id: params.txnId,
      trust_tier: params.trustTier,
      provenance: {
        ...params.provenance,
        source_event_ref: t.source_ref ?? `turn_${i + 1}`,
      },
      created_at: nowIso,
      payload:
        t.role === 'tool'
          ? {
              type: 'tool_result',
              call_id: `call_${i + 1}`,
              status: 'succeeded',
              is_error: /error|fail/i.test(t.text),
              text: t.text,
            }
          : t.role === 'user'
            ? {
                type: 'human_message',
                // Even if pasted text claims `user:`, trust_tier remains `unverified_import` / `untrusted_peer`
                text: t.text,
              }
            : {
                type: 'assistant_part',
                part_type: 'text',
                text: t.text,
              },
    };
    events.push(ev);
    if (t.role === 'assistant') {
      outputRefs.push(eventId);
    } else {
      inputRefs.push(eventId);
    }
  }

  const invocation: InvocationRecord = {
    schema_version: SCHEMA_VERSION,
    kind: 'invocation',
    invocation_id: newId('inv', 8),
    stream_id: params.streamId,
    commit_txn_id: params.txnId,
    invocation_env_ref: `env_hash_${sha256Hex(params.pkg.source_label).slice(0, 12)}`,
    input_refs: inputRefs,
    input_scope: params.pkg.mode === 'evidence_based' ? 'partial' : 'inferred',
    input_projection: inputRefs.map((id, idx) => ({
      source_event_id: id,
      projected_role: 'user',
      modality: 'text',
      order_index: idx,
      transform: 'verbatim',
    })),
    projection_fidelity: 'reconstructed',
    output_refs: outputRefs,
    created_at: nowIso,
  };

  return { events, invocation };
}
