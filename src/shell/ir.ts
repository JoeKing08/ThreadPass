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
  decisions_made: string[];
  files_and_artifacts: string[];
  open_questions: string[];
  summary_text: string;
  recent_turns: ParsedTurnMessage[];
  pending_tasks: string[];
  blockers: string[];
  tool_errors: string[];
  referenced_ids: string[];
  mailbox_ack?: { batch_id: string; message_ids: string[] };
  parse_warnings: string[];
  raw_text: string;
}

export interface ExtractedJsonResult {
  obj: Record<string, unknown> | null;
  looksLikeJson: boolean;
  parseWarning?: string;
}

function isPlainObject(val: unknown): val is Record<string, unknown> {
  return typeof val === 'object' && val !== null && !Array.isArray(val);
}

function scanBalancedJsonObjects(text: string): Record<string, unknown>[] {
  const results: Record<string, unknown>[] = [];
  let i = 0;
  while (i < text.length) {
    if (text[i] !== '{') {
      i += 1;
      continue;
    }
    let depth = 0;
    let inString = false;
    let escaped = false;
    let endIdx = -1;
    for (let j = i; j < text.length; j++) {
      const ch = text[j];
      if (inString) {
        if (escaped) {
          escaped = false;
        } else if (ch === '\\') {
          escaped = true;
        } else if (ch === '"') {
          inString = false;
        }
      } else if (ch === '"') {
        inString = true;
      } else if (ch === '{') {
        depth += 1;
      } else if (ch === '}') {
        depth -= 1;
        if (depth === 0) {
          endIdx = j;
          break;
        }
      }
    }
    if (endIdx !== -1) {
      const candidate = text.slice(i, endIdx + 1);
      try {
        const parsed: unknown = JSON.parse(candidate);
        if (isPlainObject(parsed)) {
          results.push(parsed);
        }
      } catch {
        // Skip invalid balanced span and continue scanning
      }
      i = endIdx + 1;
    } else {
      i += 1;
    }
  }
  return results;
}

/**
 * Extracts a structured JSON object without relying on non-greedy code-fence regexes,
 * so embedded triple backticks inside JSON strings, bare fences, or surrounding prose
 * never truncate extraction.
 */
export function extractStructuredJsonObject(
  rawText: string,
  expectedKeys: readonly string[],
): ExtractedJsonResult {
  const trimmed = rawText.trim();
  if (!trimmed) {
    return { obj: null, looksLikeJson: false };
  }

  const hasExpectedKey = (obj: Record<string, unknown>): boolean =>
    expectedKeys.some((k) => Object.prototype.hasOwnProperty.call(obj, k));

  const looksLikeJson = expectedKeys.some((k) =>
    new RegExp(`"${k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}"\\s*:`).test(trimmed),
  );

  // 1. Fast path if whole input is already valid JSON
  if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (isPlainObject(parsed) && (hasExpectedKey(parsed) || expectedKeys.length === 0)) {
        return { obj: parsed, looksLikeJson: true };
      }
    } catch {
      // Fall through to fence and balanced scan
    }
  }

  // 2. Scan inside tagged ```json or bare ``` code fence regions (matching from opening fence)
  const fenceStartRegex = /```(?:json)?[ \t]*\r?\n?/gi;
  const fencedMatches: Record<string, unknown>[] = [];
  let match: RegExpExecArray | null;
  while ((match = fenceStartRegex.exec(trimmed)) !== null) {
    const afterFence = trimmed.slice(match.index + match[0].length);
    const objs = scanBalancedJsonObjects(afterFence);
    for (const o of objs) {
      if (hasExpectedKey(o)) {
        fencedMatches.push(o);
      }
    }
  }
  if (fencedMatches.length > 0) {
    return { obj: fencedMatches[fencedMatches.length - 1], looksLikeJson: true };
  }

  // 3. Scan top-level balanced JSON objects anywhere in rawText
  const topLevel = scanBalancedJsonObjects(trimmed).filter(hasExpectedKey);
  if (topLevel.length > 0) {
    return { obj: topLevel[topLevel.length - 1], looksLikeJson: true };
  }

  if (looksLikeJson) {
    return {
      obj: null,
      looksLikeJson: true,
      parseWarning:
        'Detected ThreadPass JSON key(s) in input, but valid RFC-8259 JSON extraction failed (check unescaped quotes/newlines or truncated brace); fell back safely.',
    };
  }
  return { obj: null, looksLikeJson: false };
}

export function normalizeHostSessionId(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const s = value.trim();
  if (!s) return undefined;
  const lower = s.toLowerCase();
  if (
    lower === 'null' ||
    lower === 'none' ||
    lower === 'n/a' ||
    lower === 'na' ||
    lower === 'unknown' ||
    lower === 'unset' ||
    lower === 'undefined' ||
    lower.startsWith('optional-') ||
    lower.includes('if-known') ||
    lower.includes('placeholder')
  ) {
    return undefined;
  }
  return s;
}

function normalizeStringArray(val: unknown): string[] {
  if (!Array.isArray(val)) return [];
  const out: string[] = [];
  for (const item of val) {
    if (typeof item === 'string') {
      const s = item.trim();
      if (s) out.push(s);
    } else if (isPlainObject(item)) {
      const dec =
        typeof item.decision === 'string'
          ? item.decision.trim()
          : typeof item.title === 'string'
            ? item.title.trim()
            : typeof item.text === 'string'
              ? item.text.trim()
              : '';
      const rat =
        typeof item.rationale === 'string'
          ? item.rationale.trim()
          : typeof item.reason === 'string'
            ? item.reason.trim()
            : '';
      if (dec && rat) {
        out.push(`${dec} — ${rat}`);
      } else if (dec) {
        out.push(dec);
      } else {
        out.push(JSON.stringify(item));
      }
    } else if (item !== null && item !== undefined) {
      out.push(String(item));
    }
  }
  return out;
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
  const extracted = extractStructuredJsonObject(trimmed, [
    'threadpass_handoff',
    'handoff_mode',
    'summary_text',
    'recent_turns',
  ]);
  const parseWarnings: string[] = [];
  if (extracted.parseWarning) {
    parseWarnings.push(extracted.parseWarning);
  }

  if (extracted.obj) {
    const obj = extracted.obj;
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
      host_session_id: normalizeHostSessionId(obj.host_session_id),
      claimed_user_quotes: normalizeStringArray(obj.claimed_user_quotes),
      decisions_made: normalizeStringArray(obj.decisions_made),
      files_and_artifacts: normalizeStringArray(obj.files_and_artifacts),
      open_questions: normalizeStringArray(obj.open_questions),
      summary_text: String(obj.summary_text ?? obj.summary ?? ''),
      recent_turns: turns,
      pending_tasks: normalizeStringArray(obj.pending_tasks),
      blockers: normalizeStringArray(obj.blockers),
      tool_errors: normalizeStringArray(obj.tool_errors),
      referenced_ids: normalizeStringArray(obj.referenced_ids),
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
      parse_warnings: parseWarnings,
      raw_text: trimmed,
    };
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
    decisions_made: [],
    files_and_artifacts: [],
    open_questions: [],
    summary_text: summaryFallback,
    pending_tasks: [],
    blockers: [],
    tool_errors: toolErrors.slice(-6),
    referenced_ids: extractSuspiciousThreadPassIds(trimmed),
    parse_warnings: parseWarnings,
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
 * Converts parsed turns AND structured state (decisions, files, pending_tasks, blockers,
 * open_questions, tool_errors, claimed_user_quotes) into canonical EventRecords + InvocationRecord.
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

  const stateSections: string[] = [];
  if (params.pkg.decisions_made.length > 0) {
    stateSections.push(`- Decisions Made:\n  - ${params.pkg.decisions_made.join('\n  - ')}`);
  }
  if (params.pkg.files_and_artifacts.length > 0) {
    stateSections.push(`- Files & Artifacts:\n  - ${params.pkg.files_and_artifacts.join('\n  - ')}`);
  }
  if (params.pkg.pending_tasks.length > 0) {
    stateSections.push(`- Pending Tasks:\n  - ${params.pkg.pending_tasks.join('\n  - ')}`);
  }
  if (params.pkg.blockers.length > 0) {
    stateSections.push(`- Blockers:\n  - ${params.pkg.blockers.join('\n  - ')}`);
  }
  if (params.pkg.open_questions.length > 0) {
    stateSections.push(`- Open Questions:\n  - ${params.pkg.open_questions.join('\n  - ')}`);
  }
  if (params.pkg.claimed_user_quotes.length > 0) {
    stateSections.push(`- Claimed User Quotes:\n  - ${params.pkg.claimed_user_quotes.join('\n  - ')}`);
  }

  if (stateSections.length > 0) {
    const stateBody = `[STRUCTURED_HANDOFF_STATE]\n${stateSections.join('\n')}`;
    const stateFp = sha256Hex(`${params.pkg.source_label}|structured_state|${stateBody}`);
    const stateEventId = deterministicEventId(params.streamId, stateFp);
    events.push({
      schema_version: SCHEMA_VERSION,
      kind: 'event',
      event_id: stateEventId,
      stream_id: params.streamId,
      seq: seq++,
      commit_txn_id: params.txnId,
      trust_tier: params.trustTier,
      provenance: {
        ...params.provenance,
        source_event_ref: 'structured_handoff_state',
      },
      created_at: nowIso,
      payload: {
        type: 'assistant_part',
        part_type: 'text',
        text: stateBody,
      },
    });
    outputRefs.push(stateEventId);
  }

  if (params.pkg.tool_errors.length > 0) {
    const errBody = `[STRUCTURED_TOOL_ERRORS]\n- ${params.pkg.tool_errors.join('\n- ')}`;
    const errFp = sha256Hex(`${params.pkg.source_label}|structured_errors|${errBody}`);
    const errEventId = deterministicEventId(params.streamId, errFp);
    events.push({
      schema_version: SCHEMA_VERSION,
      kind: 'event',
      event_id: errEventId,
      stream_id: params.streamId,
      seq: seq++,
      commit_txn_id: params.txnId,
      trust_tier: params.trustTier,
      provenance: {
        ...params.provenance,
        source_event_ref: 'structured_tool_errors',
      },
      created_at: nowIso,
      payload: {
        type: 'tool_result',
        call_id: 'call_structured_errors',
        status: 'failed',
        is_error: true,
        text: errBody,
      },
    });
    inputRefs.push(errEventId);
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
