import { canonicalJson, deterministicEventId, newId, sha256Hex } from '../core/canonical.js';
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

export const MAX_MULTIPART_PARTS = 64;
export const MAX_MULTIPART_INPUT_BYTES = 4 * 1024 * 1024;

export interface AssembledMultipartInfo {
  group_id: string;
  total_parts: number;
}

export class IncompleteMultipartOutputError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'IncompleteMultipartOutputError';
  }
}

export interface ParsedAdoptPackage {
  mode: HandoffMode;
  continuity_level: ContinuityLevel;
  source_label: string;
  base_anchor_id?: string;
  host_session_id?: string;
  transcript_file_path?: string;
  machine_hostname?: string;
  cloud_provider?: string;
  remote_uri?: string;
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
  assembled_parts?: AssembledMultipartInfo;
  parse_warnings: string[];
  raw_text: string;
}

export interface ExtractedJsonObjectsResult {
  objects: Record<string, unknown>[];
  looksLikeJson: boolean;
  parseWarning?: string;
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
 * Extracts one or more structured JSON objects without relying on non-greedy code-fence regexes,
 * so embedded triple backticks inside JSON strings, bare fences, multipart chunks, or prose
 * never truncate extraction.
 */
export function extractStructuredJsonObjects(
  rawText: string,
  expectedKeys: readonly string[],
): ExtractedJsonObjectsResult {
  if (Buffer.byteLength(rawText, 'utf8') > MAX_MULTIPART_INPUT_BYTES) {
    throw new IncompleteMultipartOutputError(
      `Structured input exceeds maximum allowed size (${MAX_MULTIPART_INPUT_BYTES} bytes)`,
    );
  }
  const trimmed = rawText.trim();
  if (!trimmed) {
    return { objects: [], looksLikeJson: false };
  }

  const hasExpectedKey = (obj: Record<string, unknown>): boolean =>
    expectedKeys.length === 0 ||
    expectedKeys.some((k) => Object.prototype.hasOwnProperty.call(obj, k));

  const looksLikeJson = expectedKeys.some((k) =>
    new RegExp(`"${k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}"\\s*:`).test(trimmed),
  );

  // 1. Fast path if whole input is already valid JSON object or array of objects
  if (
    (trimmed.startsWith('{') && trimmed.endsWith('}')) ||
    (trimmed.startsWith('[') && trimmed.endsWith(']'))
  ) {
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (isPlainObject(parsed) && hasExpectedKey(parsed)) {
        return { objects: [parsed], looksLikeJson: true };
      }
      if (Array.isArray(parsed)) {
        const matched = parsed.filter(
          (item): item is Record<string, unknown> => isPlainObject(item) && hasExpectedKey(item),
        );
        if (matched.length > 0) {
          return { objects: matched, looksLikeJson: true };
        }
      }
    } catch {
      // Fall through to fence and balanced scan
    }
  }

  // 2. Scan inside tagged ```json or bare ``` code fence blocks using a line state machine
  // so closing ``` fences of ```ts blocks are never mistaken for opening JSON fences.
  const lines = trimmed.split(/\r?\n/);
  const fencedMatches: Record<string, unknown>[] = [];
  let inFence = false;
  let fenceLang = '';
  let fenceBuf: string[] = [];

  const collectFromFence = () => {
    if (fenceLang === '' || fenceLang === 'json') {
      const inner = fenceBuf.join('\n').trimStart();
      if (inner.startsWith('{') || inner.startsWith('[')) {
        const objs = scanBalancedJsonObjects(inner).filter(hasExpectedKey);
        fencedMatches.push(...objs);
      }
    }
    fenceLang = '';
    fenceBuf = [];
  };

  for (const line of lines) {
    if (!inFence) {
      const openMatch = line.match(/^[ \t]*```([a-zA-Z0-9_-]*)[ \t]*$/);
      if (openMatch) {
        inFence = true;
        fenceLang = openMatch[1].toLowerCase();
        fenceBuf = [];
      }
    } else if (/^[ \t]*```[ \t]*$/.test(line)) {
      inFence = false;
      collectFromFence();
    } else {
      fenceBuf.push(line);
    }
  }
  if (inFence) {
    collectFromFence();
  }
  if (fencedMatches.length > 0) {
    return { objects: fencedMatches, looksLikeJson: true };
  }

  // 3. Scan top-level balanced JSON objects anywhere in rawText
  const topLevel = scanBalancedJsonObjects(trimmed).filter(hasExpectedKey);
  if (topLevel.length > 0) {
    return { objects: topLevel, looksLikeJson: true };
  }

  if (looksLikeJson) {
    return {
      objects: [],
      looksLikeJson: true,
      parseWarning:
        'Detected ThreadPass JSON key(s) in input, but valid RFC-8259 JSON extraction failed (check unescaped quotes/newlines or truncated brace); fell back safely.',
    };
  }
  return { objects: [], looksLikeJson: false };
}

export function extractStructuredJsonObject(
  rawText: string,
  expectedKeys: readonly string[],
): ExtractedJsonResult {
  const res = extractStructuredJsonObjects(rawText, expectedKeys);
  return {
    obj: res.objects.length > 0 ? res.objects[res.objects.length - 1] : null,
    looksLikeJson: res.looksLikeJson,
    parseWarning: res.parseWarning,
  };
}

function hasMultipartFields(obj: Record<string, unknown>): boolean {
  return (
    (obj.part_index !== undefined && obj.part_index !== null) ||
    (obj.total_parts !== undefined && obj.total_parts !== null) ||
    (typeof obj.part_group_id === 'string' && obj.part_group_id.trim().length > 0) ||
    typeof obj.is_final === 'boolean'
  );
}

/**
 * Validates and orders single- or multi-part model JSON outputs.
 * When multipart envelope metadata is present, enforces per-part group_id, part_index,
 * explicit boolean is_final, max part ceiling, missing-part fail-closed erroring,
 * and refusal when multiple distinct non-multipart JSON objects appear together.
 */
export function assembleMultipartObjects(objects: readonly Record<string, unknown>[]): {
  parts: Record<string, unknown>[];
  multipart?: AssembledMultipartInfo;
} {
  if (objects.length === 0) {
    return { parts: [] };
  }
  const mpObjs = objects.filter(hasMultipartFields);
  if (mpObjs.length === 0) {
    if (objects.length > 1) {
      const firstCan = canonicalJson(objects[0]);
      const allSame = objects.every((o) => canonicalJson(o) === firstCan);
      if (!allSame) {
        throw new IncompleteMultipartOutputError(
          `Found ${objects.length} distinct ThreadPass JSON objects without multipart metadata (part_group_id, part_index, is_final); refusing to silently drop earlier objects`,
        );
      }
    }
    return { parts: [objects[0]] };
  }

  if (mpObjs.length !== objects.length) {
    throw new IncompleteMultipartOutputError(
      'Mixed multipart and non-multipart ThreadPass JSON objects in input; refusing partial assembly',
    );
  }
  if (mpObjs.length > MAX_MULTIPART_PARTS * 2) {
    throw new IncompleteMultipartOutputError(
      `Multipart output exceeds maximum object ceiling (${mpObjs.length} > ${MAX_MULTIPART_PARTS * 2})`,
    );
  }

  const groupIds = new Set<string>();
  const declaredTotals = new Set<number>();
  const byIndex = new Map<number, Record<string, unknown>>();
  const finalIndices = new Set<number>();
  const nonFinalIndices = new Set<number>();

  for (const obj of mpObjs) {
    const gid = typeof obj.part_group_id === 'string' ? obj.part_group_id.trim() : '';
    if (!gid) {
      throw new IncompleteMultipartOutputError(
        'Multipart output entry is missing a non-empty string part_group_id',
      );
    }
    groupIds.add(gid);

    const idx = obj.part_index;
    if (
      typeof idx !== 'number' ||
      !Number.isInteger(idx) ||
      idx < 1 ||
      idx > MAX_MULTIPART_PARTS
    ) {
      throw new IncompleteMultipartOutputError(
        `Multipart output entry must have integer part_index in 1..${MAX_MULTIPART_PARTS} (got ${JSON.stringify(idx)})`,
      );
    }

    if (typeof obj.is_final !== 'boolean') {
      throw new IncompleteMultipartOutputError(
        `Multipart output part ${idx} (group=${gid}) must specify explicit boolean is_final`,
      );
    }

    if (obj.total_parts !== undefined && obj.total_parts !== null) {
      const tot = obj.total_parts;
      if (
        typeof tot !== 'number' ||
        !Number.isInteger(tot) ||
        tot < 1 ||
        tot > MAX_MULTIPART_PARTS
      ) {
        throw new IncompleteMultipartOutputError(
          `Multipart output part ${idx} has invalid total_parts (expected 1..${MAX_MULTIPART_PARTS}, got ${JSON.stringify(tot)})`,
        );
      }
      declaredTotals.add(tot);
    }

    const existing = byIndex.get(idx);
    if (existing) {
      if (canonicalJson(existing) !== canonicalJson(obj)) {
        throw new IncompleteMultipartOutputError(
          `Conflicting duplicate payload for part_index=${idx}`,
        );
      }
    } else {
      byIndex.set(idx, obj);
    }
    if (obj.is_final === true) {
      finalIndices.add(idx);
    } else {
      nonFinalIndices.add(idx);
    }
  }

  if (groupIds.size > 1) {
    throw new IncompleteMultipartOutputError(
      `Conflicting part_group_id values in pasted multipart output: ${[...groupIds].join(', ')}`,
    );
  }
  if (declaredTotals.size > 1) {
    throw new IncompleteMultipartOutputError(
      `Conflicting total_parts declarations in multipart output: ${[...declaredTotals].join(', ')}`,
    );
  }

  const groupId = [...groupIds][0];
  let expectedTotal: number;

  if (declaredTotals.size === 1) {
    expectedTotal = [...declaredTotals][0];
    for (const idx of byIndex.keys()) {
      if (idx > expectedTotal) {
        throw new IncompleteMultipartOutputError(
          `Multipart output (group=${groupId}) has part_index=${idx} exceeding total_parts=${expectedTotal}`,
        );
      }
    }
    for (const idx of finalIndices) {
      if (idx !== expectedTotal) {
        throw new IncompleteMultipartOutputError(
          `Multipart output (group=${groupId}) marked part ${idx} as is_final=true before total_parts=${expectedTotal}`,
        );
      }
    }
    if (!finalIndices.has(expectedTotal) || nonFinalIndices.has(expectedTotal)) {
      throw new IncompleteMultipartOutputError(
        `Multipart output (group=${groupId}) final part ${expectedTotal} must have is_final=true`,
      );
    }
  } else {
    if (finalIndices.size === 0) {
      throw new IncompleteMultipartOutputError(
        `Incomplete multipart output (group=${groupId}): received part(s) ${[...byIndex.keys()].sort((a, b) => a - b).join(', ')} but no part has is_final=true`,
      );
    }
    if (finalIndices.size > 1) {
      throw new IncompleteMultipartOutputError(
        `Conflicting is_final=true on multiple parts (${[...finalIndices].sort((a, b) => a - b).join(', ')}) in group=${groupId}`,
      );
    }
    expectedTotal = [...finalIndices][0];
    for (const idx of byIndex.keys()) {
      if (idx > expectedTotal) {
        throw new IncompleteMultipartOutputError(
          `Multipart output (group=${groupId}) has part_index=${idx} after final part ${expectedTotal}`,
        );
      }
    }
  }

  const missing: number[] = [];
  const ordered: Record<string, unknown>[] = [];
  for (let i = 1; i <= expectedTotal; i++) {
    const entry = byIndex.get(i);
    if (!entry) {
      missing.push(i);
    } else {
      ordered.push(entry);
    }
  }
  if (missing.length > 0) {
    throw new IncompleteMultipartOutputError(
      `Incomplete multipart output (group=${groupId}): missing part(s) ${missing.join(', ')} of ${expectedTotal}`,
    );
  }

  return {
    parts: ordered,
    multipart: { group_id: groupId, total_parts: expectedTotal },
  };
}

export function normalizeOptionalMetaString(value: unknown): string | undefined {
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

export function normalizeHostSessionId(value: unknown): string | undefined {
  return normalizeOptionalMetaString(value);
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
  const extracted = extractStructuredJsonObjects(trimmed, [
    'threadpass_handoff',
    'handoff_mode',
    'summary_text',
    'recent_turns',
  ]);
  const parseWarnings: string[] = [];
  if (extracted.parseWarning) {
    parseWarnings.push(extracted.parseWarning);
  }

  if (extracted.objects.length > 0) {
    const assembled = assembleMultipartObjects(extracted.objects);
    const parts = assembled.parts;

    const dedupPush = (target: string[], seen: Set<string>, items: readonly string[]) => {
      for (const s of items) {
        if (!seen.has(s)) {
          seen.add(s);
          target.push(s);
        }
      }
    };

    let mode: HandoffMode = 'model_authored';
    let resolvedLabel: string | undefined;
    let baseAnchorId: string | undefined;
    let hostSessionId: string | undefined;
    let transcriptFilePath: string | undefined;
    let machineHostname: string | undefined;
    let cloudProvider: string | undefined;
    let remoteUri: string | undefined;

    const summaries: string[] = [];
    const seenSummaries = new Set<string>();
    const turns: ParsedTurnMessage[] = [];
    const seenTurnKeys = new Set<string>();
    const claimedUserQuotes: string[] = [];
    const seenQuotes = new Set<string>();
    const decisionsMade: string[] = [];
    const seenDecisions = new Set<string>();
    const filesAndArtifacts: string[] = [];
    const seenFiles = new Set<string>();
    const openQuestions: string[] = [];
    const seenQuestions = new Set<string>();
    const pendingTasks: string[] = [];
    const seenTasks = new Set<string>();
    const blockers: string[] = [];
    const seenBlockers = new Set<string>();
    const toolErrors: string[] = [];
    const seenErrors = new Set<string>();
    const referencedIds: string[] = [];
    const seenRefs = new Set<string>();

    let ackBatchId: string | undefined;
    const ackMessageIds: string[] = [];
    const seenAckMsgIds = new Set<string>();

    for (const obj of parts) {
      if (obj.handoff_mode === 'evidence_based') {
        mode = 'evidence_based';
      }
      if (!resolvedLabel && typeof obj.source_label === 'string' && obj.source_label.trim()) {
        resolvedLabel = obj.source_label.trim();
      }
      const partBaseAnchor = normalizeOptionalMetaString(obj.base_anchor_id);
      if (partBaseAnchor) {
        if (baseAnchorId && baseAnchorId !== partBaseAnchor) {
          throw new IncompleteMultipartOutputError(
            `Conflicting base_anchor_id across multipart output (${baseAnchorId} vs ${partBaseAnchor})`,
          );
        }
        baseAnchorId = partBaseAnchor;
      }
      hostSessionId ??= normalizeHostSessionId(obj.host_session_id);
      transcriptFilePath ??= normalizeOptionalMetaString(obj.transcript_file_path);
      machineHostname ??= normalizeOptionalMetaString(obj.machine_hostname);
      cloudProvider ??= normalizeOptionalMetaString(obj.cloud_provider);
      remoteUri ??= normalizeOptionalMetaString(obj.remote_uri);

      const rawSummary = String(obj.summary_text ?? obj.summary ?? '').trim();
      if (rawSummary && !seenSummaries.has(rawSummary)) {
        seenSummaries.add(rawSummary);
        summaries.push(rawSummary);
      }

      if (Array.isArray(obj.recent_turns)) {
        for (let idx = 0; idx < obj.recent_turns.length; idx++) {
          const t = obj.recent_turns[idx] as Record<string, unknown>;
          const role: ParsedTurnMessage['role'] =
            t?.role === 'user' || t?.role === 'assistant' || t?.role === 'tool'
              ? t.role
              : 'assistant';
          const text = String(t?.text ?? t?.content ?? '');
          const sourceRef = String(t?.source_ref ?? `turn_${turns.length + 1}`);
          const dedupKey = `${role}|${sourceRef}|${text}`;
          if (!seenTurnKeys.has(dedupKey)) {
            seenTurnKeys.add(dedupKey);
            turns.push({ role, text, source_ref: sourceRef });
          }
        }
      }

      dedupPush(claimedUserQuotes, seenQuotes, normalizeStringArray(obj.claimed_user_quotes));
      dedupPush(decisionsMade, seenDecisions, normalizeStringArray(obj.decisions_made));
      dedupPush(filesAndArtifacts, seenFiles, normalizeStringArray(obj.files_and_artifacts));
      dedupPush(openQuestions, seenQuestions, normalizeStringArray(obj.open_questions));
      dedupPush(pendingTasks, seenTasks, normalizeStringArray(obj.pending_tasks));
      dedupPush(blockers, seenBlockers, normalizeStringArray(obj.blockers));
      dedupPush(toolErrors, seenErrors, normalizeStringArray(obj.tool_errors));
      dedupPush(referencedIds, seenRefs, normalizeStringArray(obj.referenced_ids));

      if (
        obj.mailbox_ack &&
        typeof obj.mailbox_ack === 'object' &&
        typeof (obj.mailbox_ack as Record<string, unknown>).batch_id === 'string' &&
        Array.isArray((obj.mailbox_ack as Record<string, unknown>).message_ids)
      ) {
        const mb = obj.mailbox_ack as Record<string, unknown>;
        const bid = String(mb.batch_id);
        if (ackBatchId && ackBatchId !== bid) {
          throw new IncompleteMultipartOutputError(
            `Conflicting mailbox_ack.batch_id across multipart output (${ackBatchId} vs ${bid})`,
          );
        }
        ackBatchId = bid;
        for (const mid of (mb.message_ids as unknown[]).map(String)) {
          if (!seenAckMsgIds.has(mid)) {
            seenAckMsgIds.add(mid);
            ackMessageIds.push(mid);
          }
        }
      }
    }

    return {
      mode,
      continuity_level:
        mode === 'evidence_based' ? 'Evidence Recovery' : 'Prompt Reconstruction',
      source_label: resolvedLabel ?? sourceLabel,
      ...(baseAnchorId ? { base_anchor_id: baseAnchorId } : {}),
      host_session_id: hostSessionId,
      transcript_file_path: transcriptFilePath,
      machine_hostname: machineHostname,
      cloud_provider: cloudProvider,
      remote_uri: remoteUri,
      claimed_user_quotes: claimedUserQuotes,
      decisions_made: decisionsMade,
      files_and_artifacts: filesAndArtifacts,
      open_questions: openQuestions,
      summary_text: summaries.join('\n\n'),
      recent_turns: turns,
      pending_tasks: pendingTasks,
      blockers: blockers,
      tool_errors: toolErrors,
      referenced_ids: referencedIds,
      mailbox_ack: ackBatchId ? { batch_id: ackBatchId, message_ids: ackMessageIds } : undefined,
      ...(assembled.multipart ? { assembled_parts: assembled.multipart } : {}),
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

  const recent = turns.slice(-24);
  if (turns.length > 24) {
    parseWarnings.push(
      `Raw fallback truncated ${turns.length - 24} earlier turn(s) from inline events; full raw text preserved in snapshot blob when available.`,
    );
  }
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
  turnScopeSeed?: string;
}): { events: EventRecord[]; invocation: InvocationRecord } {
  const nowIso = params.nowIso ?? new Date().toISOString();
  let seq = params.startSeq ?? 1;
  const events: EventRecord[] = [];
  const inputRefs: string[] = [];
  const outputRefs: string[] = [];

  const scopePrefix = params.turnScopeSeed ? `${params.turnScopeSeed}|` : '';

  for (let i = 0; i < params.pkg.recent_turns.length; i++) {
    const t = params.pkg.recent_turns[i];
    const fp = sha256Hex(`${scopePrefix}${params.pkg.source_label}|${i}|${t.role}|${t.text}`);
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
    const stateFp = sha256Hex(`${scopePrefix}${params.pkg.source_label}|structured_state|${stateBody}`);
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
    const errFp = sha256Hex(`${scopePrefix}${params.pkg.source_label}|structured_errors|${errBody}`);
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
