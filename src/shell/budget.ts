import { sha256Hex } from '../core/canonical.js';

export interface HostBudgetSpec {
  effective_context_budget: number; // chars/tokens conservative ceiling
  per_turn_input_limit: number;
  budget_confidence: 'exact' | 'estimated' | 'heuristic';
  supports_attachments: boolean;
}

export const DEFAULT_HOST_BUDGET: HostBudgetSpec = {
  effective_context_budget: 120_000,
  per_turn_input_limit: 24_000,
  budget_confidence: 'heuristic',
  supports_attachments: true,
};

export interface BudgetedSection {
  key: string;
  priority: 1 | 2 | 3 | 4 | 5; // 1 = non-droppable core briefing/role, 5 = foldable verbatim extra history
  title: string;
  body: string;
  content_hash: string;
  pointer_ref?: string;
}

export interface PackedEnvelopeResult {
  envelope_id: string;
  full_markdown: string;
  manifest_hash: string;
  utf8_bytes: number;
  estimated_tokens: number;
  parts: Array<{
    part_index: number;
    total_parts: number;
    part_hash: string;
    text: string;
  }>;
  folded_sections: string[];
  deduplicated_sections: string[];
}

export class BudgetExceededError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'BudgetExceededError';
  }
}

/**
 * Rough cross-tokenizer conservative token estimate (CJK chars ~1.6 tokens, ASCII ~0.4 tokens).
 */
export function estimateConservativeTokens(text: string): number {
  let cjk = 0;
  for (let i = 0; i < text.length; i++) {
    if (text.charCodeAt(i) > 0x2e80) cjk += 1;
  }
  const nonCjk = text.length - cjk;
  return Math.ceil(cjk * 1.6 + nonCjk * 0.4 + 32);
}

function safeSurrogateSliceEnd(str: string, start: number, maxLen: number): number {
  let end = Math.min(str.length, start + maxLen);
  if (end < str.length && end > start) {
    const prevCode = str.charCodeAt(end - 1);
    if (prevCode >= 0xd800 && prevCode <= 0xdbff) {
      end -= 1;
    }
  }
  return end <= start ? Math.min(str.length, start + maxLen) : end;
}

/**
 * Splits markdown into chunks along newline boundaries so fence markers and lines
 * are never severed mid-line or mid-surrogate-pair.
 */
export function splitIntoLineAwareChunks(full: string, payloadMax: number): string[] {
  const limit = Math.max(256, payloadMax);
  const lines = full.split('\n');
  const chunks: string[] = [];
  let cur = '';

  for (const line of lines) {
    if (line.length > limit) {
      if (cur.length > 0) {
        chunks.push(cur);
        cur = '';
      }
      let pos = 0;
      while (pos < line.length) {
        const end = safeSurrogateSliceEnd(line, pos, limit);
        chunks.push(line.slice(pos, end));
        pos = end;
      }
      continue;
    }

    const candidate = cur.length === 0 ? line : `${cur}\n${line}`;
    if (candidate.length <= limit) {
      cur = candidate;
    } else {
      chunks.push(cur);
      cur = line;
    }
  }

  if (cur.length > 0) {
    chunks.push(cur);
  }
  return chunks.length > 0 ? chunks : [''];
}

/**
 * Applies `effective_context_budget > completeness_floor` rule, deduplicates identical
 * content blocks across multi-root sources by `content_hash`, trims verbose priority-3
 * bodies fairly before folding lower-priority sections deterministically, fails closed
 * if priority-1 core exceeds total budget, and splits into line-aware `Part i/N` chunks
 * when exceeding `per_turn_input_limit`.
 */
export function packBudgetedEnvelope(
  envelopeId: string,
  sections: readonly BudgetedSection[],
  budget: HostBudgetSpec = DEFAULT_HOST_BUDGET,
): PackedEnvelopeResult {
  const seenHashes = new Map<string, string>();
  const seenBlockHashes = new Map<string, string>();
  const deduplicated: string[] = [];
  const normalizedSections: BudgetedSection[] = [];

  for (const sec of sections) {
    const firstKey = seenHashes.get(sec.content_hash);
    if (firstKey && sec.body.length > 200) {
      deduplicated.push(`${sec.key} -> ${firstKey}`);
      normalizedSections.push({
        ...sec,
        body: `[DEDUPLICATED: identical content_hash=${sec.content_hash.slice(0, 16)} already presented in ${firstKey}]`,
      });
    } else {
      seenHashes.set(sec.content_hash, sec.key);
      if (sec.priority >= 2) {
        const lines = sec.body.split('\n');
        const dedupedLines = lines.map((line) => {
          const trimmed = line.trim();
          if (
            trimmed.length >= 180 &&
            !trimmed.startsWith('<<<') &&
            !trimmed.startsWith('[SECURITY NOTICE:')
          ) {
            const bHash = sha256Hex(trimmed);
            const firstOwner = seenBlockHashes.get(bHash);
            if (firstOwner && firstOwner !== sec.key) {
              deduplicated.push(`${sec.key}#block -> ${firstOwner}`);
              return `[DEDUPLICATED_BLOCK: content_hash=${bHash.slice(0, 16)} already presented in ${firstOwner}]`;
            }
            if (!firstOwner) {
              seenBlockHashes.set(bHash, sec.key);
            }
          }
          return line;
        });
        normalizedSections.push({ ...sec, body: dedupedLines.join('\n') });
      } else {
        normalizedSections.push(sec);
      }
    }
  }

  const folded: string[] = [];
  const working = normalizedSections.map((s) => ({ ...s }));

  const renderAll = (items: readonly BudgetedSection[]) =>
    items.map((s) => `## ${s.title}\n\n${s.body}`).join('\n\n');

  let full = renderAll(working);

  // Fair middle-trim pass on verbose priority-3 sections before dropping whole sources
  if (estimateConservativeTokens(full) > budget.effective_context_budget) {
    const trimCandidates = working
      .filter((s) => s.priority === 3 && s.body.length > 2400)
      .sort((a, b) => b.body.length - a.body.length);

    for (const sec of trimCandidates) {
      if (estimateConservativeTokens(full) <= budget.effective_context_budget) {
        break;
      }
      const headEnd = safeSurrogateSliceEnd(sec.body, 0, 1000);
      let tailStart = Math.max(headEnd, sec.body.length - 1000);
      if (tailStart > 0 && tailStart < sec.body.length) {
        const c = sec.body.charCodeAt(tailStart);
        if (c >= 0xdc00 && c <= 0xdfff) tailStart += 1;
      }
      sec.body = `${sec.body.slice(0, headEnd)}\n[... MIDDLE TRIMMED FOR BUDGET; run \`thp peek\` on ${sec.pointer_ref ?? sec.key} if needed ...]\n${sec.body.slice(tailStart)}`;
      folded.push(`${sec.key}#trimmed`);
      full = renderAll(working);
    }
  }

  // Deterministic folding from priority 5 down to 3 until within effective_context_budget
  for (const prio of [5, 4, 3] as const) {
    if (estimateConservativeTokens(full) <= budget.effective_context_budget) {
      break;
    }
    for (const sec of working) {
      if (sec.priority !== prio) continue;
      if (estimateConservativeTokens(full) <= budget.effective_context_budget) break;
      folded.push(sec.key);
      sec.body = `[FOLDED_FOR_BUDGET: pointer=${sec.pointer_ref ?? sec.key}; original_sha256=${sec.content_hash.slice(0, 16)}; run \`thp peek\` or inspect full bundle if needed]`;
      full = renderAll(working);
    }
  }

  if (estimateConservativeTokens(full) > budget.effective_context_budget) {
    throw new BudgetExceededError(
      `Core sections exceed target effective_context_budget (${estimateConservativeTokens(full)} > ${budget.effective_context_budget}, confidence=${budget.budget_confidence}). Refusing silent truncation (fail-closed).`,
    );
  }

  const manifestHash = sha256Hex(full);
  const utf8Bytes = Buffer.byteLength(full, 'utf8');
  const estimatedTokens = estimateConservativeTokens(full);

  if (full.length <= budget.per_turn_input_limit) {
    return {
      envelope_id: envelopeId,
      full_markdown: full,
      manifest_hash: manifestHash,
      utf8_bytes: utf8Bytes,
      estimated_tokens: estimatedTokens,
      parts: [
        {
          part_index: 1,
          total_parts: 1,
          part_hash: sha256Hex(full),
          text: full,
        },
      ],
      folded_sections: folded,
      deduplicated_sections: deduplicated,
    };
  }

  // Split into line-aware multi-part clipboard chunks respecting per_turn_input_limit
  const headerReserve = 420;
  const payloadMax = Math.max(1000, budget.per_turn_input_limit - headerReserve);
  const rawChunks = splitIntoLineAwareChunks(full, payloadMax);
  const totalParts = rawChunks.length;
  const parts = rawChunks.map((chunk, idx) => {
    const partIndex = idx + 1;
    const isFinal = partIndex === totalParts;
    const partHash = sha256Hex(chunk);
    const wrapped = [
      `<!-- THREADPASS_CHUNK envelope_id=${envelopeId} part=${partIndex}/${totalParts} part_hash=${partHash} -->`,
      isFinal
        ? `[FINAL PART ${partIndex}/${totalParts} | manifest_hash=${manifestHash}] All parts delivered. Verify part hashes and execute the Current Role Briefing instructions below.`
        : `[PART ${partIndex}/${totalParts}] Interaction protocol convention: do NOT execute tasks or generate final output until FINAL PART ${totalParts}/${totalParts} arrives (reply briefly: "Received Part ${partIndex}/${totalParts}").`,
      `<<<NON_EXECUTABLE_PART_PAYLOAD part="${partIndex}/${totalParts}">>>`,
      chunk,
      `<<<END_PART_PAYLOAD>>>`,
    ].join('\n\n');
    return {
      part_index: partIndex,
      total_parts: totalParts,
      part_hash: partHash,
      text: wrapped,
    };
  });

  return {
    envelope_id: envelopeId,
    full_markdown: full,
    manifest_hash: manifestHash,
    utf8_bytes: utf8Bytes,
    estimated_tokens: estimatedTokens,
    parts,
    folded_sections: folded,
    deduplicated_sections: deduplicated,
  };
}
