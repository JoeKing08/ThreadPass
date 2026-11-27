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
export function splitIntoLineAwareChunks(
  full: string,
  payloadMax: number,
  minLimit = 256,
): string[] {
  const floor = Math.max(16, Math.min(256, minLimit));
  const limit = Math.max(floor, payloadMax);
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
 * Splits markdown into chunks respecting BOTH line boundaries AND XML `<archived_reference_...>` /
 * legacy `<<<UNTRUSTED_DATA_...>>>` fence blocks so every single part keeps balanced open/close tags.
 */
export function splitIntoFenceAwareChunks(full: string, payloadMax: number): string[] {
  const limit = Math.max(256, payloadMax);
  const rawLines = full.split('\n');
  const units: Array<{ kind: 'plain' | 'xml_fence' | 'legacy_fence'; text: string }> = [];

  let i = 0;
  while (i < rawLines.length) {
    const line = rawLines[i];
    const trimmedLine = line.trim();
    const xmlOpen = trimmedLine.match(/^<(archived_reference_[a-f0-9]+)\b[^>]*>$/);
    if (xmlOpen) {
      const closeTag = `</${xmlOpen[1]}>`;
      const buf: string[] = [line];
      let j = i + 1;
      let closed = false;
      while (j < rawLines.length) {
        buf.push(rawLines[j]);
        if (rawLines[j].trim() === closeTag) {
          closed = true;
          j += 1;
          break;
        }
        j += 1;
      }
      if (closed) {
        units.push({ kind: 'xml_fence', text: buf.join('\n') });
        i = j;
        continue;
      }
    }

    const legacyOpen = trimmedLine.match(/^<<<(UNTRUSTED_DATA_[A-Z0-9]+)\b/);
    if (legacyOpen) {
      const closeMarker = `<<<END_${legacyOpen[1]}>>>`;
      const buf: string[] = [line];
      let j = i + 1;
      let closed = false;
      while (j < rawLines.length) {
        buf.push(rawLines[j]);
        if (rawLines[j].trim() === closeMarker) {
          closed = true;
          j += 1;
          break;
        }
        j += 1;
      }
      if (closed) {
        units.push({ kind: 'legacy_fence', text: buf.join('\n') });
        i = j;
        continue;
      }
    }

    units.push({ kind: 'plain', text: line });
    i += 1;
  }

  // Expand any oversized unit before packing
  const boundedUnits: string[] = [];
  for (const unit of units) {
    if (unit.text.length <= limit) {
      boundedUnits.push(unit.text);
      continue;
    }

    if (unit.kind === 'xml_fence') {
      const fLines = unit.text.split('\n');
      const openLine = fLines[0].trim();
      const closeLine = fLines[fLines.length - 1].trim();
      const inner = fLines.slice(1, -1).join('\n');
      const segAttrBudget = ' segment="9999/9999"'.length;
      const innerMax = limit - (openLine.length + segAttrBudget + closeLine.length + 2);
      if (innerMax >= 16) {
        const innerSlices = splitIntoLineAwareChunks(inner, innerMax, 16);
        const total = innerSlices.length;
        for (let idx = 0; idx < total; idx++) {
          const segOpen = openLine.replace(/>$/, ` segment="${idx + 1}/${total}">`);
          boundedUnits.push(`${segOpen}\n${innerSlices[idx]}\n${closeLine}`);
        }
        continue;
      }
    } else if (unit.kind === 'legacy_fence') {
      const fLines = unit.text.split('\n');
      const headerCount = fLines.length >= 3 && fLines[1].startsWith('[SECURITY NOTICE:') ? 2 : 1;
      const header = fLines.slice(0, headerCount).join('\n');
      const footer = fLines[fLines.length - 1];
      const inner = fLines.slice(headerCount, -1).join('\n');
      const innerMax = limit - (header.length + footer.length + 2);
      if (innerMax >= 16) {
        const innerSlices = splitIntoLineAwareChunks(inner, innerMax, 16);
        for (const s of innerSlices) {
          boundedUnits.push(`${header}\n${s}\n${footer}`);
        }
        continue;
      }
    }

    boundedUnits.push(...splitIntoLineAwareChunks(unit.text, limit));
  }

  const chunks: string[] = [];
  let cur = '';
  for (const u of boundedUnits) {
    if (u.length > limit) {
      if (cur.length > 0) {
        chunks.push(cur);
        cur = '';
      }
      chunks.push(...splitIntoLineAwareChunks(u, limit));
      continue;
    }
    const candidate = cur.length === 0 ? u : `${cur}\n${u}`;
    if (candidate.length <= limit) {
      cur = candidate;
    } else {
      chunks.push(cur);
      cur = u;
    }
  }
  if (cur.length > 0) {
    chunks.push(cur);
  }
  return chunks.length > 0 ? chunks : [''];
}

function isFencedBlock(block: string): boolean {
  return block.startsWith('<archived_reference_') || block.startsWith('<<<UNTRUSTED_DATA_');
}

function splitFenceAwareBlocks(body: string): string[] {
  const lines = body.split('\n');
  const blocks: string[] = [];
  let activeFenceClose: string | null = null;
  let curLines: string[] = [];

  const flushCur = () => {
    if (curLines.length > 0) {
      const joined = curLines.join('\n');
      if (joined.trim().length > 0) {
        blocks.push(joined);
      }
      curLines = [];
    }
  };

  for (const line of lines) {
    if (activeFenceClose) {
      curLines.push(line);
      if (line.trim() === activeFenceClose) {
        flushCur();
        activeFenceClose = null;
      }
      continue;
    }

    const xmlMatch = line.match(/^<(archived_reference_[a-f0-9]+)\b[^>]*>/);
    if (xmlMatch) {
      flushCur();
      activeFenceClose = `</${xmlMatch[1]}>`;
      curLines.push(line);
      continue;
    }

    const legacyMatch = line.match(/^<<<(UNTRUSTED_DATA_[A-Z0-9]+)\b/);
    if (legacyMatch) {
      flushCur();
      activeFenceClose = `<<<END_${legacyMatch[1]}>>>`;
      curLines.push(line);
      continue;
    }

    if (line.trim() === '') {
      flushCur();
      continue;
    }

    curLines.push(line);
  }
  flushCur();
  return blocks;
}

function trimSingleFenceBlock(
  block: string,
  pointerRef: string,
  keepPerSide = 900,
): string {
  const marker = `\n[... MIDDLE TRIMMED FOR BUDGET; run \`thp peek\` on ${pointerRef} if needed ...]\n`;
  const lines = block.split('\n');
  const isXmlFence =
    lines.length >= 2 &&
    lines[0].startsWith('<archived_reference_') &&
    lines[lines.length - 1].startsWith('</archived_reference_');
  const isLegacyFence =
    lines.length >= 3 &&
    lines[0].startsWith('<<<UNTRUSTED_DATA_') &&
    lines[lines.length - 1].startsWith('<<<END_UNTRUSTED_DATA_');
  if (!isXmlFence && !isLegacyFence) {
    return block;
  }
  const headerLines =
    isLegacyFence && lines[1].startsWith('[SECURITY NOTICE:')
      ? lines.slice(0, 2)
      : lines.slice(0, 1);
  const footerLine = lines[lines.length - 1];
  const inner = lines.slice(headerLines.length, -1).join('\n');
  if (inner.length <= keepPerSide * 2 + 120) {
    return block;
  }
  const headEnd = safeSurrogateSliceEnd(inner, 0, keepPerSide);
  let tailStart = Math.max(headEnd, inner.length - keepPerSide);
  if (tailStart > 0 && tailStart < inner.length) {
    const c = inner.charCodeAt(tailStart);
    if (c >= 0xdc00 && c <= 0xdfff) tailStart += 1;
  }
  return `${headerLines.join('\n')}\n${inner.slice(0, headEnd)}${marker}${inner.slice(tailStart)}\n${footerLine}`;
}

function trimBodySafely(body: string, pointerRef: string): string {
  const marker = `\n[... MIDDLE TRIMMED FOR BUDGET; run \`thp peek\` on ${pointerRef} if needed ...]\n`;
  // Keep each <archived_reference_...> / <<<UNTRUSTED_DATA_...>>> block atomic even across blank lines
  const blocks = splitFenceAwareBlocks(body);
  const fencedCount = blocks.filter(isFencedBlock).length;
  if (blocks.length >= 3 && fencedCount >= 2) {
    const headBlocks: string[] = [blocks[0]];
    const tailBlocks: string[] = [blocks[blocks.length - 1]];
    let headLen = blocks[0].length;
    let tailLen = blocks[blocks.length - 1].length;
    let left = 1;
    let right = blocks.length - 2;
    // Always reserve at least 1 block between [left, right] to drop so fences are never severed
    while (left < right && headLen + blocks[left].length + 2 <= 900) {
      headBlocks.push(blocks[left]);
      headLen += blocks[left].length + 2;
      left += 1;
    }
    while (right > left && tailLen + blocks[right].length + 2 <= 900) {
      tailBlocks.unshift(blocks[right]);
      tailLen += blocks[right].length + 2;
      right -= 1;
    }
    if (left <= right) {
      return `${headBlocks.join('\n\n')}${marker}${tailBlocks.join('\n\n')}`;
    }
  }

  // If body is wrapped in a single outer fence (e.g. packReconcileEnvelope), preserve opening/closing fence lines
  if (blocks.length === 1 && fencedCount === 1) {
    const trimmedSingle = trimSingleFenceBlock(body, pointerRef, 900);
    if (trimmedSingle.length < body.length) {
      return trimmedSingle;
    }
  }

  // For 2-block (or small multi-block) fenced bodies where middle dropping didn't trigger,
  // trim inside each oversized block rather than slicing across fence boundaries.
  if (fencedCount >= 1) {
    const trimmedBlocks = blocks.map((blk) =>
      isFencedBlock(blk) ? trimSingleFenceBlock(blk, pointerRef, 450) : blk,
    );
    const joined = trimmedBlocks.join('\n\n');
    if (joined.length < body.length) {
      return joined;
    }
    return body;
  }

  const headEnd = safeSurrogateSliceEnd(body, 0, 1000);
  let tailStart = Math.max(headEnd, body.length - 1000);
  if (tailStart > 0 && tailStart < body.length) {
    const c = body.charCodeAt(tailStart);
    if (c >= 0xdc00 && c <= 0xdfff) tailStart += 1;
  }
  return `${body.slice(0, headEnd)}${marker}${body.slice(tailStart)}`;
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
            !trimmed.startsWith('<archived_reference_') &&
            !trimmed.startsWith('</archived_reference_') &&
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

  const headerReserve = 420;
  const estimatePartCount = (text: string): number => {
    if (text.length <= budget.per_turn_input_limit) return 1;
    const payloadMax = Math.max(256, budget.per_turn_input_limit - headerReserve);
    return splitIntoFenceAwareChunks(text, payloadMax).length;
  };
  const estimateTotalTokensWithChunkOverhead = (text: string): number => {
    const partCount = estimatePartCount(text);
    const overheadTokens = partCount > 1 ? partCount * 110 : 0;
    return estimateConservativeTokens(text) + overheadTokens;
  };

  let full = renderAll(working);

  // Fair middle-trim pass on verbose priority-3 sections before dropping whole sources
  if (estimateTotalTokensWithChunkOverhead(full) > budget.effective_context_budget) {
    const trimCandidates = working
      .filter((s) => s.priority === 3 && s.body.length > 2400)
      .sort((a, b) => b.body.length - a.body.length);

    for (const sec of trimCandidates) {
      if (estimateTotalTokensWithChunkOverhead(full) <= budget.effective_context_budget) {
        break;
      }
      const trimmedBody = trimBodySafely(sec.body, sec.pointer_ref ?? sec.key);
      if (trimmedBody.length < sec.body.length) {
        sec.body = trimmedBody;
        folded.push(`${sec.key}#trimmed`);
        full = renderAll(working);
      }
    }
  }

  // Deterministic folding from priority 5 down to 3 until within effective_context_budget
  for (const prio of [5, 4, 3] as const) {
    if (estimateTotalTokensWithChunkOverhead(full) <= budget.effective_context_budget) {
      break;
    }
    for (const sec of working) {
      if (sec.priority !== prio) continue;
      if (estimateTotalTokensWithChunkOverhead(full) <= budget.effective_context_budget) break;
      folded.push(sec.key);
      sec.body = `[FOLDED_FOR_BUDGET: pointer=${sec.pointer_ref ?? sec.key}; original_sha256=${sec.content_hash.slice(0, 16)}; run \`thp peek\` or inspect full bundle if needed]`;
      full = renderAll(working);
    }
  }

  const finalEstimatedTokens = estimateTotalTokensWithChunkOverhead(full);
  if (finalEstimatedTokens > budget.effective_context_budget) {
    throw new BudgetExceededError(
      `Core sections exceed target effective_context_budget (${finalEstimatedTokens} > ${budget.effective_context_budget}, confidence=${budget.budget_confidence}). Refusing silent truncation (fail-closed).`,
    );
  }

  const manifestHash = sha256Hex(full);
  const utf8Bytes = Buffer.byteLength(full, 'utf8');

  if (full.length <= budget.per_turn_input_limit) {
    return {
      envelope_id: envelopeId,
      full_markdown: full,
      manifest_hash: manifestHash,
      utf8_bytes: utf8Bytes,
      estimated_tokens: finalEstimatedTokens,
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
  if (budget.per_turn_input_limit < headerReserve + 256) {
    throw new BudgetExceededError(
      `per_turn_input_limit (${budget.per_turn_input_limit}) is too small to carry ThreadPass chunk headers (minimum ${headerReserve + 256}).`,
    );
  }
  const payloadMax = budget.per_turn_input_limit - headerReserve;
  const rawChunks = splitIntoFenceAwareChunks(full, payloadMax);
  const totalParts = rawChunks.length;
  const parts = rawChunks.map((chunk, idx) => {
    const partIndex = idx + 1;
    const isFinal = partIndex === totalParts;
    const partHash = sha256Hex(chunk);
    const wrapped = [
      `[ThreadPass Part ${partIndex}/${totalParts} | env=${envelopeId} | sha256=${partHash}]`,
      isFinal
        ? `[Final part ${partIndex}/${totalParts} | manifest=${manifestHash}] Handoff complete; please follow the Session Handoff Overview.`
        : `[More parts coming: ${partIndex}/${totalParts}] Split for length; please wait for Part ${totalParts}/${totalParts} before starting (reply "Received ${partIndex}/${totalParts}").`,
      `<handoff_chunk part="${partIndex}/${totalParts}">`,
      chunk,
      `</handoff_chunk>`,
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
    estimated_tokens: finalEstimatedTokens,
    parts,
    folded_sections: folded,
    deduplicated_sections: deduplicated,
  };
}
