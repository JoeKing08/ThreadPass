import type { RedactionHook, RedactionResult } from '../../core/ports.js';
import type { BlobManifestEntry } from '../../core/types.js';

interface SecretPattern {
  name: string;
  regex: RegExp;
}

const BUILTIN_PATTERNS: SecretPattern[] = [
  { name: 'github_pat', regex: /github_pat_[A-Za-z0-9_]{20,}/g },
  { name: 'github_classic_token', regex: /\bgh[pousr]_[A-Za-z0-9]{20,}\b/g },
  { name: 'openai_like_key', regex: /\bsk-(?:proj-|ant-|live-|test-)?[A-Za-z0-9_-]{16,}\b/g },
  { name: 'aws_access_key', regex: /\bAKIA[0-9A-Z]{16}\b/g },
  { name: 'google_api_key', regex: /\bAIza[0-9A-Za-z_-]{35}\b/g },
  {
    name: 'pem_private_key',
    regex: /-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----/g,
  },
  { name: 'bearer_token', regex: /\bBearer\s+[A-Za-z0-9._-]{16,}\b/gi },
  {
    name: 'kv_secret',
    regex: /\b(password|passwd|api_key|apikey|secret_key|access_token|auth_token)\s*[:=]\s*(['"]?)[A-Za-z0-9._~+/-]{8,}\2/gi,
  },
];

export class SlidingWindowRedactor implements RedactionHook {
  private readonly customPatterns: SecretPattern[] = [];

  constructor(customLiteralsOrPatterns: readonly string[] = []) {
    for (let i = 0; i < customLiteralsOrPatterns.length; i++) {
      const val = customLiteralsOrPatterns[i];
      if (!val) continue;
      const escaped = val.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      this.customPatterns.push({
        name: `custom_secret_${i}`,
        regex: new RegExp(escaped, 'g'),
      });
    }
  }

  private getAllPatterns(): SecretPattern[] {
    return [...BUILTIN_PATTERNS, ...this.customPatterns];
  }

  /**
   * Overlapping sliding-window streaming sanitization across chunks so secrets
   * spanning buffer boundaries or multi-line PEM blocks cannot slip through.
   */
  sanitizeStreamChunks(
    chunks: readonly string[],
    chunkWindowOverlap = 256,
  ): RedactionResult {
    if (chunks.length === 0) {
      return { sanitized: '', redactedCount: 0, matchedRuleNames: [] };
    }
    const overlapSize = Math.max(chunkWindowOverlap, 256);
    let buffer = '';
    const emitted: string[] = [];
    let totalRedacted = 0;
    const ruleSet = new Set<string>();

    for (const chunk of chunks) {
      buffer += chunk;
      const firstPass = this.applyPatterns(buffer);
      buffer = firstPass.text;
      totalRedacted += firstPass.count;
      for (const r of firstPass.rules) ruleSet.add(r);

      if (buffer.length > overlapSize * 2) {
        // Hold buffer flush if an unclosed PEM private key header is present (up to 64KB bound)
        const lastBegin = buffer.lastIndexOf('-----BEGIN ');
        const lastEnd = buffer.lastIndexOf('-----END ');
        const hasOpenPem = lastBegin !== -1 && (lastEnd === -1 || lastEnd < lastBegin);
        if (!hasOpenPem || buffer.length > 65536) {
          const flushLen = buffer.length - overlapSize;
          emitted.push(buffer.slice(0, flushLen));
          buffer = buffer.slice(flushLen);
        }
      }
    }
    if (buffer.length > 0) {
      const tailPass = this.applyPatterns(buffer);
      emitted.push(tailPass.text);
      totalRedacted += tailPass.count;
      for (const r of tailPass.rules) ruleSet.add(r);
    }

    const joined = emitted.join('');
    // Secondary verification pass across seam boundaries
    const verifyPass = this.applyPatterns(joined);
    totalRedacted += verifyPass.count;
    for (const r of verifyPass.rules) ruleSet.add(r);

    return {
      sanitized: verifyPass.text,
      redactedCount: totalRedacted,
      matchedRuleNames: [...ruleSet],
    };
  }

  sanitizeText(text: string): RedactionResult {
    // Split into bounded chunks to exercise the exact same cross-chunk sliding window
    const chunkSize = 1024;
    const chunks: string[] = [];
    for (let i = 0; i < text.length; i += chunkSize) {
      chunks.push(text.slice(i, i + chunkSize));
    }
    return this.sanitizeStreamChunks(chunks, 256);
  }

  scanForSecrets(text: string): string[] {
    const matched: string[] = [];
    for (const pat of this.getAllPatterns()) {
      const re = new RegExp(pat.regex.source, pat.regex.flags);
      if (re.test(text)) {
        matched.push(pat.name);
      }
    }
    return matched;
  }

  private applyPatterns(input: string): {
    text: string;
    count: number;
    rules: string[];
  } {
    let text = input;
    let count = 0;
    const rules: string[] = [];
    for (const pat of this.getAllPatterns()) {
      const re = new RegExp(pat.regex.source, pat.regex.flags);
      text = text.replace(re, (match) => {
        count += 1;
        if (!rules.includes(pat.name)) rules.push(pat.name);
        if (pat.name === 'kv_secret') {
          const eqIdx = match.search(/[:=]/);
          if (eqIdx > 0) {
            return `${match.slice(0, eqIdx + 1)} [REDACTED:${pat.name}]`;
          }
        }
        return `[REDACTED:${pat.name}]`;
      });
    }
    return { text, count, rules };
  }
}

/**
 * Checks whether raw bytes look like opaque binary content rather than UTF-8 text.
 */
export function isOpaqueBinary(bytes: Uint8Array): boolean {
  const sampleLen = Math.min(bytes.length, 4096);
  if (sampleLen === 0) return false;
  let nonText = 0;
  for (let i = 0; i < sampleLen; i++) {
    const b = bytes[i];
    if (b === 0) return true;
    if (b < 0x09 || (b > 0x0d && b < 0x20)) {
      nonText += 1;
    }
  }
  return nonText / sampleLen > 0.15;
}

export class EgressBlockedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EgressBlockedError';
  }
}

/**
 * Outbound Egress Gate: runs latest secret patterns over outbound text/envelopes
 * and blocks non-approved Blob references (`local_only` / `unscannable_binary`).
 */
export function assertOutboundEgressSafe(
  redactor: RedactionHook,
  outboundText: string,
  referencedBlobs: readonly BlobManifestEntry[] = [],
  options: { allowRedactedOutput?: boolean } = {},
): string {
  const hits = redactor.scanForSecrets(outboundText);
  if (hits.length > 0) {
    if (options.allowRedactedOutput) {
      return redactor.sanitizeText(outboundText).sanitized;
    }
    throw new EgressBlockedError(
      `Outbound Egress Gate blocked secret leakage (${hits.join(', ')})`,
    );
  }
  for (const b of referencedBlobs) {
    if (b.sync_policy !== 'approved') {
      throw new EgressBlockedError(
        `Outbound Egress Gate blocked blob ${b.sha256} with sync_policy=${b.sync_policy}`,
      );
    }
  }
  return outboundText;
}
