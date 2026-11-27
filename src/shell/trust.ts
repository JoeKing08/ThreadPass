import { createHmac } from 'node:crypto';
import { canonicalJson, newHighEntropyToken, sha256Hex } from '../core/canonical.js';
import type {
  ConfirmationProof,
  ConfirmationPurpose,
  ProvenanceChain,
  TrustTier,
} from '../core/types.js';

export const TRUST_ADAPTER_VERSION = 'threadpass-shell/0.1.0';

/**
 * Computes TrustTier purely from verified provenance metadata.
 * Self-claimed fields inside pasted text or model outputs NEVER grant `human_authority`.
 */
export function computeTrustTier(
  provenance: ProvenanceChain,
  expectedExactContentSha256?: string,
): TrustTier {
  const { verification_method, confirmation_proof } = provenance;

  if (verification_method === 'local_cli_direct') {
    return 'human_authority';
  }

  if (verification_method === 'native_structured_role') {
    return 'human_authority';
  }

  if (verification_method === 'unverified_text_import') {
    if (
      confirmation_proof &&
      confirmation_proof.purpose === 'authorize_instruction' &&
      expectedExactContentSha256 &&
      verifyConfirmationProof(
        confirmation_proof,
        expectedExactContentSha256,
        'authorize_instruction',
      )
    ) {
      return 'human_authority';
    }
    return 'unverified_import';
  }

  if (verification_method === 'model_reported') {
    if (
      confirmation_proof &&
      confirmation_proof.purpose === 'authorize_instruction' &&
      expectedExactContentSha256 &&
      verifyConfirmationProof(
        confirmation_proof,
        expectedExactContentSha256,
        'authorize_instruction',
      )
    ) {
      return 'human_authority';
    }
    return 'untrusted_peer';
  }

  return 'untrusted_unknown';
}

export function normalizeConfirmationText(text: string): string {
  return text.replace(/\r\n/g, '\n').trim();
}

export function issueConfirmationProof(
  contentText: string,
  purpose: ConfirmationPurpose,
  channel: 'local_cli' | 'thp_web',
  hmacSecret?: string,
): ConfirmationProof {
  const contentSha = sha256Hex(normalizeConfirmationText(contentText));
  const proof: ConfirmationProof = {
    nonce: newHighEntropyToken(16),
    content_sha256: contentSha,
    purpose,
    channel,
    confirmed_at: new Date().toISOString(),
  };
  if (hmacSecret) {
    proof.hmac_hex = createHmac('sha256', hmacSecret)
      .update(canonicalJson(proof))
      .digest('hex');
  }
  return proof;
}

export function verifyConfirmationProof(
  proof: ConfirmationProof,
  expectedContentSha256: string,
  requiredPurpose: ConfirmationPurpose,
  hmacSecret?: string,
): boolean {
  if (proof.purpose !== requiredPurpose) {
    return false;
  }
  if (proof.content_sha256 !== expectedContentSha256) {
    return false;
  }
  if (!proof.nonce || proof.nonce.length < 16) {
    return false;
  }
  if (hmacSecret) {
    const { hmac_hex, ...rest } = proof;
    const expected = createHmac('sha256', hmacSecret)
      .update(canonicalJson(rest))
      .digest('hex');
    return hmac_hex === expected;
  }
  return true;
}

/**
 * Maps internal TrustTier codes to gentle, descriptive origin labels for model prompts.
 */
export function formatOriginLabel(tier: TrustTier): string {
  switch (tier) {
    case 'human_authority':
      return 'user_confirmed';
    case 'unverified_import':
      return 'imported_transcript';
    case 'untrusted_peer':
      return 'prior_agent_or_peer';
    case 'untrusted_observation':
      return 'tool_output';
    case 'untrusted_unknown':
    default:
      return 'unclassified_extension';
  }
}

export function sanitizeXmlAttr(val: string): string {
  return val.replace(/["'<>\r\n]/g, '_');
}

/**
 * Safely encloses non-`human_authority` text inside explicit XML archived-reference blocks.
 * Escapes closing XML tags and legacy fence markers so quoted content cannot break out.
 */
export function renderNonInstructionFence(params: {
  sourceLabel: string;
  trustTier: TrustTier;
  anchorOrRef: string;
  bodyText: string;
}): string {
  const tagName = `archived_reference_${sha256Hex(params.anchorOrRef).slice(0, 8)}`;
  const neutralized = params.bodyText
    .replace(
      /<\s*\/?\s*(?:archived_reference(?:_[a-z0-9]+)?|handoff_chunk|system|instructions?)\b[^>]*>/gi,
      '[escaped_tag]',
    )
    .replace(
      /<<<\s*\/?\s*(?:UNTRUSTED_DATA|END_UNTRUSTED|SYSTEM|INSTRUCTION|NON_EXECUTABLE_PART_PAYLOAD|END_PART_PAYLOAD)[^>]*>>>/gi,
      '[escaped_marker]',
    );
  return [
    `<${tagName} source="${sanitizeXmlAttr(params.sourceLabel)}" origin="${formatOriginLabel(params.trustTier)}" ref="${sanitizeXmlAttr(params.anchorOrRef)}">`,
    neutralized,
    `</${tagName}>`,
  ].join('\n');
}
