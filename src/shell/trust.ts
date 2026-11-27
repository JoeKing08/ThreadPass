import {
  createHmac,
  createPrivateKey,
  createPublicKey,
  sign as cryptoSign,
  timingSafeEqual,
  verify as cryptoVerify,
} from 'node:crypto';
import { canonicalJson, newHighEntropyToken, sha256Hex } from '../core/canonical.js';
import type {
  ConfirmationProof,
  ConfirmationPurpose,
  ProvenanceChain,
  TrustTier,
  TrustedDeviceKey,
} from '../core/types.js';
import type { LocalDeviceIdentity } from '../profile/local-offline/crypto-keys.js';

export const TRUST_ADAPTER_VERSION = 'threadpass-shell/0.1.0';

export type ReferenceOriginSummary = TrustTier | 'mixed_origin';

export type ConfirmationCredential =
  | { identity: LocalDeviceIdentity }
  | { hmacSecret: string };

export interface ConfirmationVerifier {
  trustedKeys?: readonly TrustedDeviceKey[];
  projectId?: string;
  threadId?: string;
  localIdentity?: LocalDeviceIdentity;
  hmacSecret?: string;
}

/**
 * Computes TrustTier purely from verified provenance metadata and cryptographic ConfirmationProof.
 * Self-claimed fields inside pasted text or unsigned proofs NEVER grant `human_authority`.
 */
export function computeTrustTier(
  provenance: ProvenanceChain,
  expectedExactContentSha256?: string,
  verifier?: ConfirmationVerifier | string,
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
        verifier,
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
        verifier,
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
  credential: ConfirmationCredential | string,
): ConfirmationProof {
  const cred: ConfirmationCredential | undefined =
    typeof credential === 'string'
      ? credential.trim().length > 0
        ? { hmacSecret: credential }
        : undefined
      : credential;
  if (!cred) {
    throw new Error(
      'issueConfirmationProof requires a local device Ed25519 identity or non-empty HMAC secret',
    );
  }
  if ('hmacSecret' in cred && (!cred.hmacSecret || cred.hmacSecret.trim().length === 0)) {
    throw new Error('issueConfirmationProof requires a non-empty HMAC secret');
  }

  const contentSha = sha256Hex(normalizeConfirmationText(contentText));
  const baseProof: ConfirmationProof = {
    nonce: newHighEntropyToken(16),
    content_sha256: contentSha,
    purpose,
    channel,
    confirmed_at: new Date().toISOString(),
  };

  if ('identity' in cred) {
    const unsigned: Omit<ConfirmationProof, 'signature_b64'> = {
      ...baseProof,
      signer_device_id: cred.identity.device_id,
      public_key_spki_b64: cred.identity.public_key_spki_b64,
    };
    const privKey = createPrivateKey({
      key: Buffer.from(cred.identity.private_key_pkcs8_b64, 'base64'),
      format: 'der',
      type: 'pkcs8',
    });
    const sig = cryptoSign(null, Buffer.from(canonicalJson(unsigned), 'utf8'), privKey);
    return {
      ...unsigned,
      signature_b64: sig.toString('base64'),
    };
  }

  return {
    ...baseProof,
    hmac_hex: createHmac('sha256', cred.hmacSecret)
      .update(canonicalJson(baseProof))
      .digest('hex'),
  };
}

export function verifyConfirmationProof(
  proof: ConfirmationProof,
  expectedContentSha256: string,
  requiredPurpose: ConfirmationPurpose,
  verifier?: ConfirmationVerifier | string,
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

  const v: ConfirmationVerifier =
    typeof verifier === 'string' ? { hmacSecret: verifier } : verifier ?? {};

  // 1. Ed25519 device signature verification
  if (proof.signature_b64 && proof.public_key_spki_b64 && proof.signer_device_id) {
    let trustedMatch = false;
    if (
      v.localIdentity &&
      v.localIdentity.device_id === proof.signer_device_id &&
      v.localIdentity.public_key_spki_b64 === proof.public_key_spki_b64
    ) {
      trustedMatch = true;
    } else if (v.trustedKeys) {
      trustedMatch = v.trustedKeys.some(
        (k) =>
          k.device_id === proof.signer_device_id &&
          k.public_key_spki_b64 === proof.public_key_spki_b64 &&
          k.status === 'active' &&
          (!v.projectId || k.project_id === v.projectId) &&
          (k.thread_id === undefined || !v.threadId || k.thread_id === v.threadId),
      );
    }
    if (!trustedMatch) {
      return false;
    }

    const { signature_b64, ...unsigned } = proof;
    try {
      const pubKey = createPublicKey({
        key: Buffer.from(proof.public_key_spki_b64, 'base64'),
        format: 'der',
        type: 'spki',
      });
      return cryptoVerify(
        null,
        Buffer.from(canonicalJson(unsigned), 'utf8'),
        pubKey,
        Buffer.from(signature_b64, 'base64'),
      );
    } catch {
      return false;
    }
  }

  // 2. HMAC verification (requires explicit shared secret)
  if (proof.hmac_hex && v.hmacSecret && v.hmacSecret.length > 0) {
    const { hmac_hex, ...rest } = proof;
    const expectedHex = createHmac('sha256', v.hmacSecret)
      .update(canonicalJson(rest))
      .digest('hex');
    const a = Buffer.from(hmac_hex, 'hex');
    const b = Buffer.from(expectedHex, 'hex');
    if (a.length === 0 || a.length !== b.length) return false;
    return timingSafeEqual(a, b);
  }

  // Unsigned proofs are never accepted
  return false;
}

/**
 * Maps internal TrustTier / mixed aggregate codes to gentle, descriptive origin labels for model prompts.
 */
export function formatOriginLabel(tier: ReferenceOriginSummary): string {
  switch (tier) {
    case 'human_authority':
      return 'user_confirmed';
    case 'unverified_import':
      return 'imported_transcript';
    case 'untrusted_peer':
      return 'prior_agent_or_peer';
    case 'untrusted_observation':
      return 'tool_output';
    case 'mixed_origin':
      return 'mixed_origin';
    case 'untrusted_unknown':
    default:
      return 'unclassified_extension';
  }
}

export function sanitizeXmlAttr(val: string, maxEscapedLen = 60): string {
  const normalized = val.replace(/[\r\n]+/g, ' ');
  let out = '';
  for (const ch of normalized) {
    let esc: string;
    switch (ch) {
      case '&':
        esc = '&amp;';
        break;
      case '<':
        esc = '&lt;';
        break;
      case '>':
        esc = '&gt;';
        break;
      case '"':
        esc = '&quot;';
        break;
      case "'":
        esc = '&apos;';
        break;
      default:
        esc = ch;
    }
    if (out.length + esc.length > maxEscapedLen) break;
    out += esc;
  }
  return out;
}

/**
 * Safely encloses non-`human_authority` text inside explicit XML archived-reference blocks.
 * Escapes closing XML tags and legacy fence markers so quoted content cannot break out.
 */
export function renderNonInstructionFence(params: {
  sourceLabel: string;
  trustTier: ReferenceOriginSummary;
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
    `<${tagName} source="${sanitizeXmlAttr(params.sourceLabel, 50)}" origin="${formatOriginLabel(params.trustTier)}" ref="${sanitizeXmlAttr(params.anchorOrRef, 48)}">`,
    neutralized,
    `</${tagName}>`,
  ].join('\n');
}
