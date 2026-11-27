import {
  createPrivateKey,
  createPublicKey,
  generateKeyPairSync,
  sign as cryptoSign,
  verify as cryptoVerify,
} from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { canonicalJson, newId } from '../../core/canonical.js';
import type { CommitIntentProof, DeviceKeyStatus, TrustedDeviceKey } from '../../core/types.js';
import { ensureExclusiveAcl } from './acl-and-fs.js';

export interface LocalDeviceIdentity {
  device_id: string;
  public_key_spki_b64: string;
  private_key_pkcs8_b64: string;
  created_at: string;
}

export interface TrustedKeysRegistry {
  schema_version: string;
  keys: TrustedDeviceKey[];
}

export function loadOrCreateDeviceIdentity(
  homeDir: string,
): LocalDeviceIdentity {
  const keysDir = path.join(homeDir, 'device-identity');
  ensureExclusiveAcl(keysDir, 'dir');
  const idFile = path.join(keysDir, 'device-key.json');
  if (fs.existsSync(idFile)) {
    return JSON.parse(fs.readFileSync(idFile, 'utf8')) as LocalDeviceIdentity;
  }
  const { publicKey, privateKey } = generateKeyPairSync('ed25519');
  const pubDer = publicKey.export({ type: 'spki', format: 'der' });
  const privDer = privateKey.export({ type: 'pkcs8', format: 'der' });
  const identity: LocalDeviceIdentity = {
    device_id: newId('dev', 8),
    public_key_spki_b64: Buffer.from(pubDer).toString('base64'),
    private_key_pkcs8_b64: Buffer.from(privDer).toString('base64'),
    created_at: new Date().toISOString(),
  };
  fs.writeFileSync(idFile, JSON.stringify(identity, null, 2), 'utf8');
  ensureExclusiveAcl(idFile, 'file');
  return identity;
}

export function commitIntentSigningPayload(
  proof: Omit<CommitIntentProof, 'signature_b64'>,
): Buffer {
  return Buffer.from(canonicalJson(proof), 'utf8');
}

export function signCommitIntentProof(
  identity: LocalDeviceIdentity,
  fields: Omit<
    CommitIntentProof,
    'proof_type' | 'mode' | 'signer_device_id' | 'public_key_spki_b64' | 'signature_b64'
  >,
  mode: 'ed25519_signed' | 'local_hash_integrity' = 'ed25519_signed',
): CommitIntentProof {
  if (mode === 'local_hash_integrity') {
    return {
      proof_type: 'signed_attestation',
      mode: 'local_hash_integrity',
      ...fields,
      signer_device_id: identity.device_id,
    };
  }

  const base: Omit<CommitIntentProof, 'signature_b64'> = {
    proof_type: 'signed_attestation',
    mode: 'ed25519_signed',
    ...fields,
    signer_device_id: identity.device_id,
    public_key_spki_b64: identity.public_key_spki_b64,
  };
  const privKey = createPrivateKey({
    key: Buffer.from(identity.private_key_pkcs8_b64, 'base64'),
    format: 'der',
    type: 'pkcs8',
  });
  const sig = cryptoSign(null, commitIntentSigningPayload(base), privKey);
  return {
    ...base,
    signature_b64: sig.toString('base64'),
  };
}

export function verifyCommitIntentProofSignature(
  proof: CommitIntentProof,
  trustedKeys: readonly TrustedDeviceKey[],
  txnId: string,
  projectId: string,
  threadId: string,
): {
  valid: boolean;
  reason?: string;
  keyStatus?: DeviceKeyStatus;
} {
  if (proof.mode === 'local_hash_integrity') {
    return {
      valid: false,
      reason: 'local_hash_integrity proof is valid only for single-device local mode, not trusted_device_keys verification',
    };
  }
  if (!proof.signature_b64 || !proof.public_key_spki_b64) {
    return { valid: false, reason: 'Missing Ed25519 signature or public key' };
  }

  const keyEntry = trustedKeys.find(
    (k) =>
      k.device_id === proof.signer_device_id &&
      k.project_id === projectId &&
      (k.thread_id === undefined || k.thread_id === threadId),
  );
  if (!keyEntry) {
    return {
      valid: false,
      reason: `Device ${proof.signer_device_id} is not out-of-band pinned in trusted_device_keys for project ${projectId}`,
    };
  }
  if (keyEntry.public_key_spki_b64 !== proof.public_key_spki_b64) {
    return {
      valid: false,
      reason: `Public key mismatch for device ${proof.signer_device_id}`,
    };
  }

  const { signature_b64, ...unsigned } = proof;
  let sigValid = false;
  try {
    const pubKey = createPublicKey({
      key: Buffer.from(keyEntry.public_key_spki_b64, 'base64'),
      format: 'der',
      type: 'spki',
    });
    sigValid = cryptoVerify(
      null,
      commitIntentSigningPayload(unsigned),
      pubKey,
      Buffer.from(signature_b64, 'base64'),
    );
  } catch {
    sigValid = false;
  }

  if (!sigValid) {
    return {
      valid: false,
      reason: `Invalid Ed25519 signature from device ${proof.signer_device_id}`,
    };
  }

  if (keyEntry.status === 'active') {
    return { valid: true, keyStatus: 'active' };
  }
  if (keyEntry.status === 'retired') {
    if (keyEntry.pinned_txn_ids.includes(txnId)) {
      return { valid: true, keyStatus: 'retired' };
    }
    return {
      valid: false,
      keyStatus: 'retired',
      reason: `Key ${proof.signer_device_id} is retired and txn ${txnId} is not in pinned_txn_ids`,
    };
  }
  // revoked: fail-closed unless explicitly checkpoint-pinned before compromise audit
  if (keyEntry.pinned_txn_ids.includes(txnId)) {
    return { valid: true, keyStatus: 'revoked' };
  }
  return {
    valid: false,
    keyStatus: 'revoked',
    reason: `Key ${proof.signer_device_id} is revoked and txn ${txnId} is not checkpoint-pinned`,
  };
}

export function loadTrustedDeviceKeys(homeDir: string): TrustedKeysRegistry {
  const filePath = path.join(homeDir, 'trusted-device-keys.json');
  if (!fs.existsSync(filePath)) {
    return { schema_version: '1.0.0', keys: [] };
  }
  return JSON.parse(fs.readFileSync(filePath, 'utf8')) as TrustedKeysRegistry;
}

export function saveTrustedDeviceKeys(
  homeDir: string,
  registry: TrustedKeysRegistry,
): void {
  ensureExclusiveAcl(homeDir, 'dir');
  const filePath = path.join(homeDir, 'trusted-device-keys.json');
  fs.writeFileSync(filePath, JSON.stringify(registry, null, 2), 'utf8');
  ensureExclusiveAcl(filePath, 'file');
}

export function pinTrustedDeviceKey(
  homeDir: string,
  entry: Omit<TrustedDeviceKey, 'pinned_at' | 'pinned_txn_ids'> & {
    pinned_txn_ids?: string[];
  },
): TrustedDeviceKey {
  const reg = loadTrustedDeviceKeys(homeDir);
  const existing = reg.keys.find(
    (k) =>
      k.device_id === entry.device_id &&
      k.project_id === entry.project_id &&
      k.thread_id === entry.thread_id,
  );
  if (existing) {
    existing.public_key_spki_b64 = entry.public_key_spki_b64;
    existing.status = entry.status;
    existing.note = entry.note ?? existing.note;
    if (entry.pinned_txn_ids) {
      existing.pinned_txn_ids = [
        ...new Set([...existing.pinned_txn_ids, ...entry.pinned_txn_ids]),
      ];
    }
    saveTrustedDeviceKeys(homeDir, reg);
    return existing;
  }
  const created: TrustedDeviceKey = {
    ...entry,
    pinned_at: new Date().toISOString(),
    pinned_txn_ids: entry.pinned_txn_ids ?? [],
  };
  reg.keys.push(created);
  saveTrustedDeviceKeys(homeDir, reg);
  return created;
}

export function updateTrustedDeviceKeyStatus(
  homeDir: string,
  deviceId: string,
  projectId: string,
  status: DeviceKeyStatus,
  addPinnedTxnIds: readonly string[] = [],
): TrustedDeviceKey {
  const reg = loadTrustedDeviceKeys(homeDir);
  const found = reg.keys.find(
    (k) => k.device_id === deviceId && k.project_id === projectId,
  );
  if (!found) {
    throw new Error(
      `Trusted device key ${deviceId} not found for project ${projectId}`,
    );
  }
  found.status = status;
  if (addPinnedTxnIds.length > 0) {
    found.pinned_txn_ids = [
      ...new Set([...found.pinned_txn_ids, ...addPinnedTxnIds]),
    ];
  }
  saveTrustedDeviceKeys(homeDir, reg);
  return found;
}
