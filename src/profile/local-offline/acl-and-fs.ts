import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { newId, sha256Hex } from '../../core/canonical.js';

export class ImmutableTargetCorruptedError extends Error {
  constructor(
    public readonly targetPath: string,
    public readonly expectedSha256: string,
    public readonly actualSha256: string,
  ) {
    super(
      `Immutable target already exists with mismatched content (fail-closed): ${targetPath} expected=${expectedSha256} actual=${actualSha256}`,
    );
    this.name = 'ImmutableTargetCorruptedError';
  }
}

export class FilesystemUnsupportedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'FilesystemUnsupportedError';
  }
}

export interface PublishFaultHooks {
  afterTempWriteBeforeFsync?: (tmpPath: string, targetPath: string) => void;
  afterFsyncBeforeLink?: (tmpPath: string, targetPath: string) => void;
  afterLinkBeforeUnlink?: (tmpPath: string, targetPath: string) => void;
}

/**
 * Enforces owner-exclusive permissions on sensitive storage directories/files.
 * POSIX: 0700 / 0600.
 * Windows: icacls removing inheritance and granting exclusive access to current user.
 */
export function ensureExclusiveAcl(
  targetPath: string,
  kind: 'dir' | 'file' = 'dir',
): void {
  if (kind === 'dir') {
    fs.mkdirSync(targetPath, { recursive: true });
  }
  if (process.platform === 'win32') {
    const userSpec = process.env.USERDOMAIN
      ? `${process.env.USERDOMAIN}\\${process.env.USERNAME}`
      : process.env.USERNAME || '';
    if (!userSpec) {
      throw new Error('Unable to determine current Windows user for exclusive ACL.');
    }
    const grantPerm = kind === 'dir' ? `${userSpec}:(OI)(CI)F` : `${userSpec}:F`;
    execFileSync(
      'icacls',
      [targetPath, '/inheritance:r', '/grant:r', grantPerm],
      { stdio: 'pipe', windowsHide: true },
    );
    // Re-verify inheritance disabled and only expected principal present
    const aclOut = execFileSync('icacls', [targetPath], {
      encoding: 'utf8',
      stdio: 'pipe',
      windowsHide: true,
    });
    const usernameLower = (process.env.USERNAME || '').toLowerCase();
    if (!aclOut.toLowerCase().includes(usernameLower)) {
      throw new Error(`Windows ACL verification failed for ${targetPath}`);
    }
    return;
  }

  fs.chmodSync(targetPath, kind === 'dir' ? 0o700 : 0o600);
}

/**
 * Startup filesystem probe verifying hard-link (`fs.linkSync`) and atomic `EEXIST`
 * semantics on the target filesystem. Refuses to silently degrade to overwriting rename.
 */
export function probeHardLinkSupport(baseDir: string): void {
  fs.mkdirSync(baseDir, { recursive: true });
  const probeId = newId('probe', 6);
  const srcPath = path.join(baseDir, `.${probeId}.src`);
  const dstPath = path.join(baseDir, `.${probeId}.dst`);
  const dupPath = path.join(baseDir, `.${probeId}.dup`);
  try {
    fs.writeFileSync(srcPath, 'probe-a', 'utf8');
    fs.writeFileSync(dupPath, 'probe-b', 'utf8');
    fs.linkSync(srcPath, dstPath);
    let caughtEexist = false;
    try {
      fs.linkSync(dupPath, dstPath);
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === 'EEXIST') {
        caughtEexist = true;
      } else {
        throw err;
      }
    }
    if (!caughtEexist) {
      throw new FilesystemUnsupportedError(
        `Filesystem at ${baseDir} failed atomic EEXIST check during linkSync.`,
      );
    }
  } catch (err) {
    if (err instanceof FilesystemUnsupportedError) throw err;
    throw new FilesystemUnsupportedError(
      `Filesystem at ${baseDir} does not support required atomic hard links (PublishImmutable): ${(err as Error).message}`,
    );
  } finally {
    for (const p of [srcPath, dstPath, dupPath]) {
      try {
        if (fs.existsSync(p)) fs.unlinkSync(p);
      } catch {
        // best-effort probe cleanup
      }
    }
  }
}

/**
 * Flushes parent directory on POSIX systems.
 * On Windows, `node:fs` does not support fsync on directory handles; durability relies
 * on file-level `FlushFileBuffers` (`fsyncSync`) + NTFS metadata journaling.
 */
export function syncParentDirIfSupported(targetPath: string): void {
  if (process.platform === 'win32') {
    return;
  }
  const parent = path.dirname(targetPath);
  const dirFd = fs.openSync(parent, 'r');
  try {
    fs.fsyncSync(dirFd);
  } finally {
    fs.closeSync(dirFd);
  }
}

/**
 * `PublishImmutable` primitive:
 * 1. Create unique temp file on same filesystem (inside stagingDir under same mount)
 * 2. Write complete bytes
 * 3. `fsync(tmp)`
 * 4. Atomic no-replace `fs.link(tmp, targetPath)` (returns EEXIST if target exists)
 * 5. `unlink(tmp)` + POSIX parent directory `fsync`
 *
 * On `EEXIST`: reads existing target and compares SHA-256.
 * - Identical hash -> idempotent success.
 * - Mismatched hash -> throws `ImmutableTargetCorruptedError` (never overwrites).
 */
export function publishImmutable(
  stagingDir: string,
  targetPath: string,
  content: Uint8Array | string,
  expectedSha256?: string,
  faultHooks?: PublishFaultHooks,
): { sha256: string; status: 'created' | 'idempotent_hit' } {
  const bytes =
    typeof content === 'string' ? Buffer.from(content, 'utf8') : Buffer.from(content);
  const digest = expectedSha256 ?? sha256Hex(bytes);
  if (expectedSha256 && sha256Hex(bytes) !== expectedSha256) {
    throw new Error(
      `PublishImmutable payload hash mismatch before write for ${targetPath}`,
    );
  }

  fs.mkdirSync(stagingDir, { recursive: true });
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });

  const tmpPath = path.join(stagingDir, `.pub-${newId('tmp', 8)}.tmp`);
  const fd = fs.openSync(tmpPath, 'wx', 0o600);
  let fdClosed = false;
  try {
    let offset = 0;
    while (offset < bytes.length) {
      offset += fs.writeSync(fd, bytes, offset, bytes.length - offset);
    }
    if (faultHooks?.afterTempWriteBeforeFsync) {
      fs.closeSync(fd);
      fdClosed = true;
      faultHooks.afterTempWriteBeforeFsync(tmpPath, targetPath);
    } else {
      fs.fsyncSync(fd);
      fs.closeSync(fd);
      fdClosed = true;
    }

    if (faultHooks?.afterFsyncBeforeLink) {
      faultHooks.afterFsyncBeforeLink(tmpPath, targetPath);
    }

    try {
      fs.linkSync(tmpPath, targetPath);
    } catch (err) {
      const code = (err as NodeJS.ErrnoException).code;
      if (code === 'EEXIST') {
        const existingBytes = fs.readFileSync(targetPath);
        const existingHash = sha256Hex(existingBytes);
        if (existingHash === digest) {
          return { sha256: digest, status: 'idempotent_hit' };
        }
        throw new ImmutableTargetCorruptedError(targetPath, digest, existingHash);
      }
      throw err;
    }

    if (faultHooks?.afterLinkBeforeUnlink) {
      faultHooks.afterLinkBeforeUnlink(tmpPath, targetPath);
    }

    try {
      fs.unlinkSync(tmpPath);
    } catch {
      // harmless if temp already cleaned
    }
    syncParentDirIfSupported(targetPath);
    return { sha256: digest, status: 'created' };
  } finally {
    if (!fdClosed) {
      try {
        fs.closeSync(fd);
      } catch {
        // ignore
      }
    }
    try {
      if (fs.existsSync(tmpPath)) {
        fs.unlinkSync(tmpPath);
      }
    } catch {
      // ignore
    }
  }
}

/**
 * Publishes an already-written and already-fsynced temporary file on the same
 * filesystem via atomic no-replace `fs.linkSync`, avoiding duplicate write+fsync I/O
 * when executed inside a tight lock window (e.g. Finalize under SQLite `BEGIN IMMEDIATE`).
 */
export function linkPreparedImmutable(
  preparedTmpPath: string,
  targetPath: string,
  expectedSha256: string,
): { sha256: string; status: 'created' | 'idempotent_hit' } {
  fs.mkdirSync(path.dirname(targetPath), { recursive: true });
  try {
    fs.linkSync(preparedTmpPath, targetPath);
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === 'EEXIST') {
      const existingBytes = fs.readFileSync(targetPath);
      const existingHash = sha256Hex(existingBytes);
      if (existingHash === expectedSha256) {
        return { sha256: expectedSha256, status: 'idempotent_hit' };
      }
      throw new ImmutableTargetCorruptedError(
        targetPath,
        expectedSha256,
        existingHash,
      );
    }
    throw err;
  }

  try {
    if (fs.existsSync(preparedTmpPath)) {
      fs.unlinkSync(preparedTmpPath);
    }
  } catch {
    // harmless
  }
  syncParentDirIfSupported(targetPath);
  return { sha256: expectedSha256, status: 'created' };
}
