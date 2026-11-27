import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { sha256Hex } from '../core/canonical.js';
import { SCHEMA_VERSION, type WorkspaceManifest } from '../core/types.js';

/**
 * Collects a three-state `Workspace State Manifest` (`git_repo | plain_dir | none`).
 * Git is strictly an optional enhancement; plain directories and pure conversation
 * workflows (`none`) run completely without Git.
 */
export function captureWorkspaceManifest(
  workspaceDir?: string,
  explicitWorkspaceId?: string,
): WorkspaceManifest {
  if (!workspaceDir) {
    return {
      schema_version: SCHEMA_VERSION,
      workspace_id: explicitWorkspaceId ?? 'ws_none',
      workspace_kind: 'none',
      vcs: 'none',
      dirty_files: [],
      untracked_files: [],
      status_hash: sha256Hex('ws_none'),
      completeness: 'complete',
      blind_spots: [],
    };
  }

  const resolved = path.resolve(workspaceDir);
  if (!fs.existsSync(resolved)) {
    return {
      schema_version: SCHEMA_VERSION,
      workspace_id:
        explicitWorkspaceId ?? `ws_${sha256Hex(resolved).slice(0, 12)}`,
      workspace_kind: 'none',
      vcs: 'none',
      root_hint: resolved,
      dirty_files: [],
      untracked_files: [],
      status_hash: sha256Hex(`missing:${resolved}`),
      completeness: 'partial',
      blind_spots: [`Workspace directory not accessible on this device: ${resolved}`],
    };
  }

  // Try optional Git probe first
  let gitRoot: string | undefined;
  try {
    gitRoot = execFileSync('git', ['-C', resolved, 'rev-parse', '--show-toplevel'], {
      encoding: 'utf8',
      stdio: 'pipe',
      windowsHide: true,
    }).trim();
  } catch {
    gitRoot = undefined;
  }

  if (gitRoot) {
    const branch = safeGit(resolved, ['rev-parse', '--abbrev-ref', 'HEAD']) ?? 'DETACHED';
    const headCommit = safeGit(resolved, ['rev-parse', 'HEAD']) ?? 'UNBORN';
    const remoteUrl = safeGit(resolved, ['remote', 'get-url', 'origin']);
    const statusRaw =
      safeGit(resolved, ['status', '--porcelain=v1']) ?? '';
    const dirtyFiles: string[] = [];
    const untrackedFiles: string[] = [];
    for (const line of statusRaw.split(/\r?\n/)) {
      if (!line.trim()) continue;
      if (line.startsWith('?? ')) {
        untrackedFiles.push(line.slice(3).trim());
      } else {
        dirtyFiles.push(line.slice(3).trim());
      }
    }
    const statusHash = sha256Hex(
      `${gitRoot}|${headCommit}|${dirtyFiles.join(',')}|${untrackedFiles.join(',')}`,
    );
    return {
      schema_version: SCHEMA_VERSION,
      workspace_id:
        explicitWorkspaceId ?? `ws_${sha256Hex(gitRoot).slice(0, 12)}`,
      workspace_kind: 'git_repo',
      vcs: 'git',
      root_hint: gitRoot,
      remote_url: remoteUrl,
      branch,
      head_commit: headCommit,
      dirty_files: dirtyFiles,
      untracked_files: untrackedFiles,
      status_hash: statusHash,
      completeness: 'partial',
      blind_spots: [
        'gitignored files, nested submodules, and external build caches are not hashed into workspace_manifest',
      ],
    };
  }

  // Plain directory fallback (`plain_dir`, vcs = 'none')
  const files: string[] = [];
  const walk = (dir: string, depth: number) => {
    if (depth > 2 || files.length > 200) return;
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === '.git' || entry.name === 'node_modules' || entry.name === 'dist') {
        continue;
      }
      const full = path.join(dir, entry.name);
      const rel = path.relative(resolved, full).replace(/\\/g, '/');
      if (entry.isDirectory()) {
        walk(full, depth + 1);
      } else {
        const st = fs.statSync(full);
        files.push(`${rel}:${st.size}`);
      }
    }
  };
  walk(resolved, 0);

  return {
    schema_version: SCHEMA_VERSION,
    workspace_id:
      explicitWorkspaceId ?? `ws_${sha256Hex(resolved).slice(0, 12)}`,
    workspace_kind: 'plain_dir',
    vcs: 'none',
    root_hint: resolved,
    dirty_files: files,
    untracked_files: [],
    status_hash: sha256Hex(files.join('\n')),
    completeness: 'partial',
    blind_spots: [
      'plain_dir scan capped at depth=2 / 200 entries; excludes node_modules and dist',
    ],
  };
}

function safeGit(cwd: string, args: string[]): string | undefined {
  try {
    return execFileSync('git', ['-C', cwd, ...args], {
      encoding: 'utf8',
      stdio: 'pipe',
      windowsHide: true,
    }).trim();
  } catch {
    return undefined;
  }
}
