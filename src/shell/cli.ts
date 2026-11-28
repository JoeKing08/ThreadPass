#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import { createInterface } from 'node:readline/promises';
import { sha256Hex } from '../core/canonical.js';
import { ThreadPassService, type TranscriptInputOptions } from './service.js';
import { normalizeConfirmationText } from './trust.js';
import { startThreadPassWeb } from './web.js';

interface ParsedArgs {
  positional: string[];
  flags: Record<string, string | boolean>;
  files: string[];
}

const BOOLEAN_FLAGS = new Set([
  'help',
  'authorize',
  'foreign-canonical',
  'attachment',
  'require-disaster-redundant',
  'disaster-verified',
  'act-as-main',
  'rebuild',
  'can-redelegate',
]);

function parseCliArgs(argv: string[]): ParsedArgs {
  const positional: string[] = [];
  const flags: Record<string, string | boolean> = {};
  const files: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg.startsWith('--')) {
      const eq = arg.indexOf('=');
      if (eq > 2) {
        const key = arg.slice(2, eq);
        const val = arg.slice(eq + 1);
        flags[key] = val;
        if (key === 'file') files.push(val);
      } else {
        const key = arg.slice(2);
        const next = argv[i + 1];
        if (!BOOLEAN_FLAGS.has(key) && next !== undefined && !next.startsWith('--')) {
          flags[key] = next;
          if (key === 'file') files.push(next);
          i += 1;
        } else {
          flags[key] = true;
        }
      }
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags, files };
}

function strFlag(flags: Record<string, string | boolean>, name: string): string | undefined {
  const v = flags[name];
  return typeof v === 'string' ? v : undefined;
}

function reqFlag(flags: Record<string, string | boolean>, name: string): string {
  const v = strFlag(flags, name);
  if (!v) throw new Error(`Missing required option --${name}`);
  return v;
}

function intFlag(
  flags: Record<string, string | boolean>,
  name: string,
  minValue = 0,
): number | undefined {
  const raw = strFlag(flags, name);
  if (raw === undefined) return undefined;
  const num = Number(raw);
  if (!Number.isInteger(num) || num < minValue) {
    throw new Error(`Invalid --${name} (${raw}): must be an integer >= ${minValue}`);
  }
  return num;
}

function readInputText(parsed: ParsedArgs): string {
  if (parsed.files.length > 0) {
    return parsed.files.map((f) => fs.readFileSync(f, 'utf8')).join('\n\n');
  }
  const text = strFlag(parsed.flags, 'text');
  if (text !== undefined) return text;
  if (!process.stdin.isTTY) {
    return fs.readFileSync(0, 'utf8');
  }
  throw new Error('Provide input via --file <path> (repeatable for multipart outputs), --text <string>, or stdin');
}

async function maybeIssueCliConfirmationProof(
  svc: ThreadPassService,
  flags: Record<string, string | boolean>,
  rawText: string,
) {
  if (flags.authorize !== true) {
    return undefined;
  }
  if (!process.stdin.isTTY) {
    throw new Error(
      'authorize_instruction requires an interactive terminal (TTY) human review; non-interactive invocations cannot grant human_authority',
    );
  }
  const normalized = normalizeConfirmationText(rawText);
  const contentHash = sha256Hex(normalized);
  const shortCode = contentHash.slice(0, 8);
  const preview = normalized.length > 240 ? `${normalized.slice(0, 240)}...` : normalized;
  process.stderr.write(
    `\n[ThreadPass Human Authorization Review]\n` +
      `  purpose: authorize_instruction (grants Ed25519 signed human_authority)\n` +
      `  sha256 : ${contentHash}\n` +
      `  code   : ${shortCode}\n` +
      `  bytes  : ${Buffer.byteLength(normalized, 'utf8')}\n` +
      `  preview: ${JSON.stringify(preview)}\n`,
  );
  const rl = createInterface({ input: process.stdin, output: process.stderr });
  try {
    const ans = (
      await rl.question(`Type "yes" or code "${shortCode}" to authorize this exact content: `)
    )
      .trim()
      .toLowerCase();
    if (ans !== 'y' && ans !== 'yes' && ans !== shortCode) {
      throw new Error('Human authorization declined by operator.');
    }
  } finally {
    rl.close();
  }
  return svc.issueLocalConfirmationProof(normalized, 'authorize_instruction', 'local_cli');
}

function readTranscriptOptions(
  flags: Record<string, string | boolean>,
): TranscriptInputOptions & { hostSessionId?: string } {
  const uploadPath = strFlag(flags, 'upload-transcript');
  let uploadedTranscriptText: string | undefined;
  let uploadedTranscriptFilename: string | undefined;
  if (uploadPath) {
    uploadedTranscriptText = fs.readFileSync(uploadPath, 'utf8');
    uploadedTranscriptFilename = path.basename(uploadPath);
  }
  return {
    transcriptFilePath: strFlag(flags, 'transcript-file'),
    machineHostname: strFlag(flags, 'machine'),
    machineFingerprint: strFlag(flags, 'machine-fingerprint'),
    cloudProvider: strFlag(flags, 'cloud-host'),
    remoteUri: strFlag(flags, 'remote-uri'),
    hostSessionId: strFlag(flags, 'host-session-id'),
    uploadedTranscriptText,
    uploadedTranscriptFilename,
  };
}

function printHelp(): void {
  console.log(`ThreadPass CLI (thp) — Pure Node.js local-offline kernel & prompt shell

Usage:
  thp init --project <id> [--thread <id>] [--main <stream>] [--workspace <dir>] [--summary <text>] [--authorize] [--host-session-id <id>] [--transcript-file <path>] [--cloud-host <provider>] [--upload-transcript <path>]
  thp status --project <id> --thread <id>
  thp prompt adopt-extract [--source <label>]
  thp prompt checkpoint --project <id> --thread <id> [--stream <id>]
  thp adopt --project <id> --thread <id> --source <label> (--file <path>... | --text <str>) [--authorize] [--workspace <dir>] [--transcript-file <path>] [--machine <host>] [--cloud-host <provider>] [--remote-uri <url>] [--host-session-id <id>] [--upload-transcript <path>]
  thp reconcile pack --project <id> --thread <id> [--sources s1,s2,...] [--goal <text>] [--workspace <dir>] [--attachment]
  thp reconcile commit --project <id> --thread <id> [--pack <envelope_id>] [--sources s1,s2,...] (--file <path>... | --text <str>) [--authorize] [--confirm-topics <t1,t2>] [--workspace <dir>] [--transcript-file <path>] [--upload-transcript <path>]
  thp handoff pack --project <id> --thread <id> [--stream <id>] [--goal <text>] [--workspace <dir>] [--attachment]
  thp ingest --project <id> --thread <id> --stream <id> (--file <path>... | --text <str>) [--base-anchor <anchor_id>] [--authorize] [--ack-batch <batch_id> --ack-msgs m1,m2] [--workspace <dir>] [--transcript-file <path>] [--upload-transcript <path>]
  thp fork --project <id> --thread <id> --task <brief> [--origin <stream>] [--source-anchor <anchor_id>] [--worker <stream>] [--authorize] [--workspace <dir>] [--act-as-main] [--transcript-file <path>] [--machine <host>] [--cloud-host <provider>] [--remote-uri <url>] [--host-session-id <id>] [--upload-transcript <path>]
  thp grant issue --project <id> --thread <id> --grantee <stream> --max-spawns <n> [--issuer <stream>] [--parent-grant <id>] [--max-depth <n>] [--can-redelegate]
  thp grant revoke --project <id> --thread <id> --grant <id> [--reason <text>]
  thp mailbox send --project <id> --thread <id> --from <stream> --to <stream> --text <msg> [--authorize]
  thp peek --project <id> --thread <id> --stream <id>
  thp fsck --project <id> --thread <id> [--rebuild]
  thp gc --project <id> --thread <id> [--grace-ms <ms>]
  thp device show | pin --project <id> --device <id> --pubkey <b64> | revoke --project <id> --device <id>
  thp web [--project <id>] [--thread <id>] [--idle-ms <ms>]
`);
}

async function main(): Promise<void> {
  const parsed = parseCliArgs(process.argv.slice(2));
  const { positional, flags } = parsed;
  if (positional.length === 0 || flags.help || positional[0] === 'help') {
    printHelp();
    return;
  }

  const svc = new ThreadPassService();
  const cmd = positional[0];
  const sub = positional[1];

  if (cmd === 'init') {
    const tOpts = readTranscriptOptions(flags);
    const summary = strFlag(flags, 'summary');
    const summaryText = summary ?? 'Initialize ThreadPass Main coordination stream.';
    const proof = await maybeIssueCliConfirmationProof(svc, flags, summaryText);
    const out = svc.initThread({
      projectId: reqFlag(flags, 'project'),
      threadId: strFlag(flags, 'thread'),
      mainStreamId: strFlag(flags, 'main'),
      workspaceDir: strFlag(flags, 'workspace'),
      summary,
      confirmationProof: proof,
      ...tOpts,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'status') {
    const out = svc.getStatus(reqFlag(flags, 'project'), reqFlag(flags, 'thread'));
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'prompt' && sub === 'adopt-extract') {
    console.log(svc.getAdoptExtractPrompt(strFlag(flags, 'source')));
    return;
  }

  if (cmd === 'prompt' && sub === 'checkpoint') {
    const out = svc.getCheckpointPrompt(
      reqFlag(flags, 'project'),
      reqFlag(flags, 'thread'),
      strFlag(flags, 'stream'),
    );
    console.log(out.prompt);
    return;
  }

  if (cmd === 'adopt') {
    const rawText = readInputText(parsed);
    const proof = await maybeIssueCliConfirmationProof(svc, flags, rawText);
    const tOpts = readTranscriptOptions(flags);
    const out = svc.adoptExternal({
      projectId: reqFlag(flags, 'project'),
      threadId: reqFlag(flags, 'thread'),
      sourceLabel: strFlag(flags, 'source') ?? 'external_ai',
      rawText,
      confirmationProof: proof,
      allowForeignCanonical: flags['foreign-canonical'] === true,
      workspaceDir: strFlag(flags, 'workspace'),
      ...tOpts,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'reconcile' && sub === 'pack') {
    const sourcesCsv = strFlag(flags, 'sources');
    const out = svc.packReconcile({
      projectId: reqFlag(flags, 'project'),
      threadId: reqFlag(flags, 'thread'),
      sourceStreamIds: sourcesCsv ? sourcesCsv.split(',').filter(Boolean) : undefined,
      mainGoal: strFlag(flags, 'goal'),
      workspaceDir: strFlag(flags, 'workspace'),
      writeAttachment: flags.attachment === true,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'reconcile' && sub === 'commit') {
    const rawText = readInputText(parsed);
    const proof = await maybeIssueCliConfirmationProof(svc, flags, rawText);
    const tOpts = readTranscriptOptions(flags);
    const sourcesStr = strFlag(flags, 'sources');
    const confirmTopicsStr = strFlag(flags, 'confirm-topics');
    const out = svc.commitReconcile({
      projectId: reqFlag(flags, 'project'),
      threadId: reqFlag(flags, 'thread'),
      sourceStreamIds: sourcesStr ? sourcesStr.split(',').filter(Boolean) : [],
      packEnvelopeId: strFlag(flags, 'pack'),
      receiptRawText: rawText,
      confirmedConflictTopics: confirmTopicsStr
        ? confirmTopicsStr.split(',').map((s) => s.trim()).filter(Boolean)
        : undefined,
      confirmationProof: proof,
      workspaceDir: strFlag(flags, 'workspace'),
      ...tOpts,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'handoff' && sub === 'pack') {
    const out = svc.packHandoff({
      projectId: reqFlag(flags, 'project'),
      threadId: reqFlag(flags, 'thread'),
      streamId: strFlag(flags, 'stream'),
      goalOverride: strFlag(flags, 'goal'),
      workspaceDir: strFlag(flags, 'workspace'),
      writeAttachment: flags.attachment === true,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'ingest') {
    const rawText = readInputText(parsed);
    const proof = await maybeIssueCliConfirmationProof(svc, flags, rawText);
    const ackBatch = strFlag(flags, 'ack-batch');
    const ackMsgs = strFlag(flags, 'ack-msgs');
    const tOpts = readTranscriptOptions(flags);
    const out = svc.ingestReply({
      projectId: reqFlag(flags, 'project'),
      threadId: reqFlag(flags, 'thread'),
      streamId: reqFlag(flags, 'stream'),
      rawText,
      expectedBaseAnchorId: strFlag(flags, 'base-anchor'),
      confirmationProof: proof,
      ackBatchId: ackBatch,
      ackMessageIds: ackMsgs ? ackMsgs.split(',').filter(Boolean) : undefined,
      workspaceDir: strFlag(flags, 'workspace'),
      ...tOpts,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'fork') {
    const taskBrief = reqFlag(flags, 'task');
    const proof = await maybeIssueCliConfirmationProof(svc, flags, taskBrief);
    const tOpts = readTranscriptOptions(flags);
    const out = svc.forkDispatch({
      projectId: reqFlag(flags, 'project'),
      threadId: reqFlag(flags, 'thread'),
      originStreamId: strFlag(flags, 'origin'),
      sourceAnchorId: strFlag(flags, 'source-anchor'),
      workerStreamId: strFlag(flags, 'worker'),
      taskBrief,
      confirmationProof: proof,
      workspaceDir: strFlag(flags, 'workspace'),
      requireDisasterRedundant: flags['require-disaster-redundant'] === true,
      disasterVerified: flags['disaster-verified'] === true,
      actAsMain: flags['act-as-main'] === true,
      ...tOpts,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'grant' && sub === 'issue') {
    const maxSpawns = intFlag(flags, 'max-spawns', 1);
    if (maxSpawns === undefined) {
      throw new Error('Missing required option --max-spawns');
    }
    const maxDepth = intFlag(flags, 'max-depth', 0);
    const out = svc.issueDispatchGrant({
      projectId: reqFlag(flags, 'project'),
      threadId: reqFlag(flags, 'thread'),
      issuerStreamId: strFlag(flags, 'issuer'),
      granteeStreamId: reqFlag(flags, 'grantee'),
      parentGrantId: strFlag(flags, 'parent-grant'),
      maxTotalSpawns: maxSpawns,
      maxDepth,
      canRedelegate: flags['can-redelegate'] === true,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'grant' && sub === 'revoke') {
    const out = svc.revokeDispatchGrant({
      projectId: reqFlag(flags, 'project'),
      threadId: reqFlag(flags, 'thread'),
      grantId: reqFlag(flags, 'grant'),
      reason: strFlag(flags, 'reason'),
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'gc') {
    const graceMs = intFlag(flags, 'grace-ms', 0) ?? 0;
    const out = svc.engine.gcOrphanBlobs(
      reqFlag(flags, 'project'),
      reqFlag(flags, 'thread'),
      {
        gracePeriodMs: graceMs,
      },
    );
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'mailbox' && sub === 'send') {
    const msgText = reqFlag(flags, 'text');
    const proof = await maybeIssueCliConfirmationProof(svc, flags, msgText);
    const out = svc.sendMailboxMessage({
      projectId: reqFlag(flags, 'project'),
      threadId: reqFlag(flags, 'thread'),
      fromStreamId: reqFlag(flags, 'from'),
      targetStreamId: reqFlag(flags, 'to'),
      text: msgText,
      confirmationProof: proof,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'peek') {
    const out = svc.peekStream(
      reqFlag(flags, 'project'),
      reqFlag(flags, 'thread'),
      reqFlag(flags, 'stream'),
    );
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'fsck') {
    const out = svc.runFsck(reqFlag(flags, 'project'), reqFlag(flags, 'thread'), {
      rebuildSqlite: flags.rebuild === true,
    });
    console.log(JSON.stringify(out, null, 2));
    return;
  }

  if (cmd === 'device') {
    if (sub === 'show') {
      console.log(
        JSON.stringify(
          {
            device_id: svc.engine.identity.device_id,
            machine_fingerprint: svc.engine.machine.machine_fingerprint,
            machine_hostname: svc.engine.machine.hostname,
            fingerprint_source: svc.engine.machine.fingerprint_source,
            public_key_spki_b64: svc.engine.identity.public_key_spki_b64,
            home_dir: svc.engine.homeDir,
          },
          null,
          2,
        ),
      );
      return;
    }
    if (sub === 'pin') {
      const out = svc.pinDeviceKey(
        reqFlag(flags, 'project'),
        reqFlag(flags, 'device'),
        reqFlag(flags, 'pubkey'),
        strFlag(flags, 'thread'),
      );
      console.log(JSON.stringify(out, null, 2));
      return;
    }
    if (sub === 'revoke' || sub === 'retire') {
      const out = svc.revokeOrRetireDeviceKey(
        reqFlag(flags, 'project'),
        reqFlag(flags, 'device'),
        sub === 'revoke' ? 'revoked' : 'retired',
      );
      console.log(JSON.stringify(out, null, 2));
      return;
    }
  }

  if (cmd === 'web') {
    const idleMs = intFlag(flags, 'idle-ms', 0) ?? 15 * 60 * 1000;
    const handle = await startThreadPassWeb({
      service: svc,
      initialProjectId: strFlag(flags, 'project'),
      initialThreadId: strFlag(flags, 'thread'),
      idleTimeoutMs: idleMs,
    });
    console.log(`ThreadPass temporary local Web shell running (ephemeral, NOT a daemon):`);
    console.log(`  URL: ${handle.url}`);
    console.log(`  Content SHA-256 note: bound to 127.0.0.1:${handle.port}, closes on idle or Quit.`);
    // Suppress unused import check while keeping sha256Hex handy if needed
    void sha256Hex;
    return;
  }

  throw new Error(`Unknown command: ${positional.join(' ')}`);
}

main().catch((err) => {
  console.error(`thp error: ${(err as Error).message}`);
  process.exit(1);
});
