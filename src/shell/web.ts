import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { newHighEntropyToken, sha256Hex } from '../core/canonical.js';
import type { ThreadPassService } from './service.js';
import { issueConfirmationProof, normalizeConfirmationText } from './trust.js';

export interface WebShellOptions {
  service: ThreadPassService;
  initialProjectId?: string;
  initialThreadId?: string;
  idleTimeoutMs?: number;
  host?: string;
}

export interface WebShellHandle {
  url: string;
  port: number;
  token: string;
  close: () => Promise<void>;
}

function readJsonBody(req: http.IncomingMessage, maxBytes = 4 * 1024 * 1024): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    req.on('data', (chunk: Buffer) => {
      total += chunk.length;
      if (total > maxBytes) {
        reject(new Error('Request body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8').trim();
      if (!raw) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(raw) as Record<string, unknown>);
      } catch {
        reject(new Error('Invalid JSON request body'));
      }
    });
    req.on('error', reject);
  });
}

function sendJson(res: http.ServerResponse, status: number, payload: unknown): void {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
    'X-Content-Type-Options': 'nosniff',
  });
  res.end(body);
}

function renderHtmlPage(
  token: string,
  initialProjectId: string,
  initialThreadId: string,
): string {
  const safeToken = JSON.stringify(token);
  const safeProj = JSON.stringify(initialProjectId);
  const safeThr = JSON.stringify(initialThreadId);
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8" />
<title>ThreadPass Web Shell (thp web)</title>
<style>
  :root { --bg: #0f172a; --card: #1e293b; --fg: #e2e8f0; --muted: #94a3b8; --acc: #38bdf8; --ok: #22c55e; --warn: #f59e0b; --err: #ef4444; }
  body { margin: 0; font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--fg); line-height: 1.5; }
  header { display: flex; justify-content: space-between; align-items: center; padding: 14px 20px; background: #020617; border-bottom: 1px solid #334155; }
  .badge { font-size: 12px; padding: 3px 8px; border-radius: 99px; background: #1e3a8a; color: #bfdbfe; }
  main { max-width: 1120px; margin: 18px auto; padding: 0 16px; display: grid; gap: 16px; }
  .bar, .card { background: var(--card); border: 1px solid #334155; border-radius: 10px; padding: 14px 16px; }
  .row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
  label { font-size: 13px; color: var(--muted); display: block; margin-bottom: 4px; }
  input[type=text], textarea { width: 100%; box-sizing: border-box; background: #0b1220; color: var(--fg); border: 1px solid #475569; border-radius: 6px; padding: 8px 10px; font-family: ui-monospace, monospace; font-size: 13px; }
  textarea { min-height: 120px; resize: vertical; }
  textarea.drag-over { border-color: var(--acc); background: #0c1e35; }
  button { background: var(--acc); color: #082f49; border: none; border-radius: 6px; padding: 8px 14px; font-weight: 600; cursor: pointer; }
  button.sec { background: #334155; color: var(--fg); }
  button.danger { background: var(--err); color: #fff; }
  .tabs { display: flex; gap: 8px; flex-wrap: wrap; }
  .tab { background: #1e293b; color: var(--muted); border: 1px solid #334155; }
  .tab.active { background: var(--acc); color: #082f49; }
  .panel { display: none; }
  .panel.active { display: block; }
  pre { background: #020617; border: 1px solid #334155; border-radius: 8px; padding: 10px; overflow: auto; max-height: 320px; font-size: 12px; white-space: pre-wrap; }
  .part-box { border: 1px solid #475569; border-radius: 8px; padding: 10px; margin-top: 8px; background: #0b1220; }
  .streams-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 8px; margin: 8px 0; }
  .stream-item { background: #0b1220; border: 1px solid #334155; border-radius: 6px; padding: 8px; font-size: 12px; }
  .alert { display: none; padding: 10px 14px; border-radius: 8px; font-size: 13px; border: 1px solid; }
  .alert.show { display: block; }
  .alert.err { background: #450a0a; border-color: #ef4444; color: #fecaca; }
  .alert.ok { background: #052e16; border-color: #22c55e; color: #bbf7d0; }
  .file-label { font-size: 12px; color: var(--acc); cursor: pointer; display: inline-flex; align-items: center; gap: 6px; }
</style>
</head>
<body>
<header>
  <div>
    <strong>ThreadPass</strong> <span class="badge">thp web · 127.0.0.1 Ephemeral Shell · Zero Daemon</span>
  </div>
  <div class="row">
    <button class="sec" onclick="runUiAction('fsckOut', refreshStatus)">刷新状态</button>
    <button class="danger" onclick="shutdownServer()">退出并关闭服务</button>
  </div>
</header>
<main>
  <datalist id="streamIdsList"></datalist>
  <div id="uiAlert" class="alert" role="status"></div>

  <div class="bar row">
    <div style="flex:1; min-width:180px;"><label>Project ID</label><input id="projectId" type="text" /></div>
    <div style="flex:1; min-width:180px;"><label>Thread ID</label><input id="threadId" type="text" /></div>
    <div style="align-self:flex-end;"><button onclick="runUiAction('adoptOut', initThread)">新建 / 初始化 Main</button></div>
  </div>

  <div class="tabs">
    <button class="tab active" onclick="showTab('adopt', this)">1. 收编外部 AI (Adopt)</button>
    <button class="tab" onclick="showTab('reconcile', this)">2. 多源合并 (Reconcile)</button>
    <button class="tab" onclick="showTab('handoff', this)">3. 备份、续聊与派发 (Checkpoint / Handoff / Fork / Mailbox)</button>
    <button class="tab" onclick="showTab('fsck', this)">4. 谱系与校验 (Lineage & fsck)</button>
  </div>

  <section id="panel-adopt" class="card panel active">
    <h3>收编外部 AI 会话（支持结构化 JSON 或直接粘贴原始对话兜底）</h3>
    <div class="row">
      <div style="flex:1;"><label>Source Label（如 ai_gpt / ai_claude / ai_1）</label><input id="adoptSource" type="text" value="external_ai_1" /></div>
      <div style="align-self:flex-end;"><button class="sec" onclick="runUiAction('adoptOut', () => copyAdoptPrompt(this))">复制提取 Prompt 模板</button></div>
    </div>
    <div class="row" style="justify-content:space-between; margin-top:10px;">
      <label style="margin:0;">粘贴源 AI 回复或原始对话全文（自动跨块脱敏 + 防重复收编反查，支持拖放 .md/.txt）</label>
      <label class="file-label">选择文件导入<input type="file" accept=".md,.txt,.json,text/plain,text/markdown" style="display:none;" onchange="loadFileInto('adoptText', this)" /></label>
    </div>
    <textarea id="adoptText" placeholder="在此粘贴或拖放外部 AI 输出的 JSON 交接块或原始聊天记录..."></textarea>
    <div class="row" style="margin-top:8px;">
      <label style="display:flex;align-items:center;gap:6px;margin:0;color:var(--fg);">
        <input id="adoptConfirm" type="checkbox" /> 我已审阅上述确切原文并签发哈希绑定人类授权凭证（human_authority）
      </label>
      <button onclick="runUiAction('adoptOut', doAdopt)">提交收编 (Adopt)</button>
    </div>
    <pre id="adoptOut"></pre>
  </section>

  <section id="panel-reconcile" class="card panel">
    <h3>显式多源合并（选择要纳入 Main 的会话流）</h3>
    <div id="streamsChecklist" class="streams-grid"></div>
    <label>Main 综合目标</label>
    <input id="recGoal" type="text" value="Synthesize all selected streams into unified ThreadPass Main engineering plan." />
    <div class="row" style="margin-top:8px;">
      <label>单次粘贴上限 (chars)：<input id="recLimit" type="text" value="24000" style="width:90px;display:inline;" /></label>
      <button onclick="runUiAction('recOut', packReconcile)">生成 Reconcile 合并信封 & 分片</button>
      <button class="sec" onclick="downloadLatestMd()">导出 .md 附件</button>
    </div>
    <div id="recParts"></div>
    <div class="row" style="justify-content:space-between; margin-top:12px;">
      <label style="margin:0;">粘贴合并 AI 返回的 ReconciliationReceipt（或纯文本总结，支持拖放文件）</label>
      <label class="file-label">选择文件导入<input type="file" accept=".md,.txt,.json,text/plain,text/markdown" style="display:none;" onchange="loadFileInto('recReceipt', this)" /></label>
    </div>
    <textarea id="recReceipt" placeholder="粘贴或拖放 JSON ReconciliationReceipt 或总结文本"></textarea>
    <div class="row" style="margin-top:8px;">
      <label style="display:flex;align-items:center;gap:6px;margin:0;color:var(--fg);">
        <input id="recConfirm" type="checkbox" /> 人工审阅确认合并裁决并绑定内容哈希
      </label>
      <button onclick="runUiAction('recOut', commitReconcile)">落盘 Reconciliation & 更新 Main</button>
    </div>
    <pre id="recOut"></pre>
  </section>

  <section id="panel-handoff" class="card panel">
    <h3>A. 阶段备份回 Main / 子流 (Checkpoint Ingest)</h3>
    <div class="row">
      <div style="flex:1;"><label>Stream ID（留空默认 Main）</label><input id="ckptStream" type="text" list="streamIdsList" placeholder="例如 main 或 sub_a" /></div>
      <div style="flex:1;"><label>工作区目录（可选）</label><input id="ckptWorkspace" type="text" placeholder="例如 C:\\Projects\\ThreadPass" /></div>
      <div style="align-self:flex-end;"><button class="sec" onclick="runUiAction('hoOut', () => copyCheckpointPrompt(this))">复制已纳管流备份 Prompt</button></div>
    </div>
    <div class="row" style="justify-content:space-between; margin-top:8px;">
      <label style="margin:0;">粘贴 AI 返回的检查点 JSON（或这段新对话原文，支持拖放文件）</label>
      <label class="file-label">选择文件导入<input type="file" accept=".md,.txt,.json,text/plain,text/markdown" style="display:none;" onchange="loadFileInto('ckptText', this)" /></label>
    </div>
    <textarea id="ckptText" placeholder="粘贴或拖放检查点 JSON 或新阶段对话原文..."></textarea>
    <div class="row" style="margin-top:8px;">
      <label style="display:flex;align-items:center;gap:6px;margin:0;color:var(--fg);">
        <input id="ckptConfirm" type="checkbox" /> 我已审阅原文并签发哈希绑定授权凭证
      </label>
      <button onclick="runUiAction('hoOut', doIngest)">提交备份存档 (Ingest)</button>
    </div>

    <hr style="border-color:#334155;margin:16px 0;" />
    <h3>B. 跨宿主续聊 (Handoff Pack)</h3>
    <div class="row">
      <div style="flex:1;"><label>Target Stream ID（留空默认 Main）</label><input id="hoStream" type="text" list="streamIdsList" /></div>
      <div style="width:130px;"><label>单次上限 (chars)</label><input id="hoLimit" type="text" value="24000" /></div>
      <div style="align-self:flex-end;"><button onclick="runUiAction('hoOut', packHandoff)">打包续聊 Handoff</button></div>
      <div style="align-self:flex-end;"><button class="sec" onclick="downloadLatestMd()">导出 .md 附件</button></div>
    </div>
    <div id="hoParts"></div>

    <hr style="border-color:#334155;margin:16px 0;" />
    <h3>C. 派生独立 Subagent 子流 (Atomic Fork)</h3>
    <div class="row">
      <div style="flex:1;"><label>Origin Stream（留空默认 Main）</label><input id="forkOrigin" type="text" list="streamIdsList" /></div>
      <div style="flex:1;"><label>Worker Stream Name（可选，如 sub_a）</label><input id="forkWorker" type="text" placeholder="sub_a" /></div>
      <div style="flex:1;"><label>独立工作区 / worktree 目录（可选）</label><input id="forkWs" type="text" /></div>
    </div>
    <div class="row" style="margin-top:8px;">
      <div style="flex:1;"><label>派生子流任务描述 (Fork Task Brief)</label><input id="forkTask" type="text" placeholder="Implement sub-module X" /></div>
      <div style="align-self:flex-end;"><button onclick="runUiAction('hoOut', doFork)">原子派发 Fork</button></div>
    </div>
    <div id="forkPromptBox"></div>

    <hr style="border-color:#334155;margin:16px 0;" />
    <h3>D. 跨流异步发信 (Mailbox Send)</h3>
    <div class="row">
      <div style="flex:1;"><label>From Stream ID</label><input id="mbFrom" type="text" list="streamIdsList" placeholder="sub_a" /></div>
      <div style="flex:1;"><label>To Stream ID</label><input id="mbTo" type="text" list="streamIdsList" placeholder="main" /></div>
      <div style="flex:2;"><label>Message</label><input id="mbText" type="text" placeholder="Async note for target stream..." /></div>
      <div style="align-self:flex-end;"><button class="sec" onclick="runUiAction('hoOut', doMailboxSend)">发送消息</button></div>
    </div>

    <pre id="hoOut" style="margin-top:12px;"></pre>
  </section>

  <section id="panel-fsck" class="card panel">
    <h3>Lineage 状态与四步 fsck 核验重建</h3>
    <div class="row">
      <button onclick="runUiAction('fsckOut', () => runFsck(false))">运行 4 步 thp fsck</button>
      <button class="sec" onclick="runUiAction('fsckOut', () => runFsck(true))">删除 SQLite 并重建 (fsck --rebuild)</button>
    </div>
    <pre id="fsckOut"></pre>
  </section>
</main>
<script>
const TOKEN = ${safeToken};
document.getElementById('projectId').value = ${safeProj} || 'threadpass-bootstrap';
document.getElementById('threadId').value = ${safeThr} || '';
let latestMarkdown = '';
let latestStreams = [];

function showAlert(msg, kind) {
  const el = document.getElementById('uiAlert');
  if (!msg) {
    el.className = 'alert';
    el.textContent = '';
    return;
  }
  el.className = 'alert show ' + (kind === 'ok' ? 'ok' : 'err');
  el.textContent = String(msg);
}

async function runUiAction(outId, fn) {
  showAlert('');
  try {
    await fn();
  } catch (err) {
    const msg = (err && err.message) ? err.message : String(err);
    showAlert('操作失败：' + msg, 'err');
    const outEl = document.getElementById(outId);
    if (outEl) outEl.textContent = 'ERROR: ' + msg;
  }
}

async function copyTextWithFeedback(text, btn) {
  if (navigator.clipboard) {
    await navigator.clipboard.writeText(text);
  }
  if (btn) {
    const orig = btn.textContent;
    btn.textContent = '已复制 ✓';
    setTimeout(() => { btn.textContent = orig; }, 1500);
  }
}

function wireDropAndFile(textareaId) {
  const ta = document.getElementById(textareaId);
  if (!ta) return;
  ta.addEventListener('dragover', (e) => {
    e.preventDefault();
    ta.classList.add('drag-over');
  });
  ta.addEventListener('dragleave', () => ta.classList.remove('drag-over'));
  ta.addEventListener('drop', async (e) => {
    e.preventDefault();
    ta.classList.remove('drag-over');
    const file = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
    if (file) ta.value = await file.text();
  });
}

async function loadFileInto(textareaId, inputEl) {
  const file = inputEl && inputEl.files && inputEl.files[0];
  if (file) {
    document.getElementById(textareaId).value = await file.text();
  }
}

async function api(path, body) {
  const res = await fetch(path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-ThreadPass-Token': TOKEN },
    body: JSON.stringify(body || {})
  });
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || ('HTTP ' + res.status));
  return data;
}

function showTab(id, btn) {
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
  document.getElementById('panel-' + id).classList.add('active');
  btn.classList.add('active');
  if (id === 'reconcile' || id === 'handoff') refreshStatus().catch(() => {});
}

function getIds() {
  return {
    projectId: document.getElementById('projectId').value.trim(),
    threadId: document.getElementById('threadId').value.trim()
  };
}

async function initThread() {
  const { projectId, threadId } = getIds();
  const res = await api('/api/init', { projectId, threadId: threadId || undefined });
  document.getElementById('threadId').value = res.thread_id;
  await refreshStatus();
  showAlert('已初始化 Thread ' + res.thread_id + ' (main=' + res.main_stream_id + ')', 'ok');
}

async function refreshStatus() {
  const { projectId, threadId } = getIds();
  if (!projectId || !threadId) return;
  const st = await api('/api/status', { projectId, threadId });
  latestStreams = st.streams || [];
  const box = document.getElementById('streamsChecklist');
  box.textContent = '';
  const dl = document.getElementById('streamIdsList');
  dl.textContent = '';
  for (const s of latestStreams) {
    const sid = String(s.stream_id || '');
    const opt = document.createElement('option');
    opt.value = sid;
    dl.appendChild(opt);

    const div = document.createElement('div');
    div.className = 'stream-item';
    const label = document.createElement('label');
    label.style.color = 'var(--fg)';
    label.style.margin = '0';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.className = 'rec-src';
    cb.value = sid;
    cb.checked = !s.is_main;
    const strong = document.createElement('strong');
    strong.textContent = ' ' + sid;
    label.appendChild(cb);
    label.appendChild(strong);
    const meta = document.createElement('div');
    meta.textContent = 'anchor: ' + (s.head_anchor_id || '-') + ' | health: ' + String(s.health || '') + (s.is_main ? ' (MAIN)' : '');
    div.appendChild(label);
    div.appendChild(meta);
    box.appendChild(div);
  }
  document.getElementById('fsckOut').textContent = JSON.stringify(st, null, 2);
}

async function copyAdoptPrompt(btn) {
  const src = document.getElementById('adoptSource').value.trim() || 'external_ai';
  const res = await api('/api/prompt/adopt-extract', { sourceLabel: src });
  await copyTextWithFeedback(res.prompt, btn);
  document.getElementById('adoptOut').textContent = res.prompt;
}

async function doAdopt() {
  const { projectId, threadId } = getIds();
  const rawText = document.getElementById('adoptText').value;
  const sourceLabel = document.getElementById('adoptSource').value.trim();
  const confirm = document.getElementById('adoptConfirm').checked;
  const res = await api('/api/adopt', { projectId, threadId, sourceLabel, rawText, confirmAuthorize: confirm });
  document.getElementById('adoptOut').textContent = JSON.stringify(res, null, 2);
  showAlert('已收编流 ' + res.stream_id + ' (trust=' + res.trust_tier + ')', 'ok');
  await refreshStatus();
}

function renderParts(containerId, parts) {
  const el = document.getElementById(containerId);
  el.textContent = '';
  for (const p of parts) {
    const d = document.createElement('div');
    d.className = 'part-box';
    const btn = document.createElement('button');
    btn.className = 'sec';
    btn.textContent = '一键复制 Part ' + p.part_index + '/' + p.total_parts;
    btn.onclick = () => copyTextWithFeedback(p.text, btn);
    const pre = document.createElement('pre');
    pre.textContent = p.text;
    d.appendChild(btn);
    d.appendChild(pre);
    el.appendChild(d);
  }
}

async function packReconcile() {
  const { projectId, threadId } = getIds();
  const selected = Array.from(document.querySelectorAll('.rec-src:checked')).map(c => c.value);
  const perTurnLimit = Number(document.getElementById('recLimit').value) || 24000;
  const res = await api('/api/reconcile/pack', {
    projectId,
    threadId,
    sourceStreamIds: selected,
    mainGoal: document.getElementById('recGoal').value,
    perTurnLimit
  });
  latestMarkdown = res.full_markdown;
  renderParts('recParts', res.parts);
  document.getElementById('recOut').textContent = JSON.stringify({
    envelope_id: res.envelope_id,
    manifest_hash: res.manifest_hash,
    parts: res.parts.length,
    estimated_tokens: res.estimated_tokens,
    deduplicated_sections: res.deduplicated_sections,
    folded_sections: res.folded_sections
  }, null, 2);
}

function downloadLatestMd() {
  if (!latestMarkdown) {
    showAlert('暂无可导出的信封，请先生成 Reconcile 或 Handoff 包。', 'err');
    return;
  }
  const blob = new Blob([latestMarkdown], { type: 'text/markdown;charset=utf-8' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'threadpass-bundle.md';
  a.click();
}

async function commitReconcile() {
  const { projectId, threadId } = getIds();
  const selected = Array.from(document.querySelectorAll('.rec-src:checked')).map(c => c.value);
  const receiptRawText = document.getElementById('recReceipt').value;
  const confirmAuthorize = document.getElementById('recConfirm').checked;
  const res = await api('/api/reconcile/commit', {
    projectId, threadId, sourceStreamIds: selected, receiptRawText, confirmAuthorize
  });
  document.getElementById('recOut').textContent = JSON.stringify(res, null, 2);
  showAlert('已合并提交到 Main (anchor=' + res.anchor_id + ')', 'ok');
  await refreshStatus();
}

async function copyCheckpointPrompt(btn) {
  const { projectId, threadId } = getIds();
  const streamId = document.getElementById('ckptStream').value.trim() || undefined;
  const res = await api('/api/prompt/checkpoint', { projectId, threadId, streamId });
  await copyTextWithFeedback(res.prompt, btn);
  document.getElementById('hoOut').textContent = res.prompt;
}

async function doIngest() {
  const { projectId, threadId } = getIds();
  let streamId = document.getElementById('ckptStream').value.trim();
  if (!streamId) {
    const st = await api('/api/status', { projectId, threadId });
    streamId = st.thread && st.thread.main_stream_id;
  }
  const rawText = document.getElementById('ckptText').value;
  const workspaceDir = document.getElementById('ckptWorkspace').value.trim() || undefined;
  const confirmAuthorize = document.getElementById('ckptConfirm').checked;
  const res = await api('/api/ingest', {
    projectId, threadId, streamId, rawText, workspaceDir, confirmAuthorize
  });
  document.getElementById('hoOut').textContent = JSON.stringify(res, null, 2);
  showAlert('已提交检查点存档到流 ' + streamId, 'ok');
  await refreshStatus();
}

async function packHandoff() {
  const { projectId, threadId } = getIds();
  const streamId = document.getElementById('hoStream').value.trim() || undefined;
  const perTurnLimit = Number(document.getElementById('hoLimit').value) || 24000;
  const res = await api('/api/handoff/pack', { projectId, threadId, streamId, perTurnLimit });
  latestMarkdown = res.full_markdown;
  renderParts('hoParts', res.parts);
  document.getElementById('hoOut').textContent = JSON.stringify({
    stream_id: res.stream_id,
    anchor_id: res.anchor_id,
    offered_batch_id: res.offered_batch_id,
    parts: res.parts.length
  }, null, 2);
}

async function doFork() {
  const { projectId, threadId } = getIds();
  const originStreamId = document.getElementById('forkOrigin').value.trim() || undefined;
  const workerStreamId = document.getElementById('forkWorker').value.trim() || undefined;
  const workspaceDir = document.getElementById('forkWs').value.trim() || undefined;
  const taskBrief = document.getElementById('forkTask').value;
  const res = await api('/api/fork', {
    projectId, threadId, originStreamId, workerStreamId, workspaceDir, taskBrief
  });
  const box = document.getElementById('forkPromptBox');
  box.textContent = '';
  if (res.worker_prompt) {
    renderParts('forkPromptBox', [{ part_index: 1, total_parts: 1, text: res.worker_prompt }]);
  }
  document.getElementById('hoOut').textContent = JSON.stringify({
    dispatch_id: res.dispatch_id,
    worker_stream_id: res.worker_stream_id,
    worker_anchor_id: res.worker_anchor_id,
    durability_state: res.durability_state,
    release_gated: res.release_gated
  }, null, 2);
  showAlert('已派生子流 ' + res.worker_stream_id, 'ok');
  await refreshStatus();
}

async function doMailboxSend() {
  const { projectId, threadId } = getIds();
  const fromStreamId = document.getElementById('mbFrom').value.trim();
  const targetStreamId = document.getElementById('mbTo').value.trim();
  const text = document.getElementById('mbText').value;
  const res = await api('/api/mailbox/send', {
    projectId, threadId, fromStreamId, targetStreamId, text
  });
  document.getElementById('hoOut').textContent = JSON.stringify(res, null, 2);
  showAlert('已发送异步消息 ' + res.message_id, 'ok');
  await refreshStatus();
}

async function runFsck(rebuildSqlite) {
  const { projectId, threadId } = getIds();
  const res = await api('/api/fsck', { projectId, threadId, rebuildSqlite });
  document.getElementById('fsckOut').textContent = JSON.stringify(res, null, 2);
}

async function shutdownServer() {
  await api('/api/shutdown', {});
  document.body.textContent = '';
  const msg = document.createElement('div');
  msg.style.padding = '40px';
  msg.style.fontFamily = 'sans-serif';
  msg.style.color = '#e2e8f0';
  msg.textContent = 'thp web 已安全退出（无后台常驻进程）。';
  document.body.appendChild(msg);
}

wireDropAndFile('adoptText');
wireDropAndFile('recReceipt');
wireDropAndFile('ckptText');
refreshStatus().catch(() => {});
</script>
</body>
</html>`;
}

export async function startThreadPassWeb(
  options: WebShellOptions,
): Promise<WebShellHandle> {
  const bindHost = options.host ?? '127.0.0.1';
  if (bindHost !== '127.0.0.1') {
    throw new Error('thp web MUST bind exclusively to 127.0.0.1');
  }
  const token = newHighEntropyToken(32);
  const idleMs = options.idleTimeoutMs ?? 15 * 60 * 1000;
  let idleTimer: NodeJS.Timeout | undefined;
  let serverPort = 0;

  const isAllowedOriginOrHost = (req: http.IncomingMessage): boolean => {
    const allowedHosts = new Set([
      `127.0.0.1:${serverPort}`,
      `localhost:${serverPort}`,
    ]);
    const hostHdr = (req.headers.host ?? '').toLowerCase();
    if (!allowedHosts.has(hostHdr)) {
      return false;
    }
    const origin = req.headers.origin;
    if (origin) {
      const allowedOrigins = new Set([
        `http://127.0.0.1:${serverPort}`,
        `http://localhost:${serverPort}`,
      ]);
      if (!allowedOrigins.has(origin.toLowerCase())) {
        return false;
      }
    }
    return true;
  };

  const server = http.createServer(async (req, res) => {
    resetIdleTimer();
    if (!isAllowedOriginOrHost(req)) {
      sendJson(res, 403, { error: 'Forbidden Host/Origin (DNS Rebinding protection)' });
      return;
    }

    const reqUrl = new URL(req.url ?? '/', `http://127.0.0.1:${serverPort}`);
    if (req.method === 'GET' && reqUrl.pathname === '/') {
      const qTok = reqUrl.searchParams.get('token');
      if (qTok !== token) {
        sendJson(res, 401, { error: 'Unauthorized: invalid or missing session token' });
        return;
      }
      const html = renderHtmlPage(
        token,
        options.initialProjectId ?? '',
        options.initialThreadId ?? '',
      );
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store',
        'Content-Security-Policy':
          "default-src 'self' 'unsafe-inline'; connect-src 'self'; object-src 'none'; base-uri 'none'",
      });
      res.end(html);
      return;
    }

    if (req.method === 'POST' && reqUrl.pathname.startsWith('/api/')) {
      const hdrTok = req.headers['x-threadpass-token'];
      if (hdrTok !== token) {
        sendJson(res, 401, { error: 'Unauthorized: invalid X-ThreadPass-Token' });
        return;
      }
      try {
        const body = await readJsonBody(req);
        const svc = options.service;

        if (reqUrl.pathname === '/api/init') {
          sendJson(
            res,
            200,
            svc.initThread({
              projectId: String(body.projectId || 'threadpass-bootstrap'),
              threadId: body.threadId ? String(body.threadId) : undefined,
              workspaceDir: body.workspaceDir ? String(body.workspaceDir) : undefined,
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/status') {
          sendJson(
            res,
            200,
            svc.getStatus(String(body.projectId), String(body.threadId)),
          );
          return;
        }
        if (reqUrl.pathname === '/api/prompt/adopt-extract') {
          sendJson(res, 200, {
            prompt: svc.getAdoptExtractPrompt(
              body.sourceLabel ? String(body.sourceLabel) : undefined,
            ),
          });
          return;
        }
        if (reqUrl.pathname === '/api/prompt/checkpoint') {
          sendJson(
            res,
            200,
            svc.getCheckpointPrompt(
              String(body.projectId),
              String(body.threadId),
              body.streamId ? String(body.streamId) : undefined,
            ),
          );
          return;
        }
        if (reqUrl.pathname === '/api/ingest') {
          const rawText = String(body.rawText ?? '');
          const proof = body.confirmAuthorize
            ? issueConfirmationProof(
                normalizeConfirmationText(rawText),
                'authorize_instruction',
                'thp_web',
              )
            : undefined;
          sendJson(
            res,
            200,
            svc.ingestReply({
              projectId: String(body.projectId),
              threadId: String(body.threadId),
              streamId: String(body.streamId),
              rawText,
              confirmationProof: proof,
              ackBatchId: body.ackBatchId ? String(body.ackBatchId) : undefined,
              ackMessageIds: Array.isArray(body.ackMessageIds)
                ? body.ackMessageIds.map(String)
                : undefined,
              workspaceDir: body.workspaceDir ? String(body.workspaceDir) : undefined,
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/mailbox/send') {
          sendJson(
            res,
            200,
            svc.sendMailboxMessage({
              projectId: String(body.projectId),
              threadId: String(body.threadId),
              fromStreamId: String(body.fromStreamId),
              targetStreamId: String(body.targetStreamId),
              text: String(body.text ?? ''),
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/adopt') {
          const rawText = String(body.rawText ?? '');
          const proof = body.confirmAuthorize
            ? issueConfirmationProof(
                normalizeConfirmationText(rawText),
                'authorize_instruction',
                'thp_web',
              )
            : undefined;
          sendJson(
            res,
            200,
            svc.adoptExternal({
              projectId: String(body.projectId),
              threadId: String(body.threadId),
              sourceLabel: body.sourceLabel ? String(body.sourceLabel) : 'web_adopt',
              rawText,
              confirmationProof: proof,
              allowForeignCanonical: Boolean(body.allowForeignCanonical),
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/reconcile/pack') {
          const perTurn =
            typeof body.perTurnLimit === 'number' && body.perTurnLimit >= 1200
              ? body.perTurnLimit
              : 24000;
          sendJson(
            res,
            200,
            svc.packReconcile({
              projectId: String(body.projectId),
              threadId: String(body.threadId),
              sourceStreamIds: Array.isArray(body.sourceStreamIds)
                ? body.sourceStreamIds.map(String)
                : undefined,
              mainGoal: body.mainGoal ? String(body.mainGoal) : undefined,
              budget: {
                effective_context_budget: 120000,
                per_turn_input_limit: perTurn,
                budget_confidence: 'heuristic',
                supports_attachments: true,
              },
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/reconcile/commit') {
          const receiptRawText = String(body.receiptRawText ?? '');
          const proof = body.confirmAuthorize
            ? issueConfirmationProof(
                normalizeConfirmationText(receiptRawText),
                'authorize_instruction',
                'thp_web',
              )
            : undefined;
          sendJson(
            res,
            200,
            svc.commitReconcile({
              projectId: String(body.projectId),
              threadId: String(body.threadId),
              sourceStreamIds: Array.isArray(body.sourceStreamIds)
                ? body.sourceStreamIds.map(String)
                : [],
              receiptRawText,
              confirmationProof: proof,
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/handoff/pack') {
          const perTurn =
            typeof body.perTurnLimit === 'number' && body.perTurnLimit >= 1200
              ? body.perTurnLimit
              : 24000;
          sendJson(
            res,
            200,
            svc.packHandoff({
              projectId: String(body.projectId),
              threadId: String(body.threadId),
              streamId: body.streamId ? String(body.streamId) : undefined,
              budget: {
                effective_context_budget: 120000,
                per_turn_input_limit: perTurn,
                budget_confidence: 'heuristic',
                supports_attachments: true,
              },
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/fork') {
          sendJson(
            res,
            200,
            svc.forkDispatch({
              projectId: String(body.projectId),
              threadId: String(body.threadId),
              originStreamId: body.originStreamId ? String(body.originStreamId) : undefined,
              workerStreamId: body.workerStreamId ? String(body.workerStreamId) : undefined,
              workspaceDir: body.workspaceDir ? String(body.workspaceDir) : undefined,
              taskBrief: String(body.taskBrief ?? 'Execute scoped subtask'),
              requireDisasterRedundant: Boolean(body.requireDisasterRedundant),
              disasterVerified: Boolean(body.disasterVerified),
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/fsck') {
          sendJson(
            res,
            200,
            svc.runFsck(String(body.projectId), String(body.threadId), {
              rebuildSqlite: Boolean(body.rebuildSqlite),
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/shutdown') {
          sendJson(res, 200, { closing: true });
          void close();
          return;
        }

        sendJson(res, 404, { error: `Unknown endpoint ${reqUrl.pathname}` });
      } catch (err) {
        sendJson(res, 400, { error: (err as Error).message });
      }
      return;
    }

    sendJson(res, 404, { error: 'Not found' });
  });

  const close = () =>
    new Promise<void>((resolve) => {
      if (idleTimer) {
        clearTimeout(idleTimer);
        idleTimer = undefined;
      }
      server.close(() => resolve());
    });

  const resetIdleTimer = () => {
    if (idleTimer) clearTimeout(idleTimer);
    if (idleMs > 0) {
      idleTimer = setTimeout(() => {
        void close();
      }, idleMs);
      idleTimer.unref();
    }
  };

  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      serverPort = (server.address() as AddressInfo).port;
      resetIdleTimer();
      resolve();
    });
  });

  // Unused hash helper guard for content confirmation previews
  void sha256Hex;

  return {
    url: `http://127.0.0.1:${serverPort}/?token=${token}`,
    port: serverPort,
    token,
    close,
  };
}
