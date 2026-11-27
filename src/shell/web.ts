import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { newHighEntropyToken, sha256Hex } from '../core/canonical.js';
import type { ThreadPassService } from './service.js';
import { normalizeConfirmationText } from './trust.js';

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

function reqBodyStr(body: Record<string, unknown>, key: string): string {
  const val = body[key];
  if (typeof val !== 'string' || !val.trim()) {
    throw new Error(`Missing required field: ${key}`);
  }
  return val.trim();
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
  const safeScriptJson = (v: string) => JSON.stringify(v).replace(/</g, '\\u003c');
  const safeToken = safeScriptJson(token);
  const safeProj = safeScriptJson(initialProjectId);
  const safeThr = safeScriptJson(initialThreadId);
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
    <div class="row" style="margin-top:8px;">
      <div style="flex:2; min-width:220px;"><label>原宿主本地会话文件路径（可选，同机 grep 定位）</label><input id="adoptTranscriptPath" type="text" placeholder="例如 C:\\Users\\...\\transcript_full.jsonl" /></div>
      <div style="flex:1; min-width:140px;"><label>云端宿主标识（可选，无本地日志时）</label><input id="adoptCloudHost" type="text" placeholder="如 devin_cloud / web_gpt" /></div>
      <div style="flex:1; min-width:140px;"><label>Host Session ID（可选）</label><input id="adoptHostSess" type="text" placeholder="可选会话 ID" /></div>
    </div>
    <div class="row" style="margin-top:8px; justify-content:space-between;">
      <label class="file-label">可选：上传 Web 端原始会话文件备份（脱敏存入本地 blob）<input id="adoptUploadFile" type="file" accept=".md,.txt,.json,.jsonl,text/plain,text/markdown" /></label>
    </div>
    <div class="row" style="justify-content:space-between; margin-top:10px;">
      <label style="margin:0;">粘贴源 AI 回复或原始对话全文（支持按顺序粘贴/追加多段分片 JSON，自动脱敏 + 防重复收编反查，支持拖放 .md/.txt）</label>
      <div class="row" style="gap:10px;">
        <label class="file-label">选择文件导入<input type="file" accept=".md,.txt,.json,text/plain,text/markdown" style="display:none;" onchange="loadFileInto('adoptText', this)" /></label>
        <label class="file-label">+ 追加分片文件<input type="file" accept=".md,.txt,.json,text/plain,text/markdown" style="display:none;" onchange="appendFileInto('adoptText', this)" /></label>
      </div>
    </div>
    <textarea id="adoptText" placeholder="在此粘贴、拖放或追加外部 AI 输出的单段/多段 JSON 交接块或原始聊天记录..."></textarea>
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
      <label style="margin:0;">粘贴合并 AI 返回的 ReconciliationReceipt（支持多段分片 JSON 或纯文本总结，支持拖放文件）</label>
      <div class="row" style="gap:10px;">
        <label class="file-label">选择文件导入<input type="file" accept=".md,.txt,.json,text/plain,text/markdown" style="display:none;" onchange="loadFileInto('recReceipt', this)" /></label>
        <label class="file-label">+ 追加分片文件<input type="file" accept=".md,.txt,.json,text/plain,text/markdown" style="display:none;" onchange="appendFileInto('recReceipt', this)" /></label>
      </div>
    </div>
    <textarea id="recReceipt" placeholder="粘贴、拖放或追加单段/多段 JSON ReconciliationReceipt 或总结文本"></textarea>
    <div class="row" style="margin-top:8px;">
      <div style="flex:2; min-width:200px;"><label>合并 AI 原生会话文件路径（可选）</label><input id="recTranscriptPath" type="text" placeholder="可选本地日志路径" /></div>
      <div style="flex:1; min-width:140px;"><label>云端宿主（可选）</label><input id="recCloudHost" type="text" placeholder="如 claude_web" /></div>
    </div>
    <div class="row" style="margin-top:8px;">
      <label class="file-label">可选：上传合并 AI 原始会话副本（自动脱敏）<input id="recUploadFile" type="file" accept=".md,.txt,.json,.jsonl,text/plain,text/markdown" /></label>
    </div>
    <div class="row" style="margin-top:8px;">
      <div style="flex:1; min-width:220px;"><label>Bound Reconcile Pack ID（生成信封时自动填入，防止来源漂移）</label><input id="recPackId" type="text" placeholder="env_xxx" /></div>
      <div style="flex:1; min-width:220px;"><label>仅确认指定冲突主题（可选，逗号分隔；留空且勾选确认则确认全部）</label><input id="recConfirmTopics" type="text" placeholder="Topic A, Topic B" /></div>
    </div>
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
      <div style="flex:1;"><label>Base Anchor ID（防过期提交校验，可选）</label><input id="ckptBaseAnchor" type="text" placeholder="anc_xxx（点击右侧按钮自动填入）" /></div>
      <div style="flex:1;"><label>工作区目录（可选）</label><input id="ckptWorkspace" type="text" placeholder="例如 C:\\Projects\\ThreadPass" /></div>
      <div style="align-self:flex-end;"><button class="sec" onclick="runUiAction('hoOut', () => copyCheckpointPrompt(this))">复制已纳管流备份 Prompt</button></div>
    </div>
    <div class="row" style="justify-content:space-between; margin-top:8px;">
      <label style="margin:0;">粘贴 AI 返回的检查点 JSON（支持多段分片 JSON 或这段新对话原文，支持拖放文件）</label>
      <div class="row" style="gap:10px;">
        <label class="file-label">选择文件导入<input type="file" accept=".md,.txt,.json,text/plain,text/markdown" style="display:none;" onchange="loadFileInto('ckptText', this)" /></label>
        <label class="file-label">+ 追加分片文件<input type="file" accept=".md,.txt,.json,text/plain,text/markdown" style="display:none;" onchange="appendFileInto('ckptText', this)" /></label>
      </div>
    </div>
    <textarea id="ckptText" placeholder="粘贴、拖放或追加单段/多段检查点 JSON 或新阶段对话原文..."></textarea>
    <div class="row" style="margin-top:8px;">
      <div style="flex:2; min-width:200px;"><label>当前 AI 原生会话文件路径（可选）</label><input id="ckptTranscriptPath" type="text" placeholder="可选本地日志路径" /></div>
      <div style="flex:1; min-width:140px;"><label>云端宿主（可选）</label><input id="ckptCloudHost" type="text" placeholder="如 devin_cloud" /></div>
    </div>
    <div class="row" style="margin-top:8px;">
      <label class="file-label">可选：上传该阶段原始会话副本（自动脱敏）<input id="ckptUploadFile" type="file" accept=".md,.txt,.json,.jsonl,text/plain,text/markdown" /></label>
    </div>
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
    <div class="row" style="margin-top:8px;">
      <div style="flex:2; min-width:200px;"><label>子流原生会话路径（可选）</label><input id="forkTranscriptPath" type="text" placeholder="可选本地日志路径" /></div>
      <div style="flex:1; min-width:140px;"><label>子流云端宿主（可选）</label><input id="forkCloudHost" type="text" placeholder="如 devin_cloud" /></div>
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

async function appendFileInto(textareaId, inputEl) {
  const file = inputEl && inputEl.files && inputEl.files[0];
  if (!file) return;
  const ta = document.getElementById(textareaId);
  const next = await file.text();
  ta.value = ta.value.trim() ? (ta.value.trimEnd() + '\n\n' + next) : next;
  inputEl.value = '';
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

async function readOptionalUpload(inputId) {
  const el = document.getElementById(inputId);
  const file = el && el.files && el.files[0];
  if (!file) return {};
  return { uploadedTranscriptText: await file.text(), uploadedTranscriptFilename: file.name };
}

async function prepareWebAuthChallenge(rawText) {
  const ch = await api('/api/confirm/prepare', { contentText: rawText, purpose: 'authorize_instruction' });
  const ok = window.confirm(
    '确认签发一次性 human_authority 授权凭证？\\n' +
    'SHA-256: ' + ch.content_sha256 + '\\n' +
    '字节数: ' + ch.byte_length + '\\n' +
    '内容预览: ' + ch.preview
  );
  if (!ok) throw new Error('Operator declined human authorization review.');
  return { confirmChallengeId: ch.challenge_id, confirmContentSha256: ch.content_sha256 };
}

async function doAdopt() {
  const { projectId, threadId } = getIds();
  const rawText = document.getElementById('adoptText').value;
  const sourceLabel = document.getElementById('adoptSource').value.trim();
  const confirm = document.getElementById('adoptConfirm').checked;
  const authFields = confirm ? await prepareWebAuthChallenge(rawText) : {};
  const transcriptFilePath = document.getElementById('adoptTranscriptPath').value.trim() || undefined;
  const cloudProvider = document.getElementById('adoptCloudHost').value.trim() || undefined;
  const hostSessionId = document.getElementById('adoptHostSess').value.trim() || undefined;
  const upload = await readOptionalUpload('adoptUploadFile');
  const res = await api('/api/adopt', {
    projectId, threadId, sourceLabel, rawText, ...authFields,
    transcriptFilePath, cloudProvider, hostSessionId, ...upload
  });
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
  document.getElementById('recPackId').value = res.envelope_id || '';
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
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'threadpass-bundle.md';
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function commitReconcile() {
  const { projectId, threadId } = getIds();
  const selected = Array.from(document.querySelectorAll('.rec-src:checked')).map(c => c.value);
  const receiptRawText = document.getElementById('recReceipt').value;
  const packEnvelopeId = document.getElementById('recPackId').value.trim() || undefined;
  const confirmTopicsRaw = document.getElementById('recConfirmTopics').value.trim();
  const confirmedConflictTopics = confirmTopicsRaw
    ? confirmTopicsRaw.split(',').map(s => s.trim()).filter(Boolean)
    : undefined;
  const confirmAuthorize = document.getElementById('recConfirm').checked;
  const authFields = confirmAuthorize ? await prepareWebAuthChallenge(receiptRawText) : {};
  const transcriptFilePath = document.getElementById('recTranscriptPath').value.trim() || undefined;
  const cloudProvider = document.getElementById('recCloudHost').value.trim() || undefined;
  const upload = await readOptionalUpload('recUploadFile');
  const res = await api('/api/reconcile/commit', {
    projectId, threadId, sourceStreamIds: selected, receiptRawText, packEnvelopeId,
    confirmedConflictTopics, ...authFields,
    transcriptFilePath, cloudProvider, ...upload
  });
  document.getElementById('recOut').textContent = JSON.stringify(res, null, 2);
  showAlert('已合并提交到 Main (anchor=' + res.anchor_id + ')', 'ok');
  await refreshStatus();
}

async function copyCheckpointPrompt(btn) {
  const { projectId, threadId } = getIds();
  const streamId = document.getElementById('ckptStream').value.trim() || undefined;
  const res = await api('/api/prompt/checkpoint', { projectId, threadId, streamId });
  document.getElementById('ckptBaseAnchor').value = res.anchor_id || '';
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
  const expectedBaseAnchorId = document.getElementById('ckptBaseAnchor').value.trim() || undefined;
  const workspaceDir = document.getElementById('ckptWorkspace').value.trim() || undefined;
  const confirmAuthorize = document.getElementById('ckptConfirm').checked;
  const authFields = confirmAuthorize ? await prepareWebAuthChallenge(rawText) : {};
  const transcriptFilePath = document.getElementById('ckptTranscriptPath').value.trim() || undefined;
  const cloudProvider = document.getElementById('ckptCloudHost').value.trim() || undefined;
  const upload = await readOptionalUpload('ckptUploadFile');
  const res = await api('/api/ingest', {
    projectId, threadId, streamId, rawText, expectedBaseAnchorId, workspaceDir, ...authFields,
    transcriptFilePath, cloudProvider, ...upload
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
  const transcriptFilePath = document.getElementById('forkTranscriptPath').value.trim() || undefined;
  const cloudProvider = document.getElementById('forkCloudHost').value.trim() || undefined;
  const res = await api('/api/fork', {
    projectId, threadId, originStreamId, workerStreamId, workspaceDir, taskBrief,
    transcriptFilePath, cloudProvider
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

  const pendingChallenges = new Map<
    string,
    { content_sha256: string; purpose: 'authorize_instruction'; expires_at: number }
  >();

  const consumeWebChallengeProof = (
    svc: ThreadPassService,
    body: Record<string, unknown>,
    rawText: string,
  ) => {
    const challengeId =
      typeof body.confirmChallengeId === 'string' && body.confirmChallengeId.trim()
        ? body.confirmChallengeId.trim()
        : undefined;
    if (challengeId) {
      const entry = pendingChallenges.get(challengeId);
      pendingChallenges.delete(challengeId);
      if (!entry || entry.expires_at < Date.now()) {
        throw new Error('Human confirmation challenge is missing, already used, or expired');
      }
      const actualSha = sha256Hex(normalizeConfirmationText(rawText));
      if (entry.content_sha256 !== actualSha) {
        throw new Error('Human confirmation challenge content SHA-256 mismatch');
      }
      if (
        typeof body.confirmContentSha256 === 'string' &&
        body.confirmContentSha256.trim() !== actualSha
      ) {
        throw new Error('Human confirmation confirmContentSha256 mismatch');
      }
      return svc.issueLocalConfirmationProof(rawText, entry.purpose, 'thp_web');
    }
    if (body.confirmAuthorize) {
      throw new Error(
        'confirmAuthorize requires a server-issued confirmChallengeId from /api/confirm/prepare bound to the reviewed content hash',
      );
    }
    return undefined;
  };

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
              projectId:
                typeof body.projectId === 'string' && body.projectId.trim()
                  ? body.projectId.trim()
                  : 'threadpass-bootstrap',
              threadId: typeof body.threadId === 'string' && body.threadId.trim() ? body.threadId.trim() : undefined,
              workspaceDir:
                typeof body.workspaceDir === 'string' && body.workspaceDir.trim()
                  ? body.workspaceDir.trim()
                  : undefined,
              transcriptFilePath:
                typeof body.transcriptFilePath === 'string' && body.transcriptFilePath.trim()
                  ? body.transcriptFilePath.trim()
                  : undefined,
              machineHostname:
                typeof body.machineHostname === 'string' && body.machineHostname.trim()
                  ? body.machineHostname.trim()
                  : undefined,
              cloudProvider:
                typeof body.cloudProvider === 'string' && body.cloudProvider.trim()
                  ? body.cloudProvider.trim()
                  : undefined,
              remoteUri:
                typeof body.remoteUri === 'string' && body.remoteUri.trim()
                  ? body.remoteUri.trim()
                  : undefined,
              hostSessionId:
                typeof body.hostSessionId === 'string' && body.hostSessionId.trim()
                  ? body.hostSessionId.trim()
                  : undefined,
              uploadedTranscriptText:
                typeof body.uploadedTranscriptText === 'string' ? body.uploadedTranscriptText : undefined,
              uploadedTranscriptFilename:
                typeof body.uploadedTranscriptFilename === 'string' && body.uploadedTranscriptFilename.trim()
                  ? body.uploadedTranscriptFilename.trim()
                  : undefined,
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/status') {
          sendJson(
            res,
            200,
            svc.getStatus(reqBodyStr(body, 'projectId'), reqBodyStr(body, 'threadId')),
          );
          return;
        }
        if (reqUrl.pathname === '/api/prompt/adopt-extract') {
          sendJson(res, 200, {
            prompt: svc.getAdoptExtractPrompt(
              typeof body.sourceLabel === 'string' && body.sourceLabel.trim()
                ? body.sourceLabel.trim()
                : undefined,
            ),
          });
          return;
        }
        if (reqUrl.pathname === '/api/prompt/checkpoint') {
          sendJson(
            res,
            200,
            svc.getCheckpointPrompt(
              reqBodyStr(body, 'projectId'),
              reqBodyStr(body, 'threadId'),
              typeof body.streamId === 'string' && body.streamId.trim() ? body.streamId.trim() : undefined,
            ),
          );
          return;
        }
        if (reqUrl.pathname === '/api/confirm/prepare') {
          const contentText = String(body.contentText ?? '');
          if (!contentText.trim()) {
            throw new Error('confirm/prepare requires non-empty contentText');
          }
          const purpose =
            body.purpose === 'authorize_instruction' || body.purpose === undefined
              ? 'authorize_instruction'
              : null;
          if (!purpose) {
            throw new Error('confirm/prepare only supports purpose=authorize_instruction');
          }
          const now = Date.now();
          for (const [k, v] of pendingChallenges.entries()) {
            if (v.expires_at < now) pendingChallenges.delete(k);
          }
          const normalized = normalizeConfirmationText(contentText);
          const contentSha = sha256Hex(normalized);
          const challengeId = `chk_${newHighEntropyToken(16)}`;
          pendingChallenges.set(challengeId, {
            content_sha256: contentSha,
            purpose,
            expires_at: now + 5 * 60 * 1000,
          });
          sendJson(res, 200, {
            challenge_id: challengeId,
            content_sha256: contentSha,
            review_code: contentSha.slice(0, 8),
            byte_length: Buffer.byteLength(normalized, 'utf8'),
            preview: normalized.length > 240 ? `${normalized.slice(0, 240)}...` : normalized,
          });
          return;
        }
        if (reqUrl.pathname === '/api/ingest') {
          const rawText = String(body.rawText ?? '');
          const proof = consumeWebChallengeProof(svc, body, rawText);
          sendJson(
            res,
            200,
            svc.ingestReply({
              projectId: reqBodyStr(body, 'projectId'),
              threadId: reqBodyStr(body, 'threadId'),
              streamId: reqBodyStr(body, 'streamId'),
              rawText,
              expectedBaseAnchorId:
                typeof body.expectedBaseAnchorId === 'string' && body.expectedBaseAnchorId.trim()
                  ? body.expectedBaseAnchorId.trim()
                  : undefined,
              confirmationProof: proof,
              ackBatchId: typeof body.ackBatchId === 'string' && body.ackBatchId.trim() ? body.ackBatchId.trim() : undefined,
              ackMessageIds: Array.isArray(body.ackMessageIds)
                ? body.ackMessageIds.map(String)
                : undefined,
              workspaceDir:
                typeof body.workspaceDir === 'string' && body.workspaceDir.trim()
                  ? body.workspaceDir.trim()
                  : undefined,
              transcriptFilePath:
                typeof body.transcriptFilePath === 'string' && body.transcriptFilePath.trim()
                  ? body.transcriptFilePath.trim()
                  : undefined,
              machineHostname:
                typeof body.machineHostname === 'string' && body.machineHostname.trim()
                  ? body.machineHostname.trim()
                  : undefined,
              cloudProvider:
                typeof body.cloudProvider === 'string' && body.cloudProvider.trim()
                  ? body.cloudProvider.trim()
                  : undefined,
              remoteUri:
                typeof body.remoteUri === 'string' && body.remoteUri.trim()
                  ? body.remoteUri.trim()
                  : undefined,
              hostSessionId:
                typeof body.hostSessionId === 'string' && body.hostSessionId.trim()
                  ? body.hostSessionId.trim()
                  : undefined,
              uploadedTranscriptText:
                typeof body.uploadedTranscriptText === 'string' ? body.uploadedTranscriptText : undefined,
              uploadedTranscriptFilename:
                typeof body.uploadedTranscriptFilename === 'string' && body.uploadedTranscriptFilename.trim()
                  ? body.uploadedTranscriptFilename.trim()
                  : undefined,
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/mailbox/send') {
          sendJson(
            res,
            200,
            svc.sendMailboxMessage({
              projectId: reqBodyStr(body, 'projectId'),
              threadId: reqBodyStr(body, 'threadId'),
              fromStreamId: reqBodyStr(body, 'fromStreamId'),
              targetStreamId: reqBodyStr(body, 'targetStreamId'),
              text: String(body.text ?? ''),
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/adopt') {
          const rawText = String(body.rawText ?? '');
          const proof = consumeWebChallengeProof(svc, body, rawText);
          sendJson(
            res,
            200,
            svc.adoptExternal({
              projectId: reqBodyStr(body, 'projectId'),
              threadId: reqBodyStr(body, 'threadId'),
              sourceLabel:
                typeof body.sourceLabel === 'string' && body.sourceLabel.trim()
                  ? body.sourceLabel.trim()
                  : 'web_adopt',
              rawText,
              confirmationProof: proof,
              allowForeignCanonical: Boolean(body.allowForeignCanonical),
              transcriptFilePath:
                typeof body.transcriptFilePath === 'string' && body.transcriptFilePath.trim()
                  ? body.transcriptFilePath.trim()
                  : undefined,
              machineHostname:
                typeof body.machineHostname === 'string' && body.machineHostname.trim()
                  ? body.machineHostname.trim()
                  : undefined,
              cloudProvider:
                typeof body.cloudProvider === 'string' && body.cloudProvider.trim()
                  ? body.cloudProvider.trim()
                  : undefined,
              remoteUri:
                typeof body.remoteUri === 'string' && body.remoteUri.trim()
                  ? body.remoteUri.trim()
                  : undefined,
              hostSessionId:
                typeof body.hostSessionId === 'string' && body.hostSessionId.trim()
                  ? body.hostSessionId.trim()
                  : undefined,
              uploadedTranscriptText:
                typeof body.uploadedTranscriptText === 'string' ? body.uploadedTranscriptText : undefined,
              uploadedTranscriptFilename:
                typeof body.uploadedTranscriptFilename === 'string' && body.uploadedTranscriptFilename.trim()
                  ? body.uploadedTranscriptFilename.trim()
                  : undefined,
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/reconcile/pack') {
          let perTurn = 24000;
          if (body.perTurnLimit !== undefined) {
            if (typeof body.perTurnLimit !== 'number' || !Number.isFinite(body.perTurnLimit) || body.perTurnLimit < 700) {
              throw new Error('perTurnLimit must be a number >= 700');
            }
            perTurn = body.perTurnLimit;
          }
          sendJson(
            res,
            200,
            svc.packReconcile({
              projectId: reqBodyStr(body, 'projectId'),
              threadId: reqBodyStr(body, 'threadId'),
              sourceStreamIds: Array.isArray(body.sourceStreamIds)
                ? body.sourceStreamIds.map(String)
                : undefined,
              mainGoal: typeof body.mainGoal === 'string' && body.mainGoal.trim() ? body.mainGoal.trim() : undefined,
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
          const proof = consumeWebChallengeProof(svc, body, receiptRawText);
          sendJson(
            res,
            200,
            svc.commitReconcile({
              projectId: reqBodyStr(body, 'projectId'),
              threadId: reqBodyStr(body, 'threadId'),
              sourceStreamIds: Array.isArray(body.sourceStreamIds)
                ? body.sourceStreamIds.map(String)
                : [],
              receiptRawText,
              packEnvelopeId:
                typeof body.packEnvelopeId === 'string' && body.packEnvelopeId.trim()
                  ? body.packEnvelopeId.trim()
                  : undefined,
              confirmedConflictTopics: Array.isArray(body.confirmedConflictTopics)
                ? body.confirmedConflictTopics.map(String)
                : undefined,
              confirmAllConflicts:
                typeof body.confirmAllConflicts === 'boolean' ? body.confirmAllConflicts : undefined,
              confirmationProof: proof,
              transcriptFilePath:
                typeof body.transcriptFilePath === 'string' && body.transcriptFilePath.trim()
                  ? body.transcriptFilePath.trim()
                  : undefined,
              machineHostname:
                typeof body.machineHostname === 'string' && body.machineHostname.trim()
                  ? body.machineHostname.trim()
                  : undefined,
              cloudProvider:
                typeof body.cloudProvider === 'string' && body.cloudProvider.trim()
                  ? body.cloudProvider.trim()
                  : undefined,
              remoteUri:
                typeof body.remoteUri === 'string' && body.remoteUri.trim()
                  ? body.remoteUri.trim()
                  : undefined,
              uploadedTranscriptText:
                typeof body.uploadedTranscriptText === 'string' ? body.uploadedTranscriptText : undefined,
              uploadedTranscriptFilename:
                typeof body.uploadedTranscriptFilename === 'string' && body.uploadedTranscriptFilename.trim()
                  ? body.uploadedTranscriptFilename.trim()
                  : undefined,
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/handoff/pack') {
          let perTurn = 24000;
          if (body.perTurnLimit !== undefined) {
            if (typeof body.perTurnLimit !== 'number' || !Number.isFinite(body.perTurnLimit) || body.perTurnLimit < 700) {
              throw new Error('perTurnLimit must be a number >= 700');
            }
            perTurn = body.perTurnLimit;
          }
          sendJson(
            res,
            200,
            svc.packHandoff({
              projectId: reqBodyStr(body, 'projectId'),
              threadId: reqBodyStr(body, 'threadId'),
              streamId: typeof body.streamId === 'string' && body.streamId.trim() ? body.streamId.trim() : undefined,
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
              projectId: reqBodyStr(body, 'projectId'),
              threadId: reqBodyStr(body, 'threadId'),
              originStreamId:
                typeof body.originStreamId === 'string' && body.originStreamId.trim()
                  ? body.originStreamId.trim()
                  : undefined,
              sourceAnchorId:
                typeof body.sourceAnchorId === 'string' && body.sourceAnchorId.trim()
                  ? body.sourceAnchorId.trim()
                  : undefined,
              workerStreamId:
                typeof body.workerStreamId === 'string' && body.workerStreamId.trim()
                  ? body.workerStreamId.trim()
                  : undefined,
              workspaceDir:
                typeof body.workspaceDir === 'string' && body.workspaceDir.trim()
                  ? body.workspaceDir.trim()
                  : undefined,
              taskBrief:
                typeof body.taskBrief === 'string' && body.taskBrief.trim()
                  ? body.taskBrief.trim()
                  : 'Execute scoped subtask',
              requireDisasterRedundant: Boolean(body.requireDisasterRedundant),
              disasterVerified: Boolean(body.disasterVerified),
              transcriptFilePath:
                typeof body.transcriptFilePath === 'string' && body.transcriptFilePath.trim()
                  ? body.transcriptFilePath.trim()
                  : undefined,
              machineHostname:
                typeof body.machineHostname === 'string' && body.machineHostname.trim()
                  ? body.machineHostname.trim()
                  : undefined,
              cloudProvider:
                typeof body.cloudProvider === 'string' && body.cloudProvider.trim()
                  ? body.cloudProvider.trim()
                  : undefined,
              remoteUri:
                typeof body.remoteUri === 'string' && body.remoteUri.trim()
                  ? body.remoteUri.trim()
                  : undefined,
              hostSessionId:
                typeof body.hostSessionId === 'string' && body.hostSessionId.trim()
                  ? body.hostSessionId.trim()
                  : undefined,
              uploadedTranscriptText:
                typeof body.uploadedTranscriptText === 'string' ? body.uploadedTranscriptText : undefined,
              uploadedTranscriptFilename:
                typeof body.uploadedTranscriptFilename === 'string' && body.uploadedTranscriptFilename.trim()
                  ? body.uploadedTranscriptFilename.trim()
                  : undefined,
            }),
          );
          return;
        }
        if (reqUrl.pathname === '/api/fsck') {
          sendJson(
            res,
            200,
            svc.runFsck(reqBodyStr(body, 'projectId'), reqBodyStr(body, 'threadId'), {
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
      server.closeAllConnections?.();
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
