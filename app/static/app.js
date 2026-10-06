/* 机载以太网 TAS 门控调度审计前端（原生 JS）。 */
"use strict";

const $ = (sel) => document.querySelector(sel);
const flowsBody = $("#flows-body");
const gatesBody = $("#gates-body");
const verdictCard = $("#verdict-card");
const staleNote = $("#stale-note");
const timelineCard = $("#timeline-card");
const waitPanel = $("#waitchain-panel");
const instanceSelect = $("#instance-select");
const waitBody = $("#waitchain-body");
const waitError = $("#waitchain-error");
const waitStatus = $("#waitchain-status");
const MAX_TIMELINE_ROWS = 400;

let currentAuditId = null;
let currentInstances = [];

const CATEGORY_LABELS = {
  gate_closed: "门关闭 · 等待下一窗口",
  window_too_short: "窗口剩余不足 · 不启动",
  higher_priority_tx: "更高优先级帧先占出口（非抢占）",
  nonpreemptive_hold: "非抢占占用（含跨周期遗留）",
  same_flow_fifo: "同流 FIFO · 前序实例未完成"
};

/* ---------------- 示例输入 ---------------- */

const EXAMPLES = {
  schedulable: {
    audit_id: "A350-TAS-2026-S01",
    gate_period: 1000,
    flows: [
      { flow_id: "CTRL", priority: 0, period: 1000, transmit_time: 120, deadline: 1000 },
      { flow_id: "A", priority: 1, period: 1000, transmit_time: 200, deadline: 1000 },
      { flow_id: "B", priority: 2, period: 2000, transmit_time: 300, deadline: 2000 }
    ],
    gate_entries: [
      { start: 0, end: 700, priorities: [0, 1, 2] }
    ]
  },
  miss: {
    audit_id: "A350-TAS-2026-M01",
    gate_period: 1000,
    flows: [
      { flow_id: "HI", priority: 0, period: 1000, transmit_time: 600, deadline: 1000 },
      { flow_id: "LO", priority: 1, period: 1000, transmit_time: 300, deadline: 700 }
    ],
    // LO 在 600..900 窗口才能发，600 开始 900 发完，超过 700 截止期；
    // 每周期队列最终排空，故为超期裁决而非不收敛。
    gate_entries: [
      { start: 0, end: 600, priorities: [0] },
      { start: 600, end: 900, priorities: [1] }
    ]
  },
  growth: {
    audit_id: "A350-TAS-2026-G01",
    gate_period: 1000,
    flows: [
      // 每周期需 800us，门控只给 500us，边界队列逐周期增长。
      { flow_id: "FLOOD", priority: 0, period: 1000, transmit_time: 800, deadline: 1000 }
    ],
    gate_entries: [
      { start: 0, end: 500, priorities: [0] }
    ]
  }
};

/* ---------------- 表单构建 ---------------- */

function flowRow(f = {}) {
  const tr = document.createElement("tr");
  tr.className = "flow-row";
  tr.innerHTML = `
    <td><input class="f-id" type="text" value="${f.flow_id ?? ""}"></td>
    <td><input class="f-prio" type="number" min="0" max="7" step="1" value="${f.priority ?? 0}"></td>
    <td><input class="f-period" type="number" min="1" step="1" value="${f.period ?? 1000}"></td>
    <td><input class="f-tx" type="number" min="1" step="1" value="${f.transmit_time ?? 100}"></td>
    <td><input class="f-dl" type="number" min="1" step="1" value="${f.deadline ?? 1000}"></td>
    <td><button type="button" class="btn-del">删除</button></td>`;
  tr.querySelector(".btn-del").addEventListener("click", () => {
    if (flowsBody.children.length > 1) tr.remove();
    invalidateVerdict();
  });
  tr.querySelectorAll("input").forEach((i) => i.addEventListener("input", invalidateVerdict));
  return tr;
}

function gateRow(g = {}) {
  const tr = document.createElement("tr");
  tr.className = "gate-row";
  const boxes = [0, 1, 2, 3, 4, 5, 6, 7].map((p) => `
    <label><input type="checkbox" class="g-prio" value="${p}"
      ${(g.priorities ?? []).includes(p) ? "checked" : ""}>${p}</label>`).join("");
  tr.innerHTML = `
    <td><input class="g-start" type="number" min="0" step="1" value="${g.start ?? 0}"></td>
    <td><input class="g-end" type="number" min="1" step="1" value="${g.end ?? 100}"></td>
    <td><div class="prio-checkboxes">${boxes}</div></td>
    <td><button type="button" class="btn-del">删除</button></td>`;
  tr.querySelector(".btn-del").addEventListener("click", () => {
    if (gatesBody.children.length > 1) tr.remove();
    invalidateVerdict();
  });
  tr.querySelectorAll("input").forEach((i) => i.addEventListener("input", invalidateVerdict));
  return tr;
}

function loadForm(data) {
  $("#audit-id").value = data.audit_id;
  $("#gate-period").value = data.gate_period;
  flowsBody.replaceChildren(...data.flows.map(flowRow));
  gatesBody.replaceChildren(...data.gate_entries.map(gateRow));
  invalidateVerdict();
}

/* ---------------- 收集与提交 ---------------- */

function collectPayload() {
  const flows = [...flowsBody.querySelectorAll(".flow-row")].map((tr) => ({
    flow_id: tr.querySelector(".f-id").value.trim(),
    priority: parseInt(tr.querySelector(".f-prio").value, 10),
    period: parseInt(tr.querySelector(".f-period").value, 10),
    transmit_time: parseInt(tr.querySelector(".f-tx").value, 10),
    deadline: parseInt(tr.querySelector(".f-dl").value, 10)
  }));
  const gate_entries = [...gatesBody.querySelectorAll(".gate-row")].map((tr) => ({
    start: parseInt(tr.querySelector(".g-start").value, 10),
    end: parseInt(tr.querySelector(".g-end").value, 10),
    priorities: [...tr.querySelectorAll(".g-prio:checked")].map((c) => parseInt(c.value, 10))
  }));
  return {
    audit_id: $("#audit-id").value.trim(),
    gate_period: parseInt($("#gate-period").value, 10),
    flows,
    gate_entries
  };
}

function showError(msg) {
  const box = $("#form-error");
  box.hidden = false;
  box.textContent = msg;
}
function clearError() { $("#form-error").hidden = true; }

function invalidateVerdict() {
  // 修改输入即清除旧裁决，避免页面残留过期结论。
  verdictCard.hidden = true;
  timelineCard.hidden = true;
  waitPanel.hidden = true;
  instanceSelect.replaceChildren();
  waitBody.replaceChildren();
  waitError.hidden = true;
  waitError.textContent = "";
  waitStatus.textContent = "";
  currentAuditId = null;
  currentInstances = [];
  staleNote.hidden = false;
  clearError();
}

async function submitSchedule() {
  clearError();
  let payload;
  try {
    payload = collectPayload();
  } catch (err) {
    showError("输入解析失败：" + err.message);
    invalidateVerdict();
    return;
  }
  let resp;
  try {
    resp = await fetch("/api/submit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
  } catch (err) {
    invalidateVerdict();
    showError("提交失败（网络错误）：" + err.message);
    return;
  }
  const data = await resp.json();
  if (!resp.ok) {
    invalidateVerdict();
    if (resp.status === 409) {
      showError(`冲突（409）：${data.message}。原冻结裁决保持不变。`);
      if (data.existing_decision) renderDecision(data.existing_decision, true);
    } else {
      showError(`提交失败（${resp.status}）：${data.message}`);
    }
    return;
  }
  staleNote.hidden = true;
  renderDecision(data, false);
}

async function loadFrozen() {
  clearError();
  const auditId = $("#audit-id").value.trim();
  if (!auditId) { showError("请先填写审计标识"); return; }
  const resp = await fetch(`/api/decisions/${encodeURIComponent(auditId)}`);
  const data = await resp.json();
  if (!resp.ok) {
    invalidateVerdict();
    showError(`读取失败（${resp.status}）：${data.message}`);
    return;
  }
  staleNote.hidden = true;
  renderDecision(data, false);
}

/* ---------------- 裁决渲染 ---------------- */

function fmtTime(v) { return v === null || v === undefined ? "—" : `${v} μs`; }

function renderDecision(data, readonlyExisting) {
  verdictCard.hidden = false;
  $("#v-audit").textContent = data.audit_id;
  $("#v-hash").textContent = data.content_hash.slice(0, 16) + "…";
  $("#v-frozen-at").textContent = new Date(data.frozen_at * 1000).toLocaleString();
  const d = data.decision;
  const badge = $("#verdict-badge");
  badge.textContent = d.verdict + (readonlyExisting ? "（原冻结裁决）" : "（已冻结）");
  badge.className = "badge " + d.verdict;
  $("#verdict-summary").textContent = d.summary;

  const body = $("#verdict-body");
  body.className = "evidence";
  if (d.verdict === "SCHEDULABLE") body.innerHTML = renderSchedulable(d);
  else if (d.verdict === "DEADLINE_MISS") body.innerHTML = renderMiss(d);
  else body.innerHTML = renderGrowth(d);

  renderTimeline(d.timeline, d.verdict);
  setupInstancePicker(data.audit_id, d);
}

/* ---------------- 实例选择与等待链 ---------------- */

function instanceLabel(it) {
  const state = it.unsent ? "未发送" :
    (it.transmit_end !== null && it.transmit_end !== undefined ? `发于${it.transmit_start}μs` : "发送中");
  return `${it.frame}（P${it.priority}，释放 ${it.release}μs，截止 ${it.deadline}μs，${state}）`;
}

function setupInstancePicker(auditId, d) {
  currentAuditId = auditId;
  currentInstances = (d.instances && d.instances.instances) || [];
  waitPanel.hidden = false;
  waitBody.replaceChildren();
  waitError.hidden = true;
  waitStatus.textContent = "";
  instanceSelect.replaceChildren();

  if (!currentInstances.length) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "（裁决中无可追溯实例）";
    instanceSelect.appendChild(opt);
    instanceSelect.disabled = true;
    $("#waitchain-btn").disabled = true;
    return;
  }
  instanceSelect.disabled = false;
  $("#waitchain-btn").disabled = false;

  // 默认选中首个超期且未开始发送的帧，便于直接复核关键证据。
  let preferred = null;
  if (d.verdict === "DEADLINE_MISS") preferred = d.first_overdue_frame.frame;
  currentInstances.forEach((it, idx) => {
    const opt = document.createElement("option");
    opt.value = it.frame;
    opt.textContent = instanceLabel(it) + (it.queryable ? "" : " · 证据窗口外");
    if (!it.queryable) { opt.disabled = true; }
    instanceSelect.appendChild(opt);
    if (it.frame === preferred) instanceSelect.value = it.frame;
  });
  if (preferred) instanceSelect.value = preferred;

  // DEADLINE_MISS 已内嵌首个超期帧等待链，直接展示；其余等用户选择。
  if (d.verdict === "DEADLINE_MISS" && d.first_overdue_frame.wait_chain) {
    renderWaitChain(d.first_overdue_frame.wait_chain, true);
  } else {
    waitStatus.textContent = "请选择实例后读取其连续等待区间。";
  }
}

async function loadWaitChain() {
  waitError.hidden = true;
  waitStatus.textContent = "读取中…";
  const frame = instanceSelect.value;
  if (!currentAuditId || !frame) {
    showWaitError("请先选择一个已出现的流实例。");
    return;
  }
  const url = `/api/decisions/${encodeURIComponent(currentAuditId)}/wait-chain?frame=${encodeURIComponent(frame)}`;
  let resp;
  try {
    resp = await fetch(url);
  } catch (err) {
    showWaitError("等待链读取失败（网络错误）：" + err.message);
    return;
  }
  const data = await resp.json();
  if (!resp.ok) {
    // 实例不存在 / 不可追溯：明确报错，不清空也不改写冻结裁决。
    showWaitError(`等待链不可用（${resp.status} ${data.error}）：${data.message}`);
    return;
  }
  waitStatus.textContent = "";
  renderWaitChain(data.wait_chain, false);
}

function showWaitError(msg) {
  waitStatus.textContent = "";
  waitBody.replaceChildren();
  waitError.hidden = false;
  waitError.textContent = msg;
}

function renderWaitChain(ch, embedded) {
  waitError.hidden = true;
  const fr = ch.frame;
  const cov = ch.coverage;
  let html = `<div class="wc-head">
    <h4>实例 ${fr.frame}（流 ${fr.flow_id}，P${fr.priority}，序号 ${fr.seq}）</h4>
    <p class="meta">
      证据覆盖 [${cov[0]}, ${cov[1]}) μs · 总等待 ${ch.total_wait_us} μs ·
      释放 ${fr.release} / 入队 ${fr.enqueue} / 开始 ${fmtTime(fr.transmit_start)} /
      完成 ${fmtTime(fr.transmit_end)} / 截止 ${fr.deadline} μs ·
      ${fr.unsent ? '<b class="tx-late">截止期前未发送</b>' : "已开始发送"}
      ${embedded ? "（内嵌于冻结裁决）" : "（实时接口读取，冻结结果不变）"}
    </p>
    <p class="meta">${ch.coverage_note}</p></div>`;

  if (!ch.intervals.length) {
    html += `<p class="meta">该实例释放即开始发送，等待区间为空。</p>`;
  } else {
    html += `<h4>按时间连续、无重叠的等待区间</h4>
      <table class="wc-table"><thead><tr>
        <th>#</th><th>区间 [起,止) μs</th><th>时长</th><th>队列长度</th>
        <th>队内位置</th><th>本优先级门</th><th>可发送最高优先级</th>
        <th>阻塞类别</th><th>关联帧（流 / 释放时刻）</th><th>归因说明</th>
      </tr></thead><tbody>`;
    for (const iv of ch.intervals) {
      const top = iv.top_ready_priority === null ? "无（无帧可启动）" : `P${iv.top_ready_priority}`;
      const gate = iv.gate_state === "open"
        ? '<span class="tx-ok">开放</span>' : '<span class="tx-late">关闭</span>';
      const src = iv.source_frame
        ? `${iv.source_frame}（${iv.source_flow} / 释放 ${iv.source_release}μs）` : "—";
      html += `<tr>
        <td>${iv.index}</td>
        <td class="queue-cell">[${iv.from}, ${iv.to})</td>
        <td>${iv.duration_us}</td>
        <td>${iv.queue_length}</td>
        <td>${iv.queue_position ?? "—"}</td>
        <td>${gate}</td>
        <td>${top}</td>
        <td><span class="cat-badge cat-${iv.blocking_category}">${CATEGORY_LABELS[iv.blocking_category] || iv.blocking_category}</span></td>
        <td class="queue-cell">${src}</td>
        <td>${iv.detail}</td></tr>`;
    }
    html += `</tbody></table>`;
    html += `<p class="meta">区间首尾相接、无重叠；各类等待合计：` +
      Object.entries(ch.duration_by_category)
        .map(([k, v]) => `${CATEGORY_LABELS[k] || k} ${v}μs`).join("，") + `。</p>`;
  }
  waitBody.innerHTML = html;
}

function evidenceTable(rows) {
  const head = `
    <tr><th>帧</th><th>释放</th><th>入队</th><th>开始发送</th><th>发送完成</th>
    <th>截止期</th><th>结果</th></tr>`;
  const body = rows.map((r) => `
    <tr>
      <td>${r.frame}</td><td>${fmtTime(r.release)}</td><td>${fmtTime(r.enqueue)}</td>
      <td>${fmtTime(r.start)}</td><td>${fmtTime(r.finish)}</td>
      <td>${fmtTime(r.deadline)}</td>
      <td class="${r.on_time ? "tx-ok" : "tx-late"}">${r.on_time ? "按期" : "超期"}</td>
    </tr>`).join("");
  return `<table><thead>${head}</thead><tbody>${body}</tbody></table>`;
}

function renderSchedulable(d) {
  let html = `
    <p class="meta">超周期 H = ${d.hyperperiod} μs；
    证据窗口 [0, ${d.proof_window[1]}) μs；连续两个超周期边界排空：
    ${d.cycle_snapshots.filter((s) => s.drained && s.t > 0).slice(-2)
      .map((s) => `t=${s.t}μs 待发=${s.pending}`).join("，") || "—"}。</p>`;
  for (const [flowId, rows] of Object.entries(d.flow_evidence)) {
    const allOnTime = rows.every((r) => r.on_time) && rows.length > 0;
    html += `<h4>流 ${flowId} — ${rows.length} 个周期实例，${allOnTime ? "全部按期 ✓" : "存在超期 ✗"}</h4>`;
    html += evidenceTable(rows);
  }
  return html;
}

function renderMiss(d) {
  const f = d.first_overdue_frame;
  let html = `<h4>首个超期帧</h4>
    <table><tbody>
      <tr><th>帧</th><td>${f.frame}（流 ${f.flow_id}，优先级 ${f.priority}，序号 ${f.seq}）</td></tr>
      <tr><th>释放时间</th><td>${fmtTime(f.release)}</td></tr>
      <tr><th>入队时间</th><td>${fmtTime(f.enqueue)}</td></tr>
      <tr><th>开始发送</th><td>${fmtTime(f.transmit_start)}</td></tr>
      <tr><th>发送完成</th><td>${fmtTime(f.transmit_end)}</td></tr>
      <tr><th>截止期</th><td>${fmtTime(f.deadline)}（超期时状态：${f.state_when_overdue}）</td></tr>
      <tr><th>说明</th><td>${f.note}</td></tr>
    </tbody></table>
    <h4>阻塞来源（合并视图；逐时隙连续归因见下方“流实例等待链复核”）</h4>`;
  if (!f.blockers.length) html += `<p class="meta">无更细粒度阻塞记录。</p>`;
  for (const b of f.blockers) {
    html += `<div class="blocker"><b>${b.type}</b> [${b.from}, ${b.to}) μs：${b.detail}` +
      (b.source_frame ? `（来源：${b.source_frame}）` : "") + `</div>`;
  }
  html += `<h4>门控周期边界队列样本</h4><table><thead><tr><th>边界时刻</th><th>待发帧总数</th></tr></thead><tbody>`;
  html += d.boundary_samples.map((s) => `<tr><td>${s.t} μs</td><td>${s.pending}</td></tr>`).join("");
  html += `</tbody></table>`;
  return html;
}

function renderGrowth(d) {
  let html = `<p class="meta">系统拒绝：${d.summary}。连续增长证据如下，模拟未被截断为“通过”。</p>
    <h4>门控周期边界待发帧总数（连续增长链）</h4>`;
  for (const s of d.growth_chain) {
    html += `<div class="growth">t = ${s.t} μs：待发帧 = ${s.pending}（较上一边界增长）</div>`;
  }
  html += `<h4>全部边界样本</h4><table><thead><tr><th>边界时刻</th><th>待发帧总数</th></tr></thead><tbody>`;
  html += d.queue_growth.map((s) => `<tr><td>${s.t} μs</td><td>${s.pending}</td></tr>`).join("");
  html += `</tbody></table>`;
  if (d.late_frames.length) {
    html += `<h4>已超期帧</h4><table><thead><tr><th>帧</th><th>释放</th><th>截止期</th><th>开始发送</th></tr></thead><tbody>`;
    html += d.late_frames.map((f) => `<tr><td>${f.frame}</td><td>${fmtTime(f.release)}</td>
      <td>${fmtTime(f.deadline)}</td><td>${fmtTime(f.transmit_start)}</td></tr>`).join("");
    html += `</tbody></table>`;
  }
  return html;
}

/* ---------------- 逐时隙时间线 ---------------- */

function renderTimeline(timeline, verdict) {
  timelineCard.hidden = false;
  const shown = timeline.slice(0, MAX_TIMELINE_ROWS);
  const rows = shown.map((seg) => {
    const qparts = Object.entries(seg.queues).map(([p, frames]) =>
      `P${p}: [${frames.join(", ") || "空"}]`).join(" ");
    let txCell = `<span class="tx-idle">空闲</span>`;
    if (seg.transmitting) {
      const tx = seg.transmitting;
      const cls = tx.on_time ? "tx-ok" : "tx-late";
      const flag = tx.on_time ? "" : "（发送越过截止期）";
      txCell = `<span class="${cls}">发送 ${tx.frame}（P${tx.priority}，
        ${tx.start}→${tx.finish} μs）${flag}</span>`;
    }
    return `<tr>
      <td>[${seg.start}, ${seg.end})</td>
      <td class="gate-open-box">${seg.gate_open.length ? "P" + seg.gate_open.join(", P") : "全关"}</td>
      <td class="queue-cell">${qparts || "—"}</td>
      <td class="tx">${txCell}</td></tr>`;
  }).join("");
  $("#timeline-body").innerHTML = rows;
  $("#timeline-note").textContent =
    `共 ${timeline.length} 个时隙，最多展示前 ${MAX_TIMELINE_ROWS} 个；裁决：${verdict}。`;
}

/* ---------------- 健康检查与事件绑定 ---------------- */

async function pingHealth() {
  try {
    const r = await fetch("/api/health");
    const d = await r.json();
    const el = $("#health");
    el.textContent = "服务在线 · 已冻结 " + d.frozen_ids.length + " 份裁决";
    el.className = "health ok";
  } catch (err) {
    $("#health").textContent = "服务不可用";
  }
}

$("#add-flow").addEventListener("click", () => {
  flowsBody.appendChild(flowRow());
  invalidateVerdict();
});
$("#add-gate").addEventListener("click", () => {
  gatesBody.appendChild(gateRow());
  invalidateVerdict();
});
$("#submit-btn").addEventListener("click", submitSchedule);
$("#load-btn").addEventListener("click", loadFrozen);
$("#waitchain-btn").addEventListener("click", loadWaitChain);
instanceSelect.addEventListener("change", () => {
  // 切换实例仅清旧链，等待用户显式读取（不改动冻结裁决）。
  waitBody.replaceChildren();
  waitError.hidden = true;
  waitStatus.textContent = "已选择实例，点击“读取等待链”。";
});
$("#example-schedulable").addEventListener("click", () => loadForm(EXAMPLES.schedulable));
$("#example-miss").addEventListener("click", () => loadForm(EXAMPLES.miss));
$("#example-growth").addEventListener("click", () => loadForm(EXAMPLES.growth));
$("#audit-id").addEventListener("input", invalidateVerdict);
$("#gate-period").addEventListener("input", invalidateVerdict);

/* 初始表单 */
flowsBody.appendChild(flowRow({ flow_id: "CTRL", priority: 0, period: 1000, transmit_time: 100, deadline: 1000 }));
gatesBody.appendChild(gateRow({ start: 0, end: 500, priorities: [0] }));
pingHealth();
setInterval(pingHealth, 5000);
