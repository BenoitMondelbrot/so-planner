const API = {
  plans: () => fetch("/plans").then(r => r.json()),
  createPlan: (name) => fetch("/plans", {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify({name})
  }).then(r => r.json()),
  runGreedy: (id) => fetch(`/plans/${id}/run-greedy`, {method:"POST"}).then(r => r.json()),
  heatmap: (id) => fetch(`/plans/${id}/heatmap`).then(r => r.json()),
  optimizeFrom: (id) => fetch(`/optimize/from-plan/${id}`, {method:"POST"}).then(r => r.json()),
};

const el = (id) => document.getElementById(id);
const toast = (msg, ok=false) => {
  const t = el("toast");
  t.style.display = "block";
  t.style.borderColor = ok ? "#14532d" : "#7f1d1d";
  t.style.color = ok ? "#86efac" : "#fecaca";
  t.textContent = msg;
  setTimeout(()=> t.style.display="none", 3000);
};

let state = {
  plans: [],
  selectedId: null,
  heatmap: null,
};

function fmtPlan(p) {
  const dt = new Date(p.created_at);
  const s = dt.toISOString().slice(0,19).replace("T"," ");
  return `#${p.id} • ${p.name} • ${p.origin} • ${p.status} • ${s}`;
}

function renderPlans() {
  const box = el("plans"); box.innerHTML = "";
  state.plans.forEach(p => {
    const div = document.createElement("div");
    div.className = "plan-item" + (p.id === state.selectedId ? " active" : "");
    div.onclick = () => { state.selectedId = p.id; renderPlans(); loadHeatmap(); };
    const left = document.createElement("div");
    left.style.display = "flex";
    left.style.flexDirection = "column";
    left.style.gap = "2px";
    const name = document.createElement("div");
    name.textContent = p.name;
    const meta = document.createElement("div");
    meta.className = "muted";
    meta.textContent = `#${p.id} • ${p.origin}`;
    left.appendChild(name); left.appendChild(meta);

    const badge = document.createElement("div");
    badge.className = "badge " + (p.status || "ready");
    badge.textContent = p.status;

    div.appendChild(left); div.appendChild(badge);
    box.appendChild(div);
  });
  el("plansCount").textContent = `${state.plans.length} вер.`;
  const sel = state.plans.find(p => p.id === state.selectedId);
  el("selInfo").textContent = sel ? fmtPlan(sel) : "— не выбрана —";
}

async function reloadPlans() {
  const data = await API.plans();
  state.plans = data;
  if (!state.selectedId && data.length) {
    state.selectedId = data[0].id;
  }
  renderPlans();
}

function colorForUtil(u) {
  if (u <= 0.8) return "#16a34a";  // зелёный
  if (u <= 1.0) return "#f59e0b";  // янтарь
  return "#ef4444";                // красный
}

function drawHeatmap(data) {
  const root = el("heatmap");
  root.innerHTML = "";
  if (!data || !data.machines || data.machines.length === 0) {
    root.innerHTML = `<div class="muted" style="padding:12px;">Нет данных по heatmap</div>`;
    return;
  }
  const machines = data.machines;
  const dates = data.dates;
  const util = data.util; // ключ "machine|date" → число

  // таблица
  const table = document.createElement("table");
  table.style.borderCollapse = "collapse";
  const thead = document.createElement("thead");
  const trh = document.createElement("tr");
  const th0 = document.createElement("th");
  th0.textContent = "Machine \\ Date";
  th0.style.position = "sticky"; th0.style.left = "0";
  th0.style.background = "#0b1220"; th0.style.zIndex = "2";
  th0.style.padding = "6px 8px"; th0.style.borderRight = "1px solid #1f2937";
  trh.appendChild(th0);

  dates.forEach(d => {
    const th = document.createElement("th");
    th.textContent = d;
    th.style.padding = "6px 8px";
    th.style.borderBottom = "1px solid #1f2937";
    th.style.borderLeft = "1px solid #111827";
    th.style.fontWeight = "500";
    trh.appendChild(th);
  });
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  machines.forEach(m => {
    const tr = document.createElement("tr");
    const th = document.createElement("th");
    th.textContent = m;
    th.style.position = "sticky"; th.style.left = "0";
    th.style.background = "#0b1220"; th.style.zIndex = "1";
    th.style.padding = "4px 8px"; th.style.borderRight = "1px solid #1f2937";
    th.style.textAlign = "left";
    tr.appendChild(th);

    dates.forEach(d => {
      const key = `${m}|${d}`;
      const u = Number(util[key] ?? 0);
      const td = document.createElement("td");
      td.title = `${m} • ${d} • util=${u.toFixed(2)}`;
      td.style.width = "28px";
      td.style.height = "22px";
      td.style.borderLeft = "1px solid #0f172a";
      td.style.borderBottom = "1px solid #0f172a";
      td.style.background = colorForUtil(u);
      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  root.appendChild(table);
}

async function loadHeatmap() {
  const id = state.selectedId;
  if (!id) { drawHeatmap(null); return; }
  const res = await API.heatmap(id);
  drawHeatmap(res);
}

function setBusy(disabled) {
  ["btnCreate","btnRefresh","btnRunGreedy","btnRunMilp"].forEach(id=>{
    const b = el(id); if (b) b.disabled = disabled;
  });
}

async function onCreate() {
  const name = el("planName").value.trim();
  if (!name) return toast("Укажите имя версии");
  setBusy(true);
  try {
    const res = await API.createPlan(name);
    toast(`Создана версия #${res.id}`, true);
    await reloadPlans();
    state.selectedId = res.id;
    renderPlans();
  } catch (e) {
    console.error(e);
    toast("Ошибка создания версии");
  } finally {
    setBusy(false);
  }
}

async function onRunGreedy() {
  if (!state.selectedId) return toast("Сначала выберите версию");
  setBusy(true);
  try {
    const res = await API.runGreedy(state.selectedId);
    toast(`Greedy: ok, операций=${res.ops}, дней=${res.days}`, true);
    await loadHeatmap();
    await reloadPlans();
  } catch (e) {
    console.error(e);
    toast("Ошибка запуска Greedy");
  } finally {
    setBusy(false);
  }
}

async function onRunMilp() {
  if (!state.selectedId) return toast("Сначала выберите версию");
  setBusy(true);
  try {
    const res = await API.optimizeFrom(state.selectedId);
    toast(`MILP создал версию #${res.plan_id}`, true);
    await reloadPlans();
  } catch (e) {
    console.error(e);
    toast("Ошибка MILP");
  } finally {
    setBusy(false);
  }
}

// init
window.addEventListener("DOMContentLoaded", async () => {
  el("btnCreate").onclick = onCreate;
  el("btnRefresh").onclick = reloadPlans;
  el("btnRunGreedy").onclick = onRunGreedy;
  el("btnRunMilp").onclick = onRunMilp;
  await reloadPlans();
  await loadHeatmap();
});
