// ==============================
// main.js - Netunna REDE Splitter v5.3
// ==============================

const HEARTBEAT_MS = 3 * 60 * 1000; // 3 minutos

// ------------------------------
// RELÓGIO EM HORÁRIO DE BRASÍLIA
// ------------------------------
function atualizarRelogio() {
  const agora = new Date();
  const opcoes = { timeZone: 'America/Sao_Paulo', hour12: false };
  const dataHora = agora.toLocaleString('pt-BR', opcoes);
  document.getElementById("currentTime").textContent = dataHora;
}
atualizarRelogio();
setInterval(atualizarRelogio, 60000);

// ------------------------------
// FUNÇÕES AUXILIARES
// ------------------------------
function parseBRDateTime(s) {
  if (!s) return null;
  const m = s.match(/^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return null;
  const [_, dd, mm, yyyy, HH, MM, SS] = m;
  return new Date(yyyy, mm - 1, dd, HH, MM, SS);
}

function formatRelativo(msDiff) {
  if (!msDiff) return "—";
  const s = Math.floor(msDiff / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  const r = s % 60;
  return `${m}m ${r}s`;
}

// ------------------------------
// STATUS DO SISTEMA
// ------------------------------
function inferStatusFromScan(scan) {
  let lastTs = null;
  if (scan?.input?.length) {
    scan.input.forEach(item => {
      const dt = parseBRDateTime(item.data_hora);
      if (dt && (!lastTs || dt > lastTs)) lastTs = dt;
    });
  }
  const now = new Date();
  const delta = lastTs ? now - lastTs : null;
  const ativo = delta != null && delta <= HEARTBEAT_MS;
  return { ativo, delta };
}

function pintarStatus({ ativo, delta = null }) {
  const barra = document.getElementById("statusBar");
  if (ativo) {
    barra.className = "status-indicador ativo";
    barra.innerHTML = `<span></span> 🟢 Ativo • Última atividade: há ${formatRelativo(delta)}`;
  } else {
    barra.className = "status-indicador parado";
    barra.innerHTML = `<span></span> 🔴 Parado • Última atividade: há ${formatRelativo(delta)}`;
  }
}

// ------------------------------
// CONSULTAS À API
// ------------------------------
async function atualizarStatusComFallback(scan) {
  try {
    const resp = await fetch("/api/status");
    const data = await resp.json();
    const ok = data.status?.toLowerCase() === "ativo" || data.online || data.ok;
    if (ok) {
      pintarStatus({ ativo: true });
      return data.logs || [];
    } else {
      const { ativo, delta } = inferStatusFromScan(scan || {});
      pintarStatus({ ativo, delta });
      return data.logs || [];
    }
  } catch {
    const { ativo, delta } = inferStatusFromScan(scan || {});
    pintarStatus({ ativo, delta });
    return [];
  }
}

// ------------------------------
// CARREGAMENTO DE ARQUIVOS
// ------------------------------
async function loadFiles() {
  try {
    const resp = await fetch("/api/scan");
    const scan = await resp.json();
    const logsStatus = await atualizarStatusComFallback(scan);

    // ---------- TABELA INPUT ----------
    const tbodyIn = document.querySelector("#inputTable tbody");
    tbodyIn.innerHTML = "";
    if (!scan.input?.length) {
      tbodyIn.innerHTML = `<tr><td colspan="2" style="text-align:center;">Nenhum arquivo enviado.</td></tr>`;
    } else {
      scan.input.forEach(i => {
        const tr = document.createElement("tr");
        tr.innerHTML = `<td class='mono'>${i.nome}</td><td>${i.data_hora}</td>`;
        tbodyIn.appendChild(tr);
      });
    }
    document.getElementById("inputCount").textContent = `📊 Exibindo ${scan.input?.length || 0} registros.`;

    // ---------- ARQUIVOS GERADOS ----------
    const outputContainer = document.getElementById("outputContainer");
    const resumo = document.getElementById("outputSummary");
    outputContainer.innerHTML = "";

    if (!scan.output?.length) {
      resumo.textContent = "📊 Nenhum lote processado ainda.";
      outputContainer.innerHTML = `<p style="text-align:center;">Nenhum arquivo gerado.</p>`;
    } else {
      const grupos = {};
      scan.output.forEach(i => {
        const lote = i.lote || "NSA_000";
        if (!grupos[lote]) grupos[lote] = [];
        grupos[lote].push(i);
      });
      resumo.textContent = `📊 ${Object.keys(grupos).length} lotes processados — total de ${scan.output.length} arquivos.`;

      Object.keys(grupos).sort().forEach(lote => {
        const div = document.createElement("div");
        div.classList.add("lote-grupo");
        const header = document.createElement("div");
        header.classList.add("lote-header");
        header.innerHTML = `<h3>📦 ${lote} <span class='badge'>${grupos[lote].length}</span></h3>
                            <button class='toggle-btn' onclick='toggleLote(this)'>+</button>`;
        const content = document.createElement("div");
        content.classList.add("lote-content");
        content.innerHTML = `
          <table>
            <thead><tr><th>Arquivo</th><th>Data/Hora</th><th>Ação</th></tr></thead>
            <tbody>
              ${grupos[lote]
                .map(a => `<tr>
                  <td class='mono'>${a.nome}</td>
                  <td>${a.data_hora}</td>
                  <td><a href='/api/download/${encodeURIComponent(a.nome)}' target='_blank' style='color:#ff6d00;text-decoration:none;'>⬇️ Baixar</a></td>
                </tr>`).join("")}
            </tbody>
          </table>`;
        div.appendChild(header);
        div.appendChild(content);
        outputContainer.appendChild(div);
      });
    }

    // ---------- LOGS ----------
    const tbodyLog = document.querySelector("#logTable tbody");
    const logs = scan.logs?.length ? scan.logs : logsStatus;
    tbodyLog.innerHTML = "";
    if (!logs?.length) {
      tbodyLog.innerHTML = `<tr><td colspan="7" style="text-align:center;">Nenhum log encontrado.</td></tr>`;
    } else {
      logs.slice().reverse().forEach(l => {
        const statusClass = l.status === "OK" ? "ok" : "erro";
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${l.data_hora}</td>
          <td class='mono'>${l.arquivo}</td>
          <td>${l.tipo}</td>
          <td>${l.total_trailer}</td>
          <td>${l.total_processado}</td>
          <td class='${statusClass}'>${l.status}</td>
          <td>${l.detalhe}</td>`;
        tbodyLog.appendChild(tr);
      });
    }
    document.getElementById("logCount").textContent = `📊 Exibindo ${logs?.length || 0} registros.`;

  } catch (err) {
    console.error("Erro ao carregar dados:", err);
    document.getElementById("statusBar").innerHTML = `<span></span> ⚠️ Erro ao consultar API`;
  }
}

// ------------------------------
// FUNÇÕES DE INTERFACE
// ------------------------------
function toggleLote(btn) {
  const grupo = btn.closest(".lote-grupo");
  const content = grupo.querySelector(".lote-content");
  const isOpen = content.classList.contains("open");

  document.querySelectorAll(".lote-content.open").forEach(c => {
    c.classList.remove("open");
    c.parentElement.classList.remove("open");
    c.previousElementSibling.querySelector("button").textContent = "+";
  });

  if (!isOpen) {
    content.classList.add("open");
    grupo.classList.add("open");
    btn.textContent = "−";
  }
}

// ------------------------------
// DOWNLOAD ALL
// ------------------------------
async function downloadAll() {
  try {
    const resp = await fetch("/api/download-all");
    const data = await resp.json();
    if (data.zips?.length) {
      alert(`Foram gerados ${data.zips.length} arquivos ZIP.`);
      data.zips.forEach(z => {
        const a = document.createElement("a");
        a.href = `/zips/${z}`;
        a.download = z;
        a.click();
      });
    } else {
      alert("Nenhum ZIP foi gerado.");
    }
  } catch {
    alert("Erro ao gerar os ZIPs.");
  }
}

// ------------------------------
// AUTOATUALIZAÇÃO
// ------------------------------
loadFiles();
setInterval(loadFiles, 30000);
