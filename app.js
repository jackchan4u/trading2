const STOCKS = [
  "NVDA",
  "MRVL",
  "AMD",
  "UNH",
  "QBTS",
  "APLD",
  "SOUN",
  "CRWV",
  "PSTG",
  "CLSK",
  "LSMC",
];

const CRYPTOS = [
  "BTC/USDT",
  "XRP/USDT",
  "ADA/USDT",
  "SOL/USDT",
  "HBAR/USDT",
];

const dom = {
  stockFilter: document.querySelector("#stockFilter"),
  cryptoFilter: document.querySelector("#cryptoFilter"),
  stockInterval: document.querySelector("#stockInterval"),
  cryptoInterval: document.querySelector("#cryptoInterval"),
  applySettings: document.querySelector("#applySettings"),
  stocksBody: document.querySelector("#stocksBody"),
  cryptoBody: document.querySelector("#cryptoBody"),
  stockStatus: document.querySelector("#stockStatus"),
  cryptoStatus: document.querySelector("#cryptoStatus"),
  lastUpdated: document.querySelector("#lastUpdated"),
  alertForm: document.querySelector("#alertForm"),
  alertSymbol: document.querySelector("#alertSymbol"),
  alertLevel: document.querySelector("#alertLevel"),
  alertDirection: document.querySelector("#alertDirection"),
  alertsList: document.querySelector("#alertsList"),
  filingsList: document.querySelector("#filingsList"),
  newsList: document.querySelector("#newsList"),
  pressList: document.querySelector("#pressList"),
  cryptoNewsList: document.querySelector("#cryptoNewsList"),
  cryptoPressList: document.querySelector("#cryptoPressList"),
  openMobileLink: document.querySelector("#openMobileLink"),
  mobileUrl: document.querySelector("#mobileUrl"),
};

const DEFAULT_STOCK_INTERVAL_SEC = 120;
const MIN_STOCK_INTERVAL_SEC = 120;
const MAX_HISTORY_POINTS = 300;
const HISTORY_STORAGE_KEY = "priceHistoryV1";
const ALERTS_STORAGE_KEY = "priceAlertsV1";
const FILINGS_REFRESH_MS = 5 * 60 * 1000;
const NEWS_REFRESH_MS = 3 * 60 * 1000;
const PRESS_REFRESH_MS = 6 * 60 * 1000;
const CRYPTO_NEWS_REFRESH_MS = 6 * 60 * 1000;
const CRYPTO_PRESS_REFRESH_MS = 8 * 60 * 1000;

const state = {
  stockIntervalMs: DEFAULT_STOCK_INTERVAL_SEC * 1000,
  cryptoIntervalMs: 15000,
  stockTimer: null,
  cryptoTimer: null,
};

const stockRows = new Map();
const cryptoRows = new Map();
const latestStocks = new Map();
const latestCryptos = new Map();
const history = new Map();
let historySaveTimer = null;
let alerts = [];

const cryptoPairs = CRYPTOS.map((label) => ({
  label,
  apiSymbol: label.replace("/", ""),
}));
const CRYPTO_TICKERS = Array.from(
  new Set(CRYPTOS.map((label) => label.split("/")[0]))
);
const CRYPTO_NEWS_SYMBOLS = CRYPTO_TICKERS.map((symbol) => `${symbol}-USD`);

const formatUsd = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 6,
});

const formatPercent = new Intl.NumberFormat("en-US", {
  style: "percent",
  maximumFractionDigits: 2,
});

const sessionLabels = {
  premarket: "Pre-mercado",
  open: "Mercado",
  after: "Post-mercado",
  closed: "Cerrado",
};

function formatNumber(value, digits = 2) {
  if (!Number.isFinite(value)) return "--";
  return value.toFixed(digits);
}

function formatPrice(value) {
  if (!Number.isFinite(value)) return "--";
  const abs = Math.abs(value);
  if (abs >= 1000) return formatUsd.format(value);
  if (abs >= 100) return `$${value.toFixed(2)}`;
  if (abs >= 1) return `$${value.toFixed(4)}`;
  return `$${value.toFixed(6)}`;
}

function formatTime(date) {
  if (!date) return "--";
  return date.toLocaleTimeString("es-ES", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function formatChange(change, percent) {
  if (!Number.isFinite(change)) return "--";
  const sign = change > 0 ? "+" : "";
  const pct = Number.isFinite(percent)
    ? ` (${formatPercent.format(percent / 100)})`
    : "";
  return `${sign}${change.toFixed(2)}${pct}`;
}

async function updateMobileLink() {
  if (!dom.openMobileLink || !dom.mobileUrl) return;
  const fallbackUrl = window.location.origin;
  try {
    const response = await fetch("/api/lan");
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Error API");
    }
    const url = payload.url || fallbackUrl;
    dom.openMobileLink.href = url;
    dom.mobileUrl.textContent = url;
  } catch (error) {
    dom.openMobileLink.href = fallbackUrl;
    dom.mobileUrl.textContent = fallbackUrl;
  }
}

function formatCryptoNewsSymbol(symbol) {
  if (!symbol) return "";
  return symbol.split("-")[0];
}

function getTranslatedTitle(item, fallback) {
  return item.titleTranslated || item.title || fallback;
}

function formatClassification(value) {
  if (!value) return "sin clasificar";
  return String(value);
}

function formatImpact(value) {
  if (!value) return "sin datos";
  return String(value);
}

function formatIgnoreFlag(value) {
  if (value === true) return "Ignorar";
  if (value === false) return "Relevante";
  return "Sin datos";
}

function normalizeText(value, fallback) {
  if (!value) return fallback;
  const trimmed = String(value).trim();
  return trimmed || fallback;
}

function formatSession(value) {
  if (!value) return "--";
  return sessionLabels[value] || "--";
}

function formatDateTime(timestamp) {
  if (!Number.isFinite(timestamp)) return "--";
  return new Date(timestamp).toLocaleString("es-ES", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function loadHistory() {
  try {
    const raw = localStorage.getItem(HISTORY_STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    Object.entries(parsed).forEach(([symbol, points]) => {
      if (!Array.isArray(points)) return;
      const normalized = points
        .map((item) => ({ t: item[0], p: item[1] }))
        .filter((item) => Number.isFinite(item.p) && Number.isFinite(item.t));
      history.set(symbol, normalized.slice(-MAX_HISTORY_POINTS));
    });
  } catch (error) {
    history.clear();
  }
}

function scheduleHistorySave() {
  if (historySaveTimer) return;
  historySaveTimer = setTimeout(() => {
    historySaveTimer = null;
    const payload = {};
    history.forEach((points, symbol) => {
      payload[symbol] = points.map((item) => [item.t, item.p]);
    });
    localStorage.setItem(HISTORY_STORAGE_KEY, JSON.stringify(payload));
  }, 800);
}

function recordHistory(symbol, price, timestampMs) {
  if (!Number.isFinite(price)) return;
  const safeTs = Number.isFinite(timestampMs) ? timestampMs : Date.now();
  const series = history.get(symbol) || [];
  const last = series[series.length - 1];
  if (last && last.t === safeTs) return;
  series.push({ t: safeTs, p: price });
  if (series.length > MAX_HISTORY_POINTS) {
    series.splice(0, series.length - MAX_HISTORY_POINTS);
  }
  history.set(symbol, series);
  scheduleHistorySave();
}

function getSeries(symbol) {
  const series = history.get(symbol) || [];
  return series.map((item) => item.p);
}

function calculateRSI(values, period = 14) {
  if (values.length < period + 1) return null;
  let gains = 0;
  let losses = 0;
  const start = values.length - period;
  for (let i = start; i < values.length; i += 1) {
    const diff = values[i] - values[i - 1];
    if (diff > 0) gains += diff;
    if (diff < 0) losses += Math.abs(diff);
  }
  if (losses === 0) return 100;
  const rs = gains / losses;
  return 100 - 100 / (1 + rs);
}

function emaSeries(values, period) {
  if (values.length < period) return null;
  const k = 2 / (period + 1);
  const seed = values.slice(0, period).reduce((sum, val) => sum + val, 0) / period;
  const result = [seed];
  for (let i = period; i < values.length; i += 1) {
    const next = values[i] * k + result[result.length - 1] * (1 - k);
    result.push(next);
  }
  return result;
}

function calculateMACD(values) {
  if (values.length < 26) return null;
  const ema12 = emaSeries(values, 12);
  const ema26 = emaSeries(values, 26);
  if (!ema12 || !ema26) return null;
  const offset = ema12.length - ema26.length;
  const macdSeries = ema26.map((value, idx) => {
    const ema12Value = ema12[idx + offset];
    return ema12Value - value;
  });
  const signalSeries = emaSeries(macdSeries, 9);
  return {
    macd: macdSeries[macdSeries.length - 1],
    signal: signalSeries ? signalSeries[signalSeries.length - 1] : null,
  };
}

function setStatus(target, message) {
  target.textContent = message;
}

function createRow(symbol, variant) {
  const row = document.createElement("div");
  row.className = `row data ${variant}`;
  if (variant === "stocks") {
    row.innerHTML = `
      <div class="symbol" data-label="Simbolo">${symbol}</div>
      <div class="price" data-label="Precio">--</div>
      <div class="change neutral" data-label="Variacion">--</div>
      <div class="rsi" data-label="RSI">--</div>
      <div class="macd" data-label="MACD">--</div>
      <div class="session" data-label="Sesion">--</div>
      <div class="time" data-label="Actualizado">--</div>
    `;
  } else {
    row.innerHTML = `
      <div class="symbol" data-label="Par">${symbol}</div>
      <div class="price" data-label="Precio">--</div>
      <div class="change neutral" data-label="Variacion">--</div>
      <div class="rsi" data-label="RSI">--</div>
      <div class="macd" data-label="MACD">--</div>
      <div class="time" data-label="Actualizado">--</div>
    `;
  }
  return row;
}

function updateRow(row, payload) {
  const priceEl = row.querySelector(".price");
  const changeEl = row.querySelector(".change");
  const rsiEl = row.querySelector(".rsi");
  const macdEl = row.querySelector(".macd");
  const sessionEl = row.querySelector(".session");
  const timeEl = row.querySelector(".time");

  if (payload.error) {
    priceEl.textContent = "--";
    changeEl.textContent = payload.error;
    changeEl.className = "change neutral";
    if (rsiEl) rsiEl.textContent = "--";
    if (macdEl) macdEl.textContent = "--";
    if (sessionEl) {
      sessionEl.textContent = "--";
      sessionEl.className = "session";
    }
    timeEl.textContent = "--";
    return;
  }

  priceEl.textContent = formatPrice(payload.price);
  changeEl.textContent = formatChange(payload.change, payload.changePercent);

  if (payload.change > 0) {
    changeEl.className = "change up";
  } else if (payload.change < 0) {
    changeEl.className = "change down";
  } else {
    changeEl.className = "change neutral";
  }

  if (rsiEl) {
    rsiEl.textContent = formatNumber(payload.rsi, 1);
  }
  if (macdEl) {
    if (Number.isFinite(payload.macd)) {
      const macd = formatNumber(payload.macd, 3);
      const signal = Number.isFinite(payload.signal)
        ? formatNumber(payload.signal, 3)
        : "--";
      macdEl.textContent = `${macd} / ${signal}`;
    } else {
      macdEl.textContent = "--";
    }
  }

  if (sessionEl) {
    sessionEl.textContent = formatSession(payload.session);
    sessionEl.className = `session ${payload.session || ""}`.trim();
  }

  timeEl.textContent = formatTime(payload.updatedAt);
}

function createTables() {
  STOCKS.forEach((symbol) => {
    const row = createRow(symbol, "stocks");
    dom.stocksBody.appendChild(row);
    stockRows.set(symbol, row);
  });

  cryptoPairs.forEach((pair) => {
    const row = createRow(pair.label, "crypto");
    dom.cryptoBody.appendChild(row);
    cryptoRows.set(pair.label, row);
  });
}

function applyFilter(type) {
  const isStock = type === "stocks";
  if (isStock && !dom.stockFilter) return;
  if (!isStock && !dom.cryptoFilter) return;
  const filterValue = isStock ? dom.stockFilter.value : dom.cryptoFilter.value;
  const rows = isStock ? stockRows : cryptoRows;
  const body = isStock ? dom.stocksBody : dom.cryptoBody;
  const symbols = isStock ? STOCKS : CRYPTOS;
  const source = isStock ? latestStocks : latestCryptos;

  const items = symbols.map((symbol) => source.get(symbol) || { symbol });
  const withPerf = items.filter((item) => Number.isFinite(item.changePercent));

  let visible = symbols;
  if (filterValue === "gainers") {
    visible = withPerf
      .filter((item) => item.changePercent > 0)
      .sort((a, b) => b.changePercent - a.changePercent)
      .map((item) => item.symbol);
  } else if (filterValue === "losers") {
    visible = withPerf
      .filter((item) => item.changePercent < 0)
      .sort((a, b) => a.changePercent - b.changePercent)
      .map((item) => item.symbol);
  } else if (filterValue === "top5") {
    visible = withPerf
      .slice()
      .sort((a, b) => b.changePercent - a.changePercent)
      .slice(0, 5)
      .map((item) => item.symbol);
  } else if (filterValue === "bottom5") {
    visible = withPerf
      .slice()
      .sort((a, b) => a.changePercent - b.changePercent)
      .slice(0, 5)
      .map((item) => item.symbol);
  }

  const visibleSet = new Set(visible);
  symbols.forEach((symbol) => {
    const row = rows.get(symbol);
    if (!row) return;
    row.style.display = visibleSet.has(symbol) ? "" : "none";
  });
  visible.forEach((symbol) => {
    const row = rows.get(symbol);
    if (row) body.appendChild(row);
  });
}

function loadSettings() {
  const savedStockInterval =
    Number(localStorage.getItem("stockInterval")) ||
    DEFAULT_STOCK_INTERVAL_SEC;
  const savedCryptoInterval = Number(localStorage.getItem("cryptoInterval")) || 15;

  state.stockIntervalMs =
    Math.max(MIN_STOCK_INTERVAL_SEC, savedStockInterval) * 1000;
  state.cryptoIntervalMs = Math.max(5, savedCryptoInterval) * 1000;

  dom.stockInterval.value = String(state.stockIntervalMs / 1000);
  dom.cryptoInterval.value = savedCryptoInterval;
}

function persistSettings() {
  localStorage.setItem("stockInterval", String(state.stockIntervalMs / 1000));
  localStorage.setItem("cryptoInterval", String(state.cryptoIntervalMs / 1000));
}

function loadAlerts() {
  try {
    const raw = localStorage.getItem(ALERTS_STORAGE_KEY);
    alerts = raw ? JSON.parse(raw) : [];
    if (!Array.isArray(alerts)) alerts = [];
    alerts = alerts.map((alert) => {
      if (Number.isFinite(alert.createdAt)) return alert;
      const parts = String(alert.id || "").split("_");
      const createdAt = Number(parts[1]) || Date.now();
      return { ...alert, createdAt };
    });
    saveAlerts();
  } catch (error) {
    alerts = [];
  }
}

function saveAlerts() {
  localStorage.setItem(ALERTS_STORAGE_KEY, JSON.stringify(alerts));
}

function renderAlerts() {
  if (!dom.alertsList) return;
  const sorted = alerts
    .slice()
    .sort(
      (a, b) =>
        (b.triggeredAt || b.createdAt || 0) -
        (a.triggeredAt || a.createdAt || 0)
    );
  if (!sorted.length) {
    dom.alertsList.innerHTML = "<div class=\"empty\">Sin alertas activas.</div>";
    return;
  }
  dom.alertsList.innerHTML = sorted
    .map((alert) => {
      const directionLabel =
        alert.direction === "above" ? "Ruptura al alza" : "Ruptura a la baja";
      const status = alert.triggeredAt ? "Disparada" : "Armada";
      const timestamp = alert.triggeredAt || alert.createdAt;
      const statusClass = alert.triggeredAt ? "alert-item triggered" : "alert-item";
      return `
        <div class="${statusClass}">
          <div>
            <div class="alert-title">${alert.symbol} · ${directionLabel}</div>
            <div class="alert-meta">Nivel ${formatNumber(alert.level, 4)} · ${status} · ${formatDateTime(timestamp)}</div>
          </div>
          <div class="alert-actions">
            ${
              alert.triggeredAt
                ? `<button data-action="reset" data-id="${alert.id}" class="ghost">Rearmar</button>`
                : ""
            }
            <button data-action="remove" data-id="${alert.id}" class="ghost">Eliminar</button>
          </div>
        </div>
      `;
    })
    .join("");
}

function addAlert(symbol, level, direction) {
  const id = `alert_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`;
  alerts.unshift({
    id,
    symbol,
    level,
    direction,
    triggeredAt: null,
    lastState: null,
    createdAt: Date.now(),
  });
  saveAlerts();
  renderAlerts();
}

function resetAlert(id) {
  alerts = alerts.map((alert) =>
    alert.id === id ? { ...alert, triggeredAt: null } : alert
  );
  saveAlerts();
  renderAlerts();
}

function removeAlert(id) {
  alerts = alerts.filter((alert) => alert.id !== id);
  saveAlerts();
  renderAlerts();
}

function getCurrentPrice(symbol) {
  if (latestStocks.has(symbol)) return latestStocks.get(symbol).price;
  if (latestCryptos.has(symbol)) return latestCryptos.get(symbol).price;
  return null;
}

function evaluateAlerts() {
  let changed = false;
  alerts = alerts.map((alert) => {
    const price = getCurrentPrice(alert.symbol);
    if (!Number.isFinite(price)) return alert;
    const currentState = price >= alert.level ? "above" : "below";
    const shouldTrigger =
      alert.lastState &&
      currentState !== alert.lastState &&
      ((alert.direction === "above" && currentState === "above") ||
        (alert.direction === "below" && currentState === "below"));
    const nextAlert = { ...alert, lastState: currentState };
    if (shouldTrigger) {
      nextAlert.triggeredAt = Date.now();
      changed = true;
    }
    return nextAlert;
  });
  if (changed) {
    saveAlerts();
    renderAlerts();
  }
}

function populateSymbolSelects() {
  if (dom.alertSymbol) {
    const options = [
      ...STOCKS.map((symbol) => `<option value="${symbol}">${symbol}</option>`),
      ...CRYPTOS.map((symbol) => `<option value="${symbol}">${symbol}</option>`),
    ].join("");
    dom.alertSymbol.innerHTML = options;
  }
}

async function fetchFilings() {
  const response = await fetch("/api/filings");
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Error API");
  }
  return Array.isArray(payload.data) ? payload.data : [];
}

async function fetchNews() {
  const response = await fetch("/api/news");
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Error API");
  }
  return Array.isArray(payload.data) ? payload.data : [];
}

async function fetchPress() {
  const response = await fetch("/api/press");
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Error API");
  }
  return Array.isArray(payload.data) ? payload.data : [];
}

async function fetchCryptoNews() {
  const url = `/api/news?symbols=${encodeURIComponent(CRYPTO_NEWS_SYMBOLS.join(","))}`;
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Error API");
  }
  return Array.isArray(payload.data) ? payload.data : [];
}

async function fetchCryptoPress() {
  const url = `/api/press?symbols=${encodeURIComponent(CRYPTO_TICKERS.join(","))}`;
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Error API");
  }
  return Array.isArray(payload.data) ? payload.data : [];
}

async function loadFilings() {
  if (!dom.filingsList) return;
  dom.filingsList.innerHTML = "<div class=\"empty\">Cargando informes...</div>";
  try {
    const items = await fetchFilings();
    if (!items.length) {
      dom.filingsList.innerHTML = "<div class=\"empty\">Sin informes recientes.</div>";
      return;
    }
    dom.filingsList.innerHTML = items
      .map(
        (item) => {
          const impact = formatImpact(item.impact);
          const thesis = normalizeText(item.thesis, "no determinable");
          const summary = normalizeText(item.summary, "Sin datos verificables");
          const what = normalizeText(item.whatHappened, "No hay datos verificables");
          return `
        <div class="list-item">
          <div>
            <div class="list-title">${item.form || "Informe"}</div>
            <div class="list-meta">${[item.symbol, item.date].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">Impacto: ${impact} · Tesis: ${thesis}</div>
            <div class="list-meta">Resumen: ${summary}</div>
            <div class="list-meta">Que paso: ${what}</div>
          </div>
          <a class="link" href="${item.link}" target="_blank" rel="noopener">Ver</a>
        </div>
      `;
        }
      )
      .join("");
  } catch (error) {
    dom.filingsList.innerHTML = `<div class="empty">${error.message}</div>`;
  }
}

async function loadNews() {
  if (!dom.newsList) return;
  dom.newsList.innerHTML = "<div class=\"empty\">Cargando noticias...</div>";
  try {
    const items = await fetchNews();
    if (!items.length) {
      dom.newsList.innerHTML = "<div class=\"empty\">Sin noticias recientes.</div>";
      return;
    }
    dom.newsList.innerHTML = items
      .map(
        (item) => {
          const classification = formatClassification(item.classification);
          const impact = formatImpact(item.impact);
          const ignore = formatIgnoreFlag(item.ignore);
          const reason = normalizeText(item.reason, "Sin datos verificables");
          return `
        <div class="list-item">
          <div>
            <div class="list-title">${getTranslatedTitle(item, "Noticia")}</div>
            <div class="list-meta">${[item.symbol, item.date].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">Clasificacion: ${classification} · Impacto: ${impact} · ${ignore}</div>
            <div class="list-meta">Motivo: ${reason}</div>
          </div>
          <a class="link" href="${item.link}" target="_blank" rel="noopener">Leer</a>
        </div>
      `;
        }
      )
      .join("");
  } catch (error) {
    dom.newsList.innerHTML = `<div class="empty">${error.message}</div>`;
  }
}

async function loadPress() {
  if (!dom.pressList) return;
  dom.pressList.innerHTML = "<div class=\"empty\">Cargando notas de prensa...</div>";
  try {
    const items = await fetchPress();
    const officialItems = items.filter((item) => item.official === true);
    const fallbackItems = items.filter((item) => item.fallback === true);
    if (!officialItems.length) {
      if (!fallbackItems.length) {
        dom.pressList.innerHTML = "<div class=\"empty\">Sin NDP oficiales.</div>";
        return;
      }
      const hint =
        "<div class=\"list-hint\">Sin NDP oficiales; mostrando ultima noticia por ticker (no oficial).</div>";
      const list = fallbackItems
        .map(
          (item) => `
        <div class="list-item">
          <div>
            <div class="list-title">${getTranslatedTitle(item, "Nota de prensa")}</div>
            <div class="list-meta">${[item.symbol, item.date].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">No oficial (solo comprobacion)</div>
          </div>
          <a class="link" href="${item.link}" target="_blank" rel="noopener">Leer</a>
        </div>
      `
        )
        .join("");
      dom.pressList.innerHTML = `${hint}${list}`;
      return;
    }
    const list = officialItems
      .map(
        (item) => `
        <div class="list-item">
          <div>
            <div class="list-title">${getTranslatedTitle(item, "Nota de prensa")}</div>
            <div class="list-meta">${[item.symbol, item.date].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">Fuente oficial: ${item.source || "N/D"}</div>
          </div>
          <a class="link" href="${item.link}" target="_blank" rel="noopener">Leer</a>
        </div>
      `
      )
      .join("");
    dom.pressList.innerHTML = list;
  } catch (error) {
    dom.pressList.innerHTML = `<div class="empty">${error.message}</div>`;
  }
}

async function loadCryptoPress() {
  if (!dom.cryptoPressList) return;
  dom.cryptoPressList.innerHTML =
    "<div class=\"empty\">Cargando notas de prensa cripto...</div>";
  try {
    const items = await fetchCryptoPress();
    const officialItems = items.filter((item) => item.official === true);
    if (!officialItems.length) {
      dom.cryptoPressList.innerHTML = "<div class=\"empty\">Sin NDP oficiales.</div>";
      return;
    }
    const list = officialItems
      .map(
        (item) => `
        <div class="list-item">
          <div>
            <div class="list-title">${getTranslatedTitle(item, "Nota de prensa")}</div>
            <div class="list-meta">${[item.symbol, item.date].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">Fuente oficial: ${item.source || "N/D"}</div>
          </div>
          <a class="link" href="${item.link}" target="_blank" rel="noopener">Leer</a>
        </div>
      `
      )
      .join("");
    dom.cryptoPressList.innerHTML = list;
  } catch (error) {
    dom.cryptoPressList.innerHTML = `<div class="empty">${error.message}</div>`;
  }
}

async function loadCryptoNews() {
  if (!dom.cryptoNewsList) return;
  dom.cryptoNewsList.innerHTML = "<div class=\"empty\">Cargando noticias cripto...</div>";
  try {
    const items = await fetchCryptoNews();
    if (!items.length) {
      dom.cryptoNewsList.innerHTML =
        "<div class=\"empty\">Sin noticias cripto recientes.</div>";
      return;
    }
    dom.cryptoNewsList.innerHTML = items
      .map((item) => {
        const classification = formatClassification(item.classification);
        const impact = formatImpact(item.impact);
        const ignore = formatIgnoreFlag(item.ignore);
        const reason = normalizeText(item.reason, "Sin datos verificables");
        const symbol = formatCryptoNewsSymbol(item.symbol);
        return `
        <div class="list-item">
          <div>
            <div class="list-title">${getTranslatedTitle(item, "Noticia")}</div>
            <div class="list-meta">${[symbol, item.date].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">Clasificacion: ${classification} · Impacto: ${impact} · ${ignore}</div>
            <div class="list-meta">Motivo: ${reason}</div>
          </div>
          <a class="link" href="${item.link}" target="_blank" rel="noopener">Leer</a>
        </div>
      `;
      })
      .join("");
  } catch (error) {
    dom.cryptoNewsList.innerHTML = `<div class="empty">${error.message}</div>`;
  }
}

async function fetchStocksBatch() {
  const url = `/api/stocks?symbols=${encodeURIComponent(STOCKS.join(","))}`;
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Error API");
  }
  return {
    data: Array.isArray(payload.data) ? payload.data : [],
    meta: payload.meta || {},
  };
}

async function fetchCryptoBatch() {
  const symbols = cryptoPairs.map((pair) => pair.apiSymbol);
  const url = `https://api.binance.com/api/v3/ticker/24hr?symbols=${encodeURIComponent(
    JSON.stringify(symbols)
  )}`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error("Error API");
  }
  return response.json();
}

async function updateStocks() {
  setStatus(dom.stockStatus, "Acciones: actualizando...");
  try {
    const payload = await fetchStocksBatch();
    const data = payload.data;
    const mapped = new Map(data.map((item) => [item.symbol, item]));
    let okCount = 0;

    STOCKS.forEach((symbol) => {
      const row = stockRows.get(symbol);
      const item = mapped.get(symbol);
      if (!row) return;
      if (!item) {
        updateRow(row, { error: "Sin datos" });
        return;
      }
      if (item.error) {
        updateRow(row, { error: item.error });
        return;
      }
      const price = Number(item.price);
      const change = Number(item.change);
      const changePercent = Number(item.changePercent);
      if (!Number.isFinite(price)) {
        updateRow(row, { error: "Sin datos" });
        return;
      }

      const updatedAtMs = Number.isFinite(item.updatedAt)
        ? item.updatedAt * 1000
        : Date.now();
      recordHistory(symbol, price, updatedAtMs);
      const series = getSeries(symbol);
      const rsi = calculateRSI(series);
      const macdData = calculateMACD(series) || {};
      const updatedAt = new Date(updatedAtMs);

      latestStocks.set(symbol, {
        symbol,
        price,
        change,
        changePercent,
        session: item.marketState,
        updatedAt: updatedAtMs,
      });
      updateRow(row, {
        price,
        change: Number.isFinite(change) ? change : 0,
        changePercent: Number.isFinite(changePercent) ? changePercent : 0,
        rsi,
        macd: macdData.macd,
        signal: macdData.signal,
        session: item.marketState,
        updatedAt,
      });
      okCount += 1;
    });

    setStatus(dom.stockStatus, `Acciones: ${okCount}/${STOCKS.length}`);
    if (payload.meta?.error) {
      setStatus(
        dom.stockStatus,
        `Acciones: ${okCount}/${STOCKS.length} · ${payload.meta.error}`
      );
    }
    dom.lastUpdated.textContent = formatTime(new Date());
    applyFilter("stocks");
    evaluateAlerts();
  } catch (error) {
    const message = error?.message || "Error API";
    setStatus(dom.stockStatus, `Acciones: ${message}`);
    STOCKS.forEach((symbol) => {
      const row = stockRows.get(symbol);
      if (row) updateRow(row, { error: message });
    });
  }
}

async function updateCryptos() {
  setStatus(dom.cryptoStatus, "Cripto: actualizando...");
  try {
    const data = await fetchCryptoBatch();
    const mapped = new Map(
      data.map((item) => [item.symbol, item])
    );

    cryptoPairs.forEach((pair) => {
      const row = cryptoRows.get(pair.label);
      const item = mapped.get(pair.apiSymbol);
      if (!row || !item) {
        updateRow(row, { error: "Sin datos" });
        return;
      }

      const updatedAtMs = Date.now();
      const price = Number(item.lastPrice);
      recordHistory(pair.label, price, updatedAtMs);
      const series = getSeries(pair.label);
      const rsi = calculateRSI(series);
      const macdData = calculateMACD(series) || {};

      latestCryptos.set(pair.label, {
        symbol: pair.label,
        price,
        change: Number(item.priceChange),
        changePercent: Number(item.priceChangePercent),
        updatedAt: updatedAtMs,
      });
      updateRow(row, {
        price,
        change: Number(item.priceChange),
        changePercent: Number(item.priceChangePercent),
        rsi,
        macd: macdData.macd,
        signal: macdData.signal,
        updatedAt: new Date(updatedAtMs),
      });
    });

    setStatus(dom.cryptoStatus, `Cripto: ${cryptoPairs.length}/${cryptoPairs.length}`);
    dom.lastUpdated.textContent = formatTime(new Date());
    applyFilter("crypto");
    evaluateAlerts();
  } catch (error) {
    setStatus(dom.cryptoStatus, "Cripto: error de conexion");
    cryptoPairs.forEach((pair) => {
      const row = cryptoRows.get(pair.label);
      if (row) updateRow(row, { error: "Error API" });
    });
  }
}

function startTimers() {
  if (state.stockTimer) clearInterval(state.stockTimer);
  if (state.cryptoTimer) clearInterval(state.cryptoTimer);

  state.stockTimer = setInterval(updateStocks, state.stockIntervalMs);
  state.cryptoTimer = setInterval(updateCryptos, state.cryptoIntervalMs);
}

function applySettings() {
  state.stockIntervalMs =
    Math.max(
      MIN_STOCK_INTERVAL_SEC,
      Number(dom.stockInterval.value) || DEFAULT_STOCK_INTERVAL_SEC
    ) * 1000;
  state.cryptoIntervalMs =
    Math.max(5, Number(dom.cryptoInterval.value) || 15) * 1000;

  persistSettings();
  dom.stockInterval.value = String(state.stockIntervalMs / 1000);
  startTimers();
  updateStocks();
  updateCryptos();
  updateMobileLink();
}

function init() {
  createTables();
  loadHistory();
  loadSettings();
  loadAlerts();
  populateSymbolSelects();
  renderAlerts();
  applyFilter("stocks");
  applyFilter("crypto");
  dom.applySettings.addEventListener("click", applySettings);
  if (dom.stockFilter) {
    dom.stockFilter.addEventListener("change", () => applyFilter("stocks"));
  }
  if (dom.cryptoFilter) {
    dom.cryptoFilter.addEventListener("change", () => applyFilter("crypto"));
  }
  if (dom.alertForm) {
    dom.alertForm.addEventListener("submit", (event) => {
      event.preventDefault();
      const symbol = dom.alertSymbol.value;
      const level = Number(dom.alertLevel.value);
      const direction = dom.alertDirection.value;
      if (!symbol || !Number.isFinite(level)) return;
      addAlert(symbol, level, direction);
      dom.alertLevel.value = "";
    });
  }
  if (dom.alertsList) {
    dom.alertsList.addEventListener("click", (event) => {
      const action = event.target?.dataset?.action;
      const id = event.target?.dataset?.id;
      if (!action || !id) return;
      if (action === "remove") removeAlert(id);
      if (action === "reset") resetAlert(id);
    });
  }
  startTimers();
  updateStocks();
  updateCryptos();
  loadFilings();
  loadNews();
  loadPress();
  loadCryptoNews();
  loadCryptoPress();
  setInterval(loadFilings, FILINGS_REFRESH_MS);
  setInterval(loadNews, NEWS_REFRESH_MS);
  setInterval(loadPress, PRESS_REFRESH_MS);
  setInterval(loadCryptoNews, CRYPTO_NEWS_REFRESH_MS);
  setInterval(loadCryptoPress, CRYPTO_PRESS_REFRESH_MS);
}

init();
