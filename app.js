const STOCKS = [
  "NVDA",
  "MRVL",
  "AMD",
  "UNH",
  "QBTS",
  "APLD",
  "SOUN",
  "CRWV",
  "CLSK",
];

const CRYPTOS = [
  "BTC/USDT",
  "XRP/USDT",
  "ADA/USDT",
  "HBAR/USDT",
  "XLM/USDT",
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
const STALE_DATA_SECONDS = 60;
const MAX_HISTORY_POINTS = 300;
const HISTORY_STORAGE_KEY = "priceHistoryV1";
const ALERTS_STORAGE_KEY = "priceAlertsV1";
const FILINGS_SEEN_KEY = "filingsSeenV2";
const FILINGS_REFRESH_MS = 5 * 60 * 1000;
const NEWS_REFRESH_MS = 3 * 60 * 1000;
const PRESS_REFRESH_MS = 6 * 60 * 1000;
const CRYPTO_NEWS_REFRESH_MS = 6 * 60 * 1000;
const CRYPTO_PRESS_REFRESH_MS = 8 * 60 * 1000;
const EVENTS_REFRESH_MS = NEWS_REFRESH_MS;
const EVENTS_CACHE_MS = EVENTS_REFRESH_MS;
const CRYPTO_EVENT_WINDOW_HOURS = 24;
const MARKET_TIMEZONE = "America/New_York";
const MARKET_STATE_STALE_MS = 15 * 60 * 1000;
const LIST_CACHE_LIMIT = 60;
const PRESS_STORAGE_KEY = "pressCacheV1";
const NEWS_STORAGE_KEY = "newsCacheV1";
const FILINGS_STORAGE_KEY = "filingsCacheV2";
const CRYPTO_NEWS_STORAGE_KEY = "cryptoNewsCacheV1";
const CRYPTO_PRESS_STORAGE_KEY = "cryptoPressCacheV1";
const EVENTS_STORAGE_KEY = "eventsCacheV1";
const EVENTS_LIST_LIMIT = 6;

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
let eventsSnapshot = null;
let eventsSnapshotAt = 0;
let eventsSnapshotPromise = null;
const listExpansion = {
  filings: false,
  press: false,
  news: false,
  cryptoPress: false,
  cryptoNews: false,
};

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

const formatInteger = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 0,
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

function toNumberOrNaN(value) {
  if (value === null || value === undefined || value === "") {
    return NaN;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : NaN;
}

function formatVolume(value) {
  if (!Number.isFinite(value)) return "--";
  return formatInteger.format(Math.round(value));
}

function formatRange(low, high) {
  if (!Number.isFinite(low) || !Number.isFinite(high)) return "--";
  return `${formatNumber(low, 2)} - ${formatNumber(high, 2)}`;
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

function normalizeChangePercent(changePercent, price, change) {
  if (!Number.isFinite(price) || !Number.isFinite(change)) {
    return Number.isFinite(changePercent) ? changePercent : null;
  }
  const base = price - change;
  if (!Number.isFinite(base) || base === 0) {
    return Number.isFinite(changePercent) ? changePercent : null;
  }
  return (change / base) * 100;
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

function formatEventDate(value) {
  if (!value) return "";
  const parsed = Date.parse(value);
  if (!Number.isNaN(parsed)) return formatDateTime(parsed);
  return String(value);
}

function getItemTimeMs(item) {
  const direct = Number(item && item.timestamp);
  if (Number.isFinite(direct)) {
    return direct > 1e12 ? direct : direct * 1000;
  }
  const raw = item && (item.date || item.fecha_evento);
  if (raw) {
    const parsed = Date.parse(raw);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return 0;
}

function filterWindowWithLatest(items, hours, getSymbol) {
  const cutoff = Date.now() - (hours * 60 * 60 * 1000);
  const sorted = [...items].sort((a, b) => getItemTimeMs(b) - getItemTimeMs(a));
  const within = sorted.filter((item) => {
    const ts = getItemTimeMs(item);
    return ts && ts >= cutoff;
  });
  const seen = new Set(within.map(buildItemId).filter(Boolean));
  const latestBySymbol = new Map();
  sorted.forEach((item) => {
    const symbol = getSymbol(item);
    if (!symbol) return;
    if (!latestBySymbol.has(symbol)) latestBySymbol.set(symbol, item);
  });
  latestBySymbol.forEach((item) => {
    const id = buildItemId(item);
    if (!id || seen.has(id)) return;
    within.push(item);
    seen.add(id);
  });
  return within.sort((a, b) => getItemTimeMs(b) - getItemTimeMs(a));
}

function renderEventList(container, items, stateKey, emptyText, renderItem) {
  if (!container) return;
  if (!items.length) {
    container.innerHTML = `<div class="empty">${emptyText}</div>`;
    return;
  }
  const expanded = listExpansion[stateKey];
  const visible = expanded ? items : items.slice(0, EVENTS_LIST_LIMIT);
  const list = visible.map(renderItem).join("");
  const needsToggle = items.length > EVENTS_LIST_LIMIT;
  const toggleLabel = expanded ? "Ver menos" : "Ver más";
  const toggle = needsToggle
    ? `<div class="list-toggle-wrap"><button class="list-toggle" data-toggle="${stateKey}">${toggleLabel}</button></div>`
    : "";
  container.innerHTML = `${list}${toggle}`;
}

function formatImpactLevel(value) {
  if (!value) return "desconocido";
  return String(value);
}

function formatShares(value) {
  if (!Number.isFinite(value)) return "--";
  const formatter = new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 0,
  });
  return formatter.format(value);
}

function formatValue(value) {
  if (!Number.isFinite(value)) return "--";
  return formatUsd.format(value);
}

function formatDilutive(value) {
  if (value === true) return "si";
  if (value === false) return "no";
  return normalizeText(value, "desconocido");
}

function loadSeenFilings() {
  try {
    const raw = localStorage.getItem(FILINGS_SEEN_KEY);
    if (!raw) return new Set();
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return new Set();
    return new Set(parsed);
  } catch (error) {
    return new Set();
  }
}

function saveSeenFilings(seen) {
  try {
    const values = Array.from(seen).slice(-200);
    localStorage.setItem(FILINGS_SEEN_KEY, JSON.stringify(values));
  } catch (error) {
    return;
  }
}

function getLocalMarketState() {
  if (typeof Intl === "undefined" || !Intl.DateTimeFormat) return null;
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: MARKET_TIMEZONE,
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = formatter.formatToParts(new Date());
  const values = {};
  parts.forEach((part) => {
    if (part.type !== "literal") values[part.type] = part.value;
  });
  const weekday = values.weekday;
  const hour = Number(values.hour);
  const minute = Number(values.minute);
  const dayIndex = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"].indexOf(weekday);
  if (!Number.isFinite(hour) || !Number.isFinite(minute) || dayIndex < 0) return null;
  if (dayIndex === 0 || dayIndex === 6) return "closed";
  const total = hour * 60 + minute;
  if (total >= 240 && total < 570) return "premarket";
  if (total >= 570 && total < 960) return "open";
  if (total >= 960 && total < 1200) return "after";
  return "closed";
}

function resolveMarketState(serverState, updatedAtMs) {
  if (serverState && serverState !== "closed") return serverState;
  const localState = getLocalMarketState();
  if (!localState || localState === "closed") return serverState || "closed";
  if (!updatedAtMs || Date.now() - updatedAtMs > MARKET_STATE_STALE_MS) {
    return serverState || "closed";
  }
  return localState;
}

function buildItemId(item) {
  if (!item || typeof item !== "object") return "";
  if (item.link || item.url) return String(item.link || item.url);
  if (item.resumen && item.resumen.link) return String(item.resumen.link);
  const fallback = [
    item.ticker || item.symbol,
    item.tipo_evento || item.form,
    item.title || (item.resumen ? item.resumen.titulo : ""),
    item.fecha_evento || item.date,
  ]
    .filter(Boolean)
    .join("|");
  return fallback;
}

function getItemTimestamp(item) {
  const direct = Number(item.timestamp);
  if (Number.isFinite(direct)) return direct;
  const rawDate = item ? (item.date || item.fecha_evento) : "";
  const dateValue = rawDate ? Date.parse(rawDate) : NaN;
  if (!Number.isNaN(dateValue)) return dateValue / 1000;
  return 0;
}

function readListCache(storageKey) {
  try {
    const raw = localStorage.getItem(storageKey);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    return [];
  }
}

function writeListCache(storageKey, items) {
  try {
    localStorage.setItem(storageKey, JSON.stringify(items));
  } catch (error) {
    return;
  }
}

function mergeCachedItems(storageKey, incoming, limit) {
  const existing = readListCache(storageKey);
  const map = new Map();
  const allItems = existing.concat(incoming || []);
  allItems.forEach((item) => {
    const id = buildItemId(item);
    if (!id) return;
    if (map.has(id)) {
      map.set(id, { ...map.get(id), ...item });
    } else {
      map.set(id, item);
    }
  });
  const merged = Array.from(map.values()).sort(
    (a, b) => getItemTimestamp(b) - getItemTimestamp(a)
  );
  const limited = merged.slice(0, limit);
  writeListCache(storageKey, limited);
  return limited;
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
      <div class="session" data-label="Sesion">--</div>
      <div class="volume" data-label="Volumen">--</div>
      <div class="week-range" data-label="Rango 52s">--</div>
      <div class="day-range" data-label="Rango dia">--</div>
      <div class="time" data-label="Actualizado">--</div>
    `;
  } else {
    row.innerHTML = `
      <div class="symbol" data-label="Par">${symbol}</div>
      <div class="price" data-label="Precio">--</div>
      <div class="change neutral" data-label="Variacion">--</div>
      <div class="time" data-label="Actualizado">--</div>
    `;
  }
  return row;
}

function updateRow(row, payload) {
  const priceEl = row.querySelector(".price");
  const changeEl = row.querySelector(".change");
  const sessionEl = row.querySelector(".session");
  const volumeEl = row.querySelector(".volume");
  const weekRangeEl = row.querySelector(".week-range");
  const dayRangeEl = row.querySelector(".day-range");
  const timeEl = row.querySelector(".time");

  if (payload.error) {
    priceEl.textContent = "--";
    changeEl.textContent = payload.error;
    changeEl.className = "change neutral";
    if (sessionEl) {
      sessionEl.textContent = "--";
      sessionEl.className = "session";
    }
    if (volumeEl) volumeEl.textContent = "--";
    if (weekRangeEl) weekRangeEl.textContent = "--";
    if (dayRangeEl) dayRangeEl.textContent = "--";
    timeEl.textContent = "--";
    return;
  }

  priceEl.textContent = formatPrice(payload.price);
  if (payload.baselineError) {
    changeEl.textContent = payload.baselineError;
    changeEl.className = "change neutral";
  } else {
    changeEl.textContent = formatChange(payload.change, payload.changePercent);
    if (payload.change > 0) {
      changeEl.className = "change up";
    } else if (payload.change < 0) {
      changeEl.className = "change down";
    } else {
      changeEl.className = "change neutral";
    }
  }

  if (sessionEl) {
    sessionEl.textContent = formatSession(payload.session);
    sessionEl.className = `session ${payload.session || ""}`.trim();
  }

  if (volumeEl) {
    volumeEl.textContent = formatVolume(payload.volume);
  }
  if (weekRangeEl) {
    weekRangeEl.textContent = formatRange(payload.week52Low, payload.week52High);
  }
  if (dayRangeEl) {
    dayRangeEl.textContent = formatRange(payload.dayLow, payload.dayHigh);
  }

  const updatedAtValue = payload.updatedAt;
  let updatedAtMs = null;
  if (updatedAtValue instanceof Date) {
    updatedAtMs = updatedAtValue.getTime();
  } else if (Number.isFinite(updatedAtValue)) {
    updatedAtMs = updatedAtValue;
  } else if (updatedAtValue) {
    const parsed = new Date(updatedAtValue);
    if (!Number.isNaN(parsed.getTime())) {
      updatedAtMs = parsed.getTime();
    }
  }
  if (Number.isFinite(updatedAtMs)) {
    const stale = (Date.now() - updatedAtMs) > (STALE_DATA_SECONDS * 1000);
    const timeLabel = formatTime(new Date(updatedAtMs));
    if (stale) {
      timeEl.innerHTML = `<span class="stale-icon" aria-hidden="true">🟢</span> ${timeLabel}`;
    } else {
      timeEl.textContent = timeLabel;
    }
    timeEl.title = stale ? `Dato con mas de ${STALE_DATA_SECONDS}s` : "";
  } else {
    timeEl.textContent = "--";
    timeEl.title = "";
  }
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

async function fetchEvents() {
  const response = await fetch("/events");
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.error || "Error API");
  }
  return {
    data: Array.isArray(payload.data) ? payload.data : [],
    errors: Array.isArray(payload.errors) ? payload.errors : [],
  };
}

async function getEventsSnapshot() {
  const now = Date.now();
  if (eventsSnapshot && (now - eventsSnapshotAt) < EVENTS_CACHE_MS) {
    return eventsSnapshot;
  }
  if (eventsSnapshotPromise) {
    return eventsSnapshotPromise;
  }
  eventsSnapshotPromise = fetchEvents()
    .then((snapshot) => {
      eventsSnapshot = snapshot;
      eventsSnapshotAt = Date.now();
      eventsSnapshotPromise = null;
      writeListCache(EVENTS_STORAGE_KEY, snapshot.data);
      return snapshot;
    })
    .catch((error) => {
      eventsSnapshotPromise = null;
      throw error;
    });
  return eventsSnapshotPromise;
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
  const renderItems = (items) => {
    const seen = loadSeenFilings();
    const nextSeen = new Set(seen);
    renderEventList(
      dom.filingsList,
      items,
      "filings",
      "Sin informes recientes.",
      (item) => {
        const resumen = item.resumen || {};
        const detail = resumen.detalle || {};
        const impact = formatImpactLevel(item.impacto);
        const eventType = normalizeText(
          detail.evento || item.tipo_evento,
          "sin clasificar"
        );
        const insiderRaw = normalizeText(detail.insider, "no aplica");
        const action = normalizeText(detail.accion, "");
        const insiderLabel = [
          insiderRaw !== "no aplica" ? insiderRaw : "",
          action,
        ]
          .filter(Boolean)
          .join(" · ");
        const insiderLine = insiderLabel
          ? `<div class="list-meta"><span class="meta-label">Insider:</span> <span class="meta-value">${insiderLabel}</span></div>`
          : "";
        const sharesValue =
          detail.acciones === null || detail.acciones === undefined
            ? NaN
            : Number(detail.acciones);
        const valueAmount =
          detail.valor_usd === null || detail.valor_usd === undefined
            ? NaN
            : Number(detail.valor_usd);
        const shares = formatShares(sharesValue);
        const value = formatValue(valueAmount);
        const dilutive = normalizeText(item.dilutivo, "no");
        const title = normalizeText(
          resumen.titulo || item.tipo_evento,
          "Informe"
        );
        const dateLabel = formatEventDate(item.fecha_evento);
        const itemsValue =
          Array.isArray(detail.items) && detail.items.length
            ? detail.items.join(", ")
            : "";
        const itemsLine = itemsValue
          ? `<div class="list-meta"><span class="meta-label">Elementos:</span> <span class="meta-value">${itemsValue}</span></div>`
          : "";
        const materialLine =
          detail.material === true
            ? "<div class=\"list-meta\"><span class=\"meta-label\">Material:</span> <span class=\"meta-value\">si</span></div>"
            : detail.material === false
              ? "<div class=\"list-meta\"><span class=\"meta-label\">Material:</span> <span class=\"meta-value\">no</span></div>"
              : "";
        const errorLine = detail.error
          ? `<div class="list-meta error">Error tecnico: ${detail.error}</div>`
          : "";
        const id = buildItemId(item);
        const isNew = id ? !seen.has(id) : false;
        if (id) nextSeen.add(id);
        const badge = isNew ? "<span class=\"tag tag--new\">Nuevo</span>" : "";
        const url = resumen.link || "";
        return `
        <div class="list-item">
          <div>
            <div class="list-title">${title} ${badge}</div>
            <div class="list-meta">${[item.ticker, dateLabel].filter(Boolean).join(" · ")}</div>
            <div class="list-meta"><span class="meta-label">Evento:</span> <span class="meta-value">${eventType}</span> · <span class="meta-label">Impacto:</span> <span class="meta-value">${impact}</span></div>
            ${insiderLine}
            <div class="list-meta"><span class="meta-label">Acciones:</span> <span class="meta-value">${shares}</span> · <span class="meta-label">Valor:</span> <span class="meta-value">${value}</span> · <span class="meta-label">Dilutivo:</span> <span class="meta-value">${dilutive}</span></div>
            ${itemsLine}
            ${materialLine}
            ${errorLine}
          </div>
          <a class="link" href="${url}" target="_blank" rel="noopener">Ver</a>
        </div>
      `;
      }
    );
    saveSeenFilings(nextSeen);
  };
  try {
    const snapshot = await getEventsSnapshot();
    const items = snapshot.data.filter((item) =>
      String(item.tipo_evento || "").startsWith("SEC_")
    );
    writeListCache(FILINGS_STORAGE_KEY, items);
    renderItems(items);
  } catch (error) {
    const cached = readListCache(FILINGS_STORAGE_KEY);
    if (cached.length) {
      renderItems(cached);
      return;
    }
    dom.filingsList.innerHTML = `<div class="empty">${error.message}</div>`;
  }
}

async function loadNews() {
  if (!dom.newsList) return;
  dom.newsList.innerHTML = "<div class=\"empty\">Cargando noticias...</div>";
  const renderItems = (items) => {
    renderEventList(
      dom.newsList,
      items,
      "news",
      "Sin noticias recientes.",
      (item) => {
        const resumen = item.resumen || {};
        const impact = formatImpact(item.impacto);
        const title = normalizeText(
          resumen.tituloTranslated || resumen.titulo,
          "Noticia"
        );
        const source = normalizeText(resumen.fuente, "N/D");
        const dateLabel = formatEventDate(item.fecha_evento);
        return `
        <div class="list-item">
          <div>
            <div class="list-title">${title}</div>
            <div class="list-meta">${[item.ticker, dateLabel].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">Impacto: ${impact} · Fuente: ${source}</div>
          </div>
          <a class="link" href="${resumen.link || ""}" target="_blank" rel="noopener">Leer</a>
        </div>
      `;
      }
    );
  };
  try {
    const snapshot = await getEventsSnapshot();
    const items = snapshot.data.filter(
      (item) => item.tipo_evento === "NEWS"
    );
    writeListCache(NEWS_STORAGE_KEY, items);
    renderItems(items);
  } catch (error) {
    const cached = readListCache(NEWS_STORAGE_KEY);
    if (cached.length) {
      renderItems(cached);
      return;
    }
    dom.newsList.innerHTML = `<div class="empty">${error.message}</div>`;
  }
}

async function loadPress() {
  if (!dom.pressList) return;
  dom.pressList.innerHTML = "<div class=\"empty\">Cargando notas de prensa...</div>";
  const renderItems = (items) => {
    renderEventList(
      dom.pressList,
      items,
      "press",
      "Sin NDP oficiales.",
      (item) => {
        const resumen = item.resumen || {};
        const title = normalizeText(
          resumen.tituloTranslated || resumen.titulo,
          "Nota de prensa"
        );
        const source = normalizeText(resumen.fuente, "N/D");
        const dateLabel = formatEventDate(item.fecha_evento);
        return `
        <div class="list-item">
          <div>
            <div class="list-title">${title}</div>
            <div class="list-meta">${[item.ticker, dateLabel].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">Fuente oficial: ${source}</div>
          </div>
          <a class="link" href="${resumen.link || ""}" target="_blank" rel="noopener">Leer</a>
        </div>
      `;
      }
    );
  };
  try {
    const snapshot = await getEventsSnapshot();
    const items = snapshot.data.filter(
      (item) => item.tipo_evento === "PRESS_RELEASE"
    );
    writeListCache(PRESS_STORAGE_KEY, items);
    renderItems(items);
  } catch (error) {
    const cached = readListCache(PRESS_STORAGE_KEY);
    if (cached.length) {
      renderItems(cached);
      return;
    }
    dom.pressList.innerHTML = `<div class="empty">${error.message}</div>`;
  }
}

async function loadCryptoPress() {
  if (!dom.cryptoPressList) return;
  dom.cryptoPressList.innerHTML =
    "<div class=\"empty\">Cargando notas de prensa cripto...</div>";
  try {
    const items = mergeCachedItems(
      CRYPTO_PRESS_STORAGE_KEY,
      await fetchCryptoPress(),
      LIST_CACHE_LIMIT
    );
    const filtered = filterWindowWithLatest(
      items,
      CRYPTO_EVENT_WINDOW_HOURS,
      (item) => (item.symbol || "").toString().trim()
    );
    renderEventList(
      dom.cryptoPressList,
      filtered,
      "cryptoPress",
      "Sin NDP oficiales.",
      (item) => {
        const title = getTranslatedTitle(item, "Nota de prensa");
        const source = normalizeText(item.source, "N/D");
        return `
        <div class="list-item">
          <div>
            <div class="list-title">${title}</div>
            <div class="list-meta">${[item.symbol, item.date].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">Fuente oficial: ${source}</div>
          </div>
          <a class="link" href="${item.link}" target="_blank" rel="noopener">Leer</a>
        </div>
      `;
      }
    );
  } catch (error) {
    const cached = readListCache(CRYPTO_PRESS_STORAGE_KEY);
    if (cached.length) {
      const filtered = filterWindowWithLatest(
        cached,
        CRYPTO_EVENT_WINDOW_HOURS,
        (item) => (item.symbol || "").toString().trim()
      );
      renderEventList(
        dom.cryptoPressList,
        filtered,
        "cryptoPress",
        "Sin NDP oficiales.",
        (item) => {
          const title = getTranslatedTitle(item, "Nota de prensa");
          const source = normalizeText(item.source, "N/D");
          return `
        <div class="list-item">
          <div>
            <div class="list-title">${title}</div>
            <div class="list-meta">${[item.symbol, item.date].filter(Boolean).join(" · ")}</div>
            <div class="list-meta">Fuente oficial: ${source}</div>
          </div>
          <a class="link" href="${item.link}" target="_blank" rel="noopener">Leer</a>
        </div>
      `;
        }
      );
      return;
    }
    dom.cryptoPressList.innerHTML = `<div class="empty">${error.message}</div>`;
  }
}

async function loadCryptoNews() {
  if (!dom.cryptoNewsList) return;
  dom.cryptoNewsList.innerHTML = "<div class=\"empty\">Cargando noticias cripto...</div>";
  try {
    const items = mergeCachedItems(
      CRYPTO_NEWS_STORAGE_KEY,
      await fetchCryptoNews(),
      LIST_CACHE_LIMIT
    );
    const filtered = filterWindowWithLatest(
      items,
      CRYPTO_EVENT_WINDOW_HOURS,
      (item) => formatCryptoNewsSymbol(item.symbol)
    );
    renderEventList(
      dom.cryptoNewsList,
      filtered,
      "cryptoNews",
      "Sin noticias cripto recientes.",
      (item) => {
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
      }
    );
  } catch (error) {
    const cached = readListCache(CRYPTO_NEWS_STORAGE_KEY);
    if (cached.length) {
      const filtered = filterWindowWithLatest(
        cached,
        CRYPTO_EVENT_WINDOW_HOURS,
        (item) => formatCryptoNewsSymbol(item.symbol)
      );
      renderEventList(
        dom.cryptoNewsList,
        filtered,
        "cryptoNews",
        "Sin noticias cripto recientes.",
        (item) => {
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
        }
      );
      return;
    }
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
      const change = (item.change === null || item.change === undefined)
        ? NaN
        : Number(item.change);
      const changePercent = (item.changePercent === null || item.changePercent === undefined)
        ? NaN
        : Number(item.changePercent);
      if (!Number.isFinite(price)) {
        updateRow(row, { error: "Sin datos" });
        return;
      }

      const updatedAtMs = Number.isFinite(item.updatedAt)
        ? item.updatedAt * 1000
        : Date.now();
      const session = resolveMarketState(item.marketState, updatedAtMs);
      const updatedAt = new Date(updatedAtMs);
      const volume = toNumberOrNaN(item.volume);
      const dayLow = toNumberOrNaN(item.dayLow);
      const dayHigh = toNumberOrNaN(item.dayHigh);
      const week52Low = toNumberOrNaN(item.week52Low);
      const week52High = toNumberOrNaN(item.week52High);

      latestStocks.set(symbol, {
        symbol,
        price,
        change,
        changePercent,
        session,
        updatedAt: updatedAtMs,
        volume,
        dayLow,
        dayHigh,
        week52Low,
        week52High,
      });
      updateRow(row, {
        price,
        change: Number.isFinite(change) ? change : 0,
        changePercent: Number.isFinite(changePercent) ? changePercent : 0,
        session,
        volume,
        dayLow,
        dayHigh,
        week52Low,
        week52High,
        updatedAt,
      });
      okCount += 1;
    });

    setStatus(dom.stockStatus, `Acciones: ${okCount}/${STOCKS.length}`);
    if (payload.meta && payload.meta.error) {
      setStatus(
        dom.stockStatus,
        `Acciones: ${okCount}/${STOCKS.length} · ${payload.meta.error}`
      );
    }
    dom.lastUpdated.textContent = formatTime(new Date());
    applyFilter("stocks");
    evaluateAlerts();
  } catch (error) {
    const message = (error && error.message) ? error.message : "Error API";
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
      const changeValue = Number(item.priceChange);
      const changePercent = normalizeChangePercent(
        Number(item.priceChangePercent),
        price,
        changeValue
      );
      latestCryptos.set(pair.label, {
        symbol: pair.label,
        price,
        change: changeValue,
        changePercent,
        updatedAt: updatedAtMs,
      });
      updateRow(row, {
        price,
        change: changeValue,
        changePercent: Number.isFinite(changePercent) ? changePercent : 0,
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
      const target = event.target;
      const dataset = target && target.dataset ? target.dataset : null;
      const action = dataset ? dataset.action : null;
      const id = dataset ? dataset.id : null;
      if (!action || !id) return;
      if (action === "remove") removeAlert(id);
      if (action === "reset") resetAlert(id);
    });
  }
  document.addEventListener("click", (event) => {
    const target = event.target;
    const button = target && target.closest
      ? target.closest("[data-toggle]")
      : null;
    const toggle = button && button.dataset ? button.dataset.toggle : null;
    if (!toggle || !(toggle in listExpansion)) return;
    listExpansion[toggle] = !listExpansion[toggle];
    if (toggle === "filings") loadFilings();
    if (toggle === "press") loadPress();
    if (toggle === "news") loadNews();
  });
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
