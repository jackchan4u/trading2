import gzip
import json
import os
import re
import socket
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
from collections import deque
from datetime import datetime, time as dt_time
from email.utils import parsedate_to_datetime
from html import unescape
from zoneinfo import ZoneInfo
import xml.etree.ElementTree as ET

from flask import Flask, jsonify, request, send_from_directory

DEFAULT_SYMBOLS = [
    "NVDA",
    "MRVL",
    "AMD",
    "UNH",
    "QBTS",
    "APLD",
    "SOUN",
    "CRWV",
    "CLSK",
]

TWELVE_CREDITS_PER_MINUTE = int(os.environ.get("TWELVE_CREDITS_PER_MINUTE", "8"))
TWELVE_DAILY_LIMIT = int(os.environ.get("TWELVE_DAILY_LIMIT", "800"))
DEFAULT_MIN_SYMBOL_REFRESH_SEC = 20 * 60
_symbol_cache = {}
_credit_log = deque()
_refresh_lock = threading.Lock()
_twelve_daily_used = 0
_twelve_daily_date = None

app = Flask(__name__, static_folder=".", static_url_path="")
TWELVE_DATA_URL = "https://api.twelvedata.com/quote"
TWELVE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}
NASDAQ_DATA_URL = "https://api.nasdaq.com/api/quote/{symbol}/info?assetclass=stocks"
NASDAQ_SUMMARY_URL = (
    "https://api.nasdaq.com/api/quote/{symbol}/summary?assetclass=stocks"
)
NASDAQ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}
MARKET_TZ = ZoneInfo("America/New_York")
CONFIG_PATH = os.environ.get(
    "CONFIG_PATH", os.path.join(os.path.dirname(__file__), "config.json")
)
SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSION_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
SEC_CACHE_PATH = os.environ.get(
    "SEC_CACHE_PATH", os.path.join(os.path.dirname(__file__), "sec_tickers.json")
)
SEC_CACHE_TTL = 60 * 60 * 12
BASELINE_CACHE_PATH = os.environ.get(
    "BASELINE_CACHE_PATH",
    os.path.join(os.path.dirname(__file__), "previous_close.json"),
)
BASELINE_UPDATE_CUTOFF = dt_time(16, 5)
NEWS_FEED_URL = (
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
)
NEWS_CACHE_TTL = 60 * 5
NEWS_LIMIT = 12
PRESS_LIMIT = 12
NEWS_FETCH_LIMIT = max(1, int(os.environ.get("NEWS_FETCH_LIMIT", "30")))
NEWS_PER_SYMBOL_LIMIT = max(1, int(os.environ.get("NEWS_PER_SYMBOL_LIMIT", "6")))
FILINGS_LIMIT = 12
FILINGS_PER_SYMBOL_LIMIT = max(
    1, int(os.environ.get("FILINGS_PER_SYMBOL_LIMIT", "6"))
)
FILINGS_CACHE_PATH = os.environ.get(
    "FILINGS_CACHE_PATH", os.path.join(os.path.dirname(__file__), "filings_cache.json")
)
FILINGS_CACHE_MAX = max(1, int(os.environ.get("FILINGS_CACHE_MAX", "500")))
FILINGS_CACHE_VERSION = "v12"
TARGET_FILING_FORMS = {
    "8-K",
    "8-K/A",
    "10-K",
    "10-K/A",
    "4",
    "4/A",
    "144",
    "144/A",
}
STOOQ_URL = "https://stooq.com/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
KNOWN_CIKS = {
    "NVDA": "0001045810",
    "MRVL": "0001835632",
    "AMD": "0000002488",
    "UNH": "0000731766",
    "SOUN": "0001850453",
    "APLD": "0001787195",
}
NEWS_HEADERS = {
    "User-Agent": "PulseBoard/1.0 (local)",
    "Accept": "application/rss+xml",
}
STOOQ_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/csv",
}
_ticker_cik_cache = {}
_ticker_cik_loaded_at = 0.0
_filings_cache = {}
_news_cache = {}
_press_cache = {}
_translation_cache = {}
_baseline_cache = None
TRANSLATE_DEFAULT_URLS = (
    "https://translate.googleapis.com/translate_a/single",
    "https://libretranslate.de/translate",
    "https://translate.astian.org/translate",
    "https://libretranslate.com/translate",
)
OPENAI_TRANSLATE_URL = "https://api.openai.com/v1/responses"
OPENAI_DEFAULT_MODEL = "gpt-4o-mini"
TRANSLATE_TIMEOUT = 6
TRANSLATE_CACHE_TTL = 60 * 60 * 12
TRANSLATE_HEADERS = {
    "User-Agent": "PulseBoard/1.0 (local)",
    "Accept": "application/json",
}
WEB_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
OPENAI_TIMEOUT = 12
OPENAI_MAX_OUTPUT_TOKENS = 1200
ANALYSIS_CACHE_TTL = 60 * 60 * 12
MAX_FILING_TEXT_CHARS = 6000
_analysis_cache = {}
_filing_text_cache = {}
_processed_filings_cache = None


def load_config():
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


CONFIG = load_config()


def _sec_headers():
    config = load_config()
    user_agent = (
        os.environ.get("SEC_USER_AGENT", "").strip()
        or str(config.get("secUserAgent", "")).strip()
    )
    if not user_agent or "you@example.com" in user_agent:
        raise ValueError(
            "Configura secUserAgent en config.json con un email real."
        )
    return {"User-Agent": user_agent, "Accept": "application/json"}


def _fetch_json(url, headers, timeout=8):
    request_obj = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request_obj, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        if exc.code == 403 and "sec.gov" in url:
            raise ValueError(
                "SEC 403: configura secUserAgent en config.json con un contacto real."
            ) from exc
        raise
    return json.loads(payload)


def _fetch_text(url, headers, timeout=8):
    request_obj = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request_obj, timeout=timeout) as response:
        return response.read().decode("utf-8")


def _read_response_text(response):
    payload = response.read()
    encoding = response.headers.get("Content-Encoding", "").lower()
    if "gzip" in encoding:
        payload = gzip.decompress(payload)
    return payload.decode("utf-8")


def _fetch_nasdaq_json(url, timeout=8):
    request_obj = urllib.request.Request(url, headers=NASDAQ_HEADERS)
    with urllib.request.urlopen(request_obj, timeout=timeout) as response:
        payload = _read_response_text(response)
    return json.loads(payload)


def _get_translation_settings():
    config = load_config()
    disabled = os.environ.get("TRANSLATE_DISABLED", "").strip().lower()
    if disabled in ("1", "true", "yes"):
        return None
    enabled = config.get("translateEnabled")
    if isinstance(enabled, bool) and not enabled:
        return None
    provider = (
        os.environ.get("TRANSLATE_PROVIDER", "").strip()
        or str(config.get("translateProvider", "")).strip()
    ).lower()
    url_setting = os.environ.get("TRANSLATE_API_URL", "").strip()
    url_list_setting = os.environ.get("TRANSLATE_API_URLS", "").strip()
    config_url_setting = config.get("translateApiUrl", "")
    urls = []
    if url_list_setting:
        urls = [item.strip() for item in url_list_setting.split(",") if item.strip()]
    elif url_setting:
        urls = [url_setting]
    elif isinstance(config_url_setting, list):
        urls = [str(item).strip() for item in config_url_setting if str(item).strip()]
    elif isinstance(config_url_setting, str) and config_url_setting.strip():
        urls = [config_url_setting.strip()]
    target = (
        os.environ.get("TRANSLATE_TARGET", "").strip()
        or str(config.get("translateTarget", "")).strip()
        or "es"
    )
    source = (
        os.environ.get("TRANSLATE_SOURCE", "").strip()
        or str(config.get("translateSource", "")).strip()
        or "auto"
    )
    api_key = (
        os.environ.get("TRANSLATE_API_KEY", "").strip()
        or str(config.get("translateApiKey", "")).strip()
    )
    if provider == "openai":
        openai_key = api_key or os.environ.get("OPENAI_API_KEY", "").strip()
        if not openai_key or not target:
            return None
        openai_url = urls[0] if urls else OPENAI_TRANSLATE_URL
        model = (
            os.environ.get("TRANSLATE_MODEL", "").strip()
            or str(config.get("translateModel", "")).strip()
            or OPENAI_DEFAULT_MODEL
        )
        return {
            "endpoints": [{"provider": "openai", "url": openai_url}],
            "target": target,
            "source": source,
            "key": openai_key,
            "model": model,
        }
    if not urls:
        urls = list(TRANSLATE_DEFAULT_URLS)
    if not urls or not target:
        return None
    endpoints = []
    for url in urls:
        if "translate.googleapis.com/translate_a/single" in url:
            endpoint_provider = "google"
        elif "api.openai.com" in url:
            endpoint_provider = "openai"
        elif provider:
            endpoint_provider = provider
        else:
            endpoint_provider = "libretranslate"
        endpoints.append({"provider": endpoint_provider, "url": url})
    return {
        "endpoints": endpoints,
        "target": target,
        "source": source,
        "key": api_key,
    }


def _translation_cache_get(key):
    cached = _translation_cache.get(key)
    if not cached:
        return None
    if (time.time() - cached["time"]) >= TRANSLATE_CACHE_TTL:
        del _translation_cache[key]
        return None
    return cached["value"]


def _translate_text_libretranslate(text, settings, url):
    payload = {
        "q": text,
        "source": settings["source"],
        "target": settings["target"],
        "format": "text",
    }
    if settings.get("key"):
        payload["api_key"] = settings["key"]
    data = json.dumps(payload).encode("utf-8")
    request_obj = urllib.request.Request(
        url,
        data=data,
        headers={
            **TRANSLATE_HEADERS,
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request_obj, timeout=TRANSLATE_TIMEOUT) as response:
            raw = response.read().decode("utf-8")
    except Exception:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    translated = None
    if isinstance(parsed, dict):
        translated = (
            parsed.get("translatedText")
            or parsed.get("translation")
            or parsed.get("translated_text")
        )
    elif isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        translated = (
            parsed[0].get("translatedText")
            or parsed[0].get("translation")
            or parsed[0].get("translated_text")
        )
    if not isinstance(translated, str):
        return None
    translated = translated.strip()
    return translated or None


def _translate_text_google(text, settings, url):
    params = urllib.parse.urlencode(
        {
            "client": "gtx",
            "sl": settings["source"] or "auto",
            "tl": settings["target"],
            "dt": "t",
            "q": text,
        }
    )
    request_url = f"{url}?{params}"
    request_obj = urllib.request.Request(
        request_url, headers=TRANSLATE_HEADERS
    )
    try:
        with urllib.request.urlopen(request_obj, timeout=TRANSLATE_TIMEOUT) as response:
            raw = response.read().decode("utf-8")
    except Exception:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(parsed, list) or not parsed:
        return None
    parts = parsed[0]
    if not isinstance(parts, list):
        return None
    translated_parts = []
    for segment in parts:
        if not isinstance(segment, list) or not segment:
            continue
        if isinstance(segment[0], str):
            translated_parts.append(segment[0])
    translated = "".join(translated_parts).strip()
    return translated or None


def _translate_text_openai(text, settings, url):
    translations = _translate_texts_openai([text], settings, url)
    return translations.get(text)


def _translate_texts_openai(texts, settings, url=None):
    if not texts:
        return {}
    api_key = settings.get("key")
    if not api_key:
        return {}
    if not url:
        for endpoint in settings.get("endpoints", []):
            if endpoint.get("provider") == "openai":
                url = endpoint.get("url")
                break
    if not url:
        return {}
    model = settings.get("model") or OPENAI_DEFAULT_MODEL
    target = settings.get("target") or "es"
    prompt = (
        f"Translate the following English titles to {target}. "
        "Return ONLY a JSON array of strings in the same order."
    )
    payload = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [{"type": "text", "text": prompt}],
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": json.dumps(texts, ensure_ascii=True)}
                ],
            },
        ],
        "temperature": 0.2,
        "max_output_tokens": 1000,
    }
    data = json.dumps(payload).encode("utf-8")
    request_obj = urllib.request.Request(
        url,
        data=data,
        headers={
            **TRANSLATE_HEADERS,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
    )
    try:
        with urllib.request.urlopen(request_obj, timeout=TRANSLATE_TIMEOUT) as response:
            raw = response.read().decode("utf-8")
    except Exception:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    output_text = None
    if isinstance(parsed, dict):
        for item in parsed.get("output", []) or []:
            for content in item.get("content", []) or []:
                if content.get("type") == "output_text":
                    output_text = content.get("text")
                    break
            if output_text:
                break
    if not output_text:
        return {}
    try:
        translated_list = json.loads(output_text)
    except json.JSONDecodeError:
        start = output_text.find("[")
        end = output_text.rfind("]")
        if start == -1 or end == -1 or end <= start:
            return {}
        try:
            translated_list = json.loads(output_text[start : end + 1])
        except json.JSONDecodeError:
            return {}
    if not isinstance(translated_list, list):
        return {}
    translations = {}
    for index, translated in enumerate(translated_list):
        if index >= len(texts):
            break
        if isinstance(translated, str) and translated.strip():
            translations[texts[index]] = translated.strip()
    return translations


def _translate_text(text, settings):
    if not text:
        return None
    endpoints = settings.get("endpoints") or []
    for endpoint in endpoints:
        provider = endpoint.get("provider")
        url = endpoint.get("url")
        if not url:
            continue
        if provider == "google":
            translated = _translate_text_google(text, settings, url)
        elif provider == "openai":
            translated = _translate_text_openai(text, settings, url)
        else:
            translated = _translate_text_libretranslate(text, settings, url)
        if translated:
            return translated
    return None


def _has_provider(settings, provider):
    endpoints = settings.get("endpoints") or []
    for endpoint in endpoints:
        if endpoint.get("provider") == provider:
            return True
    return False


def _without_provider(settings, provider):
    endpoints = [ep for ep in settings.get("endpoints", []) if ep.get("provider") != provider]
    trimmed = dict(settings)
    trimmed["endpoints"] = endpoints
    return trimmed


def _translate_texts(texts, settings):
    if not texts:
        return {}
    translations = {}
    now = time.time()
    unique_texts = list(dict.fromkeys(texts))
    for text in unique_texts:
        key = (settings["source"], settings["target"], text)
        cached = _translation_cache_get(key)
        if cached:
            translations[text] = cached
    missing = [text for text in unique_texts if text not in translations]
    if missing and _has_provider(settings, "openai"):
        batch = _translate_texts_openai(missing, settings)
        for text, translated in batch.items():
            key = (settings["source"], settings["target"], text)
            _translation_cache[key] = {"time": now, "value": translated}
            translations[text] = translated
        missing = [text for text in missing if text not in translations]
    if missing:
        fallback_settings = _without_provider(settings, "openai")
        for text in missing:
            translated = _translate_text(text, fallback_settings)
            if translated:
                key = (settings["source"], settings["target"], text)
                _translation_cache[key] = {"time": now, "value": translated}
                translations[text] = translated
    return translations


def _apply_title_translations(items):
    settings = _get_translation_settings()
    if not settings or not items:
        return items
    titles = []
    for item in items:
        title = item.get("title")
        if title:
            normalized = str(title).strip()
            if normalized:
                titles.append(normalized)
    if not titles:
        return items
    translations = _translate_texts(titles, settings)
    for item in items:
        title = item.get("title")
        if not title:
            continue
        normalized = str(title).strip()
        translated = translations.get(normalized)
        if translated and translated != normalized:
            item["titleTranslated"] = translated
    return items


def _analysis_cache_get(key):
    cached = _analysis_cache.get(key)
    if not cached:
        return None
    if (time.time() - cached["time"]) >= ANALYSIS_CACHE_TTL:
        del _analysis_cache[key]
        return None
    return cached["value"]


def _analysis_cache_set(key, value):
    _analysis_cache[key] = {"time": time.time(), "value": value}


def _get_openai_settings():
    config = load_config()
    api_key = (
        os.environ.get("OPENAI_API_KEY", "").strip()
        or str(config.get("openaiApiKey", "")).strip()
    )
    if not api_key:
        return None
    url = (
        os.environ.get("OPENAI_API_URL", "").strip()
        or str(config.get("openaiApiUrl", "")).strip()
        or OPENAI_TRANSLATE_URL
    )
    model = (
        os.environ.get("OPENAI_MODEL", "").strip()
        or str(config.get("openaiModel", "")).strip()
        or OPENAI_DEFAULT_MODEL
    )
    return {"api_key": api_key, "url": url, "model": model}


def _openai_request(payload, settings):
    data = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    request_obj = urllib.request.Request(
        settings["url"],
        data=data,
        headers={
            **TRANSLATE_HEADERS,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {settings['api_key']}",
        },
    )
    try:
        with urllib.request.urlopen(request_obj, timeout=OPENAI_TIMEOUT) as response:
            raw = response.read().decode("utf-8")
    except Exception:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    output_text = None
    if isinstance(parsed, dict):
        for item in parsed.get("output", []) or []:
            for content in item.get("content", []) or []:
                if content.get("type") == "output_text":
                    output_text = content.get("text")
                    break
            if output_text:
                break
    return output_text


def _parse_json_value(raw):
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("[")
        end = raw.rfind("]")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(raw[start : end + 1])
            except json.JSONDecodeError:
                return None
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(raw[start : end + 1])
            except json.JSONDecodeError:
                return None
    return None


def _strip_html(value):
    if not value:
        return ""
    cleaned = re.sub(r"(?is)<script.*?>.*?</script>", " ", value)
    cleaned = re.sub(r"(?is)<style.*?>.*?</style>", " ", cleaned)
    cleaned = re.sub(r"(?is)<head.*?>.*?</head>", " ", cleaned)
    cleaned = re.sub(r"(?is)<[^>]+>", " ", cleaned)
    cleaned = unescape(cleaned)
    return " ".join(cleaned.split())


def _filing_text_cache_get(link):
    cached = _filing_text_cache.get(link)
    if not cached:
        return None
    if (time.time() - cached["time"]) >= ANALYSIS_CACHE_TTL:
        del _filing_text_cache[link]
        return None
    return {
        "text": cached.get("text", ""),
        "error": cached.get("error", ""),
    }


def _filing_text_cache_set(link, text, error=""):
    _filing_text_cache[link] = {
        "time": time.time(),
        "text": text,
        "error": error,
    }


def _load_processed_filings_cache():
    global _processed_filings_cache
    if _processed_filings_cache is not None:
        return _processed_filings_cache
    try:
        with open(FILINGS_CACHE_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            _processed_filings_cache = data
        else:
            _processed_filings_cache = {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        _processed_filings_cache = {}
    return _processed_filings_cache


def _save_processed_filings_cache():
    cache = _load_processed_filings_cache()
    if len(cache) > FILINGS_CACHE_MAX:
        items = sorted(
            cache.items(),
            key=lambda entry: entry[1].get("processedAt", 0),
            reverse=True,
        )[:FILINGS_CACHE_MAX]
        cache = dict(items)
        _processed_filings_cache.clear()
        _processed_filings_cache.update(cache)
    try:
        with open(FILINGS_CACHE_PATH, "w", encoding="utf-8") as handle:
            json.dump(cache, handle, ensure_ascii=True)
    except OSError:
        return


def _load_baseline_cache():
    global _baseline_cache
    if _baseline_cache is not None:
        return _baseline_cache
    try:
        with open(BASELINE_CACHE_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        _baseline_cache = data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        _baseline_cache = {}
    return _baseline_cache


def _save_baseline_cache():
    cache = _load_baseline_cache()
    tmp_path = f"{BASELINE_CACHE_PATH}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(cache, handle, ensure_ascii=True, indent=2)
    os.replace(tmp_path, BASELINE_CACHE_PATH)


def _get_baseline(symbol):
    cache = _load_baseline_cache()
    entry = cache.get(str(symbol).upper())
    if not isinstance(entry, dict):
        return None, ""
    close_value = _to_float(entry.get("close"))
    date_label = entry.get("date") or ""
    return close_value, date_label


def _set_baseline(symbol, close_value, date_label):
    if close_value is None:
        return
    cache = _load_baseline_cache()
    cache[str(symbol).upper()] = {
        "close": close_value,
        "date": date_label or "",
    }
    _save_baseline_cache()


def _log_price_metrics(symbol, last_price, previous_close, variation, variation_pct):
    timestamp = datetime.utcnow().isoformat()
    app.logger.info(
        "price_metrics ticker=%s last_price=%s previous_close=%s variation=%s variation_pct=%s timestamp=%s",
        symbol,
        last_price,
        previous_close,
        variation,
        variation_pct,
        timestamp,
    )


def _get_processed_filing(link):
    if not link:
        return None
    cache = _load_processed_filings_cache()
    entry = cache.get(link)
    if not isinstance(entry, dict):
        return None
    if entry.get("cacheVersion") != FILINGS_CACHE_VERSION:
        return None
    return entry


def _set_processed_filing(link, payload):
    if not link:
        return
    cache = _load_processed_filings_cache()
    payload["cacheVersion"] = FILINGS_CACHE_VERSION
    cache[link] = payload
    _save_processed_filings_cache()


def _fetch_filing_payload(link):
    if not link:
        return "", "Falta la URL del filing."
    try:
        headers = _sec_headers()
    except Exception as exc:
        error = str(exc) or "No se pudo configurar el User-Agent para SEC."
        return "", error
    headers["Accept"] = "text/html,application/xml,text/xml,text/plain"
    try:
        request_obj = urllib.request.Request(link, headers=headers)
        with urllib.request.urlopen(request_obj, timeout=8) as response:
            raw = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            try:
                payload = raw.decode(charset)
            except (LookupError, UnicodeDecodeError):
                payload = raw.decode("utf-8", errors="replace")
    except Exception as exc:
        return "", f"Error descargando el filing: {exc}"
    if not payload:
        return "", "Documento vacio o no legible."
    return payload, ""


def _fetch_filing_text(link):
    if not link:
        return "", "Falta la URL del filing."
    cached = _filing_text_cache_get(link)
    if cached is not None:
        return cached["text"], cached.get("error", "")
    payload, error = _fetch_filing_payload(link)
    if error:
        _filing_text_cache_set(link, "", error)
        return "", error
    text = _strip_html(payload)
    if len(text) > MAX_FILING_TEXT_CHARS:
        text = text[:MAX_FILING_TEXT_CHARS]
    if not text:
        error = "Documento vacio o no legible."
        _filing_text_cache_set(link, "", error)
        return "", error
    _filing_text_cache_set(link, text, "")
    return text, ""


def _analysis_key(kind, item):
    link = item.get("link") or ""
    if link:
        if kind == "filing":
            return (kind, "v2", link)
        return (kind, link)
    title = item.get("title") or ""
    date = item.get("date") or ""
    symbol = item.get("symbol") or ""
    form = item.get("form") or ""
    if kind == "filing":
        return (kind, "v2", title, date, symbol, form)
    return (kind, title, date, symbol, form)


def _is_target_filing_form(form):
    if not form:
        return False
    return str(form).strip().upper() in TARGET_FILING_FORMS


def _extract_first_number(pattern, text):
    if not text:
        return None
    match = re.search(pattern, text, re.IGNORECASE)
    if not match:
        return None
    raw = match.group(1)
    raw = raw.replace(",", "")
    try:
        value = float(raw)
    except ValueError:
        return None
    return int(value) if value.is_integer() else value


def _infer_event_type(form, text):
    form = (form or "").strip().upper()
    if form.startswith("4"):
        return "Transaccion insider (Form 4)"
    if form.startswith("144"):
        return "Aviso de venta (Form 144)"
    text_lower = (text or "").lower()
    candidates = [
        ("Resultados financieros", ["results of operations", "earnings release", "financial results"]),
        ("Contrato material", ["material definitive agreement", "definitive agreement"]),
        ("Financiacion", ["credit agreement", "notes", "financing", "loan agreement", "at-the-market"]),
        ("M&A", ["acquisition", "merger", "combination", "purchase agreement"]),
        ("Reestructuracion", ["bankruptcy", "restructuring", "insolvency"]),
        ("Listado", ["delisting", "listing", "nasdaq", "nyse", "notice of suspension"]),
    ]
    for label, keywords in candidates:
        if any(keyword in text_lower for keyword in keywords):
            return label
    return "Evento corporativo (8-K)"


def _infer_insider_action(form, text):
    form = (form or "").strip().upper()
    if not (form.startswith("4") or form.startswith("144")):
        return "no aplica"
    text_lower = (text or "").lower()
    buy_terms = ("purchase", "acquired", "buy", "bought")
    sell_terms = ("sale", "sold", "dispose", "disposed")
    has_buy = any(term in text_lower for term in buy_terms)
    has_sell = any(term in text_lower for term in sell_terms)
    if has_buy and has_sell:
        return "mixto"
    if has_buy:
        return "compra"
    if has_sell:
        return "venta"
    return "desconocido"


def _infer_dilutive(form, text):
    form = (form or "").strip().upper()
    if form.startswith("4") or form.startswith("144"):
        return "no"
    text_lower = (text or "").lower()
    dilutive_terms = (
        "dilution",
        "dilutive",
        "equity offering",
        "common stock",
        "issuance of shares",
        "registered offering",
        "private placement",
    )
    if any(term in text_lower for term in dilutive_terms):
        return "si"
    return "desconocido"


def _fallback_filing_analysis(item, content):
    symbol = item.get("symbol") or ""
    form = item.get("form") or ""
    date = item.get("date") or ""
    event_type = _infer_event_type(form, content)
    insider_action = _infer_insider_action(form, content)
    shares = _extract_first_number(r"([0-9][0-9,\\.]+)\\s+shares", content)
    value = _extract_first_number(r"\\$\\s*([0-9][0-9,\\.]+)", content)
    dilutive = _infer_dilutive(form, content)
    impact = "bajo"
    if event_type in ("M&A", "Financiacion", "Reestructuracion"):
        impact = "alto"
    elif event_type in ("Resultados financieros", "Contrato material"):
        impact = "medio"
    summary = f"{symbol} presento {form} el {date}. Evento: {event_type}."
    if insider_action not in ("no aplica", "desconocido"):
        summary = f"{summary} Insider: {insider_action}."
    return {
        "summary": summary.strip(),
        "impact": impact,
        "eventType": event_type,
        "insiderAction": insider_action,
        "shares": shares,
        "value": value,
        "dilutive": dilutive,
    }


def _strip_xml_ns(tag):
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _xml_bool(value):
    return str(value).strip().lower() in ("1", "true", "yes")


def _build_xml_text_map(root):
    mapping = {}
    for elem in root.iter():
        tag = _strip_xml_ns(elem.tag).lower()
        text = (elem.text or "").strip()
        if not text:
            continue
        mapping.setdefault(tag, []).append(text)
    return mapping


def _xml_first_text(mapping, candidates):
    for name in candidates:
        values = mapping.get(name.lower())
        if values:
            return values[0]
    return ""


def _xml_value_in(node, parent_tag):
    target = parent_tag.lower()
    for parent in node.iter():
        if _strip_xml_ns(parent.tag).lower() == target:
            for child in parent.iter():
                if _strip_xml_ns(child.tag).lower() == "value":
                    if child.text and child.text.strip():
                        return child.text.strip()
    return ""


def _parse_number_value(value):
    if value is None:
        return None
    text = str(value)
    match = re.search(r"-?\d[\d,]*\.?\d*", text)
    if not match:
        return None
    cleaned = match.group(0).replace(",", "").strip()
    if cleaned == "" or cleaned == "-":
        return None
    try:
        num = float(cleaned)
    except ValueError:
        return None
    return int(num) if num.is_integer() else num


def _sanitize_xml(payload):
    return re.sub(
        r"&(?![a-zA-Z]+;|#\d+;|#x[0-9a-fA-F]+;)",
        "&amp;",
        payload,
    )


def _extract_xml_fragment(payload, tag):
    pattern = rf"(<{tag}\\b[^>]*>.*?</{tag}>)"
    match = re.search(pattern, payload, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return match.group(1)


def _extract_tag_value(payload, tag):
    pattern = rf"<(?:\\w+:)?{tag}\\b[^>]*>(.*?)</(?:\\w+:)?{tag}>"
    match = re.search(pattern, payload, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return unescape(match.group(1)).strip()


def _extract_open_tag_value(payload, tag):
    pattern = rf"<(?:\\w+:)?{tag}\\b[^>]*>\\s*([^<]+)"
    match = re.search(pattern, payload, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return unescape(match.group(1)).strip()


def _extract_first_open_tag_value(payload, tags):
    for tag in tags:
        value = _extract_open_tag_value(payload, tag)
        if value:
            return value
    return ""


def _extract_first_tag_value(payload, tags):
    for tag in tags:
        value = _extract_tag_value(payload, tag)
        if value:
            return value
    return ""


def _extract_tag_pairs_loose(payload):
    pairs = re.findall(
        r"<(?:\\w+:)?([A-Za-z0-9_\\-]+)\\b[^>]*>([^<]+)</(?:\\w+:)?\\1>",
        payload,
        re.IGNORECASE | re.DOTALL,
    )
    cleaned = []
    for tag, value in pairs:
        text = unescape(value).strip()
        if not text:
            continue
        cleaned.append((tag, text))
    return cleaned


def _normalize_tag_key(tag):
    return re.sub(r"[^a-z0-9]", "", tag.lower())


def _pick_tag_value(pairs, substrings):
    for tag, value in pairs:
        key = _normalize_tag_key(tag)
        if any(sub in key for sub in substrings):
            return value
    return ""


def _safe_parse_xml(payload, tag_candidates):
    if not payload:
        return None, "Documento vacio o no legible."
    try:
        return ET.fromstring(payload), ""
    except Exception as exc:
        last_error = exc
    candidates = []
    xml_start = payload.find("<?xml")
    if xml_start != -1:
        candidates.append(payload[xml_start:])
    for tag in tag_candidates:
        fragment = _extract_xml_fragment(payload, tag)
        if fragment:
            candidates.append(fragment)
    seen = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        try:
            return ET.fromstring(candidate), ""
        except Exception:
            try:
                return ET.fromstring(_sanitize_xml(candidate)), ""
            except Exception:
                continue
    return None, f"No se pudo parsear XML: {last_error}"


def _extract_number_from_label(text, labels):
    for label in labels:
        match = re.search(
            rf"{label}\\s*[:\\-]?\\s*([$0-9,\\.]+)",
            text,
            re.IGNORECASE,
        )
        if match:
            return _parse_number_value(match.group(1))
    return None


def _extract_number_after_label(text, label, window=240):
    if not text:
        return None
    lowered = text.lower()
    label_lower = label.lower()
    idx = lowered.find(label_lower)
    if idx == -1:
        return None
    start = idx + len(label)
    segment = text[start : start + window]
    match = re.search(r"([$0-9][0-9,.]+)", segment)
    if not match:
        return None
    return _parse_number_value(match.group(1))


def _extract_date_after_label(text, label, window=240):
    if not text:
        return ""
    lowered = text.lower()
    label_lower = label.lower()
    idx = lowered.find(label_lower)
    if idx == -1:
        return ""
    start = idx + len(label)
    segment = text[start : start + window]
    match = re.search(r"(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4})", segment)
    return match.group(1) if match else ""


def _extract_date(text):
    match = re.search(
        r"(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4})",
        text,
    )
    return match.group(1) if match else ""


def _parse_form4_text(text):
    normalized = " ".join((text or "").split())
    if not normalized:
        return None
    action = "desconocido"
    if re.search(r"Acquired\\s+Disposed\\s+Code\\s*A", normalized, re.IGNORECASE):
        action = "compra"
    elif re.search(r"Acquired\\s+Disposed\\s+Code\\s*D", normalized, re.IGNORECASE):
        action = "venta"
    else:
        inferred = _infer_insider_action("4", normalized)
        if inferred in ("compra", "venta", "mixto"):
            action = inferred
    shares = _extract_number_from_label(
        normalized,
        [
            "Transaction Shares",
            "Number of Shares",
            "Shares",
            "Amount of Securities",
        ],
    )
    price = _extract_number_from_label(
        normalized,
        [
            "Transaction Price",
            "Price per Share",
            "Price",
        ],
    )
    if shares is None:
        return None
    txn_type = "desconocido"
    lowered = normalized.lower()
    if "open market" in lowered:
        txn_type = "open market"
    elif "option exercise" in lowered or "exercise of option" in lowered:
        txn_type = "option exercise"
    elif "rsu" in lowered or "restricted stock" in lowered or "award" in lowered:
        txn_type = "rsu/award"
    event_type = "Insider buying" if action == "compra" else "Insider selling"
    summary = f"Form 4: {event_type}. Acciones: {shares}."
    if price:
        summary = f"{summary} Precio medio: {price}."
    return {
        "event_type": event_type,
        "insider_action": action,
        "insider_role": "",
        "shares": shares,
        "value_usd": price * shares if price else None,
        "price": price,
        "transaction_type": txn_type,
        "summary": summary,
        "dilutive": False,
        "impact": "medio",
    }


def _parse_form4_html_table(payload):
    if not payload:
        return []
    match = re.search(
        r"Table I - Non-Derivative Securities.*?</table>",
        payload,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []
    table_html = match.group(0)
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.IGNORECASE | re.DOTALL)
    transactions = []
    txn_type_map = {
        "P": "open market",
        "S": "open market",
        "M": "option exercise",
        "A": "RSU/award",
        "F": "tax/fee",
        "G": "gift",
    }
    for row_html in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.IGNORECASE | re.DOTALL)
        if len(cells) < 8:
            continue
        values = [_strip_html(cell) for cell in cells]
        code = values[3].strip().upper()
        acq_disp = values[6].strip().upper()
        shares = _parse_number_value(values[5])
        price = _parse_number_value(values[7].replace("$", ""))
        if shares is None:
            continue
        action = ""
        if acq_disp == "A":
            action = "buy"
        elif acq_disp == "D":
            action = "sell"
        elif code in ("P", "M", "A"):
            action = "buy"
        elif code in ("S", "F", "D", "G"):
            action = "sell"
        transactions.append(
            {
                "action": action or "desconocido",
                "shares": shares,
                "price": price,
                "type": txn_type_map.get(code, "desconocido"),
            }
        )
    return transactions


def _extract_form4_role(text):
    if not text:
        return ""
    roles = []
    if re.search(r"X\\s+Director", text, re.IGNORECASE):
        roles.append("Director")
    if re.search(r"X\\s+Officer", text, re.IGNORECASE):
        roles.append("Officer")
    if re.search(r"X\\s+10%\\s+Owner", text, re.IGNORECASE):
        roles.append("10% Owner")
    return ", ".join(roles)


def _extract_form144_seller_role(normalized):
    if not normalized:
        return "", ""
    seller = ""
    seller_match = re.search(
        r"Name of Person for Whose Account the Securities Are to Be Sold\s*[:\-]?\s*(.+?)(?=Relationship to Issuer|Name of Issuer|Title of the Securities|CUSIP|$)",
        normalized,
        re.IGNORECASE,
    )
    if seller_match:
        seller = seller_match.group(1).strip()
        if "See the definition" in seller:
            seller = seller.split("See the definition", 1)[0].strip()
    role_match = re.search(
        r"Relationship to Issuer\s*[:\-]?\s*(.+?)(?=Name of Issuer|Title of the Securities|CUSIP|$)",
        normalized,
        re.IGNORECASE,
    )
    role = role_match.group(1).strip() if role_match else ""
    if role:
        for cutoff in ("144:", "Securities Information", "Title of the"):
            if cutoff in role:
                role = role.split(cutoff, 1)[0].strip()
                break
        if "Relationship to Issuer" in role:
            role = ""
    if not role:
        role_parts = []
        if re.search(r"Relationship to Issuer\s+Director", normalized, re.IGNORECASE) or re.search(r"\bDirector\b", normalized):
            role_parts.append("Director")
        if re.search(r"Relationship to Issuer\s+Officer", normalized, re.IGNORECASE) or re.search(r"\bOfficer\b", normalized):
            role_parts.append("Officer")
        if re.search(r"10% Stockholder", normalized, re.IGNORECASE):
            role_parts.append("10% Stockholder")
        if role_parts:
            role = ", ".join(role_parts)
    return seller, role


def _parse_form144_text(text):
    normalized = " ".join((text or "").split())
    if not normalized:
        return None
    seller, role = _extract_form144_seller_role(normalized)
    shares = _extract_number_from_label(
        normalized,
        [
            "Number of Shares",
            "Number of Securities",
            "Aggregate Amount of Securities",
            "Amount of Securities",
        ],
    )
    shares = shares or _extract_number_after_label(
        normalized,
        "Number of Shares or Other Units To Be Sold",
    )
    shares = shares or _extract_number_after_label(
        normalized,
        "Number of Shares",
    )
    value = _extract_number_from_label(
        normalized,
        [
            "Aggregate Market Value",
            "Approximate Market Value",
            "Aggregate Sales Price",
            "Value of Securities",
        ],
    )
    value = value or _extract_number_after_label(
        normalized,
        "Aggregate Market Value",
    )
    value = value or _extract_number_after_label(
        normalized,
        "Approximate Market Value",
    )
    date_raw = _extract_date(normalized)
    if not date_raw:
        date_raw = _extract_date_after_label(
            normalized,
            "Approximate Date of Sale",
        )
    if not date_raw:
        date_raw = _extract_date_after_label(
            normalized,
            "Date of Sale",
        )
    if not (seller or shares or value or date_raw):
        return None
    summary = "Form 144: Insider proposed sale."
    if seller:
        summary = f"{summary} Vendedor: {seller}."
    if shares:
        summary = f"{summary} Acciones: {shares}."
    if value:
        summary = f"{summary} Valor aprox: {value}."
    if date_raw:
        summary = f"{summary} Fecha estimada: {date_raw}."
    return {
        "event_type": "Insider proposed sale",
        "insider_action": "venta",
        "insider_role": role or "",
        "shares": shares,
        "value_usd": value,
        "summary": summary,
        "dilutive": False,
        "impact": "medio",
    }


def _parse_form144_html_table(payload):
    if not payload:
        return {}
    header_match = re.search(
        r"Number of Shares or Other Units To Be Sold",
        payload,
        re.IGNORECASE,
    )
    if not header_match:
        return {}
    after = payload[header_match.start() :]
    rows = re.findall(
        r"</tr>\s*<tr[^>]*>(.*?)</tr>",
        after,
        re.IGNORECASE | re.DOTALL,
    )
    if not rows:
        return {}
    total_shares = None
    total_value = None
    date_raw = ""
    for row_html in rows:
        cells = re.findall(
            r"<td[^>]*>(.*?)</td>",
            row_html,
            re.IGNORECASE | re.DOTALL,
        )
        if len(cells) < 6:
            continue
        values = [_strip_html(cell) for cell in cells]
        shares = _parse_number_value(values[2])
        value = _parse_number_value(values[3])
        date = _extract_date(values[5]) if len(values) > 5 else ""
        if shares is not None:
            total_shares = shares if total_shares is None else total_shares + shares
        if value is not None:
            total_value = value if total_value is None else total_value + value
        if not date_raw and date:
            date_raw = date
    return {
        "shares": total_shares,
        "value": total_value,
        "date": date_raw,
    }


def _parse_form4_payload(payload):
    root, error = _safe_parse_xml(payload, ["ownershipDocument"])
    if not root:
        fallback = _parse_form4_text(_strip_html(payload))
        if fallback:
            return fallback, ""
        html_transactions = _parse_form4_html_table(payload)
        if html_transactions:
            buy_shares = sum(t["shares"] for t in html_transactions if t["action"] == "buy")
            sell_shares = sum(t["shares"] for t in html_transactions if t["action"] == "sell")
            buy_value = sum(
                (t["shares"] * t["price"])
                for t in html_transactions
                if t["action"] == "buy" and t["price"] is not None
            )
            sell_value = sum(
                (t["shares"] * t["price"])
                for t in html_transactions
                if t["action"] == "sell" and t["price"] is not None
            )
            if buy_shares >= sell_shares:
                event_type = "Insider buying"
                shares = buy_shares
                value = buy_value if buy_value else None
                avg_price = (buy_value / buy_shares) if buy_shares and buy_value else None
                action = "compra" if sell_shares == 0 else "mixto"
            else:
                event_type = "Insider selling"
                shares = sell_shares
                value = sell_value if sell_value else None
                avg_price = (sell_value / sell_shares) if sell_shares and sell_value else None
                action = "venta"
            txn_type = next(
                (t["type"] for t in html_transactions if t["action"] in ("buy", "sell")),
                "desconocido",
            )
            role = _extract_form4_role(" ".join(_strip_html(payload).split()))
            summary = f"Form 4: {event_type}."
            if shares:
                summary = f"{summary} Acciones: {shares}."
            if avg_price:
                summary = f"{summary} Precio medio: {avg_price}."
            return {
                "event_type": event_type,
                "insider_action": action,
                "insider_role": role,
                "shares": shares,
                "value_usd": value,
                "price": avg_price,
                "transaction_type": txn_type,
                "summary": summary,
                "dilutive": False,
                "impact": "medio",
            }, ""
        return None, error
    owners = []
    roles = []
    for owner in root.iter():
        if _strip_xml_ns(owner.tag) != "reportingOwner":
            continue
        owner_map = _build_xml_text_map(owner)
        name = _xml_first_text(
            owner_map,
            ["reportingownername", "rptownername", "ownername"],
        )
        if name:
            owners.append(name)
        role_parts = []
        is_director = _xml_bool(_xml_first_text(owner_map, ["isdirector"]))
        is_officer = _xml_bool(_xml_first_text(owner_map, ["isofficer"]))
        is_ten = _xml_bool(_xml_first_text(owner_map, ["istenpercentowner"]))
        is_other = _xml_bool(_xml_first_text(owner_map, ["isother"]))
        officer_title = _xml_first_text(owner_map, ["officertitle"])
        other_text = _xml_first_text(owner_map, ["othertext"])
        if is_director:
            role_parts.append("Director")
        if is_officer:
            role_parts.append(
                f"Officer ({officer_title})" if officer_title else "Officer"
            )
        if is_ten:
            role_parts.append("10% Owner")
        if is_other:
            role_parts.append(other_text or "Other")
        if role_parts:
            roles.append(", ".join(role_parts))
    transactions = []
    for txn in root.iter():
        if _strip_xml_ns(txn.tag) != "nonDerivativeTransaction":
            continue
        shares_raw = _xml_value_in(txn, "transactionShares")
        price_raw = _xml_value_in(txn, "transactionPricePerShare")
        acq_disp = _xml_value_in(txn, "transactionAcquiredDisposedCode").upper()
        txn_map = _build_xml_text_map(txn)
        code = _xml_first_text(txn_map, ["transactioncode"]).upper()
        shares = _parse_number_value(shares_raw)
        price = _parse_number_value(price_raw)
        action = ""
        if acq_disp == "A":
            action = "buy"
        elif acq_disp == "D":
            action = "sell"
        elif code in ("P", "M", "A"):
            action = "buy"
        elif code in ("S", "F", "D"):
            action = "sell"
        txn_type_map = {
            "P": "open market",
            "S": "open market",
            "M": "option exercise",
            "A": "RSU/award",
            "F": "tax/fee",
        }
        txn_type = txn_type_map.get(code, "desconocido")
        if shares is None:
            continue
        transactions.append(
            {
                "action": action or "desconocido",
                "shares": shares,
                "price": price,
                "type": txn_type,
            }
        )
    if not transactions:
        return None, "No se encontraron transacciones en el Form 4."
    buy_shares = sum(t["shares"] for t in transactions if t["action"] == "buy")
    sell_shares = sum(t["shares"] for t in transactions if t["action"] == "sell")
    buy_value = sum(
        (t["shares"] * t["price"])
        for t in transactions
        if t["action"] == "buy" and t["price"] is not None
    )
    sell_value = sum(
        (t["shares"] * t["price"])
        for t in transactions
        if t["action"] == "sell" and t["price"] is not None
    )
    if buy_shares >= sell_shares:
        event_type = "Insider buying"
        shares = buy_shares
        value = buy_value if buy_value else None
        avg_price = (buy_value / buy_shares) if buy_shares and buy_value else None
        action = "compra" if sell_shares == 0 else "mixto"
    else:
        event_type = "Insider selling"
        shares = sell_shares
        value = sell_value if sell_value else None
        avg_price = (sell_value / sell_shares) if sell_shares and sell_value else None
        action = "venta"
    txn_type = next(
        (t["type"] for t in transactions if t["action"] in ("buy", "sell")),
        "desconocido",
    )
    owner_name = owners[0] if owners else ""
    owner_role = roles[0] if roles else ""
    summary = f"Form 4: {event_type}."
    if owner_name:
        summary = f"{summary} Insider: {owner_name}."
    if shares:
        summary = f"{summary} Acciones: {shares}."
    if avg_price:
        summary = f"{summary} Precio medio: {avg_price}."
    return {
        "event_type": event_type,
        "insider_action": action,
        "insider_role": owner_role,
        "shares": shares,
        "value_usd": value,
        "price": avg_price,
        "transaction_type": txn_type,
        "summary": summary,
        "dilutive": False,
        "impact": "medio",
    }, ""


def _parse_form144_payload(payload):
    root, error = _safe_parse_xml(payload, ["form144", "edgarSubmission", "document"])
    if not root:
        seller = _extract_first_tag_value(
            payload,
            [
                "nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold",
                "nameOfPersonForWhoseAccountSecuritiesToBeSold",
                "ownerName",
                "personName",
                "reportingOwnerName",
            ],
        ) or _extract_first_open_tag_value(
            payload,
            [
                "nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold",
                "nameOfPersonForWhoseAccountSecuritiesToBeSold",
                "ownerName",
                "personName",
                "reportingOwnerName",
            ],
        )
        role = _extract_first_tag_value(
            payload,
            [
                "relationshipToIssuer",
                "relationshipOfPersonToIssuer",
                "relationship",
                "officerTitle",
                "title",
            ],
        ) or _extract_first_open_tag_value(
            payload,
            [
                "relationshipToIssuer",
                "relationshipOfPersonToIssuer",
                "relationship",
                "officerTitle",
                "title",
            ],
        )
        shares_raw = _extract_first_tag_value(
            payload,
            [
                "numberOfShares",
                "numberOfSharesProposedToBeSold",
                "amountOfSecuritiesToBeSold",
                "aggregateAmountOfSecuritiesToBeSold",
            ],
        ) or _extract_first_open_tag_value(
            payload,
            [
                "numberOfShares",
                "numberOfSharesProposedToBeSold",
                "amountOfSecuritiesToBeSold",
                "aggregateAmountOfSecuritiesToBeSold",
            ],
        )
        value_raw = _extract_first_tag_value(
            payload,
            [
                "aggregateMarketValue",
                "approximateMarketValue",
                "aggregateSalesPrice",
                "valueOfSecurities",
            ],
        ) or _extract_first_open_tag_value(
            payload,
            [
                "aggregateMarketValue",
                "approximateMarketValue",
                "aggregateSalesPrice",
                "valueOfSecurities",
            ],
        )
        date_raw = _extract_first_tag_value(
            payload,
            [
                "estimatedDateOfSale",
                "dateOfSale",
                "approximateSaleDate",
            ],
        ) or _extract_first_open_tag_value(
            payload,
            [
                "estimatedDateOfSale",
                "dateOfSale",
                "approximateSaleDate",
            ],
        )
        table_data = _parse_form144_html_table(payload)
        if not (seller and role):
            text_normalized = " ".join(_strip_html(payload).split())
            if text_normalized:
                text_seller, text_role = _extract_form144_seller_role(text_normalized)
                seller = seller or text_seller
                role = role or text_role
        shares = _parse_number_value(shares_raw)
        value = _parse_number_value(value_raw)
        if table_data:
            shares = shares or table_data.get("shares")
            value = value or table_data.get("value")
            date_raw = date_raw or table_data.get("date", "")
        if not (seller or shares or value or date_raw):
            pairs = _extract_tag_pairs_loose(payload)
            if pairs:
                seller = seller or _pick_tag_value(
                    pairs,
                    ["personforwhoseaccount", "ownername", "personname", "reportingowner"],
                )
                role = role or _pick_tag_value(
                    pairs,
                    ["relationshiptoissuer", "officertitle", "relationship"],
                )
                shares = shares or _parse_number_value(
                    _pick_tag_value(pairs, ["numberofshares", "amountofsecurities", "aggregatesecurities"])
                )
                value = value or _parse_number_value(
                    _pick_tag_value(pairs, ["aggregatemarketvalue", "salesprice", "valueofsecurities"])
                )
                date_raw = date_raw or _pick_tag_value(
                    pairs,
                    ["estimateddateofsale", "dateofsale", "saledate"],
                )
        if seller or shares or value or date_raw:
            summary = "Form 144: Insider proposed sale."
            if seller:
                summary = f"{summary} Vendedor: {seller}."
            if shares:
                summary = f"{summary} Acciones: {shares}."
            if value:
                summary = f"{summary} Valor aprox: {value}."
            if date_raw:
                summary = f"{summary} Fecha estimada: {date_raw}."
            return {
                "event_type": "Insider proposed sale",
                "insider_action": "venta",
                "insider_role": role or "",
                "shares": shares,
                "value_usd": value,
                "summary": summary,
                "dilutive": False,
                "impact": "medio",
            }, ""
        fallback = _parse_form144_text(_strip_html(payload))
        if fallback:
            return fallback, ""
        return None, error
    mapping = _build_xml_text_map(root)
    seller = _xml_first_text(
        mapping,
        [
            "nameofpersonforwhoseaccountthesecuritiesaretobesold",
            "nameofpersonforwhoseaccountthesecuritiesaretobesold",
            "personname",
            "ownername",
            "reportingownername",
        ],
    )
    role = _xml_first_text(
        mapping,
        [
            "relationshiptoissuer",
            "relationshipofpersontissuer",
            "relationshiptoperson",
            "relationship",
            "officertitle",
            "title",
        ],
    )
    shares_raw = _xml_first_text(
        mapping,
        [
            "numberofshares",
            "numberofsharesproposedtobesold",
            "amountofsecuritiestobesold",
            "aggregateamountofsecuritiestobesold",
        ],
    )
    value_raw = _xml_first_text(
        mapping,
        [
            "aggregatemarketvalue",
            "approximatemarketvalue",
            "aggregatesalesprice",
            "valueofsecurities",
        ],
    )
    date_raw = _xml_first_text(
        mapping,
        [
            "estimateddateofsale",
            "dateofsale",
            "approximatesaledate",
        ],
    )
    shares = _parse_number_value(shares_raw)
    value = _parse_number_value(value_raw)
    summary = "Form 144: Insider proposed sale."
    if seller:
        summary = f"{summary} Vendedor: {seller}."
    if shares:
        summary = f"{summary} Acciones: {shares}."
    if value:
        summary = f"{summary} Valor aprox: {value}."
    if date_raw:
        summary = f"{summary} Fecha estimada: {date_raw}."
    return {
        "event_type": "Insider proposed sale",
        "insider_action": "venta",
        "insider_role": role or "",
        "shares": shares,
        "value_usd": value,
        "summary": summary,
        "dilutive": False,
        "impact": "medio",
    }, ""


def _extract_8k_items(text):
    if not text:
        return []
    matches = re.findall(r"\\bItem\\s+([0-9]{1,2}\\.\\d{2})", text, re.IGNORECASE)
    items = []
    for item in matches:
        normalized = item.strip()
        if normalized not in items:
            items.append(normalized)
    return items


def _classify_8k_event(items, text):
    item_map = {
        "1.01": "Acuerdo material",
        "1.02": "Terminacion de acuerdo",
        "2.01": "M&A",
        "2.02": "Resultados / guidance",
        "2.03": "Financiacion",
        "2.04": "Default o aceleracion",
        "2.05": "Reestructuracion",
        "2.06": "Impairment",
        "3.02": "Venta de acciones",
        "3.03": "Modificacion de derechos",
        "5.02": "Cambios en directivos",
        "7.01": "Divulgacion",
        "8.01": "Otros eventos",
    }
    material_items = {
        "1.01",
        "2.01",
        "2.02",
        "2.03",
        "2.04",
        "2.05",
        "2.06",
        "3.02",
        "3.03",
        "5.02",
    }
    event_type = item_map.get(items[0], "Evento corporativo (8-K)") if items else "Evento corporativo (8-K)"
    material = any(item in material_items for item in items)
    text_lower = (text or "").lower()
    if "guidance" in text_lower and event_type == "Resultados / guidance":
        event_type = "Resultados / guidance"
    dilutive = ("equity offering" in text_lower or "registered offering" in text_lower)
    if "common stock" in text_lower or "private placement" in text_lower:
        dilutive = True
    if "item 3.02" in text_lower:
        dilutive = True
    impact = "alto" if material else "medio"
    return event_type, material, dilutive, impact


def _parse_8k_payload(text):
    items = _extract_8k_items(text)
    event_type, material, dilutive, impact = _classify_8k_event(items, text)
    shares = _extract_first_number(r"([0-9][0-9,\\.]+)\\s+shares", text)
    value = _extract_first_number(r"\\$\\s*([0-9][0-9,\\.]+)", text)
    items_label = ", ".join(items) if items else "N/D"
    summary = f"8-K Items: {items_label}. Evento: {event_type}."
    if material:
        summary = f"{summary} Impacto material: si."
    return {
        "event_type": event_type,
        "insider_action": "no aplica",
        "insider_role": "",
        "shares": shares,
        "value_usd": value,
        "summary": summary,
        "dilutive": bool(dilutive),
        "impact": impact,
        "items": items,
        "material": material,
    }, ""


def _extract_10k_items(text):
    if not text:
        return []
    matches = re.findall(
        r"\\bItem\\s+([0-9]{1,2}[A]?)",
        text,
        re.IGNORECASE,
    )
    items = []
    for item in matches:
        normalized = item.strip().upper()
        if normalized not in items:
            items.append(normalized)
    return items


def _parse_10k_payload(text):
    items = _extract_10k_items(text)
    items_label = ", ".join(items) if items else "N/D"
    event_type = "Reporte anual (10-K)"
    summary = f"10-K Items: {items_label}."
    return {
        "event_type": event_type,
        "insider_action": "no aplica",
        "insider_role": "",
        "shares": None,
        "value_usd": None,
        "summary": summary,
        "dilutive": False,
        "impact": "medio",
        "items": items,
    }, ""


def _process_filing_item(item):
    link = item.get("link") or ""
    cached = _get_processed_filing(link)
    if cached and (cached.get("event_type") or cached.get("documentError")):
        return cached
    symbol = item.get("symbol") or item.get("ticker") or ""
    form = (item.get("form") or item.get("form_type") or "").strip().upper()
    date = item.get("date") or ""
    result = {
        "ticker": symbol,
        "symbol": symbol,
        "form_type": form,
        "form": form,
        "event_type": "",
        "eventType": "",
        "summary": "",
        "shares": None,
        "value_usd": None,
        "value": None,
        "insider_role": "",
        "insiderRole": "",
        "dilutive": False,
        "date": date,
        "link": link,
        "url": link,
        "impact": "",
        "documentError": "",
    }
    parsed = None
    error = ""
    if form.startswith("4"):
        payload, error = _fetch_filing_payload(link)
        if not error:
            parsed, error = _parse_form4_payload(payload)
    elif form.startswith("144"):
        payload, error = _fetch_filing_payload(link)
        if not error:
            parsed, error = _parse_form144_payload(payload)
    elif form.startswith("8-K"):
        text, error = _fetch_filing_text(link)
        if not error:
            parsed, error = _parse_8k_payload(text)
    elif form.startswith("10-K"):
        text, error = _fetch_filing_text(link)
        if not error:
            parsed, error = _parse_10k_payload(text)
    else:
        error = "Form no soportado para procesamiento."
    if error:
        result["documentError"] = error
        result["summary"] = f"Error tecnico: {error}"
        result["event_type"] = "Error tecnico"
        result["eventType"] = "Error tecnico"
        result["impact"] = "bajo"
    elif parsed:
        result.update(parsed)
        result["eventType"] = parsed.get("event_type", "")
        result["insiderRole"] = parsed.get("insider_role", "")
        result["insiderAction"] = parsed.get("insider_action", "")
        result["transactionType"] = parsed.get("transaction_type", "")
        result["value"] = parsed.get("value_usd")
    result["timestamp"] = _parse_iso_date(date) or 0
    result["processedAt"] = time.time()
    _set_processed_filing(link, result)
    return result


def _process_filings(items):
    processed = []
    for item in items:
        processed.append(_process_filing_item(item))
    return processed


def _apply_news_analysis(items):
    settings = _get_openai_settings()
    if not settings or not items:
        return items
    pending = []
    for item in items:
        key = _analysis_key("news", item)
        cached = _analysis_cache_get(key)
        if cached:
            item.update(cached)
        else:
            pending.append((item, key))
    if not pending:
        return items
    payload_items = []
    for index, (item, _) in enumerate(pending):
        payload_items.append(
            {
                "id": index,
                "title": item.get("title") or "",
                "source": item.get("source") or "",
                "link": item.get("link") or "",
                "date": item.get("date") or "",
                "symbol": item.get("symbol") or "",
            }
        )
    prompt = (
        "Clasifica titulares SOLO con la informacion dada. No inventes. "
        "Si no hay hecho verificable o no es oficial, usa clasificacion "
        "\"opinion/ruido\" y ignore=true. Devuelve SOLO un JSON array "
        "con objetos {id, classification, impact, ignore, reason}. "
        "classification: \"hecho confirmado\"|\"narrativa\"|\"opinion/ruido\". "
        "impact: \"alto\"|\"medio\"|\"bajo\". ignore: true/false. "
        "reason: breve, max 120 chars."
    )
    payload = {
        "model": settings["model"],
        "input": [
            {
                "role": "system",
                "content": [{"type": "text", "text": prompt}],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(payload_items, ensure_ascii=True),
                    }
                ],
            },
        ],
        "temperature": 0.2,
        "max_output_tokens": OPENAI_MAX_OUTPUT_TOKENS,
    }
    raw = _openai_request(payload, settings)
    parsed = _parse_json_value(raw)
    if not isinstance(parsed, list):
        return items
    mapped = {}
    for entry in parsed:
        if not isinstance(entry, dict):
            continue
        entry_id = entry.get("id")
        if entry_id is None:
            continue
        try:
            entry_index = int(entry_id)
        except (TypeError, ValueError):
            continue
        mapped[entry_index] = {
            "classification": entry.get("classification"),
            "impact": entry.get("impact"),
            "ignore": entry.get("ignore"),
            "reason": entry.get("reason"),
        }
    for index, (item, key) in enumerate(pending):
        result = mapped.get(index)
        if not result:
            continue
        item.update(result)
        _analysis_cache_set(key, result)
    return items


def _apply_filings_analysis(items):
    if not items:
        return items
    return _process_filings(items)


def _load_cik_cache():
    try:
        with open(SEC_CACHE_PATH, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        if isinstance(data, dict):
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return {}


def _save_cik_cache(data):
    try:
        with open(SEC_CACHE_PATH, "w", encoding="utf-8") as handle:
            json.dump(data, handle)
    except OSError:
        pass


def _get_cik_map():
    global _ticker_cik_cache, _ticker_cik_loaded_at
    now = time.time()
    if _ticker_cik_cache and (now - _ticker_cik_loaded_at) < SEC_CACHE_TTL:
        return _ticker_cik_cache
    cached = _load_cik_cache()
    if cached:
        _ticker_cik_cache = cached
        _ticker_cik_loaded_at = now
        return _ticker_cik_cache

    mapping = dict(KNOWN_CIKS)
    try:
        data = _fetch_json(SEC_TICKER_URL, _sec_headers())
        if isinstance(data, dict):
            for _, item in data.items():
                if not isinstance(item, dict):
                    continue
                ticker = item.get("ticker")
                cik = item.get("cik_str")
                if ticker and cik is not None:
                    mapping[str(ticker).upper()] = str(cik).zfill(10)
    except Exception:
        pass

    if mapping:
        _ticker_cik_cache = mapping
        _ticker_cik_loaded_at = now
        _save_cik_cache(mapping)
    return mapping


def _get_filings(symbol):
    symbol = symbol.upper()
    cached = _filings_cache.get(symbol)
    if cached and (time.time() - cached["time"]) < SEC_CACHE_TTL:
        return cached["data"]

    cik_map = _get_cik_map()
    cik = cik_map.get(symbol)
    if not cik:
        raise ValueError("No hay CIK para este symbol")

    url = SEC_SUBMISSION_URL.format(cik=cik)
    data = _fetch_json(url, _sec_headers())
    filings = data.get("filings", {}).get("recent", {})
    accession_numbers = filings.get("accessionNumber", [])
    forms = filings.get("form", [])
    dates = filings.get("filingDate", [])
    primary_docs = filings.get("primaryDocument", [])

    items = []
    for idx in range(len(accession_numbers)):
        accession = accession_numbers[idx]
        form = forms[idx] if idx < len(forms) else ""
        if not _is_target_filing_form(form):
            continue
        date = dates[idx] if idx < len(dates) else ""
        primary = primary_docs[idx] if idx < len(primary_docs) else ""
        accession_no = accession.replace("-", "")
        base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no}"
        link = f"{base}/{primary}" if primary else f"{base}/{accession}-index.html"
        items.append(
            {
                "symbol": symbol,
                "form": form,
                "date": date,
                "link": link,
            }
        )
        if len(items) >= FILINGS_PER_SYMBOL_LIMIT:
            break
    processed = _process_filings(items)
    _filings_cache[symbol] = {"time": time.time(), "data": processed}
    return processed


def _get_news(symbol, limit=None):
    symbol = symbol.upper()
    cached = _news_cache.get(symbol)
    if cached and (time.time() - cached["time"]) < NEWS_CACHE_TTL:
        items = cached["data"]
        if limit is None:
            limit = NEWS_PER_SYMBOL_LIMIT
        return items[:limit] if limit else []

    url = NEWS_FEED_URL.format(symbol=urllib.parse.quote(symbol))
    payload = _fetch_text(url, NEWS_HEADERS)
    root = ET.fromstring(payload)
    items = []
    for item in root.findall(".//item")[:NEWS_FETCH_LIMIT]:
        title = item.findtext("title") or ""
        link = item.findtext("link") or ""
        date = item.findtext("pubDate") or ""
        source = item.findtext("source") or ""
        items.append({"title": title, "link": link, "date": date, "source": source})

    _news_cache[symbol] = {"time": time.time(), "data": items}
    if limit is None:
        limit = NEWS_PER_SYMBOL_LIMIT
    return items[:limit] if limit else []


def _parse_iso_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        return None


def _infer_date_from_url(url):
    if not url:
        return None
    match = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", url)
    if not match:
        return None
    try:
        year, month, day = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        return datetime(year, month, day, tzinfo=ZoneInfo("UTC")).timestamp()
    except Exception:
        return None


def _get_filings_stream(symbols):
    items = []
    for symbol in symbols:
        try:
            entries = _get_filings(symbol)
        except Exception:
            continue
        items.extend(entries)

    items.sort(key=lambda item: item.get("timestamp") or 0, reverse=True)
    deduped = []
    seen = set()
    for item in items:
        link = item.get("link")
        if link and link in seen:
            continue
        seen.add(link)
        deduped.append(item)
        if len(deduped) >= FILINGS_LIMIT:
            break
    return deduped


def _parse_rss_date(value):
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        return parsed.timestamp()
    except Exception:
        return None


def _get_news_stream(symbols):
    items = []
    for symbol in symbols:
        try:
            entries = _get_news(symbol, limit=NEWS_PER_SYMBOL_LIMIT)
        except Exception:
            continue
        for entry in entries:
            timestamp = _parse_rss_date(entry.get("date"))
            items.append(
                {
                    "title": entry.get("title"),
                    "link": entry.get("link"),
                    "date": entry.get("date"),
                    "source": entry.get("source"),
                    "symbol": symbol,
                    "timestamp": timestamp,
                }
            )

    items.sort(key=lambda item: item.get("timestamp") or 0, reverse=True)
    deduped = []
    seen = set()
    for item in items:
        link = item.get("link")
        if link and link in seen:
            continue
        seen.add(link)
        deduped.append(item)
        if len(deduped) >= NEWS_LIMIT:
            break
    return deduped


PRESS_SOURCES = (
    "PRNewswire",
    "PR Newswire",
    "Business Wire",
    "GlobeNewswire",
    "ACCESSWIRE",
    "Newsfile",
)
PRESS_KEYWORDS = (
    "press release",
    "pr newswire",
    "business wire",
    "globe newswire",
    "accesswire",
    "newsfile",
    "news release",
)
PRESS_HOSTS = (
    "prnewswire.com",
    "businesswire.com",
    "globenewswire.com",
    "accesswire.com",
    "newsfilecorp.com",
    "sec.gov",
)

PRESS_FEED_HEADERS = {
    "User-Agent": WEB_USER_AGENT,
    "Accept": "application/rss+xml, application/xml, text/xml",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
}
PRESS_PAGE_HEADERS = {
    "User-Agent": WEB_USER_AGENT,
    "Accept": "text/html, application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
}

_press_discovery_cache = {}
_press_page_cache = {}
PRESS_PAGE_CACHE_TTL = int(os.environ.get("PRESS_PAGE_CACHE_TTL", "1800"))
_press_feed_cache = {}
PRESS_FEED_CACHE_TTL = int(os.environ.get("PRESS_FEED_CACHE_TTL", "1800"))
PRESS_FEED_TIMEOUT = int(os.environ.get("PRESS_FEED_TIMEOUT", "4"))
PRESS_PAGE_TIMEOUT = int(os.environ.get("PRESS_PAGE_TIMEOUT", "6"))
PRESS_FETCH_BUDGET_SEC = int(os.environ.get("PRESS_FETCH_BUDGET_SEC", "12"))
PRESS_MAX_AGE_HOURS = int(os.environ.get("PRESS_MAX_AGE_HOURS", "0"))


def _press_fallback_latest_enabled():
    config = load_config()
    value = config.get("pressFallbackLatest")
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes")
    raw = os.environ.get("PRESS_FALLBACK_LATEST", "").strip().lower()
    if raw:
        return raw in ("1", "true", "yes")
    return False


def _press_per_symbol_limit():
    config = load_config()
    value = config.get("pressPerSymbolLimit")
    if isinstance(value, int):
        return max(0, value)
    raw = os.environ.get("PRESS_PER_SYMBOL_LIMIT", "").strip()
    if raw.isdigit():
        return max(0, int(raw))
    return 1


def _press_max_age_hours():
    config = load_config()
    value = config.get("pressMaxAgeHours")
    if isinstance(value, int):
        return max(0, value)
    raw = os.environ.get("PRESS_MAX_AGE_HOURS", "").strip()
    if raw.isdigit():
        return max(0, int(raw))
    return PRESS_MAX_AGE_HOURS


def _is_press_release(item):
    source = (item.get("source") or "").lower()
    link = item.get("link") or ""
    title = (item.get("title") or "").lower()
    host = urllib.parse.urlparse(link).netloc.lower()
    host_match = any(host.endswith(hostname) for hostname in PRESS_HOSTS)
    source_match = any(source_key.lower() in source for source_key in PRESS_SOURCES)
    keyword_match = any(keyword in title for keyword in PRESS_KEYWORDS)
    return host_match or source_match or keyword_match


def _coerce_url_list(value):
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _expand_press_template(template, symbol):
    return (
        template.replace("{symbol}", symbol)
        .replace("{symbol_lower}", symbol.lower())
        .replace("{symbol_upper}", symbol.upper())
    )


def _get_press_feed_global_urls():
    config = load_config()
    urls = []
    env_urls = os.environ.get("PRESS_FEED_URLS", "").strip()
    if env_urls:
        urls.extend([item.strip() for item in env_urls.split(",") if item.strip()])
    urls.extend(_coerce_url_list(config.get("pressFeedUrls")))
    return list(dict.fromkeys(urls))


def _get_press_feed_urls_for_symbol(symbol):
    config = load_config()
    urls = []
    feed_map = config.get("pressFeedMap") or {}
    if isinstance(feed_map, dict):
        entry = feed_map.get(symbol.upper()) or feed_map.get(symbol.lower())
        urls.extend(_coerce_url_list(entry))
    templates = []
    env_templates = os.environ.get("PRESS_FEED_TEMPLATES", "").strip()
    if env_templates:
        templates.extend(
            [item.strip() for item in env_templates.split(",") if item.strip()]
        )
    templates.extend(_coerce_url_list(config.get("pressFeedTemplates")))
    for template in templates:
        urls.append(_expand_press_template(template, symbol))
    return list(dict.fromkeys([url for url in urls if url]))


def _get_press_allowed_domains(symbol):
    if not symbol:
        return []
    config = load_config()
    domain_map = config.get("pressAllowedDomains") or {}
    if not isinstance(domain_map, dict):
        return []
    entry = domain_map.get(symbol.upper()) or domain_map.get(symbol.lower())
    domains = _coerce_url_list(entry)
    return [domain.lower() for domain in domains if domain]


def _press_discovery_cache_get(url):
    cached = _press_discovery_cache.get(url)
    if not cached:
        return None
    if (time.time() - cached["time"]) >= PRESS_PAGE_CACHE_TTL:
        del _press_discovery_cache[url]
        return None
    return cached["feeds"]


def _press_discovery_cache_set(url, feeds):
    _press_discovery_cache[url] = {"time": time.time(), "feeds": feeds}


def _get_press_source_pages_for_symbol(symbol):
    config = load_config()
    pages = []
    page_map = config.get("pressSourcePages") or {}
    if isinstance(page_map, dict):
        entry = page_map.get(symbol.upper()) or page_map.get(symbol.lower())
        pages.extend(_coerce_url_list(entry))
    env_pages = os.environ.get("PRESS_SOURCE_PAGES", "").strip()
    if env_pages:
        pages.extend([item.strip() for item in env_pages.split(",") if item.strip()])
    return list(dict.fromkeys([page for page in pages if page]))


def _extract_html_attr(tag, attr):
    pattern = re.compile(
        rf"{attr}\s*=\s*(\"([^\"]*)\"|'([^']*)'|([^\s>]+))",
        re.IGNORECASE,
    )
    match = pattern.search(tag)
    if not match:
        return ""
    value = match.group(2) or match.group(3) or match.group(4) or ""
    return unescape(value.strip())


def _guess_feed_urls(page_url):
    parsed = urllib.parse.urlparse(page_url)
    if not parsed.netloc:
        return []
    base = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.rstrip("/")
    guesses = [
        f"{base}/rss",
        f"{base}/rss.xml",
        f"{base}/rss/news-releases.xml",
        f"{base}/rss/press-releases.xml",
        f"{base}/rss/news.xml",
        f"{base}/rss/press.xml",
        f"{base}/feed",
        f"{base}/feed/",
        f"{base}/news/rss",
        f"{base}/news-releases/rss",
        f"{base}/press-releases/rss",
    ]
    if path:
        guesses.extend(
            [
                f"{base}{path}/rss",
                f"{base}{path}/rss.xml",
                f"{base}{path}/feed",
                f"{base}{path}/feed/",
                f"{base}{path}?rss=1",
                f"{base}{path}?format=rss",
                f"{base}{path}?output=rss",
            ]
        )
        parent = "/".join(path.split("/")[:-1])
        if parent:
            guesses.extend(
                [
                    f"{base}/{parent}/rss",
                    f"{base}/{parent}/rss.xml",
                    f"{base}/{parent}/feed",
                    f"{base}/{parent}?rss=1",
                ]
            )
    return list(dict.fromkeys(guesses))


def _discover_feed_urls(page_url):
    cached = _press_discovery_cache_get(page_url)
    if cached is not None:
        return cached
    try:
        payload = _fetch_text(page_url, PRESS_PAGE_HEADERS, timeout=PRESS_PAGE_TIMEOUT)
    except Exception:
        _press_discovery_cache_set(page_url, [])
        return []
    if _parse_feed_items(payload):
        _press_discovery_cache_set(page_url, [page_url])
        return [page_url]
    feeds = []
    for tag in re.findall(r"<link[^>]+>", payload, flags=re.IGNORECASE):
        rel = _extract_html_attr(tag, "rel").lower()
        link_type = _extract_html_attr(tag, "type").lower()
        href = _extract_html_attr(tag, "href")
        if not href:
            continue
        if "alternate" in rel and ("rss" in link_type or "atom" in link_type):
            feeds.append(urllib.parse.urljoin(page_url, href))
    if not feeds:
        _press_discovery_cache_set(page_url, [])
        return []
    feeds = list(dict.fromkeys([feed for feed in feeds if feed]))
    _press_discovery_cache_set(page_url, feeds)
    return feeds


def _press_page_cache_get(url):
    cached = _press_page_cache.get(url)
    if not cached:
        return None
    if (time.time() - cached["time"]) >= PRESS_PAGE_CACHE_TTL:
        del _press_page_cache[url]
        return None
    return cached["items"]


def _press_page_cache_set(url, items):
    _press_page_cache[url] = {"time": time.time(), "items": items}


def _press_feed_cache_get(url):
    cached = _press_feed_cache.get(url)
    if not cached:
        return None
    if (time.time() - cached["time"]) >= PRESS_FEED_CACHE_TTL:
        del _press_feed_cache[url]
        return None
    return cached["items"]


def _press_feed_cache_set(url, items):
    _press_feed_cache[url] = {"time": time.time(), "items": items}


def _coerce_json_ld_list(value):
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    return []


def _extract_json_ld_items(payload):
    scripts = re.findall(
        r"<script[^>]*type=[\"']application/ld\+json[\"'][^>]*>(.*?)</script>",
        payload,
        flags=re.IGNORECASE | re.DOTALL,
    )
    items = []
    for script in scripts:
        script = script.strip()
        if not script:
            continue
        try:
            data = json.loads(script)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and "@graph" in data:
            items.extend(_coerce_json_ld_list(data.get("@graph")))
        else:
            items.extend(_coerce_json_ld_list(data))
    return items


def _json_ld_types(item):
    value = item.get("@type")
    if isinstance(value, list):
        return [str(entry).lower() for entry in value if entry]
    if isinstance(value, str):
        return [value.lower()]
    return []


def _extract_json_ld_url(item, base_url):
    url = item.get("url")
    if isinstance(url, str) and url:
        return urllib.parse.urljoin(base_url, url)
    entity = item.get("mainEntityOfPage")
    if isinstance(entity, str) and entity:
        return urllib.parse.urljoin(base_url, entity)
    if isinstance(entity, dict):
        entity_url = entity.get("@id") or entity.get("url")
        if isinstance(entity_url, str) and entity_url:
            return urllib.parse.urljoin(base_url, entity_url)
    entity_id = item.get("@id")
    if isinstance(entity_id, str) and entity_id:
        return urllib.parse.urljoin(base_url, entity_id)
    return ""


def _collect_json_ld_press_items(payload, page_url):
    candidates = _extract_json_ld_items(payload)
    items = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        types = _json_ld_types(item)
        if "itemlist" in types:
            elements = item.get("itemListElement") or []
            if isinstance(elements, dict):
                elements = [elements]
            if isinstance(elements, list):
                for element in elements:
                    element_item = None
                    if isinstance(element, dict):
                        element_item = element.get("item") or element
                    else:
                        element_item = element
                    if isinstance(element_item, dict):
                        title = (
                            element_item.get("headline")
                            or element_item.get("name")
                            or element_item.get("title")
                            or ""
                        )
                        link = _extract_json_ld_url(element_item, page_url)
                        date = (
                            element_item.get("datePublished")
                            or element_item.get("dateCreated")
                            or ""
                        )
                    elif isinstance(element_item, str):
                        title = ""
                        link = urllib.parse.urljoin(page_url, element_item)
                        date = ""
                    else:
                        continue
                    if title or link:
                        items.append(
                            {
                                "title": title,
                                "link": link,
                                "date": date,
                                "source": "",
                            }
                        )
            continue
        if not any(
            entry in ("newsarticle", "pressrelease", "blogposting", "article")
            for entry in types
        ):
            continue
        title = item.get("headline") or item.get("name") or item.get("title") or ""
        if not title:
            continue
        link = _extract_json_ld_url(item, page_url)
        date = item.get("datePublished") or item.get("dateCreated") or ""
        source = ""
        publisher = item.get("publisher")
        if isinstance(publisher, dict):
            source = publisher.get("name") or ""
        items.append(
            {
                "title": title,
                "link": link,
                "date": date,
                "source": source,
            }
        )
    return items


def _should_keep_press_link(href):
    if not href:
        return False
    lower = href.lower()
    if lower.startswith("#") or lower.startswith("javascript:"):
        return False
    keywords = (
        "press-release",
        "pressreleases",
        "press-releases",
        "news-release",
        "news-releases",
        "newsroom",
        "/news/",
        "/news-",
        "/news_",
        "/press/",
        "/press-",
        "/blog/",
        "/blog-",
    )
    return any(key in lower for key in keywords)


def _collect_anchor_press_items(payload, page_url):
    items = []
    for match in re.finditer(
        r"<a\s+[^>]*href\s*=\s*(\"([^\"]*)\"|'([^']*)'|([^\s>]+))[^>]*>(.*?)</a>",
        payload,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        href = match.group(2) or match.group(3) or match.group(4) or ""
        text = _strip_html(match.group(5))
        if not text or len(text) < 15:
            continue
        if not _should_keep_press_link(href):
            continue
        url = urllib.parse.urljoin(page_url, href.strip())
        items.append(
            {
                "title": text,
                "link": url,
                "date": "",
                "source": "",
            }
        )
    return items


def _fetch_press_page_items(page_url, symbol):
    cached = _press_page_cache_get(page_url)
    if cached is not None:
        return [dict(item, symbol=symbol) for item in cached]
    try:
        payload = _fetch_text(page_url, PRESS_PAGE_HEADERS, timeout=PRESS_PAGE_TIMEOUT)
    except Exception:
        _press_page_cache_set(page_url, [])
        return []
    items = _collect_json_ld_press_items(payload, page_url)
    if not items:
        items = _collect_anchor_press_items(payload, page_url)
    default_source = urllib.parse.urlparse(page_url).netloc or page_url
    normalized = []
    for entry in items[:PRESS_LIMIT]:
        date = entry.get("date") or ""
        timestamp = _parse_rss_date(date) or _parse_iso_date(date)
        if not timestamp:
            timestamp = _infer_date_from_url(entry.get("link") or "")
        normalized.append(
            {
                "title": entry.get("title") or "",
                "link": entry.get("link") or "",
                "date": date,
                "source": entry.get("source") or default_source,
                "symbol": symbol,
                "timestamp": timestamp,
                "isPress": True,
                "official": True,
                "feedUrl": page_url,
            }
        )
    _press_page_cache_set(page_url, normalized)
    return normalized


def _parse_feed_items(payload):
    try:
        root = ET.fromstring(payload)
    except Exception:
        return []
    items = []
    if root.tag.endswith("feed"):
        for entry in root.findall(".//{*}entry"):
            title = entry.findtext("{*}title") or ""
            link = ""
            link_el = (
                entry.find("{*}link[@rel='alternate']") or entry.find("{*}link")
            )
            if link_el is not None:
                link = link_el.get("href") or (link_el.text or "")
            date = entry.findtext("{*}updated") or entry.findtext("{*}published") or ""
            source = entry.findtext("{*}source/{*}title") or ""
            items.append(
                {
                    "title": title,
                    "link": link,
                    "date": date,
                    "source": source,
                }
            )
    else:
        for entry in root.findall(".//{*}item"):
            title = entry.findtext("title") or entry.findtext("{*}title") or ""
            link = entry.findtext("link") or entry.findtext("{*}link") or ""
            date = (
                entry.findtext("pubDate")
                or entry.findtext("{*}pubDate")
                or entry.findtext("{*}date")
                or ""
            )
            source = entry.findtext("source") or entry.findtext("{*}source") or ""
            items.append(
                {
                    "title": title,
                    "link": link,
                    "date": date,
                    "source": source,
                }
            )
    return items


def _fetch_press_feed_items(url, symbol):
    cached = _press_feed_cache_get(url)
    if cached is None:
        try:
            payload = _fetch_text(url, PRESS_FEED_HEADERS, timeout=PRESS_FEED_TIMEOUT)
        except Exception:
            _press_feed_cache_set(url, [])
            return []
        parsed = _parse_feed_items(payload)
        _press_feed_cache_set(url, parsed)
    else:
        parsed = cached
    if not parsed:
        return []
    default_source = urllib.parse.urlparse(url).netloc or url
    allowed_domains = _get_press_allowed_domains(symbol)
    items = []
    for entry in parsed:
        link = entry.get("link") or ""
        if link and allowed_domains:
            host = urllib.parse.urlparse(link).netloc.lower()
            if not any(host.endswith(domain) for domain in allowed_domains):
                continue
        date = entry.get("date") or ""
        timestamp = _parse_rss_date(date) or _parse_iso_date(date)
        items.append(
            {
                "title": entry.get("title") or "",
                "link": link,
                "date": date,
                "source": entry.get("source") or default_source,
                "symbol": symbol,
                "timestamp": timestamp,
                "isPress": True,
                "official": True,
                "feedUrl": url,
            }
        )
    return items


def _get_press_stream(symbols):
    cache_key = tuple(symbols)
    cached = _press_cache.get(cache_key)
    if cached and (time.time() - cached["time"]) < NEWS_CACHE_TTL:
        return cached["data"]

    start_time = time.time()
    items = []
    global_urls = _get_press_feed_global_urls()
    for url in global_urls:
        if (time.time() - start_time) >= PRESS_FETCH_BUDGET_SEC:
            break
        items.extend(_fetch_press_feed_items(url, ""))
    for symbol in symbols:
        if (time.time() - start_time) >= PRESS_FETCH_BUDGET_SEC:
            break
        symbol_has_items = False
        feed_urls = _get_press_feed_urls_for_symbol(symbol)
        source_pages = _get_press_source_pages_for_symbol(symbol)
        for page in source_pages:
            if (time.time() - start_time) >= PRESS_FETCH_BUDGET_SEC:
                break
            discovered = _discover_feed_urls(page)
            if discovered:
                feed_urls.extend(discovered)
            else:
                page_items = _fetch_press_page_items(page, symbol)
                if page_items:
                    symbol_has_items = True
                    items.extend(page_items)
        feed_urls = list(dict.fromkeys([url for url in feed_urls if url]))
        for url in feed_urls:
            if (time.time() - start_time) >= PRESS_FETCH_BUDGET_SEC:
                break
            feed_items = _fetch_press_feed_items(url, symbol)
            if feed_items:
                symbol_has_items = True
                items.extend(feed_items)
        if (time.time() - start_time) >= PRESS_FETCH_BUDGET_SEC:
            break
        if not symbol_has_items:
            try:
                entries = _get_news(symbol, limit=NEWS_FETCH_LIMIT)
            except Exception:
                continue
            for entry in entries:
                if not _is_press_release(entry):
                    continue
                timestamp = _parse_rss_date(entry.get("date"))
                items.append(
                    {
                        "title": entry.get("title"),
                        "link": entry.get("link"),
                        "date": entry.get("date"),
                        "source": entry.get("source"),
                        "symbol": symbol,
                        "timestamp": timestamp,
                        "feedUrl": "yahoo",
                    }
                )

    max_age_hours = _press_max_age_hours()
    if max_age_hours > 0:
        cutoff = time.time() - (max_age_hours * 3600)
        items = [
            item
            for item in items
            if (item.get("timestamp") or 0) >= cutoff
        ]

    items.sort(key=lambda item: item.get("timestamp") or 0, reverse=True)
    deduped = []
    seen = set()
    for item in items:
        link = item.get("link")
        if link and link in seen:
            continue
        seen.add(link)
        item["isPress"] = True
        item["official"] = True
        deduped.append(item)

    per_symbol_limit = _press_per_symbol_limit()
    if len(symbols) > 1 and per_symbol_limit > 0:
        limited = []
        counts = {}
        for item in deduped:
            symbol = item.get("symbol")
            if not symbol:
                continue
            count = counts.get(symbol, 0)
            if count >= per_symbol_limit:
                continue
            counts[symbol] = count + 1
            limited.append(item)
        if limited:
            deduped = limited
    else:
        deduped = deduped[:PRESS_LIMIT]
    if not deduped and _press_fallback_latest_enabled():
        fallback_items = []
        seen_links = set()
        cutoff = None
        max_age_hours = _press_max_age_hours()
        if max_age_hours > 0:
            cutoff = time.time() - (max_age_hours * 3600)
        for symbol in symbols:
            try:
                entries = _get_news(symbol)
            except Exception:
                continue
            if not entries:
                continue
            entry = entries[0]
            link = entry.get("link")
            if link and link in seen_links:
                continue
            seen_links.add(link)
            timestamp = _parse_rss_date(entry.get("date"))
            if cutoff is not None and (timestamp or 0) < cutoff:
                continue
            fallback_items.append(
                {
                    "title": entry.get("title"),
                    "link": link,
                    "date": entry.get("date"),
                    "source": entry.get("source"),
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "isPress": False,
                    "official": False,
                    "fallback": True,
                }
            )
            if len(fallback_items) >= PRESS_LIMIT:
                break
        if fallback_items:
            deduped = fallback_items
    _press_cache[cache_key] = {"time": time.time(), "data": deduped}
    return deduped


def _parse_stooq_csv(payload):
    lines = [line.strip() for line in payload.splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    values = lines[1].split(",")
    if len(values) < 8:
        return None
    open_price = _to_float(values[3])
    close_price = _to_float(values[6])
    price = close_price or open_price
    if price is None:
        return None
    return {
        "price": price,
        "open": open_price,
        "close": close_price,
    }


def _fetch_stooq_quote(symbol, previous_close=None):
    stooq_symbol = f"{symbol.lower()}.us"
    url = STOOQ_URL.format(symbol=urllib.parse.quote(stooq_symbol))
    payload = _fetch_text(url, STOOQ_HEADERS)
    parsed = _parse_stooq_csv(payload)
    if not parsed:
        return None
    price = parsed["price"]
    change = None
    base = previous_close
    if base is None:
        base = parsed["open"] if parsed["open"] is not None else parsed["close"]
    if base is not None:
        change = price - base
    change_percent = None
    if change is not None and base:
        change_percent = (change / base) * 100
    return {
        "price": price,
        "change": change,
        "changePercent": change_percent,
        "previousClose": previous_close,
        "marketState": _market_state(),
    }


def fetch_stooq_quotes(symbols):
    results = {}
    for symbol in symbols:
        entry = _symbol_cache.get(symbol)
        prev_close = None
        if entry and entry.get("data"):
            prev_close = entry["data"].get("previousClose")
        try:
            quote = _fetch_stooq_quote(symbol, prev_close)
            if quote:
                results[symbol] = quote
        except Exception:
            continue
        time.sleep(0.08)
    return results


def _nasdaq_first_number(*values):
    for value in values:
        parsed = _to_float_loose(value)
        if parsed is not None:
            return parsed
    return None


def _nasdaq_previous_close_from_summary(payload):
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    summary = data.get("summaryData")
    if not isinstance(summary, dict):
        return None
    for key, value in summary.items():
        key_norm = str(key).strip().lower().replace(" ", "")
        if "previousclose" in key_norm:
            return _to_float_loose(value)
    return None


def _fetch_nasdaq_quote(symbol):
    url = NASDAQ_DATA_URL.format(symbol=urllib.parse.quote(symbol))
    payload = _fetch_nasdaq_json(url)
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None

    primary = data.get("primaryData")
    if not isinstance(primary, dict):
        primary = {}
    extended = data.get("extendedMarket")
    if not isinstance(extended, dict):
        extended = {}

    market_state = _normalize_nasdaq_market_state(
        data.get("marketStatus") or data.get("marketStatusIndicator")
    )
    extended_state = _normalize_nasdaq_market_state(
        data.get("extendedMarketStatus") or extended.get("marketStatus")
    )
    if extended_state and not market_state:
        market_state = extended_state

    price = _nasdaq_first_number(
        primary.get("lastSalePrice"),
        primary.get("price"),
        data.get("lastSalePrice"),
        data.get("price"),
    )
    change = _nasdaq_first_number(
        primary.get("netChange"),
        primary.get("change"),
        data.get("netChange"),
        data.get("change"),
    )
    change_percent = _nasdaq_first_number(
        primary.get("percentageChange"),
        primary.get("changePercent"),
        data.get("percentageChange"),
        data.get("changePercent"),
    )
    previous_close = _nasdaq_first_number(
        data.get("previousClose"),
        data.get("previousClosePrice"),
        data.get("prevClose"),
    )
    extended_price = _nasdaq_first_number(
        data.get("extendedMarketPrice"),
        extended.get("lastSalePrice"),
        extended.get("price"),
    )
    extended_change = _nasdaq_first_number(
        data.get("extendedMarketChange"),
        extended.get("netChange"),
        extended.get("change"),
    )
    extended_percent = _nasdaq_first_number(
        data.get("extendedMarketPercentageChange"),
        extended.get("percentageChange"),
        extended.get("changePercent"),
    )

    if market_state in ("premarket", "after") and extended_price is not None:
        price = extended_price
        if extended_change is not None:
            change = extended_change
        if extended_percent is not None:
            change_percent = extended_percent

    if previous_close is None:
        summary_url = NASDAQ_SUMMARY_URL.format(
            symbol=urllib.parse.quote(symbol)
        )
        try:
            summary_payload = _fetch_nasdaq_json(summary_url)
        except Exception:
            summary_payload = None
        previous_close = _nasdaq_previous_close_from_summary(summary_payload)

    if previous_close is None and price is not None and change is not None:
        previous_close = price - change
    if change is None and price is not None and previous_close is not None:
        change = price - previous_close
    if (
        change_percent is None
        and change is not None
        and previous_close
    ):
        change_percent = (change / previous_close) * 100
    if (
        change is None
        and price is not None
        and change_percent is not None
    ):
        base = 1 + (change_percent / 100)
        if base:
            if previous_close is None:
                previous_close = price / base
            if previous_close is not None:
                change = price - previous_close

    if price is None:
        return None

    return {
        "price": price,
        "change": change,
        "changePercent": change_percent,
        "previousClose": previous_close,
        "marketState": market_state or _market_state(),
    }


def fetch_nasdaq_quotes(symbols):
    results = {}
    for symbol in symbols:
        try:
            quote = _fetch_nasdaq_quote(symbol)
        except Exception:
            quote = None
        if quote:
            results[symbol] = quote
        else:
            results[symbol] = {"error": "Sin datos Nasdaq"}
        time.sleep(0.08)
    return results


def parse_symbols():
    raw = request.args.get("symbols", "")
    if not raw:
        return DEFAULT_SYMBOLS
    symbols = [item.strip().upper() for item in raw.split(",") if item.strip()]
    return symbols if symbols else DEFAULT_SYMBOLS


def _get_lan_ip():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    try:
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        return None
    return None


def _available_credits():
    daily_remaining = _daily_remaining()
    now = time.time()
    while _credit_log and (now - _credit_log[0]) > 60:
        _credit_log.popleft()
    per_minute = max(0, TWELVE_CREDITS_PER_MINUTE - len(_credit_log))
    return min(per_minute, daily_remaining)


def _consume_credits(amount):
    now = time.time()
    for _ in range(amount):
        _credit_log.append(now)
    _consume_daily_credits(amount)


def _eligible_symbols(symbols):
    now = time.time()
    candidates = []
    min_refresh = _min_symbol_refresh_sec()
    for index, symbol in enumerate(symbols):
        entry = _symbol_cache.get(symbol)
        last_update = entry.get("updatedAt") if entry else 0
        if last_update == 0 or (now - last_update) >= min_refresh:
            candidates.append((last_update, index, symbol))
    candidates.sort(key=lambda item: (item[0], item[1]))
    return [symbol for _, __, symbol in candidates]


def _rotation_batch(symbols, batch_size):
    if not symbols or batch_size <= 0:
        return []
    total = len(symbols)
    if total <= batch_size:
        return list(symbols)
    minute = int(time.time() // 60)
    start = (minute * batch_size) % total
    return [symbols[(start + offset) % total] for offset in range(batch_size)]


def _min_symbol_refresh_sec():
    config = load_config()
    raw = os.environ.get("MIN_SYMBOL_REFRESH_SEC", "")
    if not raw:
        raw = config.get("minSymbolRefreshSec", "")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = DEFAULT_MIN_SYMBOL_REFRESH_SEC
    if value <= 0:
        value = DEFAULT_MIN_SYMBOL_REFRESH_SEC
    return value


def _stock_provider():
    config = load_config()
    raw = (
        os.environ.get("STOCK_DATA_PROVIDER", "").strip()
        or str(config.get("stockDataProvider", "")).strip()
    )
    value = raw.lower()
    if value in ("twelvedata", "twelve", "twelve_data", "twelve-data"):
        return "twelvedata"
    if value in ("stooq",):
        return "stooq"
    if value in ("nasdaq", "ndaq"):
        return "nasdaq"
    return "nasdaq"


def _reset_daily_credits_if_needed():
    global _twelve_daily_date, _twelve_daily_used
    today = datetime.utcnow().date()
    if _twelve_daily_date != today:
        _twelve_daily_date = today
        _twelve_daily_used = 0


def _daily_remaining():
    _reset_daily_credits_if_needed()
    return max(0, TWELVE_DAILY_LIMIT - _twelve_daily_used)


def _consume_daily_credits(amount):
    global _twelve_daily_used
    _reset_daily_credits_if_needed()
    _twelve_daily_used = min(TWELVE_DAILY_LIMIT, _twelve_daily_used + amount)


def _mark_daily_limit_reached():
    global _twelve_daily_used
    _reset_daily_credits_if_needed()
    _twelve_daily_used = TWELVE_DAILY_LIMIT


def _is_daily_limit_error(message):
    if not message:
        return False
    text = str(message).lower()
    return (
        "out of api credits" in text
        or "daily limit" in text
        or "limit being" in text
        or "free plan" in text
    )


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_float_loose(value):
    if value is None:
        return None
    if isinstance(value, dict):
        if "value" in value:
            value = value.get("value")
        elif "raw" in value:
            value = value.get("raw")
        else:
            return None
    text = str(value).strip()
    if not text or text.lower() in ("n/a", "na", "--"):
        return None
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    text = (
        text.replace("$", "")
        .replace(",", "")
        .replace("%", "")
        .replace("+", "")
    )
    return _to_float(text)


def _market_state():
    now = datetime.now(MARKET_TZ)
    if now.weekday() >= 5:
        return "closed"
    current = now.time()
    if dt_time(4, 0) <= current < dt_time(9, 30):
        return "premarket"
    if dt_time(9, 30) <= current < dt_time(16, 0):
        return "open"
    if dt_time(16, 0) <= current < dt_time(20, 0):
        return "after"
    return "closed"


def _normalize_nasdaq_market_state(value):
    if not value:
        return None
    text = str(value).strip().lower()
    if "pre" in text:
        return "premarket"
    if "after" in text or "post" in text:
        return "after"
    if "open" in text:
        return "open"
    if "close" in text:
        return "closed"
    return None


def _normalize_state(value):
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value) if isinstance(value, (bool, int)) else None


def _fetch_twelve_data(symbols, api_key):
    params = urllib.parse.urlencode({"symbol": ",".join(symbols), "apikey": api_key})
    url = f"{TWELVE_DATA_URL}?{params}"
    request_obj = urllib.request.Request(url, headers=TWELVE_HEADERS)
    with urllib.request.urlopen(request_obj, timeout=8) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    if isinstance(data, dict) and data.get("status") == "error":
        message = data.get("message") or "Error API"
        raise ValueError(message)
    if isinstance(data, dict) and data.get("code") and data.get("message"):
        raise ValueError(data.get("message") or "Error API")
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], list):
        return _normalize_symbol_map(data["data"])
    if isinstance(data, dict) and "data" in data and isinstance(data["data"], dict):
        return _normalize_symbol_map(data["data"])
    if isinstance(data, list):
        return _normalize_symbol_map(data)
    if isinstance(data, dict) and "symbol" in data:
        return _normalize_symbol_map([data])
    if isinstance(data, dict) and any(symbol in data for symbol in symbols):
        return _normalize_symbol_map(data)
    return _normalize_symbol_map(data) if isinstance(data, dict) else {}


def _normalize_symbol_map(data):
    normalized = {}
    if isinstance(data, dict):
        iterable = data.items()
    elif isinstance(data, list):
        iterable = [(None, item) for item in data]
    else:
        return normalized

    for key, item in iterable:
        if not isinstance(item, dict):
            continue
        symbol = item.get("symbol") or key
        if not symbol:
            continue
        symbol_key = str(symbol).upper()
        normalized[symbol_key] = item
        if "." in symbol_key:
            normalized[symbol_key.split(".")[0]] = item
    return normalized


def fetch_quotes(symbols, api_key):
    results = {}
    if not symbols:
        return results
    payload = _fetch_twelve_data(symbols, api_key)
    state = _market_state()
    now = datetime.now(MARKET_TZ)
    today = now.date().isoformat()
    after_close = now.time() >= BASELINE_UPDATE_CUTOFF
    for symbol in symbols:
        item = payload.get(symbol)
        if not isinstance(item, dict):
            continue
        if item.get("status") == "error":
            message = item.get("message") or item.get("code") or "Error Twelve Data"
            results[symbol] = {"error": message}
            continue
        price = _to_float(item.get("price"))
        if price is None:
            price = _to_float(
                item.get("close")
                or item.get("previous_close")
                or item.get("prev_close")
                or item.get("previousClose")
            )
        if price is None:
            results[symbol] = {"error": "Sin precio en Twelve Data"}
            continue
        candidate_close = _to_float(
            item.get("previous_close")
            or item.get("prev_close")
            or item.get("previousClose")
            or item.get("close")
        )
        baseline_close, baseline_date = _get_baseline(symbol)
        if baseline_close is None and candidate_close is not None:
            _set_baseline(symbol, candidate_close, "unknown")
            baseline_close = candidate_close
            baseline_date = "unknown"
        if after_close and candidate_close is not None and baseline_date != today:
            _set_baseline(symbol, candidate_close, today)
            baseline_close = candidate_close
            baseline_date = today
        change = None
        change_percent = None
        baseline_error = ""
        if baseline_close is None or baseline_close == 0:
            baseline_error = "Error baseline: falta cierre previo"
        else:
            change = price - baseline_close
            change_percent = (change / baseline_close) * 100
        is_open = _normalize_state(item.get("is_market_open"))
        market_state = "open" if is_open else state
        results[symbol] = {
            "price": price,
            "change": change,
            "changePercent": change_percent,
            "previousClose": baseline_close,
            "baselineError": baseline_error,
            "marketState": market_state,
        }
    return results


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/stocks")
def api_stocks():
    symbols = parse_symbols()
    provider = _stock_provider()
    api_key = None
    if provider == "twelvedata":
        api_key = (
            request.args.get("apikey", "").strip()
            or os.environ.get("TWELVE_DATA_KEY", "").strip()
            or str(CONFIG.get("twelveDataKey", "")).strip()
        )
        if not api_key:
            return (
                jsonify(
                    {
                        "error": (
                            "API key requerida (config.json o env TWELVE_DATA_KEY)"
                        )
                    }
                ),
                400,
            )

    refresh_list = []
    error_message = None
    with _refresh_lock:
        if provider == "twelvedata":
            refresh_budget = _available_credits()
            rotation_list = _rotation_batch(
                symbols, min(8, TWELVE_CREDITS_PER_MINUTE, len(symbols))
            )
        else:
            refresh_budget = len(symbols)
            rotation_list = list(symbols)
        refresh_candidates = _eligible_symbols(rotation_list)
        refresh_list = refresh_candidates[:refresh_budget]
        if refresh_list:
            try:
                if provider == "twelvedata":
                    quotes = fetch_quotes(refresh_list, api_key)
                elif provider == "nasdaq":
                    quotes = fetch_nasdaq_quotes(refresh_list)
                elif provider == "stooq":
                    quotes = fetch_stooq_quotes(refresh_list)
                else:
                    quotes = {}
                    error_message = "Proveedor no soportado"
            except Exception as exc:
                quotes = {}
                error_message = str(exc) or "Error API"
                if provider == "twelvedata" and _is_daily_limit_error(
                    error_message
                ):
                    _mark_daily_limit_reached()
            if provider == "twelvedata":
                _consume_credits(len(refresh_list))
            now = time.time()
            for symbol in refresh_list:
                payload = quotes.get(symbol)
                if payload:
                    _symbol_cache[symbol] = {"data": payload, "updatedAt": now}
                else:
                    if provider == "nasdaq":
                        fallback_error = "Error Nasdaq"
                    elif provider == "stooq":
                        fallback_error = "Error Stooq"
                    else:
                        fallback_error = "Error API"
                    error_text = error_message or fallback_error
                    _symbol_cache[symbol] = {
                        "data": {"error": error_text},
                        "updatedAt": now,
                    }
    data = []
    for symbol in symbols:
        entry = _symbol_cache.get(symbol)
        if not entry:
            _log_price_metrics(symbol, None, None, None, None)
            data.append({"symbol": symbol, "error": "Pendiente"})
            continue
        payload = dict(entry["data"])
        payload["symbol"] = symbol
        payload["updatedAt"] = entry["updatedAt"]
        _log_price_metrics(
            symbol,
            payload.get("price"),
            payload.get("previousClose"),
            payload.get("change"),
            payload.get("changePercent"),
        )
        data.append(payload)

    response = {
        "updatedAt": int(time.time()),
        "data": data,
        "meta": {
            "creditsPerMinute": (
                TWELVE_CREDITS_PER_MINUTE
                if provider == "twelvedata"
                else 0
            ),
            "refreshed": len(refresh_list),
            "error": error_message,
            "provider": provider,
        },
    }
    return jsonify(response)


@app.route("/api/filings")
def api_filings():
    symbol = request.args.get("symbol", "").strip()
    try:
        if symbol:
            data = _get_filings(symbol)
            return jsonify({"symbol": symbol.upper(), "data": data})
        symbols = parse_symbols()
        data = _get_filings_stream(symbols)
        return jsonify({"data": data})
    except Exception as exc:
        return jsonify({"error": str(exc) or "Error API"}), 502


@app.route("/api/news")
def api_news():
    symbol = request.args.get("symbol", "").strip()
    try:
        if symbol:
            data = _get_news(symbol)
            _apply_title_translations(data)
            _apply_news_analysis(data)
            return jsonify({"symbol": symbol.upper(), "data": data})
        symbols = parse_symbols()
        data = _get_news_stream(symbols)
        _apply_title_translations(data)
        _apply_news_analysis(data)
        return jsonify({"data": data})
    except Exception as exc:
        return jsonify({"error": str(exc) or "Error API"}), 502


@app.route("/api/press")
def api_press():
    symbol = request.args.get("symbol", "").strip()
    try:
        if symbol:
            data = _get_press_stream([symbol.upper()])
            _apply_title_translations(data)
            return jsonify({"symbol": symbol.upper(), "data": data})
        symbols = parse_symbols()
        data = _get_press_stream(symbols)
        _apply_title_translations(data)
        return jsonify({"data": data})
    except Exception as exc:
        return jsonify({"error": str(exc) or "Error API"}), 502


@app.route("/api/lan")
def api_lan():
    host = request.host.split(":")[0]
    port = request.host.split(":")[1] if ":" in request.host else os.environ.get("PORT", "4173")
    ip = _get_lan_ip() or host
    return jsonify({"ip": ip, "port": port, "url": f"http://{ip}:{port}"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "4173"))
    app.run(host="0.0.0.0", port=port, debug=False)
