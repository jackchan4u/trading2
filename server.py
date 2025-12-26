import json
import os
import re
import socket
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
    "PSTG",
    "CLSK",
    "LSMC",
]

TWELVE_CREDITS_PER_MINUTE = int(os.environ.get("TWELVE_CREDITS_PER_MINUTE", "8"))
TWELVE_DAILY_LIMIT = int(os.environ.get("TWELVE_DAILY_LIMIT", "800"))
DEFAULT_MIN_SYMBOL_REFRESH_SEC = 20 * 60
_symbol_cache = {}
_credit_log = deque()
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
NEWS_FEED_URL = (
    "https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"
)
NEWS_CACHE_TTL = 60 * 5
NEWS_LIMIT = 12
PRESS_LIMIT = 12
NEWS_FETCH_LIMIT = max(1, int(os.environ.get("NEWS_FETCH_LIMIT", "30")))
NEWS_PER_SYMBOL_LIMIT = max(1, int(os.environ.get("NEWS_PER_SYMBOL_LIMIT", "6")))
FILINGS_LIMIT = 12
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
    return cached["text"]


def _filing_text_cache_set(link, text):
    _filing_text_cache[link] = {"time": time.time(), "text": text}


def _fetch_filing_text(link):
    if not link:
        return ""
    cached = _filing_text_cache_get(link)
    if cached is not None:
        return cached
    try:
        headers = _sec_headers()
    except Exception:
        _filing_text_cache_set(link, "")
        return ""
    headers["Accept"] = "text/html"
    try:
        payload = _fetch_text(link, headers)
    except Exception:
        _filing_text_cache_set(link, "")
        return ""
    text = _strip_html(payload)
    if len(text) > MAX_FILING_TEXT_CHARS:
        text = text[:MAX_FILING_TEXT_CHARS]
    _filing_text_cache_set(link, text)
    return text


def _analysis_key(kind, item):
    link = item.get("link") or ""
    if link:
        return (kind, link)
    title = item.get("title") or ""
    date = item.get("date") or ""
    symbol = item.get("symbol") or ""
    form = item.get("form") or ""
    return (kind, title, date, symbol, form)


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
    settings = _get_openai_settings()
    if not settings or not items:
        return items
    pending = []
    for item in items:
        key = _analysis_key("filing", item)
        cached = _analysis_cache_get(key)
        if cached:
            item.update(cached)
        else:
            pending.append((item, key))
    if not pending:
        return items
    payload_items = []
    targets = []
    for item, key in pending:
        content = _fetch_filing_text(item.get("link"))
        if not content:
            result = {
                "summary": "Sin datos verificables",
                "whatHappened": "No hay datos verificables",
                "impact": "bajo",
                "thesis": "no determinable",
            }
            item.update(result)
            _analysis_cache_set(key, result)
            continue
        target_id = len(payload_items)
        payload_items.append(
            {
                "id": target_id,
                "symbol": item.get("symbol") or "",
                "form": item.get("form") or "",
                "date": item.get("date") or "",
                "link": item.get("link") or "",
                "content": content,
            }
        )
        targets.append((target_id, item, key))
    if not payload_items:
        return items
    prompt = (
        "Resume SOLO con el contenido provisto. No inventes. "
        "Si content esta vacio, usa summary \"Sin datos verificables\" "
        "y whatHappened \"No hay datos verificables\". "
        "Devuelve SOLO un JSON array con objetos {id, summary, whatHappened, impact, thesis}. "
        "impact: \"alto\"|\"medio\"|\"bajo\". "
        "thesis: \"cambia\"|\"no cambia\"|\"no determinable\"."
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
            "summary": entry.get("summary"),
            "whatHappened": entry.get("whatHappened"),
            "impact": entry.get("impact"),
            "thesis": entry.get("thesis"),
        }
    for target_id, item, key in targets:
        result = mapped.get(target_id)
        if not result:
            continue
        item.update(result)
        _analysis_cache_set(key, result)
    return items


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
    for idx in range(min(6, len(accession_numbers))):
        accession = accession_numbers[idx]
        form = forms[idx] if idx < len(forms) else ""
        date = dates[idx] if idx < len(dates) else ""
        primary = primary_docs[idx] if idx < len(primary_docs) else ""
        accession_no = accession.replace("-", "")
        base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no}"
        link = f"{base}/{primary}" if primary else f"{base}/{accession}-index.html"
        items.append(
            {
                "form": form,
                "date": date,
                "link": link,
            }
        )

    _filings_cache[symbol] = {"time": time.time(), "data": items}
    return items


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
        for entry in entries:
            timestamp = _parse_iso_date(entry.get("date"))
            items.append(
                {
                    "form": entry.get("form"),
                    "date": entry.get("date"),
                    "link": entry.get("link"),
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


def _fetch_stooq_quote(symbol, previous_price=None):
    stooq_symbol = f"{symbol.lower()}.us"
    url = STOOQ_URL.format(symbol=urllib.parse.quote(stooq_symbol))
    payload = _fetch_text(url, STOOQ_HEADERS)
    parsed = _parse_stooq_csv(payload)
    if not parsed:
        return None
    price = parsed["price"]
    change = None
    base = previous_price
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
        "marketState": _market_state(),
    }


def fetch_stooq_quotes(symbols):
    results = {}
    for symbol in symbols:
        entry = _symbol_cache.get(symbol)
        prev_price = None
        if entry and entry.get("data"):
            prev_price = entry["data"].get("price")
        try:
            quote = _fetch_stooq_quote(symbol, prev_price)
            if quote:
                results[symbol] = quote
        except Exception:
            continue
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
    for symbol in symbols:
        item = payload.get(symbol)
        if not isinstance(item, dict) or item.get("status") == "error":
            continue
        price = _to_float(item.get("price"))
        if price is None:
            continue
        change = _to_float(item.get("change"))
        change_percent = _to_float(item.get("percent_change"))
        is_open = _normalize_state(item.get("is_market_open"))
        market_state = "open" if is_open else state
        results[symbol] = {
            "price": price,
            "change": change,
            "changePercent": change_percent,
            "marketState": market_state,
        }
    return results


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/api/stocks")
def api_stocks():
    symbols = parse_symbols()
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

    refresh_budget = _available_credits()
    refresh_list = _eligible_symbols(symbols)[:refresh_budget]
    error_message = None
    if refresh_list:
        try:
            quotes = fetch_quotes(refresh_list, api_key)
        except Exception as exc:
            quotes = {}
            error_message = str(exc) or "Error API"
            if _is_daily_limit_error(error_message):
                _mark_daily_limit_reached()
        if refresh_list:
            missing = [symbol for symbol in refresh_list if symbol not in quotes]
            if missing:
                fallback = fetch_stooq_quotes(missing)
                quotes.update(fallback)
        _consume_credits(len(refresh_list))
        now = time.time()
        for symbol in refresh_list:
            payload = quotes.get(symbol)
            if payload:
                _symbol_cache[symbol] = {"data": payload, "updatedAt": now}
            else:
                _symbol_cache[symbol] = {
                    "data": {"error": "Sin datos"},
                    "updatedAt": now,
                }
    elif _daily_remaining() == 0:
        fallback_symbols = _eligible_symbols(symbols)
        if fallback_symbols:
            try:
                quotes = fetch_stooq_quotes(fallback_symbols)
            except Exception:
                quotes = {}
            if quotes:
                now = time.time()
                for symbol in fallback_symbols:
                    payload = quotes.get(symbol)
                    if payload:
                        _symbol_cache[symbol] = {"data": payload, "updatedAt": now}
    data = []
    for symbol in symbols:
        entry = _symbol_cache.get(symbol)
        if not entry:
            data.append({"symbol": symbol, "error": "Pendiente"})
            continue
        payload = dict(entry["data"])
        payload["symbol"] = symbol
        payload["updatedAt"] = entry["updatedAt"]
        data.append(payload)

    response = {
        "updatedAt": int(time.time()),
        "data": data,
        "meta": {
            "creditsPerMinute": TWELVE_CREDITS_PER_MINUTE,
            "refreshed": len(refresh_list),
            "error": error_message,
        },
    }
    return jsonify(response)


@app.route("/api/filings")
def api_filings():
    symbol = request.args.get("symbol", "").strip()
    try:
        if symbol:
            data = _get_filings(symbol)
            _apply_filings_analysis(data)
            return jsonify({"symbol": symbol.upper(), "data": data})
        symbols = parse_symbols()
        data = _get_filings_stream(symbols)
        _apply_filings_analysis(data)
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
