"""מערכת ניטור טקסטואלית לנתניהו ולליכוד.

מקורות: ערוצי טלגרם ציבוריים + פידי RSS.
התראות: UltraMsg לקבוצת WhatsApp, דרך תור שליחה עמיד (outbox).
אחסון: SQLite — מונע דיווחים כפולים גם אחרי restart.

עקרונות מרכזיים בגרסה הזאת:
1. דדופ דו-שכבתי: פר-פריט (item_key) ופר-סיפור (clusters), כדי שאותה
   ידיעה משמונה מקורות לא תיהפך לשמונה הודעות בקבוצה.
2. שליחה אסינכרונית מתור מתמיד — סריקה לא נחסמת, ופריט שנכשל בשליחה
   לא הולך לאיבוד.
3. /health מחזיר 503 כשהמנוע תקוע, כדי ש-Render יפעיל restart במקום
   שהמערכת תמות בשקט.
"""

from __future__ import annotations

import hashlib
import html
import json
import logging
import os
import re
import sqlite3
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import feedparser
import pytz
import requests
from bs4 import BeautifulSoup
from flask import Flask, jsonify, request

from sources import (
    ALWAYS_ALERT_TELEGRAM,
    DEFAULT_SOURCE_WEIGHT,
    PRIMARY_TELEGRAM,
    RSS_FEEDS,
    SOURCE_WEIGHTS,
    TELEGRAM_CHANNELS,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


# מזהה גרסה — מופיע ב-/health וב-/routing כדי לדעת מיד אם הקוד
# שרץ בפועל הוא זה שהועלה לגיטהאב.
BUILD = "2026-08-20.pm-brief"

ISRAEL_TZ = pytz.timezone("Asia/Jerusalem")
DATA_DIR = Path(os.getenv("DATA_DIR", "./data")).resolve()
DB_PATH = DATA_DIR / "monitor.db"

ULTRA_ID = os.getenv("ULTRA_ID", "").strip()
ULTRA_TOKEN = os.getenv("ULTRA_TOKEN", "").strip()
GROUP_ID = os.getenv("GROUP_ID", "").strip()
WHATSAPP_DRY_RUN = env_bool("WHATSAPP_DRY_RUN", True)
SEND_STARTUP_MESSAGE = env_bool("SEND_STARTUP_MESSAGE", False)
SEND_EXISTING_ON_START = env_bool("SEND_EXISTING_ON_START", False)
# בסריקת האתחול מושתק רק מה שישן. ידיעה שפורסמה בדקות האחרונות
# תישלח גם אחרי דיפלוי, כדי שהעלייה מחדש לא תיצור חור בכיסוי.
BOOTSTRAP_FRESH_MINUTES = env_int("BOOTSTRAP_FRESH_MINUTES", 25, 0, 240)

SCAN_INTERVAL_SECONDS = env_int("SCAN_INTERVAL_SECONDS", 90, 30, 3600)
REQUEST_TIMEOUT_SECONDS = env_int("REQUEST_TIMEOUT_SECONDS", 12, 3, 60)
MAX_WORKERS = env_int("MAX_WORKERS", 12, 2, 30)
TELEGRAM_MESSAGES_PER_SCAN = env_int("TELEGRAM_MESSAGES_PER_SCAN", 20, 5, 50)

# דדופ בין מקורות
# החלון נמדד מהפעילות האחרונה באשכול, לא מיצירתו — סיפור שממשיך
# להתגלגל נשאר "אותו סיפור" כל עוד מגיעים אליו דיווחים.
CLUSTER_WINDOW_MINUTES = env_int("CLUSTER_WINDOW_MINUTES", 25, 2, 240)
CLUSTER_SIMILARITY = env_float("CLUSTER_SIMILARITY", 0.5, 0.2, 0.95)
CLUSTER_MIN_SHARED_TOKENS = env_int("CLUSTER_MIN_SHARED_TOKENS", 4, 2, 20)
# מדד הכלה — תופס כותרת קצרה מול פסקה ארוכה על אותו סיפור,
# מצב ש-Jaccard מפספס כי האיחוד גדול בהרבה מהחיתוך.
CLUSTER_CONTAINMENT = env_float("CLUSTER_CONTAINMENT", 0.72, 0.3, 1.0)
CLUSTER_MIN_CONTAINED_TOKENS = env_int("CLUSTER_MIN_CONTAINED_TOKENS", 3, 2, 15)
CLUSTER_CONTAINMENT_STRICT = env_float("CLUSTER_CONTAINMENT_STRICT", 0.9, 0.4, 1.0)
# רצפה שמונעת מאשכול גדול לבלוע פריטים שרק במקרה חולקים מילים
CLUSTER_JACCARD_FLOOR = env_float("CLUSTER_JACCARD_FLOOR", 0.22, 0.05, 0.5)
# תקרת טוקנים לאשכול — האשכול צובר מילים ממקורות שמצטרפים אליו
CLUSTER_MAX_TOKENS = env_int("CLUSTER_MAX_TOKENS", 30, 10, 200)
ESCALATION_THRESHOLD = env_int("ESCALATION_THRESHOLD", 5, 0, 50)

# תור שליחה
SEND_MIN_INTERVAL_SECONDS = env_float("SEND_MIN_INTERVAL_SECONDS", 1.5, 0.2, 30.0)
SEND_MAX_ATTEMPTS = env_int("SEND_MAX_ATTEMPTS", 60, 1, 500)
# כמה זמן להמשיך לנסות לפני ויתור. ניתוק של UltraMsg יכול להימשך
# שעות — הודעה לא נזרקת אחרי חמש דקות אלא ממתינה לחיבור.
RETRY_WINDOW_HOURS = env_int("RETRY_WINDOW_HOURS", 12, 1, 72)

# סיכומים
# ברירת מחדל: דיג'סט בכל שעה, 24/7. אפשר לצמצם עם DIGEST_HOURS="8,14,20".
def _parse_hours(raw: str) -> tuple[int, ...]:
    """תומך ב-"8-22", ב-"8,14,20" וב-"all"."""
    raw = raw.strip().lower()
    if raw in {"all", "24/7", "*"}:
        return tuple(range(24))
    hours: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if "-" in part:
            start, _, end = part.partition("-")
            if start.strip().isdigit() and end.strip().isdigit():
                first, last = int(start), int(end)
                hours.update(
                    range(first, last + 1)
                    if first <= last
                    # טווח שחוצה חצות, למשל 22-2
                    else list(range(first, 24)) + list(range(0, last + 1))
                )
        elif part.isdigit() and 0 <= int(part) <= 23:
            hours.add(int(part))
    return tuple(sorted(h for h in hours if 0 <= h <= 23))


# ברירת מחדל: סיכום שעתי בין 08:00 ל-22:00 בלבד.
# בלילה המערכת ממשיכה לסרוק ולשלוח התראות בזמן אמת — רק הסיכום
# השעתי שותק. לשינוי: DIGEST_HOURS="all" או "8,14,20".
# סיכומים בשעות קבועות במקום סיכום שעתי. פורמט: "HH:MM,HH:MM".
def _parse_slots(raw: str) -> tuple[tuple[int, int], ...]:
    slots: set[tuple[int, int]] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        hour, _, minute = part.partition(":")
        if not hour.strip().isdigit():
            continue
        h = int(hour)
        m_ = int(minute) if minute.strip().isdigit() else 0
        if 0 <= h <= 23 and 0 <= m_ <= 59:
            slots.add((h, m_))
    return tuple(sorted(slots))


DIGEST_SLOTS = _parse_slots(os.getenv("DIGEST_SLOTS", "12:00,16:00,20:30"))
DIGEST_HOURS: tuple[int, ...] = ()
DIGEST_LOOKBACK_HOURS = env_int("DIGEST_LOOKBACK_HOURS", 1, 1, 24)

# --- סיכום בוקר: מה שקרה בלילה ולא דווח בשעה עגולה ---
MORNING_BRIEF_HOUR = env_int("MORNING_BRIEF_HOUR", 8, 0, 23)
MORNING_BRIEF_ENABLED = env_bool("MORNING_BRIEF_ENABLED", True)
# --- סיכום לילה: כל היום בהודעה אחת, לפי חשיבות ---
NIGHT_WRAP_HOUR = env_int("NIGHT_WRAP_HOUR", 23, 0, 23)
NIGHT_WRAP_ENABLED = env_bool("NIGHT_WRAP_ENABLED", True)
WRAP_MAX_STORIES = env_int("WRAP_MAX_STORIES", 30, 5, 80)

# --- תדריך לראש הממשלה: 09:00 ו-21:00 ---
BRIEF_HOURS = tuple(
    sorted(
        {
            int(part)
            for part in os.getenv("PM_BRIEF_HOURS", "9,21").split(",")
            if part.strip().isdigit() and 0 <= int(part) <= 23
        }
    )
)
PM_BRIEF_ENABLED = env_bool("PM_BRIEF_ENABLED", True)
PM_BRIEF_ITEMS = env_int("PM_BRIEF_ITEMS", 6, 3, 15)
PM_BRIEF_MODEL = os.getenv("PM_BRIEF_MODEL", "claude-sonnet-4-6")
# הוחלף בסיכום הלילה של 23:00. להפעלה מחדש: DAILY_SUMMARY_ENABLED=true
DAILY_SUMMARY_ENABLED = env_bool("DAILY_SUMMARY_ENABLED", False)
DAILY_SUMMARY_HOUR = env_int("DAILY_SUMMARY_HOUR", 18, 0, 23)

# --- סיווג טון דרך ה-API של קלוד ---
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()
TONE_ENABLED = env_bool("TONE_ENABLED", True) and bool(ANTHROPIC_API_KEY)
TONE_MODEL = os.getenv("TONE_MODEL", "claude-sonnet-4-6")
TONE_TIMEOUT = env_int("TONE_TIMEOUT", 20, 5, 60)

# --- מהירות התפשטות ---
VELOCITY_SOURCES = env_int("VELOCITY_SOURCES", 4, 2, 30)
VELOCITY_MINUTES = env_int("VELOCITY_MINUTES", 10, 1, 120)

# --- בריאות מקורות ---
SILENT_SOURCE_HOURS = env_int("SILENT_SOURCE_HOURS", 6, 1, 72)
HEARTBEAT_HOUR = env_int("HEARTBEAT_HOUR", 8, 0, 23)
# סיכום הבוקר ממלא את התפקיד הזה. להפעלה: HEARTBEAT_ENABLED=true
HEARTBEAT_ENABLED = env_bool("HEARTBEAT_ENABLED", False)

# --- ערוץ גיבוי כשוואטסאפ נופל ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
BACKUP_AFTER_FAILURES = env_int("BACKUP_AFTER_FAILURES", 3, 1, 20)

# --- שקט בשבת ובחג ---
# ניטור רציף 24/7. מי שירצה שקט בשבת יגדיר QUIET_SHABBAT=true.
QUIET_SHABBAT = env_bool("QUIET_SHABBAT", False)
CANDLE_MINUTES = env_int("CANDLE_MINUTES", 40, 0, 120)
HAVDALAH_MINUTES = env_int("HAVDALAH_MINUTES", 42, 0, 120)
JERUSALEM_LAT = 31.7683
JERUSALEM_LON = 35.2137

# --- פקודות מתוך קבוצת הוואטסאפ ---
COMMANDS_ENABLED = env_bool("COMMANDS_ENABLED", True)

# --- העברה מקבוצות וואטסאפ אחרות שהמספר חבר בהן ---
# פורמט: RELAY_GROUPS="1203...@g.us=פורום עיתונאים,1204...@g.us=מטה"
# --- ניתוב לקבוצה נפרדת לפי מטרה ---
# קבוצת היעד לאזכורי שרה נתניהו. ריק = נשלח לקבוצה הראשית.
SARA_GROUP_ID = os.getenv("SARA_GROUP_ID", "").strip()
# האם אזכור של שרה יגיע גם לקבוצה הראשית, או רק לייעודית
SARA_ALSO_MAIN = env_bool("SARA_ALSO_MAIN", False)

RELAY_ENABLED = env_bool("RELAY_ENABLED", True)
RELAY_SHOW_SENDER = env_bool("RELAY_SHOW_SENDER", True)


def _parse_relay_groups() -> dict[str, str]:
    result: dict[str, str] = {}
    for chunk in os.getenv("RELAY_GROUPS", "").split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "=" in chunk:
            group_id, label = chunk.split("=", 1)
            result[group_id.strip()] = label.strip() or "קבוצת וואטסאפ"
        else:
            result[chunk] = "קבוצת וואטסאפ"
    return result


RELAY_GROUPS = _parse_relay_groups()

# היקף ההעברה — מצומצם יותר מהניטור הכללי.
# ברירת מחדל: רק נתניהו והליכוד. "all" = כל המטרות המנוטרות.
_relay_scope = os.getenv("RELAY_SUBJECTS", "netanyahu").strip().lower()
RELAY_SUBJECTS: set[str] = (
    set()
    if _relay_scope in {"all", "*"}
    else {part.strip() for part in _relay_scope.split(",") if part.strip()}
)

# --- X (טוויטר) ---
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "").strip()
X_ACCOUNTS = tuple(
    handle.strip().lstrip("@")
    for handle in os.getenv("X_ACCOUNTS", "netanyahu").split(",")
    if handle.strip()
)
X_ENABLED = env_bool("X_ENABLED", True) and bool(X_BEARER_TOKEN) and bool(X_ACCOUNTS)
X_MAX_RESULTS = env_int("X_MAX_RESULTS", 10, 5, 100)

# --- דירוג משמעותיות ---
# מתחת לסף הסיפור לא נדחף מיד — הוא יופיע בדיג'סט השעתי.
# 0 = דיווח בזמן אמת על הכל. הדדופ הוא מה שמונע הצפה, לא הסף.
# להעלאה: פקודת "סף 45" בקבוצה, או PUSH_THRESHOLD בדשבורד.
PUSH_THRESHOLD_DEFAULT = env_int("PUSH_THRESHOLD", 0, 0, 100)

DB_BUSY_TIMEOUT_MS = env_int("DB_BUSY_TIMEOUT_MS", 30000, 1000, 120000)
DB_INIT_RETRIES = env_int("DB_INIT_RETRIES", 8, 1, 30)

# --- סיכום חדשות שעתי ---
HEADLINE_RETENTION_HOURS = env_int("HEADLINE_RETENTION_HOURS", 48, 2, 336)
ROUNDUP_TOPICS = env_int("ROUNDUP_TOPICS", 6, 1, 20)
ROUNDUP_HEADLINES = env_int("ROUNDUP_HEADLINES", 12, 1, 40)
ROUNDUP_MAX_ITEMS = env_int("ROUNDUP_MAX_ITEMS", 500, 50, 3000)
# מקורות שנחשבים "כותרות ראשיות" לצורך הסיכום
HEADLINE_MIN_WEIGHT = env_float("HEADLINE_MIN_WEIGHT", 0.8, 0.1, 1.0)

RETENTION_DAYS = env_int("RETENTION_DAYS", 30, 3, 365)
PORT = env_int("PORT", 10000, 1, 65535)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("media-monitor")

HTTP = requests.Session()
HTTP.mount(
    "https://",
    requests.adapters.HTTPAdapter(
        pool_connections=MAX_WORKERS + 4, pool_maxsize=MAX_WORKERS + 4
    ),
)
HTTP.headers.update(
    {
        # אתרי חדשות ישראליים חוסמים User-Agent של בוט ב-403.
        "User-Agent": os.getenv(
            "USER_AGENT",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        ),
        "Accept": (
            "application/rss+xml, application/xml, application/atom+xml, "
            "text/xml;q=0.9, text/html;q=0.8, */*;q=0.5"
        ),
        "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
    }
)

app = Flask(__name__)
_threads_lock = threading.Lock()
_monitor_thread: threading.Thread | None = None
_sender_thread: threading.Thread | None = None
_watchdog_thread: threading.Thread | None = None

_runtime_status: dict[str, Any] = {
    "started_at": None,
    "last_scan_started": None,
    "last_scan_finished": None,
    "last_scan_new_stories": 0,
    "last_scan_duplicates": 0,
    "last_scan_suppressed": 0,
    "last_scan_items": 0,
    "last_scan_candidates": 0,
    "last_scan_sources_ok": 0,
    "last_scan_headlines": 0,
    "last_scan_seconds": None,
    "last_error": None,
    "consecutive_errors": 0,
    "iterations": 0,
    "last_send_ok": None,
    "last_send_error": None,
    "thread_error": None,
    "role": "starting",
    "consecutive_send_failures": 0,
    "whatsapp_connected": None,
    "whatsapp_status_detail": None,
    "backup_channel_used": None,
    "quiet_period": False,
    "tone_calls": 0,
    "tone_failures": 0,
}

BOOT_MONOTONIC = time.monotonic()
HEALTH_GRACE_SECONDS = env_int("HEALTH_GRACE_SECONDS", 180, 30, 900)

# ---------------------------------------------------------------------------
# Relevance matching
# ---------------------------------------------------------------------------

HEB = "\u0590-\u05FF"
HEB_PREFIX = "[והבלמשכ]{0,2}"

# --- מטרות הניטור -----------------------------------------------------------
# strong = מספיק לבדו. weak = דורש הקשר פוליטי (למשל "לפיד" שהוא גם אבוקה).

WATCHLIST: dict[str, dict[str, Any]] = {
    # ראשונה בסדר בכוונה: כשידיעה נוגעת גם לשרה וגם לנתניהו,
    # היא מסווגת כשרה ומנותבת לקבוצה הייעודית.
    "sara": {
        "label": "שרה נתניהו",
        "emoji": "👤",
        # שם מלא בדיוק, ותו לא.
        "strong": ("שרה נתניהו",),
        "weak": (),
        "latin": ("sara netanyahu", "sarah netanyahu"),
    },
    "netanyahu": {
        "label": "נתניהו / הליכוד",
        "emoji": "🚨",
        "strong": ("בנימין נתניהו", "נתניהו", "ביבי", "ליכוד", "ליכודניק",
                   "ליכודניקים"),
        "weak": (),
        "latin": ("netanyahu", "bibi", "likud"),
    },
    "eisenkot": {
        "label": "גדי אייזנקוט",
        "emoji": "🎙️",
        "strong": ("אייזנקוט", "איזנקוט", "גדי אייזנקוט"),
        "weak": (),
        "latin": ("eisenkot",),
    },
    "bennett": {
        "label": "נפתלי בנט",
        "emoji": "🎙️",
        "strong": ("נפתלי בנט", "בנט"),
        "weak": (),
        "latin": ("bennett",),
    },
    "lapid": {
        "label": "יאיר לפיד",
        "emoji": "🎙️",
        # "לפיד" לבדו הוא גם אבוקה ומשואה — לכן weak.
        "strong": ("יאיר לפיד",),
        "weak": ("לפיד",),
        "latin": ("lapid",),
    },
    "golan": {
        "label": "יאיר גולן",
        "emoji": "🎙️",
        # "גולן" לבדו לא נכנס בכלל — רמת הגולן מופיעה בחדשות כל היום,
        # ולעיתים קרובות בדיוק בהקשר פוליטי.
        "strong": ("יאיר גולן",),
        "weak": (),
        "latin": ("yair golan",),
    },
    "liberman": {
        "label": "אביגדור ליברמן",
        "emoji": "🎙️",
        "strong": ("ליברמן", "אביגדור ליברמן"),
        "weak": (),
        "latin": ("liberman", "lieberman"),
    },
}

PRIME_MINISTER_TERMS = ("ראש הממשלה", "רה״מ", 'רה"מ', "רה׳׳מ")

FOREIGN_CONTEXT = (
    "בריטניה", "הבריטי", "קנדה", "הקנדי", "הודו", "ההודי", "אוסטרליה",
    "האוסטרלי", "איטליה", "האיטלקי", "ספרד", "הספרדי", "צרפת", "הצרפתי",
    "יוון", "היווני", "פולין", "הפולני", "יפן", "היפני", "הונגריה",
    "ההונגרי", "אלבניה", "בלגיה", "הולנד", "שוודיה", "נורבגיה", "אירלנד",
    "סלובניה", "צ׳כיה", 'צ"כיה', "ניו זילנד", "הניו זילנדי",
)

PARTIES = (
    "ליכוד", "יש עתיד", "המחנה הממלכתי", "ישראל ביתנו", "ש״ס", 'ש"ס', "שס",
    "יהדות התורה", "עוצמה יהודית", "הציונות הדתית", "כחול לבן", "העבודה",
    "מרצ", "הדמוקרטים", "רע״מ", 'רע"מ', "חד״ש", 'חד"ש', "בלד", "נועם",
    "הבית היהודי", "ימינה", "בנט 2026",
)

POLITICAL_CONTEXT = PARTIES + (
    "כנסת", "מפלגה", "מפלגת", "מנדט", "מנדטים", "קואליציה", "אופוזיציה",
    "ממשלה", "בחירות", "ח״כ", 'ח"כ', "חבר הכנסת", "השר", "שרת", "פוליטי",
    "פוליטית", "גוש", "ראשות הממשלה", "אחוז החסימה", "פריימריז",
)

POLL_WORDS = ("סקר", "סקרים", "מדגם", "משאל", "נסקרים", "התפלגות")
POLL_CONTEXT = POLITICAL_CONTEXT + (
    "פאנל", "מכון", "לאזר", "לזר", "מנו גבע", "קמיל פוקס", "דיירקט פולס",
    "מדגם", "מצביעים", "בוחרים",
)

STATEMENT_WORDS = (
    "אמר", "אמרה", "הצהיר", "מסר", "תקף", "הגיב", "כתב", "צייץ", "פרסם",
    "בריאיון", "בראיון", "ריאיון", "ראיון", "בדבריו", "בדבריה", "הודיע",
    "קרא", "טען", "הבהיר", "נאם", "נאום", "התייחס", "זעם", "שיגר",
    "בהודעה", "בהצהרה", "מתראיין", "בכנס", "בישיבת",
)


def _hebrew_pattern(word: str) -> str:
    return rf"(?<![{HEB}]){HEB_PREFIX}{re.escape(word)}(?![{HEB}])"


def _build_re(hebrew: Iterable[str] = (), latin: Iterable[str] = ()) -> re.Pattern[str] | None:
    parts = [_hebrew_pattern(word) for word in hebrew]
    parts += [rf"\b{re.escape(word)}\b" for word in latin]
    if not parts:
        return None
    return re.compile("|".join(parts), re.IGNORECASE)


for _entry in WATCHLIST.values():
    _entry["strong_re"] = _build_re(_entry["strong"], _entry["latin"])
    _entry["weak_re"] = _build_re(_entry["weak"])

# הזיהוי מצומצם לשם המלא בלבד, לפי בקשה מפורשת. כל ניסיון לזהות
# "שרה" בהקשר גרר אזכורים של שרות בממשלה, ולכן המסלול הרופף הוסר
# כליל — אין התאמה חלקית ואין הסתמכות על הקשר.

PM_RE = _build_re(PRIME_MINISTER_TERMS)
FOREIGN_RE = _build_re(FOREIGN_CONTEXT)
POLITICAL_RE = _build_re(POLITICAL_CONTEXT)
POLL_WORD_RE = _build_re(POLL_WORDS)
POLL_CONTEXT_RE = _build_re(POLL_CONTEXT)
STATEMENT_RE = _build_re(STATEMENT_WORDS)

HEBREW_STOPWORDS = {
    "של",
    "על",
    "את",
    "עם",
    "לא",
    "זה",
    "זו",
    "הוא",
    "היא",
    "הם",
    "הן",
    "אחרי",
    "לפני",
    "כי",
    "גם",
    "אבל",
    "יותר",
    "כל",
    "אשר",
    "כדי",
    "מה",
    "מי",
    "אם",
    "או",
    "בין",
    "אל",
    "עד",
    "כך",
    "כמו",
    "היום",
    "אמר",
    "אמרה",
    "אמרו",
    "לאחר",
    "בעקבות",
    "לפי",
    "כפי",
    "נגד",
    "בתוך",
    "עוד",
    "רק",
    "כבר",
    "יש",
    "אין",
    "היה",
    "הייתה",
    "יהיה",
    "תוך",
    "בשל",
    "מול",
    "אצל",
    "הזה",
    "הזו",
    "כאשר",
}


def strip_prefix(token: str) -> str:
    if len(token) > 3 and token[0] in "והבלמשכ":
        return token[1:]
    return token


def significant_tokens(text: str) -> set[str]:
    """טוקנים משמעותיים להשוואת דמיון בין ידיעות ממקורות שונים."""
    cleaned = re.sub(r"https?://\S+", " ", text)
    raw = re.findall(rf"[{HEB}a-zA-Z0-9]+", cleaned)
    tokens: set[str] = set()
    for token in raw:
        token = token.lower()
        if token in HEBREW_STOPWORDS:
            continue
        token = strip_prefix(token)
        # מספרים דו-ספרתיים נשמרים: בסקרים המספר הוא התוכן המבחין
        # ("הליכוד 31"), ובלעדיו כותרת קצרה לא מזוהה מול ידיעה מלאה.
        if token.isdigit():
            if len(token) < 2:
                continue
        elif len(token) < 3 or token in HEBREW_STOPWORDS:
            continue
        tokens.add(token)
    return tokens


def jaccard(left: set[str], right: set[str]) -> tuple[float, int]:
    if not left or not right:
        return 0.0, 0
    shared = left & right
    union = left | right
    return len(shared) / len(union), len(shared)


def is_same_story(item: set[str], cluster: set[str]) -> tuple[bool, float, int]:
    """שני מסלולי התאמה, כי מקורות מדווחים באורכים שונים מאוד.

    מסלול א — דמיון Jaccard: לטקסטים באורך דומה.
    מסלול ב — הכלה: כותרת קצרה שכולה מוכלת בפסקה ארוכה. Jaccard
    מפספס אותה כי האיחוד גדול בהרבה מהחיתוך, ולכן נמדדת גם ההכלה
    ביחס לצד הקצר — עם רצפת Jaccard שמונעת בליעה של פריטים זרים.
    """
    if not item or not cluster:
        return False, 0.0, 0
    shared = len(item & cluster)
    if shared < 2:
        return False, 0.0, shared

    jac = shared / len(item | cluster)
    containment = shared / min(len(item), len(cluster))

    if jac >= CLUSTER_SIMILARITY and shared >= CLUSTER_MIN_SHARED_TOKENS:
        return True, jac, shared
    # ככל שיש פחות מילים משותפות, נדרשת הכלה גבוהה יותר — שלוש מילים
    # משותפות זה מעט מדי כדי להסתמך על הכלה חלקית.
    required = (
        CLUSTER_CONTAINMENT
        if shared >= CLUSTER_MIN_SHARED_TOKENS
        else CLUSTER_CONTAINMENT_STRICT
    )
    if (
        containment >= required
        and shared >= CLUSTER_MIN_CONTAINED_TOKENS
        and jac >= CLUSTER_JACCARD_FLOOR
    ):
        return True, containment, shared
    return False, max(jac, 0.0), shared


_custom_cache: dict[str, Any] = {"raw": None, "compiled": {}}


def custom_subjects() -> dict[str, dict[str, Any]]:
    raw = get_state("custom_subjects") or "{}"
    if _custom_cache["raw"] != raw:
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            data = {}
        compiled: dict[str, dict[str, Any]] = {}
        for key, entry in data.items():
            terms = entry.get("terms") or [entry.get("label", key)]
            compiled[key] = {
                "label": entry.get("label", key),
                "emoji": "🎯",
                "strong_re": _build_re(terms),
                "weak_re": None,
            }
        _custom_cache["raw"] = raw
        _custom_cache["compiled"] = compiled
    return dict(_custom_cache["compiled"])


def all_subjects() -> dict[str, dict[str, Any]]:
    merged = dict(WATCHLIST)
    merged.update(custom_subjects())
    return merged


def subject_entry(key: str) -> dict[str, Any]:
    return all_subjects().get(key, {})


def detect_subjects(text: str) -> list[str]:
    """אילו מהמטרות מוזכרות בטקסט."""
    political = bool(POLITICAL_RE and POLITICAL_RE.search(text))
    subjects: list[str] = []
    for key, entry in all_subjects().items():
        strong_re = entry.get("strong_re")
        weak_re = entry.get("weak_re")
        if strong_re is not None and strong_re.search(text):
            subjects.append(key)
        elif political and weak_re is not None and weak_re.search(text):
            subjects.append(key)

    # "ראש הממשלה" — רלוונטי אלא אם מדובר בבירור בראש ממשלה זר.
    if (
        "netanyahu" not in subjects
        and PM_RE is not None
        and PM_RE.search(text)
        and not (FOREIGN_RE and FOREIGN_RE.search(text))
    ):
        subjects.append("netanyahu")
    return subjects


MANDATE_RE = re.compile(rf"(?<![{HEB}]){HEB_PREFIX}מנדט(?:ים)?(?![{HEB}])")


def is_political_poll(text: str, subjects: list[str]) -> bool:
    """כל סקר פוליטי בישראל — גם כשאף אחת מהמטרות לא מוזכרת בו."""
    has_poll_word = bool(POLL_WORD_RE and POLL_WORD_RE.search(text))
    # כלל שני: מנדטים יחד עם מספר הם מפת סקר גם בלי המילה "סקר",
    # למשל "בחירות 2026: שוויון - 23 מנדטים לכל אחד".
    has_mandate_figures = bool(
        MANDATE_RE.search(text) and re.search(r"\d{1,3}", text)
    )
    if not (has_poll_word or has_mandate_figures):
        return False
    if subjects:
        return True
    return bool(POLL_CONTEXT_RE and POLL_CONTEXT_RE.search(text))


def looks_like_statement(text: str) -> bool:
    if STATEMENT_RE and STATEMENT_RE.search(text):
        return True
    return bool(re.search(r'["\u05f4\u201c\u201d].{8,}["\u05f4\u201c\u201d]', text))


def classify(text: str) -> tuple[str | None, list[str], bool]:
    """מחזיר (סוג, מטרות, האם התבטאות)."""
    subjects = detect_subjects(text)
    if is_political_poll(text, subjects):
        return "poll", subjects, False
    if subjects:
        return subjects[0], subjects, looks_like_statement(text)
    return None, [], False


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_local = threading.local()


def db() -> sqlite3.Connection:
    """חיבור SQLite אחד לכל thread, במקום חיבור חדש לכל שאילתה."""
    connection: sqlite3.Connection | None = getattr(_local, "connection", None)
    if connection is not None:
        return connection

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(
        DB_PATH, timeout=20, isolation_level=None, check_same_thread=False
    )
    connection.row_factory = sqlite3.Row
    # busy_timeout חייב להיקבע ראשון, אחרת הפקודות הבאות נכשלות מיד
    # במקום להמתין. WAL דורש נעילה בלעדית ויכול להיכשל כששני מופעים
    # חולקים את אותו דיסק בזמן דיפלוי — זו לא סיבה להפיל את השירות.
    connection.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
    try:
        connection.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError as exc:
        LOGGER.warning("Could not enable WAL (continuing anyway): %s", exc)
    try:
        connection.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.OperationalError:
        pass
    _local.connection = connection
    return connection


def table_columns(table: str) -> set[str]:
    return {
        str(row["name"]) for row in db().execute(f"PRAGMA table_info({table})").fetchall()
    }


def migrate_db() -> None:
    """הוספת עמודות לטבלאות שכבר קיימות בדיסק מגרסאות קודמות.

    CREATE TABLE IF NOT EXISTS לא נוגע בטבלה קיימת, ולכן בלי המיגרציה
    הזאת כל סריקה תיפול על עמודה חסרה.
    """
    connection = db()
    additions = {
        "mentions": {
            "cluster_id": "INTEGER",
            "is_duplicate": "INTEGER NOT NULL DEFAULT 0",
        },
        "outbox": {
            "sent_at_utc": "TEXT",
            "last_error": "TEXT",
            "target": "TEXT",
        },
        "clusters": {
            "escalated": "INTEGER NOT NULL DEFAULT 0",
            "tone": "TEXT",
            "angle": "TEXT",
            "response_needed": "INTEGER NOT NULL DEFAULT 0",
            "score": "INTEGER NOT NULL DEFAULT 0",
            "pushed": "INTEGER NOT NULL DEFAULT 0",
        },
    }
    for table, columns in additions.items():
        existing = table_columns(table)
        if not existing:
            continue
        for column, definition in columns.items():
            if column in existing:
                continue
            connection.execute(
                f"ALTER TABLE {table} ADD COLUMN {column} {definition}"
            )
            LOGGER.info("Schema migration: added %s.%s", table, column)


def init_db() -> None:
    """יוצר סכימה. עמיד לנעילה זמנית של מופע אחר על אותו דיסק."""
    last_error: Exception | None = None
    for attempt in range(DB_INIT_RETRIES):
        try:
            _init_db_once()
            return
        except sqlite3.OperationalError as exc:
            last_error = exc
            delay = min(20, 2 ** attempt)
            LOGGER.warning(
                "Database busy on init (attempt %s/%s), retrying in %ss: %s",
                attempt + 1, DB_INIT_RETRIES, delay, exc,
            )
            # החיבור עלול להיות במצב לא תקין — נפתח אותו מחדש בניסיון הבא
            connection = getattr(_local, "connection", None)
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
                _local.connection = None
            time.sleep(delay)
    raise RuntimeError(f"Database unavailable after retries: {last_error}")


def _init_db_once() -> None:
    db().executescript(
        """
        CREATE TABLE IF NOT EXISTS seen_items (
            item_key TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            link TEXT,
            first_seen_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS clusters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tokens TEXT NOT NULL,
            sample_text TEXT NOT NULL,
            kind TEXT NOT NULL,
            sources TEXT NOT NULL,
            source_count INTEGER NOT NULL DEFAULT 1,
            escalated INTEGER NOT NULL DEFAULT 0,
            tone TEXT,
            angle TEXT,
            response_needed INTEGER NOT NULL DEFAULT 0,
            score INTEGER NOT NULL DEFAULT 0,
            pushed INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_key TEXT UNIQUE NOT NULL,
            cluster_id INTEGER,
            local_date TEXT NOT NULL,
            local_time TEXT NOT NULL,
            source_key TEXT NOT NULL,
            source_display TEXT NOT NULL,
            text TEXT NOT NULL,
            link TEXT,
            kind TEXT NOT NULL,
            is_duplicate INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            body TEXT NOT NULL,
            target TEXT,
            priority INTEGER NOT NULL DEFAULT 5,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_utc TEXT NOT NULL,
            created_at_utc TEXT NOT NULL,
            sent_at_utc TEXT,
            last_error TEXT
        );

        CREATE TABLE IF NOT EXISTS headlines (
            item_key TEXT PRIMARY KEY,
            source_key TEXT NOT NULL,
            display TEXT NOT NULL,
            text TEXT NOT NULL,
            link TEXT,
            relevant INTEGER NOT NULL DEFAULT 0,
            created_at_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_headlines_created
            ON headlines(created_at_utc);

        CREATE TABLE IF NOT EXISTS source_health (
            source_key TEXT PRIMARY KEY,
            display TEXT NOT NULL,
            last_item_utc TEXT NOT NULL,
            items_seen INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS app_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_mentions_date ON mentions(local_date);
        CREATE INDEX IF NOT EXISTS idx_clusters_created ON clusters(created_at_utc);
        CREATE INDEX IF NOT EXISTS idx_outbox_pending
            ON outbox(status, next_attempt_utc);
        """
    )
    migrate_db()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_state(key: str) -> str | None:
    row = db().execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
    return str(row["value"]) if row else None


def set_state(key: str, value: str) -> None:
    db().execute(
        """
        INSERT INTO app_state(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def filter_unseen(keys: list[str]) -> set[str]:
    """מחזיר את המפתחות שעדיין לא נראו — בשאילתות batch ולא אחת לפריט."""
    unseen = set(keys)
    connection = db()
    for start in range(0, len(keys), 400):
        chunk = keys[start : start + 400]
        placeholders = ",".join("?" * len(chunk))
        rows = connection.execute(
            f"SELECT item_key FROM seen_items WHERE item_key IN ({placeholders})",
            chunk,
        ).fetchall()
        unseen.difference_update(str(row["item_key"]) for row in rows)
    return unseen


def mark_seen(item_key: str, source: str, link: str) -> None:
    db().execute(
        """
        INSERT OR IGNORE INTO seen_items(item_key, source, link, first_seen_utc)
        VALUES(?, ?, ?, ?)
        """,
        (item_key, source, link, utc_now_iso()),
    )


def mentions_for_date(local_date: str) -> list[sqlite3.Row]:
    return list(
        db().execute(
            """
            SELECT * FROM mentions
            WHERE local_date = ?
            ORDER BY local_time ASC, id ASC
            """,
            (local_date,),
        ).fetchall()
    )


def cleanup_database() -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    outbox_cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    connection = db()
    connection.execute("DELETE FROM seen_items WHERE first_seen_utc < ?", (cutoff,))
    connection.execute("DELETE FROM mentions WHERE created_at_utc < ?", (cutoff,))
    connection.execute("DELETE FROM clusters WHERE created_at_utc < ?", (cutoff,))
    connection.execute(
        "DELETE FROM headlines WHERE created_at_utc < ?",
        ((datetime.now(timezone.utc)
          - timedelta(hours=HEADLINE_RETENTION_HOURS)).isoformat(),),
    )
    connection.execute(
        "DELETE FROM outbox WHERE status IN ('sent','failed') AND created_at_utc < ?",
        (outbox_cutoff,),
    )
    LOGGER.info("Database cleanup completed")


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------


def israel_now() -> datetime:
    return datetime.now(ISRAEL_TZ)


def sunset_local(day: datetime) -> datetime | None:
    """חישוב שקיעה בירושלים (NOAA מקורב) — בלי ספריות חיצוניות."""
    import math

    n = day.timetuple().tm_yday
    lat = math.radians(JERUSALEM_LAT)
    gamma = 2 * math.pi / 365 * (n - 1)
    decl = (
        0.006918
        - 0.399912 * math.cos(gamma)
        + 0.070257 * math.sin(gamma)
        - 0.006758 * math.cos(2 * gamma)
        + 0.000907 * math.sin(2 * gamma)
        - 0.002697 * math.cos(3 * gamma)
        + 0.00148 * math.sin(3 * gamma)
    )
    eqtime = 229.18 * (
        0.000075
        + 0.001868 * math.cos(gamma)
        - 0.032077 * math.sin(gamma)
        - 0.014615 * math.cos(2 * gamma)
        - 0.040849 * math.sin(2 * gamma)
    )
    cos_ha = (
        math.cos(math.radians(90.833)) / (math.cos(lat) * math.cos(decl))
        - math.tan(lat) * math.tan(decl)
    )
    if not -1 <= cos_ha <= 1:
        return None
    ha = math.degrees(math.acos(cos_ha))
    minutes_utc = 720 + 4 * (ha - JERUSALEM_LON) - eqtime
    utc_midnight = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return (utc_midnight + timedelta(minutes=minutes_utc)).astimezone(ISRAEL_TZ)


def _yom_tov_dates(year: int) -> set[tuple[int, int]]:
    """תאריכי חג שבהם אין מלאכה. משתמש ב-pyluach אם מותקן."""
    try:
        from pyluach import dates as _pd  # type: ignore
    except Exception:
        return set()

    result: set[tuple[int, int]] = set()
    day = datetime(year, 1, 1)
    while day.year == year:
        try:
            heb = _pd.HebrewDate.from_pydate(day.date())
            holiday = heb.holiday(israel=True)
        except Exception:
            holiday = None
        if holiday in {
            "Rosh Hashana", "Yom Kippur", "Succos", "Shemini Atzeres",
            "Simchas Torah", "Pesach", "Shavuos",
        }:
            result.add((day.month, day.day))
        day += timedelta(days=1)
    return result


_yom_tov_cache: dict[int, set[tuple[int, int]]] = {}


def is_no_work_day(day: datetime) -> bool:
    year = day.year
    if year not in _yom_tov_cache:
        _yom_tov_cache[year] = _yom_tov_dates(year)
    return (day.month, day.day) in _yom_tov_cache[year]


def in_quiet_period(now: datetime | None = None) -> bool:
    """שבת או יום טוב — מהדלקת נרות ועד צאת החג."""
    if not QUIET_SHABBAT:
        return False
    now = now or israel_now()

    for offset in (-1, 0):
        day = now + timedelta(days=offset)
        # ערב שבת או ערב חג
        next_day = day + timedelta(days=1)
        starts = day.weekday() == 4 or is_no_work_day(next_day)
        if not starts:
            continue
        sunset = sunset_local(day)
        if sunset is None:
            continue
        start = sunset - timedelta(minutes=CANDLE_MINUTES)

        # סוף התקופה: היום האחרון ברצף שאין בו מלאכה
        end_day = next_day
        for _ in range(3):
            following = end_day + timedelta(days=1)
            if is_no_work_day(following):
                end_day = following
            else:
                break
        end_sunset = sunset_local(end_day)
        if end_sunset is None:
            continue
        end = end_sunset + timedelta(minutes=HAVDALAH_MINUTES)

        if start <= now <= end:
            return True
    return False


def clean_text(value: str) -> str:
    soup = BeautifulSoup(html.unescape(value or ""), "html.parser")
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def normalized_text(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(rf"[^\w{HEB}]+", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def make_item_key(source: str, link: str, text: str) -> str:
    identity = (
        f"{source}|{link.strip()}"
        if link.strip()
        else f"{source}|{normalized_text(text)}"
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Cross-source clustering
# ---------------------------------------------------------------------------


def find_or_create_cluster(
    text: str, kind: str, source_display: str
) -> tuple[int, bool, int]:
    """מחזיר (cluster_id, is_new, source_count).

    אם הידיעה כבר דווחה במקור אחר בחלון הזמן — מצטרפים לאשכול הקיים
    במקום לשלוח התראה נוספת.
    """
    tokens = significant_tokens(text)
    now_utc = datetime.now(timezone.utc)
    window_start = (now_utc - timedelta(minutes=CLUSTER_WINDOW_MINUTES)).isoformat()
    connection = db()

    if len(tokens) >= 2:
        # התאמה רק בתוך אותה מטרה — "תקף את הממשלה בריאיון" הוא ניסוח
        # נפוץ, ובלי ההפרדה הזו התבטאויות של אנשים שונים היו מתאחדות.
        rows = connection.execute(
            """
            SELECT id, tokens, sources, source_count FROM clusters
            WHERE updated_at_utc >= ? AND kind = ?
            ORDER BY id DESC
            LIMIT 400
            """,
            (window_start, kind),
        ).fetchall()

        best_id: int | None = None
        best_score = 0.0
        best_sources: list[str] = []
        best_count = 0
        best_tokens: set[str] = set()
        for row in rows:
            other = set(str(row["tokens"]).split())
            matched, score, _shared = is_same_story(tokens, other)
            if matched and score > best_score:
                best_id = int(row["id"])
                best_score = score
                try:
                    best_sources = list(json.loads(str(row["sources"])))
                except (ValueError, TypeError):
                    best_sources = []
                best_count = int(row["source_count"])
                best_tokens = other

        if best_id is not None:
            if source_display not in best_sources:
                best_sources.append(source_display)
                best_count = len(best_sources)
            # האשכול צובר את אוצר המילים של כל המקורות שהצטרפו אליו,
            # כך שכל דיווח נוסף מקל על זיהוי הדיווח הבא.
            merged = set(best_tokens) | tokens
            if len(merged) > CLUSTER_MAX_TOKENS:
                merged = set(sorted(merged)[:CLUSTER_MAX_TOKENS])
            connection.execute(
                """
                UPDATE clusters
                SET sources = ?, source_count = ?, tokens = ?, updated_at_utc = ?
                WHERE id = ?
                """,
                (json.dumps(best_sources, ensure_ascii=False), best_count,
                 " ".join(sorted(merged)), now_utc.isoformat(), best_id),
            )
            return best_id, False, best_count

    cursor = connection.execute(
        """
        INSERT INTO clusters(
            tokens, sample_text, kind, sources, source_count,
            escalated, created_at_utc, updated_at_utc
        ) VALUES(?, ?, ?, ?, 1, 0, ?, ?)
        """,
        (
            " ".join(sorted(tokens)),
            text[:400],
            kind,
            json.dumps([source_display], ensure_ascii=False),
            now_utc.isoformat(),
            now_utc.isoformat(),
        ),
    )
    return int(cursor.lastrowid or 0), True, 1


TONE_PROMPT = """אתה אנליסט תקשורת ישראלי. לפניך ידיעה שפורסמה זה עתה.
קבע:
1. tone — היחס לנושא המרכזי: "עוין", "אוהד" או "נייטרלי".
2. angle — זווית הסיקור בשתיים עד ארבע מילים בעברית.
3. response_needed — האם הידיעה מחייבת תגובה תקשורתית מיידית (true/false).

החזר JSON בלבד, בלי טקסט נוסף ובלי סימוני קוד:
{"tone": "...", "angle": "...", "response_needed": false}

הידיעה:
"""


def analyze_tone(text: str) -> dict[str, Any] | None:
    """קריאה אחת ל-API לכל סיפור חדש. נכשל בשקט — לעולם לא חוסם התראה."""
    if not TONE_ENABLED:
        return None
    try:
        response = HTTP.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": TONE_MODEL,
                "max_tokens": 200,
                "messages": [
                    {"role": "user", "content": TONE_PROMPT + text[:1200]}
                ],
            },
            timeout=TONE_TIMEOUT,
        )
        response.raise_for_status()
        blocks = response.json().get("content", [])
        raw = "".join(
            block.get("text", "") for block in blocks if block.get("type") == "text"
        )
        raw = re.sub(r"^```(?:json)?|```$", "", raw.strip(), flags=re.MULTILINE).strip()
        parsed = json.loads(raw)
        _runtime_status["tone_calls"] = int(_runtime_status["tone_calls"]) + 1
        tone = str(parsed.get("tone", "")).strip()
        return {
            "tone": tone if tone in {"עוין", "אוהד", "נייטרלי"} else "נייטרלי",
            "angle": str(parsed.get("angle", "")).strip()[:60],
            "response_needed": bool(parsed.get("response_needed")),
        }
    except Exception as exc:
        _runtime_status["tone_failures"] = int(_runtime_status["tone_failures"]) + 1
        LOGGER.warning("Tone analysis failed: %s", str(exc)[:200])
        return None


def save_tone(cluster_id: int, tone: dict[str, Any]) -> None:
    db().execute(
        "UPDATE clusters SET tone=?, angle=?, response_needed=? WHERE id=?",
        (tone["tone"], tone["angle"], 1 if tone["response_needed"] else 0, cluster_id),
    )


def cluster_sources(cluster_id: int) -> list[str]:
    row = db().execute(
        "SELECT sources FROM clusters WHERE id = ?", (cluster_id,)
    ).fetchone()
    if not row:
        return []
    try:
        return list(json.loads(str(row["sources"])))
    except (ValueError, TypeError):
        return []


def mark_escalated(cluster_id: int) -> bool:
    cursor = db().execute(
        "UPDATE clusters SET escalated = 1 WHERE id = ? AND escalated = 0",
        (cluster_id,),
    )
    return bool(cursor.rowcount)


# ---------------------------------------------------------------------------
# Source adapters
# ---------------------------------------------------------------------------


def fetch_telegram(channel: str) -> list[dict[str, str]]:
    url = f"https://t.me/s/{channel}"
    try:
        response = HTTP.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        wraps = soup.find_all("div", class_="tgme_widget_message_wrap")
        selected = wraps[-TELEGRAM_MESSAGES_PER_SCAN:]
        messages: list[dict[str, str]] = []
        for wrap in selected:
            text_div = wrap.find("div", class_="tgme_widget_message_text")
            if not text_div:
                continue
            text = clean_text(text_div.get_text(" ", strip=True))
            if len(text) < 15:
                continue
            link_tag = wrap.find("a", class_="tgme_widget_message_date")
            link = str(link_tag.get("href", "")) if link_tag else url
            published = ""
            time_tag = wrap.find("time")
            if time_tag and time_tag.get("datetime"):
                published = str(time_tag.get("datetime"))
            message = {
                "text": text,
                "link": link,
                "source": channel,
                "published": published,
            }
            if channel in PRIMARY_TELEGRAM:
                message["force_subject"] = PRIMARY_TELEGRAM[channel]
                message["always_alert"] = "1"
            elif channel in ALWAYS_ALERT_TELEGRAM:
                message["always_alert"] = "1"
            messages.append(message)
        return messages
    except requests.RequestException as exc:
        LOGGER.warning("Telegram source failed: %s | %s", channel, exc)
        return []


def fetch_rss(name: str, url: str) -> list[dict[str, str]]:
    try:
        response = HTTP.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        messages: list[dict[str, str]] = []
        for entry in feed.entries[:15]:
            title = clean_text(str(entry.get("title", "")))
            description = clean_text(
                str(entry.get("summary", entry.get("description", "")))
            )
            text = clean_text(f"{title} {description}")
            if not text:
                continue
            published = ""
            stamp = entry.get("published_parsed") or entry.get("updated_parsed")
            if stamp:
                try:
                    published = datetime(
                        *stamp[:6], tzinfo=timezone.utc
                    ).isoformat()
                except (TypeError, ValueError):
                    published = ""
            messages.append(
                {
                    "text": text,
                    "link": str(entry.get("link", "")),
                    "source": name,
                    "title": title,
                    "published": published,
                }
            )
        return messages
    except (requests.RequestException, ValueError) as exc:
        LOGGER.warning("RSS source failed: %s | %s", name, exc)
        return []


def x_user_id(username: str) -> str | None:
    """מזהה מספרי לחשבון. נשלף פעם אחת ונשמר — קריאת פרופיל עולה כסף."""
    cached = get_state(f"x_user_id:{username}")
    if cached:
        return cached
    try:
        response = HTTP.get(
            f"https://api.x.com/2/users/by/username/{username}",
            headers={"Authorization": f"Bearer {X_BEARER_TOKEN}"},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        user_id = str(response.json().get("data", {}).get("id", ""))
        if user_id:
            set_state(f"x_user_id:{username}", user_id)
            LOGGER.info("Resolved X account @%s to id %s", username, user_id)
            return user_id
    except requests.RequestException as exc:
        LOGGER.warning("Could not resolve X account @%s: %s", username, exc)
    return None


def fetch_x(username: str) -> list[dict[str, str]]:
    """ציוצים חדשים בלבד. since_id מבטיח שסריקה ריקה לא נחשבת קריאה."""
    if not X_ENABLED:
        return []
    user_id = x_user_id(username)
    if not user_id:
        return []

    params: dict[str, Any] = {
        "max_results": X_MAX_RESULTS,
        "tweet.fields": "created_at,referenced_tweets",
        "exclude": "retweets,replies",
    }
    since_id = get_state(f"x_since:{username}")
    if since_id:
        params["since_id"] = since_id

    try:
        response = HTTP.get(
            f"https://api.x.com/2/users/{user_id}/tweets",
            headers={"Authorization": f"Bearer {X_BEARER_TOKEN}"},
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code == 429:
            LOGGER.warning("X rate limit reached — skipping this cycle")
            return []
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        LOGGER.warning("X source failed for @%s: %s", username, str(exc)[:200])
        return []

    tweets = payload.get("data") or []
    newest = payload.get("meta", {}).get("newest_id")
    if newest:
        set_state(f"x_since:{username}", str(newest))
    elif not since_id and tweets:
        # סריקה ראשונה בלי since_id — מסמנים נקודת התחלה
        set_state(f"x_since:{username}", str(tweets[0].get("id")))

    messages: list[dict[str, str]] = []
    for tweet in tweets:
        text = clean_text(str(tweet.get("text", "")))
        if not text:
            continue
        messages.append(
            {
                "text": text,
                "link": f"https://x.com/{username}/status/{tweet.get('id')}",
                "source": f"x:{username}",
                # ציוץ מהחשבון עצמו הוא מקור ראשוני — תמיד רלוונטי,
                # ותמיד נשלח גם אם התקשורת כבר דיווחה על התוכן.
                "force_subject": "netanyahu" if username.lower() == "netanyahu"
                else "",
                "always_alert": "1",
            }
        )
    if messages:
        LOGGER.info("X: %s new tweets from @%s", len(messages), username)
    return messages


def validate_rss_feeds() -> None:
    """בדיקת תקינות חד-פעמית בעלייה — פיד שבור לא נשאר שקוף."""
    broken: list[str] = []
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, 8)) as executor:
        futures = {
            executor.submit(fetch_rss, name, url): name
            for name, url in active_feeds().items()
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                if not future.result():
                    broken.append(name)
            except Exception:
                broken.append(name)
    if broken:
        LOGGER.warning("RSS feeds returned nothing: %s", ", ".join(sorted(broken)))
    else:
        LOGGER.info("All %s RSS feeds responded", len(active_feeds()))


def extra_feeds() -> dict[str, str]:
    """פידים שנוספו בזמן ריצה, בלי דיפלוי."""
    try:
        return dict(json.loads(get_state("extra_feeds") or "{}"))
    except (ValueError, TypeError):
        return {}


def active_feeds() -> dict[str, str]:
    merged = dict(RSS_FEEDS)
    merged.update(extra_feeds())
    for name in json.loads(get_state("disabled_feeds") or "[]"):
        merged.pop(name, None)
    return merged


def fetch_all_sources() -> list[tuple[str, str, str, dict[str, str]]]:
    results: list[tuple[str, str, str, dict[str, str]]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures: dict[Any, tuple[str, str, str]] = {}
        for key, display in TELEGRAM_CHANNELS.items():
            futures[executor.submit(fetch_telegram, key)] = ("TG", key, display)
        for name, url in active_feeds().items():
            futures[executor.submit(fetch_rss, name, url)] = ("RSS", name, name)
        if X_ENABLED:
            for handle in X_ACCOUNTS:
                futures[executor.submit(fetch_x, handle)] = (
                    "X", f"x:{handle}", f"𝕏 @{handle}"
                )

        for future in as_completed(futures):
            source_type, key, display = futures[future]
            try:
                for item in future.result():
                    results.append((source_type, key, display, item))
            except Exception as exc:
                LOGGER.exception("Source processing failed: %s | %s", key, exc)
    return results


# ---------------------------------------------------------------------------
# WhatsApp outbox
# ---------------------------------------------------------------------------


def split_message(text: str, limit: int = 3500) -> list[str]:
    if len(text) <= limit:
        return [text]

    parts: list[str] = []
    current = ""
    for paragraph in text.split("\n"):
        candidate = f"{current}\n{paragraph}".strip()
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            parts.append(current)
            current = ""
        while len(paragraph) > limit:
            parts.append(paragraph[:limit])
            paragraph = paragraph[limit:]
        current = paragraph
    if current:
        parts.append(current)
    return parts


def muted_until() -> datetime | None:
    raw = get_state("muted_until")
    if not raw:
        return None
    try:
        until = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return until if until > datetime.now(timezone.utc) else None


def should_hold(priority: int) -> bool:
    """שבת/חג או השתקה ידנית — ההודעה נשמרת ולא נשלחת עכשיו."""
    if priority <= 0:
        return False
    return bool(in_quiet_period() or muted_until())


DUPLICATE_SEND_WINDOW_SECONDS = env_int(
    "DUPLICATE_SEND_WINDOW_SECONDS", 900, 0, 7200
)


def already_queued(body: str, target: str) -> bool:
    """חוסם הודעה זהה שכבר נשלחה לאותה קבוצה זה עתה.

    רשת ביטחון: גם אם משום מה ירוצו שני מנועים במקביל, המשתמש
    לא יראה כפילויות.
    """
    if DUPLICATE_SEND_WINDOW_SECONDS <= 0:
        return False
    since = (
        datetime.now(timezone.utc)
        - timedelta(seconds=DUPLICATE_SEND_WINDOW_SECONDS)
    ).isoformat()
    row = db().execute(
        """
        SELECT 1 FROM outbox
        WHERE body = ? AND IFNULL(target, '') = ? AND created_at_utc >= ?
        LIMIT 1
        """,
        (body, target, since),
    ).fetchone()
    return row is not None


def enqueue_message(text: str, priority: int = 5,
                    target: str | None = None) -> None:
    """כותב לתור. הכתיבה עמידה — restart לא מאבד הודעות."""
    parts = split_message(text)
    now = utc_now_iso()
    held = should_hold(priority)
    connection = db()
    for index, part in enumerate(parts, start=1):
        body = f"({index}/{len(parts)})\n{part}" if len(parts) > 1 else part
        destination = target or GROUP_ID
        if already_queued(body, destination):
            LOGGER.warning("Skipping duplicate message to %s",
                           destination[-12:] if destination else "default")
            continue
        connection.execute(
            """
            INSERT INTO outbox(body, target, priority, status, attempts,
                               next_attempt_utc, created_at_utc)
            VALUES(?, ?, ?, ?, 0, ?, ?)
            """,
            (body, destination, priority,
             "held" if held else "pending", now, now),
        )


def deliver_telegram(body: str) -> bool:
    """ערוץ גיבוי — נכנס לפעולה רק כשוואטסאפ נכשל שוב ושוב."""
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return False
    try:
        response = HTTP.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": body[:4000]},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        _runtime_status["backup_channel_used"] = utc_now_iso()
        return True
    except requests.RequestException as exc:
        LOGGER.error("Backup channel failed too: %s", str(exc)[:200])
        return False


def deliver(body: str, target: str | None = None) -> tuple[bool, str]:
    destination = (target or GROUP_ID or "").strip()
    if WHATSAPP_DRY_RUN:
        LOGGER.info(
            "WHATSAPP_DRY_RUN → %s | %s",
            destination[-12:] or "default",
            body[:250].replace("\n", " | "),
        )
        return True, ""

    if not all((ULTRA_ID, ULTRA_TOKEN, destination)):
        return False, "Missing ULTRA_ID / ULTRA_TOKEN / destination group"

    url = f"https://api.ultramsg.com/{ULTRA_ID}/messages/chat"
    payload = {"token": ULTRA_TOKEN, "to": destination, "body": body}
    try:
        response = HTTP.post(url, data=payload, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()

        # UltraMsg מחזיר 200 גם כשהשליחה נכשלה — טלפון מנותק, מכסה
        # שנגמרה או מזהה קבוצה שגוי. השגיאה נמצאת בגוף התשובה בלבד,
        # ובלי לבדוק אותו המערכת מדווחת על הצלחה מדומה.
        detail = ""
        try:
            data = response.json()
        except ValueError:
            data = {}
        if isinstance(data, dict):
            if data.get("error"):
                detail = str(data["error"])[:300]
            else:
                sent = str(data.get("sent", "")).lower()
                if sent and sent not in {"true", "1", "yes"}:
                    detail = str(data.get("message") or data)[:300]
        if detail:
            raise requests.RequestException(f"UltraMsg rejected the message: {detail}")

        _runtime_status["consecutive_send_failures"] = 0
        _runtime_status["last_send_error"] = None
        return True, ""
    except requests.RequestException as exc:
        failures = int(_runtime_status["consecutive_send_failures"]) + 1
        _runtime_status["consecutive_send_failures"] = failures
        if failures >= BACKUP_AFTER_FAILURES and deliver_telegram(
            f"⚠️ וואטסאפ לא זמין — נשלח בערוץ גיבוי\n\n{body}"
        ):
            return True, ""
        return False, str(exc)[:400]


def sender_loop() -> None:
    supervised("sender", _sender_body)


def _sender_body() -> None:
    """שולח הודעה אחת בכל פעם, בקצב קבוע, עם backoff על כישלון."""
    init_db()
    while True:
        try:
            row = db().execute(
                """
                SELECT id, body, attempts, target, created_at_utc FROM outbox
                WHERE status = 'pending' AND next_attempt_utc <= ?
                ORDER BY priority ASC, id ASC
                LIMIT 1
                """,
                (utc_now_iso(),),
            ).fetchone()

            if row is None:
                time.sleep(2)
                continue

            ok, error = deliver(str(row["body"]), str(row["target"] or ""))
            if ok:
                db().execute(
                    "UPDATE outbox SET status='sent', sent_at_utc=? WHERE id=?",
                    (utc_now_iso(), int(row["id"])),
                )
                _runtime_status["last_send_ok"] = utc_now_iso()
                time.sleep(SEND_MIN_INTERVAL_SECONDS)
                continue

            attempts = int(row["attempts"]) + 1
            _runtime_status["last_send_error"] = error
            age_hours = 0.0
            try:
                created = datetime.fromisoformat(str(row["created_at_utc"]))
                age_hours = (
                    datetime.now(timezone.utc) - created
                ).total_seconds() / 3600
            except (ValueError, TypeError):
                pass
            # ויתור רק כשגם מספר הניסיונות וגם הזמן חרגו
            if attempts >= SEND_MAX_ATTEMPTS and age_hours >= RETRY_WINDOW_HOURS:
                db().execute(
                    "UPDATE outbox SET status='failed', attempts=?, last_error=? "
                    "WHERE id=?",
                    (attempts, error, int(row["id"])),
                )
                LOGGER.error(
                    "Message permanently failed after %s attempts / %.1fh: %s",
                    attempts, age_hours, error,
                )
            else:
                delay = min(300, 5 * (2 ** (attempts - 1)))
                retry_at = (
                    datetime.now(timezone.utc) + timedelta(seconds=delay)
                ).isoformat()
                db().execute(
                    "UPDATE outbox SET attempts=?, next_attempt_utc=?, last_error=? "
                    "WHERE id=?",
                    (attempts, retry_at, error, int(row["id"])),
                )
                LOGGER.warning(
                    "Send attempt %s failed, retrying in %ss: %s",
                    attempts, delay, error,
                )
                time.sleep(1)
        except Exception:
            LOGGER.exception("Sender loop error")
            time.sleep(5)


def release_held_messages(now: datetime) -> None:
    """יציאה משבת/השתקה — דיג'סט אחד במקום מפולת הודעות."""
    if should_hold(5):
        return
    rows = db().execute(
        "SELECT id, body FROM outbox WHERE status = 'held' ORDER BY priority, id"
    ).fetchall()
    if not rows:
        return

    headlines = []
    for row in rows:
        first = str(row["body"]).split("\n")[0].replace("*", "")
        headlines.append(first[:110])

    db().execute("UPDATE outbox SET status = 'skipped' WHERE status = 'held'")
    summary = "\n".join(f"• {line}" for line in headlines[:60])
    extra = f"\n\n… ועוד {len(headlines) - 60}" if len(headlines) > 60 else ""
    enqueue_message(
        f"🕯️ *סיכום מה שהצטבר ({len(headlines)} התראות)*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n{summary}{extra}\n\n"
        f"⏰ {now.strftime('%d/%m/%Y %H:%M')}",
        priority=0,
    )
    LOGGER.info("Released %s held messages as one digest", len(headlines))


INSTANCE_CHECK_MINUTES = env_int("INSTANCE_CHECK_MINUTES", 3, 1, 60)
_last_instance_check: dict[str, float | None] = {"at": None}


def ultramsg_status() -> tuple[bool, str]:
    """מצב החיבור של האינסטנס מול וואטסאפ."""
    if not (ULTRA_ID and ULTRA_TOKEN):
        return False, "credentials missing"
    try:
        response = HTTP.get(
            f"https://api.ultramsg.com/{ULTRA_ID}/instance/status",
            params={"token": ULTRA_TOKEN},
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = response.json()
        account = data.get("accountStatus", data) or {}
        status = str(account.get("status", "")).lower()
        return status in {"authenticated", "connected", "online"}, (
            status or json.dumps(data, ensure_ascii=False)[:120]
        )
    except (requests.RequestException, ValueError) as exc:
        return False, f"check failed: {str(exc)[:150]}"


def check_connection(now: datetime) -> None:
    """בודק את החיבור כל כמה דקות ומתריע ברגע שהוא נופל.

    ההתראה יוצאת לטלגרם, כי בדיוק ברגע הזה הוואטסאפ אינו זמין.
    """
    if WHATSAPP_DRY_RUN or not (ULTRA_ID and ULTRA_TOKEN):
        return
    last = _last_instance_check["at"]
    if last is not None and (time.monotonic() - last) < INSTANCE_CHECK_MINUTES * 60:
        return
    _last_instance_check["at"] = time.monotonic()

    connected, detail = ultramsg_status()
    _runtime_status["whatsapp_connected"] = connected
    _runtime_status["whatsapp_status_detail"] = detail
    was_connected = get_state("whatsapp_connected") != "0"

    if not connected and was_connected:
        set_state("whatsapp_connected", "0")
        set_state("disconnected_since", utc_now_iso())
        LOGGER.error("WhatsApp instance DISCONNECTED (%s)", detail)
        deliver_telegram(
            "🔴 *וואטסאפ התנתק*\n\n"
            f"מצב האינסטנס: {detail}\n"
            f"השעה: {now.strftime('%d/%m/%Y %H:%M')}\n\n"
            "ההתראות ממשיכות להצטבר בתור ויישלחו במלואן ברגע החיבור.\n"
            "לחיבור מחדש: סריקת הברקוד בפאנל של UltraMsg."
        )
    elif connected and not was_connected:
        set_state("whatsapp_connected", "1")
        since = get_state("disconnected_since")
        downtime = ""
        if since:
            try:
                minutes = (
                    datetime.now(timezone.utc) - datetime.fromisoformat(since)
                ).total_seconds() / 60
                downtime = f" (נותק {int(minutes)} דקות)"
            except ValueError:
                pass
        LOGGER.info("WhatsApp instance reconnected%s", downtime)
        enqueue_message(
            f"🟢 *החיבור לוואטסאפ חזר*{downtime}\n"
            f"{pending_outbox_count()} הודעות ממתינות נשלחות כעת.",
            priority=0,
        )


def pending_outbox_count() -> int:
    row = db().execute(
        "SELECT COUNT(*) AS c FROM outbox WHERE status = 'pending'"
    ).fetchone()
    return int(row["c"]) if row else 0


# ---------------------------------------------------------------------------
# Alerts and summaries
# ---------------------------------------------------------------------------


def subject_labels(subjects: list[str]) -> str:
    return " + ".join(
        str(subject_entry(key).get("label", key)) for key in subjects
    ) or "פוליטי"


TONE_BADGE = {"עוין": "🔴", "אוהד": "🟢", "נייטרלי": "⚪"}


def push_threshold() -> int:
    raw = get_state("push_threshold")
    if raw and raw.isdigit():
        return max(0, min(100, int(raw)))
    return PUSH_THRESHOLD_DEFAULT


def source_weight(source_key: str) -> float:
    return float(SOURCE_WEIGHTS.get(source_key, DEFAULT_SOURCE_WEIGHT))


def significance_score(
    kind: str,
    subjects: list[str],
    is_statement: bool,
    source_key: str,
    tone: dict[str, Any] | None,
    source_count: int,
    is_primary: bool,
) -> int:
    """ציון 0-100. מכפיל את המהות במשקל המקור."""
    base = 20
    if kind == "poll":
        base += 30
    if is_statement:
        base += 15
    if is_primary:
        base += 45
    if "netanyahu" in subjects:
        base += 10
    if len(subjects) > 1:
        base += 5
    if tone:
        if tone.get("tone") == "עוין":
            base += 25
        elif tone.get("tone") == "אוהד":
            base += 5
        if tone.get("response_needed"):
            base += 35
    if source_count >= 5:
        base += 25
    elif source_count >= 3:
        base += 12

    return int(max(0, min(100, round(base * source_weight(source_key)))))


def score_badge(score: int) -> str:
    if score >= 75:
        return "🔺"
    if score >= 55:
        return "🔸"
    return "▫️"


def alert_targets(subjects: list[str]) -> list[str]:
    """לאילו קבוצות ההתראה נשלחת.

    אזכור של שרה נתניהו מנותב לקבוצה הייעודית. אם SARA_ALSO_MAIN
    מופעל, הוא יישלח גם לקבוצה הראשית.
    """
    if "sara" in subjects and SARA_GROUP_ID:
        targets = [SARA_GROUP_ID]
        if SARA_ALSO_MAIN and GROUP_ID and GROUP_ID != SARA_GROUP_ID:
            targets.append(GROUP_ID)
        return targets
    return [GROUP_ID]


def alert_priority(kind: str, is_statement: bool, tone: dict[str, Any] | None) -> int:
    if tone and tone.get("response_needed"):
        return 0
    if kind == "poll":
        return 1
    if tone and tone.get("tone") == "עוין":
        return 1
    return 2 if is_statement else 4


def format_alert(display: str, text: str, link: str, kind: str,
                 subjects: list[str], is_statement: bool,
                 now: datetime, tone: dict[str, Any] | None = None,
                 score: int | None = None) -> str:
    if kind == "poll":
        who = subject_labels(subjects)
        headline = f"📊 *סקר פוליטי{f' — {who}' if who else ''} | {display}*"
    elif display.startswith("𝕏"):
        headline = f"𝕏 *ציוץ חדש | {display}*"
    elif display.startswith("📢"):
        headline = f"📢 *פרסום מהערוץ הרשמי | {display}*"
    else:
        emoji = "💬" if is_statement else str(
            subject_entry(kind).get("emoji", "🔔")
        )
        prefix = "התבטאות" if is_statement else "אזכור"
        headline = f"{emoji} *{prefix}: {subject_labels(subjects)} | {display}*"

    tone_line = ""
    if tone:
        badge = TONE_BADGE.get(str(tone.get("tone")), "⚪")
        angle = str(tone.get("angle") or "").strip()
        tone_line = f"\n{badge} {tone.get('tone')}" + (f" — {angle}" if angle else "")
        if tone.get("response_needed"):
            tone_line += "\n⚠️ *מחייב תגובה*"

    if score is not None:
        headline = f"{score_badge(score)} {headline}"

    link_line = f"\n\n🔗 {link}" if link else ""
    return (
        f"{headline}{tone_line}\n\n"
        f"💬 {text[:900]}"
        f"{link_line}\n"
        f"⏰ {now.strftime('%d/%m/%Y %H:%M:%S')}"
    )


def format_escalation(cluster_id: int, sample: str, now: datetime,
                      urgent: bool) -> str:
    sources = cluster_sources(cluster_id)
    age = cluster_age_minutes(cluster_id)
    row = db().execute(
        "SELECT created_at_utc, tone, angle FROM clusters WHERE id = ?",
        (cluster_id,),
    ).fetchone()

    first_line = ""
    if sources and row:
        first_time = datetime.fromisoformat(
            str(row["created_at_utc"])
        ).astimezone(ISRAEL_TZ)
        first_line = (
            f"\n🥇 פורסם ראשון: {sources[0]} ב-{first_time.strftime('%H:%M')}"
        )

    header = (
        f"🔥🔥 *מתפוצץ — {len(sources)} מקורות ב-{int(age)} דקות*"
        if urgent
        else f"🔥 *הידיעה מתפשטת — {len(sources)} מקורות*"
    )
    tone_line = ""
    if row and row["tone"]:
        badge = TONE_BADGE.get(str(row["tone"]), "⚪")
        tone_line = f"\n{badge} {row['tone']}"
        if row["angle"]:
            tone_line += f" — {row['angle']}"

    return (
        f"{header}{tone_line}{first_line}\n\n"
        f"💬 {sample[:400]}\n\n"
        f"📡 {', '.join(sources[:12])}\n"
        f"⏰ {now.strftime('%d/%m/%Y %H:%M')}"
    )


def record_source_health(items: list[tuple[str, str, str, dict[str, str]]]) -> None:
    """כל מקור שהחזיר פריט מסמן חיים — כדי לזהות מקור שנשתק."""
    seen: dict[str, str] = {}
    for _source_type, key, display, _item in items:
        seen[key] = display
    now = utc_now_iso()
    connection = db()
    for key, display in seen.items():
        connection.execute(
            """
            INSERT INTO source_health(source_key, display, last_item_utc, items_seen)
            VALUES(?, ?, ?, 1)
            ON CONFLICT(source_key) DO UPDATE SET
                last_item_utc = excluded.last_item_utc,
                display = excluded.display,
                items_seen = source_health.items_seen + 1
            """,
            (key, display, now),
        )


def record_headlines(
    items: list[tuple[str, str, str, dict[str, str]]],
    relevant_keys: set[str],
) -> int:
    """שומר את כל הזרם, לא רק את מה שרלוונטי למטרות.

    בלי זה אי אפשר לענות על 'מה בכותרות' או 'מה הנושא הבוער' —
    המידע הזה פשוט לא קיים במערכת.
    """
    now = utc_now_iso()
    rows = []
    for _source_type, key, display, item in items:
        text = clean_text(item.get("text", ""))
        if len(text) < 20:
            continue
        link = item.get("link", "").strip()
        item_key = make_item_key(key, link, text)
        rows.append(
            (item_key, key, display, text[:400], link,
             1 if item_key in relevant_keys else 0, now)
        )
    if not rows:
        return 0
    cursor = db().executemany(
        """
        INSERT OR IGNORE INTO headlines(
            item_key, source_key, display, text, link, relevant, created_at_utc
        ) VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return int(cursor.rowcount or 0)


def group_headlines(
    rows: list[sqlite3.Row],
) -> list[dict[str, Any]]:
    """מקבץ כותרות לנושאים לפי חפיפת תוכן בין מקורות שונים."""
    topics: list[dict[str, Any]] = []
    for row in rows:
        tokens = significant_tokens(str(row["text"]))
        if len(tokens) < 3:
            continue
        placed = False
        for topic in topics:
            same, _score, _shared = is_same_story(tokens, topic["tokens"])
            if same:
                topic["sources"].add(str(row["display"]))
                topic["count"] += 1
                merged = topic["tokens"] | tokens
                topic["tokens"] = (
                    set(sorted(merged)[:CLUSTER_MAX_TOKENS])
                    if len(merged) > CLUSTER_MAX_TOKENS
                    else merged
                )
                if source_weight(str(row["source_key"])) > topic["weight"]:
                    topic["weight"] = source_weight(str(row["source_key"]))
                    topic["text"] = str(row["text"])
                    topic["link"] = str(row["link"] or "")
                placed = True
                break
        if not placed:
            topics.append(
                {
                    "tokens": tokens,
                    "text": str(row["text"]),
                    "link": str(row["link"] or ""),
                    "sources": {str(row["display"])},
                    "count": 1,
                    "weight": source_weight(str(row["source_key"])),
                    "relevant": bool(int(row["relevant"])),
                }
            )
    return topics


def silent_sources() -> list[tuple[str, float]]:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=SILENT_SOURCE_HOURS)
    rows = db().execute(
        "SELECT display, last_item_utc FROM source_health WHERE last_item_utc < ?",
        (cutoff.isoformat(),),
    ).fetchall()
    result = []
    for row in rows:
        hours = (
            datetime.now(timezone.utc)
            - datetime.fromisoformat(str(row["last_item_utc"]))
        ).total_seconds() / 3600
        result.append((str(row["display"]), round(hours, 1)))
    return sorted(result, key=lambda item: -item[1])


def is_fresh(published: str, minutes: int) -> bool:
    """האם הפריט פורסם בחלון הזמן הנתון. בלי חותמת — נחשב ישן."""
    if not published or minutes <= 0:
        return False
    try:
        stamp = datetime.fromisoformat(published.replace("Z", "+00:00"))
    except ValueError:
        return False
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - stamp).total_seconds() / 60
    return 0 <= age <= minutes


def cluster_age_minutes(cluster_id: int) -> float:
    row = db().execute(
        "SELECT created_at_utc FROM clusters WHERE id = ?", (cluster_id,)
    ).fetchone()
    if not row:
        return 999.0
    created = datetime.fromisoformat(str(row["created_at_utc"]))
    return (datetime.now(timezone.utc) - created).total_seconds() / 60


def process_scan() -> tuple[int, int]:
    now = israel_now()
    is_bootstrapped = get_state("bootstrap_complete") == "1"
    suppress_alerts = not is_bootstrapped and not SEND_EXISTING_ON_START

    items = fetch_all_sources()
    record_source_health(items)
    _runtime_status["last_scan_items"] = len(items)
    _runtime_status["last_scan_sources_ok"] = len({key for _t, key, _d, _i in items})

    # שלב 1: סינון רלוונטיות לפני כל גישה ל-DB.
    candidates: list[
        tuple[str, str, str, str, str, str, list[str], bool, bool, bool, bool]
    ] = []
    for _source_type, key, display, item in items:
        text = clean_text(item.get("text", ""))
        if not text:
            continue
        kind, subjects, is_statement = classify(text)
        forced = item.get("force_subject") or ""
        if kind is None and forced:
            # מקור ראשוני (חשבון X של הנושא עצמו) — רלוונטי בהגדרה,
            # גם אם הטקסט לא מכיל את שמו.
            kind, subjects, is_statement = forced, [forced], True
        if kind is None:
            continue
        link = item.get("link", "").strip()
        candidates.append((make_item_key(key, link, text), key, display, text,
                           link, kind, subjects, is_statement,
                           item.get("always_alert") == "1", bool(forced),
                           is_fresh(item.get("published", ""),
                                    BOOTSTRAP_FRESH_MINUTES)))

    _runtime_status["last_scan_candidates"] = len(candidates)
    _runtime_status["last_scan_headlines"] = record_headlines(
        items, {candidate[0] for candidate in candidates}
    )

    # שלב 2: בדיקת "כבר נראה" בשאילתת batch אחת.
    unseen = filter_unseen([candidate[0] for candidate in candidates])

    new_stories: list[dict[str, Any]] = []
    escalations: list[tuple[int, str, bool]] = []
    duplicates = 0
    new_per_source: Counter[str] = Counter()

    for (item_key, key, display, text, link, kind, subjects, is_statement,
         always_alert, is_primary, fresh) in candidates:
        if item_key not in unseen:
            continue
        new_per_source[key] += 1

        # בסריקת האתחול מושתק רק מה שישן. ידיעה טרייה עוברת גם כאן,
        # כך שעלייה מחדש של השירות לא יוצרת חור בכיסוי.
        if suppress_alerts and not fresh:
            mark_seen(item_key, key, link)
            continue

        cluster_id, is_new_story, source_count = find_or_create_cluster(
            text, kind, display
        )

        db().execute(
            """
            INSERT OR IGNORE INTO mentions(
                item_key, cluster_id, local_date, local_time, source_key,
                source_display, text, link, kind, is_duplicate, created_at_utc
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item_key,
                cluster_id,
                now.strftime("%Y-%m-%d"),
                now.strftime("%H:%M:%S"),
                key,
                display,
                text[:1500],
                link,
                kind,
                0 if is_new_story else 1,
                utc_now_iso(),
            ),
        )

        if is_new_story or always_alert:
            # ציוץ מהמקור עצמו נשלח גם אם התקשורת כבר דיווחה על התוכן —
            # ההודעה המקורית חשובה יותר מהדיווח עליה.
            new_stories.append(
                {
                    "cluster_id": cluster_id,
                    "display": display,
                    "text": text,
                    "link": link,
                    "kind": kind,
                    "subjects": subjects,
                    "is_statement": is_statement,
                    "primary": is_primary,
                    "always_alert": always_alert and not is_primary,
                    "source_key": key,
                }
            )
        else:
            duplicates += 1
            age = cluster_age_minutes(cluster_id)
            urgent = source_count >= VELOCITY_SOURCES and age <= VELOCITY_MINUTES
            spread = ESCALATION_THRESHOLD and source_count >= ESCALATION_THRESHOLD
            if (urgent or spread) and mark_escalated(cluster_id):
                escalations.append((cluster_id, text, bool(urgent)))

        mark_seen(item_key, key, link)

    # שלב 3: סיווג טון במקביל — רק על סיפורים חדשים, לא על כל פריט.
    if new_stories and TONE_ENABLED:
        with ThreadPoolExecutor(max_workers=min(6, len(new_stories))) as executor:
            futures = {
                executor.submit(analyze_tone, story["text"]): story
                for story in new_stories
            }
            for future in as_completed(futures):
                story = futures[future]
                try:
                    story["tone"] = future.result()
                except Exception:
                    story["tone"] = None

    # שלב 4: דירוג משמעותיות ושליחה
    threshold = push_threshold()
    pushed = 0
    suppressed = 0

    for story in new_stories:
        tone = story.get("tone")
        if tone:
            save_tone(int(story["cluster_id"]), tone)

        score = significance_score(
            str(story["kind"]), list(story["subjects"]),
            bool(story["is_statement"]), str(story["source_key"]), tone,
            1, bool(story.get("primary")),
        )
        # ערוצים שהוגדרו במפורש כ"כל אזכור" עוקפים את הסף.
        force_push = bool(story.get("always_alert")) or bool(story.get("primary"))
        will_push = force_push or score >= threshold

        db().execute(
            "UPDATE clusters SET score = ?, pushed = ? WHERE id = ?",
            (score, 1 if will_push else 0, int(story["cluster_id"])),
        )

        if not will_push:
            suppressed += 1
            continue

        pushed += 1
        alert_body = format_alert(
            str(story["display"]), str(story["text"]), str(story["link"]),
            str(story["kind"]), list(story["subjects"]),
            bool(story["is_statement"]), now, tone, score,
        )
        alert_priority_value = (
            0 if story.get("primary")
            else alert_priority(
                str(story["kind"]), bool(story["is_statement"]), tone
            )
        )
        for destination in alert_targets(list(story["subjects"])):
            enqueue_message(alert_body, priority=alert_priority_value,
                            target=destination)

    _runtime_status["last_scan_suppressed"] = suppressed

    for cluster_id, text, urgent in escalations:
        enqueue_message(
            format_escalation(cluster_id, text, now, urgent),
            priority=0 if urgent else 3,
        )

    saturated = [
        source
        for source, count in new_per_source.items()
        if count >= TELEGRAM_MESSAGES_PER_SCAN
    ]
    if saturated:
        LOGGER.warning(
            "Sources filled the whole fetch window — items may have been missed: %s. "
            "Lower SCAN_INTERVAL_SECONDS or raise TELEGRAM_MESSAGES_PER_SCAN.",
            ", ".join(saturated),
        )

    if not is_bootstrapped:
        set_state("bootstrap_complete", "1")
        LOGGER.info(
            "Initial snapshot completed. Older items suppressed; "
            "items newer than %s minutes were sent.",
            BOOTSTRAP_FRESH_MINUTES,
        )

    return pushed, duplicates


def build_digest(now: datetime) -> str:
    """דיג'סט שעתי שנשלח תמיד — גם כשהיה שקט.

    שקט שלא מדווח נראה בדיוק כמו תקלה, ולכן ההודעה יוצאת בכל שעה
    עגולה עם שורת סטטוס שמראה כמה נסרק בפועל.
    """
    since = (
        datetime.now(timezone.utc) - timedelta(hours=DIGEST_LOOKBACK_HOURS)
    ).isoformat()
    rows = db().execute(
        """
        SELECT id, sample_text, sources, source_count, kind, tone, angle,
               score, pushed, created_at_utc, updated_at_utc
        FROM clusters
        WHERE created_at_utc >= ? OR updated_at_utc >= ?
        ORDER BY score DESC, source_count DESC, id DESC
        LIMIT 30
        """,
        (since, since),
    ).fetchall()

    mention_count = db().execute(
        "SELECT COUNT(*) AS c FROM mentions WHERE created_at_utc >= ?", (since,)
    ).fetchone()["c"]

    window = (
        "השעה האחרונה"
        if DIGEST_LOOKBACK_HOURS == 1
        else f"{DIGEST_LOOKBACK_HOURS} השעות האחרונות"
    )
    lines = [
        f"🗞️ *דיג'סט {window} — {now.strftime('%H:00 | %d/%m/%Y')}*",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    if not rows:
        lines.append(
            "\nשקט. לא נמצא אף אזכור חדש של המטרות המנוטרות בשעה האחרונה."
        )
    else:
        for index, row in enumerate(rows, start=1):
            try:
                source_list = list(json.loads(str(row["sources"])))
            except (ValueError, TypeError):
                source_list = []
            kind = str(row["kind"])
            marker = "📊" if kind == "poll" else str(
                subject_entry(kind).get("emoji", "•")
            )
            badge = TONE_BADGE.get(str(row["tone"] or ""), "")
            fresh = "" if int(row["pushed"]) else " 🆕"
            angle = f" — {row['angle']}" if row["angle"] else ""
            lines.append(
                f"\n{score_badge(int(row['score']))} *{index}.* {marker}{badge}"
                f"{fresh} {clean_text(str(row['sample_text']))[:200]}{angle}"
                f"\n   📡 {row['source_count']}: {', '.join(source_list[:6])}"
            )

    lines.append(
        f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 {len(rows)} סיפורים | {mention_count} אזכורים | "
        f"{_runtime_status['last_scan_sources_ok']} מקורות פעילים | "
        f"{_runtime_status['iterations']} סריקות\n"
        f"⏰ {now.strftime('%H:%M')}"
    )
    return "\n".join(lines)


def previous_slot(now: datetime) -> datetime:
    """נקודת ההתחלה: הסיכום הקבוע הקודם. כך כל סיכום מכסה בדיוק
    את מה שקרה מאז הקודם — בלי חורים ובלי חפיפה."""
    marks: list[datetime] = [
        now.replace(hour=h, minute=m, second=0, microsecond=0)
        for h, m in DIGEST_SLOTS
    ]
    if MORNING_BRIEF_ENABLED:
        marks.append(now.replace(hour=MORNING_BRIEF_HOUR, minute=0,
                                 second=0, microsecond=0))
    if NIGHT_WRAP_ENABLED:
        marks.append(now.replace(hour=NIGHT_WRAP_HOUR, minute=0,
                                 second=0, microsecond=0))
    if PM_BRIEF_ENABLED:
        marks.extend(
            now.replace(hour=h, minute=0, second=0, microsecond=0)
            for h in BRIEF_HOURS
        )
    earlier = [mark for mark in marks if mark <= now - timedelta(minutes=6)]
    if earlier:
        return max(earlier)
    yesterday = [mark - timedelta(days=1) for mark in marks]
    return max(yesterday) if yesterday else now - timedelta(hours=4)


def build_news_roundup(now: datetime) -> str:
    """סיכום חדשות: נושאים בוערים, כותרות, ונתניהו מאז הסיכום הקודם."""
    window_start = previous_slot(now)
    since = window_start.astimezone(timezone.utc).isoformat()
    span_hours = max(1, round((now - window_start).total_seconds() / 3600))

    rows = list(
        db().execute(
            """
            SELECT source_key, display, text, link, relevant
            FROM headlines
            WHERE created_at_utc >= ?
            ORDER BY created_at_utc DESC
            LIMIT ?
            """,
            (since, ROUNDUP_MAX_ITEMS),
        ).fetchall()
    )

    lines = [
        f"📰 *סיכום חדשות — {now.strftime('%H:%M | %d/%m/%Y')}*",
        f"_מאז {window_start.strftime('%H:%M')} ({span_hours} שעות)_",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    if not rows:
        lines.append("\nלא התקבלו פריטים מהמקורות בחלון הזמן הזה.")
        lines.append(f"\n⏰ {now.strftime('%H:%M')}")
        return "\n".join(lines)

    topics = group_headlines(rows)

    # --- נושאים בוערים: מה שמספר מקורות שונים מדווחים עליו ---
    hot = sorted(
        (topic for topic in topics if len(topic["sources"]) >= 2),
        key=lambda topic: (len(topic["sources"]), topic["weight"]),
        reverse=True,
    )[:ROUNDUP_TOPICS]

    if hot:
        lines.append("\n🔥 *הנושאים הבוערים*")
        for index, topic in enumerate(hot, start=1):
            mark = " 🚨" if topic["relevant"] else ""
            lines.append(
                f"\n*{index}.*{mark} {clean_text(topic['text'])[:180]}"
                f"\n   📡 {len(topic['sources'])} מקורות: "
                f"{', '.join(sorted(topic['sources'])[:5])}"
            )

    # --- כותרות מהמקורות המרכזיים, בלי לחזור על הנושאים הבוערים ---
    hot_tokens = [topic["tokens"] for topic in hot]
    headlines = []
    for topic in sorted(topics, key=lambda t: t["weight"], reverse=True):
        if source_weight_of_topic(topic) < HEADLINE_MIN_WEIGHT:
            continue
        # סינון רחב יותר מהקיבוץ עצמו: גם חפיפה חלקית מספיקה כדי
        # לא להציג את אותו אירוע פעמיים באותה הודעה.
        if any(
            is_same_story(topic["tokens"], other)[0]
            or len(topic["tokens"] & other) >= 3
            for other in hot_tokens
        ):
            continue
        headlines.append(topic)
        if len(headlines) >= ROUNDUP_HEADLINES:
            break

    if headlines:
        lines.append("\n\n🗞️ *עוד בכותרות*")
        for topic in headlines:
            mark = "🚨 " if topic["relevant"] else ""
            source = sorted(topic["sources"])[0]
            lines.append(
                f"• {mark}{clean_text(topic['text'])[:120]} — _{source}_"
            )

    # --- נתניהו בשעה האחרונה ---
    netanyahu = db().execute(
        """
        SELECT sample_text, sources, source_count, tone, angle, kind
        FROM clusters
        WHERE (created_at_utc >= ? OR updated_at_utc >= ?)
          AND kind IN ('netanyahu', 'poll')
        ORDER BY source_count DESC, id DESC
        LIMIT 10
        """,
        (since, since),
    ).fetchall()

    lines.append(f"\n\n🚨 *נתניהו — {span_hours} השעות האחרונות*")
    # אזכורי שרה לא מופיעים כאן — הם מנותבים לקבוצה הייעודית.
    if not netanyahu:
        lines.append("\nלא נרשם אף אזכור בחלון הזמן הזה.")
    else:
        for index, row in enumerate(netanyahu, start=1):
            try:
                source_list = list(json.loads(str(row["sources"])))
            except (ValueError, TypeError):
                source_list = []
            badge = TONE_BADGE.get(str(row["tone"] or ""), "")
            marker = "📊" if str(row["kind"]) == "poll" else "•"
            angle = f" — _{row['angle']}_" if row["angle"] else ""
            lines.append(
                f"\n{marker} *{index}.* {badge}"
                f"{clean_text(str(row['sample_text']))[:180]}{angle}"
                f"\n   📡 {', '.join(source_list[:5])}"
            )

    mentions = db().execute(
        "SELECT COUNT(*) AS c FROM mentions WHERE created_at_utc >= ?", (since,)
    ).fetchone()["c"]
    lines.append(
        f"\n━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 {len(rows)} פריטים נסרקו | {len(topics)} נושאים | "
        f"{mentions} אזכורים של המטרות | "
        f"{_runtime_status['last_scan_sources_ok']} מקורות פעילים\n"
        f"⏰ {now.strftime('%H:%M')}"
    )
    return "\n".join(lines)


def source_weight_of_topic(topic: dict[str, Any]) -> float:
    return float(topic.get("weight", DEFAULT_SOURCE_WEIGHT))


def rank_clusters(since_iso: str, limit: int) -> list[sqlite3.Row]:
    """אשכולות לפי חשיבות: ציון, מספר מקורות, ואז זמן."""
    return list(
        db().execute(
            """
            SELECT id, sample_text, sources, source_count, kind, tone, angle,
                   score, pushed, created_at_utc
            FROM clusters
            WHERE created_at_utc >= ?
            ORDER BY score DESC, source_count DESC, id DESC
            LIMIT ?
            """,
            (since_iso, limit),
        ).fetchall()
    )


def render_story_line(index: int, row: sqlite3.Row, show_new: bool) -> str:
    try:
        sources = list(json.loads(str(row["sources"])))
    except (ValueError, TypeError):
        sources = []
    kind = str(row["kind"])
    marker = "\U0001F4CA" if kind == "poll" else str(
        subject_entry(kind).get("emoji", "\u2022")
    )
    badge = TONE_BADGE.get(str(row["tone"] or ""), "")
    angle = f" \u2014 _{row['angle']}_" if row["angle"] else ""
    fresh = " \U0001F195" if show_new and not int(row["pushed"]) else ""
    when = datetime.fromisoformat(
        str(row["created_at_utc"])
    ).astimezone(ISRAEL_TZ).strftime("%H:%M")
    return (
        f"\n{score_badge(int(row['score']))} *{index}.* {marker}{badge}{fresh} "
        f"[{when}] {clean_text(str(row['sample_text']))[:190]}{angle}"
        f"\n   \U0001F4E1 {row['source_count']}: {', '.join(sources[:5])}"
    )


def build_morning_brief(now: datetime) -> str:
    """סיכום הלילה. הסיכום השעתי שותק בין 23:00 ל-08:00, וזו
    ההודעה שסוגרת את הפער."""
    # תחילת הלילה היא השעה האחרונה שבה נשלח סיכום שעתי — כלומר
    # מהרגע שהדיווח השוטף השתתק. תמיד הזמן האחרון שקדם לעכשיו.
    night_start = now.replace(
        hour=NIGHT_WRAP_HOUR, minute=0, second=0, microsecond=0
    )
    if night_start >= now:
        night_start -= timedelta(days=1)
    since = night_start.astimezone(timezone.utc).isoformat()

    rows = rank_clusters(since, WRAP_MAX_STORIES)
    hours_covered = round((now - night_start).total_seconds() / 3600)

    lines = [
        "\u2600\ufe0f *\u05d1\u05d5\u05e7\u05e8 \u05d8\u05d5\u05d1 \u2014 \u05e1\u05d9\u05db\u05d5\u05dd \u05d4\u05dc\u05d9\u05dc\u05d4*",
        f"_{night_start.strftime('%H:%M')} \u05e2\u05d3 {now.strftime('%H:%M')} "
        f"({hours_covered} \u05e9\u05e2\u05d5\u05ea)_",
        "\u2501" * 20,
    ]
    if not rows:
        lines.append(
            "\n\u05dc\u05d9\u05dc\u05d4 \u05e9\u05e7\u05d8. \u05dc\u05d0 \u05e0\u05e8\u05e9\u05dd \u05d0\u05e3 \u05d0\u05d6\u05db\u05d5\u05e8 \u05d7\u05d3\u05e9 \u05e9\u05dc \u05d4\u05de\u05d8\u05e8\u05d5\u05ea."
        )
    else:
        unreported = [row for row in rows if not int(row["pushed"])]
        if unreported:
            lines.append(
                f"\n\U0001F195 *{len(unreported)} \u05e9\u05dc\u05d0 \u05d3\u05d5\u05d5\u05d7\u05d5 \u05d1\u05d6\u05de\u05df \u05d0\u05de\u05ea*"
            )
        for index, row in enumerate(rows, start=1):
            lines.append(render_story_line(index, row, show_new=True))

    mentions = db().execute(
        "SELECT COUNT(*) AS c FROM mentions WHERE created_at_utc >= ?", (since,)
    ).fetchone()["c"]
    lines.append(
        f"\n{'\u2501' * 20}\n"
        f"\U0001F4C8 {len(rows)} \u05e1\u05d9\u05e4\u05d5\u05e8\u05d9\u05dd | {mentions} \u05d0\u05d6\u05db\u05d5\u05e8\u05d9\u05dd\n"
        f"\u23f0 {now.strftime('%d/%m/%Y %H:%M')}"
    )
    return "\n".join(lines)


def build_night_wrap(now: datetime) -> str:
    """כל היום בהודעה אחת, מסודר לפי חשיבות."""
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    since = day_start.astimezone(timezone.utc).isoformat()
    rows = rank_clusters(since, WRAP_MAX_STORIES)

    lines = [
        f"\U0001F319 *\u05e1\u05d9\u05db\u05d5\u05dd \u05d4\u05d9\u05d5\u05dd \u2014 {now.strftime('%d/%m/%Y')}*",
        "_\u05de\u05e1\u05d5\u05d3\u05e8 \u05dc\u05e4\u05d9 \u05d7\u05e9\u05d9\u05d1\u05d5\u05ea_",
        "\u2501" * 20,
    ]
    if not rows:
        lines.append("\n\u05d9\u05d5\u05dd \u05e9\u05e7\u05d8. \u05dc\u05d0 \u05e0\u05e8\u05e9\u05dd \u05d0\u05e3 \u05d0\u05d6\u05db\u05d5\u05e8.")
        lines.append(f"\n\u23f0 {now.strftime('%H:%M')}")
        return "\n".join(lines)

    by_subject: Counter[str] = Counter()
    for row in rows:
        kind = str(row["kind"])
        by_subject[
            "\U0001F4CA \u05e1\u05e7\u05e8\u05d9\u05dd" if kind == "poll"
            else str(subject_entry(kind).get("label", kind))
        ] += 1

    top = [row for row in rows if int(row["score"]) >= 55]
    rest = [row for row in rows if int(row["score"]) < 55]

    if top:
        lines.append("\n\U0001F53A *\u05d4\u05db\u05d5\u05ea\u05e8\u05d5\u05ea \u05d4\u05de\u05e8\u05db\u05d6\u05d9\u05d5\u05ea*")
        for index, row in enumerate(top, start=1):
            lines.append(render_story_line(index, row, show_new=False))
    if rest:
        lines.append("\n\n\u25ab\ufe0f *\u05e2\u05d5\u05d3 \u05de\u05d4\u05d9\u05d5\u05dd*")
        for index, row in enumerate(rest, start=len(top) + 1):
            lines.append(render_story_line(index, row, show_new=False))

    lines.append("\n\n\U0001F3AF *\u05e4\u05d9\u05dc\u05d5\u05d7 \u05dc\u05e4\u05d9 \u05de\u05d8\u05e8\u05d4*")
    lines.extend(
        f"\u2022 {subject}: {count}" for subject, count in by_subject.most_common(8)
    )

    mentions = db().execute(
        "SELECT COUNT(*) AS c FROM mentions WHERE created_at_utc >= ?", (since,)
    ).fetchone()["c"]
    lines.append(
        f"\n{'\u2501' * 20}\n"
        f"\U0001F4C8 {len(rows)} \u05e1\u05d9\u05e4\u05d5\u05e8\u05d9\u05dd | {mentions} \u05d0\u05d6\u05db\u05d5\u05e8\u05d9\u05dd\n"
        f"\u05dc\u05d9\u05dc\u05d4 \u05d8\u05d5\u05d1 \U0001F319 | \u23f0 {now.strftime('%H:%M')}"
    )
    return "\n".join(lines)


PM_BRIEF_PROMPT = """אתה יועץ תקשורת בכיר בלשכת ראש הממשלה בישראל.
לפניך הידיעות המרכזיות מהשעות האחרונות.

הפק תדריך תמציתי לראש הממשלה. עבור כל ידיעה שדורשת התייחסות, נסח:
1. "headline" — הידיעה במשפט אחד, ענייני ומדויק.
2. "why" — מדוע זה חשוב לנו, במשפט אחד.
3. "recommendation" — האם להגיב ואיך: "תגובה מיידית", "תגובה בהמשך היום",
   "להימנע מתגובה" או "לעקוב בלבד". במשפט אחד, כולל הנימוק.
4. "line" — אם מומלץ להגיב: קו מסר מוצע במשפט אחד עד שניים, בגוף
   ראשון או בשם הליכוד. אם לא מומלץ להגיב — השאר ריק.

חשוב: היה כן ומקצועי. אם ידיעה עוינת ואין לה מענה טוב, אמור זאת
במפורש והמלץ להימנע. אל תמליץ להגיב על כל דבר.

בסוף הוסף "assessment" — הערכת מצב תקשורתית בשתיים עד שלוש שורות:
מה הנרטיב המרכזי שמתגבש כרגע ומה הסיכון או ההזדמנות העיקרית.

החזר JSON בלבד, בלי טקסט נוסף ובלי סימוני קוד:
{"items": [{"headline": "...", "why": "...", "recommendation": "...",
"line": "..."}], "assessment": "..."}

הידיעות:
"""


def analyze_for_brief(stories: list[str]) -> dict[str, Any] | None:
    """ניתוח אסטרטגי דרך ה-API. נכשל בשקט — התדריך יישלח בלעדיו."""
    if not (TONE_ENABLED and ANTHROPIC_API_KEY):
        return None
    numbered = "\n\n".join(
        f"{index}. {text[:600]}" for index, text in enumerate(stories, start=1)
    )
    try:
        response = HTTP.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": PM_BRIEF_MODEL,
                "max_tokens": 2000,
                "messages": [
                    {"role": "user", "content": PM_BRIEF_PROMPT + numbered}
                ],
            },
            timeout=60,
        )
        response.raise_for_status()
        blocks = response.json().get("content", [])
        raw = "".join(
            block.get("text", "") for block in blocks
            if block.get("type") == "text"
        )
        raw = re.sub(r"^```(?:json)?|```$", "", raw.strip(),
                     flags=re.MULTILINE).strip()
        return json.loads(raw)
    except Exception as exc:
        LOGGER.warning("PM brief analysis failed: %s", str(exc)[:200])
        return None


def build_pm_brief(now: datetime) -> str:
    """תדריך תמציתי לראש הממשלה עם המלצות תגובה."""
    # חלון התדריך רחב מהחלון של סיכום רגיל: תדריך הבוקר מכסה את
    # הלילה מאז תדריך הערב, ולהפך. כך שום ידיעה לא נופלת בין השניים.
    window_start = previous_slot(now)
    min_hours = env_int("PM_BRIEF_MIN_HOURS", 8, 1, 24)
    fallback = now - timedelta(hours=min_hours)
    if window_start > fallback:
        window_start = fallback
    since = window_start.astimezone(timezone.utc).isoformat()
    rows = rank_clusters(since, PM_BRIEF_ITEMS)

    period = "בוקר" if now.hour < 14 else "ערב"
    lines = [
        f"🇮🇱 *תדריך {period} — ראש הממשלה*",
        f"_{window_start.strftime('%H:%M')} — {now.strftime('%H:%M')} | "
        f"{now.strftime('%d/%m/%Y')}_",
        "━━━━━━━━━━━━━━━━━━━━",
    ]

    if not rows:
        lines.append("\nאין ידיעות מרכזיות הדורשות התייחסות בחלון הזמן הזה.")
        lines.append(f"\n⏰ {now.strftime('%H:%M')}")
        return "\n".join(lines)

    # מקבעים את הרשימה לפני הניתוח: הכותרות שחוזרות מהמודל חייבות
    # להתאים אחת לאחת לאותן שורות שמוצגות בהודעה.
    stories = [
        {
            "text": clean_text(str(row["sample_text"])),
            "sources": row["sources"],
            "source_count": row["source_count"],
            "tone": row["tone"],
        }
        for row in rows
    ]
    analysis = analyze_for_brief([story["text"] for story in stories])
    items = (analysis or {}).get("items") or []

    for index, row in enumerate(stories, start=1):
        try:
            sources = list(json.loads(str(row["sources"])))
        except (ValueError, TypeError):
            sources = []
        badge = TONE_BADGE.get(str(row["tone"] or ""), "")
        item = items[index - 1] if index <= len(items) else {}

        headline = str(item.get("headline") or "").strip()
        lines.append(
            f"\n*{index}.* {badge} "
            f"{headline or str(row['text'])[:180]}"
        )
        lines.append(f"   📡 {row['source_count']}: {', '.join(sources[:4])}")

        why = str(item.get("why") or "").strip()
        if why:
            lines.append(f"   ▪️ {why}")
        recommendation = str(item.get("recommendation") or "").strip()
        if recommendation:
            lines.append(f"   ⚖️ *המלצה:* {recommendation}")
        line = str(item.get("line") or "").strip()
        if line:
            lines.append(f"   🗣️ _\"{line}\"_")

    assessment = str((analysis or {}).get("assessment") or "").strip()
    if assessment:
        lines.append(f"\n━━━━━━━━━━━━━━━━━━━━\n🧭 *הערכת מצב*\n{assessment}")
    elif not analysis:
        lines.append(
            "\n━━━━━━━━━━━━━━━━━━━━\n"
            "_הניתוח האסטרטגי אינו זמין — חסר ANTHROPIC_API_KEY._"
        )

    lines.append(f"\n⏰ {now.strftime('%d/%m/%Y %H:%M')}")
    return "\n".join(lines)


def build_daily_summary(local_date: str, now: datetime | None = None) -> str:
    now = now or israel_now()
    mentions = mentions_for_date(local_date)
    if not mentions:
        return (
            f"📊 *סיכום יומי — ניטור פוליטי — {now.strftime('%d/%m/%Y')}*\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            "לא נמצאו היום אזכורים חדשים."
        )

    stories = [row for row in mentions if not int(row["is_duplicate"])]
    sources = Counter(row["source_display"] for row in mentions)
    by_subject: Counter[str] = Counter()
    for row in mentions:
        kind = str(row["kind"])
        if kind == "poll":
            by_subject["📊 סקרים"] += 1
        elif kind != "poll":
            by_subject[str(subject_entry(kind).get("label", kind))] += 1

    lines = [
        f"📊 *סיכום יומי — ניטור פוליטי — {now.strftime('%d/%m/%Y')}*",
        "━━━━━━━━━━━━━━━━━━━━",
        f"*{len(stories)} סיפורים | {len(mentions)} אזכורים בסך הכל*",
    ]
    if by_subject:
        lines.append("\n🎯 *פילוח לפי מטרה:*")
        lines.extend(
            f"• {subject}: {count}" for subject, count in by_subject.most_common(8)
        )
    lines.append("\n📈 *פילוח לפי מקור:*")
    lines.extend(f"• {source}: {count}" for source, count in sources.most_common(8))
    lines.append("\n📰 *דיווחים מרכזיים:*")
    for index, row in enumerate(stories[:25], start=1):
        short = clean_text(str(row["text"]))[:180]
        kind = str(row["kind"])
        tag = (
            "📊 סקר"
            if kind == "poll"
            else str(subject_entry(kind).get("label", ""))
        )
        lines.append(
            f"\n*{index}. [{str(row['local_time'])[:5]}] {tag} | "
            f"{row['source_display']}*\n{short}"
        )
    if len(stories) > 25:
        lines.append(f"\n… ועוד {len(stories) - 25} סיפורים.")
    return "\n".join(lines)


def build_heartbeat(now: datetime) -> str:
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    rows = mentions_for_date(yesterday)
    stories = sum(1 for row in rows if not int(row["is_duplicate"]))
    silent = silent_sources()
    active = db().execute("SELECT COUNT(*) AS c FROM source_health").fetchone()["c"]

    lines = [
        f"✅ *המערכת פעילה — {now.strftime('%d/%m/%Y')}*",
        f"📡 {active} מקורות פעילים מתוך "
        f"{len(TELEGRAM_CHANNELS) + len(RSS_FEEDS)}",
        f"📰 אתמול: {stories} סיפורים, {len(rows)} אזכורים",
    ]
    if silent:
        lines.append(f"\n⚠️ *מקורות שקטים מעל {SILENT_SOURCE_HOURS} שעות:*")
        lines.extend(f"• {display} — {hours} שעות" for display, hours in silent[:10])
    return "\n".join(lines)


def check_silent_sources(now: datetime) -> None:
    silent = silent_sources()
    if not silent:
        return
    today = now.strftime("%Y-%m-%d")
    signature = f"{today}|{len(silent)}"
    if get_state("last_silent_alert") == signature:
        return
    set_state("last_silent_alert", signature)
    lines = "\n".join(f"• {display} — {hours} שעות" for display, hours in silent[:12])
    enqueue_message(
        f"⚠️ *מקורות שנשתקו*\n\nלא התקבל שום פריט מ:\n{lines}\n\n"
        "ייתכן ששם הערוץ השתנה או שהפיד נפל.",
        priority=6,
    )


def send_scheduled_summaries() -> None:
    now = israel_now()
    today = now.strftime("%Y-%m-%d")

    check_connection(now)
    release_held_messages(now)
    _runtime_status["quiet_period"] = in_quiet_period(now)

    if (
        PM_BRIEF_ENABLED
        and now.hour in BRIEF_HOURS
        and now.minute <= 5
        and get_state("last_pm_brief") != f"{today}-{now.hour:02d}"
    ):
        enqueue_message(build_pm_brief(now), priority=1)
        set_state("last_pm_brief", f"{today}-{now.hour:02d}")

    if (
        MORNING_BRIEF_ENABLED
        and now.hour == MORNING_BRIEF_HOUR
        and now.minute <= 5
        and get_state("last_morning_brief") != today
    ):
        enqueue_message(build_morning_brief(now), priority=1)
        set_state("last_morning_brief", today)

    if (
        NIGHT_WRAP_ENABLED
        and now.hour == NIGHT_WRAP_HOUR
        and now.minute <= 5
        and get_state("last_night_wrap") != today
    ):
        enqueue_message(build_night_wrap(now), priority=1)
        set_state("last_night_wrap", today)

    if (
        HEARTBEAT_ENABLED
        and now.hour == HEARTBEAT_HOUR
        and now.minute <= 5
        and get_state("last_heartbeat") != today
    ):
        enqueue_message(build_heartbeat(now), priority=7)
        set_state("last_heartbeat", today)

    if now.minute <= 5:
        check_silent_sources(now)

    for slot_hour, slot_minute in DIGEST_SLOTS:
        if now.hour == slot_hour and slot_minute <= now.minute <= slot_minute + 5:
            slot_key = f"{today}-{slot_hour:02d}{slot_minute:02d}"
            if get_state("last_digest") != slot_key:
                enqueue_message(build_news_roundup(now), priority=6)
                set_state("last_digest", slot_key)
            break

    if (
        DAILY_SUMMARY_ENABLED
        and now.hour == DAILY_SUMMARY_HOUR
        and now.minute <= 5
        and get_state("last_daily_summary") != today
    ):
        enqueue_message(build_daily_summary(today, now), priority=8)
        set_state("last_daily_summary", today)


# ---------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------


def supervised(name: str, body: Any) -> None:
    """מריץ לולאה ומתאושש מכשלים במקום למות ולהשאיר את השירות חצי-חי."""
    backoff = 5
    while True:
        try:
            body()
            return
        except Exception as exc:
            _runtime_status["thread_error"] = f"{name}: {exc}"[:400]
            LOGGER.exception("%s thread failed, restarting in %ss", name, backoff)
            time.sleep(backoff)
            backoff = min(120, backoff * 2)


def monitor_loop() -> None:
    supervised("monitor", _monitor_body)


def _monitor_body() -> None:
    init_db()
    _runtime_status["started_at"] = utc_now_iso()
    validate_rss_feeds()

    if SEND_STARTUP_MESSAGE:
        enqueue_message(
            "🚀 *בוט התקשורת עלה*\n\n"
            f"📡 {len(TELEGRAM_CHANNELS)} ערוצי טלגרם + "
            f"{len(RSS_FEEDS)} פידי חדשות\n"
            "🎯 מטרות: "
            + ", ".join(str(e["label"]) for e in WATCHLIST.values())
            + "\n📊 כל סקר פוליטי בישראל\n"
            "🧩 דדופ בין מקורות פעיל",
            priority=9,
        )

    last_cleanup = time.monotonic()
    while True:
        cycle_started = time.monotonic()
        _runtime_status["iterations"] = int(_runtime_status["iterations"]) + 1
        if _runtime_status["iterations"] == 1:
            LOGGER.info("Engine running — first scan starting")
        _runtime_status["last_scan_started"] = utc_now_iso()
        try:
            new_stories, duplicates = process_scan()
            send_scheduled_summaries()
            _runtime_status["last_scan_new_stories"] = new_stories
            _runtime_status["last_scan_duplicates"] = duplicates
            _runtime_status["last_error"] = None
            _runtime_status["consecutive_errors"] = 0
            _runtime_status["thread_error"] = None
            LOGGER.info(
                "Scan done | stories=%s | duplicates suppressed=%s | queue=%s",
                new_stories,
                duplicates,
                pending_outbox_count(),
            )
        except Exception as exc:
            _runtime_status["last_error"] = str(exc)[:500]
            _runtime_status["consecutive_errors"] = (
                int(_runtime_status["consecutive_errors"]) + 1
            )
            LOGGER.exception("Monitor cycle failed")
        finally:
            _runtime_status["last_scan_finished"] = utc_now_iso()
            _runtime_status["last_scan_seconds"] = round(
                time.monotonic() - cycle_started, 2
            )

        if time.monotonic() - last_cleanup >= 86400:
            try:
                cleanup_database()
            except Exception:
                LOGGER.exception("Database cleanup failed")
            last_cleanup = time.monotonic()

        elapsed = time.monotonic() - cycle_started
        time.sleep(max(5, SCAN_INTERVAL_SECONDS - elapsed))


def start_threads() -> None:
    """מופעל מהבקשה הראשונה בלבד.

    תהליך האב של gunicorn לא מקבל בקשות, ולכן זו כשלעצמה ההגנה מפני
    הפעלה כפולה. נעילת קובץ נוספת רק יצרה מצב שבו התהליך היחיד יכול
    לוותר על ההפעלה בשקט.
    """
    global _monitor_thread, _sender_thread
    with _threads_lock:
        _runtime_status["role"] = "engine"
        if not (_monitor_thread and _monitor_thread.is_alive()):
            _monitor_thread = threading.Thread(
                target=monitor_loop, name="media-monitor", daemon=True
            )
            _monitor_thread.start()
            LOGGER.info("Monitor thread started")
        if not (_sender_thread and _sender_thread.is_alive()):
            _sender_thread = threading.Thread(
                target=sender_loop, name="whatsapp-sender", daemon=True
            )
            _sender_thread.start()
            LOGGER.info("Sender thread started")

        global _watchdog_thread
        if not (_watchdog_thread and _watchdog_thread.is_alive()):
            _watchdog_thread = threading.Thread(
                target=watchdog, name="watchdog", daemon=True
            )
            _watchdog_thread.start()


@app.before_request
def ensure_threads() -> None:
    start_threads()


def watchdog() -> None:
    """מוודא שהמנוע עלה גם אם לא הגיעה בקשת HTTP, ומרים אותו אם נפל."""
    while True:
        time.sleep(60)
        try:
            alive = bool(_monitor_thread and _monitor_thread.is_alive())
            if not alive:
                LOGGER.error("Monitor thread is not running — restarting it")
                start_threads()
        except Exception:
            LOGGER.exception("Watchdog error")


# ה-watchdog מופעל מתוך start_threads בלבד — כלומר רק בתהליך
# שמקבל בקשות HTTP. הפעלה ברמת ה-import הייתה מריצה אותו גם
# בתהליך האב של gunicorn, והוא היה מקים שם מנוע שני שסורק ושולח
# הכול פעם נוספת.


@app.get("/")
def index() -> str:
    return """
    <!doctype html>
    <html lang="he" dir="rtl">
      <meta charset="utf-8">
      <title>מערכת ניטור תקשורת</title>
      <body style="font-family:Arial;max-width:760px;margin:60px auto;line-height:1.6">
        <h1>מערכת ניטור התקשורת פעילה</h1>
        <p>המערכת מנטרת מקורות טקסטואליים עבור אזכורים של נתניהו והליכוד.</p>
        <p><a href="/health">בדיקת מצב</a></p>
      </body>
    </html>
    """


# ---------------------------------------------------------------------------
# פקודות מתוך קבוצת הוואטסאפ
# ---------------------------------------------------------------------------

HELP_TEXT = """🤖 *פקודות זמינות*

• *סטטוס* — מצב המערכת
• *השתק* / *השתק 3* — שקט לשעה או למספר שעות
• *המשך* — ביטול ההשתקה
• *מטרות* — רשימת המטרות המנוטרות
• *מטרה+ סמוטריץ׳* — הוספת מטרה
• *מטרה- סמוטריץ׳* — הסרת מטרה
• *תדריך* — תדריך ראש הממשלה עם המלצות
• *בוקר* — סיכום הלילה
• *לילה* — סיכום היום לפי חשיבות
• *חדשות* — סיכום החדשות של השעה האחרונה
• *ויראלי* — הסרטונים הכי נצפים
• *דיגסט* — אזכורי המטרות בלבד
• *בדוק <טקסט>* — למה ידיעה מסוימת לא נשלחה
• *בדיקת שרה* — הודעת בדיקה לקבוצת שרה
• *השלמה 24* — השלמת אזכורי שרה מהשעות האחרונות
• *סף* / *סף 60* — סף הדחיפה להתראה מיידית
• *היום* — סיכום היום עכשיו
• *עזרה* — ההודעה הזו"""


def slugify(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
    return f"custom_{digest}"


def handle_command(body: str) -> str | None:
    text = body.strip()
    lowered = text.lower()

    if lowered in {"עזרה", "help", "פקודות"}:
        return HELP_TEXT

    if lowered.startswith("סטטוס"):
        silent = silent_sources()
        muted = muted_until()
        return (
            "📊 *מצב המערכת*\n\n"
            f"• סריקות: {_runtime_status['iterations']}\n"
            f"• פריטים בסריקה האחרונה: {_runtime_status['last_scan_items']} "
            f"מ-{_runtime_status['last_scan_sources_ok']} מקורות\n"
            f"• מתוכם רלוונטיים: {_runtime_status['last_scan_candidates']}\n"
            f"• כפילויות שנחסמו: {_runtime_status['last_scan_duplicates']}\n"
            f"• סיפורים בסריקה האחרונה: "
            f"{_runtime_status['last_scan_new_stories']}\n"
            f"• ממתין בתור: {pending_outbox_count()}\n"
            f"• סף דחיפה: {push_threshold()}\n"
            f"• הועברו לדיג'סט בסריקה האחרונה: "
            f"{_runtime_status['last_scan_suppressed']}\n"
            f"• מטרות: {len(all_subjects())}\n"
            f"• מקורות שקטים: {len(silent)}\n"
            f"• שבת/חג: {'כן' if in_quiet_period() else 'לא'}\n"
            f"• מושתק עד: {muted.astimezone(ISRAEL_TZ).strftime('%H:%M') if muted else 'לא מושתק'}"
        )

    if lowered.startswith("השתק"):
        parts = text.split()
        hours = 1.0
        if len(parts) > 1:
            try:
                hours = max(0.25, min(48.0, float(parts[1])))
            except ValueError:
                hours = 1.0
        until = datetime.now(timezone.utc) + timedelta(hours=hours)
        set_state("muted_until", until.isoformat())
        local = until.astimezone(ISRAEL_TZ).strftime("%H:%M")
        return f"🔇 מושתק עד {local}. הודעות יצטברו ויגיעו כדיג'סט."

    if lowered in {"המשך", "בטל השתקה", "הפעל"}:
        set_state("muted_until", "")
        release_held_messages(israel_now())
        return "🔔 ההשתקה בוטלה."

    if lowered.startswith("מטרות"):
        lines = [
            f"• {entry.get('label', key)}" for key, entry in all_subjects().items()
        ]
        return "🎯 *מטרות מנוטרות*\n\n" + "\n".join(lines) + "\n• 📊 כל סקר פוליטי"

    if text.startswith("מטרה+"):
        name = text[len("מטרה+"):].strip()
        if len(name) < 2:
            return "צריך שם מטרה, למשל: מטרה+ סמוטריץ׳"
        data = json.loads(get_state("custom_subjects") or "{}")
        data[slugify(name)] = {"label": name, "terms": [name]}
        set_state("custom_subjects", json.dumps(data, ensure_ascii=False))
        return f"🎯 נוספה מטרה: {name}"

    if text.startswith("מטרה-"):
        name = text[len("מטרה-"):].strip()
        data = json.loads(get_state("custom_subjects") or "{}")
        key = slugify(name)
        if key not in data:
            return f"לא נמצאה מטרה בשם {name}. מטרות הליבה לא ניתנות להסרה."
        data.pop(key)
        set_state("custom_subjects", json.dumps(data, ensure_ascii=False))
        return f"הוסרה המטרה: {name}"

    if lowered.startswith("סף"):
        parts = text.split()
        if len(parts) > 1 and parts[1].isdigit():
            value = max(0, min(100, int(parts[1])))
            set_state("push_threshold", str(value))
            return (
                f"🎚️ סף הדחיפה עודכן ל-{value}.\n"
                "נמוך יותר = יותר התראות מיידיות. גבוה יותר = יותר לדיג'סט."
            )
        return (
            f"🎚️ סף הדחיפה הנוכחי: {push_threshold()}\n"
            "לשינוי: *סף 60*"
        )

    if text.startswith("בדוק"):
        probe = text[len("בדוק"):].strip()
        if len(probe) < 10:
            return "לשימוש: *בדוק* ואחריו הטקסט שרוצים לבחון"
        kind, subjects, stmt = classify(probe)
        if kind is None:
            return "❌ הטקסט לא עובר את הפילטר — אין בו אזכור של מטרה מנוטרת."
        score = significance_score(kind, subjects, stmt, "ynet", None, 1, False)
        names = ", ".join(
            str(subject_entry(key).get("label", key)) for key in subjects
        )
        return (
            f"✅ *עובר את הפילטר*\n"
            f"• סוג: {'סקר' if kind == 'poll' else 'אזכור'}\n"
            f"• מטרות: {names}\n"
            f"• ציון: {score} (סף {push_threshold()})\n"
            f"• תוצאה: {'התראה מיידית' if score >= push_threshold() else 'דיגסט'}"
        )

    if lowered.startswith("השלמה"):
        parts = text.split()
        hours = 24
        if len(parts) > 1 and parts[-1].isdigit():
            hours = max(1, min(48, int(parts[-1])))
        result = run_backfill("sara", hours)
        return (
            f"🕓 *השלמת אזכורי שרה נתניהו*\n\n"
            f"• נסרקו {result['items_scanned']} פריטים מ-{hours} שעות אחורה\n"
            f"• נשלחו {result['matches_sent']} אזכורים לקבוצה הייעודית"
        )

    if lowered in {"בדיקת שרה", "בדיקה שרה"}:
        if not SARA_GROUP_ID:
            return "❌ SARA_GROUP_ID לא מוגדר בדשבורד."
        enqueue_message(
            f"✅ *בדיקת חיבור לקבוצת שרה נתניהו*\n"
            f"אם אתה רואה את זה — הניתוב עובד.\n"
            f"⏰ {israel_now().strftime('%d/%m/%Y %H:%M')}",
            priority=0,
            target=SARA_GROUP_ID,
        )
        return f"נשלחה הודעת בדיקה לקבוצה המסתיימת ב-{SARA_GROUP_ID[-10:]}"

    if lowered in {"תדריך", "תדריך ראשי", "בריף"}:
        return build_pm_brief(israel_now())

    if lowered in {"בוקר", "סיכום לילה", "מה היה בלילה"}:
        return build_morning_brief(israel_now())

    if lowered in {"לילה", "סיכום יום", "סיכום היום"}:
        return build_night_wrap(israel_now())

    if lowered in {"חדשות", "כותרות", "סיכום"}:
        return build_news_roundup(israel_now())

    if lowered in {"דיגסט", "דיג'סט", "עכשיו"}:
        return build_digest(israel_now())

    if lowered in {"היום", "סיכום"}:
        return build_daily_summary(israel_now().strftime("%Y-%m-%d"))

    return None


def relay_from_group(data: dict[str, Any], group_id: str) -> bool:
    """הודעה מקבוצת מקור עוברת באותו צינור סינון, דדופ ואשכולות."""
    text = clean_text(str(data.get("body", "")))
    if len(text) < 12:
        return False

    kind, subjects, is_statement = classify(text)
    if kind is None:
        return False

    # סינון נוסף: מהקבוצה מועבר רק מה שנוגע למטרות שבהיקף ההעברה.
    # קבוצה סטה מלאה תציף — ולכן ברירת המחדל היא נתניהו בלבד.
    if RELAY_SUBJECTS and not (set(subjects) & RELAY_SUBJECTS):
        return False

    label = RELAY_GROUPS.get(group_id, "קבוצת וואטסאפ")
    display = f"📲 {label}"
    sender = str(data.get("pushname") or data.get("author") or "").strip()
    if RELAY_SHOW_SENDER and sender:
        display = f"📲 {label} / {sender}"

    message_id = str(data.get("id") or "")
    item_key = make_item_key(f"relay:{group_id}", message_id, text)
    if not filter_unseen([item_key]):
        return False

    cluster_id, is_new_story, source_count = find_or_create_cluster(
        text, kind, display
    )
    now = israel_now()
    db().execute(
        """
        INSERT OR IGNORE INTO mentions(
            item_key, cluster_id, local_date, local_time, source_key,
            source_display, text, link, kind, is_duplicate, created_at_utc
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item_key, cluster_id, now.strftime("%Y-%m-%d"),
            now.strftime("%H:%M:%S"), f"relay:{group_id}", display,
            text[:1500], "", kind, 0 if is_new_story else 1, utc_now_iso(),
        ),
    )

    if is_new_story:
        tone = analyze_tone(text) if TONE_ENABLED else None
        if tone:
            save_tone(cluster_id, tone)
        enqueue_message(
            format_alert(display, text, "", kind, subjects, is_statement,
                         now, tone),
            priority=alert_priority(kind, is_statement, tone),
        )
    elif (
        ESCALATION_THRESHOLD
        and source_count >= ESCALATION_THRESHOLD
        and mark_escalated(cluster_id)
    ):
        enqueue_message(format_escalation(cluster_id, text, now, False),
                        priority=3)

    mark_seen(item_key, f"relay:{group_id}", "")
    db().execute(
        """
        INSERT INTO source_health(source_key, display, last_item_utc, items_seen)
        VALUES(?, ?, ?, 1)
        ON CONFLICT(source_key) DO UPDATE SET
            last_item_utc = excluded.last_item_utc,
            items_seen = source_health.items_seen + 1
        """,
        (f"relay:{group_id}", display, utc_now_iso()),
    )
    return bool(is_new_story)


@app.get("/groups")
def list_groups():
    """מציג את הקבוצות שהמספר חבר בהן, עם המזהה והשם.

    UltraMsg מחזיר את שם הקבוצה תחת שמות שדה שונים בגרסאות שונות,
    ולכן נבדקות כאן כל הווריאציות. ?raw=1 מחזיר את התשובה הגולמית.
    """
    if not (ULTRA_ID and ULTRA_TOKEN):
        return jsonify({"error": "missing UltraMsg credentials"}), 400

    def pick_id(entry: dict[str, Any]) -> str:
        for key in ("id", "_id", "jid", "chatId", "chat_id", "groupId"):
            value = entry.get(key)
            if isinstance(value, dict):
                value = value.get("_serialized") or value.get("user")
            if value:
                return str(value)
        return ""

    def pick_name(entry: dict[str, Any]) -> str:
        for key in (
            "name", "subject", "title", "formattedTitle", "groupName",
            "contactName", "pushname",
        ):
            value = entry.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        metadata = entry.get("groupMetadata")
        if isinstance(metadata, dict):
            for key in ("subject", "name", "title"):
                value = metadata.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return ""

    def count_members(entry: dict[str, Any]) -> int | None:
        participants = entry.get("participants")
        if isinstance(participants, list):
            return len(participants)
        metadata = entry.get("groupMetadata")
        if isinstance(metadata, dict) and isinstance(
            metadata.get("participants"), list
        ):
            return len(metadata["participants"])
        return None

    collected: list[dict[str, Any]] = []
    raw_dump: dict[str, Any] = {}
    for endpoint in ("groups", "chats"):
        try:
            response = HTTP.get(
                f"https://api.ultramsg.com/{ULTRA_ID}/{endpoint}",
                params={"token": ULTRA_TOKEN},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raw_dump[endpoint] = f"error: {str(exc)[:200]}"
            continue

        raw_dump[endpoint] = payload
        entries = payload if isinstance(payload, list) else payload.get("data", [])
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            group_id = pick_id(entry)
            if not group_id.endswith("@g.us"):
                continue
            if any(item["id"] == group_id for item in collected):
                continue
            collected.append(
                {
                    "id": group_id,
                    "name": pick_name(entry) or "(ללא שם)",
                    "members": count_members(entry),
                    "is_main_group": group_id == GROUP_ID,
                    "is_sara_group": bool(
                        SARA_GROUP_ID and group_id == SARA_GROUP_ID
                    ),
                }
            )

    if request.args.get("raw"):
        return jsonify(raw_dump)
    if not collected:
        return jsonify(
            {
                "groups": [],
                "hint": "לא נמצאו קבוצות. נסה ?raw=1 כדי לראות את התשובה "
                        "הגולמית של UltraMsg, או ודא שהאינסטנס מחובר.",
            }
        )
    return jsonify({"count": len(collected), "groups": collected})


@app.post("/webhook/ultramsg")
def ultramsg_webhook():
    if not COMMANDS_ENABLED:
        return jsonify({"ok": False, "reason": "commands disabled"}), 403
    try:
        payload = request.get_json(force=True, silent=True) or {}
        data = payload.get("data", {}) or {}
        if payload.get("event_type") != "message_received" or data.get("fromMe"):
            return jsonify({"ok": True, "ignored": True})
        chat = str(data.get("from", ""))

        # קבוצת מקור — מסננים ומעבירים אלינו
        if RELAY_ENABLED and chat in RELAY_GROUPS:
            forwarded = relay_from_group(data, chat)
            return jsonify({"ok": True, "relayed": forwarded})

        # הקבוצה שלנו — פקודות בלבד
        if GROUP_ID and chat != GROUP_ID:
            return jsonify({"ok": True, "ignored": "other chat"})

        reply = handle_command(str(data.get("body", "")))
        if reply:
            enqueue_message(reply, priority=0)
        return jsonify({"ok": True, "handled": bool(reply)})
    except Exception as exc:
        LOGGER.exception("Webhook error")
        return jsonify({"ok": False, "error": str(exc)[:200]}), 500


@app.get("/test")
def test_text():
    """למה ידיעה מסוימת לא הגיעה — מריץ טקסט דרך כל שלבי ההחלטה."""
    init_db()
    text = clean_text(request.args.get("text") or "")
    if not text:
        return jsonify({"error": "missing text parameter"}), 400

    kind, subjects, is_statement = classify(text)
    result: dict[str, Any] = {
        "text_length": len(text),
        "passes_filter": kind is not None,
        "kind": kind,
        "subjects": [subject_entry(key).get("label", key) for key in subjects],
        "is_statement": is_statement,
    }
    if kind is None:
        result["verdict"] = "נדחה בפילטר — אין אזכור של מטרה מנוטרת"
        return jsonify(result)

    score = significance_score(kind, subjects, is_statement, "ynet", None, 1, False)
    threshold = push_threshold()
    result["score"] = score
    result["push_threshold"] = threshold
    result["would_push"] = score >= threshold

    # האם הדדופ היה חוסם אותה
    tokens = significant_tokens(text)
    window_start = (
        datetime.now(timezone.utc) - timedelta(minutes=CLUSTER_WINDOW_MINUTES)
    ).isoformat()
    rows = db().execute(
        "SELECT id, tokens, sample_text FROM clusters "
        "WHERE updated_at_utc >= ? AND kind = ? ORDER BY id DESC LIMIT 400",
        (window_start, kind),
    ).fetchall()
    match = None
    for row in rows:
        same, sc, shared = is_same_story(tokens, set(str(row["tokens"]).split()))
        if same:
            match = {
                "cluster_id": int(row["id"]),
                "similarity": round(sc, 2),
                "shared_tokens": shared,
                "existing": str(row["sample_text"])[:120],
            }
            break
    result["duplicate_of"] = match
    result["verdict"] = (
        "היה נחסם ככפילות של סיפור קיים"
        if match
        else ("היה נשלח כהתראה" if score >= threshold else "היה עובר לדיג'סט")
    )
    return jsonify(result)


def run_backfill(subject: str, hours: int, limit: int = 40) -> dict[str, Any]:
    """משלים אזכורים מהמאגר המקומי לקבוצה הייעודית.

    טבלת הכותרות שומרת 48 שעות של כל הזרם, כולל פריטים שכבר נשלחו
    לקבוצה הראשית או שלא נשלחו כלל. כאן הם נסרקים מחדש ומופנים
    ליעד הנכון, עם סימון שמונע השלמה כפולה.
    """
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = db().execute(
        """
        SELECT source_key, display, text, link, created_at_utc
        FROM headlines WHERE created_at_utc >= ?
        ORDER BY created_at_utc ASC
        """,
        (since,),
    ).fetchall()

    targets = alert_targets([subject])
    scanned = 0
    matched: list[dict[str, str]] = []
    seen_tokens: list[set[str]] = []

    for row in rows:
        scanned += 1
        text = clean_text(str(row["text"]))
        kind, subjects, is_statement = classify(text)
        if subject not in subjects:
            continue

        item_key = make_item_key(
            str(row["source_key"]), str(row["link"] or ""), text
        )
        if get_state(f"backfill:{item_key}") == "1":
            continue

        # דדופ בתוך ההשלמה עצמה
        tokens = significant_tokens(text)
        if any(is_same_story(tokens, other)[0] for other in seen_tokens):
            set_state(f"backfill:{item_key}", "1")
            continue
        seen_tokens.append(tokens)

        matched.append(
            {
                "text": text,
                "link": str(row["link"] or ""),
                "display": str(row["display"]),
                "item_key": item_key,
                "when": str(row["created_at_utc"]),
                "kind": kind or subject,
                "subjects": subjects,
                "is_statement": is_statement,
            }
        )
        if len(matched) >= limit:
            break

    now = israel_now()
    for item in matched:
        when = datetime.fromisoformat(str(item["when"])).astimezone(ISRAEL_TZ)
        body = format_alert(
            str(item["display"]), str(item["text"]), str(item["link"]),
            str(item["kind"]), list(item["subjects"]),
            bool(item["is_statement"]), when,
        )
        for destination in targets:
            enqueue_message(f"🕓 *השלמה*\n{body}", priority=6,
                            target=destination)
        set_state(f"backfill:{item['item_key']}", "1")

    return {
        "subject": subject,
        "hours": hours,
        "items_scanned": scanned,
        "matches_sent": len(matched),
        "sent_to": [t[-14:] for t in targets],
        "preview": [item["text"][:90] for item in matched],
    }


@app.get("/backfill")
def backfill():
    """השלמת אזכורים שפורסמו לפני שהניתוב הוגדר.
    דוגמה: /backfill?subject=sara&hours=24"""
    init_db()
    subject = (request.args.get("subject") or "sara").strip().lower()
    if subject not in all_subjects():
        return jsonify(
            {"error": f"מטרה לא מוכרת: {subject}",
             "available": sorted(all_subjects())}
        ), 400
    hours = max(1, min(48, int(request.args.get("hours") or 24)))
    return jsonify(run_backfill(subject, hours))


CANDIDATE_FEEDS: dict[str, str] = {
    # חדשות 12 / מאקו
    "מאקו — חדשות": "https://rcs.mako.co.il/rss/news-military.xml",
    "מאקו — פוליטי": "https://rcs.mako.co.il/rss/news-politics.xml",
    "מאקו — ראשי": "https://www.mako.co.il/RSS/rss.xml",
    # הארץ
    "הארץ — חדשות": "https://www.haaretz.co.il/srv/htz---all-articles",
    "הארץ — פוליטי": "https://www.haaretz.co.il/cmlink/1.1617539",
    "TheMarker": "https://www.themarker.com/srv/tm---all-articles",
    # כאן
    "כאן — חדשות": "https://www.kan.org.il/rss/news.xml",
    "כאן 11": "https://www.kan.org.il/api/rss/news",
    # חדשות 13
    "חדשות 13": "https://13tv.co.il/feed/",
    "חדשות 13 — news": "https://13news.co.il/feed/",
    # ישראל היום ומעריב — נפלו ב-403, בדיקה חוזרת
    "ישראל היום": "https://www.israelhayom.co.il/rss.xml",
    "ישראל היום — פיד": "https://www.israelhayom.co.il/feed",
    "מעריב": "https://www.maariv.co.il/Rss/RssFeedsPolitiMedini",
    "מעריב — ראשי": "https://www.maariv.co.il/Rss/RssFeedsMivzakiChadashot",
    # כלכלה
    "כלכליסט": "https://www.calcalist.co.il/GeneralRSS/0,16335,L-3674,00.xml",
    "ביזפורטל": "https://www.bizportal.co.il/rss",
    # חרדים
    "כיכר השבת": "https://www.kikar.co.il/rss.xml",
    "כיכר — feed": "https://www.kikar.co.il/feed",
    "בחדרי חרדים": "https://www.bhol.co.il/rss/rss.aspx",
    # סיקור זר
    "Times of Israel": "https://www.timesofisrael.com/feed/",
    "i24 עברית": "https://www.i24news.tv/he/rss",
    "Reuters ME": "https://news.google.com/rss/search?q=Netanyahu&hl=en-US&gl=US&ceid=US:en",
    # נוספים
    "וואלה — פוליטי": "https://rss.walla.co.il/feed/2686",
    "Ynet — פוליטי": "https://www.ynet.co.il/Integration/StoryRss1854.xml",
    "זמן ישראל": "https://www.zman.co.il/feed/",
    "ערוץ 20": "https://www.now14.co.il/feed/",
}


@app.get("/testfeed")
def test_feed():
    """בודק פידים מהשרת עצמו. בלי פרמטרים — בודק את כל המועמדים.
    עם ?url=... — בודק כתובת בודדת."""
    single = (request.args.get("url") or "").strip()
    targets = (
        {"custom": single}
        if single
        else dict(CANDIDATE_FEEDS)
    )
    if request.args.get("current"):
        targets = dict(active_feeds())

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(_probe_feed, name, url): name
            for name, url in targets.items()
        }
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                results.append(
                    {"name": futures[future], "ok": False,
                     "error": str(exc)[:150]}
                )

    working = [r for r in results if r["ok"]]
    return jsonify(
        {
            "tested": len(results),
            "working": len(working),
            "results": sorted(results, key=lambda r: (not r["ok"], r["name"])),
            "ready_to_add": {r["name"]: r["url"] for r in working},
        }
    )


def _probe_feed(name: str, url: str) -> dict[str, Any]:
    started = time.monotonic()
    try:
        response = HTTP.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        status = response.status_code
        response.raise_for_status()
        feed = feedparser.parse(response.content)
        entries = list(feed.entries)
        sample = ""
        relevant = 0
        for entry in entries[:15]:
            text = clean_text(
                f"{entry.get('title', '')} "
                f"{entry.get('summary', entry.get('description', ''))}"
            )
            if not sample and text:
                sample = text[:110]
            if classify(text)[0] is not None:
                relevant += 1
        return {
            "name": name,
            "url": url,
            "ok": bool(entries),
            "http": status,
            "entries": len(entries),
            "relevant_now": relevant,
            "seconds": round(time.monotonic() - started, 1),
            "sample": sample,
        }
    except requests.RequestException as exc:
        return {
            "name": name, "url": url, "ok": False,
            "http": getattr(exc.response, "status_code", None),
            "error": str(exc)[:120],
        }
    except Exception as exc:
        return {"name": name, "url": url, "ok": False, "error": str(exc)[:120]}


@app.get("/addfeed")
def add_feed():
    """מוסיף פיד בזמן ריצה, אחרי שהוא נבדק בפועל.
    /addfeed?name=מאקו&url=https://...   |   /addfeed?remove=מאקו"""
    init_db()
    remove = (request.args.get("remove") or "").strip()
    if remove:
        extras = extra_feeds()
        if extras.pop(remove, None) is not None:
            set_state("extra_feeds", json.dumps(extras, ensure_ascii=False))
            return jsonify({"ok": True, "removed": remove,
                            "active_feeds": len(active_feeds())})
        disabled = json.loads(get_state("disabled_feeds") or "[]")
        if remove in RSS_FEEDS and remove not in disabled:
            disabled.append(remove)
            set_state("disabled_feeds", json.dumps(disabled, ensure_ascii=False))
            return jsonify({"ok": True, "disabled": remove,
                            "active_feeds": len(active_feeds())})
        return jsonify({"ok": False, "error": f"לא נמצא פיד בשם {remove}"}), 404

    name = (request.args.get("name") or "").strip()
    url = (request.args.get("url") or "").strip()
    if not name or not url:
        return jsonify(
            {"error": "צריך name ו-url",
             "example": "/addfeed?name=מאקו&url=https://..."}
        ), 400

    probe = _probe_feed(name, url)
    if not probe["ok"]:
        return jsonify({"ok": False, "reason": "הפיד לא החזיר פריטים",
                        "probe": probe}), 400

    extras = extra_feeds()
    extras[name] = url
    set_state("extra_feeds", json.dumps(extras, ensure_ascii=False))
    return jsonify(
        {"ok": True, "added": name, "entries": probe["entries"],
         "relevant_now": probe["relevant_now"],
         "active_feeds": len(active_feeds())}
    )


@app.get("/feeds")
def list_feeds():
    init_db()
    active = active_feeds()
    return jsonify(
        {
            "count": len(active),
            "built_in": list(RSS_FEEDS),
            "added_at_runtime": list(extra_feeds()),
            "disabled": json.loads(get_state("disabled_feeds") or "[]"),
            "feeds": active,
        }
    )


@app.get("/ping")
def ping_group():
    """שולח הודעת בדיקה לקבוצה, כדי לוודא שהצינור פתוח בלי לחכות
    לידיעה אמיתית. ?to=sara לקבוצת שרה, ברירת מחדל הקבוצה הראשית."""
    init_db()
    which = (request.args.get("to") or "main").strip().lower()
    if which in {"sara", "שרה"}:
        destination, label = SARA_GROUP_ID, "קבוצת שרה נתניהו"
    else:
        destination, label = GROUP_ID, "הקבוצה הראשית"

    if not destination:
        return jsonify(
            {
                "ok": False,
                "error": f"{label} לא מוגדרת",
                "hint": "SARA_GROUP_ID חסר במשתני הסביבה"
                        if which.startswith(("s", "ש")) else "GROUP_ID חסר",
            }
        ), 400

    now = israel_now()
    enqueue_message(
        f"✅ *בדיקת חיבור*\n\n"
        f"ההודעה הזו נשלחה אל {label} כדי לוודא שהצינור פתוח.\n"
        f"אם אתה רואה אותה — הניתוב עובד.\n\n"
        f"🏷️ גרסה: {BUILD}\n"
        f"⏰ {now.strftime('%d/%m/%Y %H:%M:%S')}",
        priority=0,
        target=destination,
    )
    return jsonify(
        {
            "ok": True,
            "queued_to": label,
            "target_tail": destination[-14:],
            "dry_run": WHATSAPP_DRY_RUN,
            "note": "ההודעה בתור. אמורה להגיע תוך כמה שניות."
                    if not WHATSAPP_DRY_RUN
                    else "המערכת במצב WHATSAPP_DRY_RUN — לא תישלח בפועל.",
        }
    )


@app.get("/routing")
def routing_check():
    """האם ניתוב אזכורי שרה מוגדר ועובד — תשובה מלאה במבט אחד."""
    init_db()
    sample = "שרה נתניהו הגיעה היום לטקס בבית הנשיא"
    kind, subjects, _stmt = classify(sample)
    targets = alert_targets(subjects)

    recent = db().execute(
        """
        SELECT status, target, substr(body, 1, 70) AS head, created_at_utc
        FROM outbox ORDER BY id DESC LIMIT 15
        """
    ).fetchall()

    sara_mentions = db().execute(
        "SELECT COUNT(*) AS c FROM mentions WHERE kind = 'sara'"
    ).fetchone()["c"]

    sara_recent = db().execute(
        """
        SELECT local_date, local_time, source_display, substr(text, 1, 100) AS t
        FROM mentions WHERE kind = 'sara' ORDER BY id DESC LIMIT 10
        """
    ).fetchall()

    return jsonify(
        {
            "build": BUILD,
            "sara_routing_configured": bool(SARA_GROUP_ID),
            "sara_group_tail": SARA_GROUP_ID[-14:] if SARA_GROUP_ID else None,
            "main_group_tail": GROUP_ID[-14:] if GROUP_ID else None,
            "also_send_to_main": SARA_ALSO_MAIN,
            "self_test": {
                "sample_text": sample,
                "detected_kind": kind,
                "detected_subjects": subjects,
                "would_send_to": [t[-14:] for t in targets],
                "verdict": (
                    "מנותב לקבוצת שרה"
                    if SARA_GROUP_ID and SARA_GROUP_ID in targets
                    else "נשלח לקבוצה הראשית — הניתוב לא פעיל"
                ),
            },
            "sara_mentions_total": sara_mentions,
            "sara_recent": [dict(row) for row in sara_recent],
            "recent_outbox": [
                {
                    "status": str(row["status"]),
                    "target_tail": (str(row["target"])[-14:]
                                    if row["target"] else None),
                    "head": str(row["head"]).replace("\n", " "),
                }
                for row in recent
            ],
        }
    )


@app.get("/debug")
def debug_view():
    """מה המערכת קלטה בפועל — כדי שאפשר יהיה להבחין בין
    'לא היו חדשות' לבין 'משהו חוסם'."""
    init_db()
    recent = db().execute(
        """
        SELECT local_time, source_display, kind, is_duplicate,
               substr(text, 1, 160) AS snippet
        FROM mentions ORDER BY id DESC LIMIT 40
        """
    ).fetchall()
    clusters = db().execute(
        """
        SELECT id, source_count, score, pushed, substr(sample_text, 1, 120) AS s
        FROM clusters ORDER BY id DESC LIMIT 20
        """
    ).fetchall()
    silent = silent_sources()
    return jsonify(
        {
            "scan": {
                "iterations": _runtime_status["iterations"],
                "items_fetched": _runtime_status["last_scan_items"],
                "sources_responding": _runtime_status["last_scan_sources_ok"],
                "relevant_candidates": _runtime_status["last_scan_candidates"],
                "new_stories": _runtime_status["last_scan_new_stories"],
                "duplicates_blocked": _runtime_status["last_scan_duplicates"],
                "below_threshold": _runtime_status["last_scan_suppressed"],
                "push_threshold": push_threshold(),
            },
            "outbox": {
                "pending": pending_outbox_count(),
                "sent_today": db().execute(
                    "SELECT COUNT(*) AS c FROM outbox WHERE status='sent' "
                    "AND created_at_utc >= ?",
                    ((datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),),
                ).fetchone()["c"],
                "last_send_ok": _runtime_status["last_send_ok"],
                "last_send_error": _runtime_status["last_send_error"],
                "consecutive_failures": _runtime_status["consecutive_send_failures"],
                "dry_run": WHATSAPP_DRY_RUN,
                "whatsapp_connected": _runtime_status["whatsapp_connected"],
                "whatsapp_status": _runtime_status["whatsapp_status_detail"],
                "oldest_pending_minutes": (lambda r: round(
                    (datetime.now(timezone.utc)
                     - datetime.fromisoformat(str(r["c"]))).total_seconds() / 60
                ) if r and r["c"] else None)(db().execute(
                    "SELECT MIN(created_at_utc) AS c FROM outbox "
                    "WHERE status='pending'").fetchone()),
                "credentials_present": bool(ULTRA_ID and ULTRA_TOKEN and GROUP_ID),
                "group_id_tail": GROUP_ID[-12:] if GROUP_ID else None,
                "sara_group_tail": (
                    SARA_GROUP_ID[-12:] if SARA_GROUP_ID else None
                ),
                "failed_recently": db().execute(
                    "SELECT COUNT(*) AS c FROM outbox WHERE status='failed'"
                ).fetchone()["c"],
            },
            "storage": {
                "database": str(DB_PATH),
                "persistent_disk": str(DB_PATH).startswith("/var/data"),
                "bootstrap_complete": get_state("bootstrap_complete") == "1",
                "seen_items": db().execute(
                    "SELECT COUNT(*) AS c FROM seen_items"
                ).fetchone()["c"],
                "mentions_total": db().execute(
                    "SELECT COUNT(*) AS c FROM mentions"
                ).fetchone()["c"],
            },
            "silent_sources": silent,
            "recent_clusters": [dict(row) for row in clusters],
            "recent_mentions": [dict(row) for row in recent],
        }
    )


@app.get("/search")
def search():
    init_db()
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"error": "missing q"}), 400
    rows = db().execute(
        """
        SELECT local_date, local_time, source_display, kind, text, link
        FROM mentions
        WHERE text LIKE ?
        ORDER BY id DESC LIMIT 100
        """,
        (f"%{query}%",),
    ).fetchall()
    return jsonify(
        {
            "query": query,
            "count": len(rows),
            "results": [dict(row) for row in rows],
        }
    )


@app.get("/report")
def report():
    init_db()
    day = request.args.get("date") or israel_now().strftime("%Y-%m-%d")
    rows = mentions_for_date(day)
    items = "".join(
        f"<tr><td>{row['local_time'][:5]}</td>"
        f"<td>{html.escape(str(row['source_display']))}</td>"
        f"<td>{html.escape(str(subject_entry(str(row['kind'])).get('label', row['kind'])))}</td>"
        f"<td>{html.escape(str(row['text'])[:300])}</td></tr>"
        for row in rows
        if not int(row["is_duplicate"])
    )
    return f"""<!doctype html><html lang="he" dir="rtl"><meta charset="utf-8">
    <title>דוח ניטור {day}</title>
    <body style="font-family:Arial;margin:40px;line-height:1.5">
    <h1>דוח ניטור — {day}</h1>
    <p>{len(rows)} אזכורים</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
    <tr><th>שעה</th><th>מקור</th><th>מטרה</th><th>תוכן</th></tr>{items}</table>
    </body></html>"""


@app.get("/health")
def health():
    try:
        return health_report()
    except Exception as exc:
        # /health לעולם לא מפיל את הדיפלוי בגלל תקלה בבדיקה עצמה.
        LOGGER.exception("Health check failed")
        return jsonify({"ok": False, "health_error": str(exc)[:300]}), 503


def health_report():
    booting = (time.monotonic() - BOOT_MONOTONIC) < HEALTH_GRACE_SECONDS
    try:
        init_db()
        db_ok, db_error = True, None
    except Exception as exc:
        db_ok, db_error = False, str(exc)[:300]

    monitor_alive = bool(_monitor_thread and _monitor_thread.is_alive())
    sender_alive = bool(_sender_thread and _sender_thread.is_alive())

    stale = False
    age_seconds: float | None = None
    finished = _runtime_status.get("last_scan_finished")
    if finished:
        age_seconds = (
            datetime.now(timezone.utc) - datetime.fromisoformat(str(finished))
        ).total_seconds()
        stale = age_seconds > max(300, SCAN_INTERVAL_SECONDS * 3)
    elif _runtime_status.get("started_at"):
        started_age = (
            datetime.now(timezone.utc)
            - datetime.fromisoformat(str(_runtime_status["started_at"]))
        ).total_seconds()
        stale = started_age > max(600, SCAN_INTERVAL_SECONDS * 5)

    failing = int(_runtime_status.get("consecutive_errors") or 0) >= 3
    healthy = (
        booting
        or (monitor_alive and sender_alive and db_ok and not stale and not failing)
    )
    payload = {
        "ok": healthy,
        "build": BUILD,
        "monitor_alive": monitor_alive,
        "sender_alive": sender_alive,
        "last_scan_age_seconds": round(age_seconds, 1) if age_seconds else None,
        "stale": stale,
        "failing": failing,
        "push_threshold": push_threshold() if db_ok else None,
        "booting": booting,
        "role": _runtime_status.get("role"),
        "db_ok": db_ok,
        "db_error": db_error,
        "whatsapp_dry_run": WHATSAPP_DRY_RUN,
        "credentials_present": bool(ULTRA_ID and ULTRA_TOKEN and GROUP_ID),
        "outbox_pending": pending_outbox_count() if db_ok else None,
        "database": str(DB_PATH),
        "sources": {
            "telegram": len(TELEGRAM_CHANNELS),
            "rss": len(active_feeds()) if db_ok else len(RSS_FEEDS),
            "relay_groups": len(RELAY_GROUPS),
            "relay_subjects": sorted(RELAY_SUBJECTS) or ["all"],
            "x_accounts": list(X_ACCOUNTS) if X_ENABLED else [],
        },
        "runtime": _runtime_status,
    }
    return jsonify(payload), (200 if healthy else 503)


# לא מפעילים ב-import: gunicorn טוען את המודול גם בתהליך האב.
# ההפעלה נעשית בבקשה הראשונה, שמגיעה רק לעובד — ובנוסף מוגנת
# בנעילת מופע יחיד למקרה של טעינה כפולה.


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
