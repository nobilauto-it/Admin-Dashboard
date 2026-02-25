import os
import re
import sys
import traceback
import threading
import time
import urllib.parse
from fastapi import Request
from zoneinfo import ZoneInfo
from datetime import datetime, timezone, time as dt_time
from typing import Any, Dict, List, Optional, Tuple
from starlette.requests import Request
from urllib.parse import parse_qs
import json
import unicodedata
import requests
import psycopg2
from psycopg2.extras import execute_values, Json
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# -----------------------------
# CONFIG
# -----------------------------
BITRIX_WEBHOOK = os.getenv(
    "BITRIX_WEBHOOK",
    "https://nobilauto.bitrix24.ru/rest/18397/h5c7kw97sfp3uote"
)

PG_HOST = os.getenv("PG_HOST", "194.33.40.197")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crm")
PG_USER = os.getenv("PG_USER", "crm")
PG_PASS = os.getenv("PG_PASS", "crm")

# Autoupdate
# По умолчанию включено (можно отключить через AUTO_SYNC_ENABLED=0)
# Когда Bitrix API разблокируют - синхронизация автоматически заработает
AUTO_SYNC_ENABLED = os.getenv("AUTO_SYNC_ENABLED", "0") == "1"
# Интервал авто-синка (по умолчанию 120 секунд)
AUTO_SYNC_INTERVAL_SEC = int(os.getenv("AUTO_SYNC_INTERVAL_SEC", "120"))

# Консервативные лимиты по умолчанию (вместо 0 = unlimited)
# Можно переопределить через переменные окружения для более агрессивной синхронизации
AUTO_SYNC_DEAL_LIMIT = int(os.getenv("AUTO_SYNC_DEAL_LIMIT", "50"))
AUTO_SYNC_SMART_LIMIT = int(os.getenv("AUTO_SYNC_SMART_LIMIT", "30"))
AUTO_SYNC_CONTACT_LIMIT = int(os.getenv("AUTO_SYNC_CONTACT_LIMIT", "30"))
AUTO_SYNC_LEAD_LIMIT = int(os.getenv("AUTO_SYNC_LEAD_LIMIT", "30"))

# Консервативное время работы синхронизации (10 секунд вместо 20)
# Helps avoid Bitrix operation time limit and API blocking
SYNC_TIME_BUDGET_SEC = int(os.getenv("SYNC_TIME_BUDGET_SEC", "10"))

# Консервативный интервал между запросами (1 секунда вместо 0.15)
# Helps avoid Bitrix rate limiting and API blocking
BITRIX_MIN_REQUEST_INTERVAL_SEC = float(os.getenv("BITRIX_MIN_REQUEST_INTERVAL_SEC", "1.0"))
BITRIX_MAX_RETRIES = int(os.getenv("BITRIX_MAX_RETRIES", "8"))
BITRIX_BACKOFF_BASE_SEC = float(os.getenv("BITRIX_BACKOFF_BASE_SEC", "0.7"))

# Ежедневная отправка 7 PDF-отчётов в 23:55 (Telegram + Bitrix)
REPORT_CRON_BASE_URL = os.getenv("REPORT_CRON_BASE_URL", "http://127.0.0.1:7070").strip().rstrip("/")
REPORT_CRON_TZ = os.getenv("REPORT_TZ", "Europe/Chisinau").strip() or "Europe/Chisinau"

# =====================================================================
# PDF HELPERS: "6 PDF по филиалам", и ВНУТРИ КАЖДОГО PDF — РАЗБИВКА ТАБЛИЦ
# =====================================================================
#
# ВАЖНО (почему я перенёс этот блок ВЫШЕ импорта api_data):
# api_data.py обычно импортирует build_branch_pdf / константы из main.py.
# Если main.py при этом импортит api_data.py — может быть circular import.
# Поэтому PDF-утилиты определены ДО строки `from api_data import router ...`,
# чтобы api_data мог безопасно импортировать их из main.py.
#
# =====================================================================

# reportlab (у тебя он уже стоит, раз pdf генеришь)
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm


# ---- ПОЛЯ (оставляю те, что ты писал; если у тебя другие — поменяй тут) ----
STOCK_F_BRANCH = "ufCrm34_1749209523"       # Filiala (iblock element id или строка)
STOCK_F_LOC = "ufCrm34_1751116796"          # Locația (label/id)
STOCK_F_WAIT_SVC = "ufCrm34_1760623438126"  # In asteptare service (bool)

STOCK_F_FROMDT = "ufCrm34_1748962248"       # Data plecării
STOCK_F_TODT = "ufCrm34_1748962285"         # Data returnării

STOCK_F_CARNO = "ufCrm34_1748431574"        # Nr Auto
STOCK_F_BRAND = "ufCrm34_1748347910"        # Marca
STOCK_F_MODEL = "ufCrm34_1748431620"        # Model


# ---- НАЗВАНИЯ ЛОКАЦИЙ ----
DEFAULT_SERVICE_LOCS = {
    "Testare dupa service",
    "Vulcanizare Studentilor",
    "Spalatoria",
}
DEFAULT_SALE_LOC = "Parcarea de Vânzare"


def _to_dt(v: Any) -> Optional[datetime]:
    if not v:
        return None
    if isinstance(v, datetime):
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v
    if isinstance(v, str):
        try:
            s = v.strip().replace("Z", "+00:00")
            d = datetime.fromisoformat(s)
            return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d
        except Exception:
            return None
    return None


def stock_classify_default(fields: Dict[str, Any], now: datetime) -> Tuple[str, Optional[str]]:
    """
    Возвращает (bucket, subkey)
      bucket: "CHIRIE" | "SERVICE" | "PARCARE" | "ALTE" | "FARA_STATUS"
      subkey: для ALTE — имя локации, чтобы делать отдельные таблицы по каждой локации
    """
    dt_from = _to_dt(fields.get(STOCK_F_FROMDT))
    dt_to = _to_dt(fields.get(STOCK_F_TODT))

    loc = fields.get(STOCK_F_LOC)
    loc_s = str(loc).strip() if loc is not None else ""

    wait_s = fields.get(STOCK_F_WAIT_SVC)
    wait_s_bool = str(wait_s).lower() in ("1", "true", "y", "yes", "да", "on")

    # 1) CHIRIE: есть даты и return >= now
    if dt_from and dt_to:
        try:
            if dt_to >= now:
                return ("CHIRIE", None)
        except Exception:
            return ("CHIRIE", None)

    # 2) SERVICE: флаг ожидания ИЛИ локация = сервисная
    if wait_s_bool or (loc_s and loc_s in DEFAULT_SERVICE_LOCS):
        return ("SERVICE", None)

    # 3) PARCARE (продажи)
    if loc_s and loc_s == DEFAULT_SALE_LOC:
        return ("PARCARE", None)

    # 4) ALTE: любое другое значение локации — отдельной таблицей по loc
    if loc_s:
        # ВАЖНО: сюда как раз попадёт "Prelungire" (если это значение Locația)
        return ("ALTE", loc_s)

    # 5) без статуса
    return ("FARA_STATUS", None)


def _make_table(title: str, rows: List[List[str]], styles, col_widths):
    out = []
    out.append(Paragraph(f"<b>{title}</b>", styles["Heading3"]))
    out.append(Spacer(1, 3 * mm))

    header = ["№ Auto", "Marca", "Model", "Locația", "De la", "Până la"]
    data = [header] + (rows if rows else [["", "", "", "", "", ""]])

    t = Table(data, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    out.append(t)
    out.append(Spacer(1, 7 * mm))
    return out


def build_branch_pdf(
    branch_name: str,
    branch_items_fields: List[Dict[str, Any]],
    pdf_path: str,
    classify_fn=stock_classify_default,
) -> str:
    """
    Делает ОДИН PDF ДЛЯ ОДНОГО ФИЛИАЛА, но внутри:
      CHIRIE
      SERVICE
      PARCARE
      ALTE — отдельные таблицы по каждой "Locația" (включая Prelungire если он там)
      FARA_STATUS
    """
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(
        pdf_path,
        pagesize=A4,
        leftMargin=12 * mm,
        rightMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm
    )

    now = datetime.now(timezone.utc)
    col_widths = [22 * mm, 28 * mm, 40 * mm, 38 * mm, 30 * mm, 30 * mm]

    buckets: Dict[str, Any] = {
        "CHIRIE": [],
        "SERVICE": [],
        "PARCARE": [],
        "ALTE": {},        # subkey (loc) -> rows
        "FARA_STATUS": [],
    }

    for f in branch_items_fields:
        bucket, subkey = classify_fn(f, now)

        car_no = f.get(STOCK_F_CARNO, "") or ""
        brand = f.get(STOCK_F_BRAND, "") or ""
        model = f.get(STOCK_F_MODEL, "") or ""
        loc = f.get(STOCK_F_LOC, "") or ""

        dt_from = _to_dt(f.get(STOCK_F_FROMDT))
        dt_to = _to_dt(f.get(STOCK_F_TODT))
        s_from = dt_from.strftime("%Y-%m-%d %H:%M") if dt_from else ""
        s_to = dt_to.strftime("%Y-%m-%d %H:%M") if dt_to else ""

        row = [str(car_no), str(brand), str(model), str(loc), s_from, s_to]

        if bucket == "ALTE":
            key = subkey or "Alt"
            buckets["ALTE"].setdefault(key, []).append(row)
        else:
            buckets[bucket].append(row)

    story = []
    story.append(Paragraph(f"<b>STOCK AUTO — {branch_name}</b>", styles["Title"]))
    story.append(Paragraph(datetime.now().strftime("%Y-%m-%d %H:%M"), styles["Normal"]))
    story.append(Spacer(1, 8 * mm))

    story += _make_table("CHIRIE", buckets["CHIRIE"], styles, col_widths)
    story += _make_table("SERVICE", buckets["SERVICE"], styles, col_widths)
    story += _make_table("PARCARE VÂNZARE", buckets["PARCARE"], styles, col_widths)

    # ALTE как много таблиц по локациям (сюда попадёт и Prelungire, если это Locația)
    for loc_name, rows in sorted(buckets["ALTE"].items(), key=lambda x: x[0]):
        story += _make_table(f"ALTE — {loc_name}", rows, styles, col_widths)

    story += _make_table("FĂRĂ STATUS", buckets["FARA_STATUS"], styles, col_widths)

    doc.build(story)
    return pdf_path


# -----------------------------
# FastAPI app + router
# -----------------------------
app = FastAPI(title="Bitrix24 Schema+Data Sync API")

# Import data API router
from api_data import router as data_router
app.include_router(data_router)

# Import processes-deals API router
from processes_deals_api import router as processes_deals_router
app.include_router(processes_deals_router)

# Import entity-meta-fields API router
from entity_meta_fields_api import router as entity_meta_fields_router
app.include_router(entity_meta_fields_router)

from entity_meta_data_api import router as entity_meta_data_router
app.include_router(entity_meta_data_router)

from Login import router as login_router
app.include_router(login_router)

# -----------------------------
# Bitrix REST client
# -----------------------------
class BitrixClient:
    def __init__(self, webhook_base: str):
        self.base = webhook_base.rstrip("/")
        self._last_call_ts = 0.0

    def _throttle(self):
        now = time.time()
        dt = now - self._last_call_ts
        if dt < BITRIX_MIN_REQUEST_INTERVAL_SEC:
            time.sleep(BITRIX_MIN_REQUEST_INTERVAL_SEC - dt)
        self._last_call_ts = time.time()

    def call(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}/{method}.json"
        payload = params or {}

        last_err = None
        for attempt in range(BITRIX_MAX_RETRIES):
            try:
                self._throttle()
                r = requests.post(url, json=payload, timeout=60)

                # Bitrix can return 429 with JSON or plain text
                if r.status_code == 429:
                    wait = BITRIX_BACKOFF_BASE_SEC * (2 ** attempt)
                    time.sleep(wait)
                    last_err = f"Bitrix HTTP 429: {r.text}"
                    continue

                # Проверяем HTTP 401 - может быть OVERLOAD_LIMIT в JSON
                if r.status_code == 401:
                    try:
                        data = r.json()
                        if "error" in data and str(data.get("error")) == "OVERLOAD_LIMIT":
                            print(f"WARNING: b24.call: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
                            return {"error": "OVERLOAD_LIMIT", "result": []}
                    except:
                        pass  # Если не JSON, продолжим как обычно
                    # Если не OVERLOAD_LIMIT, падаем с ошибкой
                    raise HTTPException(status_code=502, detail=f"Bitrix HTTP {r.status_code}: {r.text}")

                if r.status_code != 200:
                    raise HTTPException(status_code=502, detail=f"Bitrix HTTP {r.status_code}: {r.text}")

                # Всегда декодируем ответ как UTF-8, чтобы не терять румынские диакритики (ț, ă, ș)
                try:
                    text = r.content.decode("utf-8", errors="replace")
                except Exception:
                    text = r.text or "{}"
                data = json.loads(text)

                # Bitrix error in body
                if "error" in data:
                    err = str(data.get("error"))
                    # OVERLOAD_LIMIT - API заблокирован, возвращаем специальный ответ вместо исключения
                    if err == "OVERLOAD_LIMIT":
                        print(f"WARNING: b24.call: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
                        return {"error": "OVERLOAD_LIMIT", "result": []}
                    
                    # Typical: OPERATION_TIME_LIMIT
                    if err in ("OPERATION_TIME_LIMIT", "QUERY_LIMIT_EXCEEDED") or "LIMIT" in err:
                        wait = BITRIX_BACKOFF_BASE_SEC * (2 ** attempt)
                        time.sleep(wait)
                        last_err = f"Bitrix error: {data.get('error')} {data.get('error_description')}"
                        continue

                    raise HTTPException(
                        status_code=502,
                        detail=f"Bitrix error: {data.get('error')} {data.get('error_description')}"
                    )

                return data
            except requests.RequestException as e:
                wait = BITRIX_BACKOFF_BASE_SEC * (2 ** attempt)
                time.sleep(wait)
                last_err = repr(e)

        raise HTTPException(status_code=502, detail=f"Bitrix retry limit exceeded. Last error: {last_err}")


b24 = BitrixClient(BITRIX_WEBHOOK)

# -----------------------------
# Postgres helpers
# -----------------------------
def pg_conn():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    try:
        conn.set_client_encoding("UTF8")
    except Exception as e:
        try:
            with conn.cursor() as cur:
                cur.execute("SET client_encoding TO 'UTF8'")
            conn.commit()
        except Exception:
            print(f"WARNING: pg_conn: could not set UTF8 encoding: {e}", file=sys.stderr, flush=True)
    return conn

def ensure_meta_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_meta_entities (
            entity_key TEXT PRIMARY KEY,
            entity_kind TEXT NOT NULL,  -- deal | smart_process
            title TEXT,
            entity_type_id INT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)

        # Create table (old installs may have it without new columns)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_meta_fields (
            entity_key TEXT NOT NULL,
            b24_field TEXT NOT NULL,
            column_name TEXT NOT NULL,
            b24_type TEXT,
            is_multiple BOOLEAN DEFAULT FALSE,
            is_required BOOLEAN DEFAULT FALSE,
            is_readonly BOOLEAN DEFAULT FALSE,
            settings JSONB,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (entity_key, b24_field)
        );
        """)

        # MIGRATION: add new columns if table already existed
        cur.execute('ALTER TABLE b24_meta_fields ADD COLUMN IF NOT EXISTS b24_title TEXT;')
        cur.execute('ALTER TABLE b24_meta_fields ADD COLUMN IF NOT EXISTS b24_labels JSONB;')

        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_sync_state (
            entity_key TEXT PRIMARY KEY,
            cursor TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        
        # Классификатор источников (sursa) для сделок
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_classifier_sources (
            source_id TEXT PRIMARY KEY,
            source_name TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)

        # Кэш пользователей Bitrix (id -> name) для отображения в API без вызова Bitrix
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_users (
            id BIGINT PRIMARY KEY,
            name TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)

        # Кэш компаний CRM (id, title, raw) для расшифровки COMPANY_ID и полей компании в сделках/контактах
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_crm_company (
            id BIGINT PRIMARY KEY,
            title TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        cur.execute("ALTER TABLE b24_crm_company ADD COLUMN IF NOT EXISTS raw JSONB;")

        # Воронки сделок (категории): id — ID категории, name — название
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_deal_categories (
            id TEXT PRIMARY KEY,
            name TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        # Стадии сделок: stage_id (например C12:NEW), category_id — воронка, name — название стадии
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_deal_stages (
            stage_id TEXT PRIMARY KEY,
            category_id TEXT,
            name TEXT,
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        # Воронки смарт-процессов: entity_type_id + category_id -> название воронки
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_sp_categories (
            entity_type_id TEXT NOT NULL,
            category_id TEXT NOT NULL,
            name TEXT,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (entity_type_id, category_id)
        );
        """)
        # Enum/списочные значения полей (в т.ч. UF_CRM_*): по entity_key + b24_field + value_id — value_title
        cur.execute("""
        CREATE TABLE IF NOT EXISTS b24_field_enum (
            entity_key TEXT NOT NULL,
            b24_field TEXT NOT NULL,
            value_id TEXT NOT NULL,
            value_title TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (entity_key, b24_field, value_id)
        );
        """)
        # Пользователи для входа в CRM (логин/пароль)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS crm_users (
            id SERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            password TEXT NOT NULL,
            created_on TIMESTAMPTZ DEFAULT now()
        );
        """)
        # Конфиги entity-table по page_slug (дашборды/страницы фронта)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS entity_table_configs (
            id BIGSERIAL PRIMARY KEY,
            page_slug TEXT NOT NULL UNIQUE,
            config_version INT NOT NULL DEFAULT 1,
            config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            created_by TEXT,
            updated_by TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS entity_table_config_revisions (
            id BIGSERIAL PRIMARY KEY,
            config_id BIGINT REFERENCES entity_table_configs(id) ON DELETE SET NULL,
            page_slug TEXT NOT NULL,
            revision_no INT NOT NULL,
            config_version INT NOT NULL DEFAULT 1,
            config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            created_by TEXT
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_entity_table_config_revisions_slug_created ON entity_table_config_revisions(page_slug, created_at DESC);")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_entity_table_config_revisions_slug_rev ON entity_table_config_revisions(page_slug, revision_no);")
    conn.commit()

def get_sync_cursor(conn, entity_key: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT cursor FROM b24_sync_state WHERE entity_key=%s", (entity_key,))
        row = cur.fetchone()
    if not row or row[0] is None:
        return 0
    try:
        return int(row[0])
    except Exception:
        return 0

def validate_sync_cursor(conn, entity_key: str, table: str) -> int:
    """
    Проверяет валидность курсора и сбрасывает его, если он слишком большой.
    Это защита от старых значений курсора (смещения), которые могут быть больше реальных ID.
    """
    last_id = get_sync_cursor(conn, entity_key)
    
    if last_id == 0:
        return 0  # Нормально - начинаем с начала
    
    # Проверяем, есть ли записи с ID больше last_id в базе
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT MAX(id) FROM "{table}"')
            max_db_id = cur.fetchone()[0] or 0
        
        # Если last_id намного больше максимального ID в базе - это старый курсор (смещение)
        # Сбрасываем его для безопасности
        if last_id > max_db_id + 1000:  # Запас 1000 на случай новых записей в Bitrix
            print(f"WARNING: validate_sync_cursor: Cursor {last_id} is too large (max DB ID: {max_db_id}) for {entity_key}, resetting to 0", file=sys.stderr, flush=True)
            set_sync_cursor(conn, entity_key, 0)
            return 0
    except Exception as e:
        # Если таблица не существует или ошибка - просто возвращаем last_id
        print(f"WARNING: validate_sync_cursor: Could not validate cursor for {entity_key}: {e}", file=sys.stderr, flush=True)
        return last_id
    
    return last_id

def set_sync_cursor(conn, entity_key: str, cursor: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO b24_sync_state(entity_key, cursor)
            VALUES (%s, %s)
            ON CONFLICT (entity_key) DO UPDATE
            SET cursor = EXCLUDED.cursor,
                updated_at = now()
        """, (entity_key, str(int(cursor))))
    conn.commit()


def _upsert_b24_user(conn, user_id: int, name: Optional[str]) -> None:
    """Сохранить/обновить имя пользователя в b24_users (для API без вызова Bitrix)."""
    if name is None or not str(name).strip():
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO b24_users (id, name, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name, updated_at = now()
            """, (int(user_id), str(name).strip()))
        conn.commit()
    except Exception as e:
        print(f"WARNING: _upsert_b24_user({user_id}): {e}", file=sys.stderr, flush=True)


# -----------------------------
# Naming + type mapping
# -----------------------------
def sanitize_ident(name: str, max_len: int = 55) -> str:
    name = str(name).lower()
    name = re.sub(r"[^a-z0-9_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "field"
    if len(name) > max_len:
        name = name[:max_len].rstrip("_")
    if name[0].isdigit():
        name = f"f_{name}"
    return name

def unique_column_name(existing: set, base: str) -> str:
    if base not in existing:
        existing.add(base)
        return base
    i = 2
    while f"{base}_{i}" in existing:
        i += 1
    col = f"{base}_{i}"
    existing.add(col)
    return col

def map_b24_to_pg_type(b24_type: Optional[str], is_multiple: bool) -> str:
    if is_multiple:
        return "JSONB"
    t = (b24_type or "").lower()
    if t in ("integer", "int"):
        return "BIGINT"
    if t in ("double", "float", "number"):
        return "DOUBLE PRECISION"
    if t in ("boolean", "bool"):
        return "BOOLEAN"
    if t in ("datetime",):
        return "TIMESTAMPTZ"
    if t in ("date",):
        return "DATE"
    if t in ("string", "text", "char"):
        return "TEXT"
    return "TEXT"

def table_name_for_entity(entity_key: str) -> str:
    if entity_key == "deal":
        return "b24_crm_deal"
    if entity_key == "contact":
        return "b24_crm_contact"
    if entity_key == "lead":
        return "b24_crm_lead"
    if entity_key == "company":
        return "b24_crm_company"
    if entity_key.startswith("sp:"):
        etid = entity_key.split(":", 1)[1]
        return f"b24_sp_{sanitize_ident(etid, max_len=20)}"
    return f"b24_{sanitize_ident(entity_key)}"

# -----------------------------
# Schema creation
# -----------------------------
def ensure_table_base(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id BIGINT,
            raw JSONB,
            created_at TIMESTAMPTZ DEFAULT now(),
            updated_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{sanitize_ident(table, 40)}_id ON {table}(id);")
    conn.commit()

def ensure_columns(conn, table: str, columns: List[Tuple[str, str]]):
    with conn.cursor() as cur:
        for col, pgtype in columns:
            cur.execute(f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS "{col}" {pgtype};')
    conn.commit()

def ensure_pk_index(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS ux_{sanitize_ident(table, 40)}_id ON {table}(id);')
    conn.commit()

def upsert_meta_entities(conn, items: List[Dict[str, Any]]):
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO b24_meta_entities (entity_key, entity_kind, title, entity_type_id)
            VALUES %s
            ON CONFLICT (entity_key) DO UPDATE
            SET entity_kind = EXCLUDED.entity_kind,
                title = EXCLUDED.title,
                entity_type_id = EXCLUDED.entity_type_id,
                updated_at = now()
            """,
            [
                (
                    i["entity_key"],
                    i["entity_kind"],
                    i.get("title"),
                    i.get("entity_type_id"),
                )
                for i in items
            ],
        )
    conn.commit()

def upsert_meta_fields(conn, entity_key: str, fields: Dict[str, Any], colmap: Dict[str, str]):
    """
    Сохраняем человеко-читаемое название поля из Bitrix в b24_title,
    и все варианты label-ов в b24_labels (JSONB).
    """
    def pick_title(meta: Dict[str, Any]) -> Optional[str]:
        for k in ("title", "formLabel", "listLabel", "filterLabel", "label", "name"):
            v = meta.get(k)
            s = _label_to_string(v)
            if s:
                return s
        return None

    rows = []
    for b24_field, meta in fields.items():
        settings_val = meta.get("settings")

        labels = {
            "title": meta.get("title"),
            "formLabel": meta.get("formLabel"),
            "listLabel": meta.get("listLabel"),
            "filterLabel": meta.get("filterLabel"),
            "label": meta.get("label"),
            "name": meta.get("name"),
        }

        rows.append((
            entity_key,
            b24_field,
            colmap[b24_field],
            meta.get("type"),
            bool(meta.get("isMultiple", False)),
            bool(meta.get("isRequired", False)),
            bool(meta.get("isReadOnly", False)),
            Json(settings_val) if settings_val is not None else None,
            pick_title(meta),
            Json(labels),
        ))

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO b24_meta_fields
            (entity_key, b24_field, column_name, b24_type, is_multiple, is_required, is_readonly, settings, b24_title, b24_labels)
            VALUES %s
            ON CONFLICT (entity_key, b24_field) DO UPDATE
            SET column_name = EXCLUDED.column_name,
                b24_type = EXCLUDED.b24_type,
                is_multiple = EXCLUDED.is_multiple,
                is_required = EXCLUDED.is_required,
                is_readonly = EXCLUDED.is_readonly,
                settings = EXCLUDED.settings,
                b24_title = EXCLUDED.b24_title,
                b24_labels = EXCLUDED.b24_labels,
                updated_at = now()
            """,
            rows
        )
    conn.commit()

def sync_sources_classifier(conn):
    """
    Синхронизирует классификатор источников (sursa) из Bitrix API.
    Получает enum значения напрямую из crm.deal.userfield.list для поля UF_CRM_1749211409067.
    Это пользовательское поле типа enumeration в сделках.
    """
    from api_data import DEALS_F_SURSA
    
    # Нормализуем название поля: может быть uf_crm_... или UF_CRM_...
    DEAL_SOURCE_UF = "UF_CRM_1749211409067"  # Всегда в верхнем регистре для Bitrix API
    
    print(f"INFO: sync_sources_classifier: Starting sync from Bitrix API (crm.deal.userfield.list) for field {DEAL_SOURCE_UF}", file=sys.stderr, flush=True)
    
    try:
        # Шаг 1: Получаем список всех пользовательских полей сделок из Bitrix API
        print(f"INFO: sync_sources_classifier: Calling crm.deal.userfield.list", file=sys.stderr, flush=True)
        data = b24.call("crm.deal.userfield.list", {})
        
        result = data.get("result")
        if not result:
            print(f"ERROR: sync_sources_classifier: No result in API response", file=sys.stderr, flush=True)
            return
        
        # Bitrix может вернуть result как dict с ключом userFields/fields/items, или как list
        user_fields = []
        if isinstance(result, dict):
            user_fields = result.get("userFields") or result.get("fields") or result.get("items") or []
        elif isinstance(result, list):
            user_fields = result
        else:
            print(f"ERROR: sync_sources_classifier: Unexpected result type: {type(result)}", file=sys.stderr, flush=True)
            return
        
        if not isinstance(user_fields, list):
            print(f"ERROR: sync_sources_classifier: user_fields is not a list: {type(user_fields)}", file=sys.stderr, flush=True)
            return
        
        print(f"INFO: sync_sources_classifier: Received {len(user_fields)} user fields from Bitrix API", file=sys.stderr, flush=True)
        
        if not user_fields:
            print(f"ERROR: sync_sources_classifier: user_fields is empty!", file=sys.stderr, flush=True)
            print(f"DEBUG: sync_sources_classifier: Full API response result: {result}", file=sys.stderr, flush=True)
            return
        
        # Шаг 2: Находим нужное поле UF_CRM_1749211409067
        uf = None
        all_field_names = []
        for u in user_fields:
            if not isinstance(u, dict):
                continue
            field_name = u.get("fieldName") or u.get("FIELD_NAME") or u.get("field_name") or u.get("FIELD") or u.get("field")
            all_field_names.append(field_name)
            if field_name == DEAL_SOURCE_UF or field_name == DEALS_F_SURSA or field_name == DEALS_F_SURSA.upper():
                uf = u
                print(f"DEBUG: sync_sources_classifier: Found matching field: {field_name}, full object keys: {list(u.keys())}", file=sys.stderr, flush=True)
                break
        
        if not uf:
            print(f"ERROR: sync_sources_classifier: Field {DEAL_SOURCE_UF} not found in user fields", file=sys.stderr, flush=True)
            # Логируем все названия полей для отладки
            print(f"DEBUG: sync_sources_classifier: All field names ({len(all_field_names)}): {all_field_names[:20]}", file=sys.stderr, flush=True)
            # Проверяем, есть ли поля, содержащие нужный ID
            matching_fields = [u for u in user_fields if isinstance(u, dict) and ("1749211409067" in str(u.get("fieldName", "")) or "1749211409067" in str(u.get("FIELD_NAME", "")))]
            if matching_fields:
                print(f"DEBUG: sync_sources_classifier: Found fields containing '1749211409067': {[u.get('fieldName') or u.get('FIELD_NAME') for u in matching_fields]}", file=sys.stderr, flush=True)
            return
        
        print(f"INFO: sync_sources_classifier: Found field {DEAL_SOURCE_UF} in API response", file=sys.stderr, flush=True)
        print(f"DEBUG: sync_sources_classifier: Field object keys: {list(uf.keys())}", file=sys.stderr, flush=True)
        
        # Шаг 3: Извлекаем enum значения из поля
        # Пробуем разные ключи: items, values, ENUM, LIST
        items = (
            uf.get("items") or 
            uf.get("values") or 
            uf.get("ENUM") or 
            uf.get("LIST") or 
            uf.get("list") or
            []
        )
        
        if not isinstance(items, list):
            print(f"ERROR: sync_sources_classifier: Items is not a list: {type(items)}, value: {items}", file=sys.stderr, flush=True)
            # Логируем все ключи объекта для отладки
            print(f"DEBUG: sync_sources_classifier: All field keys and their types: {[(k, type(v).__name__) for k, v in uf.items()]}", file=sys.stderr, flush=True)
            return
        
        if not items:
            print(f"WARNING: sync_sources_classifier: No enum items found in field {DEAL_SOURCE_UF}", file=sys.stderr, flush=True)
            print(f"DEBUG: sync_sources_classifier: Field object (first 500 chars): {str(uf)[:500]}", file=sys.stderr, flush=True)
            return
        
        print(f"INFO: sync_sources_classifier: Found {len(items)} enum items in field", file=sys.stderr, flush=True)
        # Логируем первый элемент для отладки
        if items and len(items) > 0:
            print(f"DEBUG: sync_sources_classifier: First item example: {items[0]}", file=sys.stderr, flush=True)
        
        # Шаг 4: Формируем список для вставки в классификатор
        # Используем правильную логику извлечения: ID для source_id, VALUE или NAME для source_name
        rows = []
        for opt in items:
            if not isinstance(opt, dict):
                continue
            
            # Правильная логика извлечения (как в примере ChatGPT):
            # vid (source_id) = ID или VALUE (если это число)
            # name (source_name) = VALUE или NAME (VALUE может быть текстом!)
            vid = opt.get("ID") or opt.get("VALUE") or opt.get("value")
            name = opt.get("VALUE") or opt.get("NAME") or opt.get("value") or opt.get("name")
            
            # Пропускаем пустые значения
            if vid is None or vid == "" or str(vid).strip() == "":
                if len(rows) < 3:  # Логируем только первые 3 пропущенных для отладки
                    print(f"DEBUG: sync_sources_classifier: Skipping item with empty ID/VALUE: {opt}", file=sys.stderr, flush=True)
                continue
            
            # Если name отсутствует, пропускаем (не используем vid как название)
            if name is None or name == "" or str(name).strip() == "":
                print(f"WARNING: sync_sources_classifier: Skipping item with empty NAME (vid={vid}): {opt}", file=sys.stderr, flush=True)
                continue
            
            # Убеждаемся, что vid - это ID, а name - это текст
            rows.append((str(vid), str(name)))
        
        if not rows:
            print(f"ERROR: sync_sources_classifier: No valid sources to insert. Processed {len(items)} items but got 0 valid rows", file=sys.stderr, flush=True)
            return
        
        print(f"INFO: sync_sources_classifier: Prepared {len(rows)} rows for insertion", file=sys.stderr, flush=True)
        
        # Шаг 5: Вставляем/обновляем классификатор
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO b24_classifier_sources (source_id, source_name)
                VALUES %s
                ON CONFLICT (source_id) DO UPDATE
                SET source_name = EXCLUDED.source_name,
                    updated_at = now()
                """,
                rows,
                page_size=100
            )
            
            conn.commit()
            print(f"INFO: sync_sources_classifier: Successfully synced {len(rows)} sources to classifier", file=sys.stderr, flush=True)
    except Exception as e:
        conn.rollback()
        print(f"ERROR: sync_sources_classifier: Failed to sync sources from Bitrix API: {e}", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise


def sync_deal_categories(conn) -> Tuple[List[Tuple[str, str]], List[str]]:
    """Заполняет b24_deal_categories. Возвращает (rows, debug_notes)."""
    rows: List[Tuple[str, str]] = []
    debug_notes: List[str] = []
    for method, params in [
        ("crm.category.list", {"entityTypeId": 2}),
        ("crm.category.list", {}),
        ("crm.dealcategory.list", {}),
    ]:
        try:
            data = b24.call(method, params)
            result = data.get("result")
            if not result:
                debug_notes.append(f"{method}: result empty")
                continue
            raw = result.get("categories") or result.get("items") if isinstance(result, dict) else result
            if isinstance(raw, dict):
                items = list(raw.values())
            elif isinstance(raw, list):
                items = raw
            else:
                items = []
            if isinstance(result, dict) and not items and result.get("result") is not None:
                r2 = result.get("result")
                items = list(r2.values()) if isinstance(r2, dict) else (r2 if isinstance(r2, list) else [])
            debug_notes.append(f"{method}: got {len(items)} items (first type: {type(items[0]).__name__ if items else 'n/a'})")
            for cat in items:
                cid = None
                name = ""
                if isinstance(cat, dict):
                    cid = cat.get("id") or cat.get("ID") or cat.get("Id") or cat.get("entityTypeId") or cat.get("categoryId")
                    name = cat.get("name") or cat.get("NAME") or cat.get("title") or cat.get("TITLE") or ""
                elif cat is not None and not isinstance(cat, (dict, list)):
                    cid = cat
                    name = str(cat)
                if cid is None:
                    continue
                rows.append((str(cid), (name or str(cid)).strip()))
            if rows:
                break
        except Exception as e:
            debug_notes.append(f"{method}: {type(e).__name__}: {e}")
            print(f"DEBUG: sync_deal_categories {method}: {e}", file=sys.stderr, flush=True)
            continue
    if rows:
        # Убираем дубликаты по id (ON CONFLICT не допускает два обновления одной строки в одной команде)
        seen_cat: Dict[str, Tuple[str, str]] = {}
        for cid, name in rows:
            seen_cat[cid] = (cid, name)
        rows = list(seen_cat.values())
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_deal_categories (id, name)
                    VALUES %s
                    ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()
                    """,
                    rows,
                    page_size=100,
                )
            conn.commit()
            print(f"INFO: sync_deal_categories: synced {len(rows)} categories", file=sys.stderr, flush=True)
        except Exception as e:
            conn.rollback()
            debug_notes.append(f"insert error: {e}")
            print(f"WARNING: sync_deal_categories insert: {e}", file=sys.stderr, flush=True)
    else:
        debug_notes.append("no categories parsed (check result.categories / dealcategory list format)")
    return rows, debug_notes


def sync_sources_from_status(conn) -> int:
    """
    Синхронизирует стандартные источники (SOURCE) из crm.status.list ENTITY_ID=SOURCE
    в b24_classifier_sources и в b24_field_enum (deal/lead/contact source_id). Так значение «Источник»
    в сделках/лидах/контактах будет показывать название вместо кода (UC_Z315Y5 и т.д.).
    """
    try:
        data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": "SOURCE"}})
        result = data.get("result")
        if not result:
            return 0
        items = result if isinstance(result, list) else (
            result.get("result") or result.get("statuses") or result.get("items")
            or result.get("SOURCE") or result.get("source") or []
        )
        if not isinstance(items, list) or not items:
            return 0
        rows: List[Tuple[str, str]] = []
        for st in items:
            sid = None
            name = ""
            if isinstance(st, dict):
                sid = (
                    st.get("STATUS_ID") or st.get("statusId") or st.get("id") or st.get("ID")
                    or st.get("VALUE") or st.get("value") or st.get("SYMBOL_CODE")
                )
                name = st.get("NAME") or st.get("name") or st.get("title") or st.get("TITLE") or ""
            if sid is None:
                continue
            sid_str = str(sid).strip()
            name_str = (name or sid_str).strip()
            rows.append((sid_str, name_str))
        if not rows:
            return 0
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO b24_classifier_sources (source_id, source_name)
                VALUES %s
                ON CONFLICT (source_id) DO UPDATE SET source_name = EXCLUDED.source_name, updated_at = now()
                """,
                rows,
                page_size=200,
            )
            enum_rows: List[Tuple[str, str, str, str]] = []
            for sid_str, name_str in rows:
                enum_rows.append(("deal", "source_id", sid_str, name_str))
                enum_rows.append(("lead", "source_id", sid_str, name_str))
                enum_rows.append(("contact", "source_id", sid_str, name_str))
            if enum_rows:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_field_enum (entity_key, b24_field, value_id, value_title)
                    VALUES %s
                    ON CONFLICT (entity_key, b24_field, value_id) DO UPDATE SET value_title = EXCLUDED.value_title, updated_at = now()
                    """,
                    enum_rows,
                    page_size=200,
                )
        conn.commit()
        print(f"INFO: sync_sources_from_status: synced {len(rows)} standard SOURCE statuses", file=sys.stderr, flush=True)
        return len(rows)
    except Exception as e:
        conn.rollback()
        print(f"WARNING: sync_sources_from_status: {e}", file=sys.stderr, flush=True)
        return 0


def sync_deal_stages(conn) -> Tuple[List[Tuple[str, str, str]], List[str]]:
    """Заполняет b24_deal_stages из crm.status.list. Возвращает (rows, debug_notes)."""
    debug_notes: List[str] = []
    entity_ids = ["DEAL_STAGE"]
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM b24_deal_categories ORDER BY id")
        for row in cur.fetchall() or []:
            entity_ids.append(f"DEAL_STAGE_{row[0]}")
    if len(entity_ids) == 1:
        entity_ids = ["DEAL_STAGE", "DEAL_STAGE_0", "DEAL_STAGE_1", "DEAL_STAGE_2", "DEAL_STAGE_12", "DEAL_STAGE_20"]
    rows: List[Tuple[str, str, str]] = []
    for entity_id in entity_ids:
        try:
            data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": entity_id}})
            result = data.get("result")
            if not result:
                continue
            items = result if isinstance(result, list) else (result.get("result") or result.get("statuses") or result.get("items") or [])
            if not isinstance(items, list):
                items = []
            if items and len(debug_notes) == 0:
                debug_notes.append(f"status.list {entity_id}: {len(items)} items, first type={type(items[0]).__name__}")
            cat_id = entity_id.replace("DEAL_STAGE_", "") if entity_id != "DEAL_STAGE" else "0"
            for st in items:
                sid = None
                name = ""
                if isinstance(st, dict):
                    sid = st.get("STATUS_ID") or st.get("statusId") or st.get("id") or st.get("ID")
                    name = st.get("NAME") or st.get("name") or st.get("title") or st.get("TITLE") or ""
                elif st is not None and not isinstance(st, (dict, list)):
                    sid = st
                    name = str(st)
                if sid is None:
                    continue
                rows.append((str(sid), cat_id, (name or str(sid)).strip()))
        except Exception as e:
            debug_notes.append(f"status.list {entity_id}: {e}")
            print(f"WARNING: sync_deal_stages {entity_id}: {e}", file=sys.stderr, flush=True)
    if rows:
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_deal_stages (stage_id, category_id, name)
                    VALUES %s
                    ON CONFLICT (stage_id) DO UPDATE SET category_id = EXCLUDED.category_id, name = EXCLUDED.name, updated_at = now()
                    """,
                    rows,
                    page_size=200,
                )
            conn.commit()
            print(f"INFO: sync_deal_stages: synced {len(rows)} stages", file=sys.stderr, flush=True)
        except Exception as e:
            conn.rollback()
            debug_notes.append(f"stages insert: {e}")
            print(f"WARNING: sync_deal_stages insert: {e}", file=sys.stderr, flush=True)
    return rows, debug_notes


def sync_smart_process_stages(conn) -> Tuple[int, List[str]]:
    """
    Заполняет b24_deal_stages стадиями смарт-процессов из crm.status.list.
    ENTITY_ID для смарт-процесса: DYNAMIC_{entityTypeId}_STAGE_{funnelId}.
    stage_id в Bitrix приходит как DT1114_1:NEW и т.д.
    """
    debug_notes: List[str] = []
    rows: List[Tuple[str, str, str]] = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT entity_type_id FROM b24_meta_entities
                WHERE entity_kind = 'smart_process' AND entity_type_id IS NOT NULL
                ORDER BY entity_type_id
            """)
            entity_type_ids = [int(r[0]) for r in cur.fetchall() if r and r[0] is not None]
    except Exception as e:
        debug_notes.append(f"smart_stages meta: {e}")
        return 0, debug_notes
    if not entity_type_ids:
        return 0, debug_notes
    sp_category_rows: List[Tuple[str, str, str]] = []
    for etid in entity_type_ids:
        category_ids: List[int] = [0]
        try:
            data = b24.call("crm.category.list", {"entityTypeId": etid})
            result = data.get("result") or {}
            cats = result.get("categories") or result.get("result") or []
            if isinstance(cats, list) and cats:
                for c in cats:
                    if isinstance(c, dict):
                        cid = c.get("id") or c.get("ID")
                        cname = c.get("name") or c.get("NAME") or c.get("title") or c.get("TITLE") or ""
                        if cid is not None:
                            category_ids.append(int(cid))
                            sp_category_rows.append((str(etid), str(cid), (cname or str(cid)).strip()))
                category_ids = list(dict.fromkeys(category_ids))
        except Exception as e:
            debug_notes.append(f"smart_stages category etid={etid}: {e}")
        for cat_id in category_ids:
            entity_id = f"DYNAMIC_{etid}_STAGE_{cat_id}" if cat_id != 0 else f"DYNAMIC_{etid}_STAGE"
            try:
                data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": entity_id}})
                result = data.get("result")
                if not result:
                    continue
                items = result if isinstance(result, list) else (
                    result.get("result") or result.get("statuses") or result.get("items") or []
                )
                if not isinstance(items, list):
                    items = []
                cat_str = str(cat_id)
                for st in items:
                    sid = None
                    name = ""
                    if isinstance(st, dict):
                        sid = st.get("STATUS_ID") or st.get("statusId") or st.get("id") or st.get("ID")
                        name = st.get("NAME") or st.get("name") or st.get("title") or st.get("TITLE") or ""
                    elif st is not None and not isinstance(st, (dict, list)):
                        sid = st
                        name = str(st)
                    if sid is None:
                        continue
                    rows.append((str(sid), cat_str, (name or str(sid)).strip()))
            except Exception as e:
                debug_notes.append(f"smart_stages status etid={etid} cat={cat_id}: {e}")
    if rows:
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_deal_stages (stage_id, category_id, name)
                    VALUES %s
                    ON CONFLICT (stage_id) DO UPDATE SET category_id = EXCLUDED.category_id, name = EXCLUDED.name, updated_at = now()
                    """,
                    rows,
                    page_size=200,
                )
            conn.commit()
            print(f"INFO: sync_smart_process_stages: synced {len(rows)} stages", file=sys.stderr, flush=True)
        except Exception as e:
            conn.rollback()
            debug_notes.append(f"smart_stages insert: {e}")
    if sp_category_rows:
        try:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_sp_categories (entity_type_id, category_id, name)
                    VALUES %s
                    ON CONFLICT (entity_type_id, category_id) DO UPDATE SET name = EXCLUDED.name, updated_at = now()
                    """,
                    sp_category_rows,
                    page_size=200,
                )
            conn.commit()
            print(f"INFO: sync_smart_process_stages: synced {len(sp_category_rows)} SP categories", file=sys.stderr, flush=True)
        except Exception as e:
            conn.rollback()
            debug_notes.append(f"smart_stages categories insert: {e}")
    return len(rows), debug_notes


def sync_deal_types(conn) -> None:
    """Заполняет b24_field_enum типами сделок (TYPE_ID) из crm.status.list ENTITY_ID=DEAL_TYPE."""
    rows: List[Tuple[str, str, str, str]] = []
    for entity_id in ("DEAL_TYPE", "TYPE"):
        try:
            data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": entity_id}})
            result = data.get("result")
            if not result:
                continue
            items: List[Any] = []
            if isinstance(result, list):
                items = result
            elif isinstance(result, dict):
                items = result.get("result") or result.get("statuses") or result.get("items") or []
                if not isinstance(items, list):
                    items = list(result.values()) if result else []
                if not items and result:
                    # формат { "UC_XXX": {"NAME": "..."}, ... }
                    for sid, st in result.items():
                        if isinstance(st, dict):
                            name = st.get("NAME") or st.get("name") or st.get("title") or st.get("TITLE") or ""
                            items.append({"STATUS_ID": sid, "NAME": name or sid})
                        elif st is not None and not isinstance(st, (dict, list)):
                            items.append({"STATUS_ID": sid, "NAME": str(st)})
            if not isinstance(items, list):
                items = []
            for st in items:
                sid = None
                name = ""
                if isinstance(st, dict):
                    sid = st.get("STATUS_ID") or st.get("statusId") or st.get("id") or st.get("ID")
                    name = st.get("NAME") or st.get("name") or st.get("title") or st.get("TITLE") or ""
                elif st is not None and not isinstance(st, (dict, list)):
                    sid = st
                    name = str(st)
                if sid is None:
                    continue
                rows.append(("deal", "TYPE_ID", str(sid), (name or str(sid)).strip()))
            if rows:
                break
        except Exception as e:
            print(f"DEBUG: sync_deal_types {entity_id}: {e}", file=sys.stderr, flush=True)
            continue
    if not rows:
        return
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO b24_field_enum (entity_key, b24_field, value_id, value_title)
                VALUES %s
                ON CONFLICT (entity_key, b24_field, value_id) DO UPDATE SET value_title = EXCLUDED.value_title, updated_at = now()
                """,
                rows,
                page_size=100,
            )
        conn.commit()
        print(f"INFO: sync_deal_types: synced {len(rows)} deal types", file=sys.stderr, flush=True)
    except Exception as e:
        conn.rollback()
        print(f"WARNING: sync_deal_types: {e}", file=sys.stderr, flush=True)


def _userfield_items_to_enum_rows(entity_key: str, field_name: str, items: List[Any]) -> List[Tuple[str, str, str, str]]:
    rows = []
    for opt in items or []:
        if not isinstance(opt, dict):
            continue
        vid = (
            opt.get("ID") or opt.get("id") or opt.get("STATUS_ID") or opt.get("statusId")
            or opt.get("VALUE") or opt.get("value")
        )
        title = (
            opt.get("VALUE") or opt.get("value") or opt.get("NAME") or opt.get("name")
            or opt.get("TITLE") or opt.get("title")
        )
        if vid is None or (isinstance(vid, str) and not vid.strip()):
            continue
        if title is None:
            title = str(vid)
        title_clean = unicodedata.normalize("NFC", str(title).strip())
        rows.append((entity_key, field_name, str(vid).strip(), title_clean))
    return rows


def sync_field_enums(conn, entity_key: str) -> Tuple[int, List[str]]:
    """Синхронизирует enum/списочные значения полей в b24_field_enum. Возвращает (n_inserted, debug_notes)."""
    debug_notes: List[str] = []
    if entity_key == "deal":
        method = "crm.deal.userfield.list"
        use_smart_api = False
    elif entity_key == "contact":
        method = "crm.contact.userfield.list"
        use_smart_api = False
    elif entity_key == "lead":
        method = "crm.lead.userfield.list"
        use_smart_api = False
    elif entity_key == "company":
        method = "crm.company.userfield.list"
        use_smart_api = False
    elif entity_key and entity_key.startswith("sp:"):
        use_smart_api = True
        try:
            etid = int(entity_key.split(":", 1)[1])
        except (IndexError, ValueError):
            return 0, []
        method = None
    else:
        return 0, []
    try:
        field_list: List[Tuple[str, Dict[str, Any]]] = []
        if use_smart_api:
            fields = fetch_smart_fields(etid)
            if isinstance(fields, dict):
                field_list = [(str(fn), uf) for fn, uf in fields.items() if fn and isinstance(uf, dict)]
            debug_notes.append(f"{entity_key}: crm.item.fields, {len(field_list)} fields")
        else:
            data = b24.call(method, {})
            result = data.get("result")
            if not result:
                debug_notes.append(f"{entity_key} userfield.list: result empty")
                return 0, debug_notes
            if isinstance(result, list):
                for uf in result:
                    if isinstance(uf, dict):
                        fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                        if fn:
                            field_list.append((fn, uf))
                debug_notes.append(f"{entity_key}: result is list, {len(field_list)} fields")
            elif isinstance(result, dict):
                if result.get("userFields") and isinstance(result["userFields"], list):
                    for uf in result["userFields"]:
                        if isinstance(uf, dict):
                            fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                            if fn:
                                field_list.append((fn, uf))
                elif result.get("fields") and isinstance(result["fields"], dict):
                    for fn, uf in result["fields"].items():
                        if isinstance(uf, dict) and fn:
                            field_list.append((str(fn), uf))
                elif result.get("fields") and isinstance(result["fields"], list):
                    for uf in result["fields"]:
                        if isinstance(uf, dict):
                            fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                            if fn:
                                field_list.append((fn, uf))
                else:
                    for fn, uf in result.items():
                        if isinstance(uf, dict) and fn and not fn.startswith("_"):
                            field_list.append((str(fn), uf))
                debug_notes.append(f"{entity_key}: result is dict, {len(field_list)} fields")
        all_rows = []
        fields_with_items = 0
        for field_name, uf in field_list:
            raw_items = (
                uf.get("items") or uf.get("values") or uf.get("options") or uf.get("option")
                or uf.get("ENUM") or uf.get("LIST") or uf.get("list") or []
            )
            if not raw_items and isinstance(uf.get("settings"), dict):
                raw_items = uf["settings"].get("items") or uf["settings"].get("options") or uf["settings"].get("list") or []
            if isinstance(raw_items, dict):
                inner = raw_items.get("items") or raw_items.get("values")
                if inner is not None and isinstance(inner, list):
                    items = inner
                else:
                    # Bitrix crm.item.fields часто возвращает items как { "id": "title", ... } или { "id": { "VALUE": "..." }, ... }
                    def _title_from(v: Any) -> str:
                        if isinstance(v, str):
                            return v
                        if isinstance(v, dict):
                            return str(v.get("VALUE") or v.get("NAME") or v.get("title") or v.get("TITLE") or v)
                        return str(v)
                    items = [{"ID": k, "VALUE": _title_from(v)} for k, v in raw_items.items() if str(k).strip() != ""]
            else:
                items = raw_items if isinstance(raw_items, list) else []
            if not items:
                # Списочные поля смарт-процесса часто хранят entityId в settings — варианты через crm.status.entity.items.
                # Пробуем для любого поля (Transmisie, Tractiune, Filiala и т.д. могут не иметь type=list в ответе).
                settings = uf.get("settings") or uf.get("SETTINGS") or {}
                if isinstance(settings, dict):
                    eid = (
                        settings.get("entityId") or settings.get("ENTITY_ID")
                        or settings.get("listEntityId") or settings.get("LIST_ENTITY_ID")
                        or uf.get("entityId") or uf.get("listEntityId")
                    )
                    if eid:
                        try:
                            st_data = b24.call("crm.status.entity.items", {"entityId": str(eid).strip()})
                            st_res = st_data.get("result")
                            if isinstance(st_res, list):
                                st_list = st_res
                            elif isinstance(st_res, dict):
                                st_list = st_res.get("items") or st_res.get("result") or []
                            else:
                                st_list = []
                            if isinstance(st_list, list) and st_list:
                                items = []
                                for st in st_list:
                                    if isinstance(st, dict):
                                        sid = st.get("ID") or st.get("id") or st.get("STATUS_ID") or st.get("statusId")
                                        name = st.get("VALUE") or st.get("value") or st.get("NAME") or st.get("name") or st.get("TITLE")
                                        if sid is not None:
                                            items.append({"ID": str(sid), "VALUE": str(name or sid).strip()})
                                    elif st is not None:
                                        items.append({"ID": str(st), "VALUE": str(st)})
                                if items:
                                    debug_notes.append(f"{entity_key}: {field_name} from status.entity.items entityId={eid} ({len(items)} items)")
                                    print(f"INFO: sync_field_enums({entity_key}): {field_name} <- crm.status.entity.items entityId={eid} ({len(items)} values)", file=sys.stderr, flush=True)
                            else:
                                print(f"INFO: sync_field_enums({entity_key}): {field_name} entityId={eid} -> empty result", file=sys.stderr, flush=True)
                        except Exception as e:
                            debug_notes.append(f"{entity_key}: {field_name} status.entity.items: {e}")
                            print(f"WARNING: sync_field_enums({entity_key}): {field_name} entityId={eid} -> {e}", file=sys.stderr, flush=True)
            if not items:
                # Поля типа iblock_element (Transmisie, Tracțiune, Filiala и т.д.) — варианты в инфоблоке, в settings есть IBLOCK_ID
                settings = uf.get("settings") or uf.get("SETTINGS") or {}
                if isinstance(settings, dict):
                    _iblock_id = settings.get("IBLOCK_ID")
                    field_type = (
                        uf.get("type")
                        or uf.get("USER_TYPE_ID")
                        or uf.get("userTypeId")
                        or ""
                    )
                    if _iblock_id is not None and str(field_type).strip().lower() == "iblock_element":
                        try:
                            iblock_id = int(_iblock_id)
                            # Пробуем lists.element.get (REST Bitrix24 — элементы списка по IBLOCK_ID)
                            el_data = b24.call("lists.element.get", {"IBLOCK_TYPE_ID": "lists", "IBLOCK_ID": iblock_id})
                            el_res = el_data.get("result")
                            if isinstance(el_res, list) and el_res:
                                items = []
                                for el in el_res:
                                    if isinstance(el, dict):
                                        eid = el.get("ID") or el.get("id") or el.get("ELEMENT_ID")
                                        name = el.get("NAME") or el.get("name") or el.get("VALUE") or el.get("title")
                                        if eid is not None:
                                            items.append({"ID": str(eid), "VALUE": str(name or eid).strip()})
                                    elif el is not None:
                                        items.append({"ID": str(el), "VALUE": str(el)})
                                if items:
                                    debug_notes.append(f"{entity_key}: {field_name} from lists.element.get IBLOCK_ID={iblock_id} ({len(items)} items)")
                                    print(f"INFO: sync_field_enums({entity_key}): {field_name} <- lists.element.get IBLOCK_ID={iblock_id} ({len(items)} values)", file=sys.stderr, flush=True)
                            elif isinstance(el_res, dict):
                                el_list = el_res.get("elements") or el_res.get("items") or el_res.get("result") or []
                                if isinstance(el_list, list) and el_list:
                                    items = []
                                    for el in el_list:
                                        if isinstance(el, dict):
                                            eid = el.get("ID") or el.get("id") or el.get("ELEMENT_ID")
                                            name = el.get("NAME") or el.get("name") or el.get("VALUE") or el.get("title")
                                            if eid is not None:
                                                items.append({"ID": str(eid), "VALUE": str(name or eid).strip()})
                                        elif el is not None:
                                            items.append({"ID": str(el), "VALUE": str(el)})
                                    if items:
                                        debug_notes.append(f"{entity_key}: {field_name} from lists.element.get IBLOCK_ID={iblock_id} ({len(items)} items)")
                                        print(f"INFO: sync_field_enums({entity_key}): {field_name} <- lists.element.get IBLOCK_ID={iblock_id} ({len(items)} values)", file=sys.stderr, flush=True)
                            if not items:
                                print(f"INFO: sync_field_enums({entity_key}): {field_name} IBLOCK_ID={_iblock_id} -> empty or unsupported format", file=sys.stderr, flush=True)
                        except Exception as e:
                            debug_notes.append(f"{entity_key}: {field_name} lists.element.get: {e}")
                            print(f"WARNING: sync_field_enums({entity_key}): {field_name} IBLOCK_ID={_iblock_id} -> {e}", file=sys.stderr, flush=True)
            if not items:
                continue
            fields_with_items += 1
            all_rows.extend(_userfield_items_to_enum_rows(entity_key, field_name, items))
        debug_notes.append(f"{entity_key}: {fields_with_items} fields with enum items, {len(all_rows)} total values")
        if all_rows:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO b24_field_enum (entity_key, b24_field, value_id, value_title)
                    VALUES %s
                    ON CONFLICT (entity_key, b24_field, value_id) DO UPDATE SET value_title = EXCLUDED.value_title, updated_at = now()
                    """,
                    all_rows,
                    page_size=200,
                )
            conn.commit()
            print(f"INFO: sync_field_enums({entity_key}): synced {len(all_rows)} enum values", file=sys.stderr, flush=True)
        return len(all_rows), debug_notes
    except Exception as e:
        conn.rollback()
        debug_notes.append(f"{entity_key}: {type(e).__name__}: {e}")
        print(f"WARNING: sync_field_enums({entity_key}): {e}", file=sys.stderr, flush=True)
        return 0, debug_notes


def _label_to_string(val: Any) -> Optional[str]:
    """Извлекает строку из label: строка как есть, dict — берём ru/en/first."""
    if val is None:
        return None
    if isinstance(val, str) and val.strip():
        return val.strip()
    if isinstance(val, dict):
        for k in ("ru", "en", "de", "ua", "first"):
            v = val.get(k) if k != "first" else (next(iter(val.values()), None) if val else None)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for v in val.values():
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _userfield_list_to_field_titles(entity_key: str, result: Any) -> List[Tuple[str, str]]:
    """
    Парсит ответ crm.*.userfield.list и возвращает [(b24_field, human_title), ...].
    «Название поля» из админки Битрикс (как на скрине) обычно приходит в listLabel / editFormLabel / formLabel.
    """
    field_list: List[Tuple[str, Dict[str, Any]]] = []
    if isinstance(result, list):
        for uf in result:
            if isinstance(uf, dict):
                fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                if fn:
                    field_list.append((fn, uf))
    elif isinstance(result, dict):
        if result.get("userFields") and isinstance(result["userFields"], list):
            for uf in result["userFields"]:
                if isinstance(uf, dict):
                    fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                    if fn:
                        field_list.append((fn, uf))
        elif result.get("items") and isinstance(result["items"], list):
            for uf in result["items"]:
                if isinstance(uf, dict):
                    fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                    if fn:
                        field_list.append((fn, uf))
        elif result.get("fields") and isinstance(result["fields"], dict):
            for fn, uf in result["fields"].items():
                if isinstance(uf, dict) and fn:
                    field_list.append((str(fn), uf))
        elif result.get("fields") and isinstance(result["fields"], list):
            for uf in result["fields"]:
                if isinstance(uf, dict):
                    fn = uf.get("fieldName") or uf.get("FIELD_NAME") or uf.get("field_name") or uf.get("field")
                    if fn:
                        field_list.append((fn, uf))
        else:
            for fn, uf in result.items():
                if isinstance(uf, dict) and fn and not str(fn).startswith("_"):
                    field_list.append((str(fn), uf))
    # Ключи, в которых Битрикс может вернуть «Название поля» (как в админке: Код поля / Название поля)
    _title_keys = (
        "listLabel", "editFormLabel", "formLabel", "filterLabel",
        "title", "label", "name", "fieldLabel", "displayLabel", "caption", "header",
        "LIST_LABEL", "EDIT_FORM_LABEL", "FORM_LABEL", "TITLE", "LABEL", "NAME",
    )
    out: List[Tuple[str, str]] = []
    for field_name, uf in field_list:
        title = None
        for k in _title_keys:
            title = _label_to_string(uf.get(k))
            if title:
                break
        if title:
            out.append((field_name, title))
    return out


def _fields_response_to_title_pairs(fields_result: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Из ответа crm.*.fields извлекает [(b24_field, human_title), ...] для полей UF_CRM_*."""
    if not isinstance(fields_result, dict):
        return []
    out: List[Tuple[str, str]] = []
    for fn, meta in fields_result.items():
        if not fn or not isinstance(meta, dict):
            continue
        fn_str = str(fn).strip()
        if not (fn_str.upper().startswith("UF_CRM_") or fn_str.startswith("ufCrm")):
            continue
        title = None
        for k in ("listLabel", "editFormLabel", "formLabel", "filterLabel", "title", "label", "name"):
            title = _label_to_string(meta.get(k))
            if title:
                break
        if title:
            out.append((fn_str, title))
    return out


def sync_userfield_titles(conn, entity_key: str) -> int:
    """Обновляет b24_title в b24_meta_fields из crm.*.userfield.list (или crm.*.fields) для человекочитаемых названий полей."""
    if entity_key == "deal":
        method = "crm.deal.userfield.list"
        method_fallback = "crm.deal.fields"
    elif entity_key == "contact":
        method = "crm.contact.userfield.list"
        method_fallback = "crm.contact.fields"
    elif entity_key == "lead":
        method = "crm.lead.userfield.list"
        method_fallback = "crm.lead.fields"
    elif entity_key == "company":
        method = "crm.company.userfield.list"
        method_fallback = "crm.company.fields"
    else:
        return 0
    updated = 0
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT b24_field FROM b24_meta_fields WHERE entity_key = %s",
                (entity_key,),
            )
            existing = [row[0] for row in cur.fetchall() if row and row[0]]
        upper_to_field: Dict[str, str] = {str(f).upper(): f for f in existing}

        def apply_pairs(pairs: List[Tuple[str, str]]) -> int:
            n = 0
            with conn.cursor() as cur:
                for api_field, title in pairs:
                    canonical = upper_to_field.get(str(api_field).upper()) if api_field else None
                    if not canonical:
                        continue
                    cur.execute("""
                        UPDATE b24_meta_fields SET b24_title = %s, updated_at = now()
                        WHERE entity_key = %s AND b24_field = %s
                    """, (title, entity_key, canonical))
                    if cur.rowcount:
                        n += 1
            return n

        data = b24.call(method, {})
        result = data.get("result")
        if result:
            pairs = _userfield_list_to_field_titles(entity_key, result)
            if pairs:
                updated = apply_pairs(pairs)
        if updated == 0 and method_fallback:
            fallback_data = b24.call(method_fallback, {})
            fallback_result = fallback_data.get("result")
            if isinstance(fallback_result, dict):
                pairs = _fields_response_to_title_pairs(fallback_result)
                if pairs:
                    updated = apply_pairs(pairs)
                    print(f"INFO: sync_userfield_titles({entity_key}): used {method_fallback} fallback, updated {updated}", file=sys.stderr, flush=True)
        conn.commit()
        if updated:
            print(f"INFO: sync_userfield_titles({entity_key}): updated {updated} field titles", file=sys.stderr, flush=True)
        return updated
    except Exception as e:
        conn.rollback()
        print(f"WARNING: sync_userfield_titles({entity_key}): {e}", file=sys.stderr, flush=True)
        return 0


def load_entity_colmap(conn, entity_key: str) -> Dict[str, Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT b24_field, column_name, b24_type, is_multiple
            FROM b24_meta_fields
            WHERE entity_key = %s
        """, (entity_key,))
        rows = cur.fetchall()

    m: Dict[str, Dict[str, Any]] = {}
    for b24_field, column_name, b24_type, is_multiple in rows:
        m[b24_field] = {
            "column_name": column_name,
            "b24_type": b24_type,
            "is_multiple": bool(is_multiple),
        }
    return m

def normalize_value(v: Any, b24_type: Optional[str] = None, is_multiple: bool = False):
    """
    Нормализует значение для вставки в PostgreSQL.
    - Пустые строки для дат/времени/чисел преобразуются в None
    - Для is_multiple полей значения оборачиваются в Json (даже boolean)
    - FIX: если колонка numeric/double/integer, а Bitrix прислал boolean -> приводим к 0/1
    - Дополнительно: мягкий парс "Y/N", "true/false", "1/0" для boolean
    """
    # Если поле multiple, колонка имеет тип JSONB - всегда оборачиваем в Json
    if is_multiple:
        if v is None:
            return None
        if isinstance(v, (dict, list)):
            return Json(v)
        return Json([v])

    # Для не-multiple: dict/list -> Json
    if isinstance(v, (dict, list)):
        return Json(v)

    bt = (b24_type or "").lower().strip()

    # Пустые строки -> None (кроме явного string/text)
    if isinstance(v, str) and not v.strip():
        if bt not in ("string", "text", "char"):
            return None
        return v

    # Нормализация boolean строк
    if bt in ("boolean", "bool"):
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("y", "yes", "true", "1", "on"):
                return True
            if s in ("n", "no", "false", "0", "off"):
                return False

    # FIX: numeric типы, но прилетел bool -> 0/1
    if isinstance(v, bool) and bt in (
        "double", "double precision", "float", "float8", "number", "numeric", "integer", "int", "int4", "int8"
    ):
        # для double лучше float
        if bt in ("double", "double precision", "float", "float8", "number", "numeric"):
            return 1.0 if v else 0.0
        return 1 if v else 0

    # (опционально) мягкий парс чисел из строк, если Bitrix прислал "123" или "12.5"
    if isinstance(v, str) and bt in ("double", "double precision", "float", "float8", "number", "numeric", "integer", "int", "int4", "int8"):
        s = v.strip().replace(",", ".")
        try:
            if bt in ("integer", "int", "int4", "int8"):
                return int(float(s))
            return float(s)
        except Exception:
            return v  # оставляем как есть

    return v

def upsert_rows(conn, table: str, columns: List[str], rows: List[List[Any]]):
    """
    Upsert rows into table by 'id'. Uses execute_values for speed.
    FIX: updated_at исключаем из set_cols, иначе получается 2 раза:
         updated_at = EXCLUDED.updated_at, updated_at = now()
    """
    if not rows:
        return

    # на всякий случай — убираем дубликаты колонок, сохраняя порядок
    seen = set()
    col_order = []
    for c in columns:
        if c not in seen:
            seen.add(c)
            col_order.append(c)

    cols_sql = ", ".join([f'"{c}"' for c in col_order])
    tmpl = "(" + ",".join(["%s"] * len(col_order)) + ")"

    # Важно: исключаем updated_at из set_cols
    set_cols = [c for c in col_order if c not in ("id", "created_at", "updated_at")]
    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in set_cols])

    # updated_at всегда обновляем
    if set_sql:
        set_sql = set_sql + ', "updated_at" = now()'
    else:
        set_sql = '"updated_at" = now()'

    sql = f"""
    INSERT INTO {table} ({cols_sql})
    VALUES %s
    ON CONFLICT ("id") DO UPDATE
    SET {set_sql}
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows, template=tmpl, page_size=500)
    conn.commit()

def day_start_utc(tz_name: str = "Europe/Chisinau") -> datetime:
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    return start_local.astimezone(timezone.utc)

def normalize_value(v: Any, b24_type: Optional[str] = None, is_multiple: bool = False):
    """
    Нормализует значение для вставки в PostgreSQL.
    - Пустые строки для дат/времени/чисел преобразуются в None
    - Для is_multiple полей значения оборачиваются в Json (даже boolean)
    """
    # Если поле multiple, колонка имеет тип JSONB - всегда оборачиваем в Json
    if is_multiple:
        if v is None:
            return None
        # Если уже dict/list - оборачиваем в Json
        if isinstance(v, (dict, list)):
            return Json(v)
        # Если scalar (boolean, int, str) - оборачиваем в Json как массив
        return Json([v])
    
    # Для не-multiple полей: если значение dict/list, оборачиваем в Json
    if isinstance(v, (dict, list)):
        return Json(v)
    
    # Для пустых строк: преобразуем в None для всех типов, кроме TEXT
    if isinstance(v, str) and not v.strip():
        # Всегда преобразуем пустые строки в None (PostgreSQL не любит пустые строки в DATE/NUMERIC)
        # Исключение: если это явно TEXT поле
        if not b24_type or b24_type.lower() not in ("string", "text", "char"):
            return None
    
    return v

# -----------------------------
# Normalize Bitrix list response
# -----------------------------
def normalize_list_result(resp: Any) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Поддерживаем варианты:
      - crm.deal.list: {"result":[...], "next":50}
      - crm.item.list: {"result":{"items":[...]},"next":50}   (ВАЖНО: next часто сверху)
      - crm.item.list: {"result":{"items":[...], "next":50}}  (иногда next внутри)
      - иногда list напрямую
    Возвращаем (items, next_start)
    """
    if resp is None:
        return [], None

    if isinstance(resp, list):
        return [x for x in resp if isinstance(x, dict)], None

    if not isinstance(resp, dict):
        return [], None

    top_next = resp.get("next")

    # crm.deal.list: result = list
    if isinstance(resp.get("result"), list):
        items = [x for x in resp["result"] if isinstance(x, dict)]
        return items, (int(top_next) if top_next is not None else None)

    # crm.item.list: result = dict
    if isinstance(resp.get("result"), dict):
        inner = resp["result"]
        items = inner.get("items") or inner.get("result") or []
        if not isinstance(items, list):
            items = []
        items = [x for x in items if isinstance(x, dict)]

        inner_next = inner.get("next")
        nxt = inner_next if inner_next is not None else top_next
        return items, (int(nxt) if nxt is not None else None)

    return [], None

# -----------------------------
# Fetch entity fields from Bitrix
# -----------------------------
def fetch_deal_fields() -> Dict[str, Any]:
    data = b24.call("crm.deal.fields")
    res = data.get("result", {})
    return res if isinstance(res, dict) else {}

def fetch_contact_fields() -> Dict[str, Any]:
    data = b24.call("crm.contact.fields")
    res = data.get("result", {})
    return res if isinstance(res, dict) else {}

def fetch_lead_fields() -> Dict[str, Any]:
    data = b24.call("crm.lead.fields")
    res = data.get("result", {})
    return res if isinstance(res, dict) else {}


def fetch_company_fields() -> Dict[str, Any]:
    """Поля компании из Bitrix (crm.company.fields) для nested_fields в entity-meta-fields."""
    data = b24.call("crm.company.fields")
    res = data.get("result", {})
    return res if isinstance(res, dict) else {}


def fetch_smart_process_types() -> List[Dict[str, Any]]:
    data = b24.call("crm.type.list", {"select": ["id", "title", "entityTypeId"]})
    res = data.get("result", {})

    if isinstance(res, dict):
        items = res.get("types")
        if isinstance(items, list):
            return items
    if isinstance(res, list):
        return res
    return []

def fetch_smart_fields(entity_type_id: int) -> Dict[str, Any]:
    """Поля смарт-процесса. Bitrix может вернуть result.fields или result = { fieldName: meta }."""
    data = b24.call("crm.item.fields", {"entityTypeId": entity_type_id})
    res = data.get("result")
    if not isinstance(res, dict):
        return {}
    # Часто result = { "fields": { "ufCrm...": {...} } }, иногда result сам по себе — словарь полей
    return res.get("fields") if "fields" in res else res

# -----------------------------
# Bitrix list data (for insert)
# -----------------------------
def b24_list_deals(
    start_id: int = 0,
    start_offset: int = 0,
    filter_params: Optional[Dict[str, Any]] = None,
    uf_fields: Optional[List[str]] = None,
    order: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Два режима:
      1) Инкремент по ID: start_id>0, start_offset=0, order={"ID":"ASC"}, filter {">ID":start_id}
      2) Today-pass по DATE_MODIFY: start_id=0, start_offset=next, order={"DATE_MODIFY":"ASC","ID":"ASC"}, filter {">=DATE_MODIFY": "..."}
    """

    select_list = ["*"]

    # Пытаемся получить имя ответственного (если Bitrix вернет)
    for x in ("ASSIGNED_BY_NAME", "assigned_by_name", "ASSIGNED_BY", "ASSIGNED_BY.*"):
        if x not in select_list:
            select_list.append(x)

    if uf_fields:
        select_list.extend(uf_fields)
    else:
        # может работать/не работать — но у тебя есть явный список UF из meta_fields
        select_list.append("UF_*")

    params = {
        "select": select_list,
        "start": int(start_offset),
        "order": order or {"ID": "ASC"},
    }

    filter_dict = {}
    if start_id > 0:
        filter_dict[">ID"] = start_id
    if filter_params:
        filter_dict.update(filter_params)
    if filter_dict:
        params["filter"] = filter_dict

    resp = b24.call("crm.deal.list", params)

    if resp and "error" in resp and resp.get("error") == "OVERLOAD_LIMIT":
        print(f"WARNING: b24_list_deals: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
        return {"result": []}

    return resp

def b24_list_smart_items(entity_type_id: int, last_id: int = 0) -> Dict[str, Any]:
    """
    Получает смарт-процессы из Bitrix с оптимизацией (start=-1 + фильтр >ID).
    Использует рекомендацию Bitrix24 для оптимизации производительности.
    """
    params = {
        "entityTypeId": entity_type_id,
        "select": ["*"],
        "start": last_id,
        "order": {"id": "ASC"}
    }
    if last_id > 0:
        params["filter"] = {">id": last_id}
    print(f"DEBUG: b24_list_smart_items: Request params: entityTypeId={entity_type_id}, start={params['start']}, filter={params.get('filter')}", file=sys.stderr, flush=True)
    
    resp = b24.call("crm.item.list", params)
    
    # Проверяем, не заблокирован ли API
    if resp and "error" in resp and resp.get("error") == "OVERLOAD_LIMIT":
        print(f"WARNING: b24_list_smart_items: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
        return {"result": []}
    
    return resp

def b24_list_contacts(start: int = 0, filter_params: Optional[Dict[str, Any]] = None, uf_fields: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Получает контакты из Bitrix с оптимизацией (start=-1 + фильтр >ID).
    """
    select_list = ["*"]
    if uf_fields:
        select_list.extend(uf_fields)
    
    params = {
        "select": select_list,
        "start": start,
        "order": {"ID": "ASC"}
    }
    
    filter_dict = {}
    if start > 0:
        filter_dict[">ID"] = start
    if filter_params:
        filter_dict.update(filter_params)
    if filter_dict:
        params["filter"] = filter_dict
    
    print(f"DEBUG: b24_list_contacts: Request params: select={select_list[:5]}..., start={params['start']}, filter={params.get('filter')}", file=sys.stderr, flush=True)
    
    resp = b24.call("crm.contact.list", params)
    
    # Проверяем, не заблокирован ли API
    if resp and "error" in resp and resp.get("error") == "OVERLOAD_LIMIT":
        print(f"WARNING: b24_list_contacts: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
        return {"result": []}
    
    return resp

def b24_list_leads(start: int = 0, filter_params: Optional[Dict[str, Any]] = None, uf_fields: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Получает лиды из Bitrix с оптимизацией (start=-1 + фильтр >ID).
    """
    select_list = ["*"]
    if uf_fields:
        select_list.extend(uf_fields)
    
    params = {
        "select": select_list,
        "start": start,
        "order": {"ID": "ASC"}
    }
    
    filter_dict = {}
    if start > 0:
        filter_dict[">ID"] = start
    if filter_params:
        filter_dict.update(filter_params)
    if filter_dict:
        params["filter"] = filter_dict
    
    print(f"DEBUG: b24_list_leads: Request params: select={select_list[:5]}..., start={params['start']}, filter={params.get('filter')}", file=sys.stderr, flush=True)
    
    resp = b24.call("crm.lead.list", params)
    
    # Проверяем, не заблокирован ли API
    if resp and "error" in resp and resp.get("error") == "OVERLOAD_LIMIT":
        print(f"WARNING: b24_list_leads: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
        return {"result": []}
    
    return resp


def b24_list_companies(start: int = 0, filter_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Получает компании из Bitrix (crm.company.list) для справочника id -> title.
    """
    params = {
        "select": ["*"],
        "start": start,
        "order": {"ID": "ASC"},
    }
    filter_dict = {}
    if start > 0:
        filter_dict[">ID"] = start
    if filter_params:
        filter_dict.update(filter_params)
    if filter_dict:
        params["filter"] = filter_dict
    resp = b24.call("crm.company.list", params)
    if resp and "error" in resp and resp.get("error") == "OVERLOAD_LIMIT":
        print("WARNING: b24_list_companies: API blocked (OVERLOAD_LIMIT), returning empty result", file=sys.stderr, flush=True)
        return {"result": []}
    return resp


def sync_companies(conn, time_budget_sec: int = 300) -> int:
    """
    Синхронизирует справочник компаний из Bitrix в b24_crm_company (id, title, raw).
    raw — полный объект компании для отдачи полей в entity-meta-data.
    """
    total = 0
    start = 0
    begun = time.time()
    while time.time() - begun < time_budget_sec:
        resp = b24_list_companies(start=start, filter_params=None)
        items, nxt = normalize_list_result(resp)
        if not items:
            break
        rows: List[Tuple[int, str, Any]] = []
        for it in items:
            cid = it.get("ID") or it.get("id")
            if cid is None:
                continue
            title = it.get("TITLE") or it.get("title") or ""
            rows.append((int(cid), (title or str(cid)).strip(), Json(it)))
        if rows:
            try:
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO b24_crm_company (id, title, raw)
                        VALUES %s
                        ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, raw = EXCLUDED.raw, updated_at = now()
                        """,
                        rows,
                        page_size=200,
                    )
                conn.commit()
                total += len(rows)
            except Exception as e:
                conn.rollback()
                print(f"WARNING: sync_companies insert: {e}", file=sys.stderr, flush=True)
                break
        if nxt is None:
            break
        start = int(nxt)
    if total:
        print(f"INFO: sync_companies: synced {total} companies", file=sys.stderr, flush=True)
    return total


# -----------------------------
# Main schema sync
# -----------------------------
def sync_schema() -> Dict[str, Any]:
    conn = pg_conn()
    try:
        ensure_meta_tables(conn)

        deal_fields = fetch_deal_fields()
        deal_entity = {"entity_key": "deal", "entity_kind": "deal", "title": "CRM Deal", "entity_type_id": None}
        upsert_meta_entities(conn, [deal_entity])

        existing_cols = set(["id", "raw", "created_at", "updated_at"])
        deal_colmap: Dict[str, str] = {}
        for f in deal_fields.keys():
            base = sanitize_ident(f)
            deal_colmap[f] = unique_column_name(existing_cols, base)

        deal_table = table_name_for_entity("deal")
        ensure_table_base(conn, deal_table)

        deal_columns: List[Tuple[str, str]] = []
        for b24_field, meta in deal_fields.items():
            pgtype = map_b24_to_pg_type(meta.get("type"), bool(meta.get("isMultiple", False)))
            deal_columns.append((deal_colmap[b24_field], pgtype))

        ensure_columns(conn, deal_table, deal_columns)
        upsert_meta_fields(conn, "deal", deal_fields, deal_colmap)
        sync_userfield_titles(conn, "deal")

        # Синхронизируем контакты
        contact_fields = fetch_contact_fields()
        contact_entity = {"entity_key": "contact", "entity_kind": "contact", "title": "CRM Contact", "entity_type_id": None}
        upsert_meta_entities(conn, [contact_entity])
        
        existing_cols_contact = set(["id", "raw", "created_at", "updated_at"])
        contact_colmap: Dict[str, str] = {}
        for f in contact_fields.keys():
            base = sanitize_ident(f)
            contact_colmap[f] = unique_column_name(existing_cols_contact, base)
        
        contact_table = table_name_for_entity("contact")
        ensure_table_base(conn, contact_table)
        
        contact_columns: List[Tuple[str, str]] = []
        for b24_field, meta in contact_fields.items():
            pgtype = map_b24_to_pg_type(meta.get("type"), bool(meta.get("isMultiple", False)))
            contact_columns.append((contact_colmap[b24_field], pgtype))
        
        ensure_columns(conn, contact_table, contact_columns)
        upsert_meta_fields(conn, "contact", contact_fields, contact_colmap)
        sync_userfield_titles(conn, "contact")

        # Синхронизируем лиды
        lead_fields = fetch_lead_fields()
        lead_entity = {"entity_key": "lead", "entity_kind": "lead", "title": "CRM Lead", "entity_type_id": None}
        upsert_meta_entities(conn, [lead_entity])
        
        existing_cols_lead = set(["id", "raw", "created_at", "updated_at"])
        lead_colmap: Dict[str, str] = {}
        for f in lead_fields.keys():
            base = sanitize_ident(f)
            lead_colmap[f] = unique_column_name(existing_cols_lead, base)
        
        lead_table = table_name_for_entity("lead")
        ensure_table_base(conn, lead_table)
        
        lead_columns: List[Tuple[str, str]] = []
        for b24_field, meta in lead_fields.items():
            pgtype = map_b24_to_pg_type(meta.get("type"), bool(meta.get("isMultiple", False)))
            lead_columns.append((lead_colmap[b24_field], pgtype))
        
        ensure_columns(conn, lead_table, lead_columns)
        upsert_meta_fields(conn, "lead", lead_fields, lead_colmap)
        sync_userfield_titles(conn, "lead")

        # Метаполя компании (для nested_fields в entity-meta-fields; данные компаний — в b24_crm_company)
        company_fields = fetch_company_fields()
        if company_fields:
            company_entity = {"entity_key": "company", "entity_kind": "company", "title": "CRM Company", "entity_type_id": None}
            upsert_meta_entities(conn, [company_entity])
            existing_cols_company = set(["id", "raw", "created_at", "updated_at"])
            company_colmap: Dict[str, str] = {}
            for f in company_fields.keys():
                base = sanitize_ident(f)
                company_colmap[f] = unique_column_name(existing_cols_company, base)
            upsert_meta_fields(conn, "company", company_fields, company_colmap)
            sync_userfield_titles(conn, "company")

        # Синхронизируем классификатор источников: стандартные SOURCE + пользовательское поле Sursa
        sync_sources_from_status(conn)
        sync_sources_classifier(conn)

        types = fetch_smart_process_types()
        smart_entities: List[Dict[str, Any]] = []
        smart_results: List[Dict[str, Any]] = []

        for t in types:
            etid = t.get("entityTypeId") or t.get("ENTITY_TYPE_ID") or t.get("entity_type_id")
            if not etid:
                continue
            etid = int(etid)
            entity_key = f"sp:{etid}"
            smart_entities.append({
                "entity_key": entity_key,
                "entity_kind": "smart_process",
                "title": t.get("title") or t.get("TITLE") or f"SmartProcess {etid}",
                "entity_type_id": etid
            })

        if smart_entities:
            upsert_meta_entities(conn, smart_entities)

        for e in smart_entities:
            entity_key = e["entity_key"]
            etid = e["entity_type_id"]
            fields = fetch_smart_fields(etid)

            existing_cols = set(["id", "raw", "created_at", "updated_at"])
            colmap: Dict[str, str] = {}
            for f in fields.keys():
                base = sanitize_ident(f)
                colmap[f] = unique_column_name(existing_cols, base)

            table = table_name_for_entity(entity_key)
            ensure_table_base(conn, table)

            cols: List[Tuple[str, str]] = []
            for b24_field, meta in fields.items():
                pgtype = map_b24_to_pg_type(meta.get("type"), bool(meta.get("isMultiple", False)))
                cols.append((colmap[b24_field], pgtype))

            ensure_columns(conn, table, cols)
            upsert_meta_fields(conn, entity_key, fields, colmap)

            smart_results.append({"entity_key": entity_key, "table": table, "fields_count": len(fields)})

        out = {
            "ok": True,
            "deal": {"table": deal_table, "fields_count": len(deal_fields)},
            "contact": {"table": contact_table, "fields_count": len(contact_fields)},
            "lead": {"table": lead_table, "fields_count": len(lead_fields)},
            "smart_processes": {"count": len(smart_entities), "items": smart_results[:50]}
        }
        if company_fields:
            out["company"] = {"fields_count": len(company_fields)}
        return out
    finally:
        conn.close()

# -----------------------------
# Data sync (UPSERT) with cursor + time budget
# -----------------------------
def _is_unlimited(limit: int) -> bool:
    return limit <= 0

# Кэш для имен пользователей (чтобы не делать повторные запросы к Bitrix)
_user_name_cache: Dict[str, str] = {}

def sync_entity_data_deal(conn, limit: int, time_budget_sec: int) -> Dict[str, Any]:
    entity_key = "deal"
    table = table_name_for_entity(entity_key)

    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})

    # Проверяем, есть ли колонка assigned_by_name в таблице (опционально)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = 'assigned_by_name'
        """, (table,))
        has_assigned_by_name_col = cur.fetchone() is not None
    if has_assigned_by_name_col and "assigned_by_name" not in col_order:
        col_order.append("assigned_by_name")

    # Загружаем UF поля из меты (лучше чем UF_*)
    uf_fields: List[str] = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b24_field
                FROM b24_meta_fields
                WHERE entity_key = %s
                  AND b24_field ILIKE 'uf_%%'
            """, (entity_key,))
            uf_fields = [str(r[0]) for r in cur.fetchall() if r and r[0]]
    except Exception as e:
        print(f"WARNING: sync_entity_data_deal: Failed to load UF fields: {e}", file=sys.stderr, flush=True)
        uf_fields = []

    # -------- helpers: собрать row (общая логика) --------
    def build_row_from_item(it: Dict[str, Any]) -> Optional[List[Any]]:
        deal_id = it.get("ID") or it.get("id")
        if not deal_id:
            return None

        row = {c: None for c in col_order}
        row["id"] = int(deal_id)
        row["raw"] = Json(it)

        # обычные поля по colmap
        for b24_field, meta in colmap.items():
            col = meta["column_name"]
            b24_type = meta.get("b24_type")
            is_multiple = meta.get("is_multiple", False)

            value = None
            if b24_field in it:
                value = it[b24_field]
            elif isinstance(it.get("fields"), dict) and b24_field in it["fields"]:
                value = it["fields"][b24_field]
            elif b24_field.upper() in it:
                value = it[b24_field.upper()]
            elif b24_field.lower() in it:
                value = it[b24_field.lower()]
            elif isinstance(it.get("fields"), dict) and b24_field.upper() in it["fields"]:
                value = it["fields"][b24_field.upper()]
            elif isinstance(it.get("fields"), dict) and b24_field.lower() in it["fields"]:
                value = it["fields"][b24_field.lower()]

            if value is not None:
                row[col] = normalize_value(value, b24_type, is_multiple)

        # assigned_by_name — берём только если Bitrix прислал (не долбим user.get лишний раз)
        if has_assigned_by_name_col and "assigned_by_name" in col_order:
            v = None
            if "ASSIGNED_BY_NAME" in it:
                v = it.get("ASSIGNED_BY_NAME")
            elif "assigned_by_name" in it:
                v = it.get("assigned_by_name")
            # иногда ASSIGNED_BY объект
            elif isinstance(it.get("ASSIGNED_BY"), dict):
                u = it["ASSIGNED_BY"]
                n = (u.get("NAME") or "").strip()
                ln = (u.get("LAST_NAME") or "").strip()
                v = (f"{n} {ln}".strip() or u.get("FULL_NAME") or None)

            row["assigned_by_name"] = (str(v).strip() if v else None)

        return [row.get(c) for c in col_order]

    # -------- 1) Инкремент: новые сделки по >ID (100% новых) --------
    total = 0
    last_id = validate_sync_cursor(conn, entity_key, table)
    started = time.time()

    while True:
        if time.time() - started >= time_budget_sec:
            break

        resp = b24_list_deals(
            start_id=last_id,
            start_offset=0,
            filter_params=None,
            uf_fields=uf_fields,
            order={"ID": "ASC"}
        )
        items, _ = normalize_list_result(resp)

        if not items:
            set_sync_cursor(conn, entity_key, last_id if last_id > 0 else 0)
            break

        batch_rows: List[List[Any]] = []
        max_seen = last_id

        for it in items:
            r = build_row_from_item(it)
            if not r:
                continue
            batch_rows.append(r)
            deal_id_val = r[col_order.index("id")]
            if deal_id_val and int(deal_id_val) > int(max_seen):
                max_seen = int(deal_id_val)

        if batch_rows:
            upsert_rows(conn, table, col_order, batch_rows)
            total += len(batch_rows)

        last_id = int(max_seen) if max_seen else last_id
        set_sync_cursor(conn, entity_key, last_id)

        if (not _is_unlimited(limit)) and total >= limit:
            break

        # Если Bitrix вернул “короткую” пачку — вероятно, новых больше нет, выходим
        if len(items) < 50:
            break

    # -------- 2) Today-pass: все сделки изменённые сегодня (100% актуальность дня) --------
    # Экономим запросы: ограничиваем число страниц за один запуск (если сегодня изменили очень много)
    tz_name = os.getenv("B24_TZ", "Europe/Chisinau")
    dt_from_utc = day_start_utc(tz_name)
    dt_from_str = dt_from_utc.isoformat()

    max_pages = int(os.getenv("DEAL_TODAY_MAX_PAGES", "10"))  # безопасный лимит
    page = 0
    offset = 0

    while True:
        if time.time() - started >= time_budget_sec:
            break
        if page >= max_pages:
            print(f"INFO: sync_entity_data_deal: today-pass reached max_pages={max_pages}, stop early to protect API", file=sys.stderr, flush=True)
            break

        resp2 = b24_list_deals(
            start_id=0,
            start_offset=offset,
            filter_params={">=DATE_MODIFY": dt_from_str},
            uf_fields=uf_fields,
            order={"DATE_MODIFY": "ASC", "ID": "ASC"},
        )
        items2, nxt2 = normalize_list_result(resp2)

        if not items2:
            break

        batch_rows2: List[List[Any]] = []
        for it in items2:
            r = build_row_from_item(it)
            if r:
                batch_rows2.append(r)

        if batch_rows2:
            upsert_rows(conn, table, col_order, batch_rows2)
            total += len(batch_rows2)

        page += 1
        if nxt2 is None:
            break
        offset = int(nxt2)

    return {"entity": "deal", "table": table, "rows_upserted": total, "cursor_now": get_sync_cursor(conn, entity_key)}

def sync_entity_data_contact(conn, limit: int, time_budget_sec: int) -> Dict[str, Any]:
    """Синхронизация данных контактов из Bitrix"""
    entity_key = "contact"
    table = table_name_for_entity(entity_key)
    
    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})
    
    # Получаем список UF полей
    uf_fields = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b24_field
                FROM b24_meta_fields
                WHERE entity_key = %s 
                  AND b24_field ILIKE 'uf_%%'
            """, (entity_key,))
            rows = cur.fetchall()
            for row in rows:
                if row and len(row) > 0:
                    uf_fields.append(str(row[0]))
    except Exception as e:
        print(f"WARNING: Failed to load UF fields for contacts: {e}", file=sys.stderr, flush=True)
        uf_fields = []
    
    total = 0
    # Валидируем курсор (offset пагинация через start/next)
    last_offset = validate_sync_cursor(conn, entity_key, table)
    
    started = time.time()
    while True:
        if time.time() - started >= time_budget_sec:
            break
        
        resp = b24_list_contacts(start=last_offset, filter_params=None, uf_fields=uf_fields)
        items, nxt = normalize_list_result(resp)
        
        if not items:
            set_sync_cursor(conn, entity_key, last_offset if last_offset > 0 else 0)
            break
        
        rows = []
        for it in items:
            contact_id = it.get("ID") or it.get("id")
            if not contact_id:
                continue
            
            row = {c: None for c in col_order}
            row["id"] = int(contact_id)
            row["raw"] = Json(it)
            
            for b24_field, meta in colmap.items():
                col = meta["column_name"]
                b24_type = meta.get("b24_type")
                is_multiple = meta.get("is_multiple", False)
                
                value = None
                if b24_field in it:
                    value = it[b24_field]
                elif b24_field.upper() in it:
                    value = it[b24_field.upper()]
                elif b24_field.lower() in it:
                    value = it[b24_field.lower()]
                
                if value is not None:
                    row[col] = normalize_value(value, b24_type, is_multiple)
            
            row_values = [row.get(c) for c in col_order]
            rows.append(row_values)
        
        upsert_rows(conn, table, col_order, rows)
        total += len(rows)
        
        # Пагинация через start/next
        if nxt is not None:
            last_offset = nxt
            set_sync_cursor(conn, entity_key, last_offset)
        else:
            last_seen_id = rows[-1][col_order.index("id")] if rows else last_offset
            set_sync_cursor(conn, entity_key, last_seen_id if last_seen_id else last_offset)
            break
        
        if (not _is_unlimited(limit)) and total >= limit:
            break
    
    return {"entity": "contact", "table": table, "rows_upserted": total, "cursor_now": get_sync_cursor(conn, entity_key)}

def sync_entity_data_lead(conn, limit: int, time_budget_sec: int) -> Dict[str, Any]:
    """Синхронизация данных лидов из Bitrix"""
    entity_key = "lead"
    table = table_name_for_entity(entity_key)
    
    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})
    
    # Получаем список UF полей
    uf_fields = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT b24_field
                FROM b24_meta_fields
                WHERE entity_key = %s 
                  AND b24_field ILIKE 'uf_%%'
            """, (entity_key,))
            rows = cur.fetchall()
            for row in rows:
                if row and len(row) > 0:
                    uf_fields.append(str(row[0]))
    except Exception as e:
        print(f"WARNING: Failed to load UF fields for leads: {e}", file=sys.stderr, flush=True)
        uf_fields = []
    
    total = 0
    # Валидируем курсор (offset пагинация через start/next)
    last_offset = validate_sync_cursor(conn, entity_key, table)
    
    started = time.time()
    while True:
        if time.time() - started >= time_budget_sec:
            break
        
        resp = b24_list_leads(start=last_offset, filter_params=None, uf_fields=uf_fields)
        items, nxt = normalize_list_result(resp)
        
        if not items:
            set_sync_cursor(conn, entity_key, last_offset if last_offset > 0 else 0)
            break
        
        rows = []
        for it in items:
            lead_id = it.get("ID") or it.get("id")
            if not lead_id:
                continue
            
            row = {c: None for c in col_order}
            row["id"] = int(lead_id)
            row["raw"] = Json(it)
            
            for b24_field, meta in colmap.items():
                col = meta["column_name"]
                b24_type = meta.get("b24_type")
                is_multiple = meta.get("is_multiple", False)
                
                value = None
                if b24_field in it:
                    value = it[b24_field]
                elif b24_field.upper() in it:
                    value = it[b24_field.upper()]
                elif b24_field.lower() in it:
                    value = it[b24_field.lower()]
                
                if value is not None:
                    row[col] = normalize_value(value, b24_type, is_multiple)
            
            row_values = [row.get(c) for c in col_order]
            rows.append(row_values)
        
        upsert_rows(conn, table, col_order, rows)
        total += len(rows)
        
        # Пагинация через start/next
        if nxt is not None:
            last_offset = nxt
            set_sync_cursor(conn, entity_key, last_offset)
        else:
            last_seen_id = rows[-1][col_order.index("id")] if rows else last_offset
            set_sync_cursor(conn, entity_key, last_seen_id if last_seen_id else last_offset)
            break
        
        if (not _is_unlimited(limit)) and total >= limit:
            break
    
    return {"entity": "lead", "table": table, "rows_upserted": total, "cursor_now": get_sync_cursor(conn, entity_key)}

def sync_entity_data_smart(conn, entity_type_id: int, limit: int, time_budget_sec: int) -> Dict[str, Any]:
    entity_key = f"sp:{entity_type_id}"
    table = table_name_for_entity(entity_key)

    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})

    total = 0
    # Валидируем курсор (offset пагинация через start/next)
    last_offset = validate_sync_cursor(conn, entity_key, table)

    started = time.time()
    while True:
        if time.time() - started >= time_budget_sec:
            break

        resp = b24_list_smart_items(entity_type_id, last_id=last_offset)
        items, nxt = normalize_list_result(resp)

        if not items:
            set_sync_cursor(conn, entity_key, last_offset if last_offset > 0 else 0)
            break

        rows = []
        for it in items:
            item_id = it.get("id") or it.get("ID")
            if not item_id:
                continue
            
            row = {c: None for c in col_order}
            row["id"] = int(item_id)
            row["raw"] = Json(it)

            for b24_field, meta in colmap.items():
                col = meta["column_name"]
                b24_type = meta.get("b24_type")
                is_multiple = meta.get("is_multiple", False)
                
                # Просто берем значение из Bitrix и сохраняем в базу
                value = None
                
                # Проверяем в разных местах и регистрах
                if b24_field in it:
                    value = it[b24_field]
                elif isinstance(it.get("fields"), dict) and b24_field in it["fields"]:
                    value = it["fields"][b24_field]
                elif b24_field.upper() in it:
                    value = it[b24_field.upper()]
                elif b24_field.lower() in it:
                    value = it[b24_field.lower()]
                elif isinstance(it.get("fields"), dict) and b24_field.upper() in it["fields"]:
                    value = it["fields"][b24_field.upper()]
                elif isinstance(it.get("fields"), dict) and b24_field.lower() in it["fields"]:
                    value = it["fields"][b24_field.lower()]
                
                if value is not None:
                    row[col] = normalize_value(value, b24_type, is_multiple)

            rows.append([row[c] for c in col_order])

        upsert_rows(conn, table, col_order, rows)
        total += len(rows)

        # Пагинация через start/next
        if nxt is not None:
            last_offset = nxt
            set_sync_cursor(conn, entity_key, last_offset)
        else:
            last_seen_id = rows[-1][col_order.index("id")] if rows else last_offset
            set_sync_cursor(conn, entity_key, last_seen_id if last_seen_id else last_offset)
            break
        
        if (not _is_unlimited(limit)) and total >= limit:
            break
    
    return {"entity": entity_key, "table": table, "rows_upserted": total, "cursor_now": get_sync_cursor(conn, entity_key)}



def sync_data(deal_limit: int, smart_limit: int, time_budget_sec: int, contact_limit: int = 0, lead_limit: int = 0) -> Dict[str, Any]:
    conn = pg_conn()
    try:
        ensure_meta_tables(conn)
        
        # Проверяем, существуют ли таблицы для контактов и лидов
        # Если нет - автоматически создаем их через sync_schema()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                  AND table_name IN ('b24_crm_contact', 'b24_crm_lead')
            """)
            existing_tables = {row[0] for row in cur.fetchall()}
            
            if 'b24_crm_contact' not in existing_tables or 'b24_crm_lead' not in existing_tables:
                print("INFO: sync_data: Tables for contacts/leads not found, running sync_schema() automatically...", file=sys.stderr, flush=True)
                try:
                    sync_schema()
                    print("INFO: sync_data: sync_schema() completed successfully", file=sys.stderr, flush=True)
                except Exception as e:
                    print(f"WARNING: sync_data: sync_schema() failed: {e}", file=sys.stderr, flush=True)
                    traceback.print_exc()

        with conn.cursor() as cur:
            cur.execute("""
                SELECT entity_type_id
                FROM b24_meta_entities
                WHERE entity_kind = 'smart_process'
                ORDER BY entity_type_id
            """)
            smart_ids = [r[0] for r in cur.fetchall() if r[0] is not None]

        # split time budget: deals get 30%, contacts and leads get 20% each, smart share the rest (30%)
        t0 = max(1, int(time_budget_sec * 0.3))  # deals
        t1 = max(1, int(time_budget_sec * 0.2))  # contacts
        t2 = max(1, int(time_budget_sec * 0.2))  # leads
        t_rest = max(1, time_budget_sec - t0 - t1 - t2)  # smart processes
        per_smart = max(1, t_rest // max(1, len(smart_ids)))

        # Синхронизируем ВСЕ сделки (без фильтрации по статусу)
        deal_res = sync_entity_data_deal(conn, limit=deal_limit, time_budget_sec=t0)
        
        # Синхронизируем контакты
        contact_res = sync_entity_data_contact(conn, limit=contact_limit, time_budget_sec=t1)
        
        # Синхронизируем лиды
        lead_res = sync_entity_data_lead(conn, limit=lead_limit, time_budget_sec=t2)
        
        # Синхронизируем смарт-процессы
        smart_res = [sync_entity_data_smart(conn, int(etid), limit=smart_limit, time_budget_sec=per_smart) for etid in smart_ids]
        
        # Автоматически обновляем классификатор источников после синхронизации сделок
        # Это дополняет классификатор новыми источниками из сделок
        try:
            sync_sources_classifier(conn)
            print("INFO: sync_data: Sources classifier updated automatically", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"WARNING: sync_data: Failed to update sources classifier: {e}", file=sys.stderr, flush=True)
            traceback.print_exc()

        return {
            "ok": True, 
            "deal": deal_res, 
            "contact": contact_res,
            "lead": lead_res,
            "smart_processes": smart_res
        }
    finally:
        conn.close()

# -----------------------------
# Background auto-sync every 30 seconds
# -----------------------------
_sync_lock = threading.Lock()
_last_full_update_time = 0
FULL_UPDATE_INTERVAL_SEC = int(os.getenv("FULL_UPDATE_INTERVAL_SEC", "3600"))  # 1 час по умолчанию

def background_loop():
    global _last_full_update_time
    while True:
        if AUTO_SYNC_ENABLED:
            # Проверяем время - не синхронизируем с 00:00 до 06:00
            current_time_obj = datetime.now()
            current_hour = current_time_obj.hour
            # Пропускаем синхронизацию с 00:00 до 06:00 (включительно)
            if 0 <= current_hour < 6:
                print(f"INFO: background_loop: Skipping sync (night time: {current_hour:02d}:00)", file=sys.stderr, flush=True)
                time.sleep(AUTO_SYNC_INTERVAL_SEC)
                continue
            
            if _sync_lock.acquire(blocking=False):
                try:
                    # Обычная инкрементальная синхронизация
                    res_data = sync_data(
                        deal_limit=AUTO_SYNC_DEAL_LIMIT,
                        smart_limit=AUTO_SYNC_SMART_LIMIT,
                        time_budget_sec=SYNC_TIME_BUDGET_SEC,
                        contact_limit=AUTO_SYNC_CONTACT_LIMIT,
                        lead_limit=AUTO_SYNC_LEAD_LIMIT
                    )
                    print(
                        "AUTO SYNC OK:",
                        {
                            "deal": res_data.get("deal"),
                            "smart_sample": (res_data.get("smart_processes") or [])[:2],
                        },
                        flush=True
                    )
                    
                    # Периодически (раз в FULL_UPDATE_INTERVAL_SEC) обновляем assigned_by_name и справочники
                    current_time = time.time()
                    if current_time - _last_full_update_time >= FULL_UPDATE_INTERVAL_SEC:
                        # Справочники (воронки, стадии, enum) — подтягиваем новые значения из Bitrix
                        try:
                            print("INFO: background_loop: Running periodic sync reference data...", file=sys.stderr, flush=True)
                            run_sync_reference_data()
                        except Exception as e:
                            print(f"WARNING: background_loop: reference data sync failed: {e}", file=sys.stderr, flush=True)
                        print("INFO: background_loop: Starting periodic update of assigned_by_name", file=sys.stderr, flush=True)
                        try:
                            conn = pg_conn()
                            try:
                                table = table_name_for_entity("deal")
                                global _user_name_cache
                                _user_name_cache.clear()
                                
                                # Получаем список сделок без assigned_by_name (ограничиваем до 500 за раз)
                                with conn.cursor() as cur:
                                    cur.execute(f"""
                                        SELECT id, assigned_by_id
                                        FROM {table}
                                        WHERE assigned_by_id IS NOT NULL
                                          AND assigned_by_name IS NULL
                                        ORDER BY id DESC
                                        LIMIT 500
                                    """)
                                    deals_to_update = cur.fetchall()
                                
                                if deals_to_update:
                                    updated = 0
                                    for deal_id, assigned_by_id in deals_to_update:
                                        user_id_str = str(assigned_by_id).strip()
                                        
                                        if user_id_str in _user_name_cache:
                                            assigned_by_name = _user_name_cache[user_id_str]
                                        else:
                                            try:
                                                user_resp = b24.call("user.get", {"ID": user_id_str})
                                                if user_resp and "result" in user_resp and len(user_resp["result"]) > 0:
                                                    user = user_resp["result"][0]
                                                    name = user.get("NAME", "").strip()
                                                    last_name = user.get("LAST_NAME", "").strip()
                                                    if name and last_name:
                                                        assigned_by_name = f"{name} {last_name}"
                                                    elif name:
                                                        assigned_by_name = name
                                                    elif last_name:
                                                        assigned_by_name = last_name
                                                    elif user.get("FULL_NAME"):
                                                        assigned_by_name = str(user.get("FULL_NAME")).strip()
                                                    elif user.get("LOGIN"):
                                                        assigned_by_name = str(user.get("LOGIN")).strip()
                                                    else:
                                                        assigned_by_name = None
                                                    _user_name_cache[user_id_str] = assigned_by_name or user_id_str
                                                else:
                                                    assigned_by_name = None
                                                    _user_name_cache[user_id_str] = user_id_str
                                            except Exception as e:
                                                print(f"WARNING: Failed to get user name for deal {deal_id}: {e}", file=sys.stderr, flush=True)
                                                assigned_by_name = None
                                                _user_name_cache[user_id_str] = user_id_str
                                        
                                        if assigned_by_name and assigned_by_name != user_id_str:
                                            with conn.cursor() as cur:
                                                cur.execute(f"""
                                                    UPDATE {table}
                                                    SET assigned_by_name = %s
                                                    WHERE id = %s
                                                """, (assigned_by_name, deal_id))
                                                conn.commit()
                                                updated += 1
                                            try:
                                                _upsert_b24_user(conn, int(assigned_by_id), assigned_by_name)
                                            except Exception:
                                                pass
                                    
                                    print(f"INFO: background_loop: Updated {updated} deals with assigned_by_name", file=sys.stderr, flush=True)
                                else:
                                    print("INFO: background_loop: No deals need assigned_by_name update", file=sys.stderr, flush=True)
                                
                                _last_full_update_time = current_time
                            finally:
                                conn.close()
                        except Exception as e:
                            print(f"ERROR: background_loop: Failed to update assigned_by_name: {e}", file=sys.stderr, flush=True)
                            traceback.print_exc()
                except Exception:
                    traceback.print_exc()
                finally:
                    _sync_lock.release()
        time.sleep(AUTO_SYNC_INTERVAL_SEC)

def _initial_sync_thread():
    """Запускает начальную синхронизацию в отдельном потоке, чтобы не блокировать старт сервиса."""
    # Небольшая задержка, чтобы сервис успел запуститься
    time.sleep(2)
    print("INFO: _initial_sync_thread: Starting initial sync from Bitrix...", file=sys.stderr, flush=True)
    try:
        # Сначала синхронизируем схему (создаем таблицы и метаданные для всех сущностей)
        print("INFO: _initial_sync_thread: Running sync_schema() first to ensure all tables exist...", file=sys.stderr, flush=True)
        try:
            schema_result = sync_schema()
            print(f"INFO: _initial_sync_thread: sync_schema() completed: {schema_result}", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"WARNING: _initial_sync_thread: sync_schema() failed (will continue anyway): {e}", file=sys.stderr, flush=True)
            traceback.print_exc()

        # Справочники (воронки, стадии, enum) — чтобы entity-meta-data сразу показывал названия
        try:
            print("INFO: _initial_sync_thread: Running sync reference data (categories, stages, field enums)...", file=sys.stderr, flush=True)
            run_sync_reference_data()
            print("INFO: _initial_sync_thread: Reference data sync completed", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"WARNING: _initial_sync_thread: reference data sync failed (will continue): {e}", file=sys.stderr, flush=True)
        
        # Затем синхронизируем данные с увеличенным time_budget для начальной загрузки
        initial_sync_result = sync_data(
            deal_limit=0,  # Без ограничений для начальной синхронизации
            smart_limit=0,
            time_budget_sec=300,  # 5 минут для начальной синхронизации
            contact_limit=0,  # Без ограничений для начальной синхронизации
            lead_limit=0  # Без ограничений для начальной синхронизации
        )
        print(f"INFO: _initial_sync_thread: Initial sync completed: {initial_sync_result}", file=sys.stderr, flush=True)
    except Exception as e:
        print(f"WARNING: _initial_sync_thread: Initial sync failed: {e}", file=sys.stderr, flush=True)
        traceback.print_exc()


# -----------------------------
# WEBHOOK-ONLY MODE (outbound Bitrix events)
# -----------------------------
WEBHOOK_ONLY = os.getenv("WEBHOOK_ONLY", "0") == "1"

def logi(msg: str):
    print(msg, file=sys.stderr, flush=True)

def ensure_webhook_queue_schema() -> None:
    """
    Ensure queue table exists and has the columns we need.
    Matches your current schema (received_at, processed_at, etc.) and adds missing columns safely.
    """
    conn = None
    try:
        conn = pg_conn()
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.b24_webhook_queue (
            id bigserial PRIMARY KEY,
            entity_key text NOT NULL,
            entity_id bigint NOT NULL,
            event text,
            received_at timestamptz NOT NULL DEFAULT now(),
            processed_at timestamptz,
            status text NOT NULL DEFAULT 'new',
            attempts int NOT NULL DEFAULT 0,
            last_error text,
            event_name text,
            payload jsonb,
            next_run_at timestamptz DEFAULT now(),
            created_at timestamptz DEFAULT now()
        );
        """)
        # add columns if table already existed
        cur.execute("ALTER TABLE public.b24_webhook_queue ADD COLUMN IF NOT EXISTS created_at timestamptz;")
        cur.execute("ALTER TABLE public.b24_webhook_queue ADD COLUMN IF NOT EXISTS event_name text;")
        cur.execute("ALTER TABLE public.b24_webhook_queue ADD COLUMN IF NOT EXISTS payload jsonb;")
        cur.execute("ALTER TABLE public.b24_webhook_queue ADD COLUMN IF NOT EXISTS next_run_at timestamptz;")
        # defaults (safe)
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN received_at SET DEFAULT now();")
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN created_at SET DEFAULT now();")
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN next_run_at SET DEFAULT now();")
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN status SET DEFAULT 'new';")
        cur.execute("ALTER TABLE public.b24_webhook_queue ALTER COLUMN attempts SET DEFAULT 0;")
        cur.close()
    except Exception as e:
        logi(f"ERROR: ensure_webhook_queue_schema: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

def _extract_int(v) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        if isinstance(v, int):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None

def _guess_entity_from_event(event_name: str, payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[int], str]:
    """
    Returns (entity_key, entity_id, normalized_event_name)
    entity_key examples: 'deal', 'contact', 'lead', 'sp:1164'
    """
    en = (event_name or payload.get("event") or payload.get("event_name") or payload.get("EVENT_NAME") or "").strip()
    en_up = en.upper()

    # try find ID
    def pick_id() -> Optional[int]:
        # direct fields
        for k in ("id", "ID", "entity_id", "ENTITY_ID"):
            x = _extract_int(payload.get(k))
            if x:
                return x
        # common Bitrix outbound shape: data / FIELDS
        data = payload.get("data") or payload.get("DATA")
        if isinstance(data, dict):
            fields = data.get("FIELDS") or data.get("fields") or data
            if isinstance(fields, dict):
                x = _extract_int(fields.get("ID") or fields.get("id"))
                if x:
                    return x
        fields = payload.get("FIELDS") or payload.get("fields")
        if isinstance(fields, dict):
            x = _extract_int(fields.get("ID") or fields.get("id"))
            if x:
                return x
        return None

    entity_id = pick_id()

    # smart process entityTypeId
    entity_type_id = None
    for k in ("entityTypeId", "ENTITY_TYPE_ID", "entity_type_id", "ENTITYTYPEID"):
        entity_type_id = _extract_int(payload.get(k))
        if entity_type_id:
            break
    if not entity_type_id:
        data = payload.get("data") or payload.get("DATA")
        if isinstance(data, dict):
            entity_type_id = _extract_int(data.get("entityTypeId") or data.get("ENTITY_TYPE_ID") or data.get("ENTITYTYPEID"))

    if "DEAL" in en_up:
        return ("deal", entity_id, en)
    if "CONTACT" in en_up:
        return ("contact", entity_id, en)
    if "LEAD" in en_up:
        return ("lead", entity_id, en)
    if "COMPANY" in en_up:
        return ("company", entity_id, en)
    if "USER" in en_up:
        return ("user", entity_id, en)
    if entity_type_id:
        return (f"sp:{int(entity_type_id)}", entity_id, en)

    ek = payload.get("entity_key")
    if isinstance(ek, str) and ek.strip():
        return (ek.strip(), entity_id, en)

    return (None, entity_id, en)

def _enqueue_webhook_event(entity_key: str, entity_id: int, event_name: str, payload: Dict[str, Any]) -> None:
    conn = None
    try:
        conn = pg_conn()
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO public.b24_webhook_queue (entity_key, entity_id, event_name, event, payload, status, attempts, next_run_at, received_at, created_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, 'new', 0, now(), now(), now())
            """, (entity_key, entity_id, event_name, event_name, json.dumps(payload, ensure_ascii=False)))
    except Exception as e:
        logi(f"ERROR: _enqueue_webhook_event: {e}")
        traceback.print_exc()
    finally:
        if conn:
            conn.close()

def _bitrix_get_one(entity_key: str, entity_id: int) -> Optional[Dict[str, Any]]:
    try:
        if entity_key == "user":
            resp = b24.call("user.get", {"ID": str(int(entity_id))})
            if isinstance(resp, dict) and resp.get("error") == "OVERLOAD_LIMIT":
                return None
            result = resp.get("result") if isinstance(resp, dict) else None
            if isinstance(result, list) and result:
                first = result[0]
                return first if isinstance(first, dict) else None
            return None

        if entity_key == "deal":
            resp = b24.call("crm.deal.get", {"id": entity_id})
            if isinstance(resp, dict) and resp.get("error") == "OVERLOAD_LIMIT":
                return None
            return (resp.get("result") if isinstance(resp, dict) else None)

        if entity_key == "contact":
            resp = b24.call("crm.contact.get", {"id": entity_id})
            if isinstance(resp, dict) and resp.get("error") == "OVERLOAD_LIMIT":
                return None
            return (resp.get("result") if isinstance(resp, dict) else None)

        if entity_key == "lead":
            resp = b24.call("crm.lead.get", {"id": entity_id})
            if isinstance(resp, dict) and resp.get("error") == "OVERLOAD_LIMIT":
                return None
            return (resp.get("result") if isinstance(resp, dict) else None)

        if entity_key.startswith("sp:"):
            etid = int(entity_key.split(":", 1)[1])
            resp = b24.call("crm.item.get", {"entityTypeId": etid, "id": entity_id})
            if isinstance(resp, dict) and resp.get("error") == "OVERLOAD_LIMIT":
                return None
            if not isinstance(resp, dict):
                return None
            r = resp.get("result") or {}
            if isinstance(r, dict):
                return r.get("item") or None
            return None
    except Exception as e:
        logi(f"ERROR: _bitrix_get_one({entity_key},{entity_id}): {e}")
        traceback.print_exc()
    return None

def _upsert_single_item(conn, entity_key: str, item: Dict[str, Any]) -> bool:
    if entity_key == "user":
        raw_id = item.get("ID") if "ID" in item else item.get("id")
        user_id = _extract_int(raw_id)
        if not user_id:
            return False
        user_name = _user_record_to_name(item)
        if not user_name:
            return False
        _upsert_b24_user(conn, int(user_id), user_name)
        return True

    table = table_name_for_entity(entity_key)
    ensure_pk_index(conn, table)
    colmap = load_entity_colmap(conn, entity_key)
    if not colmap:
        logi(f"WARNING: webhook upsert: no colmap for {entity_key} (run schema sync once)")
        return False

    # Determine id field
    raw_id = item.get("ID") if "ID" in item else item.get("id")
    entity_id = _extract_int(raw_id)
    if not entity_id:
        return False

    # Build column order
    col_order = ["id", "raw"] + sorted({m["column_name"] for m in colmap.values()})
    # keep updated_at
    if "updated_at" not in col_order:
        col_order.append("updated_at")

    row = {c: None for c in col_order}
    row["id"] = int(entity_id)
    row["raw"] = Json(item)
    row["updated_at"] = datetime.now(timezone.utc)

    for b24_field, meta in colmap.items():
        col = meta["column_name"]
        b24_type = meta.get("b24_type")
        is_multiple = meta.get("is_multiple", False)

        value = None
        if b24_field in item:
            value = item[b24_field]
        elif b24_field.upper() in item:
            value = item[b24_field.upper()]
        elif b24_field.lower() in item:
            value = item[b24_field.lower()]
        elif isinstance(item.get("fields"), dict) and b24_field in item["fields"]:
            value = item["fields"][b24_field]

        if value is not None:
            row[col] = normalize_value(value, b24_type, is_multiple)

    upsert_rows(conn, table, col_order, [[row.get(c) for c in col_order]])
    return True


def _event_is_delete(event_name: str, payload: Optional[Dict[str, Any]] = None) -> bool:
    ev_parts: List[str] = []
    if event_name:
        ev_parts.append(str(event_name))
    if isinstance(payload, dict):
        for k in ("event", "event_name", "EVENT_NAME", "action", "ACTION"):
            v = payload.get(k)
            if v is not None:
                ev_parts.append(str(v))
        data = payload.get("data") or payload.get("DATA")
        if isinstance(data, dict):
            for k in ("event", "event_name", "EVENT_NAME", "action", "ACTION"):
                v = data.get(k)
                if v is not None:
                    ev_parts.append(str(v))

    ev_up = " | ".join(ev_parts).upper()
    return ("DELETE" in ev_up) or ("REMOVE" in ev_up)


def _delete_single_item(conn, entity_key: str, entity_id: int) -> bool:
    try:
        ek = str(entity_key or "").strip().lower()
        if ek == "user":
            table = "b24_users"
        else:
            table = table_name_for_entity(entity_key)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {table} WHERE id=%s", (int(entity_id),))
            deleted = int(cur.rowcount or 0)
        logi(f"INFO: webhook delete: entity_key={entity_key} id={entity_id} deleted_rows={deleted}")
        return True
    except Exception as e:
        logi(f"ERROR: webhook delete failed: entity_key={entity_key} id={entity_id}: {e}")
        traceback.print_exc()
        return False

def webhook_queue_worker(stop_event: threading.Event) -> None:
    logi("INFO: webhook_queue_worker started")
    while not stop_event.is_set():
        try:
            conn = pg_conn()
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, entity_key, entity_id,
                           COALESCE(event_name, event) AS event_name,
                           payload,
                           attempts
                    FROM public.b24_webhook_queue
                    WHERE status IN ('new','retry','pending')
                      AND (next_run_at IS NULL OR next_run_at <= now())
                    ORDER BY id
                    LIMIT 10
                """)
                jobs = cur.fetchall() or []
            conn.close()

            if not jobs:
                time.sleep(1.0)
                continue

            for job in jobs:
                if stop_event.is_set():
                    break
                qid = int(job["id"])
                ek = str(job["entity_key"])
                eid = int(job["entity_id"])
                ev = str(job.get("event_name") or "")
                payload = job.get("payload") if isinstance(job.get("payload"), dict) else {}
                attempts = int(job.get("attempts") or 0)

                # mark processing
                connp = pg_conn(); connp.autocommit = True
                with connp.cursor() as curp:
                    curp.execute("UPDATE public.b24_webhook_queue SET status='processing' WHERE id=%s", (qid,))
                connp.close()

                # delete event/action -> delete row from local DB, then mark queue item done
                if _event_is_delete(ev, payload):
                    connd = pg_conn(); connd.autocommit = True
                    ok_del = False
                    try:
                        ok_del = _delete_single_item(connd, ek, eid)
                    finally:
                        connd.close()

                    connm = pg_conn(); connm.autocommit=True
                    with connm.cursor() as curm:
                        if ok_del:
                            curm.execute("UPDATE public.b24_webhook_queue SET status='done', processed_at=now(), last_error=NULL WHERE id=%s", (qid,))
                        else:
                            backoff = min(300, 5 * (attempts + 1))
                            curm.execute("""
                                UPDATE public.b24_webhook_queue
                                SET status='retry', attempts=attempts+1,
                                    last_error=%s,
                                    next_run_at=now() + (%s || ' seconds')::interval
                                WHERE id=%s
                            """, ("delete failed", backoff, qid))
                    connm.close()
                    continue

                item = _bitrix_get_one(ek, eid)
                if not item:
                    backoff = min(300, 5 * (attempts + 1))
                    connr = pg_conn(); connr.autocommit=True
                    with connr.cursor() as curr:
                        curr.execute("""
                            UPDATE public.b24_webhook_queue
                            SET status='retry', attempts=attempts+1,
                                last_error=%s,
                                next_run_at=now() + (%s || ' seconds')::interval
                            WHERE id=%s
                        """, ("bitrix blocked / empty", backoff, qid))
                    connr.close()
                    continue

                connu = pg_conn(); connu.autocommit=True
                ok = False
                try:
                    ok = _upsert_single_item(connu, ek, item)
                finally:
                    connu.close()

                connf = pg_conn(); connf.autocommit=True
                with connf.cursor() as curf:
                    if ok:
                        curf.execute("UPDATE public.b24_webhook_queue SET status='done', processed_at=now(), last_error=NULL WHERE id=%s", (qid,))
                    else:
                        backoff = min(300, 5 * (attempts + 1))
                        curf.execute("""
                            UPDATE public.b24_webhook_queue
                            SET status='retry', attempts=attempts+1,
                                last_error=%s,
                                next_run_at=now() + (%s || ' seconds')::interval
                            WHERE id=%s
                        """, ("upsert failed", backoff, qid))
                connf.close()

        except Exception as e:
            logi(f"ERROR: webhook_queue_worker: {e}")
            traceback.print_exc()
            time.sleep(2.0)

    logi("INFO: webhook_queue_worker stopped")

WEBHOOK_WORKER_STOP = threading.Event()
WEBHOOK_WORKER_THREAD: Optional[threading.Thread] = None

@app.post("/webhooks/b24/dynamic-item-update")
async def b24_dynamic_item_update(request: Request):
    """
    Receiver for Bitrix outbound webhooks (create/update/delete) for deals/leads/contacts/smart-process.
    Works WITHOUT python-multipart:
      - application/json -> request.json()
      - application/x-www-form-urlencoded -> parse raw body via urllib.parse
    Supports Bitrix keys like data[FIELDS][ID], data[FIELDS][ENTITY_TYPE_ID].
    """
    try:
        import urllib.parse

        ct = (request.headers.get("content-type") or "").lower()

        # --- 1) payload parsing (NO python-multipart needed) ---
        payload: dict = {}

        if "application/json" in ct:
            payload = await request.json()
        else:
            # Bitrix чаще всего шлёт x-www-form-urlencoded
            raw = await request.body()
            s = raw.decode("utf-8", errors="ignore")
            qs = urllib.parse.parse_qs(s, keep_blank_values=True)

            # превратим qs: {k:[v]} -> {k:v}
            payload = {k: (v[0] if isinstance(v, list) and v else "") for k, v in qs.items()}

        # --- 2) helpers to read common Bitrix keys ---
        def _get_first(*keys: str) -> str:
            for k in keys:
                v = payload.get(k)
                if v is None:
                    continue
                v = str(v).strip()
                if v != "":
                    return v
            return ""

        event_name = _get_first("event", "event_name", "EVENT_NAME")

        # Bitrix формат:
        #  - deal/lead/contact: data[FIELDS][ID]
        #  - dynamic items: data[FIELDS][ID] + data[FIELDS][ENTITY_TYPE_ID]
        entity_id_str = _get_first("data[FIELDS][ID]", "data[ID]", "ID", "id")
        entity_type_id_str = _get_first("data[FIELDS][ENTITY_TYPE_ID]", "data[ENTITY_TYPE_ID]", "ENTITY_TYPE_ID")

        # --- 3) detect entity_key from event ---
        # deal/lead/contact events
        ev = (event_name or "").upper()

        entity_key = ""
        norm_event = event_name or ""

        if "ONCRMDEAL" in ev:
            entity_key = "deal"
        elif "ONCRMLEAD" in ev:
            entity_key = "lead"
        elif "ONCRMCONTACT" in ev:
            entity_key = "contact"
        elif "ONCRMCOMPANY" in ev:
            entity_key = "company"
        elif "ONUSER" in ev or ev.startswith("ONUSER"):
            entity_key = "user"
        elif "ONCRMDYNAMICITEM" in ev or "DYNAMIC" in ev:
            # smart-process / dynamic items
            if not entity_type_id_str:
                logi(
                    f"WARNING: dynamic webhook without ENTITY_TYPE_ID "
                    f"(id={entity_id_str}, event={event_name}). Skipping."
                )
                return {"ok": True, "queued": False}

            # ВАЖНО: сохраняем entity_key как sp:<ENTITY_TYPE_ID>
            entity_key = f"sp:{int(entity_type_id_str)}"

        # --- 4) validate entity_id ---
        if not entity_key or not entity_id_str:
            logi(
                f"WARNING: webhook parse failed. "
                f"content-type={ct}, len={len(str(payload))}, keys={list(payload.keys())[:30]}"
            )
            return {"ok": True, "queued": False}

        try:
            entity_id = int(entity_id_str)
        except Exception:
            logi(f"WARNING: webhook parse failed: cannot int(entity_id) from '{entity_id_str}'")
            return {"ok": True, "queued": False}

        # --- 5) enqueue ---
        _enqueue_webhook_event(entity_key, entity_id, norm_event, payload)
        return {"ok": True, "queued": True, "entity_key": entity_key, "entity_id": entity_id}

    except Exception as e:
        logi(f"ERROR: webhook endpoint: {e}")
        traceback.print_exc()
        return {"ok": False}


def _daily_reports_cron_thread():
    """В 23:55 по Europe/Chisinau вызывает отправку 7 отчётов (Telegram + Bitrix). Один раз в сутки.
    Используется файл-маркер на диске, чтобы только один процесс (при нескольких воркерах uvicorn) выполнял отправку."""
    import errno
    check_interval_sec = 60
    mark_dir = os.getenv("REPORT_CRON_MARK_DIR", "/tmp").strip() or "/tmp"
    if mark_dir != "/tmp" and not os.path.isdir(mark_dir):
        try:
            os.makedirs(mark_dir, exist_ok=True)
        except OSError as e:
            print(f"REPORT CRON: cannot create mark dir {mark_dir}: {e}, using /tmp", file=sys.stderr, flush=True)
            mark_dir = "/tmp"
    while True:
        try:
            now_local = datetime.now(ZoneInfo(REPORT_CRON_TZ))
            today_str = now_local.strftime("%Y-%m-%d")
            mark_file = os.path.join(mark_dir, f"report_cron_sent_{today_str}.mark")
            # Строго 23:55 по REPORT_CRON_TZ (Europe/Chisinau), без переопределения через env
            if now_local.hour == 23 and now_local.minute >= 55:
                if os.path.exists(mark_file):
                    # Уже отправляли сегодня — не дергаем эндпоинт
                    time.sleep(check_interval_sec)
                    continue
                # Маркер создаёт только эндпоинт при отправке; здесь только POST
                url = f"{REPORT_CRON_BASE_URL}/api/data/reports/stock_auto/pdf/send"
                print(
                    f"REPORT CRON: triggering send at {now_local.isoformat()} -> POST {url}",
                    file=sys.stderr,
                    flush=True,
                )
                try:
                    r = requests.post(url, timeout=600)
                    if r.status_code == 200:
                        print(
                            f"REPORT CRON: sent 7 reports to Telegram + Bitrix successfully",
                            file=sys.stderr,
                            flush=True,
                        )
                    else:
                        print(
                            f"REPORT CRON: POST failed status={r.status_code} body={r.text[:300]}",
                            file=sys.stderr,
                            flush=True,
                        )
                except Exception as e:
                    print(
                        f"REPORT CRON: request failed: {e}",
                        file=sys.stderr,
                        flush=True,
                    )
        except Exception as e:
            print(f"REPORT CRON: error: {e}", file=sys.stderr, flush=True)
        time.sleep(check_interval_sec)


@app.on_event("startup")
def on_startup():

    # Ежедневная отправка отчётов в 23:55 (те же 7 PDF в Telegram и в Bitrix)
    report_cron_thread = threading.Thread(target=_daily_reports_cron_thread, daemon=True)
    report_cron_thread.start()
    print(
        f"REPORT CRON: daily send at 23:55 {REPORT_CRON_TZ} -> Telegram + Bitrix",
        file=sys.stderr,
        flush=True,
    )

    # WEBHOOK ONLY: do not poll Bitrix, process only outbound events
    if WEBHOOK_ONLY:
        ensure_webhook_queue_schema()
        global WEBHOOK_WORKER_THREAD
        if WEBHOOK_WORKER_THREAD is None or not WEBHOOK_WORKER_THREAD.is_alive():
            WEBHOOK_WORKER_THREAD = threading.Thread(target=webhook_queue_worker, args=(WEBHOOK_WORKER_STOP,), daemon=True)
            WEBHOOK_WORKER_THREAD.start()
        print("WEBHOOK ONLY MODE: polling is disabled; waiting for outbound Bitrix events...", flush=True)
        return
    # Синхронизируем данные из Bitrix сразу при старте сервиса (в отдельном потоке, чтобы не блокировать запуск)
    # Это обеспечивает: БИТРИКС -> БАЗА -> PDF
    initial_sync_thread = threading.Thread(target=_initial_sync_thread, daemon=True)
    initial_sync_thread.start()
    
    # Запускаем фоновую синхронизацию каждые 30 секунд
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    print(
        "AUTO SYNC STARTED:",
        {
            "enabled": AUTO_SYNC_ENABLED,
            "interval_sec": AUTO_SYNC_INTERVAL_SEC,
            "deal_limit": AUTO_SYNC_DEAL_LIMIT,
            "smart_limit": AUTO_SYNC_SMART_LIMIT,
            "time_budget_sec": SYNC_TIME_BUDGET_SEC,
        },
        flush=True
    )

# -----------------------------
# API endpoints
# -----------------------------
ENTITY_TABLE_CONFIG_VERSION = 2


def _ensure_entity_table_config_schema(conn) -> None:
    """Safety net: create config tables even if /sync/schema wasn't called yet."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS entity_table_configs (
                id BIGSERIAL PRIMARY KEY,
                page_slug TEXT NOT NULL UNIQUE,
                config_version INT NOT NULL DEFAULT 1,
                config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                created_by TEXT,
                updated_by TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS entity_table_config_revisions (
                id BIGSERIAL PRIMARY KEY,
                config_id BIGINT REFERENCES entity_table_configs(id) ON DELETE SET NULL,
                page_slug TEXT NOT NULL,
                revision_no INT NOT NULL,
                config_version INT NOT NULL DEFAULT 1,
                config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                created_by TEXT
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_entity_table_config_revisions_slug_created ON entity_table_config_revisions(page_slug, created_at DESC);")
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_entity_table_config_revisions_slug_rev ON entity_table_config_revisions(page_slug, revision_no);")
    conn.commit()


def _ensure_entity_table_custom_fields_schema(conn) -> None:
    """Safety net: create custom fields table for entity-table feature."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS entity_table_custom_fields (
                id BIGSERIAL PRIMARY KEY,
                page_slug TEXT NOT NULL,
                table_index INT NOT NULL,
                target_entity JSONB NOT NULL DEFAULT '{}'::jsonb,
                source_entities JSONB NOT NULL DEFAULT '[]'::jsonb,
                name TEXT NOT NULL,
                code TEXT NOT NULL,
                description TEXT,
                field_type TEXT NOT NULL,
                editor TEXT,
                storage_entity_key TEXT,
                storage_table TEXT,
                storage_column TEXT,
                storage_pg_type TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                created_by TEXT,
                updated_by TEXT
            );
        """)
        cur.execute("ALTER TABLE entity_table_custom_fields ADD COLUMN IF NOT EXISTS storage_entity_key TEXT;")
        cur.execute("ALTER TABLE entity_table_custom_fields ADD COLUMN IF NOT EXISTS storage_table TEXT;")
        cur.execute("ALTER TABLE entity_table_custom_fields ADD COLUMN IF NOT EXISTS storage_column TEXT;")
        cur.execute("ALTER TABLE entity_table_custom_fields ADD COLUMN IF NOT EXISTS storage_pg_type TEXT;")
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_entity_table_custom_fields_slug_table_code
            ON entity_table_custom_fields(page_slug, table_index, code);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_entity_table_custom_fields_slug_table
            ON entity_table_custom_fields(page_slug, table_index, created_at DESC, id DESC);
        """)
    conn.commit()


def _entity_table_default_table() -> Dict[str, Any]:
    return {
        "table_title": "",
        "table_description": "",
        "entities": [],
        "fields": [],
        "column_order": [],
        "column_widths": {},
        "sort_key": None,
        "sort_dir": None,
        "show_time": False,
        "date_time_display": {},
        "filter_fields": [],
    }


def _entity_table_normalize_table(item: Any) -> Dict[str, Any]:
    base = _entity_table_default_table()
    src = item if isinstance(item, dict) else {}
    out = dict(base)
    for k in out.keys():
        if k in src and src[k] is not None:
            out[k] = src[k]
    if not isinstance(out.get("entities"), list):
        out["entities"] = []
    if not isinstance(out.get("fields"), list):
        out["fields"] = []
    if not isinstance(out.get("column_order"), list):
        out["column_order"] = []
    if not isinstance(out.get("column_widths"), dict):
        out["column_widths"] = {}
    if not isinstance(out.get("date_time_display"), dict):
        out["date_time_display"] = {}
    if not isinstance(out.get("filter_fields"), list):
        out["filter_fields"] = []
    out["show_time"] = bool(out.get("show_time"))
    return out


def _entity_table_migrate_config(raw_cfg: Any) -> Tuple[Dict[str, Any], bool]:
    """
    Migrate legacy config to current contract (v2).
    - old root entities/fields -> tables[0]
    - ensure page_mode/table_modes/tables/default fields exist
    """
    changed = False
    cfg = raw_cfg if isinstance(raw_cfg, dict) else {}
    if not isinstance(raw_cfg, dict):
        changed = True

    out: Dict[str, Any] = {}
    # page_mode
    page_mode = cfg.get("page_mode")
    if page_mode is None:
        page_mode = "edit"
        changed = True
    out["page_mode"] = page_mode

    # tables (supports legacy root format)
    tables_in = cfg.get("tables")
    if isinstance(tables_in, list) and tables_in:
        out_tables = [_entity_table_normalize_table(t) for t in tables_in]
    else:
        legacy_keys = {
            "table_title", "table_description", "entities", "fields", "column_order", "column_widths",
            "sort_key", "sort_dir", "show_time", "date_time_display", "filter_fields",
        }
        legacy = {k: cfg.get(k) for k in legacy_keys if k in cfg}
        out_tables = [_entity_table_normalize_table(legacy)]
        changed = True
    out["tables"] = out_tables

    # table_modes
    table_modes = cfg.get("table_modes")
    if not isinstance(table_modes, dict):
        table_modes = {str(i): "table" for i in range(len(out_tables))}
        changed = True
    else:
        tm: Dict[str, Any] = {}
        for i in range(len(out_tables)):
            k = str(i)
            v = table_modes.get(k, "table")
            tm[k] = v if isinstance(v, str) and v else "table"
        table_modes = tm
    out["table_modes"] = table_modes

    # version
    in_version = cfg.get("config_version")
    try:
        in_version_int = int(in_version)
    except Exception:
        in_version_int = 1
    if in_version_int < ENTITY_TABLE_CONFIG_VERSION:
        changed = True
    out["config_version"] = ENTITY_TABLE_CONFIG_VERSION
    return out, changed


def _entity_table_build_response(page_slug: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ok": True,
        "page_slug": page_slug,
        "config_version": int(cfg.get("config_version") or ENTITY_TABLE_CONFIG_VERSION),
        "page_mode": cfg.get("page_mode", "edit"),
        "table_modes": cfg.get("table_modes", {"0": "table"}),
        "tables": cfg.get("tables", [_entity_table_default_table()]),
    }


def _entity_table_actor_from_request(request: Request) -> Optional[str]:
    for hk in ("x-user-id", "x-user", "x-email", "x-login"):
        v = request.headers.get(hk)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _entity_table_is_guest(request: Request) -> bool:
    role = (request.headers.get("x-user-role") or request.headers.get("x-role") or "").strip().lower()
    x_guest = (request.headers.get("x-guest") or "").strip().lower()
    return role == "guest" or x_guest in ("1", "true", "yes", "y", "on")


_CUSTOM_FIELD_CODE_RE = re.compile(r"^custom_[a-z0-9_]+$")


def _entity_table_custom_field_db_id_to_api(v: Any) -> str:
    try:
        return f"cf_{int(v)}"
    except Exception:
        return "cf_0"


def _entity_table_custom_field_parse_id(raw_id: str) -> int:
    s = str(raw_id or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="id is required")
    if s.lower().startswith("cf_"):
        s = s[3:]
    try:
        out = int(s)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid custom field id")
    if out <= 0:
        raise HTTPException(status_code=400, detail="invalid custom field id")
    return out


def _entity_table_resolve_storage_target(target_entity: Dict[str, Any]) -> Dict[str, str]:
    """
    Frontend target_entity examples:
      deal_25            -> internal entity_key=deal -> table b24_crm_deal
      smart_process_1036 -> internal entity_key=sp:1036 -> table b24_sp_1036
    """
    if not isinstance(target_entity, dict):
        raise HTTPException(status_code=400, detail="target_entity must be an object")

    raw_entity_key = str(target_entity.get("entity_key") or "").strip()
    entity_type = str(target_entity.get("type") or "").strip().lower()
    if not raw_entity_key:
        raise HTTPException(status_code=400, detail="target_entity.entity_key is required")

    internal_entity_key: Optional[str] = None

    if entity_type in ("deal", "contact", "lead", "company"):
        internal_entity_key = entity_type
    elif entity_type == "smart_process":
        m = re.match(r"^smart_process_(\d+)$", raw_entity_key)
        if m:
            internal_entity_key = f"sp:{int(m.group(1))}"
        elif raw_entity_key.startswith("sp:"):
            try:
                internal_entity_key = f"sp:{int(raw_entity_key.split(':', 1)[1])}"
            except Exception:
                internal_entity_key = None
    else:
        if raw_entity_key in ("deal", "contact", "lead", "company"):
            internal_entity_key = raw_entity_key
        else:
            m_deal = re.match(r"^deal(?:_\d+)?$", raw_entity_key)
            if m_deal:
                internal_entity_key = "deal"
            m_sp = re.match(r"^(?:smart_process_(\d+)|sp:(\d+))$", raw_entity_key)
            if m_sp:
                etid = m_sp.group(1) or m_sp.group(2)
                internal_entity_key = f"sp:{int(etid)}"

    if not internal_entity_key:
        raise HTTPException(
            status_code=400,
            detail="Unsupported target_entity for storage. Expected deal/contact/lead/company or smart_process_<id>",
        )

    return {
        "storage_entity_key": internal_entity_key,
        "storage_table": table_name_for_entity(internal_entity_key),
    }


def _entity_table_custom_field_storage_pg_type(field_type: str) -> str:
    # Formula is not evaluated yet, so store computed/future value as text for both UI variants.
    return "TEXT"


def _entity_table_add_physical_custom_field_column(conn, storage_table: str, storage_column: str, storage_pg_type: str) -> None:
    ensure_table_base(conn, storage_table)
    ensure_columns(conn, storage_table, [(storage_column, storage_pg_type)])
    _ENTITY_TABLE_EDITOR_TABLE_COL_CACHE.pop(str(storage_table), None)


def _entity_table_drop_physical_custom_field_column(conn, storage_table: str, storage_column: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'ALTER TABLE {storage_table} DROP COLUMN IF EXISTS "{storage_column}";')
    _ENTITY_TABLE_EDITOR_TABLE_COL_CACHE.pop(str(storage_table), None)
    conn.commit()


def _entity_table_fill_custom_field_column_test_value(
    conn,
    storage_table: str,
    storage_column: str,
    value: str = "TEST",
) -> int:
    with conn.cursor() as cur:
        cur.execute(f'UPDATE {storage_table} SET "{storage_column}"=%s WHERE id IS NOT NULL;', (str(value),))
        updated = int(cur.rowcount or 0)
    return updated


def _entity_table_recalculate_custom_field_stub(conn, row: Dict[str, Any]) -> Dict[str, Any]:
    storage_table = str(row.get("storage_table") or "").strip()
    storage_column = str(row.get("storage_column") or row.get("code") or "").strip()
    if not storage_table or not storage_column:
        target_entity = row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}
        resolved = _entity_table_resolve_storage_target(target_entity)
        storage_table = resolved["storage_table"]
        storage_column = storage_column or str(row.get("code") or "").strip()
    if not storage_table or not storage_column:
        raise HTTPException(status_code=400, detail="custom field storage is not defined")

    updated_rows = _entity_table_fill_custom_field_column_test_value(conn, storage_table, storage_column, "TEST")
    return {
        "updated_rows": updated_rows,
        "storage": {
            "table": storage_table,
            "column": storage_column,
            "pg_type": row.get("storage_pg_type") or "TEXT",
        },
        "mode": "stub_test_fill",
        "editor_used": row.get("editor") or "",
    }


def _entity_table_editor_lookup_key(v: Any) -> str:
    s = str(v or "")
    try:
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass
    s = " ".join(s.strip().split())
    return s.casefold()


def _entity_table_editor_entity_candidate_keys(item: Dict[str, Any]) -> List[str]:
    keys: List[str] = []

    def add(x: Any) -> None:
        k = _entity_table_editor_lookup_key(x)
        if k:
            keys.append(k)

    if not isinstance(item, dict):
        return []

    title = item.get("title")
    entity_key = str(item.get("entity_key") or "").strip()
    entity_type = str(item.get("type") or "").strip().lower()
    add(title)
    add(entity_key)
    add(entity_type)

    # Smart-process aliases: smart_process_1036, sp:1036, 1036
    if entity_type == "smart_process" or entity_key.startswith("smart_process_") or entity_key.startswith("sp:"):
        m = re.match(r"^smart_process_(\d+)$", entity_key)
        if m:
            etid = m.group(1)
            add(f"smart_process_{etid}")
            add(f"sp:{etid}")
            add(etid)
        else:
            m2 = re.match(r"^sp:(\d+)$", entity_key)
            if m2:
                etid = m2.group(1)
                add(f"smart_process_{etid}")
                add(f"sp:{etid}")
                add(etid)

    # Deal aliases: deal_25, deal, category id as string (if present)
    if entity_type == "deal" or entity_key.startswith("deal"):
        add("deal")
        cid = item.get("category_id")
        if cid not in (None, ""):
            try:
                add(str(int(str(cid).strip())))
            except Exception:
                add(cid)

    # Remove duplicates preserving order.
    out: List[str] = []
    seen: set = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _entity_table_editor_entity_tech_keys(item: Dict[str, Any]) -> List[str]:
    keys: List[str] = []
    if not isinstance(item, dict):
        return keys
    entity_key = str(item.get("entity_key") or item.get("nested_entity_key") or "").strip()
    entity_type = str(item.get("type") or "").strip().lower()
    if entity_key:
        keys.append(_entity_table_editor_lookup_key(entity_key))
        m = re.match(r"^smart_process_(\d+)$", entity_key)
        if m:
            etid = m.group(1)
            keys.extend([
                _entity_table_editor_lookup_key(f"smart_process_{etid}"),
                _entity_table_editor_lookup_key(f"sp:{etid}"),
                _entity_table_editor_lookup_key(etid),
            ])
        m2 = re.match(r"^sp:(\d+)$", entity_key)
        if m2:
            etid = m2.group(1)
            keys.extend([
                _entity_table_editor_lookup_key(f"smart_process_{etid}"),
                _entity_table_editor_lookup_key(f"sp:{etid}"),
                _entity_table_editor_lookup_key(etid),
            ])
        if entity_key.startswith("deal"):
            keys.append(_entity_table_editor_lookup_key("deal"))
    if entity_type:
        keys.append(_entity_table_editor_lookup_key(entity_type))
    out: List[str] = []
    seen: set = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _entity_table_editor_normalize_tech_entity_key_token(entity_key_token: Any) -> str:
    s = str(entity_key_token or "").strip()
    if not s:
        return ""
    m_sp = re.match(r"^(?:smart_process_(\d+)|sp:(\d+))$", s, flags=re.IGNORECASE)
    if m_sp:
        etid = m_sp.group(1) or m_sp.group(2)
        return f"sp:{int(etid)}"
    m_deal = re.match(r"^deal(?:_\d+)?$", s, flags=re.IGNORECASE)
    if m_deal:
        # Preserve deal_25 form for row-context matching; normalize case only.
        return s.lower()
    if s.lower() in ("deal", "contact", "lead", "company"):
        return s.lower()
    return s


def _entity_table_editor_infer_entity_type_from_key(entity_key: str) -> str:
    s = str(entity_key or "").strip().lower()
    if not s:
        return ""
    if s in ("deal", "contact", "lead", "company"):
        return s
    if s.startswith("deal_"):
        return "deal"
    if s.startswith("sp:") or s.startswith("smart_process_"):
        return "smart_process"
    return ""


def _entity_table_editor_normalize_entity_item(item: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        return None
    out = dict(item)
    raw_entity_key = str(out.get("entity_key") or out.get("nested_entity_key") or "").strip()
    if raw_entity_key:
        out["entity_key"] = _entity_table_editor_normalize_tech_entity_key_token(raw_entity_key) or raw_entity_key
    if not str(out.get("type") or "").strip():
        inferred_type = _entity_table_editor_infer_entity_type_from_key(str(out.get("entity_key") or ""))
        if inferred_type:
            out["type"] = inferred_type
    return out


def _entity_table_editor_split_ref_token(token: str) -> Tuple[str, str, Optional[str]]:
    s = str(token or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="Empty field reference in editor")
    if "." not in s:
        raise HTTPException(status_code=400, detail=f"Invalid field reference '{s}', expected {{Entity.Field}}")
    entity_part, field_part = s.rsplit(".", 1)
    entity_part = entity_part.strip()
    field_part = field_part.strip()
    if not entity_part or not field_part:
        raise HTTPException(status_code=400, detail=f"Invalid field reference '{s}', expected {{Entity.Field}}")
    value_mode: Optional[str] = None
    m = re.match(r"^(.*?):(raw|display)$", field_part, flags=re.IGNORECASE)
    if m:
        field_part = m.group(1).strip()
        value_mode = m.group(2).strip().lower()
        if not field_part:
            raise HTTPException(status_code=400, detail=f"Invalid field reference '{s}'")
    return entity_part, field_part, value_mode


def _entity_table_editor_split_tech_ref_token(token: str) -> Tuple[str, Optional[str], str, Optional[str]]:
    """
    Technical token formats:
      {deal_25||TITLE}
      {deal_25|smart_process_1036|TITLE}
    Optional value mode suffix on field:
      TITLE:raw / TITLE:display
    """
    s = str(token or "").strip()
    parts = s.split("|")
    if len(parts) != 3:
        raise HTTPException(status_code=400, detail=f"Invalid technical field reference '{s}'")
    parent_entity_key = str(parts[0] or "").strip()
    nested_entity_key = str(parts[1] or "").strip() or None
    field_code = str(parts[2] or "").strip()
    if not parent_entity_key or not field_code:
        raise HTTPException(status_code=400, detail=f"Invalid technical field reference '{s}'")
    value_mode: Optional[str] = None
    m = re.match(r"^(.*?):(raw|display)$", field_code, flags=re.IGNORECASE)
    if m:
        field_code = m.group(1).strip()
        value_mode = m.group(2).strip().lower()
    if not field_code:
        raise HTTPException(status_code=400, detail=f"Invalid technical field reference '{s}'")
    return parent_entity_key, nested_entity_key, field_code, value_mode


def _entity_table_editor_parse(expr_text: str) -> Any:
    s = str(expr_text or "").strip()
    if not s:
        raise HTTPException(status_code=400, detail="editor is empty")

    i = 0
    n = len(s)

    def skip_ws() -> None:
        nonlocal i
        while i < n and s[i].isspace():
            i += 1

    def parse_string() -> Any:
        nonlocal i
        quote = s[i]
        i += 1
        out: List[str] = []
        while i < n:
            ch = s[i]
            if ch == "\\" and i + 1 < n:
                out.append(s[i + 1])
                i += 2
                continue
            if ch == quote:
                i += 1
                return ("string", "".join(out))
            out.append(ch)
            i += 1
        raise HTTPException(status_code=400, detail="Unclosed string in editor")

    def parse_number() -> Any:
        nonlocal i
        start = i
        if s[i] in "+-":
            i += 1
        has_dot = False
        while i < n and (s[i].isdigit() or (s[i] == "." and not has_dot)):
            if s[i] == ".":
                has_dot = True
            i += 1
        txt = s[start:i]
        try:
            if "." in txt:
                return ("number", float(txt))
            return ("number", int(txt))
        except Exception:
            raise HTTPException(status_code=400, detail=f"Invalid number in editor: {txt}")

    def parse_ident() -> str:
        nonlocal i
        start = i
        while i < n and (s[i].isalnum() or s[i] == "_"):
            i += 1
        ident = s[start:i]
        if not ident:
            raise HTTPException(status_code=400, detail=f"Unexpected token in editor at position {i}")
        return ident

    def parse_ref() -> Any:
        nonlocal i
        i += 1  # skip {
        start = i
        while i < n and s[i] != "}":
            i += 1
        if i >= n:
            raise HTTPException(status_code=400, detail="Unclosed { in editor")
        token = s[start:i]
        i += 1  # skip }
        if "|" in token and "." not in token:
            p_key, n_key, field_code, value_mode = _entity_table_editor_split_tech_ref_token(token)
            return ("tech_ref", p_key, n_key, field_code, value_mode)
        entity_name, field_name, value_mode = _entity_table_editor_split_ref_token(token)
        return ("ref", entity_name, field_name, value_mode)

    def parse_expr() -> Any:
        nonlocal i
        skip_ws()
        if i >= n:
            raise HTTPException(status_code=400, detail="Unexpected end of editor")
        ch = s[i]
        if ch in ("'", '"'):
            return parse_string()
        if ch == "{":
            return parse_ref()
        if ch.isdigit() or (ch in "+-" and i + 1 < n and s[i + 1].isdigit()):
            return parse_number()
        if ch.isalpha() or ch == "_":
            ident = parse_ident()
            skip_ws()
            if i < n and s[i] == "(":
                i += 1
                args: List[Any] = []
                skip_ws()
                if i < n and s[i] == ")":
                    i += 1
                    return ("call", ident.upper(), args)
                while True:
                    args.append(parse_expr())
                    skip_ws()
                    if i >= n:
                        raise HTTPException(status_code=400, detail="Unclosed function call in editor")
                    if s[i] == ",":
                        i += 1
                        continue
                    if s[i] == ")":
                        i += 1
                        break
                    raise HTTPException(status_code=400, detail=f"Unexpected token '{s[i]}' in function args")
                return ("call", ident.upper(), args)
            return ("ident", ident)
        raise HTTPException(status_code=400, detail=f"Unsupported token '{ch}' in editor")

    ast = parse_expr()
    skip_ws()
    if i != n:
        raise HTTPException(status_code=400, detail=f"Unexpected trailing editor text: {s[i:]}")
    return ast


def _entity_table_editor_collect_entities(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for src in [row.get("target_entity")] + list(row.get("source_entities") or []):
        norm = _entity_table_editor_normalize_entity_item(src)
        if not isinstance(norm, dict):
            continue
        key = json.dumps(norm, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
    return out


def _entity_table_editor_collect_source_entities(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for src in list(row.get("source_entities") or []):
        norm = _entity_table_editor_normalize_entity_item(src)
        if not isinstance(norm, dict):
            continue
        key = json.dumps(norm, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
    return out


def _entity_table_editor_resolve_entity_from_list(entities: List[Dict[str, Any]], entity_name: str) -> Optional[Dict[str, Any]]:
    lookup = _entity_table_editor_lookup_key(entity_name)
    for item in entities:
        if not isinstance(item, dict):
            continue
        for c in _entity_table_editor_entity_candidate_keys(item):
            if c == lookup:
                resolved = _entity_table_resolve_storage_target(item)
                return {
                    "input": item,
                    "storage_entity_key": resolved["storage_entity_key"],
                    "storage_table": resolved["storage_table"],
                }
    return None


def _entity_table_editor_resolve_entity_by_tech_key_from_list(entities: List[Dict[str, Any]], entity_key_token: str) -> Optional[Dict[str, Any]]:
    token_raw = str(entity_key_token or "").strip()
    token_norm = _entity_table_editor_normalize_tech_entity_key_token(token_raw) or token_raw
    lookup_candidates = [
        _entity_table_editor_lookup_key(token_raw),
        _entity_table_editor_lookup_key(token_norm),
    ]
    m_sp = re.match(r"^sp:(\d+)$", str(token_norm), flags=re.IGNORECASE)
    if m_sp:
        etid = m_sp.group(1)
        lookup_candidates.extend([
            _entity_table_editor_lookup_key(f"smart_process_{etid}"),
            _entity_table_editor_lookup_key(f"sp:{etid}"),
            _entity_table_editor_lookup_key(etid),
        ])
    lookup_set = {x for x in lookup_candidates if x}
    for item in entities:
        if not isinstance(item, dict):
            continue
        for c in _entity_table_editor_entity_tech_keys(item):
            if c in lookup_set:
                resolved = _entity_table_resolve_storage_target(item)
                return {
                    "input": item,
                    "storage_entity_key": resolved["storage_entity_key"],
                    "storage_table": resolved["storage_table"],
                }
    return None


def _entity_table_editor_resolve_entity(row: Dict[str, Any], entity_name: str) -> Dict[str, Any]:
    found = _entity_table_editor_resolve_entity_from_list(_entity_table_editor_collect_entities(row), entity_name)
    if found:
        return found
    raise HTTPException(status_code=400, detail=f"Unknown entity in editor reference: {entity_name}")


def _entity_table_editor_resolve_nested_entity(row: Dict[str, Any], entity_name: str) -> Dict[str, Any]:
    # Nested segment should resolve from source_entities first.
    found = _entity_table_editor_resolve_entity_from_list(_entity_table_editor_collect_source_entities(row), entity_name)
    if found:
        return found
    found = _entity_table_editor_resolve_entity_from_list(_entity_table_editor_collect_entities(row), entity_name)
    if found:
        return found
    raise HTTPException(status_code=400, detail=f"Unknown entity in editor reference: {entity_name}")


def _entity_table_editor_resolve_entity_tech(row: Dict[str, Any], entity_key_token: str, nested: bool = False) -> Dict[str, Any]:
    token_raw = str(entity_key_token or "").strip()
    token_norm = _entity_table_editor_normalize_tech_entity_key_token(token_raw) or token_raw
    if nested:
        found = _entity_table_editor_resolve_entity_by_tech_key_from_list(
            _entity_table_editor_collect_source_entities(row), token_norm
        )
        if found:
            return found
    found = _entity_table_editor_resolve_entity_by_tech_key_from_list(
        _entity_table_editor_collect_entities(row), token_norm
    )
    if found:
        return found
    if re.match(r"^(?:sp:\d+|smart_process_\d+)$", token_raw, flags=re.IGNORECASE):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unknown entity technical key in editor reference: {token_raw} "
                f"(normalized as {token_norm}). Supported smart-process formats: sp:<id>, smart_process_<id>"
            ),
        )
    raise HTTPException(
        status_code=400,
        detail=f"Unknown entity technical key in editor reference: {token_raw}",
    )


def _entity_table_http_error_text(e: HTTPException) -> str:
    d = getattr(e, "detail", None)
    if isinstance(d, str):
        return d
    if isinstance(d, dict):
        for k in ("message", "detail", "error"):
            v = d.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        try:
            return json.dumps(d, ensure_ascii=False)
        except Exception:
            return str(d)
    return str(d) if d is not None else "Unknown error"


def _entity_table_error_response(status_code: int, error: str, detail: str):
    return JSONResponse(status_code=status_code, content={"ok": False, "error": error, "detail": detail})


def _entity_table_editor_field_candidates(field_name: str, row_meta: Dict[str, Any]) -> List[str]:
    labels: List[str] = []
    for k in ("column_name", "b24_field", "b24_title"):
        v = row_meta.get(k)
        if isinstance(v, str) and v.strip():
            labels.append(v.strip())
    b24_labels = row_meta.get("b24_labels")
    if isinstance(b24_labels, dict):
        for v in b24_labels.values():
            s = _label_to_string(v)
            if s:
                labels.append(s)
    return labels


def _entity_table_editor_infer_crm_target_from_settings(settings: Any) -> Optional[str]:
    if isinstance(settings, str):
        try:
            settings = json.loads(settings)
        except Exception:
            settings = {}
    if not isinstance(settings, dict):
        return None
    etid = settings.get("entityTypeId") or settings.get("entity_type_id")
    if etid not in (None, ""):
        try:
            num = int(str(etid).strip())
            if num == 2:
                return "deal"
            if num == 3:
                return "contact"
            if num == 4:
                return "lead"
            if num == 5:
                return "company"
            return f"sp:{num}"
        except Exception:
            pass
    allowed: List[str] = []
    for k, v in settings.items():
        if str(v).strip().lower() not in ("y", "1", "true"):
            continue
        kk = str(k).strip().upper()
        if kk == "DEAL":
            allowed.append("deal")
        elif kk == "CONTACT":
            allowed.append("contact")
        elif kk == "LEAD":
            allowed.append("lead")
        elif kk == "COMPANY":
            allowed.append("company")
        else:
            m_dyn = re.match(r"DYNAMIC_(\d+)$", kk)
            if m_dyn:
                allowed.append(f"sp:{int(m_dyn.group(1))}")
    allowed = list(dict.fromkeys(allowed))
    if len(allowed) == 1:
        return allowed[0]
    return None


def _entity_table_editor_entity_key_from_parent_id_name(name: str) -> Optional[str]:
    s = str(name or "").strip().lower()
    m = re.match(r"parentid(\d+)$", s)
    if not m:
        return None
    num = int(m.group(1))
    if num == 2:
        return "deal"
    if num == 3:
        return "contact"
    if num == 4:
        return "lead"
    if num == 5:
        return "company"
    return f"sp:{num}"


def _entity_table_editor_infer_link_target_entity_key(meta_row: Dict[str, Any]) -> Optional[str]:
    col = str(meta_row.get("column_name") or "").strip()
    b24_field = str(meta_row.get("b24_field") or "").strip()
    b24_type = str(meta_row.get("b24_type") or "").strip().lower()
    if b24_type in ("crm_contact", "contact"):
        return "contact"
    if b24_type in ("crm_lead", "lead"):
        return "lead"
    if b24_type in ("crm_company", "company"):
        return "company"
    if b24_type in ("crm_entity", "crm"):
        from_settings = _entity_table_editor_infer_crm_target_from_settings(meta_row.get("settings"))
        if from_settings:
            return from_settings
    return _entity_table_editor_entity_key_from_parent_id_name(col) or _entity_table_editor_entity_key_from_parent_id_name(b24_field)


def _entity_table_editor_find_direct_join_from_target(
    conn,
    target_entity_key: str,
    target_table: str,
    ref_entity_key: str,
) -> Optional[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT column_name, b24_field, b24_type, settings
            FROM b24_meta_fields
            WHERE entity_key=%s
        """, (target_entity_key,))
        rows = cur.fetchall() or []

    existing_cols = _table_columns_cached(conn, target_table)
    matches: List[Dict[str, Any]] = []
    for mrow in rows:
        col = str(mrow.get("column_name") or "").strip()
        if not col or col not in existing_cols:
            continue
        target = _entity_table_editor_infer_link_target_entity_key(mrow)
        if target != ref_entity_key:
            continue
        matches.append({
            "join_column": col,
            "target_entity_key": target,
            "via_b24_field": mrow.get("b24_field"),
            "via_b24_type": mrow.get("b24_type"),
        })

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        # Ambiguous relation; caller should return token-specific error.
        return {"ambiguous": True, "candidates": matches}
    return None


def _entity_table_editor_build_rowwise_join_path(
    conn,
    current_target_entity_key: str,
    current_target_table: str,
    path_entities: List[Dict[str, Any]],
    token_full: str,
) -> List[Dict[str, Any]]:
    if not path_entities:
        return []
    first = path_entities[0]
    if (
        str(first.get("storage_entity_key") or "") != str(current_target_entity_key)
        or str(first.get("storage_table") or "") != str(current_target_table)
    ):
        raise HTTPException(
            status_code=400,
            detail=f"Row-wise join path not found for {token_full}: parent entity is not target entity context",
        )

    steps: List[Dict[str, Any]] = []
    if len(path_entities) == 1:
        return steps

    for i in range(len(path_entities) - 1):
        src = path_entities[i]
        dst = path_entities[i + 1]
        join = _entity_table_editor_find_direct_join_from_target(
            conn,
            str(src.get("storage_entity_key") or ""),
            str(src.get("storage_table") or ""),
            str(dst.get("storage_entity_key") or ""),
        )
        if not join:
            raise HTTPException(
                status_code=400,
                detail=f"Row-wise join path not found for {token_full}: no link field from target entity to source entity",
            )
        if isinstance(join, dict) and join.get("ambiguous"):
            raise HTTPException(
                status_code=400,
                detail=f"Ambiguous row-wise join path for {token_full}: multiple link fields match source entity",
            )
        join_col = str(join.get("join_column") or "").strip()
        if not join_col:
            raise HTTPException(
                status_code=400,
                detail=f"Row-wise join path not found for {token_full}: no link field from target entity to source entity",
            )
        steps.append(
            {
                "from_entity_key": str(src.get("storage_entity_key") or ""),
                "from_table": str(src.get("storage_table") or ""),
                "to_entity_key": str(dst.get("storage_entity_key") or ""),
                "to_table": str(dst.get("storage_table") or ""),
                "join_column": join_col,
                "via_b24_field": join.get("via_b24_field"),
                "via_b24_type": join.get("via_b24_type"),
            }
        )
    return steps


def _entity_table_editor_resolve_column(conn, entity_key: str, table_name: str, field_name: str) -> Dict[str, Any]:
    field_lookup = _entity_table_editor_lookup_key(field_name)
    if _CUSTOM_FIELD_CODE_RE.fullmatch(str(field_name or "").strip()) and field_name in _table_columns_cached(conn, table_name):
        return {
            "column": str(field_name).strip(),
            "b24_type": "text",
            "b24_field": str(field_name).strip(),
            "b24_title": str(field_name).strip(),
            "settings": None,
        }

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT column_name, b24_field, b24_type, b24_title, b24_labels, settings
            FROM b24_meta_fields
            WHERE entity_key=%s
        """, (entity_key,))
        rows = cur.fetchall() or []

    for meta in rows:
        col = str(meta.get("column_name") or "").strip()
        if not col:
            continue
        for cand in _entity_table_editor_field_candidates(field_name, meta):
            if _entity_table_editor_lookup_key(cand) == field_lookup:
                return {
                    "column": col,
                    "b24_type": meta.get("b24_type"),
                    "b24_field": meta.get("b24_field"),
                    "b24_title": meta.get("b24_title"),
                    "settings": meta.get("settings"),
                }

    # Fallback: direct physical column passthrough (useful for custom_* and raw technical names)
    for col in _table_columns_cached(conn, table_name):
        if _entity_table_editor_lookup_key(col) == field_lookup:
            return {
                "column": col,
                "b24_type": None,
                "b24_field": col,
                "b24_title": col,
                "settings": None,
            }

    raise HTTPException(status_code=400, detail=f"Unknown field '{field_name}' for entity '{entity_key}'")


def _entity_table_editor_resolve_column_tech(conn, entity_key: str, table_name: str, field_code: str) -> Dict[str, Any]:
    code_lookup = _entity_table_editor_lookup_key(field_code)
    if _CUSTOM_FIELD_CODE_RE.fullmatch(str(field_code or "").strip()) and str(field_code).strip() in _table_columns_cached(conn, table_name):
        return {
            "column": str(field_code).strip(),
            "b24_type": "text",
            "b24_field": str(field_code).strip(),
            "b24_title": str(field_code).strip(),
            "settings": None,
        }

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT column_name, b24_field, b24_type, b24_title, settings
            FROM b24_meta_fields
            WHERE entity_key=%s
        """, (entity_key,))
        rows = cur.fetchall() or []

    for meta in rows:
        col = str(meta.get("column_name") or "").strip()
        b24f = str(meta.get("b24_field") or "").strip()
        if b24f and _entity_table_editor_lookup_key(b24f) == code_lookup:
            return {
                "column": col,
                "b24_type": meta.get("b24_type"),
                "b24_field": meta.get("b24_field"),
                "b24_title": meta.get("b24_title"),
                "settings": meta.get("settings"),
            }
        if col and _entity_table_editor_lookup_key(col) == code_lookup:
            return {
                "column": col,
                "b24_type": meta.get("b24_type"),
                "b24_field": meta.get("b24_field") or col,
                "b24_title": meta.get("b24_title"),
                "settings": meta.get("settings"),
            }

    for col in _table_columns_cached(conn, table_name):
        if _entity_table_editor_lookup_key(col) == code_lookup:
            return {
                "column": col,
                "b24_type": None,
                "b24_field": col,
                "b24_title": col,
                "settings": None,
            }
    raise HTTPException(status_code=400, detail=f"Unknown field technical code '{field_code}' for entity '{entity_key}'")


_ENTITY_TABLE_EDITOR_TABLE_COL_CACHE: Dict[str, set] = {}


def _table_columns_cached(conn, table_name: str) -> set:
    cache_key = str(table_name)
    cols = _ENTITY_TABLE_EDITOR_TABLE_COL_CACHE.get(cache_key)
    if cols is not None:
        return cols
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
        """, (table_name,))
        cols = {str(r[0]) for r in (cur.fetchall() or []) if r and r[0]}
    _ENTITY_TABLE_EDITOR_TABLE_COL_CACHE[cache_key] = cols
    return cols


def _entity_table_editor_parse_number(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _entity_table_editor_fetch_column_values(conn, table_name: str, column_name: str) -> List[Any]:
    with conn.cursor() as cur:
        cur.execute(f'SELECT "{column_name}" FROM "{table_name}" WHERE id IS NOT NULL')
        return [r[0] for r in (cur.fetchall() or [])]


def _entity_table_editor_count_rows(conn, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{table_name}" WHERE id IS NOT NULL')
        row = cur.fetchone()
        return int(row[0] if row else 0)


def _entity_table_editor_format_result_for_text(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return str(v)
    return str(v)


def _entity_table_editor_eval_aggregate(conn, fn: str, args: List[Any], row: Dict[str, Any]) -> Any:
    fn_up = fn.upper()
    if fn_up == "COUNT" and len(args) == 0:
        target_entity = row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}
        resolved = _entity_table_resolve_storage_target(target_entity)
        return _entity_table_editor_count_rows(conn, resolved["storage_table"])

    if len(args) != 1:
        raise HTTPException(status_code=400, detail=f"{fn_up} expects exactly one argument")
    arg = args[0]
    if not (isinstance(arg, tuple) and len(arg) >= 1 and arg[0] == "field_ref"):
        raise HTTPException(status_code=400, detail=f"{fn_up} currently supports only direct field reference argument")

    ref = arg[1]
    values = _entity_table_editor_fetch_column_values(conn, ref["table"], ref["column"])
    if fn_up == "COUNT":
        return sum(1 for v in values if v is not None and str(v).strip() != "")

    nums: List[float] = []
    for v in values:
        num = _entity_table_editor_parse_number(v)
        if num is not None:
            nums.append(num)

    if fn_up == "SUM":
        return sum(nums) if nums else 0
    if fn_up == "AVG":
        return (sum(nums) / len(nums)) if nums else None
    if fn_up == "MIN":
        return min(nums) if nums else None
    if fn_up == "MAX":
        return max(nums) if nums else None
    raise HTTPException(status_code=400, detail=f"Unsupported aggregate function: {fn_up}")


def _entity_table_editor_eval_ast(conn, ast: Any, row: Dict[str, Any]) -> Any:
    if not isinstance(ast, tuple) or not ast:
        raise HTTPException(status_code=400, detail="Invalid editor AST")
    kind = ast[0]
    if kind == "number":
        return ast[1]
    if kind == "string":
        return ast[1]
    if kind == "ident":
        ident = str(ast[1] or "").strip()
        if ident.upper() == "NULL":
            return None
        raise HTTPException(status_code=400, detail=f"Unsupported identifier in editor: {ident}")
    if kind == "ref":
        entity_name = ast[1]
        field_name = ast[2]
        ent = _entity_table_editor_resolve_entity(row, entity_name)
        col_meta = _entity_table_editor_resolve_column(conn, ent["storage_entity_key"], ent["storage_table"], field_name)
        return (
            "field_ref",
            {
                "entity_key": ent["storage_entity_key"],
                "table": ent["storage_table"],
                "column": col_meta.get("column"),
                "b24_type": col_meta.get("b24_type"),
                "b24_field": col_meta.get("b24_field"),
            },
        )
    if kind == "tech_ref":
        parent_key = str(ast[1] or "").strip()
        nested_key = (str(ast[2]).strip() if len(ast) > 2 and ast[2] else None)
        field_code = str(ast[3] or "").strip()
        ent = _entity_table_editor_resolve_entity_tech(row, nested_key or parent_key, nested=bool(nested_key))
        col_meta = _entity_table_editor_resolve_column_tech(conn, ent["storage_entity_key"], ent["storage_table"], field_code)
        return (
            "field_ref",
            {
                "entity_key": ent["storage_entity_key"],
                "table": ent["storage_table"],
                "column": col_meta.get("column"),
                "b24_type": col_meta.get("b24_type"),
                "b24_field": col_meta.get("b24_field"),
            },
        )
    if kind != "call":
        raise HTTPException(status_code=400, detail=f"Unsupported editor node: {kind}")

    fn = str(ast[1] or "").upper()
    raw_args = list(ast[2] or [])
    eval_args = [_entity_table_editor_eval_ast(conn, a, row) for a in raw_args]

    if fn in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
        return _entity_table_editor_eval_aggregate(conn, fn, eval_args, row)

    if fn == "CONCAT":
        parts: List[str] = []
        for v in eval_args:
            if isinstance(v, tuple) and v and v[0] == "field_ref":
                raise HTTPException(status_code=400, detail="CONCAT with direct field reference is not supported yet")
            parts.append("" if v is None else str(v))
        return "".join(parts)

    if fn == "IFNULL":
        if len(eval_args) != 2:
            raise HTTPException(status_code=400, detail="IFNULL expects exactly two arguments")
        left = eval_args[0]
        if left is None:
            return eval_args[1]
        if isinstance(left, str) and left == "":
            return eval_args[1]
        return left

    if fn == "ROUND":
        if len(eval_args) not in (1, 2):
            raise HTTPException(status_code=400, detail="ROUND expects one or two arguments")
        val_num = _entity_table_editor_parse_number(eval_args[0])
        if val_num is None:
            return None
        digits = 0
        if len(eval_args) == 2:
            d = _entity_table_editor_parse_number(eval_args[1])
            digits = int(d or 0)
        return round(val_num, digits)

    if fn == "NUMBER":
        if len(eval_args) != 1:
            raise HTTPException(status_code=400, detail="NUMBER expects exactly one argument")
        return _entity_table_editor_parse_number(eval_args[0])

    raise HTTPException(status_code=400, detail=f"Unsupported function in editor: {fn}")


def _entity_table_editor_ast_has_aggregate(ast: Any) -> bool:
    if not isinstance(ast, tuple) or not ast:
        return False
    if ast[0] == "call":
        fn = str(ast[1] or "").upper()
        if fn in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
            return True
        return any(_entity_table_editor_ast_has_aggregate(a) for a in (ast[2] or []))
    return False


def _entity_table_editor_ref_display_kind_uses_display(ref: Dict[str, Any]) -> bool:
    return bool(ref.get("display_kind"))


def _entity_table_editor_load_user_names_by_ids(conn, ids: List[str]) -> Dict[str, str]:
    ids_int: List[int] = []
    for x in ids:
        try:
            v = int(str(x).strip())
            if v > 0:
                ids_int.append(v)
        except Exception:
            continue
    ids_int = list(dict.fromkeys(ids_int))
    if not ids_int:
        return {}
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM b24_users WHERE id = ANY(%s)", (ids_int,))
        rows = cur.fetchall() or []
    return {str(r[0]): str(r[1]) for r in rows if r and len(r) >= 2 and r[1] is not None}


def _entity_table_editor_load_stage_names(conn, stage_ids: List[str]) -> Dict[str, str]:
    keys = [str(x).strip() for x in stage_ids if str(x).strip()]
    keys = list(dict.fromkeys(keys))
    if not keys:
        return {}
    with conn.cursor() as cur:
        cur.execute("SELECT stage_id, name FROM b24_deal_stages WHERE stage_id = ANY(%s)", (keys,))
        rows = cur.fetchall() or []
    return {str(r[0]): str(r[1]) for r in rows if r and len(r) >= 2 and r[1] is not None}


def _entity_table_editor_load_enum_titles(conn, entity_key: str, b24_field: str, values: List[str]) -> Dict[str, str]:
    vals = [str(x).strip() for x in values if str(x).strip()]
    vals = list(dict.fromkeys(vals))
    if not vals or not b24_field:
        return {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT value_id, value_title
            FROM b24_field_enum
            WHERE entity_key=%s AND b24_field=%s AND value_id = ANY(%s)
        """, (entity_key, b24_field, vals))
        rows = cur.fetchall() or []
    return {str(r[0]): str(r[1]) for r in rows if r and len(r) >= 2 and r[1] is not None}


def _entity_table_editor_load_entity_titles(conn, target_entity_key: str, values: List[str]) -> Dict[str, str]:
    ids: List[int] = []
    for x in values:
        try:
            v = int(str(x).strip())
            if v > 0:
                ids.append(v)
        except Exception:
            continue
    ids = list(dict.fromkeys(ids))
    if not ids:
        return {}
    table = table_name_for_entity(target_entity_key)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f'SELECT id, title, name, raw FROM "{table}" WHERE id = ANY(%s)', (ids,))
        rows = cur.fetchall() or []
    out: Dict[str, str] = {}
    for r in rows:
        rid = r.get("id")
        raw = r.get("raw") if isinstance(r.get("raw"), dict) else {}
        title = r.get("title") or r.get("name") or raw.get("TITLE") or raw.get("NAME") or raw.get("title") or raw.get("name")
        if rid is not None and title is not None:
            out[str(rid)] = str(title)
    return out


def _entity_table_editor_build_display_map(conn, ref: Dict[str, Any], raw_values: List[Any]) -> Dict[str, str]:
    vals = [str(v).strip() for v in raw_values if v is not None and str(v).strip() != ""]
    vals = list(dict.fromkeys(vals))
    if not vals:
        return {}
    kind = str(ref.get("display_kind") or "").strip().lower()
    if kind == "user":
        return _entity_table_editor_load_user_names_by_ids(conn, vals)
    if kind == "stage":
        return _entity_table_editor_load_stage_names(conn, vals)
    if kind == "enum":
        return _entity_table_editor_load_enum_titles(conn, str(ref.get("entity_key") or ""), str(ref.get("b24_field") or ""), vals)
    if kind == "crm_link":
        target_entity_key = str(ref.get("display_target_entity_key") or "").strip()
        if target_entity_key:
            return _entity_table_editor_load_entity_titles(conn, target_entity_key, vals)
    return {}


def _entity_table_editor_ref_value_mode(ref_mode: Optional[str], ref: Dict[str, Any]) -> str:
    if ref_mode in ("raw", "display"):
        return ref_mode
    return "display" if _entity_table_editor_ref_display_kind_uses_display(ref) else "raw"


def _entity_table_editor_row_value_from_ref(
    conn,
    ref: Dict[str, Any],
    raw_value: Any,
    ref_mode: Optional[str],
    display_cache: Dict[Tuple[str, str, str], Dict[str, str]],
) -> Any:
    mode = _entity_table_editor_ref_value_mode(ref_mode, ref)
    if mode == "raw":
        return raw_value
    kind = str(ref.get("display_kind") or "").strip().lower()
    if not kind:
        return raw_value
    cache_key = (
        str(ref.get("entity_key") or ""),
        str(ref.get("column") or ""),
        mode,
    )
    dmap = display_cache.get(cache_key)
    if dmap is None:
        with conn.cursor() as cur:
            cur.execute(f'SELECT "{ref["column"]}" FROM "{ref["table"]}" WHERE id IS NOT NULL')
            values = [r[0] for r in (cur.fetchall() or [])]
        dmap = _entity_table_editor_build_display_map(conn, ref, values)
        display_cache[cache_key] = dmap
    key = "" if raw_value is None else str(raw_value).strip()
    if not key:
        return raw_value
    return dmap.get(key, raw_value)


def _entity_table_editor_extract_single_link_id(v: Any) -> Optional[int]:
    # Deterministic rule for row-wise cardinality>1: use the first linked id.
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return int(v) if v > 0 else None
    if isinstance(v, float):
        try:
            iv = int(v)
            return iv if iv > 0 else None
        except Exception:
            return None
    if isinstance(v, list):
        for item in v:
            got = _entity_table_editor_extract_single_link_id(item)
            if got:
                return got
        return None
    if isinstance(v, dict):
        # Common shapes: {"ID":123} / {"id":"123"} / {"VALUE":"123"}
        for k in ("ID", "id", "VALUE", "value"):
            if k in v:
                got = _entity_table_editor_extract_single_link_id(v.get(k))
                if got:
                    return got
        return None
    s = str(v).strip()
    if not s:
        return None
    if s.startswith("[") and s.endswith("]"):
        try:
            parsed = json.loads(s)
            return _entity_table_editor_extract_single_link_id(parsed)
        except Exception:
            pass
    if "," in s:
        for part in [p.strip() for p in s.split(",") if p.strip()]:
            got = _entity_table_editor_extract_single_link_id(part)
            if got:
                return got
        return None
    try:
        iv = int(float(s))
        return iv if iv > 0 else None
    except Exception:
        return None


def _entity_table_editor_fetch_foreign_row_cached(
    conn,
    table_name: str,
    row_id: int,
    foreign_row_cache: Dict[Tuple[str, int], Dict[str, Any]],
) -> Dict[str, Any]:
    cache_key_row = (str(table_name), int(row_id))
    foreign_db_row = foreign_row_cache.get(cache_key_row)
    if foreign_db_row is None:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f'SELECT * FROM "{table_name}" WHERE id=%s LIMIT 1', (int(row_id),))
            foreign_db_row = cur.fetchone() or {}
        foreign_row_cache[cache_key_row] = foreign_db_row
    return foreign_db_row


def _entity_table_editor_resolve_rowwise_ref_raw_value(
    conn,
    ref: Dict[str, Any],
    target_storage_table: str,
    current_row: Dict[str, Any],
    foreign_row_cache: Dict[Tuple[str, int], Dict[str, Any]],
    token_full: str,
) -> Any:
    if str(ref.get("table") or "") == str(target_storage_table):
        return current_row.get(ref["column"])

    steps = ref.get("rowwise_join_steps")
    if isinstance(steps, list) and steps:
        row_obj: Dict[str, Any] = current_row
        row_table = str(target_storage_table)
        for step in steps:
            join_col = str(step.get("join_column") or "").strip()
            to_table = str(step.get("to_table") or "").strip()
            if not join_col or not to_table:
                raise HTTPException(status_code=400, detail=f"{token_full} is not available for row_wise join yet")
            join_raw_id = row_obj.get(join_col)
            join_id_int = _entity_table_editor_extract_single_link_id(join_raw_id)
            if not join_id_int:
                return None
            row_obj = _entity_table_editor_fetch_foreign_row_cached(conn, to_table, join_id_int, foreign_row_cache)
            row_table = to_table
            if not row_obj:
                return None
        if row_table != str(ref.get("table") or ""):
            raise HTTPException(status_code=400, detail=f"{token_full} is not available for row_wise join yet")
        return row_obj.get(ref["column"])

    # Backward-compatible single-hop implicit resolver.
    join = ref.get("join_from_target")
    if not isinstance(join, dict):
        join = None
    if not join:
        raise HTTPException(status_code=400, detail=f"{token_full} is not available for row_wise join yet")
    if join.get("ambiguous"):
        raise HTTPException(status_code=400, detail=f"{token_full} has ambiguous row_wise join from target entity")
    join_col = str(join.get("join_column") or "").strip()
    if not join_col:
        raise HTTPException(status_code=400, detail=f"{token_full} is not available for row_wise join yet")
    join_id_int = _entity_table_editor_extract_single_link_id(current_row.get(join_col))
    if not join_id_int:
        return None
    foreign_db_row = _entity_table_editor_fetch_foreign_row_cached(conn, str(ref["table"]), int(join_id_int), foreign_row_cache)
    return foreign_db_row.get(ref["column"])


def _entity_table_editor_prepare_ref(conn, cf_row: Dict[str, Any], entity_name: str, field_name: str) -> Dict[str, Any]:
    ent = _entity_table_editor_resolve_entity(cf_row, entity_name)
    col_meta = _entity_table_editor_resolve_column(conn, ent["storage_entity_key"], ent["storage_table"], field_name)
    col = str(col_meta.get("column") or "").strip()
    b24_type = col_meta.get("b24_type")
    b24_field = str(col_meta.get("b24_field") or col or "").strip()
    settings = col_meta.get("settings")
    col_low = col.lower()
    b24_field_low = b24_field.lower()
    b24_type_low = str(b24_type or "").strip().lower()

    display_kind: Optional[str] = None
    display_target_entity_key: Optional[str] = None
    if col_low in ("assigned_by_id", "created_by_id", "modified_by_id", "moved_by_id", "last_activity_by", "last_activity_by_id") or b24_field_low in (
        "assigned_by_id", "created_by_id", "modified_by_id", "moved_by_id", "last_activity_by", "last_activity_by_id"
    ):
        display_kind = "user"
    elif col_low == "stage_id" or b24_field_low == "stage_id":
        display_kind = "stage"
    elif b24_type_low in ("enumeration", "enum", "list", "status"):
        display_kind = "enum"
    elif b24_type_low in ("user", "crm_user", "employee"):
        display_kind = "user"
    elif b24_type_low in ("crm_contact", "contact"):
        display_kind = "crm_link"; display_target_entity_key = "contact"
    elif b24_type_low in ("crm_lead", "lead"):
        display_kind = "crm_link"; display_target_entity_key = "lead"
    elif b24_type_low in ("crm_company", "company"):
        display_kind = "crm_link"; display_target_entity_key = "company"
    elif b24_type_low in ("crm_entity", "crm"):
        display_kind = "crm_link"
        display_target_entity_key = _entity_table_editor_infer_crm_target_from_settings(settings)

    return {
        "entity_key": ent["storage_entity_key"],
        "table": ent["storage_table"],
        "column": col,
        "b24_type": b24_type,
        "b24_field": b24_field,
        "settings": settings,
        "display_kind": display_kind,
        "display_target_entity_key": display_target_entity_key,
        "entity_name": entity_name,
        "field_name": field_name,
    }


def _entity_table_editor_prepare_ref_from_resolved_entity(
    conn,
    resolved_ent: Dict[str, Any],
    entity_name: str,
    field_name: str,
) -> Dict[str, Any]:
    col_meta = _entity_table_editor_resolve_column(
        conn,
        str(resolved_ent.get("storage_entity_key") or ""),
        str(resolved_ent.get("storage_table") or ""),
        field_name,
    )
    col = str(col_meta.get("column") or "").strip()
    b24_type = col_meta.get("b24_type")
    b24_field = str(col_meta.get("b24_field") or col or "").strip()
    settings = col_meta.get("settings")
    col_low = col.lower()
    b24_field_low = b24_field.lower()
    b24_type_low = str(b24_type or "").strip().lower()

    display_kind: Optional[str] = None
    display_target_entity_key: Optional[str] = None
    if col_low in ("assigned_by_id", "created_by_id", "modified_by_id", "moved_by_id", "last_activity_by", "last_activity_by_id") or b24_field_low in (
        "assigned_by_id", "created_by_id", "modified_by_id", "moved_by_id", "last_activity_by", "last_activity_by_id"
    ):
        display_kind = "user"
    elif col_low == "stage_id" or b24_field_low == "stage_id":
        display_kind = "stage"
    elif b24_type_low in ("enumeration", "enum", "list", "status"):
        display_kind = "enum"
    elif b24_type_low in ("user", "crm_user", "employee"):
        display_kind = "user"
    elif b24_type_low in ("crm_contact", "contact"):
        display_kind = "crm_link"; display_target_entity_key = "contact"
    elif b24_type_low in ("crm_lead", "lead"):
        display_kind = "crm_link"; display_target_entity_key = "lead"
    elif b24_type_low in ("crm_company", "company"):
        display_kind = "crm_link"; display_target_entity_key = "company"
    elif b24_type_low in ("crm_entity", "crm"):
        display_kind = "crm_link"
        display_target_entity_key = _entity_table_editor_infer_crm_target_from_settings(settings)

    return {
        "entity_key": str(resolved_ent.get("storage_entity_key") or ""),
        "table": str(resolved_ent.get("storage_table") or ""),
        "column": col,
        "b24_type": b24_type,
        "b24_field": b24_field,
        "settings": settings,
        "display_kind": display_kind,
        "display_target_entity_key": display_target_entity_key,
        "entity_name": entity_name,
        "field_name": field_name,
    }


def _entity_table_editor_prepare_tech_ref_from_resolved_entity(
    conn,
    resolved_ent: Dict[str, Any],
    entity_name: str,
    field_code: str,
) -> Dict[str, Any]:
    col_meta = _entity_table_editor_resolve_column_tech(
        conn,
        str(resolved_ent.get("storage_entity_key") or ""),
        str(resolved_ent.get("storage_table") or ""),
        field_code,
    )
    # Reuse display-kind inference path by adapting resolved entity + field label.
    # `entity_name` here is technical token segment for diagnostics only.
    ref = _entity_table_editor_prepare_ref_from_resolved_entity(conn, resolved_ent, entity_name, str(col_meta.get("column") or field_code))
    # Override exact resolved column metadata from tech resolver (prevents title-based ambiguity).
    ref["column"] = str(col_meta.get("column") or ref.get("column") or "")
    ref["b24_type"] = col_meta.get("b24_type")
    ref["b24_field"] = str(col_meta.get("b24_field") or ref.get("b24_field") or "")
    ref["settings"] = col_meta.get("settings")
    ref["field_name"] = field_code
    return ref


def _entity_table_editor_prepare_ref_rowwise(
    conn,
    cf_row: Dict[str, Any],
    target_entity_key: str,
    target_storage_table: str,
    entity_name: str,
    field_name: str,
) -> Dict[str, Any]:
    token_full = "{" + str(entity_name or "").strip() + "." + str(field_name or "").strip() + "}"
    # New frontend nested token format arrives as {ParentEntity.NestedEntity.Field}
    # Parser stores it as entity_name="ParentEntity.NestedEntity", field_name="Field".
    parts = [p.strip() for p in str(entity_name or "").split(".")]
    parts = [p for p in parts if p]
    total_segments = len(parts) + (1 if str(field_name or "").strip() else 0)
    if total_segments >= 3:
        try:
            path_entities: List[Dict[str, Any]] = []
            for idx, p in enumerate(parts):
                if idx == 0:
                    path_entities.append(_entity_table_editor_resolve_entity(cf_row, p))
                else:
                    path_entities.append(_entity_table_editor_resolve_nested_entity(cf_row, p))
        except HTTPException as e:
            # Nested path must not silently fall back to legacy 2-part resolver.
            msg = _entity_table_http_error_text(e)
            raise HTTPException(
                status_code=400,
                detail=f"Nested token resolve failed for {token_full}: {msg}",
            )
        join_steps = _entity_table_editor_build_rowwise_join_path(
            conn,
            str(target_entity_key or ""),
            str(target_storage_table or ""),
            path_entities,
            token_full,
        )
        leaf_entity = path_entities[-1]
        try:
            ref = _entity_table_editor_prepare_ref_from_resolved_entity(
                conn, leaf_entity, parts[-1], field_name
            )
        except HTTPException as e:
            msg = _entity_table_http_error_text(e)
            raise HTTPException(
                status_code=400,
                detail=f"Nested token resolve failed for {token_full}: {msg}",
            )
        ref["rowwise_join_steps"] = join_steps
        ref["token_entity_path"] = entity_name
        ref["token_full"] = token_full
        ref["resolver_path_attempted"] = "nested_path"
        ref["parsed_segments"] = parts + [str(field_name or "").strip()]
        ref["explicit_path_entities"] = [
            {
                "entity_key": pe.get("storage_entity_key"),
                "table": pe.get("storage_table"),
                "input": pe.get("input"),
            }
            for pe in path_entities
        ]
        # Ensure final entity descriptor aligns with explicit leaf from path.
        ref["entity_key"] = leaf_entity.get("storage_entity_key")
        ref["table"] = leaf_entity.get("storage_table")
        return ref

    # Backward-compatible format {Entity.Field}
    ref = _entity_table_editor_prepare_ref(conn, cf_row, entity_name, field_name)
    ref["token_full"] = token_full
    ref["resolver_path_attempted"] = "legacy_2part"
    ref["parsed_segments"] = parts + [str(field_name or "").strip()]
    return ref


def _entity_table_editor_eval_ast_rowwise(
    conn,
    ast: Any,
    cf_row: Dict[str, Any],
    target_entity_key: str,
    target_storage_table: str,
    current_row: Dict[str, Any],
    ref_cache: Dict[Tuple[str, str], Dict[str, Any]],
    display_cache: Dict[Tuple[str, str, str], Dict[str, str]],
    foreign_row_cache: Dict[Tuple[str, int], Dict[str, Any]],
) -> Any:
    if not isinstance(ast, tuple) or not ast:
        raise HTTPException(status_code=400, detail="Invalid editor AST")
    kind = ast[0]

    if kind == "number":
        return ast[1]
    if kind == "string":
        return ast[1]
    if kind == "ident":
        ident = str(ast[1] or "").strip()
        if ident.upper() == "NULL":
            return None
        raise HTTPException(status_code=400, detail=f"Unsupported identifier in editor: {ident}")
    if kind == "ref":
        entity_name = str(ast[1] or "").strip()
        field_name = str(ast[2] or "").strip()
        ref_mode = (str(ast[3]).strip().lower() if len(ast) > 3 and ast[3] else None)
        cache_key = (entity_name, field_name)
        ref = ref_cache.get(cache_key)
        if ref is None:
            ref = _entity_table_editor_prepare_ref_rowwise(
                conn, cf_row, target_entity_key, target_storage_table, entity_name, field_name
            )
            ref_cache[cache_key] = ref
        token_full = str(ref.get("token_full") or ("{" + entity_name + "." + field_name + "}"))
        raw_val = _entity_table_editor_resolve_rowwise_ref_raw_value(
            conn, ref, target_storage_table, current_row, foreign_row_cache, token_full
        )
        return _entity_table_editor_row_value_from_ref(conn, ref, raw_val, ref_mode, display_cache)
    if kind == "tech_ref":
        parent_key = str(ast[1] or "").strip()
        nested_key = (str(ast[2]).strip() if len(ast) > 2 and ast[2] else None)
        field_code = str(ast[3] or "").strip()
        ref_mode = (str(ast[4]).strip().lower() if len(ast) > 4 and ast[4] else None)
        cache_key = (f"TECH:{parent_key}|{nested_key or ''}", field_code)
        ref = ref_cache.get(cache_key)
        token_full = "{" + parent_key + "|" + (nested_key or "") + "|" + field_code + "}"
        if ref is None:
            # Technical tokens are primary path: resolve by entity_key and field code, no title matching.
            parent_ent = _entity_table_editor_resolve_entity_tech(cf_row, parent_key, nested=False)
            if nested_key:
                nested_ent = _entity_table_editor_resolve_entity_tech(cf_row, nested_key, nested=True)
                path_entities = [parent_ent, nested_ent]
                join_steps = _entity_table_editor_build_rowwise_join_path(
                    conn, target_entity_key, target_storage_table, path_entities, token_full
                )
                ref = _entity_table_editor_prepare_tech_ref_from_resolved_entity(conn, nested_ent, nested_key, field_code)
                ref["rowwise_join_steps"] = join_steps
            else:
                ref = _entity_table_editor_prepare_tech_ref_from_resolved_entity(conn, parent_ent, parent_key, field_code)
                # If technical parent is not current target entity, build single/multi-step path from target to parent.
                if (
                    str(parent_ent.get("storage_entity_key") or "") != str(target_entity_key)
                    or str(parent_ent.get("storage_table") or "") != str(target_storage_table)
                ):
                    target_ctx_ent = {
                        "input": cf_row.get("target_entity") if isinstance(cf_row.get("target_entity"), dict) else {},
                        "storage_entity_key": target_entity_key,
                        "storage_table": target_storage_table,
                    }
                    ref["rowwise_join_steps"] = _entity_table_editor_build_rowwise_join_path(
                        conn, target_entity_key, target_storage_table, [target_ctx_ent, parent_ent], token_full
                    )
            ref["token_full"] = token_full
            ref["resolver_path_attempted"] = "tech_token"
            ref_cache[cache_key] = ref
        raw_val = _entity_table_editor_resolve_rowwise_ref_raw_value(conn, ref, target_storage_table, current_row, foreign_row_cache, token_full)
        return _entity_table_editor_row_value_from_ref(conn, ref, raw_val, ref_mode, display_cache)
    if kind != "call":
        raise HTTPException(status_code=400, detail=f"Unsupported editor node: {kind}")

    fn = str(ast[1] or "").upper()
    raw_args = list(ast[2] or [])

    # Aggregates are handled by aggregate mode only.
    if fn in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
        raise HTTPException(status_code=400, detail=f"{fn} cannot be used in row-wise mode")

    eval_args = [
        _entity_table_editor_eval_ast_rowwise(
            conn, a, cf_row, target_entity_key, target_storage_table, current_row, ref_cache, display_cache, foreign_row_cache
        )
        for a in raw_args
    ]

    if fn == "CONCAT":
        return "".join("" if v is None else str(v) for v in eval_args)

    if fn == "IFNULL":
        if len(eval_args) != 2:
            raise HTTPException(status_code=400, detail="IFNULL expects exactly two arguments")
        left = eval_args[0]
        if left is None:
            return eval_args[1]
        if isinstance(left, str) and left == "":
            return eval_args[1]
        return left

    if fn == "ROUND":
        if len(eval_args) not in (1, 2):
            raise HTTPException(status_code=400, detail="ROUND expects one or two arguments")
        val_num = _entity_table_editor_parse_number(eval_args[0])
        if val_num is None:
            return None
        digits = 0
        if len(eval_args) == 2:
            d = _entity_table_editor_parse_number(eval_args[1])
            digits = int(d or 0)
        return round(val_num, digits)

    if fn == "NUMBER":
        if len(eval_args) != 1:
            raise HTTPException(status_code=400, detail="NUMBER expects exactly one argument")
        return _entity_table_editor_parse_number(eval_args[0])

    raise HTTPException(status_code=400, detail=f"Unsupported function in editor: {fn}")


def _entity_table_write_custom_field_row_values(
    conn,
    storage_table: str,
    storage_column: str,
    updates: List[Tuple[int, Optional[str]]],
) -> int:
    if not updates:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            f'UPDATE "{storage_table}" SET "{storage_column}"=%s WHERE id=%s',
            [(val, int(rid)) for rid, val in updates],
        )
    return len(updates)


def _entity_table_write_custom_field_scalar_value(conn, storage_table: str, storage_column: str, scalar_value: Optional[str]) -> int:
    with conn.cursor() as cur:
        cur.execute(f'UPDATE "{storage_table}" SET "{storage_column}"=%s WHERE id IS NOT NULL;', (scalar_value,))
        updated = int(cur.rowcount or 0)
    return updated


def _entity_table_recalculate_custom_field_editor(conn, row: Dict[str, Any]) -> Dict[str, Any]:
    storage_table = str(row.get("storage_table") or "").strip()
    storage_column = str(row.get("storage_column") or row.get("code") or "").strip()
    if not storage_table or not storage_column:
        target_entity = row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}
        resolved = _entity_table_resolve_storage_target(target_entity)
        storage_table = resolved["storage_table"]
        storage_column = storage_column or str(row.get("code") or "").strip()
    editor = str(row.get("editor") or "").strip()
    if not editor:
        raise HTTPException(status_code=400, detail="editor is empty")

    ast = _entity_table_editor_parse(editor)
    target_entity = row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}
    target_resolved = _entity_table_resolve_storage_target(target_entity) if target_entity else {
        "storage_entity_key": row.get("storage_entity_key") or "",
        "storage_table": storage_table,
    }
    target_storage_entity_key = str(target_resolved.get("storage_entity_key") or "")
    if _entity_table_editor_ast_has_aggregate(ast):
        value = _entity_table_editor_eval_ast(conn, ast, row)
        if isinstance(value, tuple) and value and value[0] == "field_ref":
            raise HTTPException(status_code=400, detail="editor cannot resolve to a direct field reference without aggregate/scalar function")
        text_value = _entity_table_editor_format_result_for_text(value)
        updated_rows = _entity_table_write_custom_field_scalar_value(conn, storage_table, storage_column, text_value)
        mode = "editor_eval"
        mode_detail = "aggregate"
        value_preview = text_value
    else:
        # Row-wise expressions: resolve direct {Entity.Field} references against current target row.
        cols_needed: set = {"id"}
        ref_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
        display_cache: Dict[Tuple[str, str, str], Dict[str, str]] = {}
        foreign_row_cache: Dict[Tuple[str, int], Dict[str, Any]] = {}

        def collect_refs(node: Any) -> None:
            if not isinstance(node, tuple) or not node:
                return
            if node[0] in ("ref", "tech_ref"):
                entity_name = str(node[1] or "").strip()
                if node[0] == "ref":
                    field_name = str(node[2] or "").strip()
                    key = (entity_name, field_name)
                else:
                    field_name = str(node[3] or "").strip()
                    nested_key = (str(node[2]).strip() if len(node) > 2 and node[2] else "")
                    key = (f"TECH:{entity_name}|{nested_key}", field_name)
                if key not in ref_cache:
                    if node[0] == "ref":
                        ref_cache[key] = _entity_table_editor_prepare_ref_rowwise(
                            conn, row, target_storage_entity_key, storage_table, entity_name, field_name
                        )
                    else:
                        ref_cache[key] = None  # built below for tech_ref
                ref = ref_cache[key]
                if not isinstance(ref, dict):
                    # build directly (tech_ref path)
                    if node[0] == "tech_ref":
                        p_key = str(node[1] or "").strip()
                        n_key = (str(node[2]).strip() if len(node) > 2 and node[2] else None)
                        token_full = "{" + p_key + "|" + (n_key or "") + "|" + field_name + "}"
                        parent_ent = _entity_table_editor_resolve_entity_tech(row, p_key, nested=False)
                        if n_key:
                            nested_ent = _entity_table_editor_resolve_entity_tech(row, n_key, nested=True)
                            ref = _entity_table_editor_prepare_tech_ref_from_resolved_entity(conn, nested_ent, n_key, field_name)
                            ref["rowwise_join_steps"] = _entity_table_editor_build_rowwise_join_path(
                                conn, target_storage_entity_key, storage_table,
                                [{"input": row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}, "storage_entity_key": target_storage_entity_key, "storage_table": storage_table}, parent_ent, nested_ent]
                                if str(parent_ent.get("storage_entity_key") or "") != str(target_storage_entity_key) else [parent_ent, nested_ent],
                                token_full
                            )
                        else:
                            ref = _entity_table_editor_prepare_tech_ref_from_resolved_entity(conn, parent_ent, p_key, field_name)
                            if str(parent_ent.get("storage_entity_key") or "") != str(target_storage_entity_key) or str(parent_ent.get("storage_table") or "") != str(storage_table):
                                ref["rowwise_join_steps"] = _entity_table_editor_build_rowwise_join_path(
                                    conn, target_storage_entity_key, storage_table,
                                    [{"input": row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}, "storage_entity_key": target_storage_entity_key, "storage_table": storage_table}, parent_ent],
                                    token_full
                                )
                        ref["token_full"] = token_full
                        ref_cache[key] = ref
                if str(ref["table"]) == str(storage_table):
                    cols_needed.add(str(ref["column"]))
                else:
                    steps = ref.get("rowwise_join_steps")
                    if isinstance(steps, list) and steps:
                        join_col = str((steps[0] or {}).get("join_column") or "").strip()
                    else:
                        join = ref.get("join_from_target")
                        if not isinstance(join, dict):
                            join = _entity_table_editor_find_direct_join_from_target(
                                conn, target_storage_entity_key, storage_table, str(ref.get("entity_key") or "")
                            )
                            ref["join_from_target"] = join
                        if not join:
                            raise HTTPException(
                                status_code=400,
                                detail=f"{{{entity_name}.{field_name}}} is not available for row_wise join yet",
                            )
                        if join.get("ambiguous"):
                            raise HTTPException(
                                status_code=400,
                                detail=f"{{{entity_name}.{field_name}}} has ambiguous row_wise join from target entity",
                            )
                        join_col = str(join.get("join_column") or "").strip()
                    if not join_col:
                        raise HTTPException(
                            status_code=400,
                            detail=f"{{{entity_name}.{field_name}}} is not available for row_wise join yet",
                        )
                    cols_needed.add(join_col)
                return
            if node[0] == "call":
                for a in (node[2] or []):
                    collect_refs(a)

        collect_refs(ast)
        select_cols = [c for c in cols_needed if c]
        cols_sql = ", ".join(f'"{c}"' for c in select_cols)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f'SELECT {cols_sql} FROM "{storage_table}" WHERE id IS NOT NULL')
            rows = cur.fetchall() or []
        updates: List[Tuple[int, Optional[str]]] = []
        for db_row in rows:
            rid = db_row.get("id")
            if rid is None:
                continue
            # `display_cache` is shared across rows for efficient ref display resolution.
            val = _entity_table_editor_eval_ast_rowwise(
                conn, ast, row, target_storage_entity_key, storage_table, db_row, ref_cache, display_cache, foreign_row_cache
            )
            updates.append((int(rid), _entity_table_editor_format_result_for_text(val)))
        updated_rows = _entity_table_write_custom_field_row_values(conn, storage_table, storage_column, updates)
        mode = "editor_eval"
        mode_detail = "row_wise"
        value_preview = updates[0][1] if updates else None

    return {
        "updated_rows": updated_rows,
        "mode": mode,
        "mode_detail": mode_detail,
        "storage": {
            "table": storage_table,
            "column": storage_column,
            "pg_type": row.get("storage_pg_type") or "TEXT",
        },
        "value_preview": value_preview,
        "custom_field_id": _entity_table_custom_field_db_id_to_api(row.get("id")),
    }


def _entity_table_validate_custom_field_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")

    page_slug = str(payload.get("page_slug") or "").strip()
    if not page_slug:
        raise HTTPException(status_code=400, detail="page_slug is required")

    if "table_index" not in payload:
        raise HTTPException(status_code=400, detail="table_index is required")
    try:
        table_index = int(payload.get("table_index"))
    except Exception:
        raise HTTPException(status_code=400, detail="table_index must be an integer")

    target_entity = payload.get("target_entity")
    if not isinstance(target_entity, dict):
        raise HTTPException(status_code=400, detail="target_entity must be an object")
    target_entity_clean = dict(target_entity)
    target_entity_key = str(target_entity_clean.get("entity_key") or "").strip()
    if not target_entity_key:
        raise HTTPException(status_code=400, detail="target_entity.entity_key is required")
    target_entity_clean["entity_key"] = target_entity_key

    custom_field = payload.get("custom_field")
    if not isinstance(custom_field, dict):
        raise HTTPException(status_code=400, detail="custom_field must be an object")

    name = str(custom_field.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="custom_field.name is required")

    code = str(custom_field.get("code") or "").strip()
    if not code:
        raise HTTPException(status_code=400, detail="custom_field.code is required")
    if not _CUSTOM_FIELD_CODE_RE.fullmatch(code):
        raise HTTPException(
            status_code=400,
            detail="custom_field.code must match ^custom_[a-z0-9_]+$",
        )

    field_type = str(custom_field.get("type") or "").strip().lower()
    if field_type not in ("single", "card"):
        raise HTTPException(status_code=400, detail="custom_field.type must be 'single' or 'card'")

    description = custom_field.get("description")
    if description is None:
        description = ""
    else:
        description = str(description)

    editor = custom_field.get("editor")
    if editor is None:
        editor = ""
    else:
        editor = str(editor)

    source_entities = payload.get("source_entities")
    if source_entities is None:
        source_entities = []
    if not isinstance(source_entities, list):
        raise HTTPException(status_code=400, detail="source_entities must be an array")

    return {
        "page_slug": page_slug,
        "table_index": table_index,
        "target_entity": target_entity_clean,
        "source_entities": source_entities,
        "custom_field": {
            "name": name,
            "code": code,
            "description": description,
            "type": field_type,
            "editor": editor,
        },
    }


def _entity_table_validate_custom_field_update_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")

    custom_field = payload.get("custom_field")
    if not isinstance(custom_field, dict):
        raise HTTPException(status_code=400, detail="custom_field must be an object")

    updates: Dict[str, Any] = {}

    if "name" in custom_field:
        name = str(custom_field.get("name") or "").strip()
        if not name:
            raise HTTPException(status_code=400, detail="custom_field.name must not be empty")
        updates["name"] = name

    if "description" in custom_field:
        desc = custom_field.get("description")
        updates["description"] = "" if desc is None else str(desc)

    if "type" in custom_field:
        field_type = str(custom_field.get("type") or "").strip().lower()
        if field_type not in ("single", "card"):
            raise HTTPException(status_code=400, detail="custom_field.type must be 'single' or 'card'")
        updates["field_type"] = field_type

    if "editor" in custom_field:
        editor = custom_field.get("editor")
        updates["editor"] = "" if editor is None else str(editor)

    if "code" in custom_field:
        code = str(custom_field.get("code") or "").strip()
        if not code:
            raise HTTPException(status_code=400, detail="custom_field.code must not be empty")
        if not _CUSTOM_FIELD_CODE_RE.fullmatch(code):
            raise HTTPException(status_code=400, detail="custom_field.code must match ^custom_[a-z0-9_]+$")
        updates["code_requested"] = code

    if not updates:
        raise HTTPException(status_code=400, detail="No updatable fields provided in custom_field")

    return updates


def _entity_table_validate_custom_field_preview_payload(payload: Any) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")
    page_slug = str(payload.get("page_slug") or "").strip()
    if not page_slug:
        raise HTTPException(status_code=400, detail="page_slug is required")
    if "table_index" not in payload:
        raise HTTPException(status_code=400, detail="table_index is required")
    try:
        table_index = int(payload.get("table_index"))
    except Exception:
        raise HTTPException(status_code=400, detail="table_index must be an integer")
    target_entity = payload.get("target_entity")
    if not isinstance(target_entity, dict):
        raise HTTPException(status_code=400, detail="target_entity must be an object")
    if not str(target_entity.get("entity_key") or "").strip():
        raise HTTPException(status_code=400, detail="target_entity.entity_key is required")
    source_entities = payload.get("source_entities")
    if source_entities is None:
        source_entities = []
    if not isinstance(source_entities, list):
        raise HTTPException(status_code=400, detail="source_entities must be an array")
    editor = payload.get("editor")
    if editor is None:
        editor = ""
    editor = str(editor)
    if not editor.strip():
        raise HTTPException(status_code=400, detail="editor is required")
    return {
        "page_slug": page_slug,
        "table_index": table_index,
        "target_entity": dict(target_entity),
        "source_entities": list(source_entities),
        "editor": editor,
    }


def _entity_table_preview_custom_field_editor(conn, row: Dict[str, Any]) -> Dict[str, Any]:
    storage_table = str(row.get("storage_table") or "").strip()
    target_entity = row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}
    if not storage_table:
        storage_table = _entity_table_resolve_storage_target(target_entity)["storage_table"]
    editor = str(row.get("editor") or "").strip()
    if not editor:
        raise HTTPException(status_code=400, detail="editor is empty")

    ast = _entity_table_editor_parse(editor)
    target_resolved = _entity_table_resolve_storage_target(target_entity)
    target_storage_entity_key = str(target_resolved.get("storage_entity_key") or "")

    with conn.cursor() as cur:
        cur.execute(f'SELECT id FROM "{storage_table}" WHERE id IS NOT NULL ORDER BY id DESC LIMIT 1')
        first_row = cur.fetchone()
    sample_row_id = int(first_row[0]) if first_row and first_row[0] is not None else None

    if _entity_table_editor_ast_has_aggregate(ast):
        value = _entity_table_editor_eval_ast(conn, ast, row)
        if isinstance(value, tuple) and value and value[0] == "field_ref":
            raise HTTPException(status_code=400, detail="editor cannot resolve to a direct field reference without aggregate/scalar function")
        return {
            "mode": "editor_eval",
            "mode_detail": "aggregate",
            "sample_value": _entity_table_editor_format_result_for_text(value),
            "sample_row_id": sample_row_id,
        }

    cols_needed: set = {"id"}
    ref_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
    display_cache: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    foreign_row_cache: Dict[Tuple[str, int], Dict[str, Any]] = {}

    def collect_refs(node: Any) -> None:
        if not isinstance(node, tuple) or not node:
            return
        if node[0] in ("ref", "tech_ref"):
            entity_name = str(node[1] or "").strip()
            if node[0] == "ref":
                field_name = str(node[2] or "").strip()
                key = (entity_name, field_name)
            else:
                field_name = str(node[3] or "").strip()
                nested_key = (str(node[2]).strip() if len(node) > 2 and node[2] else "")
                key = (f"TECH:{entity_name}|{nested_key}", field_name)
            if key not in ref_cache:
                if node[0] == "ref":
                    ref_cache[key] = _entity_table_editor_prepare_ref_rowwise(
                        conn, row, target_storage_entity_key, storage_table, entity_name, field_name
                    )
                else:
                    p_key = str(node[1] or "").strip()
                    n_key = (str(node[2]).strip() if len(node) > 2 and node[2] else None)
                    token_full = "{" + p_key + "|" + (n_key or "") + "|" + field_name + "}"
                    parent_ent = _entity_table_editor_resolve_entity_tech(row, p_key, nested=False)
                    if n_key:
                        nested_ent = _entity_table_editor_resolve_entity_tech(row, n_key, nested=True)
                        ref = _entity_table_editor_prepare_tech_ref_from_resolved_entity(conn, nested_ent, n_key, field_name)
                        base_path = [parent_ent, nested_ent]
                        if str(parent_ent.get("storage_entity_key") or "") != str(target_storage_entity_key):
                            base_path = [{"input": row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}, "storage_entity_key": target_storage_entity_key, "storage_table": storage_table}] + base_path
                        ref["rowwise_join_steps"] = _entity_table_editor_build_rowwise_join_path(conn, target_storage_entity_key, storage_table, base_path, token_full)
                    else:
                        ref = _entity_table_editor_prepare_tech_ref_from_resolved_entity(conn, parent_ent, p_key, field_name)
                        if str(parent_ent.get("storage_entity_key") or "") != str(target_storage_entity_key) or str(parent_ent.get("storage_table") or "") != str(storage_table):
                            ref["rowwise_join_steps"] = _entity_table_editor_build_rowwise_join_path(
                                conn, target_storage_entity_key, storage_table,
                                [{"input": row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}, "storage_entity_key": target_storage_entity_key, "storage_table": storage_table}, parent_ent],
                                token_full
                            )
                    ref["token_full"] = token_full
                    ref_cache[key] = ref
            ref = ref_cache[key]
            if str(ref["table"]) == str(storage_table):
                cols_needed.add(str(ref["column"]))
            else:
                steps = ref.get("rowwise_join_steps")
                if isinstance(steps, list) and steps:
                    join_col = str((steps[0] or {}).get("join_column") or "").strip()
                else:
                    join = ref.get("join_from_target")
                    if not isinstance(join, dict):
                        join = _entity_table_editor_find_direct_join_from_target(
                            conn, target_storage_entity_key, storage_table, str(ref.get("entity_key") or "")
                        )
                        ref["join_from_target"] = join
                    if not join:
                        raise HTTPException(status_code=400, detail=f"{{{entity_name}.{field_name}}} is not available for row_wise join yet")
                    if join.get("ambiguous"):
                        raise HTTPException(status_code=400, detail=f"{{{entity_name}.{field_name}}} has ambiguous row_wise join from target entity")
                    join_col = str(join.get("join_column") or "").strip()
                if not join_col:
                    raise HTTPException(status_code=400, detail=f"{{{entity_name}.{field_name}}} is not available for row_wise join yet")
                cols_needed.add(join_col)
            return
        if node[0] == "call":
            for a in (node[2] or []):
                collect_refs(a)

    collect_refs(ast)
    select_cols = [c for c in cols_needed if c]
    cols_sql = ", ".join(f'"{c}"' for c in select_cols)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f'SELECT {cols_sql} FROM "{storage_table}" WHERE id IS NOT NULL ORDER BY id DESC LIMIT 1')
        sample_row = cur.fetchone() or {}
    if not sample_row:
        return {
            "mode": "editor_eval",
            "mode_detail": "row_wise",
            "sample_value": None,
            "sample_row_id": None,
        }
    sample_val = _entity_table_editor_eval_ast_rowwise(
        conn, ast, row, target_storage_entity_key, storage_table, sample_row, ref_cache, display_cache, foreign_row_cache
    )
    return {
        "mode": "editor_eval",
        "mode_detail": "row_wise",
        "sample_value": _entity_table_editor_format_result_for_text(sample_val),
        "sample_row_id": sample_row.get("id"),
    }


def _entity_table_custom_field_row_to_api(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {
        "id": _entity_table_custom_field_db_id_to_api(row.get("id")),
        "name": row.get("name") or "",
        "code": row.get("code") or "",
        "description": row.get("description") or "",
        "type": row.get("field_type") or "",
        "editor": row.get("editor") or "",
        "target_entity": row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {},
    }
    storage_table = row.get("storage_table")
    storage_column = row.get("storage_column")
    storage_pg_type = row.get("storage_pg_type")
    if storage_table or storage_column or storage_pg_type:
        out["storage"] = {
            "table": storage_table or None,
            "column": storage_column or None,
            "pg_type": storage_pg_type or None,
        }
    return out


@app.get("/api/entity-table/config")
def get_entity_table_config(
    page_slug: str = Query(..., description="Unique page slug"),
):
    slug = (page_slug or "").strip()
    if not slug:
        raise HTTPException(status_code=400, detail="page_slug is required")

    conn = pg_conn()
    try:
        _ensure_entity_table_config_schema(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, page_slug, config_version, config_json
                FROM entity_table_configs
                WHERE page_slug=%s
                LIMIT 1
            """, (slug,))
            row = cur.fetchone()

        if not row:
            cfg, _ = _entity_table_migrate_config({})
            return _entity_table_build_response(slug, cfg)

        raw_cfg = row.get("config_json") if isinstance(row, dict) else {}
        cfg, changed = _entity_table_migrate_config(raw_cfg)
        if changed:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE entity_table_configs
                    SET config_json=%s::jsonb,
                        config_version=%s,
                        updated_at=now()
                    WHERE page_slug=%s
                """, (json.dumps(cfg, ensure_ascii=False), int(cfg.get("config_version") or ENTITY_TABLE_CONFIG_VERSION), slug))
            conn.commit()
        return _entity_table_build_response(slug, cfg)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/api/entity-table/config")
async def save_entity_table_config(request: Request):
    if _entity_table_is_guest(request):
        raise HTTPException(status_code=403, detail="Guest is not allowed to save config")

    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")

    slug = (
        str(payload.get("page_slug") or "").strip()
        or str(request.query_params.get("page_slug") or "").strip()
    )
    if not slug:
        raise HTTPException(status_code=400, detail="page_slug is required")

    # Accept both direct config payload and {config_json: {...}}
    if isinstance(payload.get("config_json"), dict):
        cfg_input = dict(payload.get("config_json") or {})
        if "page_slug" not in cfg_input:
            cfg_input["page_slug"] = slug
    else:
        cfg_input = dict(payload)

    cfg_input.pop("ok", None)
    cfg_input.pop("total", None)
    cfg_input.pop("limit", None)
    cfg_input.pop("offset", None)
    cfg_input.pop("data", None)
    cfg_input.pop("fields", None)
    cfg_input.pop("entity_key", None)
    cfg_input.pop("type", None)
    cfg_input.pop("page_slug", None)

    cfg, _ = _entity_table_migrate_config(cfg_input)
    actor = _entity_table_actor_from_request(request)

    conn = pg_conn()
    try:
        conn.autocommit = False
        _ensure_entity_table_config_schema(conn)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, created_by, created_at
                FROM entity_table_configs
                WHERE page_slug=%s
                LIMIT 1
            """, (slug,))
            existing = cur.fetchone()

            if existing:
                config_id = int(existing["id"])
                created_by = existing.get("created_by")
                cur.execute("""
                    UPDATE entity_table_configs
                    SET config_json=%s::jsonb,
                        config_version=%s,
                        updated_at=now(),
                        updated_by=%s
                    WHERE id=%s
                    RETURNING id
                """, (
                    json.dumps(cfg, ensure_ascii=False),
                    int(cfg.get("config_version") or ENTITY_TABLE_CONFIG_VERSION),
                    actor,
                    config_id,
                ))
                cur.fetchone()
            else:
                created_by = actor
                cur.execute("""
                    INSERT INTO entity_table_configs(
                        page_slug, config_version, config_json,
                        created_at, updated_at, created_by, updated_by
                    )
                    VALUES (%s, %s, %s::jsonb, now(), now(), %s, %s)
                    RETURNING id
                """, (
                    slug,
                    int(cfg.get("config_version") or ENTITY_TABLE_CONFIG_VERSION),
                    json.dumps(cfg, ensure_ascii=False),
                    created_by,
                    actor,
                ))
                ins = cur.fetchone()
                config_id = int(ins["id"]) if ins else 0

            cur.execute(
                "SELECT COALESCE(MAX(revision_no), 0) AS mx FROM entity_table_config_revisions WHERE page_slug=%s",
                (slug,),
            )
            mx_row = cur.fetchone() or {}
            next_rev = int(mx_row.get("mx") or 0) + 1

            cur.execute("""
                INSERT INTO entity_table_config_revisions(
                    config_id, page_slug, revision_no, config_version, config_json, created_at, created_by
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, now(), %s)
            """, (
                config_id if config_id > 0 else None,
                slug,
                next_rev,
                int(cfg.get("config_version") or ENTITY_TABLE_CONFIG_VERSION),
                json.dumps(cfg, ensure_ascii=False),
                actor,
            ))

        conn.commit()
        return _entity_table_build_response(slug, cfg)
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/api/entity-table/custom-fields")
async def create_entity_table_custom_field(request: Request):
    if _entity_table_is_guest(request):
        raise HTTPException(status_code=403, detail="Guest is not allowed to create custom fields")

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    data = _entity_table_validate_custom_field_payload(payload)
    actor = _entity_table_actor_from_request(request)
    storage_target = _entity_table_resolve_storage_target(data["target_entity"])
    storage_pg_type = _entity_table_custom_field_storage_pg_type(data["custom_field"]["type"])
    storage_column = data["custom_field"]["code"]
    recalculate_now = bool(payload.get("recalculate_now"))
    debug_stub_fill = bool(payload.get("debug_stub_fill"))

    conn = pg_conn()
    try:
        conn.autocommit = False
        _ensure_entity_table_custom_fields_schema(conn)
        # Create physical column in entity data table first (same transaction).
        _entity_table_add_physical_custom_field_column(
            conn,
            storage_target["storage_table"],
            storage_column,
            storage_pg_type,
        )
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id
                FROM entity_table_custom_fields
                WHERE storage_table=%s AND storage_column=%s
                LIMIT 1
            """, (storage_target["storage_table"], storage_column))
            existing_storage_ref = cur.fetchone()
            if existing_storage_ref:
                raise HTTPException(
                    status_code=409,
                    detail="custom_field.code already exists for target entity storage table",
                )

            cur.execute("""
                INSERT INTO entity_table_custom_fields(
                    page_slug, table_index, target_entity, source_entities,
                    name, code, description, field_type, editor,
                    storage_entity_key, storage_table, storage_column, storage_pg_type,
                    created_at, updated_at, created_by, updated_by
                )
                VALUES (%s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), %s, %s)
                RETURNING id, name, code, description, field_type, editor, target_entity,
                          source_entities, storage_table, storage_column, storage_pg_type
            """, (
                data["page_slug"],
                data["table_index"],
                json.dumps(data["target_entity"], ensure_ascii=False),
                json.dumps(data["source_entities"], ensure_ascii=False),
                data["custom_field"]["name"],
                data["custom_field"]["code"],
                data["custom_field"]["description"],
                data["custom_field"]["type"],
                data["custom_field"]["editor"],
                storage_target["storage_entity_key"],
                storage_target["storage_table"],
                storage_column,
                storage_pg_type,
                actor,
                actor,
            ))
            row = cur.fetchone() or {}
        recalc_result = None
        if recalculate_now:
            recalc_result = _entity_table_recalculate_custom_field_editor(conn, row)
        elif debug_stub_fill:
            recalc_result = _entity_table_recalculate_custom_field_stub(conn, row)
        conn.commit()
        out = {
            "ok": True,
            "custom_field": _entity_table_custom_field_row_to_api(row),
        }
        if recalc_result is not None:
            out["recalculate_result"] = recalc_result
        return out
    except psycopg2.errors.UniqueViolation:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(
            status_code=409,
            detail="custom_field.code already exists for this page_slug and table_index",
        )
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.get("/api/entity-table/custom-fields")
def list_entity_table_custom_fields(
    page_slug: str = Query(..., description="Unique page slug"),
    table_index: int = Query(..., description="Table index inside page config"),
):
    slug = str(page_slug or "").strip()
    if not slug:
        raise HTTPException(status_code=400, detail="page_slug is required")

    conn = pg_conn()
    try:
        _ensure_entity_table_custom_fields_schema(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, code, description, field_type, editor, target_entity,
                       storage_table, storage_column, storage_pg_type
                FROM entity_table_custom_fields
                WHERE page_slug=%s AND table_index=%s
                ORDER BY created_at ASC, id ASC
            """, (slug, int(table_index)))
            rows = cur.fetchall() or []
        return {"ok": True, "items": [_entity_table_custom_field_row_to_api(r) for r in rows]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/api/entity-table/custom-fields/preview")
async def preview_entity_table_custom_field(request: Request):
    if _entity_table_is_guest(request):
        return _entity_table_error_response(403, "Editor preview failed", "Guest is not allowed to preview custom fields")

    conn = None
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}

        data = _entity_table_validate_custom_field_preview_payload(payload)
        storage_target = _entity_table_resolve_storage_target(data["target_entity"])
        pseudo_row = {
            "id": None,
            "page_slug": data["page_slug"],
            "table_index": data["table_index"],
            "editor": data["editor"],
            "target_entity": data["target_entity"],
            "source_entities": data["source_entities"],
            "storage_entity_key": storage_target["storage_entity_key"],
            "storage_table": storage_target["storage_table"],
            "storage_column": None,
            "storage_pg_type": "TEXT",
        }

        conn = pg_conn()
        conn.autocommit = False
        _ensure_entity_table_custom_fields_schema(conn)
        preview_result = _entity_table_preview_custom_field_editor(conn, pseudo_row)
        try:
            conn.rollback()  # no writes expected; keep transaction clean
        except Exception:
            pass
        return {"ok": True, "preview_result": preview_result}
    except HTTPException as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        return _entity_table_error_response(e.status_code if getattr(e, "status_code", None) else 400, "Editor preview failed", _entity_table_http_error_text(e))
    except Exception as e:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        try:
            print(f"ERROR: preview_entity_table_custom_field: {e}", file=sys.stderr, flush=True)
            traceback.print_exc()
        except Exception:
            pass
        return _entity_table_error_response(500, "Editor preview failed", "Internal error while evaluating preview")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


@app.put("/api/entity-table/custom-fields/{custom_field_id}")
@app.patch("/api/entity-table/custom-fields/{custom_field_id}")
async def update_entity_table_custom_field(custom_field_id: str, request: Request):
    if _entity_table_is_guest(request):
        raise HTTPException(status_code=403, detail="Guest is not allowed to update custom fields")

    db_id = _entity_table_custom_field_parse_id(custom_field_id)
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    updates = _entity_table_validate_custom_field_update_payload(payload)
    actor = _entity_table_actor_from_request(request)

    conn = pg_conn()
    try:
        conn.autocommit = False
        _ensure_entity_table_custom_fields_schema(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, code, description, field_type, editor, target_entity,
                       storage_table, storage_column, storage_pg_type
                FROM entity_table_custom_fields
                WHERE id=%s
                LIMIT 1
            """, (db_id,))
            existing = cur.fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="custom field not found")

            requested_code = updates.pop("code_requested", None)
            if requested_code is not None and str(requested_code) != str(existing.get("code") or ""):
                raise HTTPException(status_code=400, detail="custom_field.code change is not supported yet")

            set_parts: List[str] = []
            params: List[Any] = []
            allowed_cols = ("name", "description", "field_type", "editor")
            for col in allowed_cols:
                if col in updates:
                    set_parts.append(f"{col}=%s")
                    params.append(updates[col])
            if not set_parts:
                raise HTTPException(status_code=400, detail="No updatable fields provided in custom_field")

            set_parts.append("updated_at=now()")
            set_parts.append("updated_by=%s")
            params.append(actor)
            params.append(db_id)

            cur.execute(f"""
                UPDATE entity_table_custom_fields
                SET {", ".join(set_parts)}
                WHERE id=%s
                RETURNING id, name, code, description, field_type, editor, target_entity,
                          storage_table, storage_column, storage_pg_type
            """, tuple(params))
            row = cur.fetchone()

        conn.commit()
        return {"ok": True, "custom_field": _entity_table_custom_field_row_to_api(row or existing)}
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/api/entity-table/custom-fields/{custom_field_id}/recalculate")
async def recalculate_entity_table_custom_field(custom_field_id: str, request: Request):
    if _entity_table_is_guest(request):
        raise HTTPException(status_code=403, detail="Guest is not allowed to recalculate custom fields")

    db_id = _entity_table_custom_field_parse_id(custom_field_id)
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    debug_stub_fill = bool(payload.get("debug_stub_fill"))

    conn = pg_conn()
    try:
        conn.autocommit = False
        _ensure_entity_table_custom_fields_schema(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, page_slug, table_index, code, editor, target_entity, source_entities,
                       storage_table, storage_column, storage_pg_type
                FROM entity_table_custom_fields
                WHERE id=%s
                LIMIT 1
            """, (db_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="custom field not found")

        if debug_stub_fill:
            result = _entity_table_recalculate_custom_field_stub(conn, row)
        else:
            result = _entity_table_recalculate_custom_field_editor(conn, row)
        conn.commit()
        return {
            "ok": True,
            "recalculate_result": {
                "updated_rows": int(result.get("updated_rows") or 0),
                "mode": result.get("mode") or ("stub_test_fill" if debug_stub_fill else "editor_eval"),
                "custom_field_id": _entity_table_custom_field_db_id_to_api(db_id),
            },
            "result": result,
        }
    except HTTPException as e:
        try:
            conn.rollback()
        except Exception:
            pass
        # Keep contract human-readable for frontend modal/alerts.
        err_text = _entity_table_http_error_text(e)
        err_name = "Editor eval failed" if (getattr(e, "status_code", 400) == 400) else "Custom field recalculate failed"
        return _entity_table_error_response(e.status_code if getattr(e, "status_code", None) else 400, err_name, err_text)
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.delete("/api/entity-table/custom-fields/{custom_field_id}")
def delete_entity_table_custom_field(custom_field_id: str, request: Request):
    if _entity_table_is_guest(request):
        raise HTTPException(status_code=403, detail="Guest is not allowed to delete custom fields")

    db_id = _entity_table_custom_field_parse_id(custom_field_id)
    actor = _entity_table_actor_from_request(request)

    conn = pg_conn()
    try:
        conn.autocommit = False
        _ensure_entity_table_custom_fields_schema(conn)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, code, target_entity, storage_table, storage_column
                FROM entity_table_custom_fields
                WHERE id=%s
                LIMIT 1
            """, (db_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="custom field not found")

            storage_table = str(row.get("storage_table") or "").strip()
            storage_column = str(row.get("storage_column") or row.get("code") or "").strip()
            if not storage_table:
                target_entity = row.get("target_entity") if isinstance(row.get("target_entity"), dict) else {}
                storage_table = _entity_table_resolve_storage_target(target_entity)["storage_table"]

            cur.execute("""
                UPDATE entity_table_custom_fields
                SET updated_at=now(), updated_by=%s
                WHERE id=%s
            """, (actor, db_id))
            cur.execute("DELETE FROM entity_table_custom_fields WHERE id=%s", (db_id,))
            deleted = cur.rowcount or 0
            if deleted > 0 and storage_table and storage_column:
                cur.execute("""
                    SELECT COUNT(*) AS cnt
                    FROM entity_table_custom_fields
                    WHERE storage_table=%s AND storage_column=%s
                """, (storage_table, storage_column))
                cnt_row = cur.fetchone() or {}
                refs_left = int(cnt_row.get("cnt") or 0)
            else:
                refs_left = 0
        if deleted > 0 and storage_table and storage_column and refs_left <= 0:
            _entity_table_drop_physical_custom_field_column(conn, storage_table, storage_column)
        conn.commit()
        if deleted <= 0:
            raise HTTPException(status_code=404, detail="custom field not found")
        return {"ok": True}
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.get("/debug/lists-elements")
def debug_lists_elements(
    iblock_id: int = Query(..., description="IBLOCK_ID списка (например 34 для Tracțiune)"),
):
    """
    Сырой ответ Bitrix lists.element.get — проверить, как в Bitrix записаны названия (Faa или Fața).
    GET /debug/lists-elements?iblock_id=34
    """
    try:
        data = b24.call("lists.element.get", {"IBLOCK_TYPE_ID": "lists", "IBLOCK_ID": iblock_id})
    except Exception as e:
        return {"ok": False, "error": str(e), "iblock_id": iblock_id}
    result = data.get("result")
    elements = result if isinstance(result, list) else (result.get("elements") or result.get("items") or []) if isinstance(result, dict) else []
    if not isinstance(elements, list):
        elements = []
    items = []
    for el in elements[:40]:
        if not isinstance(el, dict):
            continue
        name = el.get("NAME") or el.get("name") or el.get("VALUE") or el.get("value")
        items.append({
            "ID": el.get("ID") or el.get("id"),
            "NAME": name,
            "NAME_repr": repr(name) if name is not None else None,
            "NAME_codepoints": [ord(c) for c in str(name)[:15]] if name is not None else None,
        })
    return {"ok": True, "iblock_id": iblock_id, "count": len(elements), "items": items}


@app.get("/debug/smart-fields")
def debug_smart_fields(
    entity_type_id: int = Query(..., description="entityTypeId смарт-процесса, например 1114"),
):
    """
    Отладка: сырая структура полей из crm.item.fields для смарт-процесса.
    Возвращает поля, у которых есть settings или type list/enum — чтобы увидеть entityId и items.
    Вызов: GET /debug/smart-fields?entity_type_id=1114
    """
    try:
        fields = fetch_smart_fields(entity_type_id)
    except Exception as e:
        return {"ok": False, "error": str(e), "entity_type_id": entity_type_id}
    if not isinstance(fields, dict):
        return {"ok": True, "entity_type_id": entity_type_id, "fields_raw_type": type(fields).__name__, "fields_count": 0}
    # Интересуют поля: Transmisie, Tracțiune, Filiala и любые с type list/enum или с settings
    target_keys = {"ufCrm34_1748348015", "ufCrm34_1748431272", "ufCrm34_1748431413"}
    target_titles = ("transmisie", "tracțiune", "tractiune", "filiala", "tipul de combustibil")
    out = {}
    for fn, meta in fields.items():
        if not isinstance(meta, dict):
            continue
        title = (meta.get("title") or meta.get("formLabel") or meta.get("listLabel") or meta.get("b24_title") or "").lower()
        is_target = fn in target_keys or any(t in title for t in target_titles)
        has_settings = bool(meta.get("settings"))
        ftype = (meta.get("type") or "").lower()
        is_list_type = "list" in ftype or "enum" in ftype
        if not (is_target or has_settings or is_list_type):
            continue
        out[fn] = {
            "type": meta.get("type"),
            "title": meta.get("title") or meta.get("formLabel") or meta.get("listLabel"),
            "settings": meta.get("settings"),
            "entityId_from_settings": (meta.get("settings") or {}).get("entityId") if isinstance(meta.get("settings"), dict) else None,
            "listEntityId": (meta.get("settings") or {}).get("listEntityId") if isinstance(meta.get("settings"), dict) else None,
            "has_items": "items" in meta and bool(meta.get("items")),
            "items_keys": list(meta.get("items", {}).keys())[:10] if isinstance(meta.get("items"), dict) else (len(meta.get("items")) if isinstance(meta.get("items"), list) else None),
        }
    return {"ok": True, "entity_type_id": entity_type_id, "fields_count": len(fields), "relevant": out}


@app.get("/health")
def health():
    return {
        "ok": True,
        "auto_sync": {
            "enabled": AUTO_SYNC_ENABLED,
            "interval_sec": AUTO_SYNC_INTERVAL_SEC,
            "deal_limit": AUTO_SYNC_DEAL_LIMIT,
            "smart_limit": AUTO_SYNC_SMART_LIMIT,
            "time_budget_sec": SYNC_TIME_BUDGET_SEC,
        }
    }

@app.post("/sync/schema")
def sync_schema_endpoint():
    try:
        return sync_schema()
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))

@app.post("/sync/sources-classifier")
def sync_sources_classifier_endpoint():
    """
    Ручной запуск синхронизации классификатора источников.
    Заполняет b24_classifier_sources из enum значений поля источника сделок.
    """
    conn = pg_conn()
    try:
        print(f"INFO: sync_sources_classifier_endpoint: Starting manual sync", file=sys.stderr, flush=True)
        sync_sources_classifier(conn)
        # Проверяем, сколько записей в классификаторе
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM b24_classifier_sources")
            count = cur.fetchone()[0]
        print(f"INFO: sync_sources_classifier_endpoint: Completed. Total sources in classifier: {count}", file=sys.stderr, flush=True)
        return {
            "ok": True,
            "message": f"Sources classifier synced. Total sources: {count}",
            "count": count
        }
    except Exception as e:
        print(f"ERROR: sync_sources_classifier_endpoint: Exception: {e}", file=sys.stderr, flush=True)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


def _debug_bitrix_calls() -> Dict[str, Any]:
    """Пробует вызвать Bitrix API и возвращает ответы/ошибки для отладки (без записи в БД)."""
    out: Dict[str, Any] = {"categories": {}, "stages": {}, "deal_userfields": {}}
    # 1) Категории
    for method, params in [("crm.category.list", {"entityTypeId": 2}), ("crm.dealcategory.list", {})]:
        try:
            data = b24.call(method, params)
            err = data.get("error")
            result = data.get("result")
            if err:
                out["categories"][method] = {"error": err, "error_description": data.get("error_description", "")}
            else:
                rtype = type(result).__name__
                if isinstance(result, dict):
                    keys = list(result.keys())[:20]
                    out["categories"][method] = {"result_type": rtype, "result_keys": keys, "result_empty": not result}
                elif isinstance(result, list):
                    out["categories"][method] = {"result_type": rtype, "count": len(result), "result_empty": not result}
                else:
                    out["categories"][method] = {"result_type": rtype, "result_empty": not result}
        except Exception as e:
            out["categories"][method] = {"exception": str(e), "exception_type": type(e).__name__}
    # 2) Стадии
    try:
        data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": "DEAL_STAGE"}})
        err = data.get("error")
        result = data.get("result")
        if err:
            out["stages"] = {"error": err, "error_description": data.get("error_description", "")}
        else:
            rtype = type(result).__name__
            if isinstance(result, list):
                out["stages"] = {"result_type": rtype, "count": len(result), "result_empty": not result}
            elif isinstance(result, dict):
                out["stages"] = {"result_type": rtype, "result_keys": list(result.keys())[:20], "result_empty": not result}
            else:
                out["stages"] = {"result_type": rtype, "result_empty": not result}
    except Exception as e:
        out["stages"] = {"exception": str(e), "exception_type": type(e).__name__}
    # 3) Поля сделок (userfield.list) — дамп одного поля, чтобы увидеть ключи (listLabel и т.д.)
    try:
        data = b24.call("crm.deal.userfield.list", {})
        err = data.get("error")
        result = data.get("result")
        if err:
            out["deal_userfields"] = {"error": err, "error_description": data.get("error_description", "")}
        else:
            rtype = type(result).__name__
            sample: Dict[str, Any] = {}
            if isinstance(result, dict):
                keys = list(result.keys())[:30]
                sample["result_type"] = rtype
                sample["result_keys"] = keys
                sample["result_empty"] = not result
                # Полный объект одного поля (например UF_CRM_1733346976), чтобы увидеть listLabel/editFormLabel
                for k in ("UF_CRM_1733346976", "UF_CRM_1749211409067") + tuple(keys[:2]):
                    v = result.get(k) if isinstance(result, dict) else None
                    if isinstance(v, dict):
                        sample["sample_field_key"] = k
                        sample["sample_field"] = v
                        break
                if not sample.get("sample_field") and result and isinstance(result, dict):
                    first_key = next(iter(result.keys()), None)
                    if first_key:
                        sample["sample_field_key"] = first_key
                        sample["sample_field"] = result.get(first_key)
            elif isinstance(result, list) and result:
                sample["result_type"] = rtype
                sample["count"] = len(result)
                sample["result_empty"] = not result
                first = result[0] if isinstance(result[0], dict) else None
                if first:
                    sample["sample_field_key"] = first.get("fieldName") or first.get("FIELD_NAME") or "?"
                    sample["sample_field"] = first
            else:
                sample["result_type"] = rtype
                sample["result_empty"] = not result
            out["deal_userfields"] = sample
    except Exception as e:
        out["deal_userfields"] = {"exception": str(e), "exception_type": type(e).__name__}
    # 4) Типы сделок (DEAL_TYPE)
    try:
        data = b24.call("crm.status.list", {"filter": {"ENTITY_ID": "DEAL_TYPE"}})
        err = data.get("error")
        result = data.get("result")
        if err:
            out["deal_types"] = {"error": err, "error_description": data.get("error_description", "")}
        else:
            rtype = type(result).__name__
            if isinstance(result, list):
                out["deal_types"] = {"result_type": rtype, "count": len(result), "result_empty": not result}
            elif isinstance(result, dict):
                out["deal_types"] = {"result_type": rtype, "result_keys": list(result.keys())[:20], "result_empty": not result}
            else:
                out["deal_types"] = {"result_type": rtype, "result_empty": not result}
    except Exception as e:
        out["deal_types"] = {"exception": str(e), "exception_type": type(e).__name__}
    return out


def run_sync_reference_data() -> None:
    """Запускает синхронизацию справочников (воронки, стадии, типы сделок, enum, компании, названия полей UF_CRM_*) в БД."""
    conn = pg_conn()
    try:
        sync_deal_categories(conn)
        sync_deal_stages(conn)
        sync_smart_process_stages(conn)
        sync_deal_types(conn)
        sync_companies(conn)
        sync_field_enums(conn, "deal")
        sync_field_enums(conn, "contact")
        sync_field_enums(conn, "lead")
        sync_field_enums(conn, "company")
        with conn.cursor() as cur:
            cur.execute("SELECT entity_key FROM b24_meta_entities WHERE entity_key LIKE 'sp:%'")
            for row in cur.fetchall():
                sync_field_enums(conn, row[0])
        sync_userfield_titles(conn, "deal")
        sync_userfield_titles(conn, "contact")
        sync_userfield_titles(conn, "lead")
        sync_userfield_titles(conn, "company")
    except Exception as e:
        print(f"WARNING: run_sync_reference_data: {e}", file=sys.stderr, flush=True)
    finally:
        conn.close()


@app.post("/sync/reference-data")
def sync_reference_data_endpoint(debug: Optional[str] = Query(None, description="1 = только отладка Bitrix, без записи в БД")):
    """
    Синхронизирует справочники: воронки (b24_deal_categories), стадии (b24_deal_stages),
    enum-значения полей UF_CRM_* и др. (b24_field_enum) из Bitrix API.
    Вызывать после /sync/schema и при необходимости для обновления подписей.
    Параметр ?debug=1 — в ответ добавится debug с сырыми ответами/ошибками Bitrix (без записи в БД).
    """
    if debug == "1":
        try:
            debug_info = _debug_bitrix_calls()
            return {"ok": True, "message": "Debug only (no sync)", "debug": debug_info}
        except Exception as e:
            traceback.print_exc()
            return {"ok": False, "message": str(e), "debug_exception": repr(e)}
    conn = pg_conn()
    all_notes: List[str] = []
    try:
        cat_rows, cat_notes = sync_deal_categories(conn)
        all_notes.extend([f"categories: {n}" for n in cat_notes])
        stage_rows, stage_notes = sync_deal_stages(conn)
        all_notes.extend([f"stages: {n}" for n in stage_notes])
        sync_smart_process_stages(conn)
        sync_deal_types(conn)
        sync_companies(conn)
        sync_sources_from_status(conn)
        sync_sources_classifier(conn)
        enum_deal_n, enum_deal_notes = sync_field_enums(conn, "deal")
        all_notes.extend(enum_deal_notes)
        sync_field_enums(conn, "contact")
        sync_field_enums(conn, "lead")
        sync_field_enums(conn, "company")
        with conn.cursor() as cur:
            cur.execute("SELECT entity_key FROM b24_meta_entities WHERE entity_key LIKE 'sp:%'")
            for row in cur.fetchall():
                sync_field_enums(conn, row[0])
        titles_deal = sync_userfield_titles(conn, "deal")
        titles_contact = sync_userfield_titles(conn, "contact")
        titles_lead = sync_userfield_titles(conn, "lead")
        sync_userfield_titles(conn, "company")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM b24_deal_categories")
            cat_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM b24_deal_stages")
            stage_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM b24_field_enum")
            enum_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM b24_classifier_sources")
            sources_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM b24_crm_company")
            companies_count = cur.fetchone()[0]
        out = {
            "ok": True,
            "message": "Reference data synced",
            "categories": cat_count,
            "stages": stage_count,
            "sources": sources_count,
            "companies": companies_count,
            "field_enum_values": enum_count,
            "userfield_titles_updated": {"deal": titles_deal, "contact": titles_contact, "lead": titles_lead},
        }
        if cat_count == 0 or stage_count == 0 or enum_count == 0:
            out["debug_notes"] = all_notes
        return out
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


@app.get("/api/data/sources-classifier")
def get_sources_classifier():
    """
    Возвращает классификатор источников (sursa) из базы данных.
    Используется для получения mapping ID -> название источника.
    """
    conn = pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT source_id, source_name
                FROM b24_classifier_sources
                ORDER BY source_id
            """)
            rows = cur.fetchall()
        
        # Формируем словарь для удобства использования
        classifier = {}
        for row in rows:
            classifier[str(row["source_id"])] = str(row["source_name"])
        
        return {
            "ok": True,
            "count": len(classifier),
            "classifier": classifier,
            "sources": [{"id": row["source_id"], "name": row["source_name"]} for row in rows]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


    # Достаём entityTypeId и itemId из разных форматов payload Bitrix
    data = payload.get("data") or {}
    fields = data.get("FIELDS") or data.get("fields") or {}

    raw_entity_type_id = (
        payload.get("entityTypeId")
        or payload.get("entity_type_id")
        or data.get("entityTypeId")
        or data.get("entity_type_id")
    )
    raw_item_id = (
        payload.get("id")
        or payload.get("item_id")
        or fields.get("ID")
        or fields.get("id")
    )

    try:
        entity_type_id = int(raw_entity_type_id)
        item_id = int(raw_item_id)
    except Exception:
        raise HTTPException(status_code=400, detail="entityTypeId or item ID is missing/invalid")

    print(f"INFO: webhook_b24_dynamic_item_update: entityTypeId={entity_type_id}, id={item_id}", file=sys.stderr, flush=True)

    resp = b24.call("crm.item.get", {"entityTypeId": entity_type_id, "id": item_id})
    if not isinstance(resp, dict):
        raise HTTPException(status_code=502, detail="crm.item.get returned invalid response")

    result = resp.get("result") if isinstance(resp.get("result"), dict) else resp.get("result")
    item = None
    if isinstance(result, dict) and "item" in result:
        item = result["item"]
    elif isinstance(result, dict):
        item = result

    if not isinstance(item, dict):
        raise HTTPException(status_code=502, detail="crm.item.get returned no item")

    return upsert_single_smart_item(entity_type_id, item)

@app.post("/sync/data")
def sync_data_endpoint(deal_limit: int = 0, smart_limit: int = 0, time_budget_sec: int = SYNC_TIME_BUDGET_SEC, contact_limit: int = 0, lead_limit: int = 0):
    """
    deal_limit=0 / smart_limit=0 => unlimited (eventually), but still time_budget_sec applies to avoid 429.
    """
    try:
        return sync_data(deal_limit=deal_limit, smart_limit=smart_limit, time_budget_sec=time_budget_sec, contact_limit=contact_limit, lead_limit=lead_limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))

@app.post("/sync/data/full")
def sync_data_full_endpoint():
    """
    Принудительная полная синхронизация всех сделок и smart processes.
    Запускается в фоновом потоке, чтобы не блокировать ответ.
    Возвращает сразу, синхронизация продолжается в фоне.
    """
    def _full_sync():
        try:
            print("INFO: sync_data_full_endpoint: Starting full sync in background...", file=sys.stderr, flush=True)
            # Полная синхронизация без ограничений по времени и количеству
            result = sync_data(
                deal_limit=0,  # Без ограничений
                smart_limit=0,  # Без ограничений
                time_budget_sec=3600,  # 1 час на синхронизацию
                contact_limit=0,  # Без ограничений
                lead_limit=0  # Без ограничений
            )
            print(f"INFO: sync_data_full_endpoint: Full sync completed: {result}", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"ERROR: sync_data_full_endpoint: Full sync failed: {e}", file=sys.stderr, flush=True)
            traceback.print_exc()
    
    # Запускаем в отдельном потоке
    t = threading.Thread(target=_full_sync, daemon=True)
    t.start()
    
    return {
        "ok": True,
        "message": "Full sync started in background. Check logs for progress.",
        "note": "This may take several minutes depending on the number of deals."
    }

@app.post("/sync/update-assigned-by-names")
def update_assigned_by_names_endpoint(limit: int = 1000, time_budget_sec: int = 60):
    """
    Принудительно обновляет assigned_by_name для всех сделок через Bitrix API.
    Обрабатывает сделки, у которых есть assigned_by_id, но нет assigned_by_name.
    """
    conn = pg_conn()
    try:
        table = table_name_for_entity("deal")
        global _user_name_cache
        _user_name_cache.clear()
        
        # Получаем список сделок, у которых есть assigned_by_id, но нет assigned_by_name
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id, assigned_by_id
                FROM {table}
                WHERE assigned_by_id IS NOT NULL
                  AND assigned_by_name IS NULL
                ORDER BY id DESC
                LIMIT %s
            """, (limit,))
            deals_to_update = cur.fetchall()
        
        if not deals_to_update:
            return {"ok": True, "message": "No deals need updating", "updated": 0}
        
        updated = 0
        start_time = time.time()
        
        for deal_id, assigned_by_id in deals_to_update:
            # Проверяем time budget
            if time.time() - start_time >= time_budget_sec:
                print(f"INFO: update_assigned_by_names: Time budget exceeded, stopping. Updated {updated}/{len(deals_to_update)}", file=sys.stderr, flush=True)
                break
            
            user_id_str = str(assigned_by_id).strip()
            
            # Проверяем кэш
            if user_id_str in _user_name_cache:
                assigned_by_name = _user_name_cache[user_id_str]
            else:
                try:
                    user_resp = b24.call("user.get", {"ID": user_id_str})
                    if user_resp and "result" in user_resp and len(user_resp["result"]) > 0:
                        user = user_resp["result"][0]
                        name = user.get("NAME", "").strip()
                        last_name = user.get("LAST_NAME", "").strip()
                        if name and last_name:
                            assigned_by_name = f"{name} {last_name}"
                        elif name:
                            assigned_by_name = name
                        elif last_name:
                            assigned_by_name = last_name
                        elif user.get("FULL_NAME"):
                            assigned_by_name = str(user.get("FULL_NAME")).strip()
                        elif user.get("LOGIN"):
                            assigned_by_name = str(user.get("LOGIN")).strip()
                        else:
                            assigned_by_name = None
                        _user_name_cache[user_id_str] = assigned_by_name or user_id_str
                    else:
                        assigned_by_name = None
                        _user_name_cache[user_id_str] = user_id_str
                except Exception as e:
                    print(f"WARNING: Failed to get user name for deal {deal_id}, user_id {user_id_str}: {e}", file=sys.stderr, flush=True)
                    assigned_by_name = None
                    _user_name_cache[user_id_str] = user_id_str
            
            # Обновляем в базе
            if assigned_by_name and assigned_by_name != user_id_str:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {table}
                        SET assigned_by_name = %s
                        WHERE id = %s
                    """, (assigned_by_name, deal_id))
                    conn.commit()
                    updated += 1
                try:
                    _upsert_b24_user(conn, int(assigned_by_id), assigned_by_name)
                except Exception:
                    pass
        
        return {"ok": True, "updated": updated, "total": len(deals_to_update)}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()


def _collect_user_ids_from_tables(conn) -> List[int]:
    """Собрать все уникальные user ID из колонок deal/contact/lead (assigned_by_id, created_by_id и т.д.)."""
    user_cols = ["assigned_by_id", "created_by_id", "modified_by_id", "last_activity_by", "moved_by_id"]
    tables = [
        ("b24_crm_deal", user_cols),
        ("b24_crm_contact", user_cols),
        ("b24_crm_lead", user_cols),
    ]
    seen: set = set()
    with conn.cursor() as cur:
        for table, cols in tables:
            try:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = %s AND column_name = ANY(%s)
                """, (table, cols))
                existing = [r[0] for r in cur.fetchall() if r and r[0]]
                if not existing:
                    continue
                for col in existing:
                    cur.execute(f'SELECT DISTINCT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL')
                    for row in cur.fetchall() or []:
                        if row and row[0] is not None:
                            try:
                                uid = int(row[0])
                                if uid > 0:
                                    seen.add(uid)
                            except (TypeError, ValueError):
                                pass
            except Exception as e:
                print(f"WARNING: _collect_user_ids_from_tables {table}: {e}", file=sys.stderr, flush=True)
    return list(seen)


def _user_record_to_name(u: Dict[str, Any]) -> Optional[str]:
    """Из ответа user.get собрать имя пользователя."""
    name = u.get("NAME", "").strip()
    last = u.get("LAST_NAME", "").strip()
    if name and last:
        return f"{name} {last}"
    if name:
        return name
    if last:
        return last
    return (u.get("FULL_NAME") or u.get("LOGIN") or "").strip() or None


def sync_all_users_from_bitrix(conn, time_budget_sec: int = 600) -> Dict[str, Any]:
    """
    Загрузить всех пользователей из Bitrix user.get (пагинация start=0, 50, 100, ...)
    и заполнить b24_users. Один вызов — полный справочник пользователей.
    """
    started = time.time()
    synced = 0
    start = 0
    page_size = 50
    while time.time() - started < time_budget_sec:
        try:
            resp = b24.call("user.get", {"start": start})
            result = resp.get("result") if isinstance(resp, dict) else []
            if not isinstance(result, list):
                break
            if not result:
                break
            for u in result:
                uid = u.get("ID")
                if uid is None:
                    continue
                try:
                    uid_int = int(uid)
                except (TypeError, ValueError):
                    continue
                full = _user_record_to_name(u)
                if full:
                    _upsert_b24_user(conn, uid_int, full)
                    synced += 1
            if len(result) < page_size:
                break
            start += page_size
        except Exception as e:
            print(f"WARNING: sync_all_users_from_bitrix start={start}: {e}", file=sys.stderr, flush=True)
            break
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM b24_users")
        cached = cur.fetchone()[0] if cur.rowcount else 0
    return {"ok": True, "synced": synced, "total_in_cache": cached, "mode": "all"}


def sync_users_into_cache(conn, limit: int = 500, time_budget_sec: int = 120) -> Dict[str, Any]:
    """
    Заполнить b24_users из Bitrix user.get для тех user ID, которых ещё нет в кэше
    (только ID, встречающиеся в сделках/контактах/лидах).
    Для загрузки всех пользователей сразу используйте POST /sync/users?all=1
    """
    started = time.time()
    all_ids = _collect_user_ids_from_tables(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM b24_users")
        cached = {int(r[0]) for r in cur.fetchall() if r and r[0] is not None}
    missing = [uid for uid in all_ids if uid not in cached][:limit]
    if not missing:
        return {"ok": True, "synced": 0, "total_missing": 0, "cached": len(cached)}

    synced = 0
    batch_size = 50
    for i in range(0, len(missing), batch_size):
        if time.time() - started >= time_budget_sec:
            break
        batch = missing[i : i + batch_size]
        ids_str = ",".join(str(x) for x in batch)
        try:
            resp = b24.call("user.get", {"ID": ids_str})
            result = resp.get("result") if isinstance(resp, dict) else []
            if not isinstance(result, list):
                continue
            for u in result:
                uid = u.get("ID")
                if uid is None:
                    continue
                try:
                    uid_int = int(uid)
                except (TypeError, ValueError):
                    continue
                full = _user_record_to_name(u)
                if full:
                    _upsert_b24_user(conn, uid_int, full)
                    synced += 1
        except Exception as e:
            print(f"WARNING: sync_users_into_cache batch {ids_str}: {e}", file=sys.stderr, flush=True)
    return {"ok": True, "synced": synced, "total_missing": len(missing), "cached": len(cached)}


@app.post("/sync/users")
def sync_users_endpoint(
    all_users: bool = False,
    limit: int = 500,
    time_budget_sec: int = 120,
):
    """
    Заполнить b24_users (кэш имён пользователей) из Bitrix.

    - all_users=0 (по умолчанию): только те user ID, что есть в сделках/контактах/лидах и ещё не в кэше.
    - all_users=1: загрузить всех пользователей из Bitrix сразу (user.get с пагинацией).

    Пример: curl -X POST "http://127.0.0.1:7070/sync/users?all_users=1"
    """
    conn = pg_conn()
    try:
        if all_users:
            result = sync_all_users_from_bitrix(conn, time_budget_sec=min(time_budget_sec, 600))
        else:
            result = sync_users_into_cache(conn, limit=limit, time_budget_sec=time_budget_sec)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))
    finally:
        conn.close()



