import os
import re
import sys
import time
import json
from datetime import datetime, timezone
from textwrap import shorten
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import psycopg2
import requests
from fastapi import APIRouter, HTTPException
from psycopg2 import sql
from reportlab.graphics import renderPM
from reportlab.graphics.shapes import Drawing, Line, String

router = APIRouter(prefix="/api/data/reports/car_request", tags=["car-request-reports"])

BITRIX_WEBHOOK = os.getenv(
    "BITRIX_WEBHOOK",
    "https://nobilauto.bitrix24.ru/rest/18397/h5c7kw97sfp3uote",
).strip().rstrip("/")
BITRIX_WEBHOOK_REPORTS = os.getenv(
    "BITRIX_WEBHOOK_REPORTS",
    "https://nobilauto.bitrix24.ru/rest/20532/grmoroz08bush0kp",
).strip().rstrip("/")

PG_HOST = os.getenv("PG_HOST", "194.33.40.197")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "crm")
PG_USER = os.getenv("PG_USER", "crm")
PG_PASS = os.getenv("PG_PASS", "crm")

AUTO_HOME_TABLE = os.getenv("AUTO_HOME_TABLE", "b24_sp_f_1168").strip() or "b24_sp_f_1168"
AUTO_HOME_CHAT_ID = os.getenv("AUTO_HOME_CHAT_ID", "chat2846").strip() or "chat2846"
AUTO_HOME_TZ = os.getenv("AUTO_HOME_TZ", os.getenv("REPORT_TZ", "Europe/Chisinau")).strip() or "Europe/Chisinau"
AUTO_HOME_SEND_HOUR = int(os.getenv("AUTO_HOME_SEND_HOUR", "20"))
AUTO_HOME_SEND_MINUTE = int(os.getenv("AUTO_HOME_SEND_MINUTE", "0"))
AUTO_HOME_ACTIVE_STATUS = os.getenv("AUTO_HOME_ACTIVE_STATUS", "").strip()

AUTO_HOME_MARK_DIR = os.getenv("AUTO_HOME_MARK_DIR", "/tmp").strip() or "/tmp"
AUTO_HOME_ENABLED = os.getenv("AUTO_HOME_ENABLED", "1") == "1"


def _log(msg: str) -> None:
    print(f"AUTO HOME PNG: {msg}", file=sys.stderr, flush=True)


def _pg_conn():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )
    try:
        conn.set_client_encoding("UTF8")
    except Exception:
        pass
    return conn


def _table_columns(conn, table_name: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        rows = cur.fetchall()
    return [str(r[0]) for r in rows]


def _pick_col(columns: List[str], *candidates: str) -> Optional[str]:
    by_lower = {c.lower(): c for c in columns}
    for candidate in candidates:
        found = by_lower.get(candidate.lower())
        if found:
            return found
    return None


def _coerce_text(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _as_datetime(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        s = s.replace("Z", "+00:00")
        try:
            d = datetime.fromisoformat(s)
            if d.tzinfo is None:
                return d.replace(tzinfo=timezone.utc)
            return d
        except Exception:
            return None
    return None


def _load_user_name_map(conn) -> Dict[str, str]:
    out: Dict[str, str] = {}

    # 1) Try crm_users first (as requested)
    try:
        cols = _table_columns(conn, "crm_users")
        if cols:
            id_col = _pick_col(cols, "id", "ID")
            name_col = _pick_col(cols, "name", "full_name", "fio", "username", "login", "title")
            if id_col and name_col:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("SELECT {}, {} FROM {}.{}").format(
                            sql.Identifier(id_col),
                            sql.Identifier(name_col),
                            sql.Identifier("public"),
                            sql.Identifier("crm_users"),
                        )
                    )
                    for uid, uname in cur.fetchall():
                        if uid is None:
                            continue
                        uname_txt = _coerce_text(uname)
                        if uname_txt:
                            out[str(uid).strip()] = uname_txt
    except Exception:
        pass

    # 2) Fallback/merge from b24_users
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name FROM b24_users")
            for uid, uname in cur.fetchall():
                if uid is None:
                    continue
                key = str(uid).strip()
                if key and key not in out:
                    uname_txt = _coerce_text(uname)
                    if uname_txt:
                        out[key] = uname_txt
    except Exception:
        pass

    return out


def _bitrix_user_name(user_id: str) -> str:
    uid = _normalize_id(user_id)
    if not uid:
        return ""
    webhook = BITRIX_WEBHOOK_REPORTS or BITRIX_WEBHOOK
    if not webhook:
        return ""
    try:
        r = requests.post(f"{webhook}/user.get.json", json={"ID": uid}, timeout=15)
        r.raise_for_status()
        data = r.json()
        users = data.get("result") if isinstance(data, dict) else None
        if not isinstance(users, list) or not users:
            return ""
        u = users[0] if isinstance(users[0], dict) else {}
        name = _coerce_text(u.get("NAME"))
        last = _coerce_text(u.get("LAST_NAME"))
        full = f"{name} {last}".strip()
        if full:
            return full
        return _coerce_text(u.get("FULL_NAME")) or _coerce_text(u.get("LOGIN"))
    except Exception:
        return ""


def _load_car_name_map(conn) -> Dict[str, str]:
    out: Dict[str, str] = {}
    try:
        cols = _table_columns(conn, "b24_sp_f_1114")
        if not cols:
            return out

        id_col = _pick_col(cols, "id", "ID")
        name_col = _pick_col(cols, "name", "NAME", "title", "TITLE")
        plate_col = _pick_col(cols, "ufcrm34_1748431574")
        brand_col = _pick_col(cols, "ufcrm34_1748347910")
        model_col = _pick_col(cols, "ufcrm34_1748431620")
        raw_col = _pick_col(cols, "raw", "RAW")
        if not id_col:
            return out

        with conn.cursor() as cur:
            select_cols = [id_col]
            if name_col:
                select_cols.append(name_col)
            if plate_col:
                select_cols.append(plate_col)
            if brand_col:
                select_cols.append(brand_col)
            if model_col:
                select_cols.append(model_col)
            if raw_col:
                select_cols.append(raw_col)

            cur.execute(
                sql.SQL(
                    "SELECT {} FROM {}.{}"
                ).format(
                    sql.SQL(", ").join(sql.Identifier(c) for c in select_cols),
                    sql.Identifier("public"),
                    sql.Identifier("b24_sp_f_1114"),
                )
            )
            for rec in cur.fetchall():
                row = dict(zip(select_cols, rec))
                cid = row.get(id_col)
                if cid is None:
                    continue
                cname_txt = _coerce_text(row.get(name_col)) if name_col else ""
                if not cname_txt:
                    brand = _coerce_text(row.get(brand_col)) if brand_col else ""
                    model = _coerce_text(row.get(model_col)) if model_col else ""
                    plate = _coerce_text(row.get(plate_col)) if plate_col else ""
                    cname_txt = " ".join(x for x in [brand, model, plate] if x).strip()
                if not cname_txt and raw_col:
                    cname_txt = _coerce_text(
                        _raw_get(row.get(raw_col), "title", "TITLE", "name", "NAME")
                    )
                if cname_txt:
                    out[str(cid).strip()] = cname_txt
    except Exception:
        pass
    return out


def _normalize_id(v: Any) -> str:
    s = _coerce_text(v)
    if not s:
        return ""
    m = re.search(r"\d+", s)
    return m.group(0) if m else s


def _canonical_text(v: Any) -> str:
    return re.sub(r"\s+", " ", _coerce_text(v)).strip().lower()


def _raw_get(raw_obj: Any, *keys: str) -> Any:
    if raw_obj is None:
        return None
    data = raw_obj
    if isinstance(raw_obj, str):
        try:
            data = json.loads(raw_obj)
        except Exception:
            return None
    if not isinstance(data, dict):
        return None
    for k in keys:
        if k in data and data.get(k) not in (None, "", []):
            return data.get(k)
        lk = k.lower()
        for dk, dv in data.items():
            if str(dk).lower() == lk and dv not in (None, "", []):
                return dv
    return None


def _bitrix_list_items(entity_type_id: int, select_fields: List[str], order_desc: bool = True, limit: int = 500) -> List[Dict[str, Any]]:
    webhook = BITRIX_WEBHOOK_REPORTS or BITRIX_WEBHOOK
    if not webhook:
        return []

    items: List[Dict[str, Any]] = []
    start = 0
    while True:
        payload: Dict[str, Any] = {
            "entityTypeId": entity_type_id,
            "select": select_fields,
            "order": {"createdTime": "DESC" if order_desc else "ASC"},
            "start": start,
        }
        r = requests.post(f"{webhook}/crm.item.list.json", json=payload, timeout=60)
        r.raise_for_status()
        data = r.json() if r.content else {}
        result = data.get("result") if isinstance(data, dict) else {}
        page_items = result.get("items") if isinstance(result, dict) else []
        if not isinstance(page_items, list) or not page_items:
            break
        for it in page_items:
            if isinstance(it, dict):
                items.append(it)
                if len(items) >= limit:
                    return items

        nxt = result.get("next") if isinstance(result, dict) else None
        if nxt is None:
            break
        try:
            start = int(nxt)
        except Exception:
            break
    return items


def _extract_assigned_from_title(title: str) -> str:
    t = _coerce_text(title)
    if not t:
        return ""
    low = t.lower()
    marker = " берет авто "
    pos = low.find(marker)
    if pos <= 0:
        return ""
    return t[:pos].strip()


def _extract_car_from_title(title: str) -> str:
    t = _coerce_text(title)
    if not t:
        return ""
    low = t.lower()
    start_marker = "берет авто "
    end_marker = " с целью"
    s = low.find(start_marker)
    if s < 0:
        return ""
    s += len(start_marker)
    e = low.find(end_marker, s)
    if e < 0:
        return t[s:].strip()
    return t[s:e].strip()


def _fetch_rows_from_bitrix() -> List[Dict[str, Any]]:
    select_fields = [
        "id",
        "title",
        "assignedByName",
        "assignedById",
        "createdTime",
        "ufCrm58_1757152826",
        "ufCrm58_1757154090",
        "ufCrm58_1758016604",
        "ufCrm58_1758016179",
        "ufCrm58_1761065549",
    ]
    items = _bitrix_list_items(1168, select_fields=select_fields, order_desc=True, limit=500)
    if not items:
        return []

    now_local = datetime.now(ZoneInfo(AUTO_HOME_TZ))

    rows: List[Dict[str, Any]] = []
    seen_cars: set[str] = set()
    for it in items:
        created_dt = _as_datetime(_raw_get(it, "createdTime", "dateCreate", "created_at", "CREATED_AT"))
        if created_dt is None:
            continue
        created_local = created_dt.astimezone(ZoneInfo(AUTO_HOME_TZ))
        days = max((now_local.date() - created_local.date()).days, 0)

        status_txt = _coerce_text(_raw_get(it, "ufCrm58_1758016179", "UFCRM58_1758016179"))
        if AUTO_HOME_ACTIVE_STATUS and status_txt and status_txt != AUTO_HOME_ACTIVE_STATUS:
            continue

        # Include only items where UF_CRM_58_1757154090 is empty.
        blocker_val = _raw_get(it, "ufCrm58_1757154090", "UFCRM58_1757154090")
        if blocker_val not in (None, "", []):
            if _coerce_text(blocker_val):
                continue

        title = _coerce_text(_raw_get(it, "title", "TITLE"))
        assigned_txt = _coerce_text(_raw_get(it, "assignedByName", "ASSIGNED_BY_NAME", "assigned_by_name"))
        if not assigned_txt:
            assigned_id = _normalize_id(_raw_get(it, "assignedById", "ASSIGNED_BY_ID", "assigned_by_id"))
            assigned_txt = _bitrix_user_name(assigned_id) or assigned_id
        goal_txt = _coerce_text(_raw_get(it, "ufCrm58_1758016604", "UFCRM58_1758016604")) or "-"

        car_raw = _raw_get(it, "ufCrm58_1757152826", "UFCRM58_1757152826")
        car_txt = _coerce_text(car_raw)
        # Keep vehicle from 1168; title parsing is only a readability fallback for CRM-link ids.
        if not car_txt or car_txt.isdigit():
            car_txt = _extract_car_from_title(title) or car_txt
        car_id = _normalize_id(car_txt)
        car_txt = car_txt or "-"

        dedupe_key = f"id:{car_id}" if car_id else f"name:{_canonical_text(car_txt)}"
        if dedupe_key in seen_cars:
            continue
        seen_cars.add(dedupe_key)

        rows.append(
            {
                "assigned": assigned_txt or "Fara responsabil",
                "car": car_txt,
                "goal": goal_txt,
                "created": created_local,
                "days": days,
            }
        )

    return rows


def _fetch_rows_from_db() -> List[Dict[str, Any]]:
    raise RuntimeError("DB source is disabled for this report. Use Bitrix source only.")


def _fetch_rows() -> List[Dict[str, Any]]:
    return _fetch_rows_from_bitrix()


def _build_png(rows: List[Dict[str, Any]]) -> bytes:
    title_date = datetime.now(ZoneInfo(AUTO_HOME_TZ)).strftime("%d.%m.%y")
    title = f"Masini pe acasa {title_date}"

    row_block_h = 86
    header_h = 70
    footer_h = 30
    width = 1100
    height = header_h + (max(len(rows), 1) * row_block_h) + footer_h

    d = Drawing(width, height)
    d.add(Line(0, 0, width, 0, strokeColor="#d9d9d9", strokeWidth=1))

    d.add(String(40, height - 46, title, fontName="Helvetica-Bold", fontSize=40, fillColor="#e67e22"))

    y = height - header_h
    if not rows:
        d.add(String(40, y - 12, "Nu exista inregistrari pentru raport.", fontName="Helvetica", fontSize=26, fillColor="#333333"))
    else:
        for row in rows:
            first = f"- {shorten(row['assigned'], width=34, placeholder='...')} - {shorten(row['car'], width=34, placeholder='...')}"
            created_txt = row["created"].strftime("%d.%m.%Y %H:%M")
            second = f"Motiv - {shorten(row['goal'], width=28, placeholder='...')} - Luat: {created_txt} - {row['days']} zile"

            d.add(String(40, y - 10, first, fontName="Helvetica-Bold", fontSize=30, fillColor="#c0392b"))
            d.add(String(40, y - 46, second, fontName="Helvetica-Bold", fontSize=26, fillColor="#111111"))
            d.add(Line(40, y - 72, width - 40, y - 72, strokeColor="#b0b0b0", strokeWidth=1))
            y -= row_block_h

    png_data = renderPM.drawToString(d, fmt="PNG")
    return png_data


def _bitrix_post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    webhook = BITRIX_WEBHOOK_REPORTS or BITRIX_WEBHOOK
    url = f"{webhook}/{method}.json"
    r = requests.post(url, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"Bitrix error {data.get('error')}: {data.get('error_description')}")
    return data if isinstance(data, dict) else {"result": data}


def _send_png_to_chat(png_bytes: bytes, filename: str, caption: str) -> Dict[str, Any]:
    chat_raw = AUTO_HOME_CHAT_ID
    dialog_id = chat_raw if chat_raw.startswith("chat") else f"chat{chat_raw}"
    chat_id_for_commit: Any = chat_raw
    if chat_raw.startswith("chat") and chat_raw[4:].isdigit():
        chat_id_for_commit = int(chat_raw[4:])
    elif chat_raw.isdigit():
        chat_id_for_commit = int(chat_raw)

    folder = _bitrix_post("im.disk.folder.get", {"DIALOG_ID": dialog_id})
    folder_result = folder.get("result") or {}
    folder_id = str(folder_result.get("ID") or folder_result.get("id") or "").strip()
    if not folder_id:
        raise RuntimeError(f"im.disk.folder.get returned empty folder id: {folder}")

    init = _bitrix_post("disk.folder.uploadfile", {"id": folder_id, "NAME": filename})
    init_result = init.get("result") or {}
    upload_url = str(init_result.get("uploadUrl") or init_result.get("upload_url") or "").strip()
    field_name = str(init_result.get("field") or "file").strip() or "file"
    if not upload_url:
        raise RuntimeError(f"disk.folder.uploadfile did not return uploadUrl: {init}")

    upload_resp = requests.post(
        upload_url,
        files={field_name: (filename, png_bytes, "image/png")},
        timeout=120,
    )
    upload_resp.raise_for_status()
    up_json = upload_resp.json() if "application/json" in (upload_resp.headers.get("content-type") or "") else {}

    file_id = ""
    if isinstance(up_json, dict):
        result = up_json.get("result")
        if isinstance(result, dict):
            file_id = str(result.get("ID") or result.get("id") or "").strip()
        if not file_id:
            file_id = str(up_json.get("ID") or up_json.get("id") or "").strip()

    if not file_id:
        raise RuntimeError(f"upload url response has no file id: {str(up_json)[:300]}")

    commit_payload: Dict[str, Any] = {"CHAT_ID": chat_id_for_commit, "FILE_ID": file_id}
    if caption:
        commit_payload["COMMENT"] = caption
    commit = _bitrix_post("im.disk.file.commit", commit_payload)

    if caption:
        try:
            _bitrix_post("im.message.add", {"DIALOG_ID": dialog_id, "MESSAGE": caption})
        except Exception as e:
            _log(f"warning: preview message failed: {e}")

    return {
        "ok": True,
        "dialog_id": dialog_id,
        "folder_id": folder_id,
        "file_id": file_id,
        "commit": commit,
    }


def generate_and_send_auto_home_png() -> Dict[str, Any]:
    rows = _fetch_rows()
    png = _build_png(rows)
    now_local = datetime.now(ZoneInfo(AUTO_HOME_TZ))

    filename = f"masini_pe_acasa_{now_local.strftime('%Y%m%d_%H%M')}.png"
    caption = (
        f"Masini pe acasa {now_local.strftime('%d.%m.%Y')}"
        f" | inregistrari: {len(rows)}"
    )
    send_meta = _send_png_to_chat(png, filename=filename, caption=caption)

    return {
        "ok": True,
        "rows": len(rows),
        "filename": filename,
        "chat_id": AUTO_HOME_CHAT_ID,
        "send": send_meta,
    }


@router.post("/auto_home/png/send")
def send_auto_home_png_now() -> Dict[str, Any]:
    try:
        return generate_and_send_auto_home_png()
    except Exception as e:
        _log(f"manual send failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/auto_home/png/send")
def send_auto_home_png_now_get() -> Dict[str, Any]:
    try:
        return generate_and_send_auto_home_png()
    except Exception as e:
        _log(f"manual send failed (GET): {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _ensure_mark_dir(path: str) -> str:
    if path != "/tmp" and not os.path.isdir(path):
        try:
            os.makedirs(path, exist_ok=True)
        except Exception as e:
            _log(f"cannot create mark dir {path}: {e}; fallback /tmp")
            return "/tmp"
    return path


def _daily_auto_home_png_thread() -> None:
    check_interval_sec = 30
    mark_dir = _ensure_mark_dir(AUTO_HOME_MARK_DIR)

    while True:
        try:
            now_local = datetime.now(ZoneInfo(AUTO_HOME_TZ))
            day_key = now_local.strftime("%Y-%m-%d")
            mark_file = os.path.join(mark_dir, f"auto_home_png_sent_{day_key}.mark")

            if now_local.hour == AUTO_HOME_SEND_HOUR and now_local.minute >= AUTO_HOME_SEND_MINUTE:
                if os.path.exists(mark_file):
                    time.sleep(check_interval_sec)
                    continue

                try:
                    with open(mark_file, "x", encoding="utf-8") as f:
                        f.write(now_local.isoformat())
                except FileExistsError:
                    time.sleep(check_interval_sec)
                    continue

                try:
                    result = generate_and_send_auto_home_png()
                    _log(f"sent scheduled PNG successfully: rows={result.get('rows')} file={result.get('filename')}")
                except Exception as e:
                    _log(f"scheduled send failed: {e}")
                    try:
                        os.remove(mark_file)
                    except Exception:
                        pass
        except Exception as e:
            _log(f"scheduler loop error: {e}")

        time.sleep(check_interval_sec)


def start_auto_home_png_scheduler() -> bool:
    if not AUTO_HOME_ENABLED:
        _log("disabled by AUTO_HOME_ENABLED=0")
        return False

    import threading

    t = threading.Thread(target=_daily_auto_home_png_thread, daemon=True)
    t.start()
    _log(
        "daily scheduler started "
        f"at {AUTO_HOME_SEND_HOUR:02d}:{AUTO_HOME_SEND_MINUTE:02d} {AUTO_HOME_TZ}; "
        f"table=public.{AUTO_HOME_TABLE}; chat={AUTO_HOME_CHAT_ID}"
    )
    return True

