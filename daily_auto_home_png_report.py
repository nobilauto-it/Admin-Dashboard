import os
import sys
import time
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


def _fetch_rows() -> List[Dict[str, Any]]:
    with _pg_conn() as conn:
        cols = _table_columns(conn, AUTO_HOME_TABLE)
        if not cols:
            raise RuntimeError(f"Table public.{AUTO_HOME_TABLE} not found or has no columns")

        assigned_name_col = _pick_col(cols, "assigned_by_name", "ASSIGNED_BY_NAME")
        assigned_id_col = _pick_col(cols, "assigned_by_id", "ASSIGNED_BY_ID", "created_by_id", "CREATED_BY_ID")
        car_col = _pick_col(cols, "ufcrm58_1757152826", "UFCRM58_1757152826")
        goal_col = _pick_col(cols, "ufcrm58_1758016604", "UFCRM58_1758016604")
        created_col = _pick_col(cols, "created_at", "CREATED_AT", "date_create", "DATE_CREATE")
        closed_col = _pick_col(cols, "closed", "CLOSED")

        if not car_col or not goal_col or not created_col:
            raise RuntimeError(
                "Required columns are missing in public."
                f"{AUTO_HOME_TABLE}. Need UFCRM58_1757152826, UFCRM58_1758016604, created_at"
            )

        selected_cols: List[str] = []
        if assigned_name_col:
            selected_cols.append(assigned_name_col)
        elif assigned_id_col:
            selected_cols.append(assigned_id_col)
        selected_cols.extend([car_col, goal_col, created_col])

        parts = [
            sql.SQL("SELECT {}").format(
                sql.SQL(", ").join(sql.Identifier(c) for c in selected_cols)
            ),
            sql.SQL("FROM {}.{}").format(sql.Identifier("public"), sql.Identifier(AUTO_HOME_TABLE)),
            sql.SQL("WHERE {} IS NOT NULL").format(sql.Identifier(created_col)),
            sql.SQL("AND {} IS NOT NULL").format(sql.Identifier(car_col)),
            sql.SQL("AND btrim(CAST({} AS text)) <> ''").format(sql.Identifier(car_col)),
        ]

        if closed_col:
            parts.append(sql.SQL("AND ({} IS NULL OR CAST({} AS text) <> 'Y')").format(sql.Identifier(closed_col), sql.Identifier(closed_col)))

        parts.append(sql.SQL("ORDER BY {} ASC").format(sql.Identifier(created_col)))
        parts.append(sql.SQL("LIMIT 500"))

        query = sql.SQL(" ").join(parts)
        with conn.cursor() as cur:
            cur.execute(query)
            raw = cur.fetchall()

        user_name_by_id: Dict[str, str] = {}
        if assigned_name_col is None and assigned_id_col:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT id, name FROM b24_users")
                    for uid, uname in cur.fetchall():
                        if uid is None:
                            continue
                        user_name_by_id[str(uid).strip()] = _coerce_text(uname)
            except Exception:
                user_name_by_id = {}

    now_local = datetime.now(ZoneInfo(AUTO_HOME_TZ))
    rows: List[Dict[str, Any]] = []
    for r in raw:
        if len(r) == 4:
            assigned_raw, car, goal, created = r
        else:
            assigned_raw = None
            car, goal, created = r

        created_dt = _as_datetime(created)
        if created_dt is None:
            continue
        created_local = created_dt.astimezone(ZoneInfo(AUTO_HOME_TZ))
        days = max((now_local.date() - created_local.date()).days, 0)

        assigned_txt = _coerce_text(assigned_raw)
        if assigned_name_col is None and assigned_txt:
            assigned_txt = user_name_by_id.get(assigned_txt, assigned_txt)

        rows.append(
            {
                "assigned": assigned_txt or "Fara responsabil",
                "car": _coerce_text(car) or "-",
                "goal": _coerce_text(goal) or "-",
                "created": created_local,
                "days": days,
            }
        )

    return rows


def _build_png(rows: List[Dict[str, Any]]) -> bytes:
    title_date = datetime.now(ZoneInfo(AUTO_HOME_TZ)).strftime("%d.%m.%y")
    title = f"Masini pe acasa {title_date}"

    row_block_h = 68
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
            d.add(String(40, y - 42, second, fontName="Helvetica-Bold", fontSize=26, fillColor="#111111"))
            d.add(Line(40, y - 56, width - 40, y - 56, strokeColor="#b0b0b0", strokeWidth=1))
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
