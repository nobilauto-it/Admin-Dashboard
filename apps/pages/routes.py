import json
import re
import traceback
import unicodedata
from pathlib import Path

from apps.pages import blueprint
from flask import current_app, jsonify, make_response, render_template, request
from jinja2 import TemplateNotFound

try:
    import requests
except ImportError:
    requests = None

PAGES_DIR = Path(__file__).resolve().parent.parent / "templates" / "pages"
PAGES_REGISTRY = PAGES_DIR / ".generated_pages.json"
APPS_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
READ_ONLY_PAGES_FILE = APPS_DATA_DIR / "read_only_pages.json"


def _read_only_pages_list():
    """Список page_slug страниц в режиме «только просмотр» (дэшборды)."""
    if not READ_ONLY_PAGES_FILE.exists():
        return []
    try:
        with open(READ_ONLY_PAGES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _add_read_only_page(slug):
    """Добавить slug в список read-only страниц."""
    slugs = _read_only_pages_list()
    if slug in slugs:
        return
    slugs.append(slug)
    APPS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(READ_ONLY_PAGES_FILE, "w", encoding="utf-8") as f:
        json.dump(slugs, f, ensure_ascii=False, indent=2)


def _ensure_entity_table_config_columns():
    """Добавить в entity_table_config отсутствующие колонки (миграция для старых БД)."""
    from apps import db
    from sqlalchemy import text

    try:
        with db.engine.connect() as conn:
            r = conn.execute(text("PRAGMA table_info(entity_table_config)"))
            rows = r.fetchall()
    except Exception:
        return
    existing = {row[1] for row in rows} if rows else set()
    columns_to_add = [
        ("table_title", "TEXT"),
        ("table_description", "TEXT"),
        ("entities", "TEXT"),
        ("fields", "TEXT"),
        ("tables", "TEXT"),
        ("updated_at", "DATETIME"),
    ]
    for col_name, col_type in columns_to_add:
        if col_name in existing:
            continue
        try:
            with db.engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE entity_table_config ADD COLUMN {col_name} {col_type}"))
                conn.commit()
            current_app.logger.info("entity_table_config: добавлена колонка %s", col_name)
        except Exception as e:
            current_app.logger.warning("entity_table_config ADD COLUMN %s: %s", col_name, e)


def _read_registry():
    if not PAGES_REGISTRY.exists():
        return []
    try:
        with open(PAGES_REGISTRY, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as exc:
        current_app.logger.error(f"Не удалось прочитать реестр страниц: {exc}")
        return []


def _write_registry(data):
    try:
        PAGES_REGISTRY.parent.mkdir(parents=True, exist_ok=True)
        with open(PAGES_REGISTRY, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        current_app.logger.error(f"Не удалось записать реестр страниц: {exc}")


# Системные шаблоны — не показывать в списке «кастомных страниц»
_SYSTEM_PAGE_STEMS = frozenset({
    "index", "deals", "error-403", "error-404", "error-500",
    "auth-404", "auth-500", "auth-lock-screen", "auth-login", "auth-maintenance", "auth-recover-pw", "auth-register",
    "advanced-animation", "advanced-clipboard", "advanced-dragula", "advanced-files", "advanced-highlight",
    "advanced-rangeslider", "advanced-ratings", "advanced-ribbons", "advanced-sweetalerts", "advanced-toasts",
    "analytics-customers", "analytics-reports", "apps-calendar", "apps-chat", "apps-contact-list", "apps-invoice",
    "charts-apex", "charts-chartjs", "charts-justgage", "charts-toast-ui",
    "ecommerce-customer-details", "ecommerce-customers", "ecommerce-order-details", "ecommerce-orders",
    "ecommerce-products", "ecommerce-refunds", "email-templates-alert", "email-templates-basic", "email-templates-billing",
    "forms-advanced", "forms-editors", "forms-elements", "forms-img-crop", "forms-uploads", "forms-validation", "forms-wizard",
    "icons-fontawesome", "icons-icofont", "icons-iconoir", "icons-lineawesome",
    "maps-google", "maps-leaflet", "maps-vector",
    "pages-blogs", "pages-faq", "pages-gallery", "pages-notifications", "pages-pricing", "pages-profile",
    "pages-starter", "pages-timeline", "pages-treeview",
    "projects-board", "projects-create-project", "projects-files", "projects-overview", "projects-projects", "projects-teams",
    "sales-index", "tables-basic", "tables-datatable", "tables-editable",
    "ui-alerts", "ui-avatar", "ui-badges", "ui-buttons", "ui-cards", "ui-carousels", "ui-dropdowns", "ui-grids",
    "ui-images", "ui-list", "ui-modals", "ui-navbar", "ui-navs", "ui-offcanvas", "ui-paginations",
    "ui-popover-tooltips", "ui-progress", "ui-spinners", "ui-tabs-accordions", "ui-typography", "ui-videos",
})


def _list_pages_with_fallback():
    """Список страниц: реестр + недостающие по файлам (чтобы после обновления/потери JSON страницы не пропадали)."""
    registry = _read_registry()
    by_slug = {item.get("slug"): item for item in registry if item.get("slug")}
    if not PAGES_DIR.exists():
        return registry
    try:
        for p in PAGES_DIR.iterdir():
            if not p.is_file() or p.suffix != ".html" or p.name.startswith("_"):
                continue
            slug = p.stem
            if not slug or slug in _SYSTEM_PAGE_STEMS or not re.match(r"^[0-9A-Za-zА-Яа-я_-]+$", slug):
                continue
            if slug not in by_slug:
                by_slug[slug] = {"slug": slug, "title": slug}
        merged = list(by_slug.values())
        if len(merged) != len(registry):
            _write_registry(merged)
        return merged
    except Exception as exc:
        current_app.logger.warning(f"Скан папки страниц: {exc}")
    return registry


@blueprint.route('/')
def index():
    
    return render_template('pages/index.html', segment='index')


@blueprint.route('/deals')
def deals():
    """Route for deals page with data from API"""
    try:
        segment = get_segment(request)
        deals_data = []
        
        if requests is None:
            current_app.logger.error("Error: requests library is not installed. Please run: pip install requests")
        else:
            try:
                # Fetch data from API
                api_url = 'http://194.33.40.197:7070/api/data/deals'
                current_app.logger.info(f"Fetching deals data from {api_url}")
                response = requests.get(api_url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get('ok') and 'data' in data:
                        deals_data = data.get('data', [])
                        current_app.logger.info(f"Successfully loaded {len(deals_data)} deals")
                    else:
                        current_app.logger.warning(f"API returned ok=False or no data field")
                else:
                    current_app.logger.error(f"API returned status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                current_app.logger.error(f"Request error fetching deals data: {e}")
                deals_data = []
            except Exception as e:
                current_app.logger.error(f"Error fetching deals data: {e}")
                current_app.logger.error(traceback.format_exc())
                deals_data = []
        
        return render_template('pages/deals.html', segment=segment, deals=deals_data)
    except Exception as e:
        current_app.logger.error(f"Error in deals route: {e}")
        current_app.logger.error(traceback.format_exc())
        # Return error page instead of crashing
        segment = get_segment(request)
        return render_template('pages/deals.html', segment=segment, deals=[]), 500


@blueprint.route('/api/pages/create', methods=['POST'])
def create_page():
    """
    Create a simple blank page with a single "add component" dropdown.
    Accepts JSON { "name": "<page name>" }
    """
    data = request.get_json(silent=True) or {}
    raw_name = (data.get('name') or '').strip()
    if not raw_name:
        return jsonify({"ok": False, "error": "Название обязательно"}), 400

    # Allow letters/numbers/underscore/dash (including Cyrillic letters)
    slug = re.sub(r'[^0-9A-Za-zА-Яа-я_-]+', '-', raw_name).strip('-')
    if not slug:
        slug = 'page'

    filename = f"{slug}.html"
    pages_dir = PAGES_DIR
    pages_dir.mkdir(parents=True, exist_ok=True)
    target_path = (pages_dir / filename).resolve()

    # Protect against path traversal
    try:
        target_path.relative_to(pages_dir.resolve())
    except ValueError:
        return jsonify({"ok": False, "error": "Некорректное имя страницы"}), 400

    if target_path.exists():
        return jsonify({"ok": False, "error": "Страница уже существует", "slug": slug, "url": f"/{slug}"}), 409

    # Новая страница — сбрасываем конфиг таблицы для этого slug (чтобы не подтянулись старые данные)
    try:
        from apps.models import EntityTableConfig
        from apps import db
        EntityTableConfig.query.filter(
            db.func.lower(EntityTableConfig.page_slug) == slug.lower()
        ).delete(synchronize_session=False)
        db.session.commit()
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass

    page_title = raw_name
    # Шаблон с виджетом таблицы сущностей (кнопка «+», модалки, грид) или пустая страница
    widget_source = PAGES_DIR / "_entity_table_widget_source.html"
    if widget_source.exists():
        try:
            template_content = widget_source.read_text(encoding="utf-8")
        except Exception as exc:
            current_app.logger.warning(f"Не удалось прочитать шаблон виджета: {exc}")
            template_content = None
    else:
        template_content = None
    if not template_content:
        template_content = """{% extends 'layouts/vertical.html' %}

{% block title %}__PAGE_TITLE__{% endblock %}

{% block page_content %}
<div class="container-fluid">
    <div class="row">
        <div class="col-12">
        </div>
    </div>
</div>
{% endblock %}
"""
    template_content = template_content.replace("__PAGE_TITLE__", page_title).replace("__PAGE_SLUG__", slug)

    try:
        with open(target_path, 'w', encoding='utf-8') as fp:
            fp.write(template_content)
    except OSError as exc:
        current_app.logger.error(f"Ошибка записи файла {target_path}: {exc}")
        return jsonify({"ok": False, "error": "Не удалось сохранить файл"}), 500

    # обновляем реестр
    registry = _read_registry()
    registry = [item for item in registry if item.get("slug") != slug]
    registry.append({"slug": slug, "title": page_title})
    _write_registry(registry)

    current_app.logger.info(f"Создана страница {filename}")
    return jsonify({"ok": True, "slug": slug, "url": f"/{slug}"}), 201


@blueprint.route('/api/pages/list')
def list_pages():
    """Return list of generated pages for sidebar (реестр + страницы по файлам, чтобы не пропадали после обновления)."""
    pages = _list_pages_with_fallback()
    return jsonify({"ok": True, "pages": pages})


@blueprint.route('/api/pages/<slug>', methods=['DELETE'])
def delete_page(slug):
    """Delete generated page and remove from registry."""
    slug = (slug or '').strip()
    if not re.match(r'^[0-9A-Za-zА-Яа-я_-]+$', slug):
        return jsonify({"ok": False, "error": "Некорректный slug"}), 400
    pages_dir = PAGES_DIR
    target_path = (pages_dir / f"{slug}.html").resolve()
    try:
        target_path.relative_to(pages_dir.resolve())
    except ValueError:
        return jsonify({"ok": False, "error": "Некорректный путь"}), 400

    # Сначала удалить все связанные настройки (чтобы при создании страницы с тем же slug — чистый старт)
    try:
        from apps.models import EntityTableConfig
        from apps import db
        deleted = EntityTableConfig.query.filter(
            db.func.lower(EntityTableConfig.page_slug) == slug.lower()
        ).delete(synchronize_session=False)
        db.session.commit()
        if deleted:
            current_app.logger.info(f"Удалён конфиг таблицы сущностей для страницы {slug}")
    except Exception as exc:
        try:
            db.session.rollback()
        except Exception:
            pass
        current_app.logger.warning(f"EntityTableConfig при удалении страницы {slug}: {exc}")

    if target_path.exists():
        try:
            target_path.unlink()
        except OSError as exc:
            current_app.logger.error(f"Не удалось удалить файл {target_path}: {exc}")
            return jsonify({"ok": False, "error": "Не удалось удалить файл"}), 500

    registry = _read_registry()
    registry = [item for item in registry if item.get("slug") != slug]
    _write_registry(registry)
    return jsonify({"ok": True})


# -------------------------
# Entity Table (CRM) — прокси API и конфиг/шаблоны
# -------------------------

def _crm_base_url():
    return current_app.config.get('CRM_API_BASE_URL', 'http://194.33.40.197:7070').rstrip('/')


# Список воронок сделок (если бэкенд не отдаёт — используем статичный)
DEAL_CATEGORIES = [
    {"id": "2", "title": "Покупатели авто"},
    {"id": "12", "title": "Сервис"},
    {"id": "8", "title": "HR кандидаты"},
    {"id": "16", "title": "HR тек. сотрудники"},
    {"id": "20", "title": "Сделки - прокат"},
    {"id": "22", "title": "Chirie - solicitări prelucrate"},
    {"id": "0", "title": "После визита"},
    {"id": "25", "title": "Vânzări realizare"},
    {"id": "30", "title": "Vânzări auto"},
    {"id": "31", "title": "Atragere auto"},
]


@blueprint.route('/api/entity-table/deal-categories/', methods=['GET'])
def entity_table_deal_categories():
    """Список воронок сделок для выбора в модалке сущностей."""
    return jsonify({"ok": True, "categories": DEAL_CATEGORIES})


@blueprint.route('/api/entity-table/processes-deals/', methods=['GET'])
def entity_table_processes_deals():
    """Прокси: список сущностей для формы выбора (контакты, лиды, сделки, смарт-процессы)."""
    url = f"{_crm_base_url()}/api/processes-deals/"
    try:
        data = _proxy_get(url)
        return jsonify(data)
    except Exception as e:
        current_app.logger.error(f"Entity table processes-deals: {e}")
        return make_response(jsonify({"ok": False, "detail": str(e)}), 500)


@blueprint.route('/api/entity-table/entity-meta-fields/', methods=['GET'])
def entity_table_meta_fields():
    """Прокси: поля сущности для формы шестерёнки. Для сделок передаём category_id (воронку), чтобы API вернул поля и вложенности именно этой сделки."""
    entity_type = request.args.get('type', 'deal')
    entity_key = request.args.get('entity_key')
    category_id = request.args.get('category_id')
    params = {'type': entity_type}
    if entity_key:
        params['entity_key'] = entity_key
    if category_id is not None and category_id != '':
        params['category_id'] = category_id
    url = f"{_crm_base_url()}/api/entity-meta-fields/"
    try:
        data = _proxy_get(url, params=params)
        return jsonify(data)
    except Exception as e:
        current_app.logger.error(f"Entity table meta-fields: {e}")
        return make_response(jsonify({"ok": False, "detail": str(e)}), 500)


@blueprint.route('/api/entity-table/entity-meta-data/', methods=['GET'])
def entity_table_meta_data():
    """Прокси: данные сущности для грида (limit/offset). При ошибке CRM — пустой ответ 200."""
    entity_type = request.args.get('type', 'deal')
    entity_key = request.args.get('entity_key')
    limit = request.args.get('limit', '10000')
    offset = request.args.get('offset', '0')
    category_id = request.args.get('category_id')
    fields = request.args.get('fields')
    params = {'type': entity_type, 'limit': limit, 'offset': offset}
    if entity_key:
        params['entity_key'] = entity_key
    if category_id is not None and category_id != '':
        params['category_id'] = category_id
    if fields:
        params['fields'] = fields
    url = f"{_crm_base_url()}/api/entity-meta-data/"
    try:
        if requests is None:
            return jsonify({"ok": True, "data": [], "total": 0})
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            current_app.logger.warning(f"Entity table meta-data upstream: {resp.status_code} {resp.text[:200]}")
            return jsonify({"ok": True, "data": [], "total": 0})
        return jsonify(resp.json())
    except Exception as e:
        current_app.logger.error(f"Entity table meta-data: {e}")
        return jsonify({"ok": True, "data": [], "total": 0})


@blueprint.route('/api/entity-table/config', methods=['GET', 'POST'])
def entity_table_config():
    """GET: конфиг по page_slug. POST: сохранить конфиг. Сохранение в БД проекта (SQLite в apps/db.sqlite3)."""
    from apps.models import EntityTableConfig
    from apps import db

    try:
        db.create_all()
        _ensure_entity_table_config_columns()
    except Exception as e:
        current_app.logger.warning("entity_table_config create_all: %s", e)

    if request.method == 'GET':
        try:
            page_slug = (request.args.get('page_slug') or 'entity-table').strip()
            page_slug = unicodedata.normalize('NFC', page_slug)
            read_only_slugs = _read_only_pages_list()
            read_only = page_slug in read_only_slugs

            def _resp(ok=True, tables=None, table_title="", table_description="", entities=None, fields=None):
                out = {"ok": ok, "tables": tables or [], "table_title": table_title or "", "table_description": table_description or "", "entities": entities or [], "fields": fields or [], "read_only": read_only}
                return jsonify(out)

            rec = EntityTableConfig.query.filter(
                db.func.lower(EntityTableConfig.page_slug) == page_slug.lower()
            ).first()
            if not rec:
                return _resp()
            tables = rec.get_tables()
            if isinstance(tables, list) and len(tables) > 0:
                return jsonify({"ok": True, "tables": tables, "read_only": read_only})
            entities = rec.get_entities()
            fields = rec.get_fields()
            if not isinstance(entities, list):
                entities = []
            if not isinstance(fields, list):
                fields = []
            return _resp(
                table_title=rec.table_title or "",
                table_description=rec.table_description or "",
                entities=entities,
                fields=fields,
            )
        except Exception as e:
            current_app.logger.exception("Entity table config GET: %s", e)
            return jsonify({"ok": True, "tables": [], "table_title": "", "table_description": "", "entities": [], "fields": [], "read_only": False})

    data = request.get_json(silent=True) or {}
    page_slug = (data.get('page_slug') or 'entity-table').strip()
    tables = data.get('tables')
    if isinstance(tables, list):
        rec = EntityTableConfig.query.filter(
            db.func.lower(EntityTableConfig.page_slug) == page_slug.lower()
        ).first()
        if not rec:
            rec = EntityTableConfig(page_slug=page_slug)
            db.session.add(rec)
        rec.set_tables(tables)
    else:
        table_title = (data.get('table_title') or '').strip()
        table_description = (data.get('table_description') or '').strip()
        entities = data.get('entities', [])
        fields = data.get('fields', [])
        rec = EntityTableConfig.query.filter(
            db.func.lower(EntityTableConfig.page_slug) == page_slug.lower()
        ).first()
        if not rec:
            rec = EntityTableConfig(page_slug=page_slug)
            db.session.add(rec)
        rec.table_title = table_title
        rec.table_description = table_description
        rec.set_entities(entities)
        rec.set_fields(fields)
    try:
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Entity table config save: %s", e)
        return make_response(jsonify({"ok": False, "error": str(e)}), 500)


# --- Сохранить дэшборд: новая страница «только просмотр» + пункт в меню ---

@blueprint.route('/api/entity-table/save-dashboard', methods=['POST'])
def entity_table_save_dashboard():
    """
    Создать дэшборд: новая страница с копией конфига текущей, в режиме только просмотр.
    Body: { "name": "Имя", "source_page_slug": "Test" }.
    В меню появится пункт «Дэшборд Имя», страница без кнопок настройки.
    """
    from apps.models import EntityTableConfig
    from apps import db

    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    source_slug = (data.get('source_page_slug') or '').strip() or 'entity-table'
    tables_from_body = data.get('tables')  # опционально: текущие таблицы с фронта (приоритет над копией из source)
    if not name:
        return jsonify({"ok": False, "error": "Введите имя"}), 400

    slug = re.sub(r'[^0-9A-Za-zА-Яа-я_-]+', '-', name).strip('-')
    if not slug:
        slug = 'dashboard'
    slug = ('dash-' + slug).lower()
    slug = unicodedata.normalize('NFC', slug)
    filename = f"{slug}.html"
    target_path = (PAGES_DIR / filename).resolve()
    try:
        target_path.relative_to(PAGES_DIR.resolve())
    except ValueError:
        return jsonify({"ok": False, "error": "Некорректное имя"}), 400

    if target_path.exists():
        return jsonify({"ok": False, "error": "Дэшборд с таким именем уже существует", "slug": slug}), 409

    page_title = "Дэшборд " + name
    widget_source = PAGES_DIR / "_entity_table_widget_source.html"
    if not widget_source.exists():
        return jsonify({"ok": False, "error": "Шаблон виджета не найден"}), 500
    try:
        template_content = widget_source.read_text(encoding="utf-8")
    except Exception as e:
        current_app.logger.error("save-dashboard read widget: %s", e)
        return jsonify({"ok": False, "error": "Не удалось прочитать шаблон"}), 500

    template_content = template_content.replace("__PAGE_TITLE__", page_title)
    # В сгенерированном HTML файле дэшборда нужен буквальный PAGE_SLUG (файл не рендерится через Jinja)
    template_content = re.sub(
        r"var PAGE_SLUG = '[^']*';",
        "var PAGE_SLUG = " + json.dumps(slug) + ";",
        template_content,
        count=1,
    )
    if "__PAGE_SLUG__" in template_content:
        template_content = template_content.replace("__PAGE_SLUG__", slug)
    try:
        with open(target_path, 'w', encoding='utf-8') as fp:
            fp.write(template_content)
    except OSError as e:
        current_app.logger.error("save-dashboard write: %s", e)
        return jsonify({"ok": False, "error": "Не удалось создать страницу"}), 500

    try:
        db.create_all()
        _ensure_entity_table_config_columns()
        source_rec = EntityTableConfig.query.filter(
            db.func.lower(EntityTableConfig.page_slug) == source_slug.lower()
        ).first()
        new_rec = EntityTableConfig.query.filter(
            db.func.lower(EntityTableConfig.page_slug) == slug.lower()
        ).first()
        if not new_rec:
            new_rec = EntityTableConfig(page_slug=slug)
            db.session.add(new_rec)
        if isinstance(tables_from_body, list) and len(tables_from_body) > 0:
            new_rec.set_tables(tables_from_body)
            first_t = tables_from_body[0]
            new_rec.table_title = (first_t.get('table_title') or '').strip() or page_title
            new_rec.table_description = (first_t.get('table_description') or '').strip() or ""
        elif source_rec and (source_rec.get_tables() or source_rec.get_entities()):
            new_rec.table_title = source_rec.table_title or page_title
            new_rec.table_description = source_rec.table_description or ""
            new_rec.set_entities(source_rec.get_entities())
            new_rec.set_fields(source_rec.get_fields())
            new_rec.set_tables(source_rec.get_tables())
        else:
            new_rec.table_title = page_title
            new_rec.table_description = ""
            new_rec.set_entities([])
            new_rec.set_fields([])
            new_rec.set_tables([])
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("save-dashboard config copy: %s", e)
        try:
            target_path.unlink()
        except Exception:
            pass
        return jsonify({"ok": False, "error": str(e)}), 500

    _add_read_only_page(slug)

    registry = _read_registry()
    registry = [item for item in registry if item.get("slug") != slug]
    registry.append({"slug": slug, "title": page_title})
    _write_registry(registry)

    current_app.logger.info("Создан дэшборд: %s -> /%s", page_title, slug)
    return jsonify({"ok": True, "slug": slug, "url": f"/{slug}", "title": page_title}), 201


# --- Шаблоны таблиц: JSON-файл на сервере (общие для всех, без БД) ---

_TEMPLATES_FILE = Path(__file__).resolve().parent.parent / "data" / "entity_table_templates.json"


def _read_templates_file():
    """Прочитать список шаблонов из файла. При ошибке или отсутствии файла — пустой список."""
    if not _TEMPLATES_FILE.exists():
        return []
    try:
        with open(_TEMPLATES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception as e:
        current_app.logger.warning("read templates file: %s", e)
        return []


def _write_templates_file(templates_list):
    """Записать список шаблонов в файл."""
    _TEMPLATES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(_TEMPLATES_FILE, "w", encoding="utf-8") as f:
        json.dump(templates_list, f, ensure_ascii=False, indent=2)


@blueprint.route('/api/entity-table/templates', methods=['GET', 'POST'])
def entity_table_templates():
    """GET: список шаблонов из файла. POST: добавить шаблон в файл. Общие для всех пользователей."""
    if request.method == 'GET':
        templates = _read_templates_file()
        return jsonify({"ok": True, "templates": templates})

    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({"ok": False, "error": "Название обязательно"}), 400
    tables = data.get('tables') if isinstance(data.get('tables'), list) else []

    templates = _read_templates_file()
    new_id = max([t.get("id", 0) for t in templates], default=0) + 1
    templates.append({
        "id": new_id,
        "name": name,
        "entities": [],
        "fields": [],
        "tables": tables,
    })
    try:
        _write_templates_file(templates)
        return jsonify({"ok": True, "id": new_id}), 201
    except Exception as e:
        current_app.logger.exception("POST templates file: %s", e)
        return make_response(jsonify({"ok": False, "error": str(e)}), 500)


@blueprint.route('/api/entity-table/templates/<int:template_id>', methods=['DELETE'])
def entity_table_template_delete(template_id):
    """Удалить шаблон по id из файла."""
    templates = _read_templates_file()
    new_list = [t for t in templates if int(t.get("id", 0)) != int(template_id)]
    if len(new_list) == len(templates):
        return jsonify({"ok": False, "error": "Шаблон не найден"}), 404
    try:
        _write_templates_file(new_list)
        return jsonify({"ok": True})
    except Exception as e:
        current_app.logger.exception("DELETE template file: %s", e)
        return make_response(jsonify({"ok": False, "error": str(e)}), 500)


@blueprint.route('/<template>')
def route_template(template):

    try:

        if not template.endswith('.html'):
            template += '.html'

        # Detect the current page
        segment = get_segment(request)
        template_name = template if template.endswith('.html') else template + '.html'
        page_slug = template_name.replace('.html', '')
        is_dashboard_page = page_slug.lower().startswith('dash-')

        # Дашборды отдаём из исходного шаблона с контекстом (актуальная логика скрытия кнопок)
        if is_dashboard_page:
            registry = _read_registry()
            title = next((item.get("title") for item in registry if (item.get("slug") or "").lower() == page_slug.lower()), None)
            if not title:
                name = page_slug.replace("dash-", "").replace("-", " ").strip() or "Dashboard"
                title = "Дэшборд " + (name[:1].upper() + name[1:] if name else "Dashboard")
            return render_template(
                "pages/_entity_table_widget_source.html",
                segment=segment,
                is_dashboard_page=True,
                page_slug=page_slug,
                page_title=title,
            )

        # Serve the file (if exists) from app/templates/pages/FILE.html
        return render_template("pages/" + template_name, segment=segment, is_dashboard_page=is_dashboard_page)

    except TemplateNotFound:
        return render_template('pages/error-404.html'), 404

    except:
        return render_template('pages/error-500.html'), 500


# Helper - Extract current page name from request
def get_segment(request):

    try:

        segment = request.path.split('/')[-1]

        if segment == '':
            segment = 'index'

        return segment

    except:
        return None


# -------------------------
# Data Explorer Proxy APIs
# -------------------------

API_MAP = {
    "deal": {
        "fields": "http://194.33.40.197:7070/api/entity-fields/?type=deal",
        "data": "http://194.33.40.197:7070/api/entity-data/?type=deal",
    },
    "smart_process_1114": {
        "fields": "http://194.33.40.197:7070/api/entity-fields/?type=smart_process&entity_key=sp:1114",
        "data": "http://194.33.40.197:7070/api/entity-data/?type=smart_process&entity_key=sp:1114",
    },
    "contact": {
        "fields": "http://194.33.40.197:7070/api/entity-fields/?type=contact",
        "data": "http://194.33.40.197:7070/api/entity-data/?type=contact",
    },
    "lead": {
        "fields": "http://194.33.40.197:7070/api/entity-fields/?type=lead",
        "data": "http://194.33.40.197:7070/api/entity-data/?type=lead",
    },
}


def _proxy_get(url, params=None, timeout=10):
    """Small helper to proxy GET requests safely."""
    if requests is None:
        raise ImportError("requests is not installed")
    resp = requests.get(url, params=params or {}, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


@blueprint.route('/api/data-explorer/fields')
def data_explorer_fields():
    """Proxy fields to avoid CORS issues."""
    entity = request.args.get('entity', 'deal')
    entity_key = request.args.get('entity_key')
    # Dynamic smart process support
    if entity == "smart_process" and entity_key:
        target = f"http://194.33.40.197:7070/api/entity-fields/?type=smart_process&entity_key={entity_key}"
    else:
        target = API_MAP.get(entity, API_MAP["deal"])["fields"]
    try:
        data = _proxy_get(target)
        return jsonify(data)
    except Exception as e:
        current_app.logger.error(f"Error fetching fields for entity={entity}: {e}")
        return make_response(jsonify({"error": str(e)}), 500)


@blueprint.route('/api/data-explorer/data')
def data_explorer_data():
    """Proxy data to avoid CORS issues."""
    entity = request.args.get('entity', 'deal')
    entity_key = request.args.get('entity_key')
    category_id = request.args.get('category_id') or request.args.get('categoryId')
    limit = request.args.get('limit', '1000')
    offset = request.args.get('offset', '0')
    # Dynamic smart process support
    if entity == "smart_process" and entity_key:
        target = f"http://194.33.40.197:7070/api/entity-data/?type=smart_process&entity_key={entity_key}"
    elif entity == "deal" and category_id is not None:
        target = "http://194.33.40.197:7070/api/entity-data/?type=deal"
    else:
        target = API_MAP.get(entity, API_MAP["deal"])["data"]
    try:
        params = {"limit": limit, "offset": offset}
        if entity == "deal" and category_id is not None:
            params["category_id"] = category_id
            params["categoryId"] = category_id  # на случай другого имени параметра
        data = _proxy_get(target, params=params)
        return jsonify(data)
    except Exception as e:
        current_app.logger.error(f"Error fetching data for entity={entity}: {e}")
        return make_response(jsonify({"error": str(e)}), 500)


@blueprint.route('/api/data-explorer/processes')
def data_explorer_processes():
    """List all smart processes (for dropdown)."""
    url = "http://194.33.40.197:7070/api/processes-deals/"
    try:
        data = _proxy_get(url)
        return jsonify(data)
    except Exception as e:
        current_app.logger.error(f"Error fetching processes list: {e}")
        return make_response(jsonify({"error": str(e)}), 500)
