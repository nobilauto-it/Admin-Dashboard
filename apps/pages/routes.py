import json
import re
import traceback
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

    page_title = raw_name
    # Пустая страница — чистый лист, содержимое добавите позже
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
    template_content = template_content.replace("__PAGE_TITLE__", page_title)

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
    """Return list of generated pages for sidebar."""
    registry = _read_registry()
    return jsonify({"ok": True, "pages": registry})


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


@blueprint.route('/<template>')
def route_template(template):

    try:

        if not template.endswith('.html'):
            template += '.html'

        # Detect the current page
        segment = get_segment(request)

        # Serve the file (if exists) from app/templates/pages/FILE.html
        return render_template("pages/" + template, segment=segment)

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
