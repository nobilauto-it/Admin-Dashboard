# PROJECT_CONTEXT

## 1) Архитектура
- Тип: монолитное Flask-приложение с серверным рендерингом (Jinja2) + API-эндпоинты в том же приложении.
- Backend-ядро:
  - `run.py` — bootstrap приложения.
  - `apps/__init__.py` — `create_app`, регистрация blueprints, инициализация SQLAlchemy, автосоздание таблиц.
  - `apps/pages/routes.py` — основной web/API слой (почти вся прикладная логика).
  - `apps/models.py` — модели БД для конфигов таблиц/шаблонов.
  - `apps/config.py` — конфигурация окружения, БД, `CRM_API_BASE_URL`.
- Frontend:
  - Шаблоны: `apps/templates/...`
  - Статика: `apps/static/...`
  - Сборка ассетов: `gulpfile.js`, `package.json` (SCSS -> CSS, vendor libs).
- Хранение состояния:
  - SQLAlchemy + SQLite по умолчанию (`apps/db.sqlite3`) или внешняя SQL БД через env.
  - Часть настроек хранится в JSON-файлах в `apps/data` (реестр страниц, read-only страницы, режимы компонентов, шаблоны таблиц).

## 2) Точки входа
- Приложение:
  - `run.py` — точка старта (`app = create_app(...)`, `Migrate`, `Minify`, `app.run()`).
- Web/UI:
  - `GET /` -> `index()` (`apps/pages/routes.py`)
  - `GET /deals` -> `deals()`
  - `GET /<template>` -> `route_template(template)` (динамический рендер страниц, включая сгенерированные)
- API (основные):
  - Аутентификация-прокси: `POST /api/login`
  - Управление страницами: `POST /api/pages/create`, `GET /api/pages/list`, `DELETE /api/pages/<slug>`
  - Entity table:  
    `GET /api/entity-table/deal-categories/`  
    `GET /api/entity-table/processes-deals/`  
    `GET /api/entity-table/entity-meta-fields/`  
    `GET /api/entity-table/entity-meta-data/`  
    `GET|POST /api/entity-table/config`  
    `GET|POST /api/entity-table/component-modes`  
    `POST /api/entity-table/save-dashboard`  
    `GET|POST /api/entity-table/templates`  
    `DELETE /api/entity-table/templates/<int:template_id>`
  - Data explorer proxy:  
    `GET /api/data-explorer/fields`  
    `GET /api/data-explorer/data`  
    `GET /api/data-explorer/processes`

## 3) Где бизнес-логика
- Основной слой бизнес-логики находится в `apps/pages/routes.py`:
  - Правила генерации/удаления страниц и ведения реестра (`.generated_pages.json`).
  - Логика дашбордов (`save-dashboard`): создание page-файла, копирование конфигов таблиц, перевод в read-only.
  - Логика режимов компонентов (`edit/read_only/hide`) и валидация/нормализация slug.
  - Прокси-логика к CRM backend с обработкой ошибок и фолбэками.
  - Базовая миграционная логика для `entity_table_config` (`_ensure_entity_table_config_columns`).
- Модели в `apps/models.py` содержат доменную структуру (конфиг таблиц/шаблоны) и JSON-сериализацию полей.

## 4) Где интеграции
- Внешний CRM/API backend:
  - Базовый URL: `http://194.33.40.197:7070` (см. `apps/config.py`, `apps/pages/routes.py`).
  - Используется через `requests` в proxy-эндпоинтах (`/api/login`, `/api/entity-table/*`, `/api/data-explorer/*`, `/deals`).
- Инфраструктурные интеграции:
  - Flask-Migrate (`run.py`) для миграций SQLAlchemy.
  - Flask-Minify (`run.py`) для HTML minify вне debug.
  - Gunicorn-конфиг: `gunicorn-cfg.py`.

## 5) Где БД
- ORM/инициализация:
  - `apps/__init__.py`: `db = SQLAlchemy()`, `db.init_app(app)`, `db.create_all()`.
- Модели:
  - `apps/models.py`
    - `EntityTableConfig` -> таблица `entity_table_config`
    - `EntityTableTemplate` -> таблица `entity_table_template`
- Подключение:
  - `apps/config.py`:
    - По умолчанию SQLite: `sqlite:///apps/db.sqlite3`
    - При наличии env (`DB_ENGINE`, `DB_USERNAME`, `DB_PASS`, `DB_HOST`, `DB_PORT`, `DB_NAME`) — внешняя SQL БД.
- Дополнительное файловое хранилище (не SQL):
  - `apps/data/read_only_pages.json`
  - `apps/data/component_modes.json`
  - `apps/templates/pages/.generated_pages.json`
  - `apps/data/entity_table_templates.json`

## 6) Краткая карта слоев
- Presentation: `apps/templates`, `apps/static`, маршруты рендера в `apps/pages/routes.py`.
- Application/API: `apps/pages/routes.py`.
- Domain/Data model: `apps/models.py`.
- Infrastructure/config: `run.py`, `apps/config.py`, `apps/__init__.py`, `gunicorn-cfg.py`, `gulpfile.js`.

## Замечания
- Сейчас проект архитектурно концентрирует routing + бизнес-логику + интеграции в одном файле `apps/pages/routes.py` (высокая связанность).
- Есть смешение SQL-хранилища и JSON-файлов для разных типов конфигурации.
