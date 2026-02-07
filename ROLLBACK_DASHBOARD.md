# Откат функционала «Сохранить Дэшборд»

Если нужно удалить всё, что было добавлено для дэшбордов:

## 1. Удалить добавленный код вручную

### apps/pages/routes.py
- Удалить константы и функции: `READ_ONLY_PAGES_FILE`, `_read_only_pages_list()`, `_add_read_only_page()` (в начале файла после PAGES_REGISTRY).
- В `entity_table_config()` GET: убрать использование `_read_only_pages_list()` и `read_only` из ответа (вернуть старые ответы без поля `read_only` и без функции `_resp`).
- Удалить весь блок от `# --- Сохранить дэшборд` до конца функции `entity_table_save_dashboard()` (включая маршрут POST `/api/entity-table/save-dashboard`).

### apps/templates/pages/_entity_table_widget_source.html
- Убрать кнопку «Сохранить Дэшборд» из блока `#entityTableAdminToolbar` (оставить только «Добавить таблицу»).
- Удалить модальное окно `#modalSaveDashboard` (весь блок `<div class="modal fade" id="modalSaveDashboard" ...>`).
- В скрипте удалить: `saveDashboard` из API, переменную `isReadOnly`, функцию `applyReadOnlyMode()`, вызовы `isReadOnly = ...` и `applyReadOnlyMode()` в loadConfig и tryApplyStored, весь IIFE `setupSaveDashboard()`.

### apps/templates/partials/startbar.html
- Удалить строку: `window.addEventListener('pages-updated', () => loadPages());`

## 2. Удалить созданные файлы (по желанию)
- `apps/data/read_only_pages.json` (если создавался).
- Созданные дэшборд-страницы в `apps/templates/pages/` (файлы вида `dash-*.html`).

## 3. Очистить реестр страниц
В `apps/templates/pages/.generated_pages.json` удалить записи с `"slug": "dash-..."` для созданных дэшбордов.
