# Деплой: дашборды (только просмотр + секция в меню)

## Файлы для замены на сервере

Замените эти файлы в дереве приложения (пути относительно корня проекта Dastone):

| Файл | Назначение |
|------|------------|
| `apps/templates/layouts/vertical.html` | Класс на body, CSS и скрипт скрытия тулбара; класс `dashboard-toolbar-hidden` против Bootstrap `.d-flex` |
| `apps/templates/pages/_entity_table_widget_source.html` | Виджет таблиц: режим только просмотр по slug `dash-*`, подстановка `page_slug`/`page_title`, скрытие тулбара с `!important` и классом |
| `apps/pages/routes.py` | Рендер дашбордов из исходного шаблона (по slug вида `dash-*`), передача `is_dashboard_page`, `page_slug`, `page_title` из реестра |
| `apps/templates/partials/startbar.html` | Секция **Published Dashboards** ниже Custom Pages: список опубликованных дэшбордов (slug `dash-*`), иконка и удаление |

## Что не трогать

- **Реестр и данные:** `apps/data/pages_registry.json`, `apps/data/read_only_pages.json` — создаются/обновляются приложением при сохранении дашбордов, вручную не подменять.
- **Имена дашбордов** (например `dash-oleg`) нигде не захардкожены: slug и заголовок берутся из реестра или из URL.

## После замены

1. Перезапустить приложение (Gunicorn/uWSGI или `run.py`).
2. Открыть любую страницу дашборда по ссылке из меню (URL вида `/dash-<имя>`).
3. Убедиться, что кнопки «Добавить таблицу» и «Сохранить Дэшборд» скрыты (режим только просмотр).
