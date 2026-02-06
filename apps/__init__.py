import os

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from importlib import import_module


db = SQLAlchemy()

def register_extensions(app):
    db.init_app(app)

apps = ('pages',)

def register_blueprints(app):
    for module_name in apps:
        module = import_module('apps.{}.routes'.format(module_name))
        app.register_blueprint(module.blueprint)


def configure_database(app):
    """Создаём таблицы при старте приложения (entity_table_config, entity_table_template в SQLite проекта)."""
    with app.app_context():
        try:
            db.create_all()
        except Exception as e:
            print('> Error: DBMS Exception: ' + str(e))
            if 'SQLALCHEMY_DATABASE_URI' not in app.config or not app.config.get('SQLALCHEMY_DATABASE_URI', '').startswith('sqlite'):
                basedir = os.path.abspath(os.path.dirname(__file__))
                app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(basedir, 'db.sqlite3')
                print('> Fallback to SQLite ')
                db.create_all()

    @app.before_request
    def ensure_tables():
        try:
            db.create_all()
        except Exception:
            pass

    @app.teardown_request
    def shutdown_session(exception=None):
        db.session.remove()


def create_app(config):
    app = Flask(__name__)
    app.config.from_object(config)
    register_extensions(app)
    import apps.models  # noqa: F401 — регистрируем модели для db.create_all()
    register_blueprints(app)
    configure_database(app)
    return app
