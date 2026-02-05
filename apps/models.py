# -*- coding: utf-8 -*-
"""Модели для таблицы сущностей CRM: конфигурация грида и шаблоны."""
from apps import db


class EntityTableConfig(db.Model):
    """Конфигурация грида таблицы сущностей (сущности + выбранные поля) по странице."""
    __tablename__ = 'entity_table_config'

    id = db.Column(db.Integer, primary_key=True)
    page_slug = db.Column(db.String(255), nullable=False, index=True, default='entity-table')
    table_title = db.Column(db.String(255), nullable=True, default='')
    table_description = db.Column(db.Text, nullable=True, default='')
    entities = db.Column(db.Text, nullable=False, default='[]')   # JSON: [{type, entity_key, category_id?}]
    fields = db.Column(db.Text, nullable=False, default='[]')     # JSON: [{entity_key or type, human_titles: []}]
    updated_at = db.Column(db.DateTime, server_default=db.func.now(), onupdate=db.func.now())

    def get_entities(self):
        import json
        try:
            return json.loads(self.entities) if self.entities else []
        except Exception:
            return []

    def set_entities(self, value):
        import json
        self.entities = json.dumps(value, ensure_ascii=False)

    def get_fields(self):
        import json
        try:
            return json.loads(self.fields) if self.fields else []
        except Exception:
            return []

    def set_fields(self, value):
        import json
        self.fields = json.dumps(value, ensure_ascii=False)


class EntityTableTemplate(db.Model):
    """Сохранённый шаблон: название + сущности + поля."""
    __tablename__ = 'entity_table_template'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    entities = db.Column(db.Text, nullable=False, default='[]')
    fields = db.Column(db.Text, nullable=False, default='[]')
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    def get_entities(self):
        import json
        try:
            return json.loads(self.entities) if self.entities else []
        except Exception:
            return []

    def set_entities(self, value):
        import json
        self.entities = json.dumps(value, ensure_ascii=False)

    def get_fields(self):
        import json
        try:
            return json.loads(self.fields) if self.fields else []
        except Exception:
            return []

    def set_fields(self, value):
        import json
        self.fields = json.dumps(value, ensure_ascii=False)
