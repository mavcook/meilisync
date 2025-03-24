from typing import List

from meilisearch_python_sdk.models.settings import MeilisearchSettings
from pydantic import BaseModel, Extra
from pydantic_settings import BaseSettings

from meilisync.enums import ProgressType, SourceType
from meilisync.plugin import load_plugin


class Source(BaseModel):
    type: SourceType
    database: str

    class Config:
        extra = Extra.allow


class MeiliSearch(BaseModel):
    api_url: str
    api_key: str | None = None
    insert_size: int | None = None
    insert_interval: int | None = None


class BasePlugin(BaseModel):
    plugins: List[str] = []

    def plugins_cls(self):
        plugins = []
        for plugin in self.plugins or []:
            p = load_plugin(plugin)
            if p.is_global:
                plugins.append(p())
            else:
                plugins.append(p)
        return plugins


class Sync(BasePlugin):
    table: str
    pk: str = "id"
    full: bool = False
    index: str | None = None
    index_settings: MeilisearchSettings | None = None

    @property
    def index_name(self):
        return self.index or self.table
    
    @property
    def fields(self):
        # TODO: revisit and improve
        if not self.index_settings:
            # no index settings, sync all fields: wildcard settings
            return None
        
        configured_fields = [
            self.index_settings.searchable_attributes,
            self.index_settings.filterable_attributes,
            self.index_settings.sortable_attributes,
            self.index_settings.displayed_attributes,
        ]
        attrs = set()
        for _fields in configured_fields:
            if _fields:
                attrs = attrs.union(_fields)
        if len(attrs) == 0:
            return None

        # add primary key in case it isn't included in searchable
        attrs.add(self.pk) # or self.index_settings.distinct_attribute

        meili_for_db_field = {}
        for db_col_name in attrs:
            if db_col_name == '*':
                continue
            meili_field_name = db_col_name
            if '.' in db_col_name:
                db_col_name = db_col_name.split('.', 1)[0]
            meili_for_db_field[db_col_name] = meili_field_name
        return meili_for_db_field


    def __hash__(self):
        return hash(self.table)


class Progress(BaseModel):
    type: ProgressType

    class Config:
        extra = Extra.allow


class Sentry(BaseModel):
    dsn: str
    environment: str = "production"


class Settings(BaseSettings, BasePlugin):
    progress: Progress
    debug: bool = False
    source: Source
    meilisearch: MeiliSearch
    sync: List[Sync]
    should_sync_existing_indices: bool = False
    sentry: Sentry | None = None

    @property
    def tables(self):
        return [sync.table for sync in self.sync]

    def get_sync(self, table: str):
        for sync in self.sync:
            if sync.table == table:
                return sync
