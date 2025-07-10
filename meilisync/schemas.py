import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel

from meilisync.enums import EventType
# from meilisync.graphql import fetch_document_with_user_graphql
# from meilisync.settings import Sync
from loguru import logger

CURRENT_SETTINGS_HACK: 'Settings' = None

class ProgressEvent(BaseModel):
    progress: dict | None = None


class Event(ProgressEvent):
    type: EventType
    table: str | None = None
    data: dict

    def mapping_data(self, fields_mapping: Optional[dict] = None):
        data = {}
        for k, v in self.data.items():
            if isinstance(v, datetime.datetime):
                v = int(v.timestamp())
            elif isinstance(v, datetime.date):
                v = str(v)
            elif isinstance(v, Decimal):
                v = float(v)
            if fields_mapping is not None and k in fields_mapping:
                real_k = fields_mapping[k] or k
                data[real_k] = v
            elif fields_mapping is None:
                data[k] = v
        return data or self.data

    def transform(self, sync: 'Sync'):
        global CURRENT_SETTINGS_HACK
        pk = self.data[sync.pk]
        # result = fetch_document_with_user_graphql(sync.graphql_query, pk, )

        # TODO: how to handle connection better?
        if not CURRENT_SETTINGS_HACK:
            from meilisync.settings import Settings
            from meilisync.yaml_parser import parse_yaml
            CURRENT_SETTINGS_HACK = Settings.model_validate(parse_yaml('config.yml'))

        db_connection_args = CURRENT_SETTINGS_HACK.source.model_dump(exclude={'type', 'database'})

        conn_dict = psycopg2.connect(**db_connection_args, cursor_factory=psycopg2.extras.RealDictCursor)
        with conn_dict.cursor() as cur:
            cur.execute(sync.on_sync_row_query, (pk,))
            result = cur.fetchall()
        result = dict(result[0])
        result = json.loads(json.dumps(result, default=cmonnnnn))

        # logger.debug(f'transformed event to {result}')
        return result


def cmonnnnn(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    return str(obj)

import json
import psycopg2
import psycopg2.extras
