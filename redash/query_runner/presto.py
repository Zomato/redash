from collections import defaultdict
from redash.query_runner import *
from redash.utils import json_dumps, json_loads

import logging
logger = logging.getLogger(__name__)


try:
    from pyhive import presto
    from pyhive.exc import DatabaseError
    enabled = True

except ImportError:
    enabled = False

PRESTO_TYPES_MAPPING = {
    "integer": TYPE_INTEGER,
    "tinyint": TYPE_INTEGER,
    "smallint": TYPE_INTEGER,
    "long": TYPE_INTEGER,
    "bigint": TYPE_INTEGER,
    "float": TYPE_FLOAT,
    "double": TYPE_FLOAT,
    "boolean": TYPE_BOOLEAN,
    "string": TYPE_STRING,
    "varchar": TYPE_STRING,
    "date": TYPE_DATE,
}


class Presto(BaseQueryRunner):
    noop_query = 'SHOW TABLES'

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'host': {
                    'type': 'string'
                },
                'protocol': {
                    'type': 'string',
                    'default': 'http'
                },
                'port': {
                    'type': 'number'
                },
                'schema': {
                    'type': 'string'
                },
                'catalog': {
                    'type': 'string'
                },
                'username': {
                    'type': 'string'
                },
                'password': {
                    'type': 'string'
                },
                'source': {
                    'type': 'string',
                    'title': 'Source to be passed to presto',
                    'default': 'pyhive'
                },
                'blacklisted_table_schemas': {
                    'type': 'string',
                    'title': 'Comma seperated list of schemas to be discarded while querying information_schema for table metadata'
                },
                'user_impersonation': {
                    'type': 'boolean',
                    'title': 'Allows passing logged-in users email address as username to presto, Instead of the default username being sent',
                    'default': False
                }
            },
            'order': ['host', 'protocol', 'port', 'username', 'password', 'schema', 'catalog', 'source', 'blacklisted_table_schemas', 'user_impersonation'],
            'required': ['host']
        }

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def type(cls):
        return "presto"

    def get_schema(self, get_stats=False):
        schema = {}

        default_table_schemas = ['pg_catalog', 'information_schema']
        blacklisted_table_schemas = self.configuration.get('blacklisted_table_schemas', '')
        final_table_schemas = default_table_schemas + map(str.strip, blacklisted_table_schemas.split(','))
        table_schemas = ', '.join("'{0}'".format(w) for w in final_table_schemas)

        query = """
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ({0})
        """.format(table_schemas)

        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        results = json_loads(results)

        for row in results['rows']:
            table_name = '{}.{}'.format(row['table_schema'], row['table_name'])

            if table_name not in schema:
                schema[table_name] = {'name': table_name, 'columns': []}

            schema[table_name]['columns'].append(row['column_name'])

        return schema.values()

    def run_query(self, query, user):
        should_impersonate_user = self.configuration.get('user_impersonation', False)
        if not should_impersonate_user or user is None:
            username = self.configuration.get('username', 'redash')
        else:
            username = user.email

        connection = presto.connect(
            host=self.configuration.get('host', ''),
            port=self.configuration.get('port', 8080),
            protocol=self.configuration.get('protocol', 'http'),
            username=username,
            password=(self.configuration.get('password') or None),
            catalog=self.configuration.get('catalog', 'hive'),
            schema=self.configuration.get('schema', 'default'),
            source=self.configuration.get('source', 'pyhive')
        )

        cursor = connection.cursor()

        try:
            cursor.execute(query)
            column_tuples = [(i[0], PRESTO_TYPES_MAPPING.get(i[1], None))
                             for i in cursor.description]
            columns = self.fetch_columns(column_tuples)
            rows = [dict(zip(([c['name'] for c in columns]), r))
                    for i, r in enumerate(cursor.fetchall())]
            data = {'columns': columns, 'rows': rows}
            json_data = json_dumps(data)
            error = None
        except DatabaseError as db:
            json_data = None
            default_message = 'Unspecified DatabaseError: {0}'.format(
                db.message)
            if isinstance(db.message, dict):
                message = db.message.get(
                    'failureInfo', {'message', None}).get('message')
            else:
                message = None
            error = default_message if message is None else message
        except (KeyboardInterrupt, InterruptException) as e:
            cursor.cancel()
            error = "Query cancelled by user."
            json_data = None
        except Exception as ex:
            json_data = None
            error = ex.message
            if not isinstance(error, basestring):
                error = unicode(error)

        return json_data, error


register(Presto)
