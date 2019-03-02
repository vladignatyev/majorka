import re

from furl import furl
import requests

from types import *
from tsv import TabSeparated

class ConnectionError(Exception):
    pass


class DbError(Exception):
    def __init__(self, query_string, error_string):
        message = """

        Database responded with error when tried to execute SQL statement.

        SQL statement: {sql}
          DB response: {response}
        """.format(sql=query_string, response=error_string)
        super(DbError, self).__init__(message)


class SQLGenerator(object):
    def __init__(self, db_name):
        self._db_name = db_name

    def hello(self):
        return "SELECT 1;"

    def drop(self):
        return "DROP DATABASE {db};".format(db=self._db_name)

    def describe(self, table_name, from_db=None):
        return "DESCRIBE TABLE {db}.{table};".format(db=from_db or self._db_name, table=table_name)

    def describe_query(self, sql_query):
        if sql_query.endswith(';'):
            sql_query = sql_query[0:-1]
        return "DESCRIBE ({query});".format(query=sql_query)

    def create_database(self):
        return "CREATE DATABASE IF NOT EXISTS \"{db}\";".format(db=self._db_name)

    def create_table(self, table, date_column, index, columns, granularity=8192,
                     engine='MergeTree', if_not_exists=True):


        field_declaration = zip(ColumnsDef.column_names(columns), ColumnsDef.column_type_factories(columns))

        field_declaration_fmt = ",\n".join(
                map(lambda (name, type_factory): "            {name} {type}".format(name=name, type=type_factory.into_db_type()), field_declaration)
            )

        sql = """
        CREATE TABLE IF NOT EXISTS {db}.{table_name}
        (\n{field_declaration}
        ) ENGINE = MergeTree({date_column}, ({index}), {granularity})"""\
        .format(db=self._db_name,
                table_name=table,
                field_declaration=field_declaration_fmt,
                date_column=date_column,
                granularity=granularity,
                index=', '.join(index))
        return sql

    # todo: extract into importing domain
    def create_table_for_reporting_object(self, reporting_obj):
        default_fields_names = ('id', 'date_added')
        default_fields = [('id', Type.Idx()),
                          ('date_added', Type.Date())] #, "DEFAULT today()")]

        reporting_obj_columns = filter(lambda k: k[0] not in default_fields_names, reporting_obj.into_db_columns())
        field_declaration = default_fields + reporting_obj_columns

        return self.create_table(table=reporting_obj.TABLE_NAME,
                                     date_column='date_added',
                                     columns=field_declaration,
                                     index=reporting_obj.INDEX)


    def insert_values(self, table, values, columns):
        # check dimensions
        if len(values[0]) != len(columns):
            raise Exception("Dimensions of `values` and `columns` definition should match.")

        column_names = ColumnsDef.column_names(columns)
        column_factories = ColumnsDef.column_type_factories(columns)

        db_typed_values = [None] * len(values)
        for row, row_values in enumerate(values):
            db_typed_values[row] = map(lambda (f, v): f.into_db_value(py_value=v), zip(column_factories, row_values))

        tab_separated_data = TabSeparated(data=db_typed_values)

        column_names_fmt = ', '.join(column_names)

        sql = "INSERT INTO {db}.{table_name} ({column_names}) "\
              "FORMAT TabSeparated\n{tab_separated_data}".format(db=self._db_name,
                                                                 table_name=table,
                                                                 column_names=column_names_fmt,
                                                                 tab_separated_data=tab_separated_data.generate())

        return sql


class Database(object):
    def __init__(self, url, db, sqlgen=None, connection_timeout=2, data_read_timeout=2):
        self._url = furl(url)
        self._db = db
        self._db_is_created = None  # Unknown
        if sqlgen is None:
            sqlgen = SQLGenerator(db)
        self.sql = sqlgen
        self.connection_timeout, self.data_read_timeout = connection_timeout, data_read_timeout
        self.ping()

    @property
    def name(self):
        return self._db

    def ping(self):
        try:
            self.read(sql=self.sql.hello(), simple=True)
        except DbError:
            raise ConnectionError("Connection is not available.")

    def connected(self):
        self._create_database()
        return self

    def drop(self):
        self.write(self.sql.drop())
        self._db_is_created = None

    def describe(self, table, db=None):
        db = db or self._db
        result = self.read(sql=self.sql.describe(table, from_db=db),
                         columns=(('name', Type.String()), ('type', Type.String())))

        return map(lambda (o, i, l): (o['name'], factory_from_db_type(o['type'])), result)

    def describe_query(self, sql):
        result = self.read(sql=self.sql.describe_query(sql),
                            columns=(('name', Type.String()), ('type', Type.String())))

        columns = map(lambda (o, i, l): (o['name'], factory_from_db_type(o['type'])), result)
        return columns



    # todo: type checker that checks that response contains same set of columns as provided
    def read(self, sql, columns=(), simple=False):
        head_foot = self._divide(sql)

        if len(head_foot) == 2: # if SQL is multiline
            head, foot = head_foot
            response = requests.request("GET", self._query_url(head), data=foot, timeout=(self.connection_timeout, self.data_read_timeout))
        else:
            response = requests.request("GET", self._query_url(head_foot), timeout=(self.connection_timeout, self.data_read_timeout))

        if simple:
            return self._parsed_result_simple(sql, response)
        else:
            return self._parsed_result(sql, response, columns)

    def write(self, sql):
        head_foot = self._divide(sql)
        if len(head_foot) == 2: # if SQL is multiline
            head, foot = head_foot
            response = requests.request("POST", self._query_url(head), data=foot, timeout=(self.connection_timeout, self.data_read_timeout))
        else:
            response = requests.request("POST", self._query_url(head_foot), timeout=(self.connection_timeout, self.data_read_timeout))

        return self._parsed_result_simple(sql, response)
    #
    # def get_columns_for_table(self, table, db=None):
    #     db = db or self._db
    #     return map(lambda (o, i, l): (o['name'], o['type']), self.describe(table=table, db=db))
    #
    # def get_columns_for_query(self, sql):
    #     return map(lambda (o, i, l): (o['name'], o['type']), self.describe_query(sql=sql))

    def _divide(self, s):
        try:
            s.index('\r')
            divider = '\r\n'
        except ValueError:
            divider = '\n'
        return s.split(divider, 1)

    def _create_database(self):
        if self._db_is_created is None:
            self._db_is_created = self.write(sql=self.sql.create_database())

    def _parsed_result_simple(self, query, response):
        if response.status_code != 200:
            raise DbError(query, response.text)
        return True

    def _parsed_result(self, query, response, columns=()):
        if response.status_code != 200:
            raise DbError(query, response.text)

        response_strings = response.text.split('\n')
        rows = list(filter(bool, response_strings)) # clean out empty strings

        if not columns:
            return self._list_from_result(rows)

        return self._typed_dict_from_result(rows, columns_def=columns, query=query)

    def _list_from_result(self, row_strings):
        total = len(row_strings)
        for i, s in enumerate(row_strings):
            fields = s.split('\t')
            yield list(map(str, fields)), i, total

    def _typed_dict_from_result(self, row_strings, columns_def=None, query=None):
        total = len(row_strings)
        for i, s in enumerate(row_strings):
            db_values = s.split('\t')
            yield (ColumnsDef.parse_into_typed_dict(columns_def, *db_values)), i, total

    def _typed(self, type_factories, db_values):
        return map(lambda (t, v): t.from_db_value(v), zip(type_factories, db_values))

    def _dict_from_result(self, row_strings, field_names):
        total = len(row_strings)
        for i, s in enumerate(row_strings):
            fields = s.split('\t')
            yield dict(zip(field_names, fields)), i, total

    def _query_url(self, s):
        f = self._url.copy()
        f.args['query'] = s

        return f.url
