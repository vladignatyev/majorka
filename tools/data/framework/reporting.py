import re

from furl import furl
import requests

from types import ModelTypes, factory_from_db_type, factory_into_db_type
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
        return "DROP DATABASE %s;" % self._db_name

    def describe(self, table_name, from_db=None):
        return "DESCRIBE TABLE %s.%s;" % (from_db or self._db_name, table_name)

    def describe_query(self, sql_query):
        if sql_query.endswith(';'):
            sql_query = sql_query[0:-1]
        return "DESCRIBE (%s);" % sql_query

    def create_database(self):
        return "CREATE DATABASE IF NOT EXISTS \"%s\";" % self._db_name

    def create_table(self, table, date_column, index, columns, granularity=8192,
                     engine='MergeTree', if_not_exists=True):

        field_declaration = columns
        field_declaration_fmt = ",\n".join(map(lambda t: "            " + " ".join(t), field_declaration))

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
        default_fields = [('id', ModelTypes.IDX),
                          ('date_added', ModelTypes.DATE, "DEFAULT today()")]
        default_fields_names = ['id', 'date_added']
        reporting_obj_columns = filter(lambda k: k[0] not in default_fields_names, reporting_obj.into_db_columns())
        field_declaration = default_fields + reporting_obj_columns

        field_declaration = map(lambda decl: decl if decl[1] != 'Decimal' else (decl[0], 'Decimal64(5)'), field_declaration)

        return self.create_table(table=reporting_obj.TABLE_NAME,
                                     date_column='date_added',
                                     columns=field_declaration,
                                     index=reporting_obj.INDEX)


    def insert_values(self, table, values, columns):
        column_names = map(lambda k: k[0], columns)

        # check dimensions
        if len(values[0]) != len(columns):
            raise Exception("Dimensions of `values` and `columns` definition should match.")

        if type(columns[0]) is tuple:
            unzipped = zip(*columns)
            column_names = unzipped[0]
            column_types = unzipped[1]
            column_factories = map(lambda type_name: factory_into_db_type(type_name), column_types)

            db_typed_values = [None] * len(values)
            for row, row_values in enumerate(values):
                db_typed_values[row] = map(lambda (f, v): f(v), zip(column_factories, row_values))
        else:
            column_names = columns
            db_typed_values = values  # we expect that values are already db typed


        tab_separated_data = TabSeparated(data=db_typed_values)

        column_names_fmt = ', '.join( map(lambda n: str(n), column_names))

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
        return self.read(sql=self.sql.describe(table, from_db=db),
                         columns=(('name', ModelTypes.STRING), ('type', ModelTypes.STRING)))

    def describe_query(self, sql):
        return self.read(sql=self.sql.describe_query(sql),
                         columns=(('name', ModelTypes.STRING), ('type', ModelTypes.STRING)))

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

    def get_columns_for_table(self, table, db=None):
        db = db or self._db
        return map(lambda (o, i, l): (o['name'], o['type']), self.describe(table=table, db=db))

    def get_columns_for_query(self, sql):
        return map(lambda (o, i, l): (o['name'], o['type']), self.describe_query(sql=sql))

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

        try:
            # if columns is ((name1, type1), (name2, type2)...) tuple
            field_names, field_types = zip(*columns)

            if type(field_types[0]) is str:
                type_factories = map(lambda db_type: factory_from_db_type(db_type), field_types)
            else:
                type_factories = field_types
            return self._typed_dict_from_result(rows, field_names, type_factories, columns_def=columns, query=query)
        except:
            # if columns is (name1, name2...) tuple
            field_names = columns
            return self._dict_from_result(rows, field_names)

    def _list_from_result(self, row_strings):
        total = len(row_strings)
        for i, s in enumerate(row_strings):
            fields = s.split('\t')
            yield list(map(str, fields)), i, total

    def _typed_dict_from_result(self, row_strings, field_names, type_factories, columns_def=None, query=None):
        total = len(row_strings)
        for i, s in enumerate(row_strings):
            fields = s.split('\t')
            yield dict(zip(field_names, self._typed(type_factories, fields))), i, total

    def _typed(self, type_factories, vals):
        return map(lambda (t, v): t(v), zip(type_factories, vals))

    def _dict_from_result(self, row_strings, field_names):
        total = len(row_strings)
        for i, s in enumerate(row_strings):
            fields = s.split('\t')
            yield dict(zip(field_names, fields)), i, total

    def _query_url(self, s):
        f = self._url.copy()
        f.args['query'] = s

        return f.url
