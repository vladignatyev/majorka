import re

from furl import furl
import requests

from types import ModelTypes, factory_from_db_type


class ConnectionError(Exception):
    pass


class DbError(Exception):
    def __init__(self, query_string, error_string):
        message = """

        Database responded with error when tried to execute SQL statement.
        SQL statement: %s
          DB response: %s
        """ % (query_string, error_string)
        super(DbError, self).__init__(message)


class SQLGenerator(object):
    def __init__(self, db_name):
        self._db_name = db_name

    def sql_hello(self):
        return "SELECT 1;"

    def sql_drop(self):
        return "DROP DATABASE %s;" % self._db_name

    def sql_describe(self, table_name, from_db=None):
        return "DESCRIBE TABLE %s.%s;" % (from_db or self._db_name, table_name)

    def sql_create_table_for_reporting_object(self, reporting_obj):
        default_fields = [('id', ModelTypes.IDX),
                          ('date_added', ModelTypes.DATE, "DEFAULT today")]
        default_fields_names = ['id', 'date_added']
        reporting_obj_columns = filter(lambda k: k[0] not in default_fields_names, reporting_obj.into_db_columns())
        field_declaration = default_fields + reporting_obj_columns
        field_declaration_fmt = ",\n".join(map(lambda t: "            " + " ".join(t), field_declaration))
        sql = """
        CREATE TABLE IF NOT EXISTS {db}.{table_name}
        (\n{field_declaration}
        ) ENGINE = MergeTree({index})""".format(db=self._db_name,
                   table_name=reporting_obj.TABLE_NAME,
                   field_declaration=field_declaration_fmt,
                   index="\'%s\'" % '\',\''.join(reporting_obj.INDEX))

        return sql

    def sql_create_database(self):
        return "CREATE DATABASE IF NOT EXISTS \"%s\";" % self._db_name


class Database(SQLGenerator):
    def __init__(self, url, db):
        super(Database, self).__init__(db_name=db)
        self._url = furl(url)
        self._db = db
        self._db_is_created = None  # Unknown
        self.ping()

    def ping(self):
        try:
            self.read(sql=self.sql_hello(), simple=True)
        except DbError:
            raise ConnectionError("Connection is not available.")

    def connected(self):
        self._create_database()
        return self

    def drop(self):
        self.write(self.sql_drop())

    def describe(self, table, db=None):
        db = db or self._db
        return self.read(sql=self.sql_describe(table, from_db=db),
                         columns=(('name', ModelTypes.STRING), ('type', ModelTypes.STRING)))

    def read(self, sql, columns=(), simple=False):
        head_foot = self._divide(sql)

        if len(head_foot) == 2: # if SQL is multiline
            head, foot = head_foot
            response = requests.request("GET", self._query_url(head), data=foot)
        else:
            response = requests.request("GET", self._query_url(head_foot))

        if simple:
            return self._parsed_result_simple(sql, response)
        else:
            return self._parsed_result(sql, response, columns)

    def write(self, sql):
        head_foot = self._divide(sql)
        if len(head_foot) == 2: # if SQL is multiline
            head, foot = head_foot
            response = requests.request("POST", self._query_url(head), data=foot)
        else:
            response = requests.request("POST", self._query_url(head_foot))

        return self._parsed_result_simple(sql, response)



    def get_columns_for_table(self, table, db=None):
        db = db or self._db
        return map(lambda (o, i, l): (o['name'], o['type']), self.describe(table=table, db=db))

    def _divide(self, s):
        try:
            s.index('\r')
            divider = '\r\n'
        except ValueError:
            divider = '\n'
        return s.split(divider, 1)

    def _create_database(self):
        if self._db_is_created is None:
            self._db_is_created = self.write(sql=self.sql_create_database())

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
            return self._typed_dict_from_result(rows, field_names, type_factories)
        except:
            # if columns is (name1, name2...) tuple
            field_names = columns
            return self._dict_from_result(rows, field_names)

    def _list_from_result(self, row_strings):
        total = len(row_strings)
        for i, s in enumerate(row_strings):
            fields = s.split('\t')
            yield list(map(str, fields)), i, total

    def _typed_dict_from_result(self, row_strings, field_names, type_factories):
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
