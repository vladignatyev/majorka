import re

from furl import furl
import requests


class ConnectionError(Exception):
    pass


class DbError(Exception):
    def __init__(self, query_string, error_string):
        message = """
        Database responded with error when tried to execute SQL.
        SQL statement: %s
          DB response: %s
        """ % (query_string, error_string)
        super(DbError, self).__init__(message)


class Database(object):
    def __init__(self, url, db):
        self._url = furl(url)
        self._db = db
        self._db_is_created = None  # Unknown
        self.ping()

    def connected(self):
        self._create_database()
        return self

    def read(self, sql, columns=(), simple=False):
        head_foot = self._divide(sql)
        if len(head_foot) == 2: # if SQL is multiline
            head, foot = head_foot
            response = requests.request("GET", self._query_url(head), data=foot)
        else:
            response = requests.request("GET", self._query_url(head_foot))
        return self._parsed_result(sql, response, columns, simple)

    def write(self, sql):
        response = requests.request("POST", self._query_url(sql))
        return self._parsed_result(sql, response, simple=True)

    def importdata(self):
        pass

    def ping(self):
        try:
            self.read(sql="SELECT 1;", simple=True)
        except DbError:
            raise ConnectionError("Connection is not available.")

    def _divide(self, s):
        try:
            s.index('\r')
            divider = '\r\n'
        except ValueError:
            divider = '\n'
        return s.split(divider, 1)

    def _create_database(self):
        if self._db_is_created is None:
            self._db_is_created = self.write(sql="CREATE DATABASE IF NOT EXISTS \"%s\";" % self._db)

    def _parsed_result(self, query, response, columns=(), simple=False):
        if response.status_code != 200:
            raise DbError(query, response.text)

        if simple:
            return  # if no exception occured, just return empty iterator

        response_strings = response.text.split('\n')
        rows = list(filter(bool, response_strings)) # clean out empty strings

        if not columns:
            return self._list_from_result(rows)

        try:
            # if columns is ((name1, type1), (name2, type2)...) tuple
            field_names, field_types = zip(*columns)
            return self._typed_dict_from_result(rows, field_names, field_types)
        except:
            # if columns is (name1, name2...) tuple
            field_names = columns
            return self._dict_from_result(rows, field_names)

    def _list_from_result(self, row_strings):
        for i, s in enumerate(row_strings):
            fields = s.split('\t')
            yield list(map(_response_field_parser, fields)), i, total

    def _typed_dict_from_result(self, row_strings, field_names, field_types):
        total = len(row_strings)
        for i, s in enumerate(row_strings):
            fields = s.split('\t')
            yield dict(zip(field_names, self._typed(field_types, fields))), i, total

    def _typed(self, types, vals):
        return map(lambda (t, v): t(v), zip(types, vals))

    def _dict_from_result(self, row_strings, field_names):
        total = len(row_strings)
        for i, s in enumerate(row_strings):
            fields = s.split('\t')
            yield dict(zip(field_names, fields)), i, total

    def _query_url(self, s):
        f = self._url.copy()
        f.args['query'] = s

        return f.url
