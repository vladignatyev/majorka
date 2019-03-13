#-*- coding: utf-8 -*-
import sys
import os
from data.framework.reporting import Database

from prettytable import PrettyTable
def bold_white(s): return '\33[1m\33[37m %s \33[0m' % s

import click


DB_NAME = 'majorka'


def highlight(sql):
    try:
        from pygments import highlight
        from pygments.lexers import SqlLexer
        from pygments.formatters import Terminal256Formatter
        return highlight(sql, SqlLexer(), Terminal256Formatter())
    except ImportError:
        raise Exception("Install `pygments` into your environment to use code highlighting")


class Parameter(object):
    def __init__(self, name, help, type=None):
        self.name = name
        self.help = help.decode('utf-8')
        self.type = type
    def __str__(self):
        return "{name} - {help}".format(name=self.name, help=self.help.encode('utf-8'))
    def __unicode__(self):
        return self.__str__().decode('utf-8')


class ParametricQuery(object):
    def __init__(self, sql, meta, params):
        self.sql = sql
        self.meta = meta
        self.params = params

    def to_final_sql(self, **kwargs):
        params = map(lambda p: p.name, self.params)
        out = self.sql
        for name in params:
            val = kwargs.get(name, None)
            if val is None:
                return None, name
            out = out.replace("${name}".format(name=name), str(val))
        return out, None

    def source_sql(self):
        return self.sql


def connection(ch_url=None, db_name=DB_NAME):
    if ch_url is None:
        raise Exception("Set the 'CH_URL' environmental "
                        "variable or corresponding CLI param to proper Clickhouse URL!\n")

    d = Database(url=ch_url, db=db_name)
    return d.connected()

def parse_from_lines(lines):
    sql_lines = []
    meta = {'name': '', 'description': '', 'author': ''}
    params = []

    state_description_multiline = None

    for line in lines:
        if line.strip().startswith('--'):
            if state_description_multiline:
                clean_line = line.replace('--','').strip()
                if clean_line == '':
                    state_description_multiline = None
                else:
                    meta['description'] = meta['description'] + u'\n' + clean_line.decode('utf-8')
            elif line.startswith('-- name:'):
                meta['name'] = line.split('-- name:', 1)[1].strip()
            elif line.startswith('-- author:'):
                meta['author'] = line.split('-- author:', 1)[1].strip()
            elif line.startswith('-- description:'):
                meta['description'] = line.split('-- description:', 1)[1].strip().decode('utf-8')
                state_description_multiline = True
            elif line.startswith('-- $'):
                name, help = line.split('-- $',1)[1].split(': ')
                params.append(Parameter(name=name, help=help))
        else:
            sql_lines += [line]
    return ParametricQuery(sql=''.join(sql_lines), meta=meta, params=params)

def help_for_query(query):
    qname = query.meta['name'].decode('utf-8').upper()

    return """

{qname}
{dashes}
{query_desc}

{author}

""".format(qname=qname,
           dashes='=' * len(qname),
           query_desc=query.meta['description'],
           author=query.meta['author'])

def print_usage(query, missed_param):
    raise click.UsageError(u"""
{query_help}
The following parameters are required to run this script:
    {params_str}
You can provide the parameters using the following syntax:
  pysql filename.py.sql NAME VALUE NAME VALUE NAME VALUE...
""".format(param=missed_param,
           query_help=help_for_query(query),
           params_str=u'\n\t'.join(map(unicode, query.params))))


@click.command()
@click.option('--ch-url', envvar='CH_URL', required=True, help='Clickhouse URL, i.e. http://192.168.9.39:8123/. You can use CH_URL environmental variable to set this parameter.')
@click.option('--show', help='Do not execute, just output final SQL')
@click.option('--pretty-print', help='Do not execute, just output final SQL with syntax highlighting')
@click.argument('file', type=click.File(), required=True)
@click.argument('query-param', required=False, nargs=-1)
def execute(ch_url, show, pretty_print, file, query_param):
    """Executes .py.sql file on Clickhouse server and prints result.
    """
    d = connection(ch_url=ch_url)

    kvs = zip(query_param[0:2:], query_param[1:2:])

    query = parse_from_lines(lines=file.readlines())
    sql_or_none, missed_param = query.to_final_sql(**dict(kvs))

    if sql_or_none is None:
        print_usage(query, missed_param)

    sql = sql_or_none

    if show:
        print sql
        sys.exit()
    elif pretty_print:
        print highlight(sql)
        sys.exit()
    else:
        columns = d.describe_query(sql)
        col_names = zip(*columns)[0]
        col_names_capitalized = list(map(str.capitalize, col_names))

        t = PrettyTable()
        t.field_names = map(bold_white, col_names)

        items = d.read(sql=sql, columns=columns)

        for o, i, l in items:
            t.add_row(map(lambda field: o[field], col_names))

        print t


if __name__ == '__main__':
    execute(auto_envvar_prefix='CH')
