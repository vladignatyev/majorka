import os
import json

from data.framework.reporting import Database
from data.model import ENTITIES, REPORTING_DB, DataImport

from flask import Flask, request


app = Flask(__name__)


def get_targetings_sql_by_campaign(campaign_id, dimensions=()):
    strings = [None] * len(dimensions)

    for i, dimension in enumerate(dimensions):
        strings[i] = """select '{dimension}' as dim, count(distinct({dimension})) as p from majorka.hits as hit
                     where hit.campaign={campaign_id}\n""".format(campaign_id=campaign_id, dimension=dimension)

    return "union all\n".join(strings)

def get_campaigns_sql(): return "select * from majorka.campaigns;"
def get_offers_sql(): return "select * from majorka.offers;"
def get_hit_by_id_sql(hit_id): return "select * from majorka.hits as hit where hit.dim_external_id='{id}'".format(id=hit_id)

def get_data_for_targetings_tree_sql(campaign_id):
    return """select *
              from majorka.hits as hit
              left join majorka.conversions as conversion
              on conversion.external_id == hit.dim_external_id where hit.campaign={campaign}""".format(campaign=campaign_id)

@app.route("/api/campaign/<int:campaign_id>/targetings/", methods=('GET', ))
def get_targetings_by_campaign(campaign_id):
    db = app.config.db
    db.connected()

    # get dimensions names
    dimensions = filter(lambda field: 'dim_' in field, [field for field, t in list(db.describe('hits'))])
    sql = get_targetings_sql_by_campaign(campaign_id, dimensions=dimensions)

    result = {}
    for o, i, l in db.read(sql, columns=db.describe_query(sql=sql)):
        result[o['dim']] = o['p']

    return json.dumps(result, sort_keys=True, default=str, indent=4)

@app.route("/api/campaign/<int:campaign_id>/targetings/tree", methods=('GET', ))
def get_targetings_tree_data(campaign_id):
    db = app.config.db
    db.connected()

    targetings = request.args.get('targetings', '')

    sql = get_data_for_targetings_tree_sql(campaign_id=campaign_id)

    hits = [o for o, i, l in db.read(sql, columns=db.describe_query(sql=sql))]

    # inspired by: testtools.models.MultiDimensionDistribution


    return json.dumps(result, sort_keys=True, default=str, indent=4)


@app.route("/api/campaign/<int:campaign_id>/iplist", methods=('GET', ))
def get_targetings_tree_data(campaign_id):
    db = app.config.db
    db.connected()

    sql = "SELECT DISTINCT(dim_ip) FROM majorka.hits as hit WHERE hit.campaign={campaign_id};".format(campaign_id=campaign_id)

    ips = [o for o, i, l in db.read(sql, columns=db.describe_query(sql=sql))]

    # inspired by: testtools.models.MultiDimensionDistribution


    return json.dumps(ips, sort_keys=True, default=str, indent=4)


@app.route("/api/campaigns/list/", methods=('GET', ))
def get_campaigns():
    db = app.config.db
    db.connected()

    sql = get_campaigns_sql()

    result = {'list': []}
    for o, i, l in db.read(sql, columns=db.describe_query(sql=sql)):
        result['list'].append(o)

    return json.dumps(result, sort_keys=True, default=str, indent=4)


@app.route("/api/offers/list/", methods=('GET', ))
def get_offers():
    db = app.config.db
    db.connected()

    sql = get_offers_sql()

    result = {'list': []}
    for o, i, l in db.read(sql, columns=db.describe_query(sql=sql)):
        result['list'].append(o)

    return json.dumps(result, sort_keys=True, default=str, indent=4)


# @app.route("/api/hits/list/page/<int:page>/perpage/<int:per_page>/", methods=('GET', ))
# def get_hit_list(page, per_page):
#     db = app.config.db
#     db.connected()
#
#     sql = get_campaigns_sql()
#
#     result = {'list': []}
#     for o, i, l in db.read(sql, columns=db.describe_query(sql=sql)):
#         result['list'].append(o)
#
#     return json.dumps(result, sort_keys=True, default=str, indent=4)
#
#
# @app.route("/api/hit/<hit_id>/", methods=('GET', ))
# def get_hit_by_id(hit_id):
#     db = app.config.db
#     db.connected()
#
#     sql = get_hit_by_id(hit_id)
#
#     result = {'list': []}
#     for o, i, l in db.read(sql, columns=db.describe_query(sql=sql)):
#         result['list'].append(o)
#
#     return json.dumps(result, sort_keys=True, default=str, indent=4)




if __name__ == '__main__':
    clickhouse_url = os.environ.get('CLICKHOUSE_URL', None)

    if not clickhouse_url:
        raise Exception("\n\nSet the 'CLICKHOUSE_URL' environmental variable to the URL of Clickhouse instance/slave. Example: http://127.0.0.1:8123/\n")
        sys.exit(1)

    app.config.db = Database(url=clickhouse_url, db=REPORTING_DB)

    app.run()
