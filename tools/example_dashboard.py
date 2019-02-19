from datetime import datetime, timedelta

from apis.propellerads import PropellerAds
from credentials import credentials

from prettytable import PrettyTable



campaigns_stat = PrettyTable()
campaigns_stat.field_names = ['\33[1m\33[37mID\33[0m', '\33[1m\33[37mName\33[0m', '\33[1m\33[37mHits\33[0m', '\33[1m\33[37mTotal hits\33[0m', '\33[1m\33[37mCost, $\33[0m', '\33[1m\33[37mTotal, $\33[0m']


propeller = PropellerAds(**credentials)
campaigns_by_id = dict([(str(c['id']), c['name']) for c in propeller.authorized().campaigns_all()])

total_cost = 0.0
total_impressions = 0

for c in propeller.authorized().get_statistics(date_from=(datetime.now() - timedelta(days=13)),
                                               date_to=datetime.now(),
                                               group_by=(PropellerAds.GroupBy.CAMPAIGN_ID,)):
    total_cost = total_cost + float(c['money'])
    total_impressions = total_impressions + int(c['impressions'])
    campaigns_stat.add_row([str(c['campaign_id']),
                            campaigns_by_id[str(c['campaign_id'])],
                            c['impressions'],
                            total_impressions,
                            '${:7,.3f}'.format(float(c['money'])),
                            '${:7,.3f}'.format(total_cost)
                            ])


print campaigns_stat
