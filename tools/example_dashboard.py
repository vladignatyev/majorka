from datetime import datetime, timedelta

from api.propellerads import PropellerAds
from credentials import credentials

from prettytable import PrettyTable

# Adds terminal color to string
def bold_white(s): return '\33[1m\33[37m %s \33[0m' % s

# ****************************
# * Simple dashboard example *
# ****************************

# Initialize API
api = PropellerAds(**credentials)

# Load all campaigns into dict of ID -> Name
campaigns_by_id = dict([(str(c['id']), c['name']) for c in api.authorized().campaigns_all()])

# `PrettyTable` stays for formatted output of tables
t = PrettyTable()
# Setting up field names with terminal coloring: ID, Name, etc. ...
t.field_names = [bold_white('ID'),
                 bold_white('Name'),
                 bold_white('Hits'),
                 bold_white('Total hits'),
                 bold_white('Cost, $'),
                 bold_white('Total, $')]

# Totals counters
total_cost = 0.0
total_impressions = 0

# Get statistics for latest 13 days, groupped by campaign id
for c in api.authorized().get_statistics(date_from=(datetime.now() - timedelta(days=13)),
                                         date_to=datetime.now(),
                                         group_by=(PropellerAds.GroupBy.CAMPAIGN_ID,)):
    cost = float(c['money'])
    impressions = int(c['impressions'])
    id = str(c['campaign_id'])
    name = campaigns_by_id[str(c['campaign_id'])]

    # Calculate totals
    total_cost = total_cost + cost
    total_impressions = total_impressions + impressions

    # Build table row
    t.add_row([id, name, impressions, total_impressions, '${:7,.3f}'.format(cost), '${:7,.3f}'.format(total_cost)])

# Print table
print t
