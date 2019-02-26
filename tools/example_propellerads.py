import logging
from api.propellerads import PropellerAds
from credentials import credentials

# * Setting up a logger *
# ugly format because of handmade coloring
logging.basicConfig(format='\33[92m[%(name)s] \33[0m\33[90m%(asctime)-15s\33[1m\33[37m %(message)s\33[0m')
logger = logging.getLogger('propellerads')
logger.setLevel(logging.DEBUG)

# * Initializing API *
propeller = PropellerAds(credentials['username'], credentials['password'], logger=logger)

# * Querying *
# Authorization
p.authorize()
assert p.is_authorized()

# Getting campaign by statuses
for campaign in propeller.authorized().campaigns_by_statuses(PropellerAds.Status.STOPPED):
    print campaign

# Getting a statistics object groupped by ZONE_ID for latest 7 days
from datetime import datetime, timedelta
for stat in propeller.authorized().get_statistics(campaign_ids=(1791408,),
                                                  date_from=(datetime.now() - timedelta(days=7)),
                                                  date_to=datetime.now(),
                                                  group_by=(PropellerAds.GroupBy.ZONE_ID,)):
    print stat

# Getting / setting include/exclude zones
# print propeller.authorized().campaign_set_include_zones(campaign_id=1791408, zones=[634917, 762488])
print propeller.authorized().campaign_get_exclude_zones(campaign_id=1791408, zones=[634917, 762488])


# Getting all campaigns
for campaign in propeller.authorized().campaigns_all():
    print campaign

# Starting campaign
# propeller.authorized().campaign_start_by_id(1790281)

# Stopping campaign
# propeller.authorized().campaign_stop_by_id(1791408)

# Getting full campaign information
# print propeller.authorized().campaign_info_by_id(1734282)
