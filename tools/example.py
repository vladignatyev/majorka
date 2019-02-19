import logging
from apis.propellerads import PropellerAds
from credentials import credentials

# ugly format because of handmade coloring
logging.basicConfig(format='\33[92m[%(name)s] \33[0m\33[90m%(asctime)-15s\33[1m\33[37m %(message)s\33[0m')
logger = logging.getLogger('propellerads')
logger.setLevel(logging.DEBUG)

propeller = PropellerAds(credentials['username'], credentials['password'], logger=logger)
# p.authorize()
# assert p.is_authorized()
#
# for campaign in propeller.authorized().campaigns_by_statuses(PropellerAds.Status.STOPPED):
#     print campaign

# from datetime import datetime, timedelta
# for stat in propeller.authorized().get_statistics(campaign_ids=(1791408,),
#                                                   date_from=(datetime.now() - timedelta(days=7)),
#                                                   date_to=datetime.now(),
#                                                   group_by=(PropellerAds.GroupBy.ZONE_ID,)):
#     print stat


# print propeller.authorized().campaign_set_include_zones(campaign_id=1791408, zones=[634917, 762488])
print propeller.authorized().campaign_set_exclude_zones(campaign_id=1791408, zones=[634917, 762488])


# for campaign in propeller.authorized().campaigns_all():
#     print campaign

# propeller.authorized().campaigns_by_statuses(PropellerAds.Status.STOPPED)

# print json.dumps(propeller.authorized().campaigns_by_statuses(PropellerAds.Status.WORKING), indent=4, sort_keys=True)

# print json.dumps(propeller.authorized().campaigns_by_statuses(PropellerAds.Status.STOPPED), indent=4, sort_keys=True)
# print propeller.authorized().campaign_start_by_id(1790281)

# propeller.authorized().campaign_stop_by_id(1791408)
# print propeller.authorized().campaign_start_by_id(1790281)

# print propeller.authorized().campaign_get_exclude_zones(1734282)
# assert len(propeller.authorized().campaign_get_exclude_zones(1734282)) == 25

# print propeller.authorized().campaign_info_by_id(1734282)

# print propeller.authorized().campaigns_by_statuses(PropellerAds.Status.STOPPED)
