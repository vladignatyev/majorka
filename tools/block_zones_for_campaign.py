import argparse
import sys
from credentials import credentials
from api.propellerads import PropellerAds
import logging

LOG_PATH_TEMPLATE = 'block_zones_{campaign_id}.log'

def setup_logger(campaign_id):
    logger = logging.getLogger('block_zones')
    logger.setLevel(logging.DEBUG)

    log_path = LOG_PATH_TEMPLATE.format(campaign_id=campaign_id)

    file_handler = logging.FileHandler(log_path, mode='a')
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def block_zones_for_given_campaign(zones, campaign_id):
    propeller = PropellerAds(credentials['username'], credentials['password'], \
                            logger=setup_logger(campaign_id))

    propeller.authorize()
    assert propeller.is_authorized()

    logger = logging.getLogger('block_zones')

    uniq_zones = set(zones)

    existing_black_zones = propeller.authorized().campaign_get_exclude_zones(campaign_id=campaign_id)

    set_existing_black_zones = set(existing_black_zones)

    new_zones_black_list = list(set_existing_black_zones.union(uniq_zones))

    propeller.authorized().campaign_set_exclude_zones(campaign_id, new_zones_black_list)

    exclude_zones = propeller.authorized().campaign_get_exclude_zones(campaign_id=campaign_id)
    logger.info('exclude %s', exclude_zones)


parser = argparse.ArgumentParser(description="Block zones given via stdin or file for given campaign")
parser.add_argument('--campaign_id', required=True, type=int)
parser.add_argument('input_filename', nargs='?')

args = parser.parse_args()
zones = []

if args.input_filename:
    with open(args.input_filename) as f:
        zones = f.read().splitlines()
elif not sys.stdin.isatty():
    print 'reading stdin...'
    zones = sys.stdin.read().splitlines()
else:
    parser.print_help()
    sys.exit(0)

block_zones_for_given_campaign(zones, args.campaign_id)
