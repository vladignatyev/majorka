#-*- coding: utf-8 -*-
import sys
import os

import click

CURRENT_DIR = os.path.dirname(__file__)
SQL_PATH = os.path.join(CURRENT_DIR, '../sql/slice_offer_connection_zones_blacklist.sql')

DEFAULT_MIN_CLICKS = 30
DEFAULT_ACCEPTABLE_ROI = 15

class SliceBlacklist(object):
    """docstring for SliceBlacklist."""
    def __init__(self, campaign_id):
        self.min_clicks = 30
        self.acceptable_roi = 15
        if not isinstance(campaign_id, (int, long)):
            raise ValueError('should be set campaign_id')
        super(SliceBlacklist, self).__init__()

    def final_sql(self):
        template = None
        with open(SQL_PATH, 'r') as f:
            template = f.read()
        return template.format(zoneid='{zoneid}', min_clicks=self.min_clicks, \
                                acceptable_roi=self.acceptable_roi)
