#-*- coding: utf-8 -*-
import sys
import os
import logging
import textwrap
import click

CURRENT_DIR = os.path.dirname(__file__)
SQL_PATH = os.path.join(CURRENT_DIR, '../sql/slice_offer_connection_zones_blacklist.sql')

DEFAULT_MIN_CLICKS = 30
DEFAULT_ACCEPTABLE_ROI = 15

class SliceBlacklist(object):

    def __init__(self, campaign_id, logger=logging.getLogger('blacklist'), \
                        min_clicks = DEFAULT_MIN_CLICKS, \
                        acceptable_roi = DEFAULT_ACCEPTABLE_ROI):
        self.min_clicks = min_clicks
        self.acceptable_roi = acceptable_roi
        self.logger = logger


        if not isinstance(campaign_id, (int, long)):
            value_error = ValueError('should be set campaign_id')
            self.logger.exception(value_error)
            raise value_error

        self.campaign_id = campaign_id

        super(SliceBlacklist, self).__init__()

        self.logger.info(textwrap.dedent("""Created slice_blacklist algo for
                                            campaign_id - %d
                                            min_clicks - %d
                                            acceptable_roi - %d """),
                            self.campaign_id, self.min_clicks, \
                            self.acceptable_roi)

    def final_sql(self):
        template = None
        with open(SQL_PATH, 'r') as f:
            template = f.read()
        return template.format(zoneid='{zoneid}', min_clicks=self.min_clicks, \
                                acceptable_roi=self.acceptable_roi, \
                                campaign_id=self.campaign_id)
