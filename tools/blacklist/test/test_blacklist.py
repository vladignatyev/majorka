import unittest
import logging

from blacklist import *

sql_template = """select Site, Min(Clicks), MAX(ROI_ALL) FROM
(select
    dim_zone as Site,
    dim_connection_type as Connection_Type,
    name as Offer,
    count(Site) as Clicks,
    countIf(hits_conversions.external_id, hits_conversions.external_id != '') as Conversions,
    if(toFloat64(Clicks) > toFloat64(0.0), toFloat64(Conversions) / toFloat64(Clicks) * 100.0, -100.0) as CR,
    sum(cost) as Cost,
    sum(revenue) as Revenue_all,
    Revenue_all - Cost as Profit_Loss_All,
    if(toFloat64(Cost) > toFloat64(0.0), toFloat64(Profit_Loss_All) / toFloat64(Cost) * 100.0, -100.0) as ROI_ALL,
    (Revenue_all - Cost) / Clicks * 1000 as RPM
from majorka.offers as offers
inner join
    (
    select *
    from
    majorka.hits AS hits
    left join majorka.conversions AS leads
    ON hits.dim_external_id == leads.external_id
    WHERE
        dim_zone != 'AB_TEST'
        AND
        dim_zone != '{zoneid}'
        AND dim_zone !=''
        AND campaign = {campaign_id}
        AND dim_useragent != 'ApacheBench/2.3'
    )
as hits_conversions
ON hits_conversions.destination == offers.id
GROUP BY Site, Connection_Type, Offer
) as report
GROUP BY Site
HAVING MIN(Clicks) >= {min_clicks} AND MAX(ROI_ALL) <= {acceptable_roi}
order by Site
"""

expected_rendered_template = sql_template.format(zoneid='{zoneid}', \
                            min_clicks=30, \
                            acceptable_roi=15, \
                            campaign_id=0)

expected_rendered_template_with_another_params = sql_template.format( \
                            zoneid='{zoneid}', \
                            min_clicks=65, \
                            acceptable_roi=20, \
                            campaign_id=3)

CURRENT_DIR = os.path.dirname(__file__)
LOG_PATH = os.path.join(CURRENT_DIR, 'blacklist.test.log')

class BlacklistTest(unittest.TestCase):
    # def test_should_load_sql_template(self):
    #     slice_blacklist = SliceBlacklist()
    #     self.assertEquals(expected_rendered_template, slice_blacklist.__final_sql__())
    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger('blacklist.test')
        cls.logger.setLevel(logging.DEBUG)


        file_handler = logging.FileHandler(LOG_PATH, mode='w')
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        file_handler.setFormatter(formatter)

        cls.logger.addHandler(file_handler)


    def test_should_has_custom_logger(self):

        slice_blacklist = SliceBlacklist(campaign_id=0, logger=self.logger)
        self.assertEquals(id(self.logger), id(slice_blacklist.logger))

    def test_should_raise_error_when_campaign_id_is_missed(self):
        with self.assertRaises(Exception) as context:
            slice_blacklist = SliceBlacklist(campaign_id=None, logger=self.logger)

        self.assertTrue("should be set campaign_id" in context.exception)

        with open(LOG_PATH, 'r') as f:
            log_str = f.read()
            self.assertTrue('ERROR - should be set campaign_id' in log_str)

    def test_should_raise_error_when_campaign_id_is_not_int(self):
        with self.assertRaises(Exception) as context:
            slice_blacklist = SliceBlacklist(campaign_id="", logger=self.logger)

        self.assertTrue("should be set campaign_id" in context.exception)

    def test_should_has_default_min_clicks_and_acceptable_roi(self):
        slice_blacklist = SliceBlacklist(campaign_id=0, logger=self.logger)
        expected_acceptable_roi = 15
        expected_min_clicks = 30
        self.assertEquals(30, slice_blacklist.min_clicks)
        self.assertEquals(15, slice_blacklist.acceptable_roi)
        self.assertEquals(0, slice_blacklist.campaign_id)

    def test_should_render_sql_template(self):
        slice_blacklist = SliceBlacklist(min_clicks=65, acceptable_roi=20, \
                                        campaign_id=3)
        actual_sql = slice_blacklist.final_sql()
        self.assertEquals(expected_rendered_template_with_another_params, \
                            actual_sql)
