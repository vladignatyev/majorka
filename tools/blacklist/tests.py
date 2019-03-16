import unittest

from blacklist import *

expected_rendered_template = """select Site, Min(Clicks), MAX(ROI_ALL) FROM
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
        AND campaign = 0
        AND dim_useragent != 'ApacheBench/2.3'
    )
as hits_conversions
ON hits_conversions.destination == offers.id
GROUP BY Site, Connection_Type, Offer
) as report
GROUP BY Site
HAVING MIN(Clicks) >= 30 AND MAX(ROI_ALL) <= 15
order by Site
"""

class BlacklistTest(unittest.TestCase):
    # def test_should_load_sql_template(self):
    #     slice_blacklist = SliceBlacklist()
    #     self.assertEquals(expected_rendered_template, slice_blacklist.__final_sql__())
    def test_should_raise_error_when_campaign_id_is_missed(self):
        with self.assertRaises(Exception) as context:
            slice_blacklist = SliceBlacklist(campaign_id=None)

        print 'exception ', context.exception
        self.assertTrue("should be set campaign_id" in context.exception)

    def test_should_raise_error_when_campaign_id_is_not_int(self):
        with self.assertRaises(Exception) as context:
            slice_blacklist = SliceBlacklist(campaign_id="")

        print 'exception ', context.exception
        self.assertTrue("should be set campaign_id" in context.exception)

    def test_should_has_default_min_clicks_and_acceptable_roi(self):
        slice_blacklist = SliceBlacklist(campaign_id=0)
        expected_acceptable_roi = 15
        expected_min_clicks = 30
        self.assertEquals(30, slice_blacklist.min_clicks)
        self.assertEquals(15, slice_blacklist.acceptable_roi)

    def test_should_render_sql_template(self):
        slice_blacklist = SliceBlacklist(campaign_id=0)
        actual_sql = slice_blacklist.final_sql()
        self.assertEquals(expected_rendered_template, actual_sql)
