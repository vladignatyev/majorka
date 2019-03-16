select Site, Min(Clicks), MAX(ROI_ALL) FROM
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
HAVING MIN(Clicks) >= {min_clicks} AND MAX(ROI_ALL) <= {acceptable_roi}
order by Site
