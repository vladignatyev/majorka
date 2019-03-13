-- name: Показатели кампании по зонам
-- description: Запрос возвращает количество кликов, хитов, лидов,
-- доходы и расходы для выбранной кампании
--
-- $campaign: ID кампании
select
    zone,
    clicks,
    leads,
    cost,
    revenue,
    revenue - cost as profit,
    if(toFloat64(cost) > toFloat64(0.0), toFloat64(profit) / toFloat64(cost) * 100.0, -100.0) as roi,
    (revenue - cost) / clicks * 1000 as rpm
from
    (
        select
            hit.dim_zone as zone,
            sum(hit.cost) as cost,
            sum(lead.revenue) as revenue,
            count(*) as clicks
        from
            majorka.hits as hit
        left outer join
            majorka.conversions as lead
        on
            lead.external_id == hit.dim_external_id
        where
            hit.campaign = toInt64($campaign)
        group by
            zone
    ) as hit_stat

left outer join

(
    select
        hits_stat.dim_zone as zone,
        count(*) as leads
    from
        majorka.conversions as stat
    inner join
        majorka.hits as hits_stat
    on
        hits_stat.dim_external_id == stat.external_id
        and hits_stat.campaign = toInt64($campaign)
    group by
        hits_stat.dim_zone
) as leads_stat

on leads_stat.zone == hit_stat.zone
