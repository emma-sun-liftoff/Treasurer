WITH ref AS (
SELECT date_trunc('day', from_iso8601_timestamp(dt)) as dt
, campaign_id
, sum(cast(revenue_micros as double)/1000000) as revenue 
, sum(cast(case when exchange = 'VUNGLE' then revenue_micros end as DOUBLE)/1000000) as revenue_v
from analytics.trimmed_daily 
where campaign_id IN (7802, 2677, 11152, 5988)
and dt >= '2023-02-04' and dt <= '2023-04-18'
GROUP by 1,2
)

, treasurer_ds_v5 AS (
SELECT
date_trunc('day', from_iso8601_timestamp(dt)) as dt
, campaign_id
, sum(cast(treasurer_revenue_micros as double)/1000000) as revenue 
, sum(cast(case when exchange_group  = 'Vungle' then treasurer_revenue_micros end as DOUBLE)/1000000) as revenue_v
from product_analytics.treasurer_ds_v5
WHERE dt >= '2023-02-04' and dt <= '2023-04-18'
AND impression_at >= '2023-03-04' and impression_at <= '2023-04-18'
AND campaign_id IN (7802, 2677, 11152, 5988)
GROUP BY 1,2
)

, cal AS (
SELECT
a.dt AS dt 
, a.campaign_id AS campaign_id
, e.revenue AS expected_rev
, a.revenue AS actual_rev
, e.revenue_v AS expected_rev_v
, a.revenue_v AS actual_rev_v
, (e.revenue/a.revenue - 1) as delta  
, (e.revenue_v/a.revenue_v - 1) as delta_v 
from ref e
join treasurer_ds_v5 a on e.campaign_id = a.campaign_id and e.dt = a.dt
)

SELECT*
from cal
where delta >= 0.001 OR delta <= -0.001 OR delta_v >= 0.001 OR delta_v <= -0.001 
ORDER BY 1,2


