-- All Campaign List
SELECT 
DISTINCT campaign_id
from product_analytics.supply_analytics_hourly_v2
where try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") IS NOT NULL
AND dt BETWEEN '2022-12-13T00' and '2024-01-01'
AND impression_at BETWEEN '2022-12-13T00' and '2024-01-01'
ORDER BY 1



-- All Campaign Count 
SELECT 
  try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") as test_group_name
  , COUNT(DISTINCT campaign_id) as campaign_count
  , SUM(spend_micros) as treasurer_spend_micros
  , SUM(revenue_micros) as treasurer_revenue_micros
from product_analytics.supply_analytics_hourly_v2
where try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") IS NOT NULL
AND dt BETWEEN '2022-12-13T00' and '2024-01-01'
AND impression_at BETWEEN '2022-12-13T00' and '2024-01-01'
group by 1


-- All business campaigns in LO (should be the same as above)
SELECT 
  COUNT(DISTINCT campaign_id) as campaign_count
  , SUM(spend_micros) as treasurer_spend_micros
  , SUM(revenue_micros) as treasurer_revenue_micros
from product_analytics.supply_analytics_hourly_v2
where dt BETWEEN '2022-12-13T00' and '2024-01-01'
AND impression_at BETWEEN '2022-12-13T00' and '2024-01-01'


-- Treasurer Enabled Campaigns Count
    select 
    count(distinct tm.campaign_id) as campaign_count
    , ctc.target
    FROM pinpoint.public.campaign_treasurer_configs ctc 
    FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
    FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
    WHERE ec.table_name = 'treasurer_margins'
    AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
    AND CAST(ec.logged_at AS timestamp(3)) > CAST('2023-01-01' AS timestamp(3))
    GROUP BY 2


-- Treasurer Enabled Campaigns List
    select 
    tm.campaign_id
    FROM pinpoint.public.campaign_treasurer_configs ctc 
    FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
    FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
    WHERE ec.table_name = 'treasurer_margins'
    AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')


-- Treausurer Enabled Campaigns GR
SELECT 
  COUNT(DISTINCT campaign_id) as campaign_count
  , SUM(spend_micros) as treasurer_spend_micros
  , SUM(revenue_micros) as treasurer_revenue_micros
from product_analytics.supply_analytics_hourly_v2
where dt BETWEEN '2022-12-13T00' and '2024-01-01'
AND impression_at BETWEEN '2022-12-13T00' and '2024-01-01'
and campaign_id in (select distinct tm.campaign_id
    FROM pinpoint.public.campaign_treasurer_configs ctc 
    FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
    FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
    WHERE ec.table_name = 'treasurer_margins'
    AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
    -- AND ctc.target = 'NR')

-- Non-Active Campaigns Count [no update since GA]
SELECT 
target
, count(distinct campaign_id) as campaign_count
FROM(
SELECT
  tm.campaign_id
  , ctc.target
  , max(tm.updated_at) as max_update
FROM pinpoint.public.campaign_treasurer_configs ctc 
FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
WHERE ec.table_name = 'treasurer_margins'
AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
GROUP BY 1,2)
WHERE cast(max_update as date) < cast('2022-12-12' as date)
GROUP BY 1



-- Non-Active Campaigns List [no update since GA]
SELECT 
distinct campaign_id
FROM(
SELECT
  tm.campaign_id
  , ctc.target
  , max(tm.updated_at) as max_update
FROM pinpoint.public.campaign_treasurer_configs ctc 
FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
WHERE ec.table_name = 'treasurer_margins'
AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
GROUP BY 1,2)
WHERE cast(max_update as date) < cast('2022-12-12' as date)


-- Active Campaigns Count 
SELECT 
target
, count(distinct campaign_id) as campaign_count
FROM(
SELECT
  tm.campaign_id
  , ctc.target
  , max(tm.updated_at) as max_update
FROM pinpoint.public.campaign_treasurer_configs ctc 
FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
WHERE ec.table_name = 'treasurer_margins'
AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
GROUP BY 1,2)
WHERE cast(max_update as date) < cast('2022-12-12' as date)
GROUP BY 1



-- Active Campaigns List 
SELECT 
distinct campaign_id
FROM(
SELECT
  tm.campaign_id
  , ctc.target
  , max(tm.updated_at) as max_update
FROM pinpoint.public.campaign_treasurer_configs ctc 
FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
WHERE ec.table_name = 'treasurer_margins'
AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
GROUP BY 1,2)
WHERE cast(max_update as date) < cast('2022-12-12' as date)



--------------------------- campaign no update
  -- campaign list
  SELECT
  tm.campaign_id,
  tm.updated_at,
  tm.margin_type,
  tm.vungle_gross_margin AS new_vungle_gross_margin,
  tm.non_vungle_gross_margin  AS new_non_vungle_gross_margin,
  ctc.target,
  ctc.threshold,
  c.daily_revenue_limit
  FROM pinpoint.public.campaign_treasurer_configs ctc 
  FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
  LEFT JOIN pinpoint.public.campaigns c ON ctc.campaign_id = c.id
  WHERE ec.table_name = 'treasurer_margins'
  AND json_extract_scalar(ec.new_values, '$.vungle_gross_margin') IS NOT NULL 
  AND json_extract_scalar(ec.new_values, '$.non_vungle_gross_margin') IS NOT NULL
  AND tm.margin_type IS NOT NULL
  --and tm.campaign_id in (5908,4587, 27477)
  and cast(tm.updated_at as date) <= cast('2022-12-09' as date)
  order by 1


-- campaign stats
SELECT 
  campaign_id
  , try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") as test_group_name
  , SUM(revenue_micros) as treasurer_revenue_micros
from product_analytics.supply_analytics_hourly_v2
where try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") IS NOT NULL
AND dt BETWEEN '2022-12-13T00' and '2024-01-01'
AND impression_at BETWEEN '2022-12-13T00' and '2024-01-01'
AND campaign_id IN (SELECT
  DISTINCT tm.campaign_id
  FROM pinpoint.public.campaign_treasurer_configs ctc 
  FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
  WHERE ec.table_name = 'treasurer_margins'
  AND json_extract_scalar(ec.new_values, '$.vungle_gross_margin') IS NOT NULL 
  AND json_extract_scalar(ec.new_values, '$.non_vungle_gross_margin') IS NOT NULL
  AND tm.margin_type IS NOT NULL
  and cast(tm.updated_at as date) <= cast('2022-12-09' as date))
group by 1,2
order by 1



----------------- Campaigns' unique updates
---- Unique updates
select* 
from ( select 
  tm.campaign_id,
  ec.logged_at,
  json_extract_scalar(ec.new_values, '$.margin_type') AS margin_type,
  coalesce (LAG(tm.vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at),999999) AS old_vungle_gross_margin,
  coalesce (LAG(tm.non_vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY  ec.logged_at),999999) AS old_non_vungle_gross_margin,
  tm.vungle_gross_margin as new_vungle_gross_margin,
  tm.non_vungle_gross_margin as new_non_vungle_gross_margin,
  ctc.target, 
  ctc.threshold,
  c.daily_revenue_limit
  FROM pinpoint.public.campaign_treasurer_configs ctc 
  FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
  LEFT JOIN pinpoint.public.campaigns c ON ctc.campaign_id = c.id
  WHERE ec.table_name = 'treasurer_margins'
  AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
  AND tm.campaign_id in (5908, 4587)
  order by 1,2 desc)
 where  (old_vungle_gross_margin <> new_vungle_gross_margin
 OR old_non_vungle_gross_margin <> new_non_vungle_gross_margin)
 order by 2
