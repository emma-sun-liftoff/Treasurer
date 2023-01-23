-- info: fetch all Treasurer-enabled campaigns' updates

WITH info AS (
   SELECT 
    tm.campaign_id,
    case when 
      cast(ec.logged_at as date) < cast('2022-12-12' as date) then cast('2022-12-13' as timestamp(6)) 
      else cast(ec.logged_at as timestamp(3))
    end as logged_at,
    cast(coalesce(LEAD(ec.logged_at,2) OVER(PARTITION BY tm.campaign_id, json_extract_scalar(ec.old_values, '$.margin_type') ORDER BY ec.logged_at), CURRENT_DATE) as timestamp(3)) as next_logged_at,
    json_extract_scalar(ec.new_values, '$.margin_type') AS margin_type,
    coalesce (LAG(tm.vungle_gross_margin, 1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at),999999) AS old_vungle_gross_margin,
    coalesce (LAG(tm.non_vungle_gross_margin, 1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at),999999) AS old_non_vungle_gross_margin,
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
)


-- ps0: fetch all performance data from supply pipeline
, ps0 as (

  -- to fetch upper funnel data
 SELECT 
  campaign_id
  , date_trunc('day', from_iso8601_timestamp(dt)) as dt
  , try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") as test_group_name
  , CASE 
      WHEN exchange = 'VUNGLE' THEN 'Vungle'
    ELSE 'Non-Vungle'
    END AS exchange_group
  , platform
  , customer_name
  , customer_id
  , dest_app_id
  , ad_format
  , CASE 
      WHEN country = 'US' THEN 'US'
    ELSE 'Non-US'
    END as country_group
  , CASE 
      WHEN current_optimization_state IN ('cpr','cpr-vt','cprv3','cprv3-vt') THEN 'CPR'
      WHEN current_optimization_state IN ('cpa','cpa-vt') THEN 'CPA'
      WHEN current_optimization_state IN ('cpi','cpi-vt') THEN 'CPI'
    ELSE 'others'
    END  AS current_optimization_state 
  , pptype
  , SUM(impressions) as impressions
  , SUM(clicks) as clicks
  , SUM(installs) as installs
  , SUM(spend_micros) as treasurer_spend_micros
  , SUM(revenue_micros) as treasurer_revenue_micros
  , 0 as customer_revenue_micros_d7
  , 0 as events_d7
  , 0 as sum_capped_customer_revenue_7d
  , 0 as sum_squared_capped_customer_revenue_7d
from product_analytics.supply_analytics_hourly_v2
where try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") IS NOT NULL
AND dt BETWEEN '2022-12-12T00' and '2024-01-01'
AND impression_at BETWEEN '2022-12-12T00' and '2024-01-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12


UNION ALL 

-- to fetch down funnel data (we are using 7d cohorted by installs data)
 SELECT 
  campaign_id
  , date_trunc('day', from_iso8601_timestamp(install_at)) as dt
  , try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") as test_group_name
  , CASE 
      WHEN exchange = 'VUNGLE' THEN 'Vungle'
    ELSE 'Non-Vungle'
    END AS exchange_group
  , platform
  , customer_name
  , customer_id
  , dest_app_id
  , ad_format
  , CASE 
      WHEN country = 'US' THEN 'US'
    ELSE 'Non-US'
    END as country_group
  , CASE 
      WHEN current_optimization_state IN ('cpr','cpr-vt','cprv3','cprv3-vt') THEN 'CPR'
      WHEN current_optimization_state IN ('cpa','cpa-vt') THEN 'CPA'
      WHEN current_optimization_state IN ('cpi','cpi-vt') THEN 'CPI'
    ELSE 'others'
    END  AS current_optimization_state 
  , pptype
  , 0 as impressions
  , 0 as clicks
  , 0 as installs
  , 0 as treasurer_spend_micros
  , 0 as treasurer_revenue_micros
  , SUM(
      CASE
        WHEN from_iso8601_timestamp(at) - from_iso8601_timestamp(install_at) < interval '7' day
          THEN customer_revenue_micros 
      ELSE 0
      END) AS  customer_revenue_micros_d7
  , SUM(
      CASE
        WHEN from_iso8601_timestamp(at) - from_iso8601_timestamp(install_at) < interval '7' day
          THEN target_events 
      ELSE 0
      END) AS  events_d7
  , 0 as sum_capped_customer_revenue_7d
  , 0 as sum_squared_capped_customer_revenue_7d
from product_analytics.supply_analytics_hourly_v2
where try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") IS NOT NULL
AND dt BETWEEN '2022-12-12T00' and '2024-01-01'
AND impression_at BETWEEN '2022-12-12T00' and '2024-01-01'
group by 1,2,3,4,5,6,7,8,9,10,11,12

UNION ALL


-- to fetch impression level 7D cohort customer revenue data used for calculating roas CI
SELECT
campaign_id
, date_trunc('day', from_iso8601_timestamp(dt)) as dt
, test_group_name
, exchange_group
, platform
, customer_name
, customer_id
, dest_app_id
, ad_format
, country_group
, current_optimization_state
, pptype
, 0 as impressions
, 0 as clicks
, 0 as installs
, 0 as treasurer_spend_micros
, 0 as treasurer_revenue_micros
, 0 as customer_revenue_micros_d7
, 0 as events_d7
, sum(capped_customer_revenue_7d) as sum_capped_customer_revenue_7d
, sum(capped_customer_revenue_7d * capped_customer_revenue_7d) as sum_squared_capped_customer_revenue_7d
FROM(
SELECT
COALESCE(attribution_event__click__impression__bid__campaign_id
        , reeng_click__impression__bid__campaign_id
        , install__ad_click__impression__bid__campaign_id) as campaign_id
, CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') as dt
, CASE WHEN COALESCE(ab_test."group".id, 0) = 2063 THEN 'holdout'
       WHEN COALESCE(ab_test."group".id, 0) = 2064 THEN 'control'
       WHEN COALESCE(ab_test."group".id, 0) = 2065 THEN 'experiment'
       ELSE NULL 
    END AS test_group_name
, CASE 
    WHEN COALESCE(attribution_event__click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange, install__ad_click__impression__bid__bid_request__exchange) = 'VUNGLE' THEN 'Vungle'
    ELSE 'Non-Vungle'
    END AS exchange_group 
, COALESCE(attribution_event__click__impression__bid__app_platform, reeng_click__impression__bid__app_platform, install__ad_click__impression__bid__app_platform) as platform
, COALESCE(attribution_event__click__impression__bid__app_id, reeng_click__impression__bid__app_id, install__ad_click__impression__bid__app_id) as dest_app_id
, COALESCE(attribution_event__click__impression__bid__customer_id, reeng_click__impression__bid__customer_id, install__ad_click__impression__bid__customer_id) as customer_id
, null AS customer_name
, if(
  (coalesce(attribution_event__click__impression__bid__creative__type, 
           reeng_click__impression__bid__creative__type, 
           install__ad_click__impression__bid__creative__type) = 'NATIVE' or coalesce(attribution_event__click__impression__bid__bid_request__impressions[1].logical_size, 
                                                                                      reeng_click__impression__bid__bid_request__impressions[1].logical_size, 
                                                                                      install__ad_click__impression__bid__bid_request__impressions[1].logical_size) = 'L0x0'),'native',
    if(coalesce(attribution_event__click__impression__bid__bid_request__impressions[1].logical_size, 
       reeng_click__impression__bid__bid_request__impressions[1].logical_size, 
       install__ad_click__impression__bid__bid_request__impressions[1].logical_size) = 'L300x250','mrec',
      if(
        (coalesce(attribution_event__click__impression__bid__bid_request__impressions[1].logical_size, 
                  reeng_click__impression__bid__bid_request__impressions[1].logical_size, 
                  install__ad_click__impression__bid__bid_request__impressions[1].logical_size) = 'L320x50' or coalesce(attribution_event__click__impression__bid__bid_request__impressions[1].logical_size, 
                                                                                                                        reeng_click__impression__bid__bid_request__impressions[1].logical_size, 
                                                                                                                        install__ad_click__impression__bid__bid_request__impressions[1].logical_size) = 'L728x90'),'banner',
        if(
          (coalesce(attribution_event__click__impression__bid__bid_request__impressions[1].logical_size, 
                    reeng_click__impression__bid__bid_request__impressions[1].logical_size, 
                    install__ad_click__impression__bid__bid_request__impressions[1].logical_size) = 'UNKNOWN_LOGICAL_SIZE' or coalesce(attribution_event__click__impression__bid__bid_request__impressions[1].logical_size, 
                                                                                                                                       reeng_click__impression__bid__bid_request__impressions[1].logical_size, 
                                                                                                                                       install__ad_click__impression__bid__bid_request__impressions[1].logical_size) is null),'UNKNOWN_LOGICAL_SIZE','interstitial')))) as ad_format
, CASE WHEN coalesce(attribution_event__click__geo__country, reeng_click__geo__country, install__geo__country) = 'US' THEN 'US'
    ELSE 'Non-US'
    END AS country_group
, 'CPR' AS current_optimization_state
, cast(json_extract(from_utf8(coalesce(attribution_event__click__impression__bid__bid_request__raw, reeng_click__impression__bid__bid_request__raw, install__ad_click__impression__bid__bid_request__raw)), '$.imp[0].ext.pptype') as integer) as pptype
, install__ad_click__impression__auction_id
, install__ad_click__impression__channel_id
, SUM(CAST(CASE
           WHEN at - install__at < 7*24*60*60*1000 AND customer_revenue_micros < 500000000 THEN
           customer_revenue_micros
           ELSE 500000000
         END AS DOUBLE)/1000000) AS capped_customer_revenue_7d
FROM rtb.app_events AS app 
CROSS JOIN Unnest (
     install__ad_click__impression__bid__bid_request__ab_test_assignments) AS ab_test
WHERE  dt BETWEEN '2022-12-12T00' and '2024-01-01'
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') >= '2022-12-12T00' 
    AND CONCAT(SUBSTR(to_iso8601(date_trunc('day', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') <= '2024-01-01'
    AND ab_test.id = 916
    AND coalesce(attribution_event__click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange, install__ad_click__impression__bid__bid_request__exchange) <> 'LIFTOFF'
    AND coalesce(attribution_event__click__impression__bid__creative__type, reeng_click__impression__bid__creative__type, install__ad_click__impression__bid__creative__type) <> 'UNMATCHED'
    AND is_uncredited = false
    AND customer_revenue_micros > 0
    AND at - install__at < 7*24*60*60*1000
    AND for_reporting = true
    AND install__ad_click__impression__bid__price_data__model_type LIKE '%revenue%'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14) as T
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12


)


-- ps: to get all fields pulled above
, ps as (
  SELECT
  campaign_id
  , dt 
  , test_group_name
  , exchange_group
  , platform
  , customer_name
  , customer_id
  , dest_app_id
  , ad_format
  , country_group
  , current_optimization_state
  , pptype
  , SUM(impressions) as impressions
  , SUM(clicks) as clicks
  , SUM(installs) as installs
  , SUM(treasurer_spend_micros) as treasurer_spend_micros
  , SUM(treasurer_revenue_micros) as treasurer_revenue_micros
  , SUM(customer_revenue_micros_d7) as customer_revenue_micros_d7
  , SUM(events_d7) as events_d7
  , SUM(sum_capped_customer_revenue_7d) as sum_capped_customer_revenue_7d
  , SUM(sum_squared_capped_customer_revenue_7d) as sum_squared_capped_customer_revenue_7d
  FROM ps0
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)


-- prep: to merge info table and ps table so we can have performance data mapped against each Treasurer update
, prep AS ( 
  SELECT 
  ps.campaign_id as campaign_id
  , ps.dt
  , ps.test_group_name
  , ps.exchange_group
  , ps.platform
  , ps.customer_name
  , ps.customer_id
  , ps.dest_app_id
  , ps.ad_format
  , ps.country_group
  , ps.current_optimization_state
  , ps.pptype
  , info.logged_at
  , info.next_logged_at
  , info.margin_type
  , max(CASE WHEN ps.dt >= info.logged_at AND ps.dt < info.next_logged_at  THEN info.new_vungle_gross_margin 
           WHEN ps.test_group_name = 'holdout' THEN 999
           END) as new_vungle_gross_margin
  , max(CASE WHEN ps.dt >= info.logged_at AND ps.dt < info.next_logged_at  THEN info.old_vungle_gross_margin 
           WHEN ps.test_group_name = 'holdout' THEN 999
           END) as old_vungle_gross_margin
  , max(CASE WHEN ps.dt >= info.logged_at AND ps.dt < info.next_logged_at  THEN info.new_non_vungle_gross_margin 
           WHEN ps.test_group_name = 'holdout' THEN 999 
           END) as new_non_vungle_gross_margin
  , max(CASE WHEN ps.dt >= info.logged_at AND ps.dt < info.next_logged_at  THEN info.old_non_vungle_gross_margin 
           WHEN ps.test_group_name = 'holdout' THEN 999 
           END) as old_non_vungle_gross_margin
  , max(info.target) as target
  , max(info.threshold) as threshold
  , max(info.daily_revenue_limit) as daily_revenue_limit
  , MAX(0) as all_revenue_micros
  , MAX(ps.treasurer_revenue_micros) as treasurer_revenue_micros
  , MAX(0) as all_spend_micros
  , MAX(ps.treasurer_spend_micros) as treasurer_spend_micros
  , MAX(ps.impressions) as impressions
  , MAX(ps.clicks) as clicks
  , MAX(ps.installs) as installs
  , MAX(ps.customer_revenue_micros_d7) as customer_revenue_micros_d7
  , MAX(ps.events_d7) as events_d7
  , MAX(ps.sum_capped_customer_revenue_7d) as sum_capped_customer_revenue_7d
  , MAX(ps.sum_squared_capped_customer_revenue_7d) as sum_squared_capped_customer_revenue_7d
FROM ps 
LEFT JOIN info 
ON ps.campaign_id = info.campaign_id 
AND ps.test_group_name = info.margin_type
WHERE ps.dt >= CAST('2022-12-12' AS timestamp(3)) and ps.dt <= CAST('2023-11-18' AS timestamp(3))
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)


-- last: last prep for all updates+performance data
, last as (
  -- to fetch all needed data for Treasurer-enabled campaigns
  SELECT
  campaign_id
  , dt
  , test_group_name
  , exchange_group
  , platform
  , customer_name
  , customer_id
  , dest_app_id
  , ad_format
  , country_group
  , current_optimization_state
  , pptype
  , logged_at
  , next_logged_at
  , target
  , margin_type
  , old_vungle_gross_margin
  , old_non_vungle_gross_margin
  , new_vungle_gross_margin
  , new_non_vungle_gross_margin
  , threshold
  , daily_revenue_limit
  , all_revenue_micros
  , treasurer_revenue_micros
  , all_spend_micros
  , treasurer_spend_micros
  , impressions
  , clicks
  , installs
  , customer_revenue_micros_d7
  , events_d7
  , sum_capped_customer_revenue_7d
  , sum_squared_capped_customer_revenue_7d
FROM prep
WHERE new_vungle_gross_margin is not NULL
AND new_non_vungle_gross_margin is not NULL


UNION ALL

  -- to fetch all business GR/spend data (to measure Treasurer's impact)
SELECT 
  campaign_id
, date_trunc('day', from_iso8601_timestamp(dt)) as dt
, 'ALL' as test_group_name
, CASE 
      WHEN exchange = 'VUNGLE' THEN 'Vungle'
    ELSE 'Non-Vungle'
    END AS exchange_group
  , platform
  , customer_name
  , customer_id
  , dest_app_id
  , ad_format
  , CASE 
      WHEN country = 'US' THEN 'US'
    ELSE 'Non-US'
    END as country_group
  , CASE 
      WHEN current_optimization_state IN ('cpr','cpr-vt','cprv3','cprv3-vt') THEN 'CPR'
      WHEN current_optimization_state IN ('cpa','cpa-vt') THEN 'CPA'
      WHEN current_optimization_state IN ('cpi','cpi-vt') THEN 'CPI'
    ELSE 'others'
    END  AS current_optimization_state 
  , pptype
  , null as logged_at
  , null as next_logged_at
  , null as target
  , null as margin_type
  , null as old_vungle_gross_margin
  , null as old_non_vungle_gross_margin
  , null as new_vungle_gross_margin
  , null as new_non_vungle_gross_margin
  , null as threshold
  , null as daily_revenue_limit
  , SUM(revenue_micros) as all_revenue_micros 
  , null as treasurer_revenue_micros
  , SUM(spend_micros) as all_spend_micros
  , null as treasurer_spend_micros
  , null as impressions
  , null as clicks
  , null as installs
  , null as customer_revenue_micros_d7
  , null as events_d7
  , null as sum_capped_customer_revenue_7d
  , null as sum_squared_capped_customer_revenue_7d
from product_analytics.supply_analytics_hourly_v2 
where dt >= '2022-12-12'  and dt <= '2023-11-18' 
group by 1,2,3,4,5,6,7,8,9,10,11,12
)


SELECT
campaign_id
, dt
, test_group_name
, exchange_group
, platform
, customer_name
, customer_id
, dest_app_id
, ad_format
, country_group
, current_optimization_state
, pptype
, logged_at
, next_logged_at
, target
, margin_type
, old_vungle_gross_margin
, old_non_vungle_gross_margin
, new_vungle_gross_margin
, new_non_vungle_gross_margin
, threshold
, daily_revenue_limit
, all_revenue_micros
, treasurer_revenue_micros
, all_spend_micros
, treasurer_spend_micros
, impressions
, clicks
, installs
, customer_revenue_micros_d7
, events_d7
, sum_capped_customer_revenue_7d
, sum_squared_capped_customer_revenue_7d
from last
