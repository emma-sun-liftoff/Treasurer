WITH funnel as (
    -- to fetch impressions and AoVX NR
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') as impression_at
    , null as install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') as at
    , ab_test."group".name AS test_group_name
    , bid__campaign_id as campaign_id
    , bid__customer_id AS customer_id
    , CASE 
        WHEN bid__bid_request__exchange = 'VUNGLE' THEN 'Vungle'
      ELSE 'Non-Vungle'
        END AS exchange_group
    , bid__app_platform AS platform
    , cast(json_extract(from_utf8(bid__bid_request__raw), '$.imp[0].ext.pptype') AS integer) AS pptype
    , sum(1) AS impressions
    , sum(0) AS installs
    , sum(spend_micros) AS treasurer_spend_micros
    , sum(revenue_micros) AS treasurer_revenue_micros
    , sum(0) AS customer_revenue_micros
    , sum(0) AS events
    , sum(0) AS sum_capped_customer_revenue_7d
    , sum(0) AS sum_squared_capped_customer_revenue_7d
    , sum(CASE WHEN cast(json_extract(from_utf8(bid__bid_request__raw), '$.imp[0].ext.pptype') AS integer) = 1 
                    AND bid__bid_request__exchange = 'VUNGLE' 
                    THEN spend_micros * 0.3526
               WHEN cast(json_extract(from_utf8(bid__bid_request__raw), '$.imp[0].ext.pptype') AS integer) = 2
                    AND bid__bid_request__exchange = 'VUNGLE'     
                    THEN spend_micros * 0.4437
               WHEN cast(json_extract(from_utf8(bid__bid_request__raw), '$.imp[0].ext.pptype') AS integer) = 3
                    AND bid__bid_request__exchange = 'VUNGLE'     
                    THEN spend_micros * 0.3016
               WHEN cast(json_extract(from_utf8(bid__bid_request__raw), '$.imp[0].ext.pptype') AS integer) NOT IN (1,2,3)
                    AND bid__bid_request__exchange = 'VUNGLE'     
                    THEN spend_micros * 0.3319
               ELSE 0
          END) AS aovx_nr_micros_1
    , sum(CASE
        WHEN bid__bid_request__impressions[1].vungle_revenue.payout_type = 'FLAT_CPM' 
             AND bid__bid_request__exchange = 'VUNGLE'
        THEN (spend_micros - (bid__bid_request__impressions[1].vungle_revenue.publisher_flat_cpm_micros/1000))
        WHEN bid__bid_request__impressions[1].vungle_revenue.payout_type = 'REVENUE_SHARE' 
             AND bid__bid_request__exchange = 'VUNGLE'
        THEN spend_micros * (1.0 - bid__bid_request__impressions[1].vungle_revenue.publisher_revenue_share)
        WHEN bid__bid_request__impressions[1].vungle_revenue.payout_type = 'HEADER_BIDDING'
             AND bid__bid_request__exchange = 'VUNGLE'
        THEN spend_micros - mediation_price_micros
        ELSE 0
        END) AS aovx_nr_micros_2 
    FROM rtb.impressions_with_bids a
    CROSS JOIN UNNEST(bid__bid_request__ab_test_assignments) as ab_test
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND ab_test.id = 916
    GROUP BY 1,2,3,4,5,6,7,8,9

    UNION ALL 
    -- fetch matched installs
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') as impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') as install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , ab_test."group".name as test_group_name
    , ad_click__impression__bid__campaign_id as campaign_id
    , ad_click__impression__bid__customer_id as customer_id
    , CASE 
        WHEN ad_click__impression__bid__bid_request__exchange = 'VUNGLE' THEN 'Vungle'
      ELSE 'Non-Vungle'
        END AS exchange_group
    , ad_click__impression__bid__bid_request__device__platform AS platform
    , cast(json_extract(from_utf8(ad_click__impression__bid__bid_request__raw), '$.imp[0].ext.pptype') AS integer) AS pptype
    , sum(0) AS impressions
    , sum(1) AS installs
    , sum(0) AS treasurer_spend_micros
    , sum(0) AS treasurer_revenue_micros
    , sum(0) AS customer_revenue_micros
    , sum(0) AS events
    , sum(0) AS sum_capped_customer_revenue_7d
    , sum(0) AS sum_squared_capped_customer_revenue_7d
    , sum(0) AS aovx_nr_micros_1 
    , sum(0) AS aovx_nr_micros_2
    FROM rtb.matched_installs a
    CROSS JOIN UNNEST(ad_click__impression__bid__bid_request__ab_test_assignments) ab_test
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND ab_test.id = 916
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9
    
    UNION ALL 
    -- fetch unmatched installs
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(ad_click__impression__at/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , ab_test."group".name as test_group_name
    , tracker_params__campaign_id AS campaign_id 
    , b.customer_id AS customer_id
    , 'UNMATCHED' AS exchange_group
    , tracker_params__platform AS platform
    , CAST(json_extract(from_utf8(ad_click__impression__bid__bid_request__raw), '$.imp[0].ext.pptype') AS integer) AS pptype
    , sum(0) AS impressions
    , sum(1) AS installs
    , sum(0) AS treasurer_spend_micros
    , sum(0) AS treasurer_revenue_micros
    , sum(0) AS customer_revenue_micros
    , sum(0) AS events
    , sum(0) AS sum_capped_customer_revenue_7d
    , sum(0) AS sum_squared_capped_customer_revenue_7d
    , sum(0) AS aovx_nr_micros_1 
    , sum(0) AS aovx_nr_micros_2
    FROM rtb.unmatched_installs a
    CROSS JOIN UNNEST(ad_click__impression__bid__bid_request__ab_test_assignments) ab_test
    LEFT JOIN pinpoint.public.campaigns b
        ON a.tracker_params__campaign_id = b.id
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND ab_test.id = 916
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9

    UNION ALL 
    -- to fetch down funnel data (we are using 7d cohorted by installs data)
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__impression__at, reeng_click__impression__at, install__ad_click__impression__at)/1000, 'UTC'))),1,19),'Z') as impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') as install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') as at
    , ab_test."group".name as test_group_name
    , COALESCE(attribution_event__click__impression__bid__campaign_id, reeng_click__impression__bid__campaign_id, install__ad_click__impression__bid__campaign_id) as campaign_id
    , COALESCE(attribution_event__click__impression__bid__customer_id, reeng_click__impression__bid__customer_id, install__ad_click__impression__bid__customer_id) as customer_id
    , CASE WHEN COALESCE(attribution_event__click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange, install__ad_click__impression__bid__bid_request__exchange) = 'VUNGLE' THEN 'Vungle'
        ELSE 'Non-Vungle'
      END AS exchange_group
    , COALESCE(attribution_event__click__impression__bid__app_platform, reeng_click__impression__bid__app_platform, install__ad_click__impression__bid__app_platform) as platform
    , CAST(json_extract(from_utf8(coalesce(attribution_event__click__impression__bid__bid_request__raw, reeng_click__impression__bid__bid_request__raw, install__ad_click__impression__bid__bid_request__raw)), '$.imp[0].ext.pptype') AS integer) AS pptype
    , sum(0) AS impressions
    , sum(0) AS installs
    , sum(0) AS treasurer_spend_micros
    , sum(0) AS treasurer_revenue_micros
    , sum(if(customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000, customer_revenue_micros, 0)) AS  customer_revenue_micros
    , sum(if(pinpoint_event_ids.cpa_target_event_id = custom_event_id,1,0)) AS  events
    , sum(0) AS sum_capped_customer_revenue_7d
    , sum(0) AS sum_squared_capped_customer_revenue_7d
    , sum(0) AS aovx_nr_micros_1 
    , sum(0) AS aovx_nr_micros_2
    FROM rtb.matched_app_events a
    CROSS JOIN UNNEST (
           install__ad_click__impression__bid__bid_request__ab_test_assignments) AS ab_test
    LEFT JOIN (SELECT
                id as campaign_id
                , cpa_target_event_id
                FROM pinpoint.public.campaigns) pinpoint_event_ids
        ON coalesce(attribution_event__click__impression__bid__campaign_id
                , reeng_click__impression__bid__campaign_id
                , install__ad_click__impression__bid__campaign_id) = pinpoint_event_ids.campaign_id
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND ab_test.id = 916
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9
    
    UNION ALL 
    -- to fetch down funnel data (we are using 7d cohorted by installs data)
    SELECT
    CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__impression__at, reeng_click__impression__at, install__ad_click__impression__at)/1000, 'UTC'))),1,19),'Z') AS impression_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') AS install_at
    , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') AS at
    , ab_test."group".name as test_group_name
    , tracker_params__campaign_id AS campaign_id
    , b.customer_id AS customer_id
    , 'UNMATCHED' AS exchange_group
    , tracker_params__platform AS platform
    , CAST(json_extract(from_utf8(coalesce(attribution_event__click__impression__bid__bid_request__raw, reeng_click__impression__bid__bid_request__raw, install__ad_click__impression__bid__bid_request__raw)), '$.imp[0].ext.pptype') AS integer) AS pptype
    , sum(0) AS impressions
    , sum(0) AS installs
    , sum(0) AS treasurer_spend_micros
    , sum(0) AS treasurer_revenue_micros
    , sum(if(customer_revenue_micros > -100000000000 AND customer_revenue_micros < 100000000000, customer_revenue_micros, 0)) AS  customer_revenue_micros
    , sum(if(pinpoint_event_ids.cpa_target_event_id = custom_event_id,1,0)) AS  events
    , sum(0) AS sum_capped_customer_revenue_7d
    , sum(0) AS sum_squared_capped_customer_revenue_7d
    , sum(0) AS aovx_nr_micros_1 
    , sum(0) AS aovx_nr_micros_2
    FROM rtb.unmatched_app_events a
    CROSS JOIN UNNEST (
           install__ad_click__impression__bid__bid_request__ab_test_assignments) AS ab_test
    LEFT JOIN pinpoint.public.campaigns b
        ON a.tracker_params__campaign_id = b.id
    LEFT JOIN (SELECT
                id as campaign_id
                , cpa_target_event_id
                FROM pinpoint.public.campaigns) pinpoint_event_ids
        ON a.tracker_params__campaign_id = pinpoint_event_ids.campaign_id
    WHERE dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
        AND ab_test.id = 916
        AND for_reporting = TRUE
        AND NOT is_uncredited
    GROUP BY 1,2,3,4,5,6,7,8,9
    
    UNION ALL 
    -- to fetch impression level 7D cohort customer revenue data used for calculating roas CI
    SELECT
    impression_at
    , install_at
    , at 
    , test_group_name
    , campaign_id
    , customer_id
    , exchange_group
    , platform
    , pptype
    , sum(0) AS impressions
    , sum(0) AS installs
    , sum(0) AS treasurer_spend_micros
    , sum(0) AS treasurer_revenue_micros
    , sum(0) AS customer_revenue_micros
    , sum(0) AS events
    , sum(capped_customer_revenue_7d) as sum_capped_customer_revenue_7d
    , sum(capped_customer_revenue_7d * capped_customer_revenue_7d) as sum_squared_capped_customer_revenue_7d
    , sum(0) AS aovx_nr_micros_1 
    , sum(0) AS aovx_nr_micros_2
    FROM(
      SELECT
        COALESCE(attribution_event__click__impression__bid__campaign_id
              , reeng_click__impression__bid__campaign_id
              , install__ad_click__impression__bid__campaign_id) as campaign_id
        , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(COALESCE(attribution_event__click__impression__at, reeng_click__impression__at, install__ad_click__impression__at)/1000, 'UTC'))),1,19),'Z') as impression_at
        , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(install__at/1000, 'UTC'))),1,19),'Z') as install_at
        , CONCAT(SUBSTR(to_iso8601(date_trunc('hour', from_unixtime(event_timestamp/1000, 'UTC'))),1,19),'Z') as at
        , CASE WHEN COALESCE(ab_test."group".id, 0) = 2064 THEN 'control'
             WHEN COALESCE(ab_test."group".id, 0) = 2065 THEN 'experiment'
             ELSE NULL 
          END AS test_group_name
        , CASE 
          WHEN COALESCE(attribution_event__click__impression__bid__bid_request__exchange, reeng_click__impression__bid__bid_request__exchange, install__ad_click__impression__bid__bid_request__exchange) = 'VUNGLE' THEN 'Vungle'
          ELSE 'Non-Vungle'
          END AS exchange_group 
        , COALESCE(attribution_event__click__impression__bid__app_platform, reeng_click__impression__bid__app_platform, install__ad_click__impression__bid__app_platform) as platform
        , COALESCE(attribution_event__click__impression__bid__customer_id, reeng_click__impression__bid__customer_id, install__ad_click__impression__bid__customer_id) as customer_id
        , install__ad_click__impression__auction_id
        , install__ad_click__impression__channel_id
        , CAST(json_extract(from_utf8(coalesce(attribution_event__click__impression__bid__bid_request__raw, reeng_click__impression__bid__bid_request__raw, install__ad_click__impression__bid__bid_request__raw)), '$.imp[0].ext.pptype') AS integer) AS pptype
        , SUM(CAST(CASE
                   WHEN at - install__at < 7*24*60*60*1000 AND customer_revenue_micros < 500000000 THEN
                 customer_revenue_micros
                 ELSE 500000000
               END AS DOUBLE)/1000000) AS capped_customer_revenue_7d
      FROM rtb.app_events AS app 
      CROSS JOIN Unnest (
           install__ad_click__impression__bid__bid_request__ab_test_assignments) AS ab_test
      WHERE  dt >= '{{ dt }}' AND dt < '{{ dt_add(dt, hours=1) }}'
          AND ab_test.id = 916
          AND coalesce(attribution_event__click__impression__bid__creative__type, reeng_click__impression__bid__creative__type, install__ad_click__impression__bid__creative__type) <> 'UNMATCHED'
          AND is_uncredited = false
          AND customer_revenue_micros > 0
          AND at - install__at < 7*24*60*60*1000
          AND for_reporting = true
          AND install__ad_click__impression__bid__price_data__model_type LIKE '%revenue%'
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11) as T
    GROUP BY 1,2,3,4,5,6,7,8,9

)
-- to fetch all thresholds and update time
, thresholds AS (
  SELECT 
  ctc.campaign_id
   , date_trunc('hour', ec.logged_at) AS logged_at
   , date_trunc('hour', COALESCE(LEAD(ec.logged_at, 1) OVER (PARTITION BY ctc.campaign_id ORDER BY ec.logged_at), CURRENT_TIMESTAMP)) AS next_logged_at
   , json_extract_scalar(ec.new_values, '$.threshold') AS threshold
 FROM pinpoint.public.campaign_treasurer_configs ctc
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  ctc.id = ec.row_id
  WHERE ec.table_name = 'campaign_treasurer_configs'
  AND json_extract(ec.new_values, '$.threshold') IS NOT NULL 
)
-- to fetch all targets and update time 
, targets AS (
  SELECT 
  ctc.campaign_id
   , date_trunc('hour', ec.logged_at) AS logged_at
   , date_trunc('hour', COALESCE(LEAD(ec.logged_at, 1) OVER (PARTITION BY ctc.campaign_id ORDER BY ec.logged_at), CURRENT_TIMESTAMP)) AS next_logged_at
   , json_extract_scalar(ec.new_values, '$.target') AS target
  FROM pinpoint.public.campaign_treasurer_configs ctc
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  ctc.id = ec.row_id
  WHERE ec.table_name = 'campaign_treasurer_configs'
  AND json_extract(ec.new_values, '$.target') IS NOT NULL 
)
-- to fetch all margins and update time
, margins AS (
   SELECT 
    tm.campaign_id
    , date_trunc('hour', ec.logged_at) AS logged_at
    , date_trunc('hour', COALESCE(LEAD(ec.logged_at, 2) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.old_values, '$.margin_type') ORDER BY ec.logged_at), CURRENT_TIMESTAMP)) AS next_logged_at
    , json_extract_scalar(ec.new_values, '$.margin_type') AS margin_type
    , COALESCE (LAG(tm.vungle_gross_margin, 1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at), 999999) AS old_vungle_gross_margin
    , COALESCE (LAG(tm.non_vungle_gross_margin, 1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at), 999999) AS old_non_vungle_gross_margin
    , tm.vungle_gross_margin as new_vungle_gross_margin
    , tm.non_vungle_gross_margin as new_non_vungle_gross_margin
    FROM pinpoint.public.treasurer_margins tm
    FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
    WHERE ec.table_name = 'treasurer_margins'
    AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
   )
 -- to fetch all daily cap changes and update time  
 , daily_cap AS (
    SELECT 
      c.id AS campaign_id
     , date_trunc('hour', ec.logged_at) AS logged_at
    , date_trunc('hour', COALESCE(LEAD(ec.logged_at, 1) OVER (PARTITION BY c.id ORDER BY ec.logged_at), CURRENT_TIMESTAMP)) AS next_logged_at
    , json_extract_scalar(ec.new_values, '$.daily_revenue_limit') AS daily_cap
    FROM pinpoint.public.campaigns c
    FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  c.id = ec.row_id
    WHERE ec.table_name IN ('campaigns')
    AND ec.operation = 'update'
    AND json_extract(ec.new_values, '$.daily_revenue_limit') IS NOT NULL 
    AND date(ec.logged_at) BETWEEN date('2022-10-20') AND date('2024-04-30')
  )
 , latest_sfdc_partition AS (
    SELECT MAX(dt) as latest_dt 
    FROM salesforce_daily.customer_campaign__c  
    WHERE from_iso8601_timestamp(dt) >= CURRENT_TIMESTAMP - interval '2' DAY
)
 , saleforce_data AS (
    SELECT 
      b.id AS campaign_id
      , sd.sales_region__c as sales_region
      , sd.service_level__c AS service_level
    FROM salesforce_daily.customer_campaign__c sd 
    JOIN pinpoint.public.campaigns b      
        ON sd.campaign_id_18_digit__c = b.salesforce_campaign_id
    WHERE sd.dt = (select latest_dt from latest_sfdc_partition)
)
-- to aggregate measure data and calculate cohorted metrics
, measures AS (
  SELECT
    f.impression_at
    , f.install_at
    , f.at
    , f.test_group_name
    , f.campaign_id
    , f.customer_id
    , f.exchange_group
    , f.platform
    , f.pptype
    , sum(f.impressions) AS impressions
    , sum(f.installs) AS installs
    , sum(f.treasurer_spend_micros) AS treasurer_spend_micros
    , sum(f.treasurer_revenue_micros) AS treasurer_revenue_micros
    , sum(CASE WHEN from_iso8601_timestamp(f.at) - from_iso8601_timestamp(f.install_at) < interval '7' day THEN f.customer_revenue_micros ELSE 0 END) AS customer_revenue_micros_7d
    , sum(CASE WHEN from_iso8601_timestamp(f.at) - from_iso8601_timestamp(f.install_at) < interval '7' day THEN f.events ELSE 0 END) AS events_7d
    , sum(f.sum_capped_customer_revenue_7d) AS sum_capped_customer_revenue_7d
    , sum(f.sum_squared_capped_customer_revenue_7d) AS sum_squared_capped_customer_revenue_7d
    , sum(f.aovx_nr_micros_1) AS aovx_nr_micros_1
    , sum(f.aovx_nr_micros_2) AS aovx_nr_micros_2 
  FROM funnel f
  GROUP BY 1,2,3,4,5,6,7,8,9
)
  SELECT
    f.impression_at
    , f.install_at
    , f.at
    , f.test_group_name
    , f.campaign_id
    , f.customer_id
    , f.exchange_group
    , f.platform
    , f.pptype
    , cu.company AS customer_name
    , sd.sales_region
    , sd.service_level
    , c.current_optimization_state AS current_optimization_state
    , m.logged_at AS margin_updated_date
    , m.next_logged_at AS next_margin_updated_date
    , m.margin_type
    , max(f.impressions) AS impressions
    , max(f.installs) AS installs
    , max(f.treasurer_spend_micros) AS treasurer_spend_micros
    , max(f.treasurer_revenue_micros) AS treasurer_revenue_micros
    , max(f.customer_revenue_micros_7d) AS customer_revenue_micros_7d
    , max(f.events_7d) AS events_7d
    , max(f.sum_capped_customer_revenue_7d) AS sum_capped_customer_revenue_7d
    , max(f.sum_squared_capped_customer_revenue_7d) AS sum_squared_capped_customer_revenue_7d
    , max(f.aovx_nr_micros_1) AS aovx_nr_micros_1
    , max(f.aovx_nr_micros_2) AS aovx_nr_micros_2 
    , max(CASE WHEN from_iso8601_timestamp(f.impression_at) >= m.logged_at AND from_iso8601_timestamp(f.impression_at) < m.next_logged_at THEN m.new_vungle_gross_margin END) AS new_vungle_gross_margin
    , max(CASE WHEN from_iso8601_timestamp(f.impression_at) >= m.logged_at AND from_iso8601_timestamp(f.impression_at) < m.next_logged_at THEN m.old_vungle_gross_margin END) AS old_vungle_gross_margin
    , max(CASE WHEN from_iso8601_timestamp(f.impression_at) >= m.logged_at AND from_iso8601_timestamp(f.impression_at) < m.next_logged_at THEN m.new_non_vungle_gross_margin END) AS new_non_vungle_gross_margin
    , max(CASE WHEN from_iso8601_timestamp(f.impression_at) >= m.logged_at AND from_iso8601_timestamp(f.impression_at) < m.next_logged_at THEN m.old_non_vungle_gross_margin END) AS old_non_vungle_gross_margin
    , max(CASE WHEN from_iso8601_timestamp(f.impression_at) >= t.logged_at AND from_iso8601_timestamp(f.impression_at) < t.next_logged_at THEN t.target END) AS current_target
    , max(CASE WHEN from_iso8601_timestamp(f.impression_at) >= th.logged_at AND from_iso8601_timestamp(f.impression_at) < th.next_logged_at THEN th.threshold END) AS threshold
    , max(CASE WHEN from_iso8601_timestamp(f.impression_at) >= dc.logged_at AND from_iso8601_timestamp(f.impression_at) < dc.next_logged_at THEN dc.daily_cap END) AS daily_cap
  FROM measures f
  LEFT JOIN margins m
   ON f.campaign_id = m.campaign_id  AND f.test_group_name = m.margin_type
  LEFT JOIN targets t 
     ON f.campaign_id = t.campaign_id 
  LEFT JOIN thresholds th 
     ON f.campaign_id = th.campaign_id
  LEFT JOIN saleforce_data sd 
     ON f.campaign_id = sd.campaign_id
  LEFT JOIN daily_cap dc 
     ON f.campaign_id = dc.campaign_id
  LEFT JOIN pinpoint.public.customers cu
     ON f.customer_id = cu.id
  LEFT JOIN pinpoint.public.campaigns c
     ON f.campaign_id = c.id
  WHERE from_iso8601_timestamp(f.impression_at) >= m.logged_at 
    AND from_iso8601_timestamp(f.impression_at) < m.next_logged_at
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
