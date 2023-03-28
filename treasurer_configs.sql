--- This data source is dagger pipeline
WITH thresholds AS (
  SELECT 
  ctc.campaign_id
   , date_trunc('hour', CAST(ec.logged_at AS timestamp(3))) AS logged_at
   , date_trunc('hour', CAST(COALESCE(LEAD(ec.logged_at, 1) OVER (PARTITION BY ctc.campaign_id ORDER BY ec.logged_at), CURRENT_TIMESTAMP) AS timestamp(3))) AS next_logged_at
   , json_extract_scalar(ec.new_values, '$.threshold') AS threshold
 FROM pinpoint.public.campaign_treasurer_configs ctc
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  ctc.id = ec.row_id
  WHERE ec.table_name = 'campaign_treasurer_configs'
  AND json_extract(ec.new_values, '$.threshold') IS NOT NULL 
  ORDER BY 1,2
)


, targets AS (
  SELECT 
  ctc.campaign_id
   , date_trunc('hour', CAST(ec.logged_at AS timestamp(3))) AS logged_at
   , date_trunc('hour', CAST(COALESCE(LEAD(ec.logged_at, 1) OVER (PARTITION BY ctc.campaign_id ORDER BY ec.logged_at), CURRENT_TIMESTAMP) AS timestamp(3))) AS next_logged_at
   , json_extract_scalar(ec.new_values, '$.target') AS target
  FROM pinpoint.public.campaign_treasurer_configs ctc
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  ctc.id = ec.row_id
  WHERE ec.table_name = 'campaign_treasurer_configs'
  AND json_extract(ec.new_values, '$.target') IS NOT NULL 

  ORDER BY 1,2
)


, margins AS (
   SELECT 
    tm.campaign_id
    , date_trunc('hour', CAST(ec.logged_at AS timestamp(3))) AS logged_at
    , date_trunc('hour', CAST(COALESCE(LEAD(ec.logged_at, 2) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.old_values, '$.margin_type') ORDER BY ec.logged_at), CURRENT_TIMESTAMP) AS timestamp(3))) AS next_logged_at
    , json_extract_scalar(ec.new_values, '$.margin_type') AS margin_type
    , COALESCE (LAG(tm.vungle_gross_margin, 1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at), 999999) AS old_vungle_gross_margin
    , COALESCE (LAG(tm.non_vungle_gross_margin, 1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at), 999999) AS old_non_vungle_gross_margin
    , tm.vungle_gross_margin as new_vungle_gross_margin
    , tm.non_vungle_gross_margin as new_non_vungle_gross_margin
    FROM pinpoint.public.treasurer_margins tm
    FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
    WHERE ec.table_name = 'treasurer_margins'
    AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')

    ORDER BY 1,2,3,4
   )
   
 , daily_cap AS (
    SELECT 
      c.id AS campaign_id
      , date_trunc('hour', CAST(ec.logged_at AS timestamp(3))) AS logged_at
    , date_trunc('hour', CAST(COALESCE(LEAD(ec.logged_at, 1) OVER (PARTITION BY c.id ORDER BY ec.logged_at), CURRENT_TIMESTAMP) AS timestamp(3))) AS next_logged_at
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

 , measurer_data AS (
    SELECT
        dt
        , bid__campaign_id as campaign_id
        , bid__customer_id AS customer_id
        , bid__app_platform AS platform
        , sum(spend_micros) AS spend_micros
        , sum(revenue_micros) AS revenue_micros
        , sum(CASE
            WHEN bid__bid_request__impressions[1].vungle_revenue.payout_type = 'FLAT_CPM' THEN
              (spend_micros - (bid__bid_request__impressions[1].vungle_revenue.publisher_flat_cpm_micros/1000))
            WHEN bid__bid_request__impressions[1].vungle_revenue.payout_type = 'REVENUE_SHARE' THEN
              spend_micros * (1.0-bid__bid_request__impressions[1].vungle_revenue.publisher_revenue_share)
            WHEN bid__bid_request__impressions[1].vungle_revenue.payout_type = 'HEADER_BIDDING' THEN
              spend_micros - mediation_price_micros
            ELSE 0
            END) AS aovx_nr_micros 
    FROM rtb.impressions_with_bids a
    CROSS JOIN UNNEST(bid__bid_request__ab_test_assignments) as ab_test
    WHERE dt = '{{ dt }}'
        AND concat(substr(to_iso8601(date_trunc('hour', from_unixtime(at/1000, 'UTC'))),1,19),'Z') = '{{ dt }}'
    GROUP BY 1,2,3,4

)

SELECT
    f.campaign_id
    , f.customer_id
    , f.platform
    , cu.company AS customer_name
    , sd.sales_region
    , sd.service_level
    , m.logged_at AS margin_updated_date
    , m.next_logged_at AS next_margin_updated_date
    , m.margin_type
    , max(f.spend_micros) AS spend_micros
    , max(f.revenue_micros) AS revenue_micros
    , max(f.aovx_nr_micros) AS aovx_nr_micros 
    , max(CASE WHEN CAST(f.dt AS timestamp(3)) >= m.logged_at AND CAST(f.dt AS timestamp(3)) < m.next_logged_at THEN m.new_vungle_gross_margin END) AS new_vungle_gross_margin
    , max(CASE WHEN CAST(f.dt AS timestamp(3)) >= m.logged_at AND CAST(f.dt AS timestamp(3)) < m.next_logged_at THEN m.old_vungle_gross_margin END) AS old_vungle_gross_margin
    , max(CASE WHEN CAST(f.dt AS timestamp(3)) >= m.logged_at AND CAST(f.dt AS timestamp(3)) < m.next_logged_at THEN m.new_non_vungle_gross_margin END) AS new_non_vungle_gross_margin
    , max(CASE WHEN CAST(f.dt AS timestamp(3)) >= m.logged_at AND CAST(f.dt AS timestamp(3)) < m.next_logged_at THEN m.old_non_vungle_gross_margin END) AS old_non_vungle_gross_margin
    , max(CASE WHEN CAST(f.dt AS timestamp(3)) >= m.logged_at AND CAST(f.dt AS timestamp(3)) < t.next_logged_at THEN t.target END) AS current_target
    , max(CASE WHEN CAST(f.dt AS timestamp(3)) >= m.logged_at AND CAST(f.dt AS timestamp(3)) < th.next_logged_at THEN th.threshold END) AS threshold
    , max(CASE WHEN CAST(f.dt AS timestamp(3)) >= m.logged_at AND CAST(f.dt AS timestamp(3)) < dc.next_logged_at THEN dc.daily_cap END) AS daily_cap
  FROM measurer_data f
  LEFT JOIN margins m
   ON f.campaign_id = m.campaign_id 
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
  WHERE CAST(f.dt AS timestamp(3)) >= m.logged_at 
    AND CAST(f.dt AS timestamp(3)) < m.next_logged_at
  GROUP BY 1,2,3,4,5,6,7,8,9,10
