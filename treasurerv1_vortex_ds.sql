-- Total running time: <20 secs

-- Treasurer margin updates (<5 seconds)

WITH ctrl_gm AS (
 SELECT 
  tm.campaign_id
  , date(CAST(ec.logged_at AS timestamp(3))) AS updated_date
  , COALESCE(LAG(tm.vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at),0) AS ctrl_old_vungle_gross_margin
  , COALESCE(LAG(tm.non_vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY  ec.logged_at),0) AS ctrl_old_non_vungle_gross_margin
  , tm.vungle_gross_margin AS ctrl_new_vungle_gross_margin
  , tm.non_vungle_gross_margin AS ctrl_new_non_vungle_gross_margin
 FROM pinpoint.public.campaign_treasurer_configs ctc 
  FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
  WHERE ec.table_name = 'treasurer_margins'
  AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('control')
  AND date(CAST(ec.logged_at AS timestamp(3))) > date('2022-12-12') 
  AND date(CAST(ec.logged_at AS timestamp(3))) < date('2024-01-30') 
 )


 , test_gm AS (
 SELECT 
  tm.campaign_id
  , date(CAST(ec.logged_at AS timestamp(3))) AS updated_date
  , LAG(tm.vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at) AS test_old_vungle_gross_margin
  , LAG(tm.non_vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at) AS test_old_non_vungle_gross_margin
  , tm.vungle_gross_margin AS test_new_vungle_gross_margin
  , tm.non_vungle_gross_margin AS test_new_non_vungle_gross_margin
 FROM pinpoint.public.campaign_treasurer_configs ctc 
  FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
  WHERE ec.table_name = 'treasurer_margins'
  AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment')
  AND date(CAST(ec.logged_at AS timestamp(3))) > date('2022-12-12') 
  AND date(CAST(ec.logged_at AS timestamp(3))) < date('2024-01-30') 
 )


, updated_gm AS (
	SELECT 
	t.campaign_id
	, t.updated_date
	, COALESCE(ctrl_old_vungle_gross_margin, test_old_vungle_gross_margin) AS ctrl_old_vungle_gross_margin
	, COALESCE(ctrl_old_non_vungle_gross_margin, test_old_non_vungle_gross_margin) AS ctrl_old_non_vungle_gross_margin
	, test_old_vungle_gross_margin
	, test_old_non_vungle_gross_margin
	, ctrl_new_vungle_gross_margin
	, ctrl_new_non_vungle_gross_margin
	, test_new_vungle_gross_margin
	, test_new_non_vungle_gross_margin
 FROM ctrl_gm c
 JOIN test_gm t
 	ON c.campaign_id = t.campaign_id AND c.updated_date = t.updated_date
)


-- BI fields (<5 secs)
, latest_sfdc_partition AS (
    SELECT MAX(dt) as latest_dt 
    FROM salesforce_daily.customer_campaign__c  
    WHERE from_iso8601_timestamp(dt) >= CURRENT_TIMESTAMP - interval '2' DAY
)
 
   
, sfdc_data AS (
  SELECT 
  	campaign_id_18_digit__c
    , sd.sales_region__c as sales_region
    , sd.sales_sub_region__c as sales_sub_region
    , sd.service_level__c AS service_level
FROM salesforce_daily.customer_campaign__c sd 
WHERE sd.dt = (select latest_dt from latest_sfdc_partition)
)


, fields AS (
	SELECT 
   ad.campaign_id
	, ad.campaign_name
	, ad.customer_id 
	, ad.customer_name   
	, ad.dest_app_id 
	, ad.dest_app_name 
	, ad.campaign_type
  , sd.sales_region
  , sd.sales_sub_region
	, ad.platform
	, sd.service_level
	, MAX(ad.ae_email) AS ae_email 
	, MAX(ad.csm_email) AS csm_email 
	, MAX(b.daily_revenue_limit) AS daily_revenue_limit
FROM analytics.trimmed_daily ad
JOIN pinpoint.public.campaigns b 
		ON b.id = ad.campaign_id
LEFT JOIN sfdc_data sd  			
		ON sd.campaign_id_18_digit__c = b.salesforce_campaign_id
WHERE ad.dt BETWEEN '2022-12-12' and '2024-01-30'
	AND ad.campaign_id IN (SELECT DISTINCT campaign_id FROM updated_gm)
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)


-- BI measures (<10 secs)
, measures0_ AS (
 SELECT 
   date_trunc('day', from_iso8601_timestamp(ad.dt)) AS dt
  , ad.campaign_id
	, SUM(ad.installs) as installs
  , SUM(CAST(revenue_micros AS double)/power(10,6)) AS Acc_GR
	, SUM(CAST(spend_micros AS double)/power(10,6)) AS Acc_spend
	, SUM(CAST(COALESCE(CASE WHEN exchange = 'VUNGLE' THEN revenue_micros ELSE 0 END,0) AS double)/power(10,6)) AS Acc_GR_on_V
	, sum(CAST(COALESCE(CASE WHEN exchange = 'VUNGLE' THEN spend_micros ELSE 0 END,0) AS double)/power(10,6)) AS Acc_spend_on_V
FROM analytics.trimmed_daily ad
WHERE ad.dt BETWEEN '2022-12-12' and '2024-01-30'
	AND is_uncredited<>'true'
	AND ad.campaign_id IN (SELECT DISTINCT campaign_id FROM updated_gm)
GROUP BY 1,2
)


, measures1_ AS (
 SELECT 
   date_trunc('day', from_iso8601_timestamp(ad.dt)) AS dt
  , ad.campaign_id
  , SUM(ad.target_events_d7) as target_events_d7
  , SUM(CAST(ad.customer_revenue_micros_d7 AS DOUBLE)/1000000) as customer_revenue_d7
FROM analytics.trimmed_daily_attr_event_d7_v1 ad
WHERE ad.dt BETWEEN '2022-12-12' and '2024-01-30'
	AND is_uncredited<>'true'
	AND ad.campaign_id IN (SELECT DISTINCT campaign_id FROM updated_gm)
GROUP BY 1,2
)


, measures_ AS (
	SELECT
	m0.campaign_id
	, m0.dt
	, Acc_GR
	, Acc_spend
	, Acc_GR_on_V
	, Acc_spend_on_V
	, LAG(Acc_GR,1) OVER (PARTITION BY m0.campaign_id ORDER BY m0.dt) AS previous_day_Acc_GR
	, LAG(Acc_spend,1) OVER (PARTITION BY m0.campaign_id ORDER BY m0.dt) AS previous_day_Acc_spend
	, LAG(Acc_GR_on_V,1) OVER (PARTITION BY m0.campaign_id ORDER BY m0.dt) AS previous_day_Acc_GR_on_V
	, LAG(Acc_spend_on_V,1) OVER (PARTITION BY m0.campaign_id ORDER BY m0.dt) AS previous_day_Acc_spend_on_V
	, installs
	, target_events_d7
	, customer_revenue_d7
	, LAG(installs,1) OVER (PARTITION BY m1.campaign_id ORDER BY m1.dt) AS previous_day_installs
	, LAG(target_events_d7,1) OVER (PARTITION BY m1.campaign_id ORDER BY m1.dt) AS previous_day_target_events_d7
	, LAG(customer_revenue_d7,1) OVER (PARTITION BY m1.campaign_id ORDER BY m1.dt) AS previous_day_customer_revenue_d7
	FROM measures0_ m0
	LEFT JOIN measures1_ m1
		 ON m0.campaign_id = m1.campaign_id AND m0.dt = m1.dt

)


-- (< 10 secs)
, f_merge_ AS (
	SELECT
	f.campaign_id
	, m.dt
	, f.campaign_name
	, f.customer_id
	, f.customer_name
	, f.dest_app_id
	, f.dest_app_name
	, f.campaign_type
	, f.sales_region
	, f.sales_sub_region
	, f.platform
	, f.service_level
	, f.ae_email
	, f.csm_email
	, f.daily_revenue_limit
	, m.Acc_GR
	, m.Acc_spend
	, m.Acc_GR_on_V
	, m.Acc_spend_on_V
	, m.previous_day_Acc_GR
	, m.previous_day_Acc_spend
	, m.previous_day_Acc_GR_on_V
	, m.previous_day_Acc_spend_on_V
	, m.installs
	, m.target_events_d7
	, m.customer_revenue_d7
	, m.previous_day_installs
	, m.previous_day_target_events_d7
	, m.previous_day_customer_revenue_d7
	FROM measures_ m
	INNER JOIN fields f
		ON f.campaign_id = m.campaign_id
)


SELECT 
	f.campaign_id
	, date(f.dt) AS dt
	, ug.updated_date
	, f.campaign_name
	, f.customer_id 
	, f.customer_name   
	, f.dest_app_id 
	, f.dest_app_name 
	, f.campaign_type
	, f.sales_region
	, f.sales_sub_region
	, f.platform
	, f.service_level
	, f.ae_email 
	, f.csm_email 
	, f.daily_revenue_limit
	, ug.ctrl_old_vungle_gross_margin
	, ug.ctrl_old_non_vungle_gross_margin
	, ug.test_old_vungle_gross_margin
	, ug.test_old_non_vungle_gross_margin
	, ug.ctrl_new_vungle_gross_margin
	, ug.ctrl_new_non_vungle_gross_margin
	, ug.test_new_vungle_gross_margin
	, ug.test_new_non_vungle_gross_margin
	, f.Acc_GR
	, f.Acc_spend
	, f.Acc_GR_on_V
	, f.Acc_spend_on_V
	, f.previous_day_Acc_GR
	, f.previous_day_Acc_spend
	, f.previous_day_Acc_GR_on_V
	, f.previous_day_Acc_spend_on_V
	, f.installs
	, f.target_events_d7
	, f.customer_revenue_d7
	, f.previous_day_installs
	, f.previous_day_target_events_d7
	, f.previous_day_customer_revenue_d7
FROM f_merge_ f
LEFT JOIN updated_gm ug
	ON ug.campaign_id = f.campaign_id
	AND ug.updated_date = date(CAST(f.dt AS timestamp(3)))
