-- BI fields (<5 secs)

WITH latest_sfdc_partition AS (
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
WHERE ad.dt BETWEEN '2023-01-01' AND '2024-01-30'
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
WHERE ad.dt BETWEEN '2023-01-01' AND '2024-01-30'
	AND is_uncredited<>'true'
GROUP BY 1,2
)


, measures1_ AS (
 SELECT 
   date_trunc('day', from_iso8601_timestamp(ad.dt)) AS dt
  , ad.campaign_id
  , SUM(ad.target_events_d7) AS target_events_d7
  , SUM(CAST(ad.customer_revenue_micros_d7 AS DOUBLE)/1000000) AS customer_revenue_d7
FROM analytics.trimmed_daily_attr_event_d7_v1 ad
WHERE ad.dt BETWEEN '2023-01-01' AND '2024-01-30' 
	AND is_uncredited<>'true'
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


	SELECT
	f.campaign_id
	, date(m.dt) AS dt
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
