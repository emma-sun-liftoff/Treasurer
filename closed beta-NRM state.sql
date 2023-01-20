WITH 
	target_country AS (SELECT
      sales_region__c as region
    , campaign_id_18_digit__c
    , b.id as campaign_id
  FROM salesforce_daily.customer_campaign__c a 
  join pinpoint.public.campaigns b 
    on a.campaign_id_18_digit__c = b.salesforce_campaign_id
),


nrm_state AS (

SELECT
tc.region  as region
, c.id as campaign_id
, c.daily_revenue_limit
, c.state
FROM pinpoint.public.campaigns c 
LEFT JOIN target_country tc ON tc.campaign_id = c.id
WHERE date_diff('day', c.created_at, current_date) >= 33
	AND c.daily_revenue_limit > 1000
	AND c.campaign_type_id = 1 -- UA Campaigns only
    AND c.state = 'enabled'
    AND c.id = 20385
)

SELECT 
	ad.customer_id 
	, ad.customer_name 
--	, ad.dest_app_id 
--	, ad.dest_app_name 
--	, ad.dest_app_category
	, ad.campaign_id
	, ad.campaign_name
--	, ad.campaign_type
	, ns.region
--	, ad.platform
--	, ad.current_optimization_state
--	, ad.final_optimization_state
	, ns.state
	, ad.ae_email 
	, ad.csm_email 
	, ns.daily_revenue_limit
	, SUM(CAST(ad.revenue_micros_d1 as DOUBLE)/1000000) as revenue 
  	, SUM(ad.installs_d1) as installs
  	, SUM(ad.target_events_d7) as events_d7
  	, SUM(CAST(ad.customer_revenue_micros_d7 AS DOUBLE)/1000000) as customer_revenue_d7
from analytics.daily_attr_event_d7 ad
INNER JOIN nrm_state ns 
	ON ns.campaign_id = ad.campaign_id 
where date_diff('day', from_iso8601_timestamp(ad.dt), current_date) <= 14
group by 1,2,3,4,5,6,7,8,9

