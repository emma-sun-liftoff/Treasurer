WITH 
	target_country AS (SELECT
      sales_region__c as region
    , campaign_id_18_digit__c
    , b.id as campaign_id
  FROM salesforce_daily.customer_campaign__c a 
  join pinpoint.public.campaigns b 
    on a.campaign_id_18_digit__c = b.salesforce_campaign_id
),


rpi_state AS (

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
  	AND c.final_optimization_state IN ('cpi','cpi-vt')
    AND c.id NOT IN (13551,
21326,
21339,
21325,
21340,
13346,
13995,
14281,
26529,
26669,
25714,
25053,
11869,
9933,
1729,
16080,
27136,
8302,
27348,
26993,
27005,
26996,
26994,
26647,
21384,
24822,
27527,
27525,
27334,
27245,
27243,
27528,
27526,
27244,
27522,
22751,
22927,
27577,
27575,
27018,
16267,
26718,
5700,
26717,
3582,
25237,
16264,
5815,
25628,
16265,
25659,
11020,
11128,
5608,
20929,
27198,
20928,
23329,
6311,
27355,
6316,
6403,
6313,
23414,
23328,
20931,
20930,
5607,
6502,
27197,
20914,
23337,
23333,
6765,
6315,
26809,
26811,
26810,
5323,
23320,
5606,
27199,
22847,
24798,
23327,
6795,
5604,
5320,
23339,
24794,
6433,
23347,
5473,
27200,
22843,
23345,
23341,
6701,
6635,
6634,
5398,
23413,
6390,
27201,
5469,
23317,
6889,
6416,
23412,
23330,
23296,
6891,
6791,
6550,
23349,
6430,
6896,
6740,
23353,
22856,
27202,
22854,
22848,
6899,
26506,
14071,
23558,
15100,
27203,
15102,
26466,
15368,
15103,
26137,
26465,
15105,
15104,
15101,
6319,
23509,
7048,
7047,
20989,
15136,
6763,
6333,
7075,
26815,
6415,
20926,
8552,
20925,
6318,
20927,
23513,
23512,
9529,
6950,
26816,
19988,
20944,
20932,
27130,
20934,
23718,
6946,
20946,
20950,
19990,
23722,
23720,
20948,
6944,
23540,
25877,
25479,
26821,
26820,
27267,
24896,
25369,
24281,
25126
)
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
	, rs.region
--	, ad.platform
--	, ad.current_optimization_state
--	, ad.final_optimization_state
	, rs.state
	, ad.ae_email 
	, ad.csm_email 
	, rs.daily_revenue_limit
	, SUM(CAST(ad.revenue_micros_d1 as DOUBLE)/1000000) as revenue 
  	, SUM(ad.installs_d1) as installs
  	, SUM(ad.target_events_d7) as events_d7
  	, SUM(CAST(ad.customer_revenue_micros_d7 AS DOUBLE)/1000000) as customer_revenue_d7
from analytics.daily_attr_event_d7 ad
RIGHT JOIN rpi_state rs
	ON rs.campaign_id = ad.campaign_id 
where date_diff('day', from_iso8601_timestamp(ad.dt), current_date) <= 14
group by 1,2,3,4,5,6,7,8,9

