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
  LEFT JOIN pinpoint.public.campaigns c ON ctc.campaign_id = c.id
  WHERE ec.table_name = 'treasurer_margins'
  AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('control')
  AND date(CAST(ec.logged_at AS timestamp(3))) > date('2023-02-01') 
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
  , c.daily_revenue_limit
 FROM pinpoint.public.campaign_treasurer_configs ctc 
  FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
  LEFT JOIN pinpoint.public.campaigns c ON ctc.campaign_id = c.id
  WHERE ec.table_name = 'treasurer_margins'
  AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment')
  AND date(CAST(ec.logged_at AS timestamp(3))) > date('2023-02-01') 
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
	, t.daily_revenue_limit
 FROM ctrl_gm c
 JOIN test_gm t
 	ON c.campaign_id = t.campaign_id AND c.updated_date = t.updated_date
)


, latest_sfdc_partition AS (
    SELECT max(dt) as latest_dt 
    FROM salesforce_daily.account 
    WHERE from_iso8601_timestamp(dt) >= CURRENT_TIMESTAMP - interval '2' DAY
)
 
   
, sfdc_data AS (
  SELECT 
  	b.id AS campaign_id
    , sd.sales_region__c as sales_region
    , sd.sales_sub_region__c as sales_sub_region
    , sd.service_level__c AS service_level
FROM salesforce_daily.customer_campaign__c sd 
JOIN pinpoint.public.campaigns b 
ON sd.campaign_id_18_digit__c = b.salesforce_campaign_id
  WHERE sd.dt = (select latest_dt from latest_sfdc_partition)
)


, info AS (
SELECT 
    DISTINCT ad.campaign_id
	, ad.campaign_name
	, ad.customer_id 
	, ad.customer_name   
	, ad.dest_app_id 
	, ad.dest_app_name 
	, ad.campaign_type
    , sfdc_data.sales_region
    , sfdc_data.sales_sub_region
	, ad.platform
	, sfdc_data.service_level
	, ad.ae_email 
	, ad.csm_email
FROM analytics.trimmed_daily ad
  LEFT JOIN sfdc_data
    ON sfdc_data.campaign_id = ad.campaign_id
WHERE dt >= '2023-02'
)

, money_data0 AS (
	SELECT bid__campaign_id AS campaign_id
	, date_trunc('day', from_iso8601_timestamp(dt)) AS dt
	, sum(CAST(revenue_micros AS double)/power(10,6)) AS Acc_GR
	, sum(CAST(spend_micros AS double)/power(10,6)) AS Acc_spend
	, sum(CAST(COALESCE(CASE WHEN exchange = 'VUNGLE' THEN revenue_micros ELSE 0 END,0) AS double)/power(10,6)) AS Acc_GR_on_V
	, sum(CAST(COALESCE(CASE WHEN exchange = 'VUNGLE' THEN spend_micros ELSE 0 END,0) AS double)/power(10,6)) AS Acc_spend_on_V
	FROM rtb.impressions_with_bids
	WHERE dt BETWEEN '2022-12-12' and '2024-01-30'
	AND bid__margin_data__base_gross_margin is not NULL
	GROUP BY 1,2
)


, money_data AS (
	SELECT
	campaign_id
	, dt
	, Acc_GR
	, Acc_spend
	, Acc_GR_on_V
	, Acc_spend_on_V
	, LAG(Acc_GR,1) OVER (PARTITION BY campaign_id ORDER BY dt) AS previous_day_Acc_GR
	, LAG(Acc_spend,1) OVER (PARTITION BY campaign_id ORDER BY dt) AS previous_day_Acc_spend
	, LAG(Acc_GR_on_V,1) OVER (PARTITION BY campaign_id ORDER BY dt) AS previous_day_Acc_GR_on_V
	, LAG(Acc_spend_on_V,1) OVER (PARTITION BY campaign_id ORDER BY dt) AS previous_day_Acc_spend_on_V
	FROM money_data0
)


, fields AS (
	SELECT
	md.dt 
	, i.campaign_id
	, i.campaign_name
	, i.customer_id
	, i.customer_name
	, i.dest_app_id
	, i.dest_app_name
	, i.campaign_type
	, i.sales_region
	, i.sales_sub_region
	, i.platform
	, i.service_level
	, i.ae_email
	, i.csm_email
	, md.Acc_GR
	, md.Acc_spend
	, md.Acc_GR_on_V
	, md.Acc_spend_on_V
	, md.previous_day_Acc_GR
	, md.previous_day_Acc_spend
	, md.previous_day_Acc_GR_on_V
	, md.previous_day_Acc_spend_on_V
	FROM money_data md 
	INNER JOIN info i
		ON i.campaign_id = md.campaign_id

)

SELECT 
	ug.updated_date
	, ug.campaign_id
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
	, ug.ctrl_old_vungle_gross_margin
	, ug.ctrl_old_non_vungle_gross_margin
	, ug.test_old_vungle_gross_margin
	, ug.test_old_non_vungle_gross_margin
	, ug.ctrl_new_vungle_gross_margin
	, ug.ctrl_new_non_vungle_gross_margin
	, ug.test_new_vungle_gross_margin
	, ug.test_new_non_vungle_gross_margin
	, ug.daily_revenue_limit
	, f.Acc_GR
	, f.Acc_spend
	, f.Acc_GR_on_V
	, f.Acc_spend_on_V
	, f.previous_day_Acc_GR
	, f.previous_day_Acc_spend
	, f.previous_day_Acc_GR_on_V
	, f.previous_day_Acc_spend_on_V
--	, f.installs
--	, f.target_events_d7
--	, f.customer_revenue_d7
FROM updated_gm ug
JOIN fields f 
	ON ug.campaign_id = f.campaign_id
	AND ug.updated_date = date(CAST(f.dt AS timestamp(3))) 
	
