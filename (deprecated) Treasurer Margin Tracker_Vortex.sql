WITH info AS (
	SELECT bid__campaign_id AS campaign_id
	, date_trunc('day', from_iso8601_timestamp(dt)) AS dt
	, sum(CAST(revenue_micros AS double)/power(10,6)) AS Acc_GR
	, sum(CAST(spend_micros AS double)/power(10,6)) AS Acc_spend
	, sum(CAST(COALESCE(CASE WHEN exchange = 'VUNGLE' THEN revenue_micros ELSE 0 END,0) AS double)/power(10,6)) AS Acc_GR_on_V
	, sum(CAST(COALESCE(CASE WHEN exchange = 'VUNGLE' THEN spend_micros ELSE 0 END,0) AS double)/power(10,6)) AS Acc_spend_on_V
	, max(CASE 
	      WHEN bid__bid_request__exchange = 'VUNGLE' THEN bid__margin_data__base_gross_margin
	    end) AS holdout_v_gross_margin 
	, max(CASE 
	    WHEN bid__bid_request__exchange != 'VUNGLE' THEN bid__margin_data__base_gross_margin
	    end) AS holdout_nv_gross_margin 
	FROM rtb.impressions_with_bids
	WHERE dt BETWEEN '2022-12-12' and '2024-01-30'
	AND bid__margin_data__base_gross_margin is not NULL
	GROUP BY 1,2)


, base_gm AS (
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
	, holdout_v_gross_margin
	, holdout_nv_gross_margin
FROM info)


, ctrl_gm AS (
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
  , c.daily_revenue_limit
 FROM pinpoint.public.campaign_treasurer_configs ctc 
  FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
  LEFT JOIN pinpoint.public.campaigns c ON ctc.campaign_id = c.id
  WHERE ec.table_name = 'treasurer_margins'
  AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment')
  AND date(CAST(ec.logged_at AS timestamp(3))) > date('2022-12-12') 
  AND date(CAST(ec.logged_at AS timestamp(3))) < date('2024-01-30') 
 )


, updated_gm AS (
	SELECT 
	t.campaign_id
	, t.updated_date
	, COALESCE(ctrl_old_vungle_gross_margin, bg.holdout_v_gross_margin) AS ctrl_old_vungle_gross_margin
	, COALESCE(ctrl_old_non_vungle_gross_margin, bg.holdout_nv_gross_margin) AS ctrl_old_non_vungle_gross_margin
	, COALESCE(test_old_vungle_gross_margin, bg.holdout_v_gross_margin) AS test_old_vungle_gross_margin
	, COALESCE(test_old_non_vungle_gross_margin, bg.holdout_nv_gross_margin) AS test_old_non_vungle_gross_margin
	, ctrl_new_vungle_gross_margin
	, ctrl_new_non_vungle_gross_margin
	, test_new_vungle_gross_margin
	, test_new_non_vungle_gross_margin
	, bg.holdout_v_gross_margin
	, bg.holdout_nv_gross_margin
	, t.daily_revenue_limit
  , bg.Acc_GR
  , bg.Acc_spend
  , bg.Acc_GR_on_V
  , bg.Acc_spend_on_V
  , bg.previous_day_Acc_GR
  , bg.previous_day_Acc_spend
  , bg.previous_day_Acc_GR_on_V
  , bg.previous_day_Acc_spend_on_V
 FROM ctrl_gm c
 JOIN test_gm t
 	ON c.campaign_id = t.campaign_id AND c.updated_date = t.updated_date
 LEFT JOIN base_gm bg ON t.campaign_id = bg.campaign_id 
  AND t.updated_date = date(CAST(bg.dt AS timestamp(3))) 
)

SELECT*
FROM updated_gm
