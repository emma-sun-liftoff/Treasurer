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
	WHERE dt BETWEEN '2022-12-12' and '2024-01-08'
	and bid__margin_data__base_gross_margin is not null
	GROUP BY 1,2)

, base_gm AS (
	SELECT 
	campaign_id
	, dt
	, Acc_GR
	, Acc_spend
	, Acc_GR_on_V
	, Acc_spend_on_V
	, lag(Acc_GR,1) over(order by dt) AS previous_day_Acc_GR
	, lag(Acc_spend,1) over(order by dt) AS previous_day_Acc_spend
	, lag(Acc_GR_on_V,1) over(order by dt) AS previous_day_Acc_GR_on_V
	, lag(Acc_spend_on_V,1) over(order by dt) AS previous_day_Acc_spend_on_V
	, holdout_v_gross_margin
	, holdout_nv_gross_margin
FROM info)

, updated_gm AS ( 
  SELECT 
  tm.campaign_id
  , date(CAST(ec.logged_at AS timestamp(3))) AS updated_date
  , json_extract_scalar(ec.new_values, '$.margin_type') AS margin_type
  , COALESCE(LAG(tm.vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at),bg.holdout_v_gross_margin) AS old_vungle_gross_margin
  , COALESCE(LAG(tm.non_vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY  ec.logged_at),bg.holdout_nv_gross_margin) AS old_non_vungle_gross_margin
  , tm.vungle_gross_margin AS new_vungle_gross_margin
  , tm.non_vungle_gross_margin AS new_non_vungle_gross_margin
  , bg.holdout_v_gross_margin
  , bg.holdout_nv_gross_margin
  , c.daily_revenue_limit
  , bg.Acc_GR
  , bg.Acc_spend
  , bg.Acc_GR_on_V
  , bg.Acc_spend_on_V
  , bg.previous_day_Acc_GR
  , bg.previous_day_Acc_spend
  , bg.previous_day_Acc_GR_on_V
  , bg.previous_day_Acc_spend_on_V
  FROM pinpoint.public.campaign_treasurer_configs ctc 
  FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
  LEFT JOIN pinpoint.public.campaigns c ON ctc.campaign_id = c.id
  LEFT JOIN base_gm bg ON tm.campaign_id = bg.campaign_id 
  AND date(CAST(ec.logged_at AS timestamp(3))) = date(CAST(bg.dt AS timestamp(3))) 
  WHERE ec.table_name = 'treasurer_margins'
  AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
  AND date(CAST(ec.logged_at AS timestamp(3))) > date('2022-12-12')
  )

SELECT*
, CASE WHEN old_vungle_gross_margin <> new_vungle_gross_margin OR old_non_vungle_gross_margin <> new_non_vungle_gross_margin THEN 'Y' ELSE 'N' END AS is_valid_update
FROM updated_gm
