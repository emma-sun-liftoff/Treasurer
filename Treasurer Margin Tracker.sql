WITH base_gm AS  (
SELECT bid__campaign_id as campaign_id
, date_trunc('day', from_iso8601_timestamp(dt)) as dt
, max(CASE 
      WHEN bid__bid_request__exchange = 'VUNGLE' THEN bid__margin_data__base_gross_margin
    end) AS holdout_v_gross_margin 
, max(CASE 
    WHEN bid__bid_request__exchange != 'VUNGLE' THEN bid__margin_data__base_gross_margin
    end) AS holdout_nv_gross_margin 
FROM rtb.impressions_with_bids
WHERE dt BETWEEN '2022-12-12' and '2024-01-06'
and bid__margin_data__base_gross_margin is not NULL
group by 1,2
)

, updated_gm AS ( 
      SELECT 
      tm.campaign_id,
      ec.logged_at,
      json_extract_scalar(ec.new_values, '$.margin_type') AS margin_type,
      coalesce (LAG(tm.vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at),bg.holdout_v_gross_margin) AS old_vungle_gross_margin,
      coalesce (LAG(tm.non_vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY  ec.logged_at),bg.holdout_nv_gross_margin) AS old_non_vungle_gross_margin,
      tm.vungle_gross_margin as new_vungle_gross_margin,
      tm.non_vungle_gross_margin as new_non_vungle_gross_margin,
      bg.holdout_v_gross_margin,
      bg.holdout_nv_gross_margin,
      c.daily_revenue_limit
      FROM pinpoint.public.campaign_treasurer_configs ctc 
      FULL OUTER JOIN pinpoint.public.treasurer_margins tm ON tm.campaign_id = ctc.campaign_id
      FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
      LEFT JOIN pinpoint.public.campaigns c ON ctc.campaign_id = c.id
      LEFT JOIN base_gm bg ON tm.campaign_id = bg.campaign_id 
      AND date(CAST(ec.logged_at AS timestamp(3))) = date(CAST(bg.dt AS timestamp(3))) 
      WHERE ec.table_name = 'treasurer_margins'
      AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
      AND date(CAST(ec.logged_at AS timestamp(3))) > date('2022-12-12'))

SELECT*
FROM updated_gm
WHERE old_vungle_gross_margin <> new_vungle_gross_margin OR old_non_vungle_gross_margin <> new_non_vungle_gross_margin


