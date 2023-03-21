WITH ctrl_gm AS (
 SELECT 
  tm.campaign_id
  , date(CAST(ec.logged_at AS timestamp(3))) AS updated_date
  , COALESCE(LAG(tm.vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at),0) AS ctrl_old_vungle_gross_margin
  , COALESCE(LAG(tm.non_vungle_gross_margin,1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY  ec.logged_at),0) AS ctrl_old_non_vungle_gross_margin
  , tm.vungle_gross_margin AS ctrl_new_vungle_gross_margin
  , tm.non_vungle_gross_margin AS ctrl_new_non_vungle_gross_margin
  , ctc.target AS current_target
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

SELECT 
c.campaign_id AS campaign_id
, t.updated_date AS updated_date
, c.current_target AS current_target
, COALESCE(ctrl_old_vungle_gross_margin, test_old_vungle_gross_margin) AS ctrl_old_vungle_gross_margin
, COALESCE(ctrl_old_non_vungle_gross_margin, test_old_non_vungle_gross_margin) AS ctrl_old_non_vungle_gross_margin
, test_old_vungle_gross_margin
, test_old_non_vungle_gross_margin
, ctrl_new_vungle_gross_margin
, ctrl_new_non_vungle_gross_margin
, test_new_vungle_gross_margin
, test_new_non_vungle_gross_margin
 FROM ctrl_gm c
 FULL JOIN test_gm t
    ON c.campaign_id = t.campaign_id AND c.updated_date = t.updated_date
WHERE (test_old_vungle_gross_margin <> test_new_vungle_gross_margin OR test_old_non_vungle_gross_margin <> test_new_non_vungle_gross_margin OR ctrl_old_vungle_gross_margin <> ctrl_new_vungle_gross_margin OR ctrl_old_non_vungle_gross_margin <> ctrl_new_non_vungle_gross_margin)
	AND current_target IN ('NR', 'RPI')
