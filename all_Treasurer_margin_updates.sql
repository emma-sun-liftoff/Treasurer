-- we can leverage ml_adops.public.treasurer_daily_analysis table to pull all Treasurer margin updates
SELECT
campaign_id 
, date_trunc('hour',updated_at) AS updated_at
, target 
, threshold 
, margin_exploration_algorithm
, margin_reason 
, CASE WHEN old_control_gross_margin_vungle <> new_control_gross_margin_vungle 
		    OR old_control_gross_margin_non_vungle <> new_control_gross_margin_non_vungle 
	   THEN 'Yes' ELSE  'No'END AS ctrl_update
, CASE WHEN old_experiment_gross_margin_vungle <> new_experiment_gross_margin_vungle 
		    OR old_experiment_gross_margin_non_vungle <> new_experiment_gross_margin_non_vungle 
	   THEN 'Yes' ELSE 'No'END AS test_update
, ROUND((old_control_gross_margin_vungle)/(old_control_gross_margin_vungle + 1),3) AS old_ctrl_margin_vungle
, ROUND((old_control_gross_margin_non_vungle)/(old_control_gross_margin_non_vungle + 1),3) AS old_ctrl_margin_non_vungle
, ROUND((new_control_gross_margin_vungle)/(new_control_gross_margin_vungle + 1),3) AS new_ctrl_margin_vungle
, ROUND((new_control_gross_margin_non_vungle)/(new_control_gross_margin_non_vungle + 1),3) AS new_ctrl_margin_non_vungle
, ROUND((old_experiment_gross_margin_vungle)/(old_experiment_gross_margin_vungle + 1),3) AS old_test_margin_vungle
, ROUND((old_experiment_gross_margin_non_vungle)/(old_experiment_gross_margin_non_vungle + 1),3) AS old_test_margin_non_vungle
, ROUND((new_experiment_gross_margin_vungle)/(new_experiment_gross_margin_vungle + 1),3) AS new_test_margin_vungle
, ROUND((new_experiment_gross_margin_non_vungle)/(new_experiment_gross_margin_non_vungle + 1),3) AS new_test_margin_non_vungle
, measured_control_nrm AS ctrl_nrm
, measured_experiment_nrm AS test_nrm
, predicted_best_nrm 
, predicted_best_rpi 
, measured_control_rpi 
, measured_experiment_rpi 
, CASE WHEN measured_control_rpi IS NOT NULL THEN 'Yes' ELSE 'No' END AS ctrl_rpi_stat_sig
, CASE WHEN measured_experiment_rpi IS NOT NULL THEN 'Yes' ELSE 'No' END AS test_rpi_stat_sig
, CAST(control_gross_revenue AS DOUBLE)/1000000 AS cumulative_ctrl_gross_revenue 
, control_installs AS cumulative_ctrl_installs 
, CAST(experiment_gross_revenue AS DOUBLE)/1000000 AS cumulative_test_gross_revenue  
, experiment_installs AS cumulative_test_installs 
FROM ml_adops.public.treasurer_daily_analysis
WHERE control_gross_revenue IS NOT NULL
    AND experiment_gross_revenue IS NOT NULL



-- we can use elephant_changes for double check.
SELECT 
    tm.campaign_id
    , logged_at
    --, COALESCE(LEAD(ec.logged_at, 2) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.old_values, '$.margin_type') ORDER BY ec.logged_at), CURRENT_TIMESTAMP) AS next_logged_at
    , ec."source"
    , json_extract_scalar(ec.new_values, '$.margin_type') AS margin_type
    , tm.vungle_gross_margin as new_vungle_gross_margin
    , tm.non_vungle_gross_margin as new_non_vungle_gross_margin
    --, COALESCE (LAG(tm.vungle_gross_margin, 1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at), 999999) AS old_vungle_gross_margin
    --, COALESCE (LAG(tm.non_vungle_gross_margin, 1) OVER (PARTITION BY tm.campaign_id, json_extract_scalar(ec.new_values, '$.margin_type') ORDER BY ec.logged_at), 999999) AS old_non_vungle_gross_margin
    FROM pinpoint.public.treasurer_margins tm
    FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
    WHERE ec.table_name = 'treasurer_margins'
    AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
    -- AND tm.campaign_id = 4293
    ORDER BY 2 DESC 
