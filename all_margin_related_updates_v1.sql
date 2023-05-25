 

WITH all_ AS (    
SELECT 
ctc.campaign_id
, CASE WHEN ctc.target = 'disabled' THEN ctc.target ELSE tda.target END AS tda_target
, ctc.target_reasons
, 'treasurer' AS update_source
, tda.margin_reason 
, tda.margin_exploration_algorithm 
, min(tda.updated_at) AS min_update_at
FROM pinpoint.public.campaign_treasurer_configs ctc
LEFT JOIN ml_adops.public.treasurer_daily_analysis tda 
	ON ctc.campaign_id = tda.campaign_id 
	--AND ctc.target = tda.target 
WHERE ctc.campaign_id IN (
  SELECT 
  DISTINCT ctc.campaign_id
  FROM pinpoint.public.campaign_treasurer_configs ctc
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  ctc.id = ec.row_id
  WHERE ec.table_name = 'campaign_treasurer_configs'
  	AND ec.logged_at > from_iso8601_timestamp('2023-04-05')
  	--AND ctc.campaign_id IN (4293, 12624, 29288,6318)  
 )	
GROUP BY 1,2,3,4,5,6

UNION ALL 

SELECT 
campaign_id
, NULL AS tda_target
, NULL AS target_reasons
, "source"
, NULL AS margin_reason
, NULL AS margin_exploration_algorithm
, min(logged_at) AS min_updated_at
FROM (SELECT 
    tm.campaign_id
    , logged_at
    , ec."source"
    , json_extract_scalar(ec.new_values, '$.margin_type') AS margin_type
    , tm.vungle_gross_margin as new_vungle_gross_margin
    , tm.non_vungle_gross_margin as new_non_vungle_gross_margin
    FROM pinpoint.public.treasurer_margins tm
    FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  tm.id = ec.row_id
    WHERE ec.table_name = 'treasurer_margins'
    AND json_extract_scalar(ec.new_values, '$.margin_type') IN ('experiment','control')
    --AND tm.campaign_id IN (4293, 12624, 29288,6318)
    AND ec."source" <> 'treasurer'
    ORDER BY 2 DESC     
) AS TT
WHERE logged_at > from_iso8601_timestamp('2023-04-05')
GROUP BY 1,2,3,4,5,6
)

SELECT all_.campaign_id
, min_update_at
, num_target
, tda_target
, target_reasons
, update_source
, margin_reason
, margin_exploration_algorithm
FROM all_
LEFT JOIN (SELECT campaign_id, count(DISTINCT tda_target) AS num_target FROM all_ GROUP BY 1) AS num_target ON all_.campaign_id = num_target.campaign_id
WHERE all_.campaign_id NOT IN (SELECT
	DISTINCT row_id AS campaign_id
	FROM pinpoint.public.elephant_changes
	WHERE table_name = 'campaigns'
	AND operation = 'update'
	AND json_extract_scalar(new_values, '$.state') = 'paused'
	AND json_extract_scalar(new_values, '$.state_last_changed_at') > '2023-01-01'
	)
ORDER BY 1,2
