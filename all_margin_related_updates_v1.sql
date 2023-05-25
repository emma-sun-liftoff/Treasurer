 --- below it is the base query
 --  we can find all NR/RPI campaigns’ update dates when we 1) switch targets 2) apply manual adjustment 3)apply any system-wide margin update 4)when we change margin exploration algorithm. and we can find all disabled campaigns
 --  all campaigns are enabled campaigns after 2023/01/01. Any campaigns that are paused after 01/01 will be excluded.
 --  how to use each columns as filter:
 --  num_target is a good way to help you find campaigns where targets have been changed. Please reach out to me if you find num_target = 0.
 --  updated_source is a filter to find manual update (=skipper), treasurer update (=treasurer), and eng-driven adhoc update (=adhoc).
 -- 



WITH para AS (
SELECT '2023-04-05' AS start_dt)

, all_ AS (    
SELECT 
ctc.campaign_id
, tda.target AS tda_target
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
  CROSS JOIN para
  WHERE ec.table_name = 'campaign_treasurer_configs'
  	AND ec.logged_at > from_iso8601_timestamp(start_dt)
  	--AND ctc.campaign_id IN (4293, 12624, 29288,6318)  
 )	
GROUP BY 1,2,3,4,5,6

UNION ALL 

SELECT 
campaign_id
, NULL AS tda_target
, NULL AS target_reasons
, "source" AS update_source
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
CROSS JOIN para 
WHERE logged_at > from_iso8601_timestamp(start_dt)
GROUP BY 1,2,3,4,5,6

UNION ALL 


  SELECT 
  ctc.campaign_id
    , CAST(json_extract_scalar(ec.new_values, '$.target') AS varchar) AS tda_target
   ,  ctc.target_reasons AS target_reasons
    , 'target_switch' AS update_source
    , NULL AS margin_reason
    , NULL AS margin_exploration_algorithm 
   , ec.logged_at AS min_updated_at
  FROM pinpoint.public.campaign_treasurer_configs ctc
  FULL OUTER JOIN pinpoint.public.elephant_changes ec ON  ctc.id = ec.row_id
  WHERE ec.table_name = 'campaign_treasurer_configs'
  --AND CAST(json_extract(ec.new_values, '$.target') AS varchar)= 'disabled' 
  AND json_extract(ec.new_values, '$.target') IS NOT NULL 
  --AND ec.logged_at > from_iso8601_timestamp('2023-04-05')
  AND ctc.campaign_id NOT IN (SELECT
	DISTINCT row_id AS campaign_id
	FROM pinpoint.public.elephant_changes
	WHERE table_name = 'campaigns'
	AND operation = 'update'
	AND json_extract_scalar(new_values, '$.state') = 'paused'
	AND json_extract_scalar(new_values, '$.state_last_changed_at') > '2023-01-01'
	)
	
)
, all1_ AS (
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
)
SELECT*
FROM all1_
ORDER BY 1,2

-- query examples 
-- all disabled campaigns during a time frame (there are two cases: 1) disabled all the way 2) disabled after NR/RPI. If the former, num_target = 1)
-- campaigns 
-- copy and paste the first 97 line
SELECT*
FROM all1_
WHERE tda_target = 'disabled'
ORDER BY 1,2
