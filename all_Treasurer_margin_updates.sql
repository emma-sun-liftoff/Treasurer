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
