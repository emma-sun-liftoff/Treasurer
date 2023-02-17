WITH funnel AS (
SELECT 
  campaign_id
  --, date_trunc('day', from_iso8601_timestamp(dt)) as dt
  , CASE WHEN try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") = 'holdout' THEN 'holdout'
         WHEN try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") IN ('control', 'experiment') THEN 'non-holdout' 
         END AS test_group_name
  , SUM(spend_micros) as treasurer_spend_micros
  , SUM(revenue_micros) as treasurer_revenue_micros
  , SUM(CASE WHEN exchange = 'VUNGLE' THEN spend_micros END) AS spend_micros_v
  , SUM(revenue_micros) - SUM(spend_micros) AS A_NR_micros
  , SUM(CASE WHEN exchange = 'VUNGLE' AND pptype = 1 THEN spend_micros * 0.3526 * 0.9
  			 WHEN exchange = 'VUNGLE' AND pptype = 2 THEN spend_micros * 0.4437 * 0.9
  			 WHEN exchange = 'VUNGLE' AND pptype = 3 THEN spend_micros * 0.3016 * 0.9
  		END) AS AoVX_NR_micros
  , SUM(installs) AS installs
  , 0 as customer_revenue_micros_d7
  , 0 as events_d7
FROM product_analytics.supply_analytics_hourly_v2
WHERE try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") IS NOT NULL
AND dt >'2023-02-06T00' 
AND impression_at BETWEEN '2023-02-06T00' and '2023-02-14T00'
AND campaign_id IN (5908,
12817,
19805,
4293,
15225,
23471,
12624,
27670,
3582,
27253,
21384,
27396,
24215,
20040,
13013,
10714)
GROUP BY 1,2

UNION ALL 

SELECT 
  campaign_id
  --, date_trunc('day', from_iso8601_timestamp(install_at)) as dt
  , CASE WHEN try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") = 'holdout' THEN 'holdout'
         WHEN try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") IN ('control', 'experiment') THEN 'non-holdout' 
         END AS test_group_name
  , 0 as treasurer_spend_micros
  , 0 as treasurer_revenue_micros
  , 0 AS spend_micros_v
  , 0 AS A_NR_micros
  , 0 AS AoVX_NR_micros
  , 0 as installs
  , SUM(
      CASE
        WHEN from_iso8601_timestamp(at) - from_iso8601_timestamp(install_at) < interval '7' day
          THEN customer_revenue_micros 
      ELSE 0
      END) AS  customer_revenue_micros_d7
  , SUM(
      CASE
        WHEN from_iso8601_timestamp(at) - from_iso8601_timestamp(install_at) < interval '7' day
          THEN target_events 
      ELSE 0
      END) AS  events_d7
FROM product_analytics.supply_analytics_hourly_v2
WHERE try(filter(ab_test_assignments, t -> t.id = 916)[1]."group"."name") IS NOT NULL
AND dt >'2023-02-06T00' 
AND impression_at BETWEEN '2023-02-06T00' and '2023-02-14T00'
AND install_at BETWEEN '2023-02-06T00' and '2023-02-14T00'
AND campaign_id IN (5908,
12817,
19805,
4293,
15225,
23471,
12624,
27670,
3582,
27253,
21384,
27396,
24215,
20040,
13013,
10714)
GROUP BY 1,2
)


, metrics AS (
    SELECT
    campaign_id
    , test_group_name
    , sum(installs) AS installs
    , sum(events_d7) AS events_d7
    , sum(CAST(customer_revenue_micros_d7 AS double)/1000000) AS customer_revenue_d7
    , sum(CAST(treasurer_spend_micros AS double)/1000000) AS Acc_spend
    , sum(CAST(treasurer_revenue_micros AS double)/1000000) AS Acc_revenue
    , SUM(CAST(spend_micros_v AS DOUBLE)/1000000) AS spend_v
    , SUM(CAST(A_NR_micros AS DOUBLE)/1000000) AS A_NR
    , SUM(CAST(AoVX_NR_micros AS DOUBLE)/1000000) + SUM(CAST(A_NR_micros AS DOUBLE)/1000000) AS LV_NR
    FROM funnel 
    GROUP BY 1,2
)

SELECT 
    campaign_id
    , test_group_name 
    , sum(Acc_revenue)/sum(installs) AS RPI
    , CASE WHEN campaign_id IN (19805, 27253, 13624, 27396) THEN sum(Acc_revenue)/sum(events_d7) ELSE null END AS RPA7d
    , CASE WHEN campaign_id IN (19805, 27253, 13624, 27396) THEN null ELSE sum(customer_revenue_d7)/sum(Acc_revenue) END AS ROAS7d
    , sum(LV_NR)/sum(Acc_revenue) AS LV_NRM
    , sum(A_NR)/sum(Acc_revenue) AS Acc_NRM
FROM metrics
GROUP BY 1,2
