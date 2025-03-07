
--Task_1------------------------------------------
-- Підготовка даних для побудови звітів у BI системах

SELECT 
  timestamp_micros(ge.event_timestamp) as event_timestamp
  ,ge.user_pseudo_id
  ,(select value.int_value FROM ge.event_params WHERE key = 'ga_session_id') as session_id
  ,ge.event_name
  ,ge.geo.country
  ,ge.device.category
  ,ge.traffic_source.source
  ,ge.traffic_source.medium
  ,ge.traffic_source.name as campaign
FROM  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` ge
WHERE ge.event_name IN ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info', 'purchase')
AND _TABLE_SUFFIX BETWEEN '20210101' AND '20211231'
ORDER BY 1;

----Task_2-----------------------------------------
--Розрахунок конверсій в розрізі дат та каналів трафіку

WITH
  cte_events AS (
    SELECT
      TIMESTAMP_MICROS(ge.event_timestamp) AS event_timestamp,
      ge.traffic_source.source AS source,
      ge.traffic_source.medium AS medium,
      ge.traffic_source.name AS campaign,
      ge.event_name,
      ge.user_pseudo_id || CAST(
        (
          SELECT
            value.int_value
          FROM
            ge.event_params
          WHERE
            key = 'ga_session_id'
        ) AS string
      ) AS user_session_id
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` ge
    WHERE
      ge.event_name IN ('add_to_cart', 'begin_checkout', 'purchase')
  ),
  events_count_cte AS (
    SELECT
      DATE(event_timestamp) AS event_date,
      source,
      medium,
      campaign,
      COUNT(DISTINCT user_session_id) AS user_sessions_count,
      COUNT(
        DISTINCT 
          CASE event_name
            WHEN 'add_to_cart' THEN user_session_id
          END
      ) AS added_to_cart_count,
      COUNT(
        DISTINCT 
          CASE event_name
            WHEN 'begin_checkout' THEN user_session_id
          END
      ) AS began_checkout_count,
      COUNT(
        DISTINCT 
          CASE event_name
            WHEN 'purchase' THEN user_session_id
          END
      ) AS purchase_count
    FROM
      cte_events
    GROUP BY
      1,
      2,
      3,
      4
  )
SELECT
  event_date,
  source,
  medium,
  campaign,
  user_sessions_count,
  ROUND(
    (added_to_cart_count / user_sessions_count) * 100,
    1
  ) AS visit_to_cart_percent,
  ROUND(
    (began_checkout_count / user_sessions_count) * 100,
    1
  ) AS visit_to_checkout_percent,
  ROUND((purchase_count / user_sessions_count) * 100, 1) AS visit_to_purchase_percent
FROM
  events_count_cte
ORDER BY
  1 DESC;
  
  
  ---Task_3---------------------------------------------------
  -- Порівняння конверсії між різними посадковими сторінками

WITH session_data AS (
   -- Отримуємо дані про початок сесій за 2020 рік
  SELECT
    ge.user_pseudo_id || 
        CAST((SELECT value.int_value FROM ge.event_params WHERE key = 'ga_session_id') AS string) AS user_session_id
    ,REGEXP_EXTRACT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'(?:\w+\:\/\/)?[^\/]+\/([^\?#]*)'
      ) AS page_path
  FROM  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` ge
  WHERE event_name = 'session_start' 
    AND _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
),
purchase_data AS (
  -- Отримуємо дані про покупки за 2020 рік
  SELECT
    ge.user_pseudo_id || 
        CAST((SELECT value.int_value FROM ge.event_params WHERE key = 'ga_session_id') AS string) AS user_session_id
    ,REGEXP_EXTRACT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), r'(?:\w+\:\/\/)?[^\/]+\/([^\?#]*)'
    ) AS page_path
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` ge
  WHERE event_name = 'purchase' 
    AND _TABLE_SUFFIX BETWEEN '20200101' AND '20201231'
),
combined_data AS (
  -- Об'єднуємо події початку сесії та покупки
  SELECT
    s.page_path AS page_path
    ,s.user_session_id  AS user_session_id
    ,p.user_session_id AS made_purchase
  FROM session_data s
  LEFT JOIN purchase_data p
    ON s.user_session_id = p.user_session_id 
)
SELECT
  page_path
  ,COUNT(DISTINCT user_session_id) AS user_session_count
  ,COUNT(DISTINCT made_purchase) AS purchase_count
  ,ROUND((COUNT(DISTINCT made_purchase) / COUNT(DISTINCT user_session_id)) * 100, 1) AS conversion_percent
FROM combined_data
GROUP BY 1
ORDER BY 4 DESC;


---Task_4---------------------------------------------
-- Перевірка кореляції між залученістю користувачів та здійсненням покупок

WITH session_start_data AS (
   -- Отримуємо дані про початок сесій
  SELECT
      user_pseudo_id || CAST((
        SELECT
          value.int_value
        FROM
          ge.event_params
        WHERE
          key = 'ga_session_id'
      ) AS string) AS user_session_id
      ,MAX(
         CASE
          WHEN (
            SELECT
              value.string_value
            FROM
              ge.event_params
            WHERE
              key = 'session_engaged'
          ) = '1' 
          THEN 1
          ELSE 0
        END
      ) AS is_session_engaged,
      SUM(
        (
          SELECT
            value.int_value
          FROM
            ge.event_params
          WHERE
            key = 'engagement_time_msec'
        )
      ) AS sum_engagement_time_msec
  FROM  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` ge
  WHERE event_name = 'session_start' 
  GROUP BY 1
),
purchase_data AS (
  -- Отримуємо дані про покупки
  SELECT
      event_name
      ,user_pseudo_id || CAST((
        SELECT
          value.int_value
        FROM
          ge.event_params
        WHERE
          key = 'ga_session_id'
      ) AS string) AS user_session_id
      ,MAX(
         CASE
          WHEN (
            SELECT
              value.string_value
            FROM
              ge.event_params
            WHERE
              key = 'session_engaged'
          ) = '1' 
          THEN 1
          ELSE 0
        END
      ) AS is_session_engaged
      ,SUM(
        (
          SELECT
            value.int_value
          FROM
            ge.event_params
          WHERE
            key = 'engagement_time_msec'
        )
      ) AS sum_engagement_time_msec
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` ge
  WHERE event_name = 'purchase'
  GROUP BY 1, 2
),
combined_data AS (
  -- Об'єднуємо події початку сесії та покупки
  SELECT
    CASE 
      WHEN s.is_session_engaged = 1 OR p.is_session_engaged = 1 THEN 1
      ELSE 0
    END AS is_session_engaged
    ,COALESCE(s.sum_engagement_time_msec, 0) + COALESCE(p.sum_engagement_time_msec, 0) AS sum_engagement_time_msec
    ,CASE 
      WHEN p.event_name = 'purchase'  THEN 1 
      ELSE 0
    END AS made_purchase
  FROM  session_start_data s
  LEFT JOIN  purchase_data p
    ON s.user_session_id = p.user_session_id 
)
SELECT
  CORR(is_session_engaged, made_purchase) as correlation_engaged_purchase
  ,CORR(sum_engagement_time_msec, made_purchase) as correlation_time_purchase
FROM
  combined_data;


