WITH cte AS (
SELECT mak.uid
     , UPPER(mak.country) AS country
     , DATE(FROM_UNIXTIME(mak.ts / 1000)) AS "date"
     , CASE
        WHEN mak.frontend_ab_group BETWEEN 1 AND 5 THEN 'ON'
        WHEN mak.frontend_ab_group BETWEEN 6 AND 100 THEN 'OFF'
        ELSE NULL
        END AS feature_on_off
FROM mistplayetl.mistplay_android_kinesis mak
    WHERE mak.event_type IN ('SIGNUP_SCREEN_KAKAO_CLICK','SIGNUP_SCREEN_GOOGLE_CLICK','SIGNUP_SCREEN_FACEBOOK_CLICK','SIGNUP_SCREEN_EMAIL_CLICK')
    AND mak.PARTITION_0 = '2023'
    AND upper(mak.country) in ('DE','KR')
    AND DATE(FROM_UNIXTIME(mak.TS/1000)) BETWEEN DATE '2023-07-31' AND DATE '2023-08-08'
), installs AS (
    SELECT uid

         , SUM(rev / 100) AS rev
         , COUNT(DISTINCT pid) AS nb_installs
    FROM mistplayetl.installs
    WHERE DATE(createdat) BETWEEN DATE '2023-07-31' AND DATE '2023-08-08' AND state = 2
    GROUP BY 1
), iap AS (
    SELECT uid

         , SUM(amount) AS spend
         , COUNT(pid) AS nb_spends
         , COUNT(DISTINCT uid) AS nb_spender
    FROM mistplayetl.inapppurchases
    WHERE DATE(date) BETWEEN DATE '2023-07-31' AND DATE '2023-08-08'
    GROUP BY 1
), retention_raw AS (
    SELECT uid
         , activity_date
         , ROW_NUMBER() OVER(PARTITION BY uid ORDER BY activity_date DESC) AS ranker
    FROM mistplayetl.user_cohort_activity
), retention AS (
    SELECT * FROM retention_raw
    WHERE ranker = 1
)
SELECT cte.uid
     , cte.country
     , cte."date"
     , cte.feature_on_off
     , COALESCE(i.rev, 0) AS rev
     , COALESCE(i.nb_installs ,0) AS num_installs
     , COALESCE(iap.spend) AS spend
    --  , COUNT(DISTINCT cte.uid) AS num_unique_users
    --  , AVG(COALESCE(i.rev, 0)) AS ARPU
    --  , AVG(COALESCE(i.nb_installs, 0)) AS ANIPU
    --  , AVG(COALESCE(iap.spend, 0)) AS ASPU
    --  , iap.nb_spends
    --  , iap.nb_spender
    , MAX(CASE WHEN cte."date" + INTERVAL '1' day <= rr.activity_date THEN 1 ELSE 0 END) AS is_d1_ret
    , MAX(CASE WHEN cte."date" + INTERVAL '7' day <= rr.activity_date THEN 1 ELSE 0 END) AS is_d7_ret
FROM cte
LEFT JOIN installs i ON i.uid = cte.uid
LEFT JOIN iap ON iap.uid = cte.uid
LEFT JOIN retention rr ON rr.uid = cte.uid
GROUP BY 1,2,3,4,5,6,7

-- features ON and signup via other options
WITH cte AS (
SELECT mak.uid
     , UPPER(mak.country) AS country
     , DATE(FROM_UNIXTIME(mak.ts / 1000)) AS "date"
     , event_type
FROM mistplayetl.mistplay_android_kinesis mak
    WHERE mak.event_type IN ('SIGNUP_SCREEN_KAKAO_CLICK','SIGNUP_SCREEN_GOOGLE_CLICK','SIGNUP_SCREEN_FACEBOOK_CLICK','SIGNUP_SCREEN_EMAIL_CLICK')
    AND mak.PARTITION_0 = '2023'
    AND upper(mak.country) in ('DE','KR')
    AND DATE(FROM_UNIXTIME(mak.TS/1000)) BETWEEN DATE '2023-07-31' AND DATE '2023-08-08'
    AND mak.frontend_ab_group BETWEEN 1 AND 5
), installs AS (
    SELECT uid

         , SUM(rev / 100) AS rev
         , COUNT(DISTINCT pid) AS nb_installs
    FROM mistplayetl.installs
    WHERE DATE(createdat) BETWEEN DATE '2023-07-31' AND DATE '2023-08-08' AND state = 2
    GROUP BY 1
), iap AS (
    SELECT uid

         , SUM(amount) AS spend
         , COUNT(pid) AS nb_spends
         , COUNT(DISTINCT uid) AS nb_spender
    FROM mistplayetl.inapppurchases
    WHERE DATE(date) BETWEEN DATE '2023-07-31' AND DATE '2023-08-08'
    GROUP BY 1
), retention_raw AS (
    SELECT uid
         , activity_date
         , ROW_NUMBER() OVER(PARTITION BY uid ORDER BY activity_date DESC) AS ranker
    FROM mistplayetl.user_cohort_activity
), retention AS (
    SELECT * FROM retention_raw
    WHERE ranker = 1
)
SELECT cte.uid
     , cte.country
     , cte."date"
     , cte.event_type
     , COALESCE(i.rev, 0) AS rev
     , COALESCE(i.nb_installs ,0) AS num_installs
     , COALESCE(iap.spend, 0) AS spend
    --  , COUNT(DISTINCT cte.uid) AS num_unique_users
    --  , AVG(COALESCE(i.rev, 0)) AS ARPU
    --  , AVG(COALESCE(i.nb_installs, 0)) AS ANIPU
    --  , AVG(COALESCE(iap.spend, 0)) AS ASPU
    --  , iap.nb_spends
    --  , iap.nb_spender
    , MAX(CASE WHEN cte."date" + INTERVAL '1' day <= rr.activity_date THEN 1 ELSE 0 END) AS is_d1_ret
    , MAX(CASE WHEN cte."date" + INTERVAL '7' day <= rr.activity_date THEN 1 ELSE 0 END) AS is_d7_ret
FROM cte
LEFT JOIN installs i ON i.uid = cte.uid
LEFT JOIN iap ON iap.uid = cte.uid
LEFT JOIN retention rr ON rr.uid = cte.uid
GROUP BY 1,2,3,4,5,6,7