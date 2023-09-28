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
GROUP BY 1,2,3,4,5,6,7WITH last_signup_raw AS (
    SELECT uid
         , country
         , event_type
         , ROW_NUMBER() OVER(PARTITION BY UID ORDER BY ts DESC) AS RN
    FROM mistplayetl.mistplay_android_kinesis
    WHERE event_type IN ('SIGNUP_SCREEN_KAKAO_CLICK','SIGNUP_SCREEN_GOOGLE_CLICK','SIGNUP_SCREEN_FACEBOOK_CLICK','SIGNUP_SCREEN_EMAIL_CLICK')
    AND PARTITION_0 = '2023'
    AND upper(country) in ('DE','KR')
    AND "frontend_ab_group" BETWEEN 1 AND 5
    AND DATE(FROM_UNIXTIME(TS/1000)) BETWEEN DATE '2023-07-31' AND DATE '2023-08-02'
), min_rn_per_uid AS (
    SELECT uid 
         , MIN(RN) AS min_rn
    FROM last_signup_raw
    GROUP BY 1
), last_signup AS (
    SELECT l.uid
         , l.country
         , MAX(CASE WHEN event_type in ('SIGNUP_SCREEN_EMAIL_CLICK') THEN 1 ELSE 0 END) SIGNUP_SCREEN_EMAIL_CLICK
         , MAX(CASE WHEN event_type in ('SIGNUP_SCREEN_GOOGLE_CLICK') THEN 1 ELSE 0 END) SIGNUP_SCREEN_GOOGLE_CLICK
         , MAX(CASE WHEN event_type in ('SIGNUP_SCREEN_KAKAO_CLICK') THEN 1 ELSE 0 END) SIGNUP_SCREEN_KAKAO_CLICK
         , MAX(CASE WHEN event_type in ('SIGNUP_SCREEN_FACEBOOK_CLICK') THEN 1 ELSE 0 END) SIGNUP_SCREEN_FACEBOOK_CLICK
    FROM last_signup_raw l
    JOIN min_rn_per_uid m ON l.uid = m.uid AND l.rn = m.min_rn
    GROUP BY 1, 2
), registration AS (
    SELECT DISTINCT UID
         , UPPER(COUNTRY) COUNTRY
         , MAX(CASE WHEN event_type in ('REGISTRATION_COMPLETE') THEN 1 ELSE 0 END) REGISTRATION_COMPLETE
    FROM mistplayetl.mistplay_android_kinesis
    WHERE event_type in ('SIGNUP_SCREEN_EMAIL_CLICK', 'SIGNUP_SCREEN_GOOGLE_CLICK','SIGNUP_SCREEN_KAKAO_CLICK','SIGNUP_SCREEN_FACEBOOK_CLICK', 'REGISTRATION_COMPLETE')
    AND PARTITION_0 = '2023'
    AND upper(country) in ('DE','KR')
    AND "frontend_ab_group" BETWEEN 1 AND 5
    AND DATE(FROM_UNIXTIME(TS/1000)) BETWEEN DATE '2023-07-31' AND DATE '2023-08-02'
    GROUP BY 1, 2
), cte AS (
SELECT l.* 
     , r.REGISTRATION_COMPLETE
FROM last_signup l
LEFT JOIN registration r ON r.uid = l.uid
)
SELECT DISTINCT country
      , COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_EMAIL_CLICK = 1 AND REGISTRATION_COMPLETE = 1 THEN UID END)*1.0/COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_EMAIL_CLICK = 1 THEN UID END) EMAIL_CVR
      , COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_EMAIL_CLICK = 1 THEN UID END)*1.0/COUNT(DISTINCT UID)*1.0 PCT_USERS_EMAIL
      , COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_GOOGLE_CLICK = 1 AND REGISTRATION_COMPLETE = 1 THEN UID END)*1.0/COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_GOOGLE_CLICK = 1 THEN UID END) GOOGLE_CVR
      , COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_GOOGLE_CLICK = 1 THEN UID END)*1.0/COUNT(DISTINCT UID)*1.0 PCT_USERS_GOOGLE
      , COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_KAKAO_CLICK = 1 AND REGISTRATION_COMPLETE = 1 THEN UID END)*1.0/COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_KAKAO_CLICK = 1 THEN UID END) KAKAO_CVR
      , COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_KAKAO_CLICK = 1 THEN UID END)*1.0/COUNT(DISTINCT UID)*1.0 PCT_KAKAO_EMAIL
      , COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_FACEBOOK_CLICK = 1 AND REGISTRATION_COMPLETE = 1 THEN UID END)*1.0/COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_FACEBOOK_CLICK = 1 THEN UID END) FACEBOOK_CVR
      , COUNT (DISTINCT CASE WHEN SIGNUP_SCREEN_FACEBOOK_CLICK = 1 THEN UID END)*1.0/COUNT(DISTINCT UID)*1.0 PCT_FACEBOOK_EMAIL
FROM cte
GROUP BY 1

-- EMAIL FOLLOW UP ANALYSIS
WITH cte AS (
SELECT mak.uid
     , UPPER(mak.country) AS country
     , DATE(FROM_UNIXTIME(mak.ts / 1000)) AS "date"
     , CASE
        WHEN mak.frontend_ab_group BETWEEN 1 AND 50 THEN 'ON'
        WHEN mak.frontend_ab_group BETWEEN 51 AND 100 THEN 'OFF'
        ELSE NULL
        END AS feature_on_off
FROM mistplayetl.mistplay_android_kinesis mak
    WHERE mak.event_type IN ('SIGNUP_SCREEN_KAKAO_CLICK','SIGNUP_SCREEN_GOOGLE_CLICK','SIGNUP_SCREEN_FACEBOOK_CLICK','SIGNUP_SCREEN_EMAIL_CLICK')
    AND mak.PARTITION_0 = '2023'
    AND upper(mak.country) in ('DE','KR')
    AND DATE(FROM_UNIXTIME(mak.TS/1000)) BETWEEN DATE '2023-09-13' AND DATE '2023-09-20'
), installs AS (
    SELECT uid

         , SUM(rev / 100) AS rev
         , COUNT(DISTINCT pid) AS nb_installs
    FROM mistplayetl.installs
    WHERE DATE(createdat) BETWEEN DATE '2023-09-13' AND DATE '2023-09-20' AND state = 2
    GROUP BY 1
), iap AS (
    SELECT uid

         , SUM(amount) AS spend
         , COUNT(pid) AS nb_spends
         , COUNT(DISTINCT uid) AS nb_spender
    FROM mistplayetl.inapppurchases
    WHERE DATE(date) BETWEEN DATE '2023-09-13' AND DATE '2023-09-20'
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
SELECT --cte.uid
      cte.country
     --, cte."date"
     , cte.feature_on_off
     , COUNT(DISTINCT cte.uid) AS num_unique_users
     , AVG(COALESCE(i.rev, 0)) AS ARPU
     , AVG(COALESCE(i.nb_installs, 0)) AS ANIPU
     , AVG(COALESCE(iap.spend, 0)) AS ASPU
    --  , iap.nb_spends
    --  , iap.nb_spender
    , AVG(CASE WHEN cte."date" + INTERVAL '1' day <= now() THEN 1 ELSE 0 END) AS is_d1_ret
    , AVG(CASE WHEN cte."date" + INTERVAL '7' day <= now() THEN 1 ELSE 0 END) AS is_d7_ret
FROM cte
LEFT JOIN installs i ON i.uid = cte.uid
LEFT JOIN iap ON iap.uid = cte.uid
LEFT JOIN retention rr ON rr.uid = cte.uid
GROUP BY 1,2;

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
    AND DATE(FROM_UNIXTIME(mak.TS/1000)) BETWEEN DATE '2023-09-13' AND DATE '2023-09-20'
    AND mak.frontend_ab_group BETWEEN 1 AND 50
), installs AS (
    SELECT uid

         , SUM(rev / 100) AS rev
         , COUNT(DISTINCT pid) AS nb_installs
    FROM mistplayetl.installs
    WHERE DATE(createdat) BETWEEN DATE '2023-09-13' AND DATE '2023-09-20' AND state = 2
    GROUP BY 1
), iap AS (
    SELECT uid

         , SUM(amount) AS spend
         , COUNT(pid) AS nb_spends
         , COUNT(DISTINCT uid) AS nb_spender
    FROM mistplayetl.inapppurchases
    WHERE DATE(date) BETWEEN DATE '2023-09-13' AND DATE '2023-09-20'
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
SELECT --cte.uid
      cte.country
     --, cte."date"
     , cte.event_type
     , COUNT(DISTINCT cte.uid) AS num_unique_users
     , AVG(COALESCE(i.rev, 0)) AS ARPU
     , AVG(COALESCE(i.nb_installs, 0)) AS ANIPU
     , AVG(COALESCE(iap.spend, 0)) AS ASPU
    --  , iap.nb_spends
    --  , iap.nb_spender
    , AVG(CASE WHEN cte."date" + INTERVAL '1' day <= now() THEN 1 ELSE 0 END) AS is_d1_ret
    , AVG(CASE WHEN cte."date" + INTERVAL '7' day <= now() THEN 1 ELSE 0 END) AS is_d7_ret
FROM cte
LEFT JOIN installs i ON i.uid = cte.uid
LEFT JOIN iap ON iap.uid = cte.uid
LEFT JOIN retention rr ON rr.uid = cte.uid
GROUP BY 1,2;