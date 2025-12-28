/*
================================================================================
  SQL EXAM SUBMISSION REPORT
================================================================================

  Candidate: Shoval Benjer
  Position: data_analytics
  Submitted: 2025-12-08 22:39:40 UTC

================================================================================
*/


/*
--------------------------------------------------------------------------------
  Question 1: Daily Impressions and Conversions
--------------------------------------------------------------------------------

  Calculate daily impressions and conversions per date and country in the last 7 days.
--------------------------------------------------------------------------------
*/

SELECT
    report_date,
    COALESCE(country_code, 'Unknown') AS country_code,
    COUNT(*) AS daily_impressions,
    COUNT_IF(converted_pixel) AS daily_conversions,
    ROUND((COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0)) * 100, 4) AS daily_cr_percent
FROM
    exam_analytics.impressions
WHERE
    report_date BETWEEN DATEADD(day, -6, '2025-10-31') AND '2025-10-31'
GROUP BY
    1, 2
HAVING
    COUNT(*) > 50
ORDER BY
    report_date DESC, 
    daily_impressions DESC;



/*
--------------------------------------------------------------------------------
  Question 2: Yesterday's Conversion Rate per Tag
--------------------------------------------------------------------------------

  Calculate yesterday's conversion rate per tag_id, only for tags that were active at least 20 days in the last 30 days.
--------------------------------------------------------------------------------
*/

SELECT
    tag_id,
    ROUND(COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0), 4) AS conversion_rate,
    COUNT(*) AS impressions_context
FROM
    exam_analytics.impressions
JOIN
    exam_analytics.tags USING (tag_id)
WHERE
    report_date = DATEADD(day, -1, '2025-10-31')
    AND active_days_last_30 >= 20
GROUP BY
    1
ORDER BY
    2 DESC;



/*
--------------------------------------------------------------------------------
  Question 3: Average Daily Conversion Rate by Advertiser
--------------------------------------------------------------------------------

  Calculate the average daily conversion rate for each advertiser, considering only those campaigns that have reached 95% of their daily cap of impressions on the same day and received at least 1 conversion.
--------------------------------------------------------------------------------
*/

SELECT
    advertiser_id,
    ROUND(AVG(daily_cr), 4) AS average_conversion_rate
FROM (
    SELECT
        advertiser_id,
        report_date,
        COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0) AS daily_cr
    FROM
        exam_analytics.impressions
    JOIN
        exam_analytics.campaigns USING (campaign_id)
    WHERE
        report_date BETWEEN DATEADD(day, -6, '2025-10-31') AND '2025-10-31'
    GROUP BY
        report_date,
        campaign_id,
        advertiser_id,
        cap
    HAVING
        COUNT(*) >= (cap * 0.95)
        AND COUNT_IF(converted_pixel) >= 1
)
GROUP BY
    advertiser_id
ORDER BY
    average_conversion_rate DESC;



/*
--------------------------------------------------------------------------------
  Question 4: Top 3 Campaigns by Device and Country
--------------------------------------------------------------------------------

  Identify the top 3 campaigns with the highest conversion rates in the last 7 days for each device type (mobile, desktop) and country code (US, PH), limited to campaigns with more than 10 impressions.
--------------------------------------------------------------------------------
*/

SELECT
    campaign_id,
    device_type,
    country_code,
    ROUND(COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0), 2) AS conversion_rate
FROM
    exam_analytics.impressions
WHERE
    report_date BETWEEN DATEADD(day, -6, '2025-10-31') AND '2025-10-31'
    AND country_code IN ('US', 'PH')
    AND device_type IN ('mobile', 'desktop')
GROUP BY
    campaign_id,
    device_type,
    country_code
HAVING
    COUNT(*) > 10
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY device_type, country_code 
        ORDER BY COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0) DESC, COUNT(*) DESC
    ) <= 3
ORDER BY
    device_type,
    country_code,
    conversion_rate DESC;



/*
--------------------------------------------------------------------------------
  Question 5: Aggregated Dataset and Anomaly Detection
--------------------------------------------------------------------------------

  Build an hourly aggregated dataset grouped by all the non-aggregated columns in addition to total impressions, total cost, total conversions, conversion rate. Limit the data to the last 7 days, and include only the top 5 advertisers by total spend (advertiser cost), considering only the highest-spend (cost) campaign for each of those advertisers. Using your aggregated dataset, create pivot charts (Excel or similar) that clearly show meaningful trends and performance changes over the last 7 days. You may choose any pivot layout that presents the data in a way that makes trends easy to understand. Your goal is to identify any anomalies or irregular patterns you find for any of the advertiser's campaigns. For each anomaly, provide a clear and brief explanation, your reasoning, and a screenshot of the pivot chart that supports your observation.
--------------------------------------------------------------------------------
*/

SELECT
    report_date,
    DATE_PART('hour', report_time) AS report_hour,
    advertiser_id,
    campaign_id,
    COUNT(*) AS total_impressions,
    SUM(conversion_rev) AS total_cost,
    COUNT_IF(converted_pixel) AS total_conversions,
    ROUND(COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0), 4) AS conversion_rate
FROM
    exam_analytics.impressions
JOIN
    exam_analytics.campaigns USING (campaign_id)
WHERE
    report_date BETWEEN DATEADD(day, -6, '2025-10-31') AND '2025-10-31'
    AND campaign_id IN (
        SELECT campaign_id FROM (
            SELECT
                campaign_id,
                advertiser_id,
                DENSE_RANK() OVER (ORDER BY advertiser_total_spend DESC) as adv_rank,
                ROW_NUMBER() OVER (PARTITION BY advertiser_id ORDER BY campaign_total_spend DESC) as camp_rank
            FROM (
                SELECT 
                    campaign_id,
                    advertiser_id,
                    SUM(conversion_rev) as campaign_total_spend,
                    SUM(SUM(conversion_rev)) OVER (PARTITION BY advertiser_id) as advertiser_total_spend
                FROM 
                    exam_analytics.impressions
                JOIN 
                    exam_analytics.campaigns USING (campaign_id)
                WHERE 
                    report_date BETWEEN DATEADD(day, -6, '2025-10-31') AND '2025-10-31'
                GROUP BY 
                    campaign_id, advertiser_id
            ) AS base_stats
        ) AS ranked_stats
        WHERE adv_rank <= 5 AND camp_rank = 1
    )
GROUP BY
    1, 2, 3, 4
ORDER BY
    advertiser_id, report_date, report_hour



/*
--------------------------------------------------------------------------------
  Question 6: Chargeback Investigation
--------------------------------------------------------------------------------

  Sometimes advertisers request chargebacks (refunds) for traffic they believe was invalid or fraudulent. Based on the anomalies you identified in the previous question, choose one advertiser that you believe is most likely to have submitted a chargeback. Investigate that advertiser's campaigns and by querying the raw dataset, determine which tag_id (traffic source) is most likely responsible for the suspicious behavior.
--------------------------------------------------------------------------------
*/

SELECT
    tag_id,
    COUNT(*) AS impressions,
    COUNT_IF(converted_pixel) AS conversions,
    ROUND(COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0), 4) AS conversion_rate
FROM
    exam_analytics.impressions
JOIN
    exam_analytics.campaigns USING (campaign_id)
WHERE
    advertiser_id = '600450'
    AND report_date BETWEEN DATEADD(day, -6, '2025-10-31') AND '2025-10-31'
GROUP BY
    tag_id
HAVING
    COUNT(*) > 1000
ORDER BY
    conversion_rate ASC, 
    impressions DESC
LIMIT 5



/*
--------------------------------------------------------------------------------
  Question 7: (Bonus) Fraud Detection System
--------------------------------------------------------------------------------

  Using the suspicious tag_id identified in the previous question, define 3–4 fraud indicators that together could form a basic fraud detection or "fraud alert" system. Demonstrate how Indicators can be detected using a query on the raw dataset, explain why this indicator may be associated with fraudulent or invalid traffic, and describe how it contributes to flagging the traffic source as suspicious.
--------------------------------------------------------------------------------
*/

SELECT
    tag_id,
    ROUND((tag_cr - avg_cr) / NULLIF(std_cr, 0), 2) AS indicator_z_score,
    ROUND(impressions / NULLIF(unique_ips, 0), 2) AS indicator_ip_density,
    ROUND(pct_desktop, 2) AS indicator_device_monoculture,
    CASE 
        WHEN (tag_cr - avg_cr) / NULLIF(std_cr, 0) < -1.96 THEN 'FRAUD_CONFIRMED'
        ELSE 'REVIEW_REQUIRED'
    END AS final_status
FROM (
    SELECT 
        tag_id,
        COUNT(*) as impressions,
        COUNT(DISTINCT user_ip) as unique_ips,
        COUNT_IF(device_type = 'desktop') / NULLIF(COUNT(*), 0) as pct_desktop,
        COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0) as tag_cr,
        AVG(COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0)) OVER (PARTITION BY advertiser_id) as avg_cr,
        STDDEV(COUNT_IF(converted_pixel) / NULLIF(COUNT(*), 0)) OVER (PARTITION BY advertiser_id) as std_cr
    FROM exam_analytics.impressions
    JOIN exam_analytics.campaigns USING (campaign_id)
    WHERE 
        advertiser_id = '600450' 
        AND report_date BETWEEN DATEADD(day, -6, '2025-10-31') AND '2025-10-31'
    GROUP BY
        tag_id,
        advertiser_id
    HAVING COUNT(*) > 1000
)
WHERE tag_id = '837193';



/*
================================================================================
  End of SQL Exam Submission Report
  Generated by AdMaven Analytics - 2025-12-08 22:39:40 UTC
================================================================================
*/
