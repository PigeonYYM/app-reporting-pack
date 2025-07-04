{% if incremental == "true" %}
CREATE OR REPLACE TABLE `{target_dataset}.aggregate_asset_performance_{date_iso}`
{% else %}
CREATE OR REPLACE TABLE `{target_dataset}.aggregate_asset_performance`
{% endif %}
AS (

SELECT
    day AS date,
    CAST(account_id AS STRING) AS account_id,
    account_name AS account_name,
    ocid AS ocid,
    currency AS currency,
    CAST(campaign_id AS STRING) AS campaign_id,
    campaign_name AS campaign_name,
    campaign_status AS campaign_status,
    campaign_sub_type AS campaign_sub_type,
    geos AS geos,
    languages AS language,
    app_id AS app_id,
    app_store AS app_store,
    bidding_strategy AS bidding_strategy,
    target_conversions AS target_conversions,
    firebase_bidding_status AS firebase_bidding_status,
    CAST(ad_group_id AS STRING) AS ad_group_id,
    ad_group_name AS ad_group_name,
    ad_group_status AS ad_group_status,
    NULL AS ad_id,
    NULL AS ad_name,
    CAST(asset_id AS STRING) AS asset_id,
    asset AS asset,
    asset_link AS asset_link,
    asset_preview_link AS asset_preview_link,
    asset_orientation AS asset_orientation,
    NULL AS video_id,
    NULL AS video_title,
    video_duration AS video_duration,
    video_aspect_ratio AS video_aspect_ratio,
    asset_type AS asset_type,
    field_type AS field_type,
    performance_label AS performance_label,
    asset_status AS asset_status,
    asset_dimensions AS asset_dimensions,
    network AS network,
    clicks AS clicks,
    impressions AS impressions,
    cost AS cost,
    campaign_cost AS campaign_cost,
    cost_non_install_campaigns AS cost_non_install_campaigns,
    conversions AS conversions,
    installs AS installs,
    installs_adjusted AS installs_adjusted,
    inapps AS inapps,
    inapps_adjusted AS inapps_adjusted,
    view_through_conversions AS view_through_conversions,
    conversions_value AS conversions_value,
    installs_1_day AS installs_1_day,
    inapps_1_day AS inapps_1_day,
    conversions_value_1_day AS conversions_value_1_day,
    installs_3_day AS installs_3_day,
    inapps_3_day AS inapps_3_day,
    conversions_value_3_day AS conversions_value_3_day,
    installs_5_day AS installs_5_day,
    inapps_5_day AS inapps_5_day,
    conversions_value_5_day AS conversions_value_5_day,
    installs_7_day AS installs_7_day,
    inapps_7_day AS inapps_7_day,
    conversions_value_7_day AS conversions_value_7_day,
    installs_14_day AS installs_14_day,
    inapps_14_day AS inapps_14_day,
    conversions_value_14_day AS conversions_value_14_day,
    installs_30_day AS installs_30_day,
    inapps_30_day AS inapps_30_day,
    conversions_value_30_day AS conversions_value_30_day,
    NULL AS video_views,
    NULL AS p100_views,
    NULL AS p25_views,
    NULL AS p50_views,
    NULL AS p75_views
FROM
{% if incremental == "true" %}
`{target_dataset}.asset_performance_{date_iso}`
{% else %}
`{target_dataset}.asset_performance`
{% endif %}

UNION ALL

SELECT
    PARSE_DATE('%F', date) AS date,
    CAST(account_id AS STRING) AS account_id,
    account_name AS account_name,
    ocid AS ocid,
    currency AS currency,
    CAST(campaign_id AS STRING) AS campaign_id,
    campaign_name AS campaign_name,
    NULL AS campaign_status,
    'VIDEO' AS campaign_sub_type,
    NULL AS geos,
    NULL AS language,
    NULL AS app_id,
    NULL AS app_store,
    NULL AS bidding_strategy,
    NULL AS target_conversions,
    NULL AS firebase_bidding_status,
    CAST(ad_group_id AS STRING) AS ad_group_id,
    ad_group_name AS ad_group_name,
    NULL AS ad_group_status,
    CAST(ad_id AS STRING) AS ad_id,
    ad_name AS ad_name,
    NULL AS asset_id,
    NULL AS asset,
    NULL AS asset_link,
    NULL AS asset_preview_link,
    NULL AS asset_orientation,
    CAST(video_id AS STRING) AS video_id,
    video_title AS video_title,
    NULL AS video_duration,
    NULL AS video_aspect_ratio,
    NULL AS asset_type,
    NULL AS field_type,
    NULL AS performance_label,
    NULL AS asset_status,
    NULL AS asset_dimensions,
    NULL AS network,
    clicks AS clicks,
    impressions AS impressions,
    cost AS cost,
    NULL AS campaign_cost,
    NULL AS cost_non_install_campaigns,
    conversions AS conversions,
    NULL AS installs,
    NULL AS installs_adjusted,
    NULL AS inapps,
    NULL AS inapps_adjusted,
    view_through_conversions AS view_through_conversions,
    conversions_value AS conversions_value,
    NULL AS installs_1_day,
    NULL AS inapps_1_day,
    NULL AS conversions_value_1_day,
    NULL AS installs_3_day,
    NULL AS inapps_3_day,
    NULL AS conversions_value_3_day,
    NULL AS installs_5_day,
    NULL AS inapps_5_day,
    NULL AS conversions_value_5_day,
    NULL AS installs_7_day,
    NULL AS inapps_7_day,
    NULL AS conversions_value_7_day,
    NULL AS installs_14_day,
    NULL AS inapps_14_day,
    NULL AS conversions_value_14_day,
    NULL AS installs_30_day,
    NULL AS inapps_30_day,
    NULL AS conversions_value_30_day,
    video_views AS video_views,
    p100_views AS p100_views,
    p25_views AS p25_views,
    p50_views AS p50_views,
    p75_views AS p75_views
FROM
{% if incremental == "true" %}
`{target_dataset}.video_campaign_asset_performance_{date_iso}`
{% else %}
`{target_dataset}.video_campaign_asset_performance`
{% endif %}

);