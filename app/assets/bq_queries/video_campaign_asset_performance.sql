-- Copyright 2024 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Keep the X-day configuration contract aligned with asset_performance.sql.
{% set xday_event_suffixes = {'paying_users': 'payer', 'payment_count': 'revenue', 'payment_amount': 'revenue', 'retained_users': 'retain'} %}
{% set default_xday_events = {} %}
{% for day in range(1, 8) %}
{% for metric, suffix in xday_event_suffixes.items() %}
{% set _ = default_xday_events.update({metric ~ '_' ~ day ~ '_day': {'conversion_name': 'g_s2s_d' ~ day ~ '_' ~ suffix}}) %}
{% endfor %}
{% endfor %}
{% set xday_events = {} %}
{% for mapping in xday_conversions | default([default_xday_events]) %}
{% for column, event in mapping.items() %}
ASSERT {{ (column not in xday_events) | lower }}
  AS 'Duplicate xday_conversions output column';
{% set _ = xday_events.update({column: event}) %}
{% endfor %}
{% endfor %}
{% set xday_metrics = ['paying_users', 'payment_count', 'payment_amount', 'retained_users'] %}
{% set xday_value_fields = ['conversions', 'conversions_value', 'all_conversions', 'all_conversions_value'] %}
{% set xday_columns = [] %}
{% for day in range(1, 8) %}
{% for metric in xday_metrics %}
{% set _ = xday_columns.append(metric ~ '_' ~ day ~ '_day') %}
{% endfor %}
{% endfor %}
{% for column, event in xday_events.items() %}
ASSERT {{ (column in xday_columns) | lower }}
  AS 'Unknown xday_conversions output column';
ASSERT {{ ((event.get('conversion_name') is not none) != (event.get('conversion_id') is not none)) | lower }}
  AS 'Each xday_conversions entry must specify exactly one conversion_name or conversion_id';
ASSERT {{ (event.get('value_field', 'conversions') in xday_value_fields) | lower }}
  AS 'xday_conversions value_field must be conversions, conversions_value, all_conversions or all_conversions_value';
{% endfor %}

{% if incremental == "true" %}
CREATE OR REPLACE TABLE `{target_dataset}.video_campaign_asset_performance_{date_iso}`
{% else %}
CREATE OR REPLACE TABLE `{target_dataset}.video_campaign_asset_performance`
{% endif %}
AS (
  WITH
    MappingTable AS (
      SELECT
        M.account_id,
        M.campaign_id,
        M.ad_group_id,
        M.ad_id,
        ANY_VALUE(M.ad_name) AS ad_name,
        ANY_VALUE(M.ad_group_name) AS ad_group_name,
        ANY_VALUE(M.campaign_name) AS campaign_name,
        ANY_VALUE(M.account_name) AS account_name,
        ANY_VALUE(O.ocid) AS ocid,
        ANY_VALUE(M.currency) AS currency
      FROM `{bq_dataset}.ad_group_ad_mapping` AS M
      LEFT JOIN `{bq_dataset}.ocid_mapping` AS O
        USING (account_id)
      GROUP BY 1, 2, 3, 4
    )

    {% if xday_events %}
    , XDayConversions AS (
      -- Events already encode their day window; sum at the performance grain.
      SELECT
        C.date,
        C.account_id,
        C.campaign_id,
        C.ad_group_id,
        C.ad_id,
        C.video_id,
        {% for day in range(1, 8) %}
        {% for metric in xday_metrics %}
        {% set column = metric ~ '_' ~ day ~ '_day' %}
        {% set event = xday_events.get(column) %}
        {% if event %}
        {% set value_field = event.get('value_field', 'conversions_value' if metric == 'payment_amount' else 'conversions') %}
        CAST(SUM(CASE
          {% if event.get('conversion_id') is not none %}
          WHEN CAST(C.conversion_id AS STRING) = {{ event.conversion_id | string | tojson | replace('{', '\\u007b') | replace('}', '\\u007d') }}
          {% else %}
          WHEN C.conversion_name = {{ event.conversion_name | tojson | replace('{', '\\u007b') | replace('}', '\\u007d') }}
          {% endif %}
          THEN COALESCE(C.{{ value_field if value_field in xday_value_fields else 'conversions' }}, 0)
          ELSE 0
        END) AS FLOAT64) AS {{column}},
        {% else %}
        CAST(NULL AS FLOAT64) AS {{column}},
        {% endif %}
        {% endfor %}
        {% endfor %}
      FROM `{bq_dataset}.video_conversion_split` AS C
      GROUP BY 1, 2, 3, 4, 5, 6
    )
    {% endif %}
  SELECT
    VD.date AS date,
    VD.account_id AS account_id,
    AD.account_name AS account_name,
    AD.currency AS currency,
    AD.ocid AS ocid,
    VD.campaign_id AS campaign_id,
    AD.campaign_name AS campaign_name,
    VD.campaign_type AS campaign_type,
    VD.ad_group_id AS ad_group_id,
    AD.ad_group_name AS ad_group_name,
    VD.ad_id AS ad_id,
    AD.ad_name AS ad_name,
    VD.video_id AS video_id,
    VD.video_title AS video_title,
    {% for day in range(1, 8) %}
    {% for metric in xday_metrics %}
    {% set column = metric ~ '_' ~ day ~ '_day' %}
    {% if xday_events.get(column) %}
    CAST(COALESCE(XDC.{{column}}, 0) AS FLOAT64) AS {{column}},
    {% else %}
    CAST(NULL AS FLOAT64) AS {{column}},
    {% endif %}
    {% endfor %}
    {% endfor %}
    VD.clicks AS clicks,
    VD.impressions AS impressions,
    ROUND(IEEE_DIVIDE(CAST(VD.cost AS FLOAT64), 1000000.0), 3) AS cost,
    VD.conversions AS conversions,
    VD.view_through_conversions AS view_through_conversions,
    VD.conversions_value AS conversions_value,
    VD.video_trueview_views AS video_trueview_views,
    VD.p100_views_rate * VD.impressions AS p100_views,
    VD.p25_views_rate * VD.impressions AS p25_views,
    VD.p50_views_rate * VD.impressions AS p50_views,
    VD.p75_views_rate * VD.impressions AS p75_views
  FROM `{bq_dataset}.video` AS VD
  LEFT JOIN MappingTable AS AD
    ON VD.account_id = AD.account_id
      AND VD.campaign_id = AD.campaign_id
      AND VD.ad_group_id = AD.ad_group_id
      AND VD.ad_id = AD.ad_id
  {% if xday_events %}
  LEFT JOIN XDayConversions AS XDC
    ON VD.date = XDC.date
      AND VD.account_id = XDC.account_id
      AND VD.campaign_id = XDC.campaign_id
      AND VD.ad_group_id = XDC.ad_group_id
      AND VD.ad_id = XDC.ad_id
      AND VD.video_id = XDC.video_id
  {% endif %}
  -- DISPLAY is reported once via display_asset_performance at ad level.
  WHERE VD.campaign_type = 'VIDEO'
);
