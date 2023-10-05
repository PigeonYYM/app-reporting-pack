# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

CREATE OR REPLACE VIEW {legacy_dataset}.uac_perf_grouping_history AS
SELECT
    Day,
    account_name AS AccountName,
    account_id,
    Currency,
    campaign_sub_type AS CampaignSubType,
    campaign_name AS CampaignName,
    campaign_id AS CampaignID,
    ad_group_id AS AdGroupId,
    ad_group_name AS AdGroupName,
    performance_label AS PerformanceGrouping,
    N,
    CASE app_store
        WHEN 'GOOGLE_APP_STORE' THEN 'Google Play'
        WHEN 'APPLE_APP_STORE' THEN 'App Store'
        ELSE 'Other'
    END AS Store,
    app_id AS AppId,
    CASE
        WHEN bidding_strategy = "Installs" THEN "UAC Installs"
        WHEN bidding_strategy = "Installs Advanced" THEN "UAC Installs Advanced"
        WHEN bidding_strategy = "Actions" THEN "UAC Actions"
        WHEN bidding_strategy = "Maximize Conversions" THEN "UAC Maximize Conversions"
        WHEN bidding_strategy = "Target ROAS" THEN "UAC ROAS"
        WHEN bidding_strategy = "Preregistrations" THEN "UAC Pre-Registrations"
        ELSE "Unknown"
    END AS UACType,
    geos AS country_code,
    languages AS Language,
    target_conversions AS TargetConversions
FROM {target_dataset}.performance_grouping;

