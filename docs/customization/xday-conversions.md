# 素材的 1～7 日付费与留存事件

当上报事件本身已经表示 X 日付费或留存时，通过配置把事件直接映射到素材指标。
这里的 X 日含义由上报事件定义；报表不重新计算转化延迟、不按天拆分事件，
也不把 D1～DX 的事件再累加。

## 输出字段

`asset_performance`、`video_campaign_asset_performance`、
`display_asset_performance`、`aggregate_asset_performance` 及最终的
`aggregate_asset_performance_overwrite_di` 均包含以下 28 列，X 取 1～7：

| 字段 | 指标 | 默认事件名 | 取值字段 |
| --- | --- | --- | --- |
| `paying_users_X_day` | X 日付费人数 | `g_s2s_dX_payer` | `conversions` |
| `payment_count_X_day` | X 日付费次数 | `g_s2s_dX_revenue` | `conversions` |
| `payment_amount_X_day` | X 日付费金额 | `g_s2s_dX_revenue` | `conversions_value` |
| `retained_users_X_day` | X 日留存用户数 | `g_s2s_dX_retain` | `conversions` |

例如 `g_s2s_d1_revenue` 同时提供 1 日付费次数与 1 日付费金额，分别读取
`conversions` 和 `conversions_value`；D2～D7 使用同样的命名规则。
这里的字段名来自现有素材转化数据表，对应 Google Ads 的同名指标。

这些列直接聚合各事件上报的计量。人数应映射到已按业务口径统计人数的事件，
不能用普通支付次数事件替代。报表没有用户 ID，不会额外做用户去重。
所有新列使用 FLOAT64，以保留来源数据的计量精度。

## 配置事件映射

上述 21 个事件到 28 个指标的映射已经内置。已有运行配置即使没有
`xday_conversions` 也会使用默认映射；`app/config.yaml.template` 同时列出了全部配置。

如需修改映射，在实际运行配置的 `gaarf-bq.params.template` 下设置
`xday_conversions`。以下为 D1 示例：

```yaml
gaarf-bq:
  params:
    template:
      xday_conversions:
        - paying_users_1_day:
            conversion_name: g_s2s_d1_payer
            value_field: conversions
        - payment_count_1_day:
            conversion_name: g_s2s_d1_revenue
            value_field: conversions
        - payment_amount_1_day:
            conversion_name: g_s2s_d1_revenue
            value_field: conversions_value
        - retained_users_1_day:
            conversion_name: g_s2s_d1_retain
            value_field: conversions
```

显式配置会替换整组默认映射。若只配置上述 D1 示例，D2～D7 列将为 NULL；
可从配置模板复制完整 28 项，再按需修改。
配置必须使用上述列表格式，每个输出字段只能配置一次。
如需停用全部新指标，设置 `xday_conversions: [{}]`；不要使用空字典或空列表。
每项必须只指定 `conversion_name` 或 `conversion_id` 中的一个：

- `conversion_name` 精确匹配 Google Ads 转化操作名称。原素材路径通过
  `app_conversions_mapping.conversion_name` 匹配；VIDEO、DISPLAY 直接匹配事件明细中的 `conversion_name`。
  它可能与 Firebase 或第三方归因平台的原始事件名不同。
- `conversion_id` 精确匹配转化 ID，支持整数或字符串；推荐在事件名称可能重复时使用。
- `value_field` 可选，支持 `conversions`、`conversions_value`、
  `all_conversions` 和 `all_conversions_value`。
  默认人数、次数取 `conversions`，金额取 `conversions_value`。
  如需采用全部转化口径，可显式覆盖，例如：

```yaml
- payment_amount_1_day:
    conversion_name: g_s2s_d1_revenue
    value_field: all_conversions_value
```

可从现有事件映射表查询名称和 ID：

```sql
SELECT DISTINCT conversion_id, conversion_name, conversion_type, conversion_source
FROM `YOUR_PROJECT.arp.app_conversions_mapping`
ORDER BY conversion_name, conversion_id;
```

## 数据范围和执行

三条取数路径复用同一组默认事件及 `xday_conversions` 配置：

| 广告类型 | 原始事件表 | 事件聚合和关联粒度 |
| --- | --- | --- |
| MULTI_CHANNEL、DEMAND_GEN、SEARCH | `asset_conversion_split` | 日期、网络、广告组、素材、素材字段类型 |
| VIDEO | `video_conversion_split` | 日期、账号、广告系列、广告组、广告、视频 |
| DISPLAY | `display_conversion_split` | 日期、网络、账号、广告系列、广告组、广告 |

表名中的 `conversion_split` 表示按转化事件记录的数据，不表示 X 日拆分。
VIDEO 和 DISPLAY 分别从 Google Ads 的 `video`、`ad_group_ad` 资源获取事件明细，
与花费、展示等表现指标分开查询，避免事件分段改变原有表现指标。

原素材路径按输出使用的网络和素材字段类型归一化后聚合，已汇总的事件值只取一次；
事件名称匹配使用 EXISTS，避免重复映射记录放大新指标。
VIDEO 保留广告维度，同一个视频在不同广告中的事件独立统计；
DISPLAY 按原始投放网络关联事件，并将规范化后的 `network` 透传到最终结果表。
两条新路径均在关联前汇总事件、去重维度映射，避免同一事件被复制到多条表现记录。

DISPLAY 的结果代表广告整体，不提供自适应展示广告内部单张图片或单条文案的事件归因。
其最终 `asset_id` 继续使用广告 ID，并同时保留 `ad_id`、`ad_name`。
视频路径现在仅处理 VIDEO 广告系列；展示广告中的视频随 DISPLAY 广告整体统计，
避免同一展示广告进入两条汇总路径。PMax 尚未接入。

- 使用默认映射时，全部 28 项指标均启用；显式配置中遗漏的指标为 NULL。
  已配置事件但对应素材粒度没有匹配记录时为 0。
- 上述 0 值语义以原始事件表已正确刷新为前提。当前固定依赖
  `garf-io==1.1.0` / `pandas-gbq==0.33.0` 在首次零行采集时会建空表，
  但已有表遇到整批查询返回零行时，会在执行覆盖写入前返回，可能保留旧数据。
  这不同于某个广告或某个配置事件没有匹配记录；遇到整批零行时需核对原始表是否已清空。
  本次未修改项目通用写入器。
- 上述三个中间表现表及汇总表的增量版本（表名带 `_YYYYMMDD` 后缀）同样输出新字段。
  已有历史表不会因为更新代码而自动补齐。
- 更新代码后需依次运行 core、assets、aggregate、overwrite_tables 模块，
  或执行包含这些模块的常规完整流程；只运行 aggregate 无法读取旧素材表中尚不存在的新列。
  assets 模块会自动执行新增的两份事件查询；需重新取数以获得视频表新增的 `account_id`。
- 配置模板的 `customer_ids_query` 已覆盖上述五种广告类型。若已有运行配置仍只筛选
  `MULTI_CHANNEL`，需同步扩大筛选范围，否则仅投放 VIDEO／DISPLAY 的账号不会被采集。

此配置独立于 `cohort_days`，不会改变已有安装、in-app 和转化价值的延迟队列指标。
