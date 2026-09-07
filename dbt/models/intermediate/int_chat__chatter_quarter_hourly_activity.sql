-- Chatter x 15-minute-bucket grain (distinct from int_chat__chatter_channel_activity's
-- chatter x channel, all-time grain) — feeds day-over-day-style retention and
-- new-vs-returning-chatter marts, both of which need "was this chatter
-- present in bucket N" as their own dimension. 15 minutes rather than a full
-- day since Zevent only runs ~55 hours, so day-level buckets barely move.
--
-- Incremental: this model is small but stg_bronze__live_chat is not, and it's
-- rebuilt every hourly dbt run, so a full scan every run tanks the cache hit
-- ratio. Only pull messages newer than the last bucket we already have, minus
-- one bucket-width of lookback so the most recent (still-filling) bucket gets
-- correctly recomputed instead of left partial; merge on the same
-- (chatter_id, quarter_hour_bucket) key the uniqueness test already checks.
{{
  config(
    materialized='incremental',
    unique_key=['chatter_id', 'quarter_hour_bucket'],
    incremental_strategy='delete+insert'
  )
}}

select
    chatter_id,
    date_bin('15 minutes', message_sent_at, timestamptz '2000-01-01 00:00:00+00') as quarter_hour_bucket,
    count(distinct channel) as distinct_channel_count,
    count(*) as message_count
from {{ ref('stg_bronze__live_chat') }}
where chatter_id is not null and message_sent_at is not null
{% if is_incremental() %}
  and message_sent_at >= (
    select coalesce(max(quarter_hour_bucket), timestamptz '2000-01-01 00:00:00+00') - interval '15 minutes'
    from {{ this }}
  )
{% endif %}
group by 1, 2
