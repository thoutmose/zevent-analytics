-- Punctuation/CAPS/hype-emote/positive/hostile message rates per channel per
-- hour — replaces 4 separate full scans of stg_bronze__live_chat's 8.3M
-- rows (chat_hype_components_timeseries, channel_sentiment_leaderboard_timeseries,
-- channel_toxicity_leaderboard_timeseries, chat_mood_timeseries each ran
-- their own ~1.3-1.9s AVG(CASE...) scan). Word lists match
-- app/data/chat_lexicons.py exactly; \y matches a word boundary at the start
-- only (not the end), which is intentional there — it still catches plurals
-- ("connards") and stretched-out forms ("CONNASSEEEE").
--
-- Computed on the full row set, not a sample: the app's random() < sample_rate
-- exists only to dodge Streamlit's 15s statement_timeout, a constraint a dbt
-- build doesn't have. HAVING is applied to the real COUNT(*), so this is more
-- accurate than what the app currently shows, not just faster.
--
-- Sentiment/toxicity/weighted-hype scores are deliberately NOT computed here
-- — they're cheap arithmetic on the 5 rate columns below and stay in
-- repository.py/the app, where hype's weights are user-adjustable.
select
    channel,
    date_trunc('hour', message_sent_at) as hour_bucket,
    count(*) as message_count,
    avg(case when message_text like '%!!%' then 1.0 else 0.0 end) as punct_rate,
    avg(
        case
            when
                message_text = upper(message_text)
                and message_text != lower(message_text)
                and length(message_text) >= 4
            then 1.0
            else 0.0
        end
    ) as caps_rate,
    avg(
        case
            when message_text ~* '\y(lul|kekw|pog|hype)' then 1.0
            else 0.0
        end
    ) as emote_rate,
    avg(
        case
            when
                message_text
                ~* '\y(merci|super|genial|bravo|parfait|excellent|adorable|magnifique|incroyable|love)'
                then 1.0
            else 0.0
        end
    ) as positive_rate,
    avg(
        case
            when message_text ~* '\y(connard|connasse|idiot|debile|degage|ta gueule)' then 1.0
            else 0.0
        end
    ) as hostile_rate
from {{ ref('stg_bronze__live_chat') }}
where message_sent_at is not null
group by 1, 2
having count(*) >= 20
