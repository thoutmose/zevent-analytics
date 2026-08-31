-- Weak bot/throwaway-account signals combined, not a classifier: a low
-- gap_coefficient_of_variation (suspiciously regular posting rhythm) plus a
-- digit-suffixed login plus a brand-new account is worth a human look, none
-- of the three alone proves anything.
with regularity as (
    select
        chatter_id,
        count(*) as gap_count,
        avg(extract(epoch from inter_arrival)) as avg_gap_seconds,
        stddev(extract(epoch from inter_arrival)) as stddev_gap_seconds
    from {{ ref('int_chat__inter_arrival') }}
    where inter_arrival is not null
    group by 1
    having count(*) >= 5
)

select
    ca.chatter_id,
    ca.chatter,
    ca.account_created_at,
    ca.has_long_digit_suffix,
    ca.total_message_count,
    r.gap_count,
    r.avg_gap_seconds,
    r.stddev_gap_seconds,
    case
        when r.avg_gap_seconds > 0
            then r.stddev_gap_seconds / r.avg_gap_seconds
    end as gap_coefficient_of_variation
from {{ ref('int_chat__chatter_activity') }} as ca
left join regularity as r on r.chatter_id = ca.chatter_id
