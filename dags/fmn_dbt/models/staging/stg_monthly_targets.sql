with source as (
    select * from {{ source('raw', 'raw_Monthly_Targets') }}
),

final as (
    select
        record_id,
        salesperson_id,
        year,
        month,
        region,
        target_revenue_ngn,
        actual_revenue_ngn,

        -- Calculate achievement percentage
        coalesce(achievement_pct, 
        case 
            when target_revenue_ngn > 0 
            then (actual_revenue_ngn / target_revenue_ngn) * 100
            else null
        end) as achievement_pct
    from source

    -- IGNORE ---
    where record_id is not null
    and salesperson_id is not null
    and target_revenue_ngn > 0
    and  actual_revenue_ngn >= 0
)

select * from final