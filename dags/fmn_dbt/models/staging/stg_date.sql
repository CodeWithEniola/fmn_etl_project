with source as (
    select * from {{ source('raw', 'raw_Date_Table') }}
),

final as (
    select
        date as date_key,  -- Use date as primary key
        year,
        quarter,
        month,
        month_name,
        week,
        day_of_week,
        is_weekend,
        is_month_end
    from source

    where date is not null
    and year is not null
    and quarter between 1 and 4
    and month between 1 and 12
    and week between 1 and 53
)

select * from final