with source as (
    select * from {{ source('raw', 'raw_Salespersons') }}
),

final as (
    select
        salesperson_id,
        coalesce(salesperson_name, 'Unknown') AS salesperson_name,
        coalesce(region, 'Unknown') AS region,
        coalesce(team, 'Unknown') AS team,
        hire_date:: date as hire_date,
        monthly_target_ngn  -- Static monthly target (different from Monthly_Targets sheet)
    from source

    where salesperson_id is not null
    and hire_date ::date <= CURRENT_DATE  -- Ensure hire date is not in the future
    and monthly_target_ngn >= 0  -- Ensure monthly target is non-negative
)

select * from final