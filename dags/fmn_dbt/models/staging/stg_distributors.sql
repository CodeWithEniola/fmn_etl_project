with source as (
    select * from {{ source('raw', 'raw_Distributors') }}
),

final as (
    select
        distributor_id,
        coalesce(distributor_name, 'Unknown') AS distributor_name,
        coalesce(region, 'Unknown') AS region,
        coalesce(city, 'Unknown') AS city,
        coalesce(outlet_type, 'Unknown') AS outlet_type,
        onboarding_date:: date as onboarding_date,
        is_active
    from source
    where distributor_id is not null
    and onboarding_date ::date <= CURRENT_DATE  -- Ensure onboarding date is not in the future
)

select * from final