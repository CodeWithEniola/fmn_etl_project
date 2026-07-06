with source as (
    select * from {{ source('raw', 'raw_Products') }}
),

final as (
    select
        -- no null observated but to be safe for future, we will filter out nulls
        product_id,
        coalesce(product_name, 'Unknown') AS product_name,
        coalesce(category, 'Unknown') AS category,
        unit_price_ngn,
        unit_cost_ngn,
        pack_size,
        is_active
    from source
    where
        product_id is not null
        and unit_price_ngn >= 0
        and unit_cost_ngn >= 0
)

select * from final
