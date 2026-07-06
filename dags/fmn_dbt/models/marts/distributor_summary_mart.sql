-- depends_on: {{ ref('stg_transactions') }}
-- depends_on: {{ ref('stg_distributors') }}
-- depends_on: {{ ref('stg_date') }}

{{
    config(
        materialized='table',
        schema='marts',
        alias='distributor_summary_mart'
    )
}}

with
-- Step 1: Get all transactions with distributor and date context
transactions as (
    select
        distributor_id,
        transaction_date,
        revenue_ngn,
        quantity,
        gross_profit_ngn,
        is_returned,
        transaction_id,
        -- Extract month/year for grouping
        extract(year from transaction_date) as year,
        extract(month from transaction_date) as month
    from {{ ref('stg_transactions') }}
),

-- Step 2: Aggregate metrics at distributor + month level
aggregated as (
    select
        distributor_id,
        year,
        month,
        count(distinct transaction_id) as total_transactions,
        sum(revenue_ngn) as total_revenue_ngn,
        sum(quantity) as total_quantity_sold,
        sum(gross_profit_ngn) as total_gross_profit,
        sum(case when is_returned then 1 else 0 end) as total_returns
    from transactions
    group by distributor_id, year, month
),

-- Step 3: Join with distributor dimensions
joined as (
    select
        a.distributor_id,
        d.distributor_name,
        d.region,
        d.city,
        d.outlet_type,
        a.year,
        a.month,
        a.total_revenue_ngn,
        a.total_quantity_sold,
        a.total_gross_profit,
        a.total_transactions,
        a.total_returns,
        
        -- Calculate Return Rate (%)
        case 
            when a.total_transactions > 0 
            then (a.total_returns::float / a.total_transactions) * 100 
            else 0 
        end as return_rate_pct,
        
        -- Calculate Average Order Value (NGN per transaction)
        case 
            when a.total_transactions > 0 
            then a.total_revenue_ngn / a.total_transactions 
            else 0 
        end as avg_order_value_ngn,
        
        -- Calculate Gross Profit Margin (%)
        case 
            when a.total_revenue_ngn > 0 
            then (a.total_gross_profit / a.total_revenue_ngn) * 100 
            else 0 
        end as gross_profit_margin_pct
        
    from aggregated a
    left join {{ ref('stg_distributors') }} d 
        on a.distributor_id = d.distributor_id
)

-- Step 4: Final output
select
    distributor_id,
    distributor_name,
    region,
    city,
    outlet_type,
    year,
    month,
    total_revenue_ngn,
    total_quantity_sold,
    total_gross_profit,
    total_transactions,
    total_returns,
    return_rate_pct,
    avg_order_value_ngn,
    gross_profit_margin_pct
from joined
order by year desc, month desc, total_revenue_ngn desc