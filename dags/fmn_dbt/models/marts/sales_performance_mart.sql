{{ config(materialized='table') }}

with
--1. Aggregate sales data by salesperson and month
sales_monthly as (
    select
        s.salesperson_id,
        extract(year from s.transaction_date) as year,
        extract(month from s.transaction_date) as month,
        sum(s.revenue_ngn) as actual_revenue_ngn,
        sum(s.quantity) as total_quantity,
        sum(s.gross_profit_ngn) as total_gross_profit,
        count(distinct s.transaction_id) as total_transactions,
        sum(case when s.is_returned then 1 else 0 end) as total_returns
    from {{ ref('stg_transactions') }} s
    group by s.salesperson_id, year, month
),

--2. Get salesperson details
salespersons as (
    select
        salesperson_id,
        salesperson_name,
        region,
        team
    from {{ ref('stg_salespersons') }}
),

--3. Get monthly targets
targets as (
    select
        salesperson_id,
        year,
        month,
        target_revenue_ngn,
        achievement_pct as  source_achievement_pct
    from {{ ref('stg_monthly_targets') }}
)

-- 4. Combine all data into the final mart
final as (
    select
        sm.salesperson_id,
        sp.salesperson_name,
        sp.region as salesperson_region,
        sp.team,
        sm.year,
        sm.month,
        sm.actual_revenue_ngn,
        sm.total_quantity,
        sm.total_gross_profit,
        sm.total_transactions,
        sm.total_returns,
        t.target_revenue_ngn,
        t.source_achievement_pct,
        
        -- Calculate achievement % from actual vs target
        case 
            when t.target_revenue_ngn > 0 
            then (sm.actual_revenue_ngn / t.target_revenue_ngn) * 100 
            else null 
        end as calculated_achievement_pct,
        
        -- Use calculated if available, otherwise fallback to source
        coalesce(
            (sm.actual_revenue_ngn / nullif(t.target_revenue_ngn, 0)) * 100,
            t.source_achievement_pct
        ) as achievement_pct,
        
        -- Return rate %
        case 
            when sm.total_transactions > 0 
            then (sm.total_returns::float / sm.total_transactions) * 100 
            else 0 
        end as return_rate_pct,
        
        -- Average order value
        case 
            when sm.total_transactions > 0 
            then sm.actual_revenue_ngn / sm.total_transactions 
            else 0 
        end as avg_order_value_ngn
        
    from sales_monthly sm
    left join salespersons sp 
        on sm.salesperson_id = sp.salesperson_id
    left join targets t 
        on sm.salesperson_id = t.salesperson_id 
        and sm.year = t.year 
        and sm.month = t.month
)

select * from final