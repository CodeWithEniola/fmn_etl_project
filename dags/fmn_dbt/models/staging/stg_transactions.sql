with source as (
    select * from {{ ref('raw', 'raw_Transactions') }}
),

final as (
    SELECT 
    transaction_id, 
    product_id, 
    COALESCE(distributor_id, 'Unknown') AS distributor_id, 
    salesperson_id, 
    -- Date
    transaction_date::DATE AS transaction_date, 
    -- Metrics
    quantity, 
    unit_price_ngn, 
    discount_pct, 
    discount_amount_ngn, 
    revenue_ngn, 
    cogs_ngn, 
    gross_profit_ngn, 
    -- Attributes
    payment_method, 
    delivery_status, 
    transaction_status, 
    notes, 
    -- Derived column to indicate if the transaction is a return for easy filtering
    CASE 
        WHEN LOWER(transaction_status) = 'returned' THEN TRUE 
        ELSE FALSE 
    END AS is_returned, 
    -- Derived column to calculate profit margin percentage
    CASE 
        WHEN revenue_ngn > 0 THEN (gross_profit_ngn / revenue_ngn) * 100 
        ELSE NULL 
    END AS profit_margin_pct, 
    -- Derived column to calculate Revenue per Unit
    CASE 
        WHEN quantity > 0 THEN revenue_ngn / quantity 
        ELSE NULL 
    END AS unit_revenue_ngn 
FROM source 
WHERE transaction_id IS NOT NULL 
  AND product_id IS NOT NULL 
  AND salesperson_id IS NOT NULL 
  AND quantity > 0 
  AND revenue_ngn >= 0 -- Adjusted to allow $\$0$ revenue (e.g., in cases of 100% discounts)
  AND transaction_date::DATE <= CURRENT_DATE; -- Ensure transaction date is not in the future
)

select * from final