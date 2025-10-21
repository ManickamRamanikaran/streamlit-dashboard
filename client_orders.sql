-- Client Orders Query with Old JOIN Conditions
-- This file uses the original table and column names with old JOIN syntax

SELECT 
    c.cust_id,
    c.name,
    o.order_date,
    o.amount
FROM 
    customers c
INNER JOIN 
    orders_legacy o ON c.cust_id = o.customer_id
WHERE 
    o.order_date > '2023-01-01'
    AND c.cust_id IS NOT NULL
ORDER BY 
    o.order_date DESC;
