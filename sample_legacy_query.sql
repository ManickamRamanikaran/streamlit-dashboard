-- Sample SQL query with old table and column names
-- This file demonstrates the types of transformations the validator can handle

-- Customer orders query with legacy naming
SELECT 
    c.cust_id,
    c.name,
    c.email,
    o.order_id,
    o.order_date,
    o.amount,
    p.product_name
FROM 
    customers c
INNER JOIN 
    orders_legacy o ON c.cust_id = o.customer_id
LEFT JOIN
    prod.inventory p ON o.product_id = p.prod_id
WHERE 
    o.order_date >= '2023-01-01'
    AND c.cust_id IS NOT NULL
    AND o.amount > 50.00
ORDER BY 
    o.order_date DESC, c.name ASC;

-- User account query with old references
SELECT 
    u.user_id,
    u.username,
    o.order_id,
    o.amount
FROM 
    user_accounts u
INNER JOIN 
    orders_legacy o ON u.user_id = o.created_by
WHERE 
    u.user_id IN (SELECT DISTINCT created_by FROM orders_legacy)
    AND o.order_date > '2023-06-01';

-- Payment information with legacy joins
SELECT 
    c.cust_id,
    c.name,
    b.payment_method,
    b.amount
FROM 
    customers c
INNER JOIN 
    payment_info b ON c.cust_id = b.customer_ref
WHERE 
    b.payment_date >= '2023-01-01'
    AND b.status = 'completed';
