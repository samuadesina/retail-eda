SELECT
    -- Sales (fact)
    s.sale_id,
    s.sale_date,
    s.quantity,
    s.unit_price,
    s.discount_pct,
    s.total_amount,
    s.payment_method,
    s.customer_type,

    -- Products (dimension)
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    p.sku,
    p.unit_cost,
    p.margin_pct,

    -- Stores (dimension)
    st.store_id,
    st.store_name,
    st.city,
    st.region,
    st.store_type,
    st.sqft,

    -- Returns (flag)
    CASE WHEN r.sale_id IS NOT NULL THEN 1 ELSE 0 END AS is_returned,
    r.reason         AS return_reason,
    r.refund_amount,
    r.status         AS return_status

FROM retail.sales s
JOIN retail.products p   ON p.product_id = s.product_id
JOIN retail.stores st    ON st.store_id  = s.store_id
LEFT JOIN retail.returns r ON r.sale_id  = s.sale_id

ORDER BY s.sale_date;