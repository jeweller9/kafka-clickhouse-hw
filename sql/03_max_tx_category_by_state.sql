SELECT
    us_state,
    argMax(cat_id, amount) AS top_category,
    max(amount) AS max_amount
FROM teta.transactions
GROUP BY us_state
ORDER BY max_amount DESC;
