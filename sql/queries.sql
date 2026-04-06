-- Total spending by category
SELECT category, SUM(amount)
FROM clean_transactions
GROUP BY category;

-- Monthly spending trend
SELECT year, month, SUM(amount)
FROM clean_transactions
GROUP BY year, month
ORDER BY year, month;

-- High value transactions
SELECT *
FROM clean_transactions
WHERE is_high_value = TRUE;