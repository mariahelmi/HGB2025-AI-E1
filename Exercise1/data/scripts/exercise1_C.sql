SELECT product_category, sum(quantity*price_per_unit) as revenue
FROM ecomerce_data
GROUP BY product_category
Order BY revenue DESC;