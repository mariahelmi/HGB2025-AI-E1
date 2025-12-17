SELECT customer_name, sum(quantity*price_per_unit) as revenue
FROM ecomerce_data
GROUP BY customer_name
Order BY revenue DESC
LIMIT 5;