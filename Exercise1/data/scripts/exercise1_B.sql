SELECT product_category, sum(quantity)
FROM ecomerce_data
GROUP BY product_category
Order BY sum(quantity) DESC
LIMIT 3;