-- \i 'data/scripts/exercise1.sql'
DROP TABLE IF EXISTS ecomerce_data;

CREATE TABLE ecomerce_data (
  id SERIAL PRIMARY KEY,
  customer_name TEXT,
  product_category TEXT,
  quantity INTEGER,
  price_per_unit MONEY,
  order_date DATE,
  country TEXT
);

\COPY ecomerce_data(customer_name,product_category,quantity,price_per_unit,order_date,country) FROM '/data/orders_1M.csv' DELIMITER ',' CSV HEADER;