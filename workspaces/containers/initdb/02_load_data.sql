-- Maven Fuzzy Factory — carga de datos desde CSV
-- Los archivos CSV deben estar montados en /csv dentro del contenedor.
-- Ver volumen csv_data en docker-compose.yaml.

USE maven_fuzzy_factory;

LOAD DATA INFILE '/csv/products.csv'
INTO TABLE products
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, created_at, product_name);

LOAD DATA INFILE '/csv/website_sessions.csv'
INTO TABLE website_sessions
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(website_session_id, created_at, user_id, is_repeat_session,
 utm_source, utm_campaign, utm_content, device_type, http_referer);

LOAD DATA INFILE '/csv/orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, created_at, website_session_id, user_id,
 primary_product_id, items_purchased, price_usd, cogs_usd);

LOAD DATA INFILE '/csv/order_items.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_item_id, created_at, order_id, product_id,
 is_primary_item, price_usd, cogs_usd);

LOAD DATA INFILE '/csv/order_item_refunds.csv'
INTO TABLE order_item_refunds
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_item_refund_id, created_at, order_item_id, order_id, refund_amount_usd);

LOAD DATA INFILE '/csv/website_pageviews.csv'
INTO TABLE website_pageviews
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(website_pageview_id, created_at, website_session_id, pageview_url);
