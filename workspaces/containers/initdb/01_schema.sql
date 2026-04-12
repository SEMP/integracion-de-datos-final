-- Maven Fuzzy Factory — schema
-- Ejecutado automáticamente por MySQL al primer arranque del contenedor.

USE maven_fuzzy_factory;

CREATE TABLE IF NOT EXISTS products (
    product_id   INT            NOT NULL,
    created_at   DATETIME       NOT NULL,
    product_name VARCHAR(45)    NOT NULL,
    PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS website_sessions (
    website_session_id BIGINT       NOT NULL,
    created_at         DATETIME     NOT NULL,
    user_id            BIGINT       NOT NULL,
    is_repeat_session  BINARY(1)    NOT NULL,
    utm_source         VARCHAR(45)  NULL,
    utm_campaign       VARCHAR(45)  NULL,
    utm_content        VARCHAR(45)  NULL,
    device_type        VARCHAR(45)  NOT NULL,
    http_referer       VARCHAR(45)  NULL,
    PRIMARY KEY (website_session_id)
);

CREATE TABLE IF NOT EXISTS website_pageviews (
    website_pageview_id BIGINT      NOT NULL,
    created_at          DATETIME    NOT NULL,
    website_session_id  BIGINT      NOT NULL,
    pageview_url        VARCHAR(45) NOT NULL,
    PRIMARY KEY (website_pageview_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id           BIGINT          NOT NULL,
    created_at         DATETIME        NOT NULL,
    website_session_id BIGINT          NOT NULL,
    user_id            BIGINT          NOT NULL,
    primary_product_id INT             NOT NULL,
    items_purchased    INT             NOT NULL,
    price_usd          DECIMAL(6,2)    NOT NULL,
    cogs_usd           DECIMAL(6,2)    NOT NULL,
    PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id  BIGINT          NOT NULL,
    created_at     DATETIME        NOT NULL,
    order_id       BIGINT          NOT NULL,
    product_id     INT             NOT NULL,
    is_primary_item BINARY(1)      NOT NULL,
    price_usd      DECIMAL(6,2)    NOT NULL,
    cogs_usd       DECIMAL(6,2)    NOT NULL,
    PRIMARY KEY (order_item_id)
);

CREATE TABLE IF NOT EXISTS order_item_refunds (
    order_item_refund_id BIGINT          NOT NULL,
    created_at           DATETIME        NOT NULL,
    order_item_id        BIGINT          NOT NULL,
    order_id             BIGINT          NOT NULL,
    refund_amount_usd    DECIMAL(6,2)    NOT NULL,
    PRIMARY KEY (order_item_refund_id)
);
