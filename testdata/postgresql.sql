-- Create test schema
CREATE SCHEMA IF NOT EXISTS orm1_test;

-- Drop existing tables in reverse dependency order
DROP TABLE IF EXISTS orm1_test.blog_post_comments_agg CASCADE;
DROP TABLE IF EXISTS orm1_test.blog_posts_agg CASCADE;
DROP TABLE IF EXISTS orm1_test.blog_posts_composite CASCADE;
DROP TABLE IF EXISTS orm1_test.blog_posts CASCADE;
DROP TABLE IF EXISTS orm1_test.purchase_withdrawal_attachment CASCADE;
DROP TABLE IF EXISTS orm1_test.purchase_billing_payment CASCADE;
DROP TABLE IF EXISTS orm1_test.purchase_billing_attachment CASCADE;
DROP TABLE IF EXISTS orm1_test.purchase_billing CASCADE;
DROP TABLE IF EXISTS orm1_test.purchase_withdrawal CASCADE;
DROP TABLE IF EXISTS orm1_test.purchase_line_item CASCADE;
DROP TABLE IF EXISTS orm1_test.purchase CASCADE;
DROP TABLE IF EXISTS orm1_test."special""quote" CASCADE;
DROP TABLE IF EXISTS orm1_test.composite CASCADE;
DROP TABLE IF EXISTS orm1_test.simple_uuid CASCADE;
DROP TABLE IF EXISTS orm1_test.simple_auto CASCADE;

-- SimpleAuto - auto serial PK with all field attributes
CREATE TABLE orm1_test.simple_auto (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    skip_insert TEXT NOT NULL DEFAULT '',
    skip_update TEXT NOT NULL DEFAULT '',
    auto_generated BIGINT NOT NULL DEFAULT 42,
    nullable TEXT
);

-- SimpleUUID - user-assigned UUID PK with all field attributes
CREATE TABLE orm1_test.simple_uuid (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    skip_insert TEXT NOT NULL DEFAULT '',
    skip_update TEXT NOT NULL DEFAULT '',
    auto_generated BIGINT NOT NULL DEFAULT 42,
    nullable TEXT
);

-- Composite - composite PK with all field attributes
CREATE TABLE orm1_test.composite (
    key1 INTEGER NOT NULL,
    key2 INTEGER NOT NULL,
    name TEXT NOT NULL,
    skip_insert TEXT NOT NULL DEFAULT '',
    skip_update TEXT NOT NULL DEFAULT '',
    auto_generated BIGINT NOT NULL DEFAULT 42,
    nullable TEXT,
    PRIMARY KEY (key1, key2)
);

-- SpecialQuote - natural key with special characters in table/column names
CREATE TABLE orm1_test."special""quote" (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    "field""quote" TEXT NOT NULL,
    "MixedCase" TEXT NOT NULL,
    skip_insert TEXT NOT NULL DEFAULT '',
    skip_update TEXT NOT NULL DEFAULT '',
    auto_generated BIGINT NOT NULL DEFAULT 42,
    nullable TEXT
);

-- Purchase aggregate root
CREATE TABLE orm1_test.purchase (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    price NUMERIC NOT NULL
);

CREATE TABLE orm1_test.purchase_line_item (
    id TEXT PRIMARY KEY,
    purchase_id TEXT NOT NULL,
    item_index BIGINT NOT NULL,
    product TEXT,
    quantity BIGINT NOT NULL,
    FOREIGN KEY (purchase_id) REFERENCES orm1_test.purchase(id)
);

CREATE TABLE orm1_test.purchase_billing (
    id TEXT PRIMARY KEY,
    purchase_id TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    billing_time BIGINT NOT NULL,
    FOREIGN KEY (purchase_id) REFERENCES orm1_test.purchase(id)
);

CREATE TABLE orm1_test.purchase_billing_attachment (
    id TEXT PRIMARY KEY,
    purchase_billing_id TEXT NOT NULL,
    media_uri TEXT NOT NULL,
    FOREIGN KEY (purchase_billing_id) REFERENCES orm1_test.purchase_billing(id)
);

CREATE TABLE orm1_test.purchase_billing_payment (
    purchase_billing_id TEXT PRIMARY KEY,
    payment_time BIGINT NOT NULL,
    amount NUMERIC NOT NULL,
    FOREIGN KEY (purchase_billing_id) REFERENCES orm1_test.purchase_billing(id)
);

CREATE TABLE orm1_test.purchase_withdrawal (
    id TEXT PRIMARY KEY,
    purchase_id TEXT NOT NULL,
    created_at BIGINT,
    remark TEXT,
    FOREIGN KEY (purchase_id) REFERENCES orm1_test.purchase(id)
);

CREATE TABLE orm1_test.purchase_withdrawal_attachment (
    id TEXT PRIMARY KEY,
    purchase_withdrawal_id TEXT NOT NULL,
    media_uri TEXT NOT NULL,
    FOREIGN KEY (purchase_withdrawal_id) REFERENCES orm1_test.purchase_withdrawal(id)
);

-- Pagination test tables
CREATE TABLE orm1_test.blog_posts (
    id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    rating INTEGER,
    published_at BIGINT
);

CREATE TABLE orm1_test.blog_posts_composite (
    key1 BIGINT NOT NULL,
    key2 BIGINT NOT NULL,
    title TEXT NOT NULL,
    rating INTEGER,
    published_at BIGINT,
    PRIMARY KEY (key1, key2)
);

CREATE TABLE orm1_test.blog_posts_agg (
    id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    rating INTEGER,
    published_at BIGINT
);

CREATE TABLE orm1_test.blog_post_comments_agg (
    id BIGINT PRIMARY KEY,
    post_id BIGINT NOT NULL,
    content TEXT NOT NULL,
    created_at BIGINT,
    FOREIGN KEY (post_id) REFERENCES orm1_test.blog_posts_agg(id)
);
