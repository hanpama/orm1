-- SimpleAuto - auto serial PK with all field attributes
CREATE TABLE simple_auto (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    skip_insert TEXT NOT NULL DEFAULT '',
    skip_update TEXT NOT NULL DEFAULT '',
    auto_generated INTEGER NOT NULL DEFAULT 42,
    nullable TEXT
);

-- SimpleUUID - user-assigned UUID PK with all field attributes
CREATE TABLE simple_uuid (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    skip_insert TEXT NOT NULL DEFAULT '',
    skip_update TEXT NOT NULL DEFAULT '',
    auto_generated INTEGER NOT NULL DEFAULT 42,
    nullable TEXT
);

-- Composite - composite PK with all field attributes
CREATE TABLE composite (
    key1 INTEGER NOT NULL,
    key2 INTEGER NOT NULL,
    name TEXT NOT NULL,
    skip_insert TEXT NOT NULL DEFAULT '',
    skip_update TEXT NOT NULL DEFAULT '',
    auto_generated INTEGER NOT NULL DEFAULT 42,
    nullable TEXT,
    PRIMARY KEY (key1, key2)
);

-- SpecialQuote - natural key with special characters in table/column names
CREATE TABLE "special""quote" (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    "field""quote" TEXT NOT NULL,
    "MixedCase" TEXT NOT NULL,
    skip_insert TEXT NOT NULL DEFAULT '',
    skip_update TEXT NOT NULL DEFAULT '',
    auto_generated INTEGER NOT NULL DEFAULT 42,
    nullable TEXT
);

-- Purchase aggregate root
CREATE TABLE purchase (
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    price REAL NOT NULL
);

CREATE TABLE purchase_line_item (
    id TEXT PRIMARY KEY,
    purchase_id TEXT NOT NULL,
    item_index INTEGER NOT NULL,
    product TEXT,
    quantity INTEGER NOT NULL,
    FOREIGN KEY (purchase_id) REFERENCES purchase(id)
);

CREATE TABLE purchase_billing (
    id TEXT PRIMARY KEY,
    purchase_id TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    amount REAL NOT NULL,
    billing_time INTEGER NOT NULL,
    FOREIGN KEY (purchase_id) REFERENCES purchase(id)
);

CREATE TABLE purchase_billing_attachment (
    id TEXT PRIMARY KEY,
    purchase_billing_id TEXT NOT NULL,
    media_uri TEXT NOT NULL,
    FOREIGN KEY (purchase_billing_id) REFERENCES purchase_billing(id)
);

CREATE TABLE purchase_billing_payment (
    purchase_billing_id TEXT PRIMARY KEY,
    payment_time INTEGER NOT NULL,
    amount REAL NOT NULL,
    FOREIGN KEY (purchase_billing_id) REFERENCES purchase_billing(id)
);

CREATE TABLE purchase_withdrawal (
    id TEXT PRIMARY KEY,
    purchase_id TEXT NOT NULL,
    created_at INTEGER,
    remark TEXT,
    FOREIGN KEY (purchase_id) REFERENCES purchase(id)
);

CREATE TABLE purchase_withdrawal_attachment (
    id TEXT PRIMARY KEY,
    purchase_withdrawal_id TEXT NOT NULL,
    media_uri TEXT NOT NULL,
    FOREIGN KEY (purchase_withdrawal_id) REFERENCES purchase_withdrawal(id)
);

-- Pagination test tables
CREATE TABLE blog_posts (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    rating INTEGER,
    published_at INTEGER
);

CREATE TABLE blog_posts_composite (
    key1 INTEGER NOT NULL,
    key2 INTEGER NOT NULL,
    title TEXT NOT NULL,
    rating INTEGER,
    published_at INTEGER,
    PRIMARY KEY (key1, key2)
);

CREATE TABLE blog_posts_agg (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    rating INTEGER,
    published_at INTEGER
);

CREATE TABLE blog_post_comments_agg (
    id INTEGER PRIMARY KEY,
    post_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at INTEGER,
    FOREIGN KEY (post_id) REFERENCES blog_posts_agg(id)
);