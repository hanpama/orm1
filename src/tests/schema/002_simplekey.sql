CREATE TABLE purchase (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code VARCHAR(64) NOT NULL,
    user_id UUID,
    created_at TIMESTAMP DEFAULT NOW(),
    receipt JSONB
);

CREATE TABLE purchase_line_item (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    purchase_id UUID NOT NULL REFERENCES purchase(id),
    product_id UUID,
    quantity INT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE purchase_bank_transfer (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    purchase_id UUID NOT NULL REFERENCES purchase(id),
    sender_name VARCHAR(64) NOT NULL,
    transfer_time TIMESTAMP NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE purchase_bank_transfer_attachment (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    purchase_bank_transfer_id UUID NOT NULL REFERENCES purchase_bank_transfer(id),
    media_uri TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE purchase_coupon_usage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    purchase_id UUID NOT NULL REFERENCES purchase(id),
    coupon_id UUID NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (purchase_id)
);
