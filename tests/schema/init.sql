CREATE EXTENSION "uuid-ossp";

CREATE TABLE IF NOT EXISTS purchase (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code VARCHAR(64) NOT NULL,
    user_id UUID,
    created_at TIMESTAMP DEFAULT NOW(),
    receipt JSONB
);

CREATE TABLE IF NOT EXISTS purchase_line_item (
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


CREATE TABLE course (
    semester_id VARCHAR(20),
    subject_id VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    PRIMARY KEY(semester_id, subject_id)
);

CREATE TABLE course_attachment (
    id VARCHAR(20) PRIMARY KEY,
    course_semester_id VARCHAR(20) NOT NULL,
    course_subject_id VARCHAR(20) NOT NULL,
    media_uri TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (course_semester_id, course_subject_id) REFERENCES course(semester_id, subject_id)
);

CREATE TABLE course_module (
    id VARCHAR(20) PRIMARY KEY,
    course_semester_id VARCHAR(20) NOT NULL,
    course_subject_id VARCHAR(20) NOT NULL,

    title VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),

    FOREIGN KEY (course_semester_id, course_subject_id) REFERENCES course(semester_id, subject_id)
);

CREATE TABLE course_module_material (
    id VARCHAR(20) PRIMARY KEY,
    course_module_id VARCHAR(20) NOT NULL REFERENCES course_module(id),
    media_uri TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);