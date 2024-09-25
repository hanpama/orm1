CREATE TABLE blog_post (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    rating INT,
    published_at TIMESTAMPTZ
);
