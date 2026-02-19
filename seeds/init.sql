CREATE TABLE properties (
  id BIGINT PRIMARY KEY,
  price DECIMAL NOT NULL,
  bedrooms INTEGER,
  bathrooms INTEGER,
  region_origin VARCHAR(2) NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE idempotency_keys (
  request_id VARCHAR(255) PRIMARY KEY,
  created_at TIMESTAMP DEFAULT NOW()
);

-- Seed 1000 rows
INSERT INTO properties (id, price, bedrooms, bathrooms, region_origin)
SELECT i, (100000 + i), 3, 2,
CASE WHEN i % 2 = 0 THEN 'us' ELSE 'eu' END
FROM generate_series(1,1000) AS s(i);
