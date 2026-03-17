-- Realistic users table with SERIAL and inserts
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  username text UNIQUE,
  email text NOT NULL
);

INSERT INTO users (username, email) VALUES ('alice', 'alice@example.com');
INSERT INTO users (username, email) VALUES ('bob', 'bob@example.com');
