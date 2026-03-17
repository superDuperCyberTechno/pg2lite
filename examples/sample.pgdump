-- Example PostgreSQL dump
SET statement_timeout = 0;
-- Create users table with SERIAL and boolean default
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name text NOT NULL,
    active boolean DEFAULT true
);

-- Insert some rows
INSERT INTO users (name, active) VALUES ('Alice', true);
INSERT INTO users (name, active) VALUES (E'Bob\\n', false);
