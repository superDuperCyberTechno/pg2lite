-- Example with COPY
CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  name text,
  price numeric
);

COPY products (id, name, price) FROM stdin;
1	Widget	9.99
2	Gadget	19.95
\.
