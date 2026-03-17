-- Example CSV COPY with quotes, commas and newlines
CREATE TABLE csv_products (
  id integer,
  name text,
  descr text
);

COPY csv_products (id, name, descr) FROM stdin WITH CSV;
1,"Widget","A widget, with comma"
2,"Gadget","Multi-line\nDescription"
3,"Quoted","Contains ""double quotes"" inside"
\.
