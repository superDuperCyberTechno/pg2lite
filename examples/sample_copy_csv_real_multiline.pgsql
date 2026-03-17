-- CSV COPY with a real multiline quoted field
CREATE TABLE mproducts (
  id integer,
  name text,
  descr text
);

COPY mproducts (id, name, descr) FROM stdin WITH CSV;
1,"Widget","First line
Second line"
2,"Single","One line"
\.
