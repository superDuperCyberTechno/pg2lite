-- Sequence owned by table column example
CREATE SEQUENCE seq_users_id START WITH 100;

CREATE TABLE users_seq (
  id integer DEFAULT nextval('seq_users_id'::regclass) PRIMARY KEY,
  name text
);

ALTER SEQUENCE seq_users_id OWNED BY users_seq.id;
SELECT pg_catalog.setval('seq_users_id', 150, true);

INSERT INTO users_seq (id, name) VALUES (1, 'first');
INSERT INTO users_seq (id, name) VALUES (151, 'second');
