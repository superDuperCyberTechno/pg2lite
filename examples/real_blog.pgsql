-- Posts and comments with BYTEA and boolean
CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  title text,
  body text,
  published boolean DEFAULT false
);

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  post_id integer REFERENCES posts(id),
  content text,
  attachment bytea
);

INSERT INTO posts (title, body, published) VALUES ('Hello', 'First post', true);
INSERT INTO comments (post_id, content, attachment) VALUES (1, 'Nice', '\\xDEADBEEF');
