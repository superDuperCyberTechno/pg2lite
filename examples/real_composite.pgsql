-- Composite PK and unique constraint
CREATE TABLE project_members (
  project_id integer,
  user_id integer,
  role text,
  PRIMARY KEY (project_id, user_id),
  UNIQUE (project_id, role)
);

INSERT INTO project_members (project_id, user_id, role) VALUES (1, 10, 'owner');
INSERT INTO project_members (project_id, user_id, role) VALUES (1, 11, 'maintainer');
