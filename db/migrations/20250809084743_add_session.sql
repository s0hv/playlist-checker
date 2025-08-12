-- migrate:up
CREATE TABLE session (
  id TEXT PRIMARY KEY NOT NULL,
  secret_hash TEXT NOT NULL,
  created_at INT NOT NULL
);

-- migrate:down
DROP TABLE session;
