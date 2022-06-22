-- migrate:up
ALTER TABLE videos
    ADD container_override TEXT DEFAULT NULL;
-- migrate:down

