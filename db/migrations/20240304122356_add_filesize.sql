-- migrate:up
ALTER TABLE videos ADD COLUMN filesize BIGINT DEFAULT NULL;

ALTER TABLE extra_video_files ADD COLUMN total_filesize BIGINT DEFAULT NULL;
-- migrate:down

