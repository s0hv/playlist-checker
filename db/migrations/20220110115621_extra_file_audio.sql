-- migrate:up
ALTER TABLE extra_video_files ADD COLUMN audio_file TEXT DEFAULT NULL;

-- migrate:down

