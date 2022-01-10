-- migrate:up
CREATE TABLE extra_video_files (
    video_id BIGINT UNIQUE NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    thumbnail TEXT DEFAULT NULL,
    info_json TEXT DEFAULT NULL,
    other_files JSON DEFAULT NULL
);

CREATE INDEX video_file_id_index ON extra_video_files (video_id);

-- migrate:down
