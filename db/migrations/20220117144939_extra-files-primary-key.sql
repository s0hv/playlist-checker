-- migrate:up
ALTER TABLE extra_video_files
    DROP CONSTRAINT extra_video_files_video_id_key;

ALTER TABLE extra_video_files
    ADD CONSTRAINT extra_video_files_pk
        PRIMARY KEY (video_id);


-- migrate:down

