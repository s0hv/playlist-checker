-- migrate:up
ALTER TABLE extra_video_files ADD COLUMN subtitles TEXT[] DEFAULT NULL;

UPDATE extra_video_files
SET subtitles =
        CASE WHEN other_files -> 'subtitles' IS NULL
            THEN NULL
            ELSE ARRAY(SELECT json_array_elements_text(other_files -> 'subtitles'))
        END,
    other_files =
        CASE WHEN (other_files::jsonb - 'subtitles')::text = '{}'
            THEN NULL
            ELSE (other_files::jsonb - 'subtitles')::json
        END
WHERE other_files IS NOT NULL;

-- migrate:down

