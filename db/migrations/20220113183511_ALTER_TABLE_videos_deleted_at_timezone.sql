-- migrate:up
ALTER TABLE videos
    ALTER COLUMN deleted_at TYPE TIMESTAMP WITH TIME ZONE USING deleted_at::TIMESTAMP WITH TIME ZONE;

-- migrate:down

