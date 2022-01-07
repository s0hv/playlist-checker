-- migrate:up
ALTER TABLE videos RENAME download_type TO download_format;
ALTER TABLE videos RENAME download_filename TO downloaded_filename;

ALTER TABLE videos
    ADD COLUMN download BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN force_redownload BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE videos SET download=TRUE WHERE downloaded_filename IS NOT NULL;


-- migrate:down
ALTER TABLE videos RENAME download_format TO download_type;
ALTER TABLE videos RENAME downloaded_filename TO download_filename;

ALTER TABLE videos
    DROP COLUMN download,
    DROP COLUMN force_redownload;
