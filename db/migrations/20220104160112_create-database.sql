-- migrate:up
CREATE TABLE channels
(
    id         BIGSERIAL PRIMARY KEY,
    channel_id TEXT      NOT NULL,
    name       TEXT      NOT NULL,
    thumbnail  TEXT,
    site       INTEGER DEFAULT 0 NOT NULL
);

CREATE UNIQUE INDEX channels_channel_id_site_uindex
    ON channels (channel_id, site);

CREATE TABLE playlists
(
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    playlist_id TEXT UNIQUE NOT NULL,
    site        SMALLINT NOT NULL
);

CREATE INDEX playlist_site
    ON playlists (site);

CREATE TABLE tags
(
    id  BIGSERIAL PRIMARY KEY,
    tag TEXT UNIQUE NOT NULL
);

CREATE TABLE videos
(
    id                BIGSERIAL PRIMARY KEY,
    video_id          TEXT NOT NULL,
    title             TEXT NOT NULL,
    description       TEXT,
    published_at      TIMESTAMP WITH TIME ZONE,
    deleted           BOOLEAN DEFAULT FALSE,
    deleted_at        TIMESTAMP,
    site              SMALLINT NOT NULL,
    alternative       TEXT,
    thumbnail         TEXT,
    download_type     TEXT,
    download_filename TEXT,
    downloaded_format TEXT
);
CREATE UNIQUE INDEX video_id_unique ON videos(video_id, site);
CREATE INDEX title_search_index ON videos USING GIN(TO_TSVECTOR('english'::regconfig, title));

CREATE TABLE channelvideos
(
    video_id   BIGINT NOT NULL REFERENCES videos(id),
    channel_id BIGINT NOT NULL REFERENCES channels(id),
    PRIMARY KEY (video_id, channel_id)
);

CREATE INDEX channelvideos_video_id ON channelvideos (video_id);

CREATE TABLE playlistvideos
(
    playlist_id BIGINT NOT NULL
        REFERENCES playlists(id)
            ON UPDATE CASCADE
            ON DELETE CASCADE,

    video_id    BIGINT NOT NULL
        REFERENCES videos(id)
            ON UPDATE CASCADE
            ON DELETE CASCADE,

    PRIMARY KEY (playlist_id, video_id)
);

CREATE INDEX playlist_video_id ON playlistvideos (video_id);

CREATE TABLE videotags
(
    video_id BIGINT NOT NULL
        REFERENCES videos(id)
            ON UPDATE CASCADE
            ON DELETE CASCADE,

    tag_id   BIGINT NOT NULL
        REFERENCES tags(id)
            ON UPDATE CASCADE
            ON DELETE CASCADE,

    PRIMARY KEY (video_id, tag_id)
);
CREATE INDEX video_tag_id ON videotags (tag_id);

-- migrate:down

