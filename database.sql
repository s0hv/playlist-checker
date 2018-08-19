CREATE TABLE `videos` (
    `id` MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `video_id` VARCHAR(30) COLLATE utf8mb4_unicode_ci NOT NULL,
    `title` TEXT COLLATE utf8mb4_unicode_ci NOT NULL,
    `description` TEXT COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `published_at` TIMESTAMP NOT NULL,
    `deleted` BOOL DEFAULT FALSE,
    `deleted_at` TIMESTAMP NULL DEFAULT NULL,
    `site` TINYINT UNSIGNED NOT NULL,

    PRIMARY KEY (`id`),
    KEY (`site`),
    UNIQUE KEY (`video_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `channels` (
    `id` MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `channel_id` VARCHAR(40) NOT NULL,
    `name` VARCHAR(255) NOT NULL,

    PRIMARY KEY (`id`),
    UNIQUE KEY (`channel_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `channelVideos` (
    `video_id` MEDIUMINT UNSIGNED NOT NULL,
    `channel_id` MEDIUMINT UNSIGNED NOT NULL,

    FOREIGN KEY (`video_id`) REFERENCES `videos` (`id`),
    FOREIGN KEY (`channel_id`) REFERENCES `channels` (`id`)
) ENGINE=InnoDB;



CREATE TABLE `tags` (
    `id` MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `tag` VARCHAR(190) COLLATE utf8_bin,

    PRIMARY KEY (`id`),
    UNIQUE KEY (`tag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLlATE=utf8mb4_bin;


CREATE TRIGGER lcase_insert BEFORE INSERT ON `tags` FOR EACH ROW SET NEW.tag = LOWER(NEW.tag);


CREATE TABLE `videoTags` (
    `video_id` MEDIUMINT UNSIGNED NOT NULL,
    `tag_id` MEDIUMINT UNSIGNED NOT NULL,

    FOREIGN KEY (`video_id`) REFERENCES `videos` (`id`) ON DELETE CASCADE,
    FOREIGN KEY (`tag_id`) REFERENCES `tags` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB;


CREATE TABLE `playlists` (
    `id` MEDIUMINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `name` TEXT COLLATE utf8mb4_unicode_ci NOT NULL,
    `playlist_id` VARCHAR(40) COLLATE utf8mb4_unicode_ci NOT NULL,
    `site` TINYINT UNSIGNED NOT NULL,

    PRIMARY KEY (`id`),
    KEY (`site`),
    UNIQUE KEY (`playlist_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLlATE=utf8mb4_unicode_ci;


CREATE TABLE `playlistVideos` (
    `playlist_id` MEDIUMINT UNSIGNED NOT NULL,
    `video_id` MEDIUMINT UNSIGNED NOT NULL,

    FOREIGN KEY (`playlist_id`) REFERENCES `playlists` (`id`) ON DELETE CASCADE,
    FOREIGN KEY (`video_id`) REFERENCES `videos` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB;
