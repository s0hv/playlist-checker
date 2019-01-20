-- https://stackoverflow.com/a/1841399/60467
DROP TABLE newIDs;
CREATE TEMPORARY TABLE newIDs (
  ID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  parentID BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (`ID`)
);


-- videos
INSERT INTO newIDs (parentID) SELECT id FROM videos ORDER BY id ASC;
SET foreign_key_checks = 0;

UPDATE
	videos, channelVideos, newIDs
SET
	channelVideos.video_id = newIDs.ID
WHERE
	videos.id = newIDs.parentID AND
	channelVideos.video_id = newIDs.parentID;

UPDATE
	videos, newIDs, videoTags
SET
	videoTags.video_id = newIDs.ID
WHERE
	videos.id = newIDs.parentID AND
	videoTags.video_id = newIDs.parentID;

UPDATE
	videos, newIDs, playlistVideos
SET
	videos.id = newIDs.ID,
	playlistVideos.video_id = newIDs.ID
WHERE
	videos.id = newIDs.parentID AND
	playlistVideos.video_id = newIDs.parentID;


SET @s=CONCAT('ALTER TABLE videos AUTO_INCREMENT = ', (SELECT max(ID) from newIDs), ';');
PREPARE stmt1 FROM @s;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
TRUNCATE TABLE newIDs;


-- tags
INSERT INTO newIDs (parentID) SELECT id FROM tags ORDER BY id ASC;

UPDATE
	tags, newIDs, videoTags
SET
	tags.id = newIDs.ID,
	videoTags.tag_id = newIDs.ID
WHERE
	tags.id = newIDs.parentID AND
	videoTags.tag_id = newIDs.parentID;

SET @s=CONCAT('ALTER TABLE tags AUTO_INCREMENT = ', (SELECT max(ID) from newIDs), ';');
PREPARE stmt1 FROM @s;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
TRUNCATE TABLE newIDs;


-- channels
INSERT INTO newIDs (parentID) SELECT id FROM channels ORDER BY id ASC;

UPDATE
	channels, newIDs, channelVideos
SET
	channels.id = newIDs.ID,
	channelVideos.channel_id = newIDs.ID
WHERE
	channels.id = newIDs.parentID AND
	channelVideos.channel_id = newIDs.parentID;

SET @s=CONCAT('ALTER TABLE channels AUTO_INCREMENT = ', (SELECT max(ID) from newIDs), ';');
PREPARE stmt1 FROM @s;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;
DROP TABLE newIDs;
SET foreign_key_checks = 1;