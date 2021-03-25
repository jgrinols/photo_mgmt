CREATE TABLE IF NOT EXISTS indexed_faces
(
	face_id CHAR(36) NOT NULL,
	image_id CHAR(36) NOT NULL,
	piwigo_image_id MEDIUMINT(8) UNSIGNED NOT NULL,
	piwigo_category_id SMALLINT(5) UNSIGNED NOT NULL,
	face_confidence FLOAT(8,5) NOT NULL,
	face_details JSON NOT NULL,
	PRIMARY KEY (face_id),
	UNIQUE (piwigo_image_id)
);
