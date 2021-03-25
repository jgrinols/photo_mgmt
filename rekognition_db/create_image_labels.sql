CREATE TABLE IF NOT EXISTS image_labels
(
	piwigo_image_id MEDIUMINT(8) NOT NULL,
	label VARCHAR(50) NOT NULL,
	confidence FLOAT NOT NULL,
	parents JSON NULL,
	PRIMARY KEY (piwigo_image_id, label)
)
