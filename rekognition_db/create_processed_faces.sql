CREATE TABLE IF NOT EXISTS rekognition.processed_faces
(
	piwigo_image_id MEDIUMINT(8) NOT NULL,
	face_index TINYINT NOT NULL,
	face_details JSON NOT NULL,
	matched_to_face_id CHAR(36) NULL,
	PRIMARY KEY (piwigo_image_id, face_index),
	FOREIGN KEY (matched_to_face_id) REFERENCES rekognition.indexed_faces(face_id)
)
