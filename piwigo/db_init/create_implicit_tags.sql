DROP TABLE IF EXISTS piwigo.implicit_tags;

CREATE TABLE piwigo.implicit_tags
(
	implied_tag_id SMALLINT NOT NULL,
	triggered_by_tag_id SMALLINT NOT NULL,
	PRIMARY KEY (implied_tag_id, triggered_by_tag_id)
);

SELECT @family := id FROM piwigo.tags WHERE `name` = 'family';
SELECT @kids := id FROM piwigo.tags WHERE `name` = 'kids';
SELECT @henry := id FROM piwigo.tags WHERE `name` = 'henry';
SELECT @hannah := id FROM piwigo.tags WHERE `name` = 'hannah';
SELECT @chelsea := id FROM piwigo.tags WHERE `name` = 'chelsea';
SELECT @justin := id FROM piwigo.tags WHERE `name` = 'justin';
SELECT @kitty := id FROM piwigo.tags WHERE `name` = 'kitty';
SELECT @holidays := id FROM piwigo.tags WHERE `name` = 'holidays';
SELECT @christmas := id FROM piwigo.tags WHERE `name` = 'christmas';
SELECT @thanksgiving := id FROM piwigo.tags WHERE `name` = 'thanksgiving';
SELECT @halloween := id FROM piwigo.tags WHERE `name` = 'halloween';

TRUNCATE TABLE piwigo.implicit_tags;

INSERT INTO piwigo.implicit_tags ( implied_tag_id, triggered_by_tag_id )
VALUES
( @kids, @hannah ),
( @kids, @henry ),
( @family, @kids ),
( @family, @chelsea ),
( @family, @justin ),
( @family, @kitty ),
( @holidays, @christmas ),
( @holidays, @thanksgiving ),
( @holidays, @halloween )
;

INSERT INTO piwigo.image_tag ( image_id, tag_id )
SELECT DISTINCT it.image_id, imp.implied_tag_id 
FROM piwigo.image_tag it
JOIN piwigo.implicit_tags imp
ON imp.triggered_by_tag_id = it.tag_id
LEFT JOIN piwigo.image_tag it2
ON it2.image_id = it.image_id AND it2.tag_id = imp.implied_tag_id
WHERE it2.image_id IS NULL;
