DROP TABLE IF EXISTS piwigo.implicit_tags;

CREATE TABLE piwigo.implicit_tags
(
	implied_tag_id SMALLINT NOT NULL,
	triggered_by_tag_id SMALLINT NOT NULL,
	PRIMARY KEY (implied_tag_id, triggered_by_tag_id)
);

CREATE OR REPLACE VIEW piwigo.expanded_implicit_tags
AS
WITH RECURSIVE expanded_imp_tags AS
(
	SELECT implied_tag_id
		, triggered_by_tag_id
		, triggered_by_tag_id AS org_triggered_by_tag_id
		, 1 AS rnk
	FROM piwigo.implicit_tags it
	UNION ALL
	SELECT it2.implied_tag_id
		, it2.triggered_by_tag_id
		, eit.org_triggered_by_tag_id
		, rnk + 1 AS rnk
	FROM expanded_imp_tags eit
	JOIN piwigo.implicit_tags it2
	ON it2.triggered_by_tag_id = eit.implied_tag_id	
), ranked AS 
(
	SELECT implied_tag_id
		, triggered_by_tag_id
		, org_triggered_by_tag_id
		, RANK() OVER(PARTITION BY implied_tag_id, triggered_by_tag_id ORDER BY rnk DESC) AS irnk
	FROM expanded_imp_tags
)
SELECT implied_tag_id
	, org_triggered_by_tag_id AS triggered_by_tag_id
FROM ranked
WHERE irnk = 1;

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
