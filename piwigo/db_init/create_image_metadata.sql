CREATE OR REPLACE VIEW piwigo.image_metadata
AS
SELECT i.id
	, JSON_OBJECT(
		'image_id', i.id
		, 'name', i.name
		, 'comment', i.comment
		, 'author', i.author
		, 'date_creation', i.date_creation
		, 'tags', JSON_QUERY(IF(COUNT(t.id)>0,JSON_ARRAYAGG(t.name),JSON_ARRAY()), '$')
	) image_metadata
FROM piwigo.images i
LEFT JOIN piwigo.image_tag it
ON it.image_id = i.id
LEFT JOIN piwigo.tags t
ON t.id = it.tag_id
GROUP BY i.id
