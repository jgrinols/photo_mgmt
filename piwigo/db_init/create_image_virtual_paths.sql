DROP TABLE IF EXISTS piwigo.image_virtual_paths;

CREATE TABLE piwigo.image_virtual_paths
(
        image_id MEDIUMINT(8) UNSIGNED NOT NULL,
        category_id SMALLINT(5) UNSIGNED NOT NULL,
        physical_path VARCHAR(255) NOT NULL,
        virtual_path VARCHAR(255) NOT NULL,
        PRIMARY KEY (image_id, category_id)
);

INSERT INTO piwigo.image_virtual_paths
(image_id, category_id, physical_path, virtual_path)
SELECT ic.image_id
        , ic.category_id
        , CONCAT(pcp.cpath, '/', i.file)
        , CONCAT(vcp.cpath, '/', i.id, '_', i.file)
FROM piwigo.image_category ic
JOIN piwigo.images i
ON i.id = ic.image_id
JOIN piwigo.categories c
ON c.id = ic.category_id
JOIN piwigo.category_paths pcp
ON pcp.cat_id = i.storage_category_id
JOIN piwigo.category_paths vcp
ON vcp.cat_id = ic.category_id
WHERE c.dir IS NULL;
