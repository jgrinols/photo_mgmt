CREATE OR REPLACE VIEW piwigo.category_paths
AS
WITH RECURSIVE cat_paths AS
(
        SELECT id
                , 1 AS pos
                , CAST(uppercats AS VARCHAR(100)) AS cats
                , CAST(SUBSTRING_INDEX(uppercats, ',', 1) AS VARCHAR(100)) AS first_cat
        FROM piwigo.categories
        UNION ALL
        SELECT id
                , pos + 1
                , SUBSTRING(cats, CHAR_LENGTH(first_cat) + 2)
                , SUBSTRING_INDEX(SUBSTRING(cats, CHAR_LENGTH(first_cat) + 2), ',', 1)
        FROM cat_paths
        WHERE CHAR_LENGTH(cats) > CHAR_LENGTH(first_cat)
)
SELECT cp.id cat_id
        , CAST(CONCAT('./', GROUP_CONCAT(c.name ORDER BY cp.pos SEPARATOR'/')) AS VARCHAR(255)) cpath
FROM cat_paths cp
JOIN piwigo.categories c
ON c.id = CAST(cp.first_cat AS INT)
GROUP BY cp.id;
