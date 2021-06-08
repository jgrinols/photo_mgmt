DELIMITER $$
CREATE OR REPLACE TRIGGER tr_ins_aft_imagecategory
AFTER INSERT ON piwigo.image_category
FOR EACH ROW
BEGIN
        INSERT INTO piwigo.image_virtual_paths
        (image_id, category_id, physical_path, virtual_path)
        SELECT new.image_id
                , new.category_id
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
        WHERE c.dir IS NULL
                AND ic.image_id = new.image_id
                AND ic.category_id = new.category_id;

        INSERT INTO messaging.pwgo_message (message_type, `message`)
        VALUES ('IMG_METADATA' , JSON_OBJECT(
                'image_id', new.image_id
                , 'table_name', 'image_category'
                , 'table_primary_key', JSON_ARRAY(new.image_id, new.category_id)
                , 'operation', 'INSERT'
        ));

END;
$$

CREATE OR REPLACE TRIGGER tr_aft_del_imagecategory
AFTER DELETE ON piwigo.image_category
FOR EACH ROW
BEGIN
        DELETE
        FROM piwigo.image_virtual_paths
        WHERE image_id = old.image_id
                AND category_id = old.category_id;

        INSERT INTO messaging.pwgo_message (message_type, `message`)
        VALUES ('IMG_METADATA' , JSON_OBJECT(
                'image_id', old.image_id
                , 'table_name', 'image_category'
                , 'table_primary_key', JSON_ARRAY(old.image_id, old.category_id)
                , 'operation', 'DELETE'
        ));

END;
$$

DELIMITER ;
