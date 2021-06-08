DELIMITER $$
CREATE OR REPLACE TRIGGER tr_ins_aft_imagetag
AFTER INSERT ON piwigo.image_tag
FOR EACH ROW
BEGIN
        INSERT INTO messaging.pwgo_message (message_type, `message`)
        VALUES ('IMG_METADATA' , JSON_OBJECT(
                'image_id', new.image_id
                , 'table_name', 'image_tag'
                , 'table_primary_key', JSON_ARRAY(new.image_id, new.tag_id)
                , 'operation', 'INSERT'
        ));

END;
$$

CREATE OR REPLACE TRIGGER tr_aft_del_imagetag
AFTER DELETE ON piwigo.image_tag
FOR EACH ROW
BEGIN
        INSERT INTO messaging.pwgo_message (message_type, `message`)
        VALUES ('IMG_METADATA' , JSON_OBJECT(
                'image_id', old.image_id
                , 'table_name', 'image_tag'
                , 'table_primary_key', JSON_ARRAY(old.image_id, old.tag_id)
                , 'operation', 'DELETE'
        ));

END;
$$

DELIMITER ;
