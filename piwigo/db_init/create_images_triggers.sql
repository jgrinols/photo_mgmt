DELIMITER $$
CREATE OR REPLACE TRIGGER tr_ins_aft_images
AFTER INSERT ON piwigo.images
FOR EACH ROW
BEGIN
    
    INSERT INTO messaging.pwgo_message (message_type, `message`)
    VALUES ('IMG_METADATA' , JSON_OBJECT(
            'image_id', new.id
            , 'table_name', 'images'
            , 'table_primary_key', JSON_ARRAY(new.id)
            , 'operation', 'INSERT'
    ));

END;$$

DELIMITER $$
CREATE OR REPLACE TRIGGER tr_upd_aft_images
AFTER UPDATE ON piwigo.images
FOR EACH ROW
BEGIN
    
    IF (new.name != old.name OR new.comment != old.comment OR new.author != old.author OR new.date_creation != old.date_creation) THEN
        INSERT INTO messaging.pwgo_message (message_type, `message`)
        VALUES ('IMG_METADATA' , JSON_OBJECT(
                'image_id', new.id
                , 'table_name', 'images'
                , 'table_primary_key', JSON_ARRAY(new.id)
                , 'operation', 'UPDATE'
                , 'before', JSON_OBJECT(
                    'name', old.name
                    , 'comment', old.comment
                    , 'author', old.author
                    , 'date_creation', old.date_creation
                )
                , 'after', JSON_OBJECT(
                    'name', new.name
                    , 'comment', new.comment
                    , 'author', new.author
                    , 'date_creation', new.date_creation
                )
        ));
    END IF;
END;$$

CREATE OR REPLACE TRIGGER tr_del_aft_images
AFTER DELETE ON piwigo.images
FOR EACH ROW
BEGIN
        
    INSERT INTO messaging.pwgo_message (message_type, `message`)
    VALUES ('IMG_METADATA' , JSON_OBJECT(
            'image_id', old.id
            , 'table_name', 'images'
            , 'table_primary_key', JSON_ARRAY(old.id)
            , 'operation', 'DELETE'
    ));

END;$$

DELIMITER ;
