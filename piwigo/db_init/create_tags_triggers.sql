DELIMITER $$
CREATE OR REPLACE TRIGGER tr_ins_aft_tags
AFTER INSERT ON piwigo.tags
FOR EACH ROW
BEGIN
    
    INSERT INTO messaging.pwgo_message (message_type, `message`)
    VALUES ('TAGS' , JSON_OBJECT(
            'tag_id', new.id
            , 'table_name', 'tags'
            , 'table_primary_key', JSON_ARRAY(new.id)
            , 'operation', 'INSERT'
    ));

END;
$$

CREATE OR REPLACE TRIGGER tr_upd_aft_tags
AFTER UPDATE ON piwigo.tags
FOR EACH ROW
BEGIN
    
    IF new.name != old.name THEN
        INSERT INTO messaging.pwgo_message (message_type, `message`)
        VALUES ('TAGS' , JSON_OBJECT(
                'tag_id', new.id
                , 'table_name', 'tags'
                , 'table_primary_key', JSON_ARRAY(new.id)
                , 'operation', 'UPDATE'
                , 'before', JSON_OBJECT(
                    'name', old.name
                )
                , 'after', JSON_OBJECT(
                    'name', new.name
                )
        ));
    END IF;
END;
$$

CREATE OR REPLACE TRIGGER tr_del_aft_tags
AFTER DELETE ON piwigo.tags
FOR EACH ROW
BEGIN
        
    INSERT INTO messaging.pwgo_message (message_type, `message`)
    VALUES ('TAGS' , JSON_OBJECT(
            'tag_id', old.id
            , 'table_name', 'tags'
            , 'table_primary_key', JSON_ARRAY(old.id)
            , 'operation', 'DELETE'
    ));

END;
$$

DELIMITER ;
