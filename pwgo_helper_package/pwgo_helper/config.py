"""container module for Configuration"""
from __future__ import annotations

import logging, json
from typing import Optional

import click

from . import strings
# pylint: disable=reimported
from . import logging as pwgo_logging

class Configuration:
    """holds global config values"""
    instance: Configuration = None

    @staticmethod
    def get() -> Configuration:
        """returns the Configuration singleton"""
        if not Configuration.instance:
            def_cfg = Configuration()
            def_cfg.get_logger().warning("Program config is not initialized. Returning default config.")
            return def_cfg
        return Configuration.instance

    @staticmethod
    def initialize(**kwargs) -> Configuration:
        """initializes the Configuration singleton"""
        cfg = Configuration()
        cfg.initialization_args = kwargs
        cfg.db_config = json.loads(kwargs["db_conn_json"])
        for key, val in kwargs.items():
            if key == "log_level":
                pwgo_logging.set_log_level(val)
            if key == "lib_log_level":
                pwgo_logging.set_lib_log_level(val)
            if hasattr(cfg, key):
                setattr(cfg, key, val)
        # reinitialize the db scripts in case we're using non default db names
        cfg.piwigo_db_scripts = PiwigoScripts(cfg.pwgo_db_name, cfg.msg_db_name)
        cfg.rekognition_db_scripts = RekognitionScripts(cfg.rek_db_name)
        # log init parameters in a separate loop so that we're logging with
        # the configured verbosity
        click_ctx = click.get_current_context(silent=True)
        # only log init parameters if we have a click context
        # so we don't risk logging sensitive data
        for key,val in kwargs.items():
            show_val = val
            if click_ctx:
                opt = [opt for opt in click_ctx.command.params if opt.name == key]
                if opt and opt[0].hide_input:
                    show_val = "OMITTED"
            else:
                show_val = "OMITTED"
            cfg.get_logger(__name__).debug(strings.LOG_PRG_OPT(key,show_val))

        Configuration.instance = cfg
        return cfg

    def __init__(self):
        self.dry_run = False
        self.initialization_args = None
        self.db_config = None
        self.pwgo_db_name = "piwigo"
        self.msg_db_name = "messaging"
        self.rek_db_name = "rekognition"
        self.piwigo_db_scripts = PiwigoScripts()
        self.rekognition_db_scripts = RekognitionScripts()

    def get_logger(self, name: Optional[str] = None) -> logging.Logger:
        """wrapper for standard getLogger"""
        return logging.getLogger(name)

class PiwigoScripts:
    """container class for piwigo db setup scripts"""
    def __init__(self, pwgo_db_name="piwigo", msg_db_name="messaging"):
        self.create_category_paths = f"""
            CREATE OR REPLACE VIEW {pwgo_db_name}.category_paths
            AS
            WITH RECURSIVE cat_paths AS
            (
                    SELECT id
                            , 1 AS pos
                            , CAST(uppercats AS VARCHAR(100)) AS cats
                            , CAST(SUBSTRING_INDEX(uppercats, ',', 1) AS VARCHAR(100)) AS first_cat
                    FROM {pwgo_db_name}.categories
                    UNION ALL
                    SELECT id
                            , pos + 1 AS pos
                            , SUBSTRING(cats, CHAR_LENGTH(first_cat) + 2) AS cats
                            , SUBSTRING_INDEX(SUBSTRING(cats, CHAR_LENGTH(first_cat) + 2), ',', 1) AS first_cat
                    FROM cat_paths
                    WHERE CHAR_LENGTH(cats) > CHAR_LENGTH(first_cat)
            )
            SELECT cp.id cat_id
                    , CAST(CONCAT('./', GROUP_CONCAT(c.name ORDER BY cp.pos SEPARATOR'/')) AS VARCHAR(255)) cpath
            FROM cat_paths cp
            JOIN {pwgo_db_name}.categories c
            ON c.id = CAST(cp.first_cat AS INT)
            GROUP BY cp.id;
        """

        self.create_image_category_triggers = f"""
            DELIMITER $$
            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_ins_aft_imagecategory
            AFTER INSERT ON {pwgo_db_name}.image_category
            FOR EACH ROW
            BEGIN
                    INSERT INTO {pwgo_db_name}.image_virtual_paths
                    (image_id, category_id, category_uppercats, physical_path, virtual_path)
                    SELECT new.image_id
                            , new.category_id
                            , c.uppercats
                            , CONCAT(pcp.cpath, '/', i.file)
                            , CONCAT(vcp.cpath, '/', i.id, '_', i.file)
                    FROM {pwgo_db_name}.image_category ic
                    JOIN {pwgo_db_name}.images i
                    ON i.id = ic.image_id
                    JOIN {pwgo_db_name}.categories c
                    ON c.id = ic.category_id
                    JOIN {pwgo_db_name}.category_paths pcp
                    ON pcp.cat_id = i.storage_category_id
                    JOIN {pwgo_db_name}.category_paths vcp
                    ON vcp.cat_id = ic.category_id
                    WHERE c.dir IS NULL
                            AND ic.image_id = new.image_id
                            AND ic.category_id = new.category_id;

                    INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                    VALUES ('IMG_METADATA' , JSON_OBJECT(
                            'image_id', new.image_id
                            , 'table_name', 'image_category'
                            , 'table_primary_key', JSON_ARRAY(new.image_id, new.category_id)
                            , 'operation', 'INSERT'
                    ));

            END;
            $$

            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_aft_del_imagecategory
            AFTER DELETE ON {pwgo_db_name}.image_category
            FOR EACH ROW
            BEGIN
                    DELETE
                    FROM {pwgo_db_name}.image_virtual_paths
                    WHERE image_id = old.image_id
                            AND category_id = old.category_id;

                    INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                    VALUES ('IMG_METADATA' , JSON_OBJECT(
                            'image_id', old.image_id
                            , 'table_name', 'image_category'
                            , 'table_primary_key', JSON_ARRAY(old.image_id, old.category_id)
                            , 'operation', 'DELETE'
                    ));

            END;
            $$

            DELIMITER ;
        """

        self.create_image_metadata = f"""
            CREATE OR REPLACE VIEW {pwgo_db_name}.image_metadata
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
            FROM {pwgo_db_name}.images i
            LEFT JOIN {pwgo_db_name}.image_tag it
            ON it.image_id = i.id
            LEFT JOIN {pwgo_db_name}.tags t
            ON t.id = it.tag_id
            GROUP BY i.id;
        """

        self.create_image_tag_triggers = f"""
            DELIMITER $$
            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_ins_aft_imagetag
            AFTER INSERT ON {pwgo_db_name}.image_tag
            FOR EACH ROW
            BEGIN
                    INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                    VALUES ('IMG_METADATA' , JSON_OBJECT(
                            'image_id', new.image_id
                            , 'table_name', 'image_tag'
                            , 'table_primary_key', JSON_ARRAY(new.image_id, new.tag_id)
                            , 'operation', 'INSERT'
                    ));

            END;
            $$

            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_aft_del_imagetag
            AFTER DELETE ON {pwgo_db_name}.image_tag
            FOR EACH ROW
            BEGIN
                    INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                    VALUES ('IMG_METADATA' , JSON_OBJECT(
                            'image_id', old.image_id
                            , 'table_name', 'image_tag'
                            , 'table_primary_key', JSON_ARRAY(old.image_id, old.tag_id)
                            , 'operation', 'DELETE'
                    ));

            END;
            $$

            DELIMITER ;
        """

        self.create_image_virtual_paths = f"""
            DROP TABLE IF EXISTS {pwgo_db_name}.image_virtual_paths;

            CREATE TABLE {pwgo_db_name}.image_virtual_paths
            (
                    image_id MEDIUMINT(8) UNSIGNED NOT NULL,
                    category_id SMALLINT(5) UNSIGNED NOT NULL,
                    category_uppercats VARCHAR(255) NOT NULL,
                    physical_path VARCHAR(255) NOT NULL,
                    virtual_path VARCHAR(255) NOT NULL,
                    PRIMARY KEY (image_id, category_id)
            );

            INSERT INTO {pwgo_db_name}.image_virtual_paths
            (image_id, category_id, category_uppercats, physical_path, virtual_path)
            SELECT ic.image_id
                    , ic.category_id
                    , c.uppercats
                    , CONCAT(pcp.cpath, '/', i.file)
                    , CONCAT(vcp.cpath, '/', i.id, '_', i.file)
            FROM {pwgo_db_name}.image_category ic
            JOIN {pwgo_db_name}.images i
            ON i.id = ic.image_id
            JOIN {pwgo_db_name}.categories c
            ON c.id = ic.category_id
            JOIN {pwgo_db_name}.category_paths pcp
            ON pcp.cat_id = i.storage_category_id
            JOIN {pwgo_db_name}.category_paths vcp
            ON vcp.cat_id = ic.category_id
            WHERE c.dir IS NULL;

            DELIMITER $$
            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_ins_aft_img_virt_paths
            AFTER INSERT ON {pwgo_db_name}.image_virtual_paths
            FOR EACH ROW
            BEGIN
                
                INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                VALUES ('IMG_VIRT_PATH' , JSON_OBJECT(
                        'image_id', new.image_id
                        , 'table_name', 'image_virtual_paths'
                        , 'table_primary_key', JSON_ARRAY(new.image_id, new.category_id)
                        , 'operation', 'INSERT'
                        , 'values', JSON_OBJECT(
                            'physical_path', new.physical_path, 'virtual_path', new.virtual_path, 'category_uppercats', new.category_uppercats
                        )
                ));

            END;
            $$


            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_del_aft_img_virt_paths
            AFTER DELETE ON {pwgo_db_name}.image_virtual_paths
            FOR EACH ROW
            BEGIN
                    
                INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                VALUES ('IMG_VIRT_PATH' , JSON_OBJECT(
                        'image_id', old.image_id
                        , 'table_name', 'image_virtual_paths'
                        , 'table_primary_key', JSON_ARRAY(old.image_id, old.category_id)
                        , 'operation', 'DELETE'
                        , 'values', JSON_OBJECT(
                            'physical_path', old.physical_path, 'virtual_path', old.virtual_path, 'category_uppercats', old.category_uppercats
                        )
                ));

            END;$$

            DELIMITER ;
        """

        self.create_images_triggers = f"""
            DELIMITER $$
            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_ins_aft_images
            AFTER INSERT ON {pwgo_db_name}.images
            FOR EACH ROW
            BEGIN
                
                INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                VALUES ('IMG_METADATA' , JSON_OBJECT(
                        'image_id', new.id
                        , 'table_name', 'images'
                        , 'table_primary_key', JSON_ARRAY(new.id)
                        , 'operation', 'INSERT'
                ));

            END;
            $$

            DELIMITER $$
            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_upd_aft_images
            AFTER UPDATE ON {pwgo_db_name}.images
            FOR EACH ROW
            BEGIN
                
                IF (new.name != old.name OR new.comment != old.comment OR new.author != old.author OR new.date_creation != old.date_creation) THEN
                    INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
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
            END;
            $$

            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_del_aft_images
            AFTER DELETE ON {pwgo_db_name}.images
            FOR EACH ROW
            BEGIN
                    
                INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                VALUES ('IMG_METADATA' , JSON_OBJECT(
                        'image_id', old.id
                        , 'table_name', 'images'
                        , 'table_primary_key', JSON_ARRAY(old.id)
                        , 'operation', 'DELETE'
                ));

            END;$$

            DELIMITER ;
        """

        self.create_implicit_tags = f"""
            DROP TABLE IF EXISTS {pwgo_db_name}.implicit_tags;

            CREATE TABLE {pwgo_db_name}.implicit_tags
            (
                implied_tag_id SMALLINT NOT NULL,
                triggered_by_tag_id SMALLINT NOT NULL,
                PRIMARY KEY (implied_tag_id, triggered_by_tag_id)
            );

            CREATE OR REPLACE VIEW {pwgo_db_name}.expanded_implicit_tags
            AS
            WITH RECURSIVE expanded_imp_tags AS
            (
                SELECT implied_tag_id
                    , triggered_by_tag_id
                    , triggered_by_tag_id AS org_triggered_by_tag_id
                    , 1 AS rnk
                FROM {pwgo_db_name}.implicit_tags it
                UNION ALL
                SELECT it2.implied_tag_id
                    , it2.triggered_by_tag_id
                    , eit.org_triggered_by_tag_id
                    , rnk + 1 AS rnk
                FROM expanded_imp_tags eit
                JOIN {pwgo_db_name}.implicit_tags it2
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

            SELECT @family := id FROM {pwgo_db_name}.tags WHERE `name` = 'family';
            SELECT @kids := id FROM {pwgo_db_name}.tags WHERE `name` = 'kids';
            SELECT @henry := id FROM {pwgo_db_name}.tags WHERE `name` = 'henry';
            SELECT @hannah := id FROM {pwgo_db_name}.tags WHERE `name` = 'hannah';
            SELECT @chelsea := id FROM {pwgo_db_name}.tags WHERE `name` = 'chelsea';
            SELECT @justin := id FROM {pwgo_db_name}.tags WHERE `name` = 'justin';
            SELECT @kitty := id FROM {pwgo_db_name}.tags WHERE `name` = 'kitty';
            SELECT @holidays := id FROM {pwgo_db_name}.tags WHERE `name` = 'holidays';
            SELECT @christmas := id FROM {pwgo_db_name}.tags WHERE `name` = 'christmas';
            SELECT @thanksgiving := id FROM {pwgo_db_name}.tags WHERE `name` = 'thanksgiving';
            SELECT @halloween := id FROM {pwgo_db_name}.tags WHERE `name` = 'halloween';

            TRUNCATE TABLE {pwgo_db_name}.implicit_tags;

            INSERT INTO {pwgo_db_name}.implicit_tags ( implied_tag_id, triggered_by_tag_id )
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

            INSERT INTO {pwgo_db_name}.image_tag ( image_id, tag_id )
            SELECT DISTINCT it.image_id, imp.implied_tag_id 
            FROM {pwgo_db_name}.image_tag it
            JOIN {pwgo_db_name}.expanded_implicit_tags imp
            ON imp.triggered_by_tag_id = it.tag_id
            LEFT JOIN {pwgo_db_name}.image_tag it2
            ON it2.image_id = it.image_id AND it2.tag_id = imp.implied_tag_id
            WHERE it2.image_id IS NULL;
        """

        self.create_pwgo_message = f"""
            CREATE TABLE IF NOT EXISTS {msg_db_name}.pwgo_message
            (
                id INT(11) UNSIGNED AUTO_INCREMENT,
                message_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                message_type VARCHAR(25) NOT NULL,
                message JSON NOT NULL,
                PRIMARY KEY (id)
            );
        """

        self.create_tags_triggers = f"""
            DELIMITER $$
            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_ins_aft_tags
            AFTER INSERT ON {pwgo_db_name}.tags
            FOR EACH ROW
            BEGIN
                
                INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                VALUES ('TAGS' , JSON_OBJECT(
                        'tag_id', new.id
                        , 'table_name', 'tags'
                        , 'table_primary_key', JSON_ARRAY(new.id)
                        , 'operation', 'INSERT'
                ));

            END;
            $$

            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_upd_aft_tags
            AFTER UPDATE ON {pwgo_db_name}.tags
            FOR EACH ROW
            BEGIN
                
                IF new.name != old.name THEN
                    INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
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

            CREATE OR REPLACE TRIGGER {pwgo_db_name}.tr_del_aft_tags
            AFTER DELETE ON {pwgo_db_name}.tags
            FOR EACH ROW
            BEGIN
                    
                INSERT INTO {msg_db_name}.pwgo_message (message_type, `message`)
                VALUES ('TAGS' , JSON_OBJECT(
                        'tag_id', old.id
                        , 'table_name', 'tags'
                        , 'table_primary_key', JSON_ARRAY(old.id)
                        , 'operation', 'DELETE'
                ));

            END;
            $$

            DELIMITER ;
        """

class RekognitionScripts:
    """container class for rekognition db setup scripts"""
    def __init__(self, rek_db_name="Rekognition"):
        self.create_image_labels = f"""
            CREATE TABLE IF NOT EXISTS {rek_db_name}.image_labels
            (
                piwigo_image_id MEDIUMINT(8) NOT NULL,
                label VARCHAR(50) NOT NULL,
                confidence FLOAT NOT NULL,
                parents JSON NULL,
                PRIMARY KEY (piwigo_image_id, label)
            );
        """

        self.create_index_faces = f"""
            CREATE TABLE IF NOT EXISTS {rek_db_name}.indexed_faces
            (
                face_id CHAR(36) NOT NULL,
                image_id CHAR(36) NOT NULL,
                piwigo_image_id MEDIUMINT(8) UNSIGNED NOT NULL,
                piwigo_category_id SMALLINT(5) UNSIGNED NOT NULL,
                face_confidence FLOAT(8,5) NOT NULL,
                face_details JSON NOT NULL,
                PRIMARY KEY (face_id),
                UNIQUE (piwigo_image_id)
            );
        """

        self.create_processed_faces = f"""
            CREATE TABLE IF NOT EXISTS {rek_db_name}.processed_faces
            (
                piwigo_image_id MEDIUMINT(8) NOT NULL,
                face_index TINYINT NOT NULL,
                face_details JSON NOT NULL,
                matched_to_face_id CHAR(36) NULL,
                PRIMARY KEY (piwigo_image_id, face_index),
                FOREIGN KEY (matched_to_face_id) REFERENCES {rek_db_name}.indexed_faces(face_id) ON DELETE SET NULL
            );
        """

        self.create_rekognition_db = f"""
            CREATE DATABASE IF NOT EXISTS {rek_db_name};
        """
