"""container module for Configuration"""
from __future__ import annotations

import logging

from .agent import strings
from .agent.log_formatter import CustomFormatter

class Configuration:
    """holds global config values"""
    instance: Configuration = None

    @staticmethod
    def get() -> Configuration:
        """returns the Configuration singleton"""
        if not Configuration.instance:
            def_cfg = Configuration()
            def_cfg.create_logger(__name__).warning("Program config is not initialized. Returning default config.")
            return def_cfg
        return Configuration.instance

    @staticmethod
    def initialize(**kwargs):
        """initializes the Configuration singleton"""
        cfg = Configuration()
        cfg.verbosity = kwargs["verbosity"]
        cfg.dry_run = kwargs["dry_run"]
        for key,val in kwargs.items():
            cfg.create_logger(__name__).debug(strings.LOG_PRG_OPT(key,val))
        Configuration.instance = cfg

    def __init__(self):
        self.verbosity = "INFO"
        self.dry_run = False
        self.piwigo_db_scripts = PiwigoScripts()
        self.rekognition_db_scripts = RekognitionScripts()

    def create_logger(self, name: str) -> logging.Logger:
        """function to generate a logger with given name and the configured verbosity"""
        logger = logging.getLogger(name)
        if not logger.hasHandlers():
            v = self.verbosity
            logger.setLevel(v)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(v)
            console_handler.setFormatter(
                CustomFormatter("%(asctime)s - %(levelname)s: %(message)s", datefmt='%Y-%m-%d %H:%M:%S.%f')
            )
            logger.addHandler(console_handler)
        return logger

class PiwigoScripts:
    """container class for piwigo db setup scripts"""
    def __init__(self):
        self.create_category_paths = """
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
                            , pos + 1 AS pos
                            , SUBSTRING(cats, CHAR_LENGTH(first_cat) + 2) AS cats
                            , SUBSTRING_INDEX(SUBSTRING(cats, CHAR_LENGTH(first_cat) + 2), ',', 1) AS first_cat
                    FROM cat_paths
                    WHERE CHAR_LENGTH(cats) > CHAR_LENGTH(first_cat)
            )
            SELECT cp.id cat_id
                    , CAST(CONCAT('./', GROUP_CONCAT(c.name ORDER BY cp.pos SEPARATOR'/')) AS VARCHAR(255)) cpath
            FROM cat_paths cp
            JOIN piwigo.categories c
            ON c.id = CAST(cp.first_cat AS INT)
            GROUP BY cp.id;
        """

        self.create_image_category_triggers = """
            DELIMITER $$
            CREATE OR REPLACE TRIGGER piwigo.tr_ins_aft_imagecategory
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

            CREATE OR REPLACE TRIGGER piwigo.tr_aft_del_imagecategory
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
        """

        self.create_image_metadata = """
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
            GROUP BY i.id;
        """

        self.create_image_tag_triggers = """
            DELIMITER $$
            CREATE OR REPLACE TRIGGER piwigo.tr_ins_aft_imagetag
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

            CREATE OR REPLACE TRIGGER piwigo.tr_aft_del_imagetag
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
        """

        self.create_image_virtual_paths = """
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

            DELIMITER $$
            CREATE OR REPLACE TRIGGER piwigo.tr_ins_aft_img_virt_paths
            AFTER INSERT ON piwigo.image_virtual_paths
            FOR EACH ROW
            BEGIN
                
                INSERT INTO messaging.pwgo_message (message_type, `message`)
                VALUES ('IMG_VIRT_PATH' , JSON_OBJECT(
                        'image_id', new.image_id
                        , 'table_name', 'image_virtual_paths'
                        , 'table_primary_key', JSON_ARRAY(new.image_id, new.category_id)
                        , 'operation', 'INSERT'
                        , 'values', JSON_OBJECT(
                            'physical_path', new.physical_path, 'virtual_path', new.virtual_path
                        )
                ));

            END;
            $$


            CREATE OR REPLACE TRIGGER piwigo.tr_del_aft_img_virt_paths
            AFTER DELETE ON piwigo.image_virtual_paths
            FOR EACH ROW
            BEGIN
                    
                INSERT INTO messaging.pwgo_message (message_type, `message`)
                VALUES ('IMG_VIRT_PATH' , JSON_OBJECT(
                        'image_id', old.image_id
                        , 'table_name', 'image_virtual_paths'
                        , 'table_primary_key', JSON_ARRAY(old.image_id, old.category_id)
                        , 'operation', 'DELETE'
                        , 'values', JSON_OBJECT(
                            'physical_path', old.physical_path, 'virtual_path', old.virtual_path
                        )
                ));

            END;$$

            DELIMITER ;
        """

        self.create_images_triggers = """
            DELIMITER $$
            CREATE OR REPLACE TRIGGER piwigo.tr_ins_aft_images
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

            END;
            $$

            DELIMITER $$
            CREATE OR REPLACE TRIGGER piwigo.tr_upd_aft_images
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
            END;
            $$

            CREATE OR REPLACE TRIGGER piwigo.tr_del_aft_images
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
        """

        self.create_implicit_tags = """
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
            JOIN piwigo.expanded_implicit_tags imp
            ON imp.triggered_by_tag_id = it.tag_id
            LEFT JOIN piwigo.image_tag it2
            ON it2.image_id = it.image_id AND it2.tag_id = imp.implied_tag_id
            WHERE it2.image_id IS NULL;
        """

        self.create_pwgo_message = """
            CREATE TABLE IF NOT EXISTS messaging.pwgo_message
            (
                id INT(11) UNSIGNED AUTO_INCREMENT,
                message_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                message_type VARCHAR(25) NOT NULL,
                message JSON NOT NULL,
                PRIMARY KEY (id)
            );
        """

        self.create_tags_triggers = """
            DELIMITER $$
            CREATE OR REPLACE TRIGGER piwigo.tr_ins_aft_tags
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

            CREATE OR REPLACE TRIGGER piwigo.tr_upd_aft_tags
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

            CREATE OR REPLACE TRIGGER piwigo.tr_del_aft_tags
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
        """

class RekognitionScripts:
    """container class for rekognition db setup scripts"""
    def __init__(self):
        self.create_image_labels = """
            CREATE TABLE IF NOT EXISTS rekognition.image_labels
            (
                piwigo_image_id MEDIUMINT(8) NOT NULL,
                label VARCHAR(50) NOT NULL,
                confidence FLOAT NOT NULL,
                parents JSON NULL,
                PRIMARY KEY (piwigo_image_id, label)
            );
        """

        self.create_index_faces = """
            CREATE TABLE IF NOT EXISTS rekognition.indexed_faces
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

        self.create_processed_faces = """
            CREATE TABLE IF NOT EXISTS rekognition.processed_faces
            (
                piwigo_image_id MEDIUMINT(8) NOT NULL,
                face_index TINYINT NOT NULL,
                face_details JSON NOT NULL,
                matched_to_face_id CHAR(36) NULL,
                PRIMARY KEY (piwigo_image_id, face_index),
                FOREIGN KEY (matched_to_face_id) REFERENCES rekognition.indexed_faces(face_id) ON DELETE SET NULL
            );
        """

        self.create_rekognition_db = """
            CREATE DATABASE IF NOT EXISTS rekognition;
        """
