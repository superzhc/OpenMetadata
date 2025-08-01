CREATE TABLE IF NOT EXISTS universal_data_model_entity (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        fqnhash VARCHAR(256) NOT NULL ,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
        deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
   );