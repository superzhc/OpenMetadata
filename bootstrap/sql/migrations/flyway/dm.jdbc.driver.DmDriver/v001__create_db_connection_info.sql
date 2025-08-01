
CREATE TABLE IF NOT EXISTS entity_relationship (
    fromId VARCHAR(36) NOT NULL,                -- ID of the from entity
    toId VARCHAR(36) NOT NULL,                  -- ID of the to entity
    fromEntity VARCHAR(256) NOT NULL,           -- Type name of the from entity
    toEntity VARCHAR(256) NOT NULL,             -- Type name of to entity
    relation TINYINT NOT NULL,
    jsonSchema VARCHAR(256),                    -- Schema used for generating JSON
    json text,                                  -- JSON payload with additional information
    deleted NUMBER(1) NOT NULL DEFAULT 0,
    CONSTRAINT pk_entity_relationship PRIMARY KEY (fromId, toId, relation)
);

-- 创建索引
CREATE INDEX entity_relationship_from_index ON entity_relationship(fromId, relation);
CREATE INDEX entity_relationship_to_index ON entity_relationship(toId, relation);
CREATE INDEX entity_relationship_edge_index ON entity_relationship(fromId, toId, relation);

CREATE TABLE IF NOT EXISTS field_relationship (
     fromFQN VARCHAR(256) NOT NULL,              -- Fully qualified name of entity or field
     toFQN VARCHAR(256) NOT NULL,                -- Fully qualified name of entity or field
     fromType VARCHAR(256) NOT NULL,             -- Fully qualified type of entity or field
     toType VARCHAR(256) NOT NULL,               -- Fully qualified type of entity or field
     relation TINYINT NOT NULL,
     jsonSchema VARCHAR(256),                    -- Schema used for generating JSON
     json text,
    CONSTRAINT pk_field_relationship PRIMARY KEY (fromFQN, toFQN, relation)
);
-- 创建索引
CREATE INDEX from_index ON field_relationship(fromFQN, relation);
CREATE INDEX to_index ON field_relationship(toFQN, relation);

--
-- Used for storing additional metadata for an entity
--
CREATE TABLE IF NOT EXISTS entity_extension (
    id VARCHAR(36) NOT NULL,                    -- ID of the from entity
    extension VARCHAR(256) NOT NULL,            -- Extension name same as entity.fieldName
    jsonSchema VARCHAR(256) NOT NULL,           -- Schema used for generating JSON
    json JSON NOT NULL,
    PRIMARY KEY (id, extension)
);

CREATE TABLE IF NOT EXISTS entity_extension (
     id VARCHAR(36) NOT NULL,
     extension VARCHAR(256) NOT NULL,              -- Fully qualified name of entity or field
     jsonSchema VARCHAR(256) NOT NULL,                -- Fully qualified name of entity or field
     json text NOT NULL,
    CONSTRAINT pk_entity_extension PRIMARY KEY (id, extension)
);


CREATE TABLE IF NOT EXISTS dbservice_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     serviceType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.serviceType')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS messaging_service_entity (
    id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
         name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
         serviceType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.serviceType')) NOT NULL,
         json text NOT NULL CHECK (json IS JSON(LAX)),
         updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
         updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
         deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS dashboard_service_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     serviceType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.serviceType')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS pipeline_service_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     serviceType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.serviceType')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS storage_service_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     serviceType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.serviceType')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS database_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);


CREATE TABLE IF NOT EXISTS database_schema_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS table_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS metric_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS report_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS dashboard_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS ml_model_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS pipeline_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS topic_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS chart_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS location_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS thread_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     entityId VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.entityId')) NOT NULL,
     entityLink VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.about')) NOT NULL,
     assignedTo VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.addressedTo')),
     json text NOT NULL CHECK (json IS JSON(LAX)),
     createdAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.threadTs'))NOT NULL,
     createdBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.createdBy')) NOT NULL,
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     resolved tinyint GENERATED ALWAYS AS (json_value(json, '$.resolved' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS policy_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);


CREATE TABLE IF NOT EXISTS ingestion_pipeline_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER)),
     timestamp BIGINT
);

CREATE TABLE IF NOT EXISTS team_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS user_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     email VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.email')) NOT NULL,
     deactivated VARCHAR(8) GENERATED ALWAYS AS (json_value(json , '$.deactivated')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS bot_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS role_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     defaultRole tinyint GENERATED ALWAYS AS (json_value(json, '$.defaultRole' RETURNING NUMBER)),
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

--
-- Usage table where usage for all the entities is captured
--
CREATE TABLE IF NOT EXISTS entity_usage (
    id VARCHAR(36) NOT NULL,         -- Unique id of the entity
    entityType VARCHAR(20) NOT NULL, -- name of the entity for which this usage is published
    usageDate DATE,                  -- date corresponding to the usage
    count1 INT,                      -- total daily count of use on usageDate
    count7 INT,                      -- rolling count of last 7 days going back from usageDate
    count30 INT,                     -- rolling count of last 30 days going back from usageDate
    percentile1 INT,                 -- percentile rank with in same entity for given usage date
    percentile7 INT,                 -- percentile rank with in same entity for last 7 days of usage
    percentile30 INT,                -- percentile rank with in same entity for last 30 days of usage
    UNIQUE (usageDate, id)
);


CREATE TABLE IF NOT EXISTS tag_category (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     defaultRole tinyint GENERATED ALWAYS AS (json_value(json, '$.defaultRole' RETURNING NUMBER)),
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

 CREATE TABLE IF NOT EXISTS tag (
      id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
      fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
      defaultRole tinyint GENERATED ALWAYS AS (json_value(json, '$.defaultRole' RETURNING NUMBER)),
      json text NOT NULL CHECK (json IS JSON(LAX)),
      updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
      updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
      deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
 );



CREATE TABLE IF NOT EXISTS tag_usage (
    source TINYINT NOT NULL,            -- Source of the tag label
    tagFQN VARCHAR(256) NOT NULL,       -- Fully qualified name of the tag
    targetFQN VARCHAR(256) NOT NULL,    -- Fully qualified name of the entity instance or corresponding field
    labelType TINYINT NOT NULL,         -- Type of tagging: manual, automated, propagated, derived
    state TINYINT NOT NULL,             -- State of tagging: suggested or confirmed
    UNIQUE (source, tagFQN, targetFQN)
);

 CREATE TABLE IF NOT EXISTS change_event (
      eventType VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.eventType')) NOT NULL,
      entityType VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.entityType')) NOT NULL,
      userName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.userName')) NOT NULL,
      eventTime BIGINT   GENERATED ALWAYS AS (json_value(json , '$.timestamp'))NOT NULL,
      json text NOT NULL CHECK (json IS JSON(LAX))
 );

  CREATE TABLE IF NOT EXISTS webhook_entity (
       id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
       name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
       json text NOT NULL CHECK (json IS JSON(LAX)),
       deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
  );

   CREATE TABLE IF NOT EXISTS glossary_entity (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
        deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
   );

    CREATE TABLE IF NOT EXISTS glossary_term_entity (
           id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
           fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
           json text NOT NULL CHECK (json IS JSON(LAX)),
           updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
           updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
           deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
      );


