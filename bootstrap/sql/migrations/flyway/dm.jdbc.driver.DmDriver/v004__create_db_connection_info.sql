UPDATE team_entity
SET json = JSON_INSERT(json, '$.teamType', 'Group');

ALTER TABLE team_entity
ADD teamType VARCHAR(64) GENERATED ALWAYS AS (json_value(json , '$.teamType')) NOT NULL;

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.database')
WHERE serviceType = 'DynamoDB';

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.connectionOptions')
WHERE serviceType = 'DeltaLake';

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.supportsProfiler')
WHERE serviceType = 'DeltaLake';

UPDATE dashboard_service_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.username'),
        '$.connection.config.clientId',
        JSON_EXTRACT(json, '$.connection.config.username')
    )
WHERE serviceType = 'Looker';

UPDATE dashboard_service_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.password'),
        '$.connection.config.clientSecret',
        JSON_EXTRACT(json, '$.connection.config.password')
    )
WHERE serviceType = 'Looker';

UPDATE dashboard_service_entity
SET json = JSON_REMOVE(json, '$.connection.config.env')
WHERE serviceType = 'Looker';

 CREATE TABLE IF NOT EXISTS test_definition (
      id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
      name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
      json text NOT NULL CHECK (json IS JSON(LAX)),
      entityType VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.entityType')) NOT NULL,
      updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
      updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
      deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
 );

 CREATE TABLE IF NOT EXISTS test_suite (
       id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
       name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
       json text NOT NULL CHECK (json IS JSON(LAX)),
       updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
       updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
       deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
  );

 CREATE TABLE IF NOT EXISTS test_case (
       id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
       fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
       entityFQN VARCHAR(1024) GENERATED ALWAYS AS (json_value(json , '$.entityFQN')) NOT NULL,
       json text NOT NULL CHECK (json IS JSON(LAX)),
       updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
       updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
       deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
  );


UPDATE webhook_entity
SET json = JSON_INSERT(json, '$.webhookType', 'generic');

ALTER TABLE webhook_entity
ADD webhookType VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.webhookType')) NOT NULL;

CREATE TABLE IF NOT EXISTS entity_extension_time_series (
    entityFQN VARCHAR(1024) NOT NULL,           -- Entity FQN, we can refer to tables and columns
    extension VARCHAR(256) NOT NULL,            -- Extension name same as entity.fieldName
    jsonSchema VARCHAR(256) NOT NULL,           -- Schema used for generating JSON
    json text NOT NULL CHECK (json IS JSON(LAX)),
    timestamp BIGINT   GENERATED ALWAYS AS (json_value(json , '$.timestamp'))NOT NULL
);

ALTER TABLE thread_entity ADD announcementStart BIGINT  GENERATED ALWAYS AS (json_value(json , '$.announcement.startTime'));
ALTER TABLE thread_entity  ADD announcementEnd BIGINT  GENERATED ALWAYS AS (json_value(json , '$.announcement.endTime'));

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.databaseSchema','$.connection.config.oracleServiceName')
WHERE serviceType = 'Oracle';

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.hostPort')
WHERE serviceType = 'Athena';
UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.username', '$.connection.config.password')
WHERE serviceType in ('Databricks');

CREATE TABLE IF NOT EXISTS openmetadata_settings (
    id BIGINT NOT NULL AUTO_INCREMENT,
    configType VARCHAR(36) NOT NULL,
    json JSON NOT NULL,
    PRIMARY KEY (id, configType),
    UNIQUE(configType)
);

DELETE FROM entity_extension
WHERE jsonSchema IN ('tableProfile', 'columnTest', 'tableTest');

DELETE FROM ingestion_pipeline_entity
WHERE LOWER(JSON_VALUE(json, '$.pipelineType')) = 'profiler';

DELETE FROM role_entity;
DELETE FROM policy_entity;
DELETE FROM field_relationship WHERE fromType IN ('role', 'policy') OR toType IN ('role', 'policy');
DELETE FROM entity_relationship WHERE fromEntity IN ('role', 'policy') OR toEntity IN ('role', 'policy');
ALTER TABLE role_entity DROP COLUMN defaultRole;
