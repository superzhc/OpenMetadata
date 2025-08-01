
-- Remove classificationName in BigQuery
UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.classificationName') where serviceType in ('BigQuery');

-- migrate ingestAllDatabases in postgres
UPDATE dbservice_entity de2
SET json = JSON_REPLACE(
    JSON_INSERT(json,
      '$.connection.config.database',
      (select JSON_EXTRACT(json, '$.name')
        from database_entity de
        where id = (select er.toId
            from entity_relationship er
            where er.fromId = de2.id
              and er.toEntity = 'database'
            LIMIT 1
          ))
    ), '$.connection.config.ingestAllDatabases',
    true
  )
where de2.serviceType = 'Postgres'
  and JSON_EXTRACT(json, '$.connection.config.database') is NULL;


CREATE TABLE IF NOT EXISTS storage_container_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS test_connection_definition (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);


CREATE TABLE IF NOT EXISTS automations_workflow (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     workflowType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.workflowType')) NOT NULL,
     status VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.status')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

-- Do not store OM server connection, we'll set it dynamically on the resource
UPDATE ingestion_pipeline_entity
SET json = JSON_REMOVE(json, '$.openMetadataServerConnection');

CREATE TABLE IF NOT EXISTS query_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS temp_query_migration (
     tableId VARCHAR(36)NOT NULL,
     queryId VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     queryName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL
);



INSERT INTO temp_query_migration(tableId, json)
SELECT
    id,
    '{"id":"' || SYS_GUID() ||
    '","query":"' || REPLACE(REPLACE(JSON_VALUE(json, '$.query'), '\', '\\'), '"', '\"') ||
    '","users":' || NVL(JSON_VALUE(json, '$.users'), 'null') ||
    ',"checksum":"' || JSON_VALUE(json, '$.checksum') ||
    '","duration":' || TO_NUMBER(JSON_VALUE(json, '$.duration')) ||
    ',"name":"' || JSON_VALUE(json, '$.checksum') ||
    '","updatedAt":' || ROUND((SYSDATE - TO_DATE('1970-01-01', 'YYYY-MM-DD')) * 86400) ||
    ',"updatedBy":"admin","deleted":false}'
FROM entity_extension
WHERE extension = 'table.tableQueries';

-- 步骤1：创建临时表存储ID映射关系
CREATE GLOBAL TEMPORARY TABLE temp_id_mapping AS
SELECT
    JSON_VALUE(t.json, '$.id') AS new_id,
    t.json AS new_json,
    q.id AS existing_id
FROM temp_query_migration t
LEFT JOIN query_entity q ON JSON_VALUE(t.json, '$.id') = q.id;

-- 步骤2：更新已存在的记录
UPDATE query_entity q
SET json = (SELECT m.new_json FROM temp_id_mapping m
           WHERE m.existing_id = q.id)
WHERE EXISTS (SELECT 1 FROM temp_id_mapping m
             WHERE m.existing_id = q.id);

-- 步骤3：插入新记录
INSERT INTO query_entity(json)
SELECT new_json FROM temp_id_mapping
WHERE existing_id IS NULL;

-- 步骤4：清理临时表
DROP TABLE temp_id_mapping;

INSERT INTO entity_relationship(fromId, toId, fromEntity, toEntity, relation)
SELECT
    tmq.tableId,
    (SELECT qe.id FROM query_entity qe WHERE qe.name = tmq.queryName),
    'table',
    'query',
    5
FROM temp_query_migration tmq
WHERE EXISTS (
    SELECT 1 FROM query_entity qe
    WHERE qe.name = tmq.queryName
);

DELETE FROM entity_extension
WHERE id IN (
    SELECT DISTINCT tableId FROM temp_query_migration
)
AND extension = 'table.tableQueries';

DROP Table temp_query_migration;

-- remove the audience if it was wrongfully sent from the UI after editing the OM service
UPDATE metadata_service_entity
SET json = REGEXP_REPLACE(
    json,
    '("securityConfig":\{[^}]*)"audience":"[^"]*"([^}]*\})',
    '\1\2'
)
WHERE name = 'OpenMetadata'
AND NOT REGEXP_LIKE(json, '"authProvider":"google"');

ALTER TABLE user_tokens
MODIFY expiryDate BIGINT
GENERATED ALWAYS AS (TO_NUMBER(JSON_VALUE(json, '$.expiryDate'))) VIRTUAL;

CREATE TABLE IF NOT EXISTS event_subscription_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);


drop table if exists alert_action_def;
drop table if exists alert_entity;
DELETE from entity_relationship where  fromEntity = 'alert' and toEntity = 'alertAction';


CREATE TABLE IF NOT EXISTS dashboard_data_model_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

UPDATE dbservice_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.database'),
        '$.connection.config.databaseName', JSON_EXTRACT(json, '$.connection.config.database')
    )
where serviceType = 'Druid'
  and JSON_EXTRACT(json, '$.connection.config.database') is not null;

-- We were using the same jsonSchema for Pipeline Services and Ingestion Pipeline status
-- Also, we relied on the extension to store the run id
UPDATE entity_extension_time_series
SET jsonSchema = 'ingestionPipelineStatus', extension = 'ingestionPipeline.pipelineStatus'
WHERE jsonSchema = 'pipelineStatus' AND extension <> 'pipeline.PipelineStatus';

-- We are refactoring the storage service with containers. We'll remove the locations
DROP TABLE location_entity;
DELETE FROM entity_relationship WHERE fromEntity='location' OR toEntity='location';
TRUNCATE TABLE storage_service_entity;

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.storageServiceName')
WHERE serviceType = 'Glue';

UPDATE chart_entity
SET json = JSON_REMOVE(json, '$.tables');

-- Updating the tableau authentication fields
UPDATE dashboard_service_entity
SET json = JSON_INSERT(
JSON_REMOVE(json,'$.connection.config.username','$.connection.config.password'),
'$.connection.config.authType',
JSON_OBJECT(
	'username',JSON_EXTRACT(json,'$.connection.config.username'),
	'password',JSON_EXTRACT(json,'$.connection.config.password')
	)
)
WHERE serviceType = 'Tableau'
AND JSON_EXTRACT(json, '$.connection.config.username') is not null
AND JSON_EXTRACT(json, '$.connection.config.password') is not null;

UPDATE dashboard_service_entity
SET json = JSON_INSERT(
JSON_REMOVE(json,'$.connection.config.personalAccessTokenName','$.connection.config.personalAccessTokenSecret'),
'$.connection.config.authType',
JSON_OBJECT(
	'personalAccessTokenName',JSON_EXTRACT(json,'$.connection.config.personalAccessTokenName'),
	'personalAccessTokenSecret',JSON_EXTRACT(json,'$.connection.config.personalAccessTokenSecret')
	)
)
WHERE serviceType = 'Tableau'
AND JSON_EXTRACT(json, '$.connection.config.personalAccessTokenName') is not null
AND JSON_EXTRACT(json, '$.connection.config.personalAccessTokenSecret') is not null;

-- Removed property from metadataService.json
 UPDATE metadata_service_entity
 SET json = JSON_REMOVE(json, '$.allowServiceCreation')
 WHERE serviceType in ('Amundsen', 'Atlas', 'MetadataES', 'OpenMetadata');

 UPDATE metadata_service_entity
 SET json = JSON_INSERT(json, '$.provider', 'system')
 WHERE name = 'OpenMetadata';

 -- Fix Glue sample data endpoint URL to be a correct URI
 UPDATE dbservice_entity
 SET json = REPLACE(
     json,
     '"endPointURL":"https://glue.<region_name>.amazonaws.com/"',
     '"endPointURL":"https://glue.region_name.amazonaws.com/"'
 )
 WHERE serviceType = 'Glue'
 AND json LIKE '%"endPointURL":"https://glue.<region_name>.amazonaws.com/"%';

 -- Delete connectionOptions from superset
 UPDATE dashboard_service_entity
 SET json = JSON_REMOVE(json, '$.connection.config.connectionOptions')
 WHERE serviceType = 'Superset';

 -- Delete partitionQueryDuration, partitionQuery, partitionField from bigquery
 UPDATE dbservice_entity
 SET json = JSON_REMOVE(json, '$.connection.config.partitionQueryDuration', '$.connection.config.partitionQuery', '$.connection.config.partitionField')
 WHERE serviceType = 'BigQuery';

 -- Delete supportsQueryComment, scheme, hostPort, supportsProfiler from salesforce
 UPDATE dbservice_entity
 SET json = JSON_REMOVE(json, '$.connection.config.scheme', '$.connection.config.hostPort', '$.connection.config.supportsProfiler', '$.connection.config.supportsQueryComment')
 WHERE serviceType = 'Salesforce';

 -- Delete supportsProfiler from DynamoDB
 UPDATE dbservice_entity
 SET json = JSON_REMOVE(json, '$.connection.config.supportsProfiler')
 WHERE serviceType = 'DynamoDB';

 -- 更新 table_entity 表
 UPDATE table_entity
 SET json = REGEXP_REPLACE(
     json,
     '\"source\"\s*:\s*\"Tag\"',
     '"source": "Classification"'
 )
 WHERE REGEXP_LIKE(json, '\"source\"\s*:\s*\"Tag\"');

 -- 更新 ml_model_entity 表
 UPDATE ml_model_entity
 SET json = REGEXP_REPLACE(
     json,
     '\"source\"\s*:\s*\"Tag\"',
     '"source": "Classification"'
 )
 WHERE REGEXP_LIKE(json, '\"source\"\s*:\s*\"Tag\"');


-- Delete supportsProfiler from Mssql
UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.uriString')
WHERE serviceType = 'Mssql';