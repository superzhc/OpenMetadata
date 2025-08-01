CREATE TABLE IF NOT EXISTS type_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     category VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.category')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL
);

-- 修改 webhook_entity 表
-- 添加新列
ALTER TABLE webhook_entity ADD status VARCHAR(256) NOT NULL;

-- 删除旧列
ALTER TABLE webhook_entity DROP COLUMN deleted;

CREATE TABLE IF NOT EXISTS mlmodel_service_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     serviceType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.serviceType')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);


UPDATE thread_entity SET json = JSON_SET(json, '$.type', 'Conversation', '$.reactions', JSON_ARRAY());

ALTER TABLE thread_entity ADD type VARCHAR(64) GENERATED ALWAYS AS (json_value(json , '$.type'));
ALTER TABLE thread_entity ADD taskId INT  GENERATED ALWAYS AS (json_value(json , '$.task.id'));
ALTER TABLE thread_entity ADD taskStatus VARCHAR(64) GENERATED ALWAYS AS (json_value(json , '$.task.status'));
ALTER TABLE thread_entity ADD taskAssignees GENERATED ALWAYS AS (json_value(json, '$.task.assignees'));


CREATE TABLE task_sequence (id INT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id));
INSERT INTO task_sequence VALUES (0);


DELETE from ingestion_pipeline_entity where 1=1;

UPDATE dbservice_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.database'),
        '$.connection.config.databaseSchema',
        JSON_EXTRACT(json, '$.connection.config.database')
    ) where serviceType in ('Mysql','Hive','Presto','Trino','Clickhouse','SingleStore','MariaDB','Db2','Oracle');

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.database'
                    ,'$.connection.config.username'
                    ,'$.connection.config.projectId'
                    ,'$.connection.config.enablePolicyTagImport')
WHERE serviceType = 'BigQuery';

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.database')
WHERE serviceType in ('Athena','Databricks');

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.supportsProfiler', '$.connection.config.pipelineServiceName')
WHERE serviceType = 'Glue';

UPDATE dashboard_service_entity
SET json = JSON_REMOVE(json, '$.connection.config.dbServiceName')
WHERE serviceType in ('Metabase','Superset','Tableau');

DELETE FROM pipeline_service_entity WHERE 1=1;