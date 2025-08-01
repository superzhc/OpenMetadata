-- noinspection SqlNoDataSourceInspectionForFile

-- column deleted not needed for entities that don't support soft delete
ALTER TABLE query_entity DROP COLUMN deleted;
ALTER TABLE event_subscription_entity DROP COLUMN deleted;

-- create domain entity table


 CREATE TABLE IF NOT EXISTS domain_entity (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        fqnHash VARCHAR(256) NOT NULL ,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
        deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
   );

   CREATE TABLE IF NOT EXISTS data_product_entity (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        fqnHash VARCHAR(256) NOT NULL ,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
        deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
   );

CREATE TABLE IF NOT EXISTS search_service_entity (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        nameHash VARCHAR(256) NOT NULL ,
        serviceType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.serviceType')) NOT NULL,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
        deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
   );

   CREATE TABLE IF NOT EXISTS search_index_entity (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        nameHash VARCHAR(256) NOT NULL ,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
        deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
   );


-- We were hardcoding retries to 0. Since we are now using the IngestionPipeline to set them, keep existing ones to 0.
UPDATE ingestion_pipeline_entity
SET json = JSON_REPLACE(json, '$.airflowConfig.retries', 0)
WHERE JSON_EXTRACT(json, '$.airflowConfig.retries') IS NOT NULL;

CREATE TABLE IF NOT EXISTS stored_procedure_entity (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        fqnHash VARCHAR(256) NOT NULL ,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
        deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
   );


-- rename viewParsingTimeoutLimit for queryParsingTimeoutLimit
UPDATE ingestion_pipeline_entity
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.sourceConfig.config.viewParsingTimeoutLimit'),
    '$.sourceConfig.config.queryParsingTimeoutLimit',
    JSON_VALUE(json, '$.sourceConfig.config.viewParsingTimeoutLimit')
)
WHERE JSON_VALUE(json, '$.pipelineType') = 'metadata';

-- Rename sandboxDomain for instanceDomain
UPDATE dbservice_entity
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.connection.config.sandboxDomain'),
    '$.connection.config.instanceDomain',
    JSON_EXTRACT(json, '$.connection.config.sandboxDomain')
)
WHERE serviceType = 'DomoDatabase';

UPDATE dashboard_service_entity
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.connection.config.sandboxDomain'),
    '$.connection.config.instanceDomain',
    JSON_EXTRACT(json, '$.connection.config.sandboxDomain')
)
WHERE serviceType = 'DomoDashboard';

UPDATE pipeline_service_entity
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.connection.config.sandboxDomain'),
    '$.connection.config.instanceDomain',
    JSON_EXTRACT(json, '$.connection.config.sandboxDomain')
)
WHERE serviceType = 'DomoPipeline';

-- Query Entity supports service, which requires FQN for name
-- 先修改数据类型
ALTER TABLE query_entity MODIFY nameHash VARCHAR(256);
-- 再修改列名
ALTER TABLE query_entity RENAME COLUMN nameHash TO fqnHash;
CREATE INDEX bot_entity_name_index ON bot_entity(name);
CREATE INDEX chart_entity_name_index ON chart_entity(name);
CREATE INDEX classification_entity_name_index ON classification(name);
CREATE INDEX storage_container_entity_name_index ON storage_container_entity(name);
CREATE INDEX dashboard_data_model_entity_name_index ON dashboard_data_model_entity(name);
CREATE INDEX dashboard_entity_name_index ON dashboard_entity(name);
CREATE INDEX dashboard_service_entity_name_index ON dashboard_service_entity(name);
CREATE INDEX data_insight_name_index ON data_insight_chart(name);
CREATE INDEX database_entity_name_index ON database_entity(name);
CREATE INDEX database_schema_entity_name_index ON database_schema_entity(name);
CREATE INDEX dbservice_entity_name_index ON dbservice_entity(name);
CREATE INDEX event_subscription_entity_name_index ON event_subscription_entity(name);
CREATE INDEX glossary_entity_name_index ON glossary_entity(name);
CREATE INDEX glossary_term_entity_name_index ON glossary_term_entity(name);
CREATE INDEX ingestion_pipeline_entity_name_index ON ingestion_pipeline_entity(name);
CREATE INDEX kpi_entity_name_index ON kpi_entity(name);
CREATE INDEX messaing_service_entity_name_index ON messaging_service_entity(name);
CREATE INDEX metadata_service_entity_name_index ON metadata_service_entity(name);
CREATE INDEX metric_entity_name_index ON metric_entity(name);
CREATE INDEX ml_model_entity_name_index ON ml_model_entity(name);
CREATE INDEX mlmodel_service_entity_name_index ON mlmodel_service_entity(name);
CREATE INDEX pipeline_entity_name_index ON pipeline_entity(name);
CREATE INDEX pipeline_service_entity_name_index ON pipeline_service_entity(name);
CREATE INDEX policy_entity_name_index ON policy_entity(name);
CREATE INDEX query_entity_name_index ON query_entity(name);
CREATE INDEX report_entity_name_index ON report_entity(name);
CREATE INDEX role_entity_name_index ON role_entity(name);
CREATE INDEX storage_service_entity_name_index ON storage_service_entity(name);
CREATE INDEX table_entity_name_index ON table_entity(name);
CREATE INDEX tag_entity_name_index ON tag(name);
CREATE INDEX team_entity_name_index ON team_entity(name);
CREATE INDEX test_case_name_index ON test_case(name);
CREATE INDEX test_connection_definition_name_index ON test_connection_definition(name);
CREATE INDEX test_definition_name_index ON test_definition(name);
CREATE INDEX test_suite_name_index ON test_suite(name);
CREATE INDEX topic_entity_name_index ON topic_entity(name);
CREATE INDEX type_entity_name_index ON type_entity(name);
CREATE INDEX user_entity_name_index ON user_entity(name);
CREATE INDEX web_analytic_event_name_index ON web_analytic_event(name);
CREATE INDEX automations_workflow_name_index ON automations_workflow(name);


CREATE TABLE IF NOT EXISTS persona_entity (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        fqnHash VARCHAR(256) NOT NULL ,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL
   );

CREATE TABLE IF NOT EXISTS doc_store (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        entityType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.entityType')) NOT NULL,
        fqnHash VARCHAR(256) NOT NULL ,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL
   );

-- Remove Mark All Deleted Field
UPDATE ingestion_pipeline_entity
SET json = JSON_REMOVE(json, '$.sourceConfig.config.markAllDeletedTables')
WHERE JSON_VALUE(json, '$.pipelineType') = 'metadata';


-- update entityReportData from pascale to camel case
UPDATE report_data_time_series
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.reportDataType'),
    '$.reportDataType',
    'entityReportData'),
    entityFQNHash = MD5('entityReportData')
WHERE JSON_VALUE(json, '$.reportDataType') = 'EntityReportData';

-- update webAnalyticEntityViewReportData from pascale to camel case
UPDATE report_data_time_series
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.reportDataType'),
    '$.reportDataType',
    'webAnalyticEntityViewReportData'),
    entityFQNHash = MD5('webAnalyticEntityViewReportData')
WHERE JSON_VALUE(json, '$.reportDataType') = 'WebAnalyticEntityViewReportData';

-- update webAnalyticUserActivityReportData from pascale to camel case
UPDATE report_data_time_series
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.reportDataType'),
    '$.reportDataType',
    'webAnalyticUserActivityReportData'),
    entityFQNHash = MD5('webAnalyticUserActivityReportData')
WHERE JSON_VALUE(json, '$.reportDataType') = 'WebAnalyticUserActivityReportData';


CREATE TABLE IF NOT EXISTS installed_apps (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        nameHash VARCHAR(256) NOT NULL ,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
        deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
   );

   CREATE TABLE IF NOT EXISTS apps_marketplace (
        id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
        name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
        nameHash VARCHAR(256) NOT NULL ,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
        updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
        deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
   );

   CREATE TABLE IF NOT EXISTS apps_extension_time_series (
        appId VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.appId')) NOT NULL,
        json text NOT NULL CHECK (json IS JSON(LAX)),
        timestamp BIGINT   GENERATED ALWAYS AS (json_value(json , '$.timestamp'))NOT NULL
   );

-- Adding back the COLLATE queries from 1.1.5 to keep the correct VARCHAR length
ALTER TABLE glossary_term_entity MODIFY fqnHash VARCHAR(756) COLLATE ascii_bin;
-- We don't have an ID, so we'll create a temp SERIAL number and use it for deletion
ALTER TABLE entity_extension_time_series ADD COLUMN temp NUMBER(20) IDENTITY(1,1);

-- 使用子查询替代CTE（达梦8更兼容）
DELETE FROM entity_extension_time_series
WHERE temp IN (
    SELECT temp FROM (
        SELECT
            temp,
            ROW_NUMBER() OVER (
                PARTITION BY entityFQNHash, extension, timestamp
                ORDER BY entityFQNHash
            ) AS RN
        FROM entity_extension_time_series
    ) t
    WHERE t.RN > 1
);

ALTER TABLE entity_extension_time_series DROP COLUMN temp;

ALTER TABLE entity_extension_time_series MODIFY   entityFQNHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE entity_extension_time_series MODIFY   jsonSchema VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE entity_extension_time_series MODIFY   extension VARCHAR(256) COLLATE ascii_bin;

-- Airflow pipeline status set to millis
UPDATE entity_extension_time_series ts
JOIN pipeline_entity p
  ON ts.entityFQNHash  = p.fqnHash
SET ts.json = JSON_INSERT(
    JSON_REMOVE(ts.json, '$.timestamp'),
    '$.timestamp',
    JSON_EXTRACT(ts.json, '$.timestamp') * 1000
 )
WHERE ts.extension = 'pipeline.pipelineStatus'
  AND JSON_VALUE(p.json, '$.serviceType') in ('Airflow', 'GluePipeline', 'Airbyte', 'Dagster', 'DomoPipeline');
