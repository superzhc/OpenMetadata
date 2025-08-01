
-- Update table and column profile timestamps to be in milliseconds
UPDATE entity_extension_time_series
    SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.timestamp'),
    '$.timestamp',
    JSON_EXTRACT(json, '$.timestamp') * 1000
    )
WHERE
    extension  in ('table.tableProfile', 'table.columnProfile', 'testCase.testCaseResult');

-- Create report data time series table and move data from entity_extension_time_series
CREATE TABLE IF NOT EXISTS report_data_time_series (
    entityFQNHash VARCHAR(768)   NOT NULL,
    extension VARCHAR(256) NOT NULL,
    jsonSchema VARCHAR(256) NOT NULL,
    json TEXT NOT NULL CHECK (json IS JSON(LAX)),
    timestamp BIGINT GENERATED ALWAYS AS (json_value(json, '$.timestamp')) NOT NULL,
    date DATE GENERATED ALWAYS AS (CAST(FROM_UNIXTIME(JSON_VALUE(json,'$.timestamp')/1000) AS DATE)) NOT NULL
);

INSERT INTO report_data_time_series (entityFQNHash,extension,jsonSchema,json)
SELECT entityFQNHash, extension, jsonSchema, json
FROM entity_extension_time_series WHERE extension = 'reportData.reportDataResult';

DELETE FROM entity_extension_time_series
WHERE extension = 'reportData.reportDataResult';
COMMIT;


-- Create profiler data time series table and move data from entity_extension_time_series
CREATE TABLE IF NOT EXISTS profiler_data_time_series (
    entityFQNHash VARCHAR(768)  NOT NULL,
    extension VARCHAR(256) NOT NULL,
    jsonSchema VARCHAR(256) NOT NULL,
    json TEXT NOT NULL CHECK (json IS JSON(LAX)),
    operation VARCHAR(256) GENERATED ALWAYS AS (json_value(json, '$.operation')) NULL,
    timestamp BIGINT  GENERATED ALWAYS AS (json_value(json, '$.timestamp')) NOT NULL
);

INSERT INTO profiler_data_time_series (entityFQNHash,extension,jsonSchema,json)
SELECT entityFQNHash, extension, jsonSchema, json
FROM entity_extension_time_series
WHERE extension IN ('table.columnProfile', 'table.tableProfile', 'table.systemProfile');

DELETE FROM entity_extension_time_series
WHERE extension IN ('table.columnProfile', 'table.tableProfile', 'table.systemProfile');
COMMIT;


-- Create data quality data time series table and move data from entity_extension_time_series
CREATE TABLE IF NOT EXISTS data_quality_data_time_series (
    entityFQNHash VARCHAR(768)  NOT NULL,
    extension VARCHAR(256) NOT NULL,
    jsonSchema VARCHAR(256) NOT NULL,
    json JSON NOT NULL,
    timestamp BIGINT   GENERATED ALWAYS AS (json_value(json, '$.timestamp')) NOT NULL
);

INSERT INTO data_quality_data_time_series (entityFQNHash,extension,jsonSchema,json)
SELECT entityFQNHash, extension, jsonSchema, json
FROM entity_extension_time_series
WHERE extension = 'testCase.testCaseResult';

DELETE FROM entity_extension_time_series
WHERE extension = 'testCase.testCaseResult';
COMMIT;

ALTER TABLE automations_workflow MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE automations_workflow MODIFY   workflowType VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE automations_workflow MODIFY   status VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE entity_extension_time_series MODIFY   entityFQNHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE entity_extension_time_series MODIFY   jsonSchema VARCHAR(50) COLLATE ascii_bin;
ALTER TABLE entity_extension_time_series MODIFY   extension VARCHAR(100) COLLATE ascii_bin;
ALTER TABLE field_relationship MODIFY   fromFQNHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE field_relationship MODIFY   toFQNHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE thread_entity MODIFY   entityLink VARCHAR(3072) GENERATED ALWAYS AS (json_value(json, '$.about'))   NOT NULL;
ALTER TABLE event_subscription_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE ingestion_pipeline_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE bot_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE user_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE team_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE role_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE policy_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE classification MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE tag MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE tag_usage MODIFY   tagFQNHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE tag_usage MODIFY   targetFQNHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE glossary_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE glossary_term_entity MODIFY fqnHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE type_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE web_analytic_event MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE data_insight_chart MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE kpi_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE test_definition MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE test_connection_definition MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE test_suite MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE test_case MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE dbservice_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE messaging_service_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE metadata_service_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE pipeline_service_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE dashboard_service_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE mlmodel_service_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE storage_service_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE database_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE database_schema_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE table_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE dashboard_data_model_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE dashboard_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE chart_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE pipeline_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE ml_model_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE metric_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE query_entity MODIFY   nameHash VARCHAR(256) COLLATE ascii_bin;
ALTER TABLE report_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE storage_container_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
ALTER TABLE topic_entity MODIFY   fqnHash VARCHAR(768) COLLATE ascii_bin;
