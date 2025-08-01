-- Remove markDeletedTablesFromFilterOnly
UPDATE ingestion_pipeline_entity
SET json = JSON_REMOVE(json ,'$.sourceConfig.config.markDeletedTablesFromFilterOnly');

UPDATE data_insight_chart
SET json = JSON_INSERT(
	JSON_REMOVE(json, '$.dimensions'),
	'$.dimensions',
	JSON_ARRAY(
		JSON_OBJECT('name', 'entityFqn', 'chartDataType', 'STRING'),
		JSON_OBJECT('name', 'owner', 'chartDataType', 'STRING'),
		JSON_OBJECT('name', 'entityType', 'chartDataType', 'STRING'),
		JSON_OBJECT('name', 'entityHref', 'chartDataType', 'STRING')
		)
)
WHERE name = 'mostViewedEntities';

DROP TABLE webhook_entity;

CREATE TABLE IF NOT EXISTS alert_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS alert_action_def (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     alertActionType VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.alertActionType')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);


UPDATE dbservice_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.databaseSchema'),
        '$.connection.config.database',
        JSON_EXTRACT(json, '$.connection.config.databaseSchema')
    ) where serviceType in ('Db2');

DELETE from openmetadata_settings where configType = 'activityFeedFilterSetting';

UPDATE ingestion_pipeline_entity
SET json = JSON_REMOVE(json ,'$.sourceConfig.config.dbtConfigSource');


UPDATE pipeline_service_entity
SET json = JSON_INSERT(JSON_INSERT(JSON_REMOVE(json, '$.connection.config.configSource'),'$.connection.config.host', JSON_EXTRACT(json,'$.connection.config.configSource.host')),'$.connection.config.token',JSON_EXTRACT(json, '$.connection.config.configSource.token'))
WHERE  serviceType = 'Dagster' AND json_value(json , '$.connection.config.configSource.host') IS NOT NULL;

UPDATE pipeline_service_entity
SET json = JSON_INSERT(JSON_INSERT(JSON_REMOVE(json, '$.connection.config.configSource'),'$.connection.config.host', JSON_EXTRACT(json,'$.connection.config.configSource.hostPort')), '$.connection.config.token','')
WHERE  serviceType = 'Dagster' AND json_value(json , '$.connection.config.configSource') IS NOT NULL;

UPDATE topic_entity
SET json = JSON_INSERT(JSON_REMOVE(json, '$.schemaType'), '$.messageSchema', JSON_OBJECT('schemaType', JSON_EXTRACT(json, '$.schemaType')))
WHERE json_value(json , '$.schemaType') IS NOT NULL;

UPDATE topic_entity
SET json = JSON_INSERT(JSON_REMOVE(json, '$.schemaText'), '$.messageSchema.schemaText', JSON_EXTRACT(json, '$.schemaText'))
WHERE json_value(json , '$.schemaText') IS NOT NULL;

