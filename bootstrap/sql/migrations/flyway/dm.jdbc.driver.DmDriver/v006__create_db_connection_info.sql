
ALTER TABLE entity_extension_time_series MODIFY entityFQN VARCHAR(768);
CREATE INDEX entity_fqn_index ON entity_extension_time_series(entityFQN);

CREATE TABLE IF NOT EXISTS web_analytic_event (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     eventType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.eventType')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);


UPDATE bot_entity
SET json = JSON_INSERT(JSON_REMOVE(json, '$.botType'), '$.provider', 'system');

CREATE TABLE IF NOT EXISTS data_insight_chart (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     fullyQualifiedName VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.fullyQualifiedName')) NOT NULL,
     dataIndexType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.dataIndexType')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);


UPDATE role_entity
SET json = JSON_INSERT(json, '$.provider', 'system')
WHERE name in ('DataConsumer', 'DataSteward');

UPDATE policy_entity
SET json = JSON_INSERT(json, '$.provider', 'system')
WHERE fullyQualifiedName in (json, 'DataConsumerPolicy', 'DataStewardPolicy', 'OrganizationPolicy', 'TeamOnlyPolicy');

UPDATE tag_category
SET json = JSON_INSERT(json, '$.provider', 'system')
WHERE name in ('PersonalData', 'PII', 'Tier');

UPDATE tag
SET json = JSON_INSERT(json, '$.provider', 'system')
WHERE fullyQualifiedName in ('PersonalData.Personal', 'PersonalData.SpecialCategory',
'PII.None', 'PII.NonSensitive', 'PII.Sensitive',
'Tier.Tier1', 'Tier.Tier2', 'Tier.Tier3', 'Tier.Tier4', 'Tier.Tier5');

UPDATE pipeline_service_entity
SET json = JSON_INSERT(json ,'$.connection.config.configSource.hostPort', '$.connection.config.hostPort')
WHERE serviceType = 'Dagster';

UPDATE pipeline_service_entity
SET json = JSON_REMOVE(json ,'$.connection.config.hostPort', '$.connection.config.numberOfStatus')
WHERE serviceType = 'Dagster';

-- Remove categoryType
UPDATE tag_category
SET json = JSON_REMOVE(json ,'$.categoryType');

-- Set mutuallyExclusive flag
UPDATE tag_category
SET json = JSON_INSERT(json ,'$.mutuallyExclusive', 'false');

UPDATE tag_category
SET json = JSON_INSERT(json ,'$.mutuallyExclusive', 'true')
WHERE name in ('PersonalData', 'PII', 'Tier');

UPDATE tag
SET json = JSON_INSERT(json ,'$.mutuallyExclusive', 'false');

-- Merge mutually exclusive table when multiple tag labels are used
DELETE t1 FROM tag_usage t1
INNER JOIN tag_usage t2 ON (t1.targetFQN = t2.targetFQN)
WHERE (t2.tagFQN = 'Tier.Tier1' AND t1.tagFQN in ('Tier.Tier2', 'Tier.Tier3', 'Tier.Tier4', 'Tier.Tier5')) OR
(t2.tagFQN = 'Tier.Tier2' AND t1.tagFQN in ('Tier.Tier3', 'Tier.Tier4', 'Tier.Tier5')) OR
(t2.tagFQN = 'Tier.Tier3' AND t1.tagFQN in ('Tier.Tier4', 'Tier.Tier5')) OR
(t2.tagFQN = 'Tier.Tier4' AND t1.tagFQN in ('Tier.Tier5'));

DELETE t1 FROM tag_usage t1
INNER JOIN tag_usage t2 ON (t1.targetFQN = t2.targetFQN)
WHERE (t2.tagFQN = 'PII.Sensitive' AND t1.tagFQN in ('PII.NonSensitive', 'PII.None')) OR
(t2.tagFQN = 'PII.NonSensitive' AND t1.tagFQN in ('PII.None'));

DELETE t1 FROM tag_usage t1
INNER JOIN tag_usage t2 ON (t1.targetFQN = t2.targetFQN)
WHERE (t2.tagFQN = 'PersonalData.Personal' AND t1.tagFQN in ('PersonalData.SpecialCategory'));

CREATE TABLE IF NOT EXISTS kpi_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);

CREATE TABLE IF NOT EXISTS metadata_service_entity (
     id VARCHAR(36) GENERATED ALWAYS AS (json_value(json , '$.id')) NOT NULL,
     name VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.name')) NOT NULL,
      serviceType VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.serviceType')) NOT NULL,
     json text NOT NULL CHECK (json IS JSON(LAX)),
     updatedAt BIGINT   GENERATED ALWAYS AS (json_value(json , '$.updatedAt'))NOT NULL,
     updatedBy VARCHAR(256) GENERATED ALWAYS AS (json_value(json , '$.updatedBy')) NOT NULL,
     deleted tinyint GENERATED ALWAYS AS (json_value(json, '$.deleted' RETURNING NUMBER))
);



-- We are starting to store the current deployed flag. Let's mark it as false by default
UPDATE ingestion_pipeline_entity
SET json = JSON_REMOVE(json ,'$.deployed');

UPDATE ingestion_pipeline_entity
SET json = JSON_INSERT(json ,'$.deployed', 'true');

-- We removed the supportsMetadataExtraction field in the `OpenMetadataConnection` object being used in IngestionPipelines
UPDATE ingestion_pipeline_entity
SET json = JSON_REMOVE(json ,'$.openMetadataServerConnection.supportsMetadataExtraction');
