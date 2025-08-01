-- we are not using the secretsManagerCredentials
UPDATE metadata_service_entity
SET json = JSON_REMOVE(json, '$.openMetadataServerConnection.secretsManagerCredentials')
where name = 'OpenMetadata';

-- Rename githubCredentials to gitCredentials
UPDATE dashboard_service_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.githubCredentials'),
        '$.connection.config.gitCredentials',
        JSON_EXTRACT(json, '$.connection.config.githubCredentials')
    )
WHERE serviceType = 'Looker'
  AND JSON_EXTRACT(json, '$.connection.config.githubCredentials') IS NOT NULL;


-- Rename gcsConfig in BigQuery to gcpConfig
UPDATE dbservice_entity
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.connection.config.credentials.gcsConfig'),
    '$.connection.config.credentials.gcpConfig',
    JSON_EXTRACT(json, '$.connection.config.credentials.gcsConfig')
) where serviceType in ('BigQuery');

-- Rename gcsConfig in Datalake to gcpConfig
UPDATE dbservice_entity
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.connection.config.configSource.securityConfig.gcsConfig'),
    '$.connection.config.configSource.securityConfig.gcpConfig',
    JSON_EXTRACT(json, '$.connection.config.configSource.securityConfig.gcsConfig')
) where serviceType in ('Datalake')
AND JSON_EXTRACT(json, '$.connection.config.configSource.securityConfig.gcsConfig') IS NOT NULL;


-- Rename gcsConfig in dbt to gcpConfig
UPDATE ingestion_pipeline_entity
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.sourceConfig.config.dbtConfigSource.dbtSecurityConfig.gcsConfig'),
    '$.sourceConfig.config.dbtConfigSource.dbtSecurityConfig.gcpConfig',
    JSON_EXTRACT(json, '$.sourceConfig.config.dbtConfigSource.dbtSecurityConfig.gcsConfig')
)
WHERE json_value(json , '$.sourceConfig.config.type') = 'DBT'
AND JSON_EXTRACT(json, '$.sourceConfig.config.dbtConfigSource.dbtSecurityConfig.gcsConfig') IS NOT NULL;

-- Rename chartUrl in chart_entity to sourceUrl
UPDATE chart_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.chartUrl'),
        '$.sourceUrl',
        JSON_EXTRACT(json, '$.chartUrl')
    )
WHERE JSON_EXTRACT(json, '$.chartUrl') IS NOT NULL;

-- Rename dashboardUrl in dashboard_entity to sourceUrl
UPDATE dashboard_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.dashboardUrl'),
        '$.sourceUrl',
        JSON_EXTRACT(json, '$.dashboardUrl')
    )
WHERE JSON_EXTRACT(json, '$.dashboardUrl') IS NOT NULL;

-- Rename pipelineUrl in pipeline_entity to sourceUrl
UPDATE pipeline_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.pipelineUrl'),
        '$.sourceUrl',
        JSON_EXTRACT(json, '$.pipelineUrl')
    )
WHERE JSON_EXTRACT(json, '$.pipelineUrl') IS NOT NULL;


-- Rename taskUrl in pipeline_entity to sourceUrl
 -- 步骤1：创建临时表存储解析后的任务数据（达梦8语法）
CREATE GLOBAL TEMPORARY TABLE temp_pipeline_tasks1 (
    id VARCHAR(36),
    task_data CLOB,
    task_name VARCHAR(256),
    task_url VARCHAR(256)
) ON COMMIT PRESERVE ROWS;

-- 插入数据到临时表
INSERT INTO temp_pipeline_tasks1
SELECT
    pe.id,
    t.value AS task_data,
    JSON_VALUE(t.value, '$.name') AS task_name,
    JSON_VALUE(t.value, '$.taskUrl') AS task_url
FROM
    pipeline_entity pe,
    JSON_TABLE(pe.json, '$.tasks[*]' COLUMNS (value CLOB PATH '$')) t;

-- 步骤2：使用存储过程更新数据
CREATE OR REPLACE PROCEDURE update_pipeline_urls AS
  CURSOR cur_pipelines IS SELECT id, json FROM pipeline_entity;
  v_new_json CLOB;
  v_tasks_array CLOB;
BEGIN
  FOR rec IN cur_pipelines LOOP
    -- 初始化任务数组
    v_tasks_array := '[]';

    -- 为每个管道构建新的任务数组
    FOR task_rec IN (
      SELECT
        '{"name":"' || task_name ||
        '","sourceUrl":"' || task_url ||
        '","taskType":' || JSON_VALUE(task_data, '$.taskType') ||
        ',"description":' || COALESCE(TO_CHAR(JSON_VALUE(task_data, '$.description')), 'null') ||
        ',"displayName":' || COALESCE(TO_CHAR(JSON_VALUE(task_data, '$.displayName')), 'null') ||
        ',"fullyQualifiedName":' || COALESCE(TO_CHAR(JSON_VALUE(task_data, '$.fullyQualifiedName')), 'null') ||
        ',"downstreamTasks":' || COALESCE(TO_CHAR(JSON_VALUE(task_data, '$.downstreamTasks')), 'null') ||
        ',"tags":' || COALESCE(TO_CHAR(JSON_VALUE(task_data, '$.tags')), 'null') ||
        ',"endDate":' || COALESCE(TO_CHAR(JSON_VALUE(task_data, '$.endDate')), 'null') ||
        ',"startDate":' || COALESCE(TO_CHAR(JSON_VALUE(task_data, '$.startDate')), 'null') ||
        ',"taskSQL":' || COALESCE(TO_CHAR(JSON_VALUE(task_data, '$.taskSQL')), 'null') ||
        '}' AS new_task
      FROM temp_pipeline_tasks1
      WHERE id = rec.id
    ) LOOP
      -- 将任务添加到数组
      IF v_tasks_array = '[]' THEN
        v_tasks_array := '[' || task_rec.new_task || ']';
      ELSE
        v_tasks_array := REGEXP_REPLACE(v_tasks_array, '\]$', ',' || task_rec.new_task || ']');
      END IF;
    END LOOP;

    -- 构建新的JSON
    v_new_json := REGEXP_REPLACE(rec.json, '"tasks":\[.*\]', '"tasks":' || v_tasks_array);

    -- 更新记录
    UPDATE pipeline_entity SET json = v_new_json WHERE id = rec.id;
  END LOOP;

  -- 清理临时表
  DELETE FROM temp_pipeline_tasks1;
END;
/

-- 执行存储过程
CALL update_pipeline_urls();

-- 删除临时表和存储过程
DROP TABLE temp_pipeline_tasks1;
DROP PROCEDURE update_pipeline_urls;



-- Modify migrations for service connection of postgres and mysql to move password under authType

UPDATE dbservice_entity
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.connection.config.password'),
    '$.connection.config.authType',
    JSON_OBJECT(),
    '$.connection.config.authType.password',
    JSON_EXTRACT(json, '$.connection.config.password'))
where serviceType in ('Postgres', 'Mysql');


-- Clean old test connections
TRUNCATE table automations_workflow;

-- Remove sourceUrl in pipeline_entity from DatabricksPipeline & Fivetran
UPDATE pipeline_entity
SET json = JSON_REMOVE(json, '$.sourceUrl', NULL)
WHERE JSON_VALUE(json, '$.serviceType') IN ('DatabricksPipeline','Fivetran');

-- Remove sourceUrl in dashboard_entity from Mode
UPDATE dashboard_entity
SET json = JSON_REMOVE(json, '$.sourceUrl')
WHERE JSON_VALUE(json, '$.serviceType') in ('Mode');


CREATE TABLE IF NOT EXISTS SERVER_CHANGE_LOG (
    installed_rank INT IDENTITY(1,1),
    version VARCHAR(256) PRIMARY KEY,
    migrationFileName VARCHAR(256) NOT NULL,
    checksum VARCHAR(256) NOT NULL,
    installed_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS SERVER_MIGRATION_SQL_LOGS (
    version VARCHAR(256) NOT NULL,
    sqlStatement VARCHAR(10000) NOT NULL,
    checksum VARCHAR(256) PRIMARY KEY,
    executedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Update test definition parameterValues
UPDATE test_definition
SET json = JSON_INSERT(
	JSON_REMOVE(json, '$.parameterDefinition'),
	'$.parameterDefinition',
	JSON_ARRAY(
		JSON_OBJECT(
			'name', 'minValueForMeanInCol',
			'dataType', 'INT',
			'required', false,
			'description', 'Expected mean value for the column to be greater or equal than',
			'displayName', 'Min'
		),
		JSON_OBJECT(
			'name', 'maxValueForMeanInCol',
			'dataType', 'INT',
			'required', false,
			'description', 'Expected mean value for the column to be lower or equal than',
			'displayName', 'Max'
		)
	)
)
WHERE name = 'columnValueMeanToBeBetween';

UPDATE test_definition
SET json = JSON_INSERT(
	JSON_REMOVE(json, '$.parameterDefinition'),
	'$.parameterDefinition',
	JSON_ARRAY(
		JSON_OBJECT(
	        'name', 'minValueForMedianInCol',
	        'dataType', 'INT',
	        'required', false,
	        'description', 'Expected median value for the column to be greater or equal than',
	        'displayName', 'Min'
		),
		JSON_OBJECT(
        'name', 'maxValueForMedianInCol',
        'dataType', 'INT',
        'required', false,
        'description', 'Expected median value for the column to be lower or equal than',
        'displayName', 'Max'
		)
	)
)
WHERE name = 'columnValueMedianToBeBetween';

UPDATE test_definition
SET json = JSON_INSERT(
	JSON_REMOVE(json, '$.parameterDefinition'),
	'$.parameterDefinition',
	JSON_ARRAY(
		JSON_OBJECT(
	        'name', 'minValueForStdDevInCol',
	        'dataType', 'INT',
	        'required', false,
	        'description', 'Expected std. dev value for the column to be greater or equal than',
	        'displayName', 'Min'
		),
		JSON_OBJECT(
	        'name', 'maxValueForStdDevInCol',
	        'dataType', 'INT',
	        'required', false,
	        'description', 'Expected std. dev value for the column to be lower or equal than',
	        'displayName', 'Max'
		)
	)
)
WHERE name = 'columnValueStdDevToBeBetween';

UPDATE test_definition
SET json = JSON_INSERT(
	JSON_REMOVE(json, '$.parameterDefinition'),
	'$.parameterDefinition',
	JSON_ARRAY(
		JSON_OBJECT(
	        'name', 'minLength',
	        'dataType', 'INT',
	        'required', false,
	        'description', 'The {minLength} for the column value. If minLength is not included, maxLength is treated as upperBound and there will be no minimum value length',
	        'displayName', 'Min'
		),
		JSON_OBJECT(
	        'name', 'maxLength',
	        'dataType', 'INT',
	        'required', false,
	        'description', 'The {maxLength} for the column value. if maxLength is not included, minLength is treated as lowerBound and there will be no maximum value length',
	        'displayName', 'Max'
		)
	)
)
WHERE name = 'columnValueLengthsToBeBetween';

UPDATE test_definition
SET json = JSON_INSERT(
	JSON_REMOVE(json, '$.parameterDefinition'),
	'$.parameterDefinition',
	JSON_ARRAY(
		JSON_OBJECT(
        'name', 'minValue',
        'dataType', 'INT',
        'required', false,
        'description', 'The {minValue} value for the column entry. If minValue is not included, maxValue is treated as upperBound and there will be no minimum',
        'displayName', 'Min'
		),
		JSON_OBJECT(
        'name', 'maxValue',
        'dataType', 'INT',
        'required', false,
        'description', 'The {maxValue} value for the column entry. if maxValue is not included, minValue is treated as lowerBound and there will be no maximum',
        'displayName', 'Max'
		)
	)
)
WHERE name = 'columnValuesToBeBetween';

UPDATE test_definition
SET json = JSON_INSERT(
	JSON_REMOVE(json, '$.parameterDefinition'),
	'$.parameterDefinition',
	JSON_ARRAY(
		JSON_OBJECT(
        'name', 'columnNames',
        'dataType', 'STRING',
        'required', true,
        'description', 'Expected columns names of the table to match the ones in {Column Names} -- should be a coma separated string',
        'displayName', 'Column Names'
		),
		JSON_OBJECT(
        'name', 'ordered',
        'dataType', 'BOOLEAN',
        'required', false,
        'description', 'Whether or not to considered the order of the list when performing the match check',
        'displayName', 'Ordered'
		)
	)
)
WHERE name = 'tableColumnToMatchSet';

UPDATE test_definition
SET json = JSON_INSERT(
	JSON_REMOVE(json, '$.parameterDefinition'),
	'$.parameterDefinition',
	JSON_ARRAY(
		JSON_OBJECT(
        'name', 'minValue',
        'dataType', 'INT',
        'required', false,
        'description', 'Expected number of columns should be greater than or equal to {minValue}. If minValue is not included, maxValue is treated as upperBound and there will be no minimum',
        'displayName', 'Min'
		),
		JSON_OBJECT(
        'name', 'maxValue',
        'dataType', 'INT',
        'required', false,
        'description', 'Expected number of columns should be less than or equal to {maxValue}. If maxValue is not included, minValue is treated as lowerBound and there will be no maximum',
        'displayName', 'Max'
		)
	)
)
WHERE name = 'tableRowCountToBeBetween';

UPDATE test_definition
SET json = JSON_INSERT(
	JSON_REMOVE(json, '$.parameterDefinition'),
	'$.parameterDefinition',
	JSON_ARRAY(
		JSON_OBJECT(
		     'name', 'sqlExpression',
        	'displayName', 'SQL Expression',
        	'description', 'SQL expression to run against the table',
        	'dataType', 'STRING',
        	'required', 'true'
		),
		JSON_OBJECT(
			'name', 'strategy',
	        'displayName', 'Strategy',
    	    'description', 'Strategy to use to run the custom SQL query (i.e. `SELECT COUNT(<col>)` or `SELECT <col> (defaults to ROWS)',
        	'dataType', 'ARRAY',
	        'optionValues', JSON_ARRAY(
        	    'ROWS',
            	'COUNT'
    	    ),
	        'required', false
		),
		JSON_OBJECT(
			'name', 'threshold',
        	'displayName', 'Threshold',
        	'description', 'Threshold to use to determine if the test passes or fails (defaults to 0).',
        	'dataType', 'NUMBER',
        	'required', false
		)
	)
)
WHERE name = 'tableCustomSQLQuery';

-- Modify migrations for service connection of airflow to move password under authType if
-- Connection Type as Mysql or Postgres

UPDATE pipeline_service_entity
SET json = JSON_INSERT(
    JSON_REMOVE(json, '$.connection.config.connection.password'),
    '$.connection.config.connection.authType',
    JSON_OBJECT(),
    '$.connection.config.connection.authType.password',
    JSON_EXTRACT(json, '$.connection.config.connection.password'))
where serviceType = 'Airflow'
AND JSON_UNQUOTE(JSON_EXTRACT(json, '$.connection.config.connection.type')) in ('Postgres', 'Mysql')
AND JSON_EXTRACT(json, '$.connection.config.connection.password') IS NOT NULL;
