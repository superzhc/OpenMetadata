UPDATE messaging_service_entity
SET json = REGEXP_REPLACE(
    json,
    '"saslMechanism":"[^"]*"',
    '"saslMechanism":"PLAIN"'
)
WHERE (serviceType = 'Kafka' OR serviceType = 'Redpanda')
  AND json LIKE '%"saslMechanism":%'
  AND NOT REGEXP_LIKE(json, '"saslMechanism":"(GSSAPI|PLAIN|SCRAM-SHA-256|SCRAM-SHA-512|OAUTHBEARER)"');

-- Remove the Subscriptions
DELETE FROM event_subscription_entity;

-- Clean old test connections
TRUNCATE TABLE automations_workflow;

-- Remove the ibmi scheme from Db2 replace it with db2+ibm_db
UPDATE dbservice_entity
SET json = JSON_SET(json, '$.connection.config.scheme', 'db2+ibm_db')
WHERE serviceType  = 'Db2';