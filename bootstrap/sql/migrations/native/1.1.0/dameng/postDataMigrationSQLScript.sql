ALTER TABLE entity_extension_time_series DROP COLUMN entityFQN;
ALTER TABLE  field_relationship   MODIFY fromFQN VARCHAR(2096) NOT NULL;
ALTER TABLE  field_relationship MODIFY toFQN VARCHAR(2096) NOT NULL;
ALTER TABLE tag_usage   DROP COLUMN targetFQN;