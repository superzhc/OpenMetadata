ALTER TABLE test_suite MODIFY (nameHash VARCHAR(256));
ALTER TABLE test_suite RENAME COLUMN nameHash TO fqnHash;