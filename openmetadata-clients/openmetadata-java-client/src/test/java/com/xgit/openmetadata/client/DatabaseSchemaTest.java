package com.xgit.openmetadata.client;

import org.junit.Before;
import org.junit.Test;
import org.openmetadata.client.api.DatabaseSchemasApi;
import org.openmetadata.client.model.CreateDatabaseSchema;
import org.openmetadata.client.model.DatabaseSchema;

public class DatabaseSchemaTest extends OpenMetadataBaseTest {

  DatabaseSchemasApi api;

  @Before
  public void setUp() throws Exception {
    api = getClient().buildClient(DatabaseSchemasApi.class);
  }

  @Test
  public void testCreateDatabaseSchema() {
    CreateDatabaseSchema createschema = new CreateDatabaseSchema();
    String name = "sdk-schema-" + month();
    createschema.setName(name);
    createschema.setDescription("通过 SDK 创建，" + minute());
    createschema.setDisplayName(name);
    createschema.setDatabase("superz_test_clickhouse.sdk-" + month());
    DatabaseSchema schema = api.createOrUpdateDBSchema(createschema);
    System.out.println(schema);
  }
}
