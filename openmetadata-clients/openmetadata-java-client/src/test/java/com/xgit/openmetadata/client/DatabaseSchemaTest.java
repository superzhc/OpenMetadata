package com.xgit.openmetadata.client;

import com.xgit.openmetadata.client.config.ClientConfig;
import com.xgit.openmetadata.client.config.LocalServerConfig;
import org.junit.Before;
import org.junit.Test;
import org.openmetadata.client.api.DatabaseSchemasApi;
import org.openmetadata.client.model.CreateDatabaseSchema;
import org.openmetadata.client.model.DatabaseSchema;
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig;

public class DatabaseSchemaTest extends OpenMetadataTest {
  DatabaseSchemasApi api;

  @Before
  public void setUp() throws Exception {
    api = apiClient().buildClient(DatabaseSchemasApi.class);
  }

  @Override
  protected ClientConfig initClientConfig() {
    return new LocalServerConfig();
  }

  @Test
  public void testCreateDatabaseSchema() {
    CreateDatabaseSchema createschema = new CreateDatabaseSchema();
    String name = "sdk-schema";
    createschema.setName(name);
    createschema.setDescription("通过 SDK 创建");
    createschema.setDisplayName(name);
    createschema.setDatabase("superz_test_clickhouse.sdk");
    DatabaseSchema schema = api.createOrUpdateDBSchema(createschema);
    System.out.println(schema);
  }
}
