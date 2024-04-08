package com.xgit.openmetadata.client;

import com.xgit.openmetadata.client.config.ClientConfig;
import com.xgit.openmetadata.client.config.LocalServerConfig;
import org.junit.Before;
import org.junit.Test;
import org.openmetadata.client.api.DatabasesApi;
import org.openmetadata.client.model.CreateDatabase;
import org.openmetadata.client.model.Database;

/** 数据库相关服务类型 */
public class DatabaseTest extends OpenMetadataTest {

  DatabasesApi api;

  @Before
  public void setUp() throws Exception {
    api = apiClient().buildClient(DatabasesApi.class);
  }

  @Override
  protected ClientConfig initClientConfig() {
    return new LocalServerConfig();
  }

  @Test
  public void testCreateOrUpdateDatabase() {
    String name = String.format("sdk-%s");
    CreateDatabase createdatabase = new CreateDatabase();
    createdatabase.name(name);
    createdatabase.description("通过 SDK 创建数据库，");
    createdatabase.displayName(name);
    createdatabase.setService("superz_test_clickhouse");
    Database database = api.createOrUpdateDatabase(createdatabase);
    System.out.println(database);
  }
}
