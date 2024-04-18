package com.xgit.openmetadata.client;

import com.xgit.openmetadata.client.config.ClientConfig;
import com.xgit.openmetadata.client.config.DevServerConfig;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.openmetadata.client.api.DatabaseServicesApi;
import org.openmetadata.client.model.CreateDatabaseService;
import org.openmetadata.client.model.DatabaseService;
import org.openmetadata.client.model.DatabaseServiceList;

public class DatabaseServiceTest extends OpenMetadataTest {
  DatabaseServicesApi api;

  @Before
  public void setUp() throws Exception {
    api = apiClient().buildClient(DatabaseServicesApi.class);
  }

  @Override
  protected ClientConfig initClientConfig() {
    return new DevServerConfig();
  }

  @Test
  public void testListDatabaseServices() {
    DatabaseServicesApi.ListDatabaseServicesQueryParams params =
        new DatabaseServicesApi.ListDatabaseServicesQueryParams();
    DatabaseServiceList list = api.listDatabaseServices(params);
    System.out.println(list);
  }

  @Test
  public void testCreateDatabaseService() {
    String name =
        String.format(
            "sdk_%s", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
    CreateDatabaseService createDatabaseService = new CreateDatabaseService();
    createDatabaseService.setName(name);
    createDatabaseService.setDescription("通过 SDK 创建");
    createDatabaseService.setServiceType(CreateDatabaseService.ServiceTypeEnum.DORIS);
    createDatabaseService.setConnection(null);
    api.createOrUpdateDatabaseService(createDatabaseService);
  }

  @Test
  public void testGetDatabaseServiceByFQN() {
    DatabaseService dbService = api.getDatabaseServiceByFQN("dtc_dw_doris", "%2A", "non-deleted");
    System.out.println(dbService);
  }
}
