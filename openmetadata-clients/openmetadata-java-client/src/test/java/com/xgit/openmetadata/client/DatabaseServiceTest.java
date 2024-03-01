package com.xgit.openmetadata.client;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.openmetadata.client.api.DatabaseServicesApi;
import org.openmetadata.client.model.CreateDatabaseService;

public class DatabaseServiceTest extends OpenMetadataBaseTest {
  DatabaseServicesApi api;

  @Before
  public void setUp() throws Exception {
    api = getClient().buildClient(DatabaseServicesApi.class);
  }

  @Test
  public void testCreateDatabaseService() {
    String name = String.format("sdk_%s", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
    CreateDatabaseService createDatabaseService = new CreateDatabaseService();
    createDatabaseService.setName(name);
    createDatabaseService.setDescription("通过 SDK 创建");
    createDatabaseService.setServiceType(CreateDatabaseService.ServiceTypeEnum.DORIS);
    createDatabaseService.setConnection(null);
    api.createOrUpdateDatabaseService(createDatabaseService);
  }
}
