package com.xgit.openmetadata.client;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.Before;
import org.junit.Test;
import org.openmetadata.client.api.DatabasesApi;
import org.openmetadata.client.model.CreateDatabase;
import org.openmetadata.client.model.Database;

/**
 * 数据库相关服务类型
 */
public class DatabaseTest extends OpenMetadataBaseTest {

    DatabasesApi api;

    @Before
    public void setUp() throws Exception {
        api = getClient().buildClient(DatabasesApi.class);
    }

    @Test
    public void testCreateOrUpdateDatabase() {
        String name = String.format("sdk-%s", month());
        CreateDatabase createdatabase = new CreateDatabase();
        createdatabase.name(name);
        createdatabase.description("通过 SDK 创建数据库，" + minute());
        createdatabase.displayName(name);
        createdatabase.setService("superz_test_clickhouse");
        Database database = api.createOrUpdateDatabase(createdatabase);
        System.out.println(database);
    }
}
