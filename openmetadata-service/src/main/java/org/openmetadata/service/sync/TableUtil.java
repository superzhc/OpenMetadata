package org.openmetadata.service.sync;

import org.openmetadata.schema.api.services.CreateDatabaseService;
import org.openmetadata.schema.entity.data.Table;

public class TableUtil {
  private TableUtil() {}

  public static boolean isPostgres(Table table) {
    return CreateDatabaseService.DatabaseServiceType.Postgres == table.getServiceType();
  }

  public static boolean isMySQL(Table table) {
    return CreateDatabaseService.DatabaseServiceType.Mysql == table.getServiceType();
  }

  public static boolean isClickhouse(Table table) {
    return CreateDatabaseService.DatabaseServiceType.Clickhouse == table.getServiceType();
  }

  public static boolean isDoris(Table table) {
    return CreateDatabaseService.DatabaseServiceType.Doris == table.getServiceType();
  }

  public static boolean isDatabase(Table table) {
    return CreateDatabaseService.DatabaseServiceType.Postgres == table.getServiceType();
  }
}
