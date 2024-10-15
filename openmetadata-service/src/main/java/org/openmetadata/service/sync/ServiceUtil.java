package org.openmetadata.service.sync;

import org.openmetadata.schema.ServiceEntityInterface;
import org.openmetadata.schema.api.services.CreateDatabaseService;

public class ServiceUtil {
  private ServiceUtil() {}

  public static boolean isPostgres(ServiceEntityInterface service) {
    return CreateDatabaseService.DatabaseServiceType.Postgres == service.getServiceType();
  }

  public static boolean isMySQL(ServiceEntityInterface service) {
    return CreateDatabaseService.DatabaseServiceType.Mysql == service.getServiceType();
  }

  public static boolean isClickhouse(ServiceEntityInterface service) {
    return CreateDatabaseService.DatabaseServiceType.Clickhouse == service.getServiceType();
  }

  public static boolean isDoris(ServiceEntityInterface service) {
    return CreateDatabaseService.DatabaseServiceType.Doris == service.getServiceType();
  }

  public static boolean isDatabase(ServiceEntityInterface service) {
    return CreateDatabaseService.DatabaseServiceType.Postgres == service.getServiceType();
  }

  public static boolean isDatabaseSchema(ServiceEntityInterface service) {
    return CreateDatabaseService.DatabaseServiceType.Mysql == service.getServiceType()
        || CreateDatabaseService.DatabaseServiceType.Doris == service.getServiceType()
        || CreateDatabaseService.DatabaseServiceType.Clickhouse == service.getServiceType();
  }
}
