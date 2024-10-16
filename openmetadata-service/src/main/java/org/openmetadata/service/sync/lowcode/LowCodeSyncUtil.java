package org.openmetadata.service.sync.lowcode;

import org.openmetadata.schema.ServiceEntityInterface;
import org.openmetadata.schema.entity.data.Table;
import org.openmetadata.service.sync.ServiceUtil;
import org.openmetadata.service.sync.TableUtil;

public class LowCodeSyncUtil {
  private LowCodeSyncUtil() {}

  public static boolean isSync(ServiceEntityInterface service) {
    return ServiceUtil.isPostgres(service) || ServiceUtil.isMySQL(service) || ServiceUtil.isDoris(service);
  }

  public static boolean isSync(Table table) {
    return TableUtil.isPostgres(table) || TableUtil.isMySQL(table) || TableUtil.isDoris(table);
  }
}
