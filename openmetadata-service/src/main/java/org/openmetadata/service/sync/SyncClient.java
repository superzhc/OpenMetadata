package org.openmetadata.service.sync;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.openmetadata.schema.EntityInterface;
import org.openmetadata.schema.ServiceEntityInterface;
import org.openmetadata.schema.entity.data.Table;
import org.openmetadata.schema.type.ChangeDescription;
import org.openmetadata.schema.type.Column;
import org.openmetadata.service.util.EntityUtil;

@Slf4j
public abstract class SyncClient {
  public void createSource(ServiceEntityInterface service, EntityInterface entity) {}

  public boolean isUpdateSource(ChangeDescription changeDescription) {
    return EntityUtil.isFieldsChanged(changeDescription);
  }

  public void updateSource(ServiceEntityInterface service, EntityInterface entity) {}

  public void deleteSource(ServiceEntityInterface service, EntityInterface entity) {}

  public void softDeleteOrRestoreSource(ServiceEntityInterface service, EntityInterface entity, boolean delete) {}

  public void createTable(Table table) {}

  public boolean isUpdateTable(ChangeDescription changeDescription) {
    return EntityUtil.isFieldsChanged(changeDescription);
  }

  public void updateTable(Table table, List<Column> addedColumns, List<Column> deletedColumns) {}

  public void deleteTable(Table table) {}

  public void softDeleteOrRestoreTable(Table table, boolean delete) {}
}
