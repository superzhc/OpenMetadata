package org.openmetadata.service.sync;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.openmetadata.schema.EntityInterface;
import org.openmetadata.schema.entity.data.Database;
import org.openmetadata.schema.entity.data.DatabaseSchema;
import org.openmetadata.schema.entity.data.Table;
import org.openmetadata.schema.entity.services.DatabaseService;
import org.openmetadata.schema.entity.services.ServiceType;
import org.openmetadata.schema.type.ChangeDescription;
import org.openmetadata.schema.type.Column;
import org.openmetadata.schema.type.FieldChange;
import org.openmetadata.schema.type.Include;
import org.openmetadata.service.Entity;
import org.openmetadata.service.OpenMetadataApplicationConfig;
import org.openmetadata.service.sync.lowcode.LowCodeSyncClient;
import org.openmetadata.service.util.EntityUtil;
import org.openmetadata.service.util.JsonUtils;

@Slf4j
public class SyncRepository {
  // 支持同时同步多个平台
  private final List<SyncClient> clients = new ArrayList<>();

  @Getter private OpenMetadataApplicationConfig config;

  public SyncRepository(OpenMetadataApplicationConfig config) {
    this.config = config;

    // 注册同步客户端
    addClient(new LowCodeSyncClient(config.getHanYunConfiguration()));

    Entity.setSyncRepository(this);
  }

  public SyncRepository addClient(SyncClient client) {
    clients.add(client);
    return this;
  }

  public void createEntity(EntityInterface entity) {
    if (null != entity) {
      String entityType = entity.getEntityReference().getType();

      try {
        switch (entityType) {
          case Entity.DATABASE:
            {
              final DatabaseService service =
                  Entity.getServiceEntity(
                      ServiceType.DATABASE, ((Database) entity).getService(), "", Include.NON_DELETED);
              if (ServiceUtil.isDatabase(service)) {
                clients.forEach(client -> client.createSource(service, entity));
              }
            }
            break;
          case Entity.DATABASE_SCHEMA:
            {
              final DatabaseService service =
                  Entity.getServiceEntity(
                      ServiceType.DATABASE, ((DatabaseSchema) entity).getService(), "", Include.NON_DELETED);
              if (ServiceUtil.isDatabaseSchema(service)) {
                clients.forEach(client -> client.createSource(service, entity));
              }
            }
            break;
          case Entity.TABLE:
            clients.forEach(client -> client.createTable((Table) entity));
            break;
        }
      } catch (Exception ex) {
        LOG.error(
            "Sync Failed. Reason[{}], Cause[{}], Stack [{}]",
            ex.getMessage(),
            ex.getCause(),
            ExceptionUtils.getStackTrace(ex));
      }
    }
  }

  public void updateEntity(EntityInterface entity, ChangeDescription changeDescription) {
    if (null != entity) {
      String entityType = entity.getEntityReference().getType();

      try {
        switch (entityType) {
          case Entity.DATABASE:
            {
              final DatabaseService service =
                  Entity.getServiceEntity(
                      ServiceType.DATABASE, ((Database) entity).getService(), "", Include.NON_DELETED);
              if (ServiceUtil.isDatabase(service)) {
                clients.forEach(
                    client -> {
                      if (client.isUpdateSource(changeDescription)) {
                        client.updateSource(service, entity);
                      }
                    });
              }
            }
            break;
          case Entity.DATABASE_SCHEMA:
            {
              final DatabaseService service =
                  Entity.getServiceEntity(
                      ServiceType.DATABASE, ((DatabaseSchema) entity).getService(), "", Include.NON_DELETED);
              if (ServiceUtil.isDatabaseSchema(service)) {
                clients.forEach(
                    client -> {
                      if (client.isUpdateSource(changeDescription)) {
                        client.updateSource(service, entity);
                      }
                    });
              }
            }
            break;
          case Entity.TABLE:
            clients.forEach(
                client -> {
                  if (client.isUpdateTable(changeDescription)) {
                    // 获取新增字段
                    FieldChange columnsFieldAddedChange = EntityUtil.fieldAdded(changeDescription, "columns");
                    List<Column> addedColumns =
                        null == columnsFieldAddedChange
                            ? new ArrayList<>()
                            : JsonUtils.readObjects((String) columnsFieldAddedChange.getNewValue(), Column.class);

                    // 获取删除字段
                    FieldChange columnsFieldDeletedChange = EntityUtil.fieldDeleted(changeDescription, "columns");
                    List<Column> deletedColumns =
                        null == columnsFieldDeletedChange
                            ? new ArrayList<>()
                            : JsonUtils.readObjects((String) columnsFieldDeletedChange.getOldValue(), Column.class);

                    client.updateTable((Table) entity, addedColumns, deletedColumns);
                  }
                });
            break;
        }
      } catch (Exception ex) {
        LOG.error(
            "Sync Failed. Reason[{}], Cause[{}], Stack [{}]",
            ex.getMessage(),
            ex.getCause(),
            ExceptionUtils.getStackTrace(ex));
      }
    }
  }

  public void deleteEntity(EntityInterface entity) {
    if (null != entity) {
      String entityType = entity.getEntityReference().getType();

      try {
        switch (entityType) {
          case Entity.DATABASE:
            {
              final DatabaseService service =
                  Entity.getServiceEntity(
                      ServiceType.DATABASE, ((Database) entity).getService(), "", Include.NON_DELETED);
              if (ServiceUtil.isDatabase(service)) {
                clients.forEach(client -> client.deleteSource(service, entity));
              }
            }
            break;
          case Entity.DATABASE_SCHEMA:
            {
              final DatabaseService service =
                  Entity.getServiceEntity(
                      ServiceType.DATABASE, ((DatabaseSchema) entity).getService(), "", Include.NON_DELETED);
              if (ServiceUtil.isDatabaseSchema(service)) {
                clients.forEach(client -> client.deleteSource(service, entity));
              }
            }
            break;
          case Entity.TABLE:
            clients.forEach(client -> client.deleteTable((Table) entity));
            break;
        }
      } catch (Exception ex) {
        LOG.error(
            "Sync Failed. Reason[{}], Cause[{}], Stack [{}]",
            ex.getMessage(),
            ex.getCause(),
            ExceptionUtils.getStackTrace(ex));
      }
    }
  }

  public void softDeleteOrRestoreEntity(EntityInterface entity, final boolean delete) {
    if (null != entity) {
      String entityType = entity.getEntityReference().getType();

      try {
        switch (entityType) {
          case Entity.DATABASE:
            {
              final DatabaseService service =
                  Entity.getServiceEntity(
                      ServiceType.DATABASE, ((Database) entity).getService(), "", Include.NON_DELETED);
              if (ServiceUtil.isDatabase(service)) {
                clients.forEach(client -> client.softDeleteOrRestoreSource(service, entity, delete));
              }
            }
            break;
          case Entity.DATABASE_SCHEMA:
            {
              final DatabaseService service =
                  Entity.getServiceEntity(
                      ServiceType.DATABASE, ((DatabaseSchema) entity).getService(), "", Include.NON_DELETED);
              if (ServiceUtil.isDatabaseSchema(service)) {
                clients.forEach(client -> client.softDeleteOrRestoreSource(service, entity, delete));
              }
            }
            break;
          case Entity.TABLE:
            clients.forEach(client -> client.softDeleteOrRestoreTable((Table) entity, delete));
            break;
        }
      } catch (Exception ex) {
        LOG.error(
            "Sync Failed. Reason[{}], Cause[{}], Stack [{}]",
            ex.getMessage(),
            ex.getCause(),
            ExceptionUtils.getStackTrace(ex));
      }
    }
  }
}
