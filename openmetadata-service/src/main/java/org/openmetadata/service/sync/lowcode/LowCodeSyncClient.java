package org.openmetadata.service.sync.lowcode;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.openmetadata.api.configuration.HanYunConfiguration;
import org.openmetadata.schema.EntityInterface;
import org.openmetadata.schema.ServiceEntityInterface;
import org.openmetadata.schema.entity.data.Database;
import org.openmetadata.schema.entity.data.DatabaseSchema;
import org.openmetadata.schema.entity.data.Table;
import org.openmetadata.schema.entity.services.DatabaseService;
import org.openmetadata.schema.services.connections.database.ClickhouseConnection;
import org.openmetadata.schema.services.connections.database.DorisConnection;
import org.openmetadata.schema.services.connections.database.MysqlConnection;
import org.openmetadata.schema.services.connections.database.PostgresConnection;
import org.openmetadata.schema.services.connections.database.common.basicAuth;
import org.openmetadata.schema.type.ChangeDescription;
import org.openmetadata.schema.type.Column;
import org.openmetadata.schema.type.EntityReference;
import org.openmetadata.service.Entity;
import org.openmetadata.service.sync.ServiceUtil;
import org.openmetadata.service.sync.SyncClient;
import org.openmetadata.service.sync.TableUtil;
import org.openmetadata.service.util.*;

@Slf4j
public class LowCodeSyncClient extends SyncClient {

  static class SourceRequest {
    @Getter @Setter private String code;
    @Getter @Setter private String name;
    @Getter @Setter private String dbDriver;
    @Getter @Setter private String dbUrl;
    @Getter @Setter private String dbUsername;
    @Getter @Setter private String dbPassword;
    @Getter @Setter private String dbName;
    @Getter @Setter private String dbType;
    @Getter @Setter private String dbTypeExt;
    @Getter @Setter private String properties;
    @Getter @Setter private String remark;
  }

  static class TableRequest {
    @Getter @Setter private String dataOrigin = "openmetadata";
    @Getter @Setter private String dataSourceCode;
    @Getter @Setter private String tableName;
    @Getter @Setter private String tableTxt;
    @Getter @Setter private String dataTableScheme;
    @Getter @Setter private String dataTag;
    @Getter private List<ColumnRequest> fields = new ArrayList<>();

    public TableRequest addField(ColumnRequest field) {
      fields.add(field);
      return this;
    }
  }

  static class ColumnRequest {
    @Getter @Setter private String dbFieldName;
    @Getter @Setter private String dbFieldNameOld;
    @Getter @Setter private String dbFieldTxt;
    @Getter @Setter private int dbIsPersist = 1;
    @Getter @Setter private String dbLength;
    @Getter @Setter private String dbType;
    @Getter @Setter private int isShowList = 0;
    @Getter @Setter private OperateType optType;
  }

  static enum OperateType {
    Add("add"),
    Delete("delete"),
    Update("update");

    private final String value;

    private static final Map<String, OperateType> CONSTANTS = new HashMap<String, OperateType>();

    static {
      for (OperateType c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }

    OperateType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    @JsonValue
    public String value() {
      return this.value;
    }

    @JsonCreator
    public static OperateType fromValue(String value) {
      OperateType constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }
  }

  private static final String SOURCE_CODE_TEMPLATE = "%s.%s";
  public static final String COLUMN_PATTERN_FORMAT = "columns\\.([_\\w]+)\\.(%s)";

  private HanYunConfiguration configuration;
  private final String serviceURL;
  private static final String API_ENDPOINT = "/hanyun";
  private static final String SOURCE_CREATE_OR_UPDATE_ENDPOINT = "/sys/dataSource/saveOrEdit";
  private static final String SOURCE_DELETE_ENDPOINT = "/sys/dataSource/deleteByCondition";
  private static final String TABLE_CREATE_OR_UPDATE_ENDPOINT = "/sys/table/cgform/api/saveDbTableMetaInfo";
  private static final String TABLE_DELETE_ENDPOINT = "/sys/table/cgform/head/delete";

  public LowCodeSyncClient(HanYunConfiguration configuration) {
    this.configuration = configuration;
    this.serviceURL = configuration.getApiEndpoint();
  }

  @Override
  public void createSource(ServiceEntityInterface service, EntityInterface entity) {
    SourceRequest sourceRequest = buildSourceRequest(service, entity);
    if (null == sourceRequest) {
      return;
    }

    createOrUpdateSource(sourceRequest);
  }

  @Override
  public boolean isUpdateSource(ChangeDescription changeDescription) {
    return EntityUtil.containFieldsChanged(changeDescription, Entity.FIELD_DISPLAY_NAME, Entity.FIELD_DESCRIPTION);
  }

  @Override
  public void updateSource(ServiceEntityInterface service, EntityInterface entity) {
    SourceRequest sourceRequest = buildSourceRequest(service, entity);
    if (null == sourceRequest) {
      return;
    }

    createOrUpdateSource(sourceRequest);
  }

  private void createOrUpdateSource(SourceRequest requestBody) {
    String createOrUpdateURL = String.format("%s%s%s", this.serviceURL, API_ENDPOINT, SOURCE_CREATE_OR_UPDATE_ENDPOINT);

    Map<String, String> headers = new HashMap<>();

    String responseBody = HttpClientUtils.doPost(createOrUpdateURL, headers, JsonUtils.pojoToJson(requestBody));
    LOG.debug("Sync Response : {}", responseBody);
  }

  @Override
  public void deleteSource(ServiceEntityInterface service, EntityInterface entity) {
    // 删除数据源
    String sourceCode = sourceCode(entity.getEntityReference());

    String deleteURL = String.format("%s%s%s", this.serviceURL, API_ENDPOINT, SOURCE_DELETE_ENDPOINT);

    Map<String, String> headers = new HashMap<>();

    Map<String, String> requestBody = new HashMap<>();
    requestBody.put("code", sourceCode);

    String responseBody = HttpClientUtils.doPost(deleteURL, headers, JsonUtils.pojoToJson(requestBody));
    LOG.debug("Sync Response : {}", responseBody);
  }

  @Override
  public void softDeleteOrRestoreSource(ServiceEntityInterface service, EntityInterface entity, boolean delete) {
    if (delete) {
      deleteSource(service, entity);
    } else {
      createSource(service, entity);
    }
  }

  private SourceRequest buildSourceRequest(ServiceEntityInterface service, EntityInterface entity) {
    SourceRequest sourceRequest = null;
    if (ServiceUtil.isDatabase(service)) {
      sourceRequest = buildMetaDatabaseSourceRequest((DatabaseService) service, (Database) entity);
    } else if (ServiceUtil.isDatabaseSchema(service)) {
      sourceRequest = buildMetaDatabaseSchemaSourceRequest((DatabaseService) service, (DatabaseSchema) entity);
    } else {
      LOG.warn("{} 不支持同步", service.getServiceType());
    }
    return sourceRequest;
  }

  private SourceRequest buildMetaDatabaseSourceRequest(DatabaseService service, Database database) {
    SourceRequest sourceRequest = buildDBServiceSourceRequest(service);
    String sourceDBName = database.getName();
    sourceRequest.setDbName(sourceDBName);
    sourceRequest.setCode(sourceCode(database.getEntityReference()));
    sourceRequest.setName(database.getFullyQualifiedName());
    sourceRequest.setRemark(database.getDescription());

    return sourceRequest;
  }

  private SourceRequest buildMetaDatabaseSchemaSourceRequest(DatabaseService service, DatabaseSchema databaseSchema) {
    SourceRequest sourceRequest = buildDBServiceSourceRequest(service);
    String sourceDBName = databaseSchema.getName();
    sourceRequest.setDbName(sourceDBName);
    sourceRequest.setCode(sourceCode(databaseSchema.getEntityReference()));
    sourceRequest.setName(databaseSchema.getFullyQualifiedName());
    sourceRequest.setRemark(databaseSchema.getDescription());

    return sourceRequest;
  }

  private SourceRequest buildDBServiceSourceRequest(DatabaseService service) {
    return buildDBServiceSourceRequest(new SourceRequest(), service);
  }

  private SourceRequest buildDBServiceSourceRequest(SourceRequest sourceRequest, DatabaseService service) {
    // 数据库类型
    sourceRequest.setDbType(service.getServiceType().value());

    // 连接信息
    if (ServiceUtil.isPostgres(service)) {
      PostgresConnection connection = ClassUtil.convert(service.getConnection().getConfig());
      sourceRequest.setDbUrl(connection.getHostPort());
      sourceRequest.setDbUsername(connection.getUsername());
      sourceRequest.setDbPassword(((basicAuth) connection.getAuthType()).getPassword());
    } else if (ServiceUtil.isMySQL(service)) {
      MysqlConnection connection = ClassUtil.convert(service.getConnection().getConfig());
      sourceRequest.setDbUrl(connection.getHostPort());
      sourceRequest.setDbUsername(connection.getUsername());
      sourceRequest.setDbPassword(((basicAuth) connection.getAuthType()).getPassword());
    } else if (ServiceUtil.isDoris(service)) {
      DorisConnection connection = ClassUtil.convert(service.getConnection().getConfig());
      sourceRequest.setDbUrl(connection.getHostPort());
      sourceRequest.setDbUsername(connection.getUsername());
      sourceRequest.setDbPassword(connection.getPassword());
    } else if (ServiceUtil.isClickhouse(service)) {
      ClickhouseConnection connection = ClassUtil.convert(service.getConnection().getConfig());
      sourceRequest.setDbUrl(connection.getHostPort());
      sourceRequest.setDbUsername(connection.getUsername());
      sourceRequest.setDbPassword(connection.getPassword());
    }
    return sourceRequest;
  }

  @Override
  public void createTable(Table table) {
    TableRequest tableRequest = buildTableRequest(table, table.getColumns(), null);
    createOrUpdateTable(tableRequest);
  }

  @Override
  public boolean isUpdateTable(ChangeDescription changeDescription) {
    return EntityUtil.containFieldsChanged(
        changeDescription,
        Entity.FIELD_DESCRIPTION,
        "columns",
        String.format(
            COLUMN_PATTERN_FORMAT,
            String.join("|", "dataType", "arrayDataType", "dataLength", Entity.FIELD_DESCRIPTION)));
  }

  @Override
  public void updateTable(Table table, List<Column> addedColumns, List<Column> deletedColumns) {
    TableRequest tableRequest = buildTableRequest(table, addedColumns, deletedColumns);
    createOrUpdateTable(tableRequest);
  }

  private void createOrUpdateTable(TableRequest requestBody) {
    String createOrUpdateURL = String.format("%s%s%s", this.serviceURL, API_ENDPOINT, TABLE_CREATE_OR_UPDATE_ENDPOINT);

    Map<String, String> headers = new HashMap<>();

    String responseBody = HttpClientUtils.doPost(createOrUpdateURL, headers, JsonUtils.pojoToJson(requestBody));
    LOG.debug("Sync Response : {}", responseBody);
  }

  @Override
  public void deleteTable(Table table) {
    String deleteURL = String.format("%s%s%s", this.serviceURL, API_ENDPOINT, TABLE_DELETE_ENDPOINT);

    Map<String, String> headers = new HashMap<>();

    Map<String, String> params = new HashMap<>();
    params.put("dataSourceCode", tableSourceCode(table));
    params.put("tableName", table.getName());

    String responseBody = HttpClientUtils.doGet(deleteURL, headers, params);
    LOG.debug("Sync Response : {}", responseBody);
  }

  @Override
  public void softDeleteOrRestoreTable(Table table, boolean delete) {
    if (delete) {
      deleteTable(table);
    } else {
      createTable(table);
    }
  }

  private TableRequest buildTableRequest(Table table, List<Column> addedColumns, List<Column> deletedColumns) {
    TableRequest tableRequest = new TableRequest();
    tableRequest.setTableName(table.getName());
    tableRequest.setTableTxt(table.getDescription());

    tableRequest.setDataSourceCode(tableSourceCode(table));

    // 三层结构
    if (TableUtil.isDatabase(table)) {
      tableRequest.setDataTableScheme(table.getDatabaseSchema().getName());
    }

    Set<String> addedColumnNames =
        (null == addedColumns || addedColumns.size() == 0)
            ? new HashSet<>()
            : addedColumns.stream().map(Column::getName).collect(Collectors.toSet());

    for (Column column : table.getColumns()) {
      ColumnRequest columnRequest = buildColumnRequest(column);

      if (addedColumnNames.contains(column.getName())) {
        columnRequest.setOptType(OperateType.Add);
      } else {
        columnRequest.setOptType(OperateType.Update);
      }

      tableRequest.addField(columnRequest);
    }

    // 删除的列
    if (null != deletedColumns && deletedColumns.size() > 0) {
      for (Column deletedColumn : deletedColumns) {
        ColumnRequest columnRequest = buildColumnRequest(deletedColumn);
        columnRequest.setOptType(OperateType.Delete);
        tableRequest.addField(columnRequest);
      }
    }

    return tableRequest;
  }

  private String tableSourceCode(Table table) {
    String code;
    if (TableUtil.isDatabase(table)) {
      code = sourceCode(table.getDatabase());
    } else {
      code = sourceCode(table.getDatabaseSchema());
    }
    return code;
  }

  private ColumnRequest buildColumnRequest(Column column) {
    ColumnRequest columnRequest = new ColumnRequest();
    columnRequest.setDbFieldName(column.getName());
    columnRequest.setDbFieldTxt(column.getDescription());

    if (column.getDataLength() != null) {
      columnRequest.setDbLength(String.valueOf(column.getDataLength()));
    }

    columnRequest.setDbType(column.getDataType().value());
    return columnRequest;
  }

  private String sourceCode(EntityReference entityReference) {
    return entityReference.getFullyQualifiedName();
  }
}
