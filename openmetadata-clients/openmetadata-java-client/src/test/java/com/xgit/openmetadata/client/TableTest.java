package com.xgit.openmetadata.client;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.xgit.openmetadata.client.config.ClientConfig;
import com.xgit.openmetadata.client.config.LocalServerConfig;
import org.junit.Before;
import org.junit.Test;
import org.openmetadata.client.api.TablesApi;
import org.openmetadata.client.model.*;

public class TableTest extends OpenMetadataTest {

  TablesApi api;

  @Before
  public void setUp() throws Exception {
    api = apiClient().buildClient(TablesApi.class);
  }

  @Override
  protected ClientConfig initClientConfig() {
    return new LocalServerConfig();
  }

  @Test
  public void testList() {
    TableList tableList =
        api.listTables(
            "tags",
            "dtc_dw_doris.dw",
            null,
            "DataShare.Share",
            null,
            // 分页查询
            1,
            null,
            "YXNhc2Fz",
            null);
    System.out.println(tableList);
  }

  @Test
  public void testGetTables() {
    Table table = api.getTableByFQN("superz_template_doris.default.demo.device_data", "*", "all");
    System.out.println(table);
  }

  /** 创建或更新表 */
  @Test
  public void testCreateOrUpdateTable() {
    CreateTable createTable = new CreateTable();
    createTable.setName(String.format("sdk_%s"));
    createTable.setDescription("通过SDK创建的测试数据");
    createTable.setTableType(CreateTable.TableTypeEnum.REGULAR);
    createTable.setDatabaseSchema("superz_template_doris.default.superz");

    // region 创建列
    // Id
    Column cId = new Column();
    cId.setName("id");
    cId.setDataType(Column.DataTypeEnum.INT);
    cId.setDescription("唯一标识列");
    createTable.addColumnsItem(cId);

    // rId
    Column cRId = new Column();
    cRId.setName("r_id");
    cRId.setDataType(Column.DataTypeEnum.INT);
    cRId.setDescription("关联表唯一标识列");
    createTable.addColumnsItem(cRId);

    List<Column> columns = new ArrayList<>();

    Column column;
    // NUMBER
    column = new Column();
    column.setName("c_number");
    column.setDataType(Column.DataTypeEnum.NUMBER);
    column.setDisplayName("数值列");
    columns.add(column);

    // INT
    column = new Column();
    column.setName("c_int");
    column.setDataType(Column.DataTypeEnum.INT);
    column.setDisplayName("整数列");
    columns.add(column);

    // FLOAT
    column = new Column();
    column.setName("c_float");
    column.setDataType(Column.DataTypeEnum.FLOAT);
    column.setDisplayName("浮点数列");
    columns.add(column);

    // DECIMAL
    column = new Column();
    column.setName("c_decimal");
    column.setDataType(Column.DataTypeEnum.DECIMAL);
    columns.add(column);

    // TIMESTAMP
    column = new Column();
    column.setName("c_timestamp");
    column.setDataType(Column.DataTypeEnum.TIMESTAMP);
    column.setDisplayName("时间戳列");
    columns.add(column);

    // DATETIME
    column = new Column();
    column.setName("c_datetime");
    column.setDataType(Column.DataTypeEnum.DATETIME);
    column.setDisplayName("时间列");
    columns.add(column);

    // STRING
    column = new Column();
    column.setName("c_string");
    column.setDataType(Column.DataTypeEnum.STRING);
    column.setDisplayName("字符串列");
    columns.add(column);

    // VARCHAR，只有对于char，varchar等部分类型要求 datalength不允许为空
    column = new Column();
    column.setName("c_varchar");
    column.setDataType(Column.DataTypeEnum.VARCHAR);
    column.setDataLength(255);
    columns.add(column);

    /*
    column=new Column();
    column.setName("c_");
    column.setDataType(Column.DataTypeEnum);
    columns.add(column);
    * */
    createTable.getColumns().addAll(columns);

    /*排序列*/
    Column cSort = new Column();
    cSort.setName("c_sort");
    cSort.setDataType(Column.DataTypeEnum.STRING);
    cSort.setDisplayName("排序列");
    createTable.addColumnsItem(cSort);

    /*唯一值列*/
    Column cUnique = new Column();
    cUnique.setName("c_unique");
    cUnique.setDataType(Column.DataTypeEnum.STRING);
    cUnique.setDisplayName("唯一值列");
    createTable.addColumnsItem(cUnique);

    /*分区列*/
    Column p1 = new Column();
    p1.setName("p1");
    p1.setDataType(Column.DataTypeEnum.STRING);
    p1.setDescription("分区列1");
    createTable.addColumnsItem(p1);

    Column p2 = new Column();
    p2.setName("p2");
    p2.setDataType(Column.DataTypeEnum.STRING);
    p2.setDescription("分区列2");
    createTable.addColumnsItem(p2);

    Column p3 = new Column();
    p3.setName("p3");
    p3.setDataType(Column.DataTypeEnum.STRING);
    p3.setDescription("分区列3");
    createTable.addColumnsItem(p3);
    // endregion

    // 扩展，未定义自定义属性，会报错
    Map<String, Object> extension = new HashMap<>();
    extension.put(
        "createTime",
        // 值类型任意
        // new Date()
        // LocalDateTime.now()
        // "abcd"
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")));
    createTable.extension(extension);

    // 添加约束：主键
    TableConstraint primaryKey = new TableConstraint();
    primaryKey.addColumnsItem(cId.getName());
    primaryKey.constraintType(TableConstraint.ConstraintTypeEnum.PRIMARY_KEY);
    createTable.addTableConstraintsItem(primaryKey);

    // 添加约束：唯一性
    TableConstraint unique;
    unique = new TableConstraint();
    unique.addColumnsItem(cUnique.getName());
    unique.constraintType(TableConstraint.ConstraintTypeEnum.UNIQUE);
    createTable.addTableConstraintsItem(unique);

    // 添加约束：外键
    TableConstraint foreignKey = new TableConstraint();
    foreignKey.addColumnsItem(cRId.getName());
    foreignKey.constraintType(TableConstraint.ConstraintTypeEnum.FOREIGN_KEY);
    createTable.addTableConstraintsItem(foreignKey);

    // 添加约束：排序键
    TableConstraint sortKey = new TableConstraint();
    sortKey.addColumnsItem(cSort.getName());
    sortKey.constraintType(TableConstraint.ConstraintTypeEnum.SORT_KEY);
    createTable.addTableConstraintsItem(sortKey);

    // 分区
    TablePartition tablePartition = new TablePartition();
    tablePartition.columns(Arrays.asList(p1.getName(), p2.getName(), p3.getName()));
    createTable.tablePartition(tablePartition);

    // 标签
    TagLabel dwLevel = new TagLabel();
    dwLevel.name("ODS贴源层");
    dwLevel.source(TagLabel.SourceEnum.CLASSIFICATION);
    dwLevel.tagFQN("数仓分层.ODS贴源层");
    dwLevel.labelType(TagLabel.LabelTypeEnum.MANUAL);
    dwLevel.state(TagLabel.StateEnum.CONFIRMED);
    createTable.addTagsItem(dwLevel);

    Table table = api.createOrUpdateTable(createTable);
    System.out.println(table);
  }
}
