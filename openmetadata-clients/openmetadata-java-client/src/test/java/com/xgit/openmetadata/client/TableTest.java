package com.xgit.openmetadata.client;

import org.junit.Before;
import org.junit.Test;
import org.openmetadata.client.api.TablesApi;
import org.openmetadata.client.model.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TableTest extends OpenMetadataBaseTest {

    TablesApi api;

    @Before
    public void setUp() throws Exception {
        api = getClient().buildClient(TablesApi.class);
    }

    @Test
    public void testGetTables() {
        Table table = api.getTableByFQN("superz_template_doris.default.demo.device_data", "*", "all");
        System.out.println(table);
    }

    @Test
    public void testCreateOrUpdateTable() {
        CreateTable createTable = new CreateTable();
        createTable.setName(String.format("sdk-table-%s", minute()));
        createTable.setDescription("Create By SDK, " + minute());
        createTable.setTableType(CreateTable.TableTypeEnum.REGULAR);

        String monthWithYear = month();
        createTable.setDatabaseSchema(String.format("superz_test_clickhouse.sdk-%s.sdk-schema-%s", monthWithYear, monthWithYear));

        List<Column> columns = new ArrayList<>();
//        for (int i = 1; i <= 50; i++) {
//            Column column = new Column();
//            column.setName("sdk-c" + i);
//            column.setDataType(Column.DataTypeEnum.STRING);
//            column.setDataTypeDisplay("string");
//            // column.setConstraint(Column.ConstraintEnum.NOT_NULL);
//            columns.add(column);
//        }

        //region 创建列
        Column column;
        // Id
        column=new Column();
        column.setName("id");
        column.setDataType(Column.DataTypeEnum.INT);
        columns.add(column);

        // NUMBER
        column = new Column();
        column.setName("c_number");
        column.setDataType(Column.DataTypeEnum.NUMBER);
        columns.add(column);

        // INT
        column = new Column();
        column.setName("c_int");
        column.setDataType(Column.DataTypeEnum.INT);
        columns.add(column);

        // FLOAT
        column = new Column();
        column.setName("c_float");
        column.setDataType(Column.DataTypeEnum.FLOAT);
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
        columns.add(column);

        // DATETIME
        column = new Column();
        column.setName("c_datetime");
        column.setDataType(Column.DataTypeEnum.DATETIME);
        columns.add(column);

        // STRING
        column = new Column();
        column.setName("c_string");
        column.setDataType(Column.DataTypeEnum.STRING);
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

        column=new Column();
        column.setName("c_unique");
        column.setDataType(Column.DataTypeEnum.STRING);
        columns.add(column);


        createTable.setColumns(columns);
        // endregion

//        // 扩展，未定义自定义属性，会报错
//        Map<String,Object> extension=new HashMap<>();
//        extension.put("partionBy","id");
//        createTable.extension(extension);

        // 添加约束：主键
        TableConstraint primaryKey=new TableConstraint();
        primaryKey.addColumnsItem("c_string").addColumnsItem("c_varchar");
        primaryKey.constraintType(TableConstraint.ConstraintTypeEnum.PRIMARY_KEY);
        createTable.addTableConstraintsItem(primaryKey);

        TableConstraint unique;
        // 添加约束：唯一性
        unique=new TableConstraint();
        unique.addColumnsItem("c_unique");
        unique.constraintType(TableConstraint.ConstraintTypeEnum.UNIQUE);
        createTable.addTableConstraintsItem(unique);

        // 分区
        TablePartition tablePartition=new TablePartition();
        tablePartition.addColumnsItem("c_timestamp");
        createTable.tablePartition(tablePartition);

        Table table = api.createOrUpdateTable(createTable);
        System.out.println(table);
    }
}
