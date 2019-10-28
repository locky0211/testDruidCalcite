package com.tmp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 插入语句，select 表字段为sql，sql中有case选择
 */
public class SelectSqlCaseTmp extends SelectTableColumnTmpBase {


    //<表别名,<字段名>>
    private Map<String, List<String>> tableAliaColumns = new HashMap<String, List<String>>();

    private String columnAlias;//整个case结果，作为别名


    //<表别名,表名>
    private  Map<String, String> tableAliasmap = new HashMap<String, String>();

    public SelectSqlCaseTmp(int type) {

        super(type);
    }



    public Map<String, List<String>> getTableAliaColumns() {
        return tableAliaColumns;
    }

    public void setTableAliaColumns(Map<String, List<String>> tableAliaColumns) {
        this.tableAliaColumns = tableAliaColumns;
    }

    public String getColumnAlias() {
        return columnAlias;
    }

    public void setColumnAlias(String columnAlias) {
        this.columnAlias = columnAlias;
    }

    public Map<String, String> getTableAliasmap() {
        return tableAliasmap;
    }

    public void setTableAliasmap(Map<String, String> tableAliasmap) {
        this.tableAliasmap = tableAliasmap;
    }

    /**
     * 添加源表、字段数据
     *
     * @param tableAliaName
     * @param sourceTableColumn
     */
    public void addSourceTableInfo(String tableAliaName, String sourceTableColumn) {

        List<String> tableColumns = tableAliaColumns.get(tableAliaName);
        if (tableColumns == null) {
            tableColumns = new ArrayList<String>();
            tableAliaColumns.put(tableAliaName, tableColumns);
        }

        tableColumns.add(sourceTableColumn);
    }
}
