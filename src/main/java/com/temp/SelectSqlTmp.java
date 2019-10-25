package com.temp;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 插入语句，select 表字段为sql关联多个表字段
 */
public class SelectSqlTmp extends SelectTableColumnTmpBase {


    //<表别名,<字段名>>
    private Map<String, List<String>> tableAliaColumns = new HashMap<String, List<String>>();

    private String aliaName;//别名

    public SelectSqlTmp(int type) {

        super(type);
    }


    public Map<String, List<String>> getTableAliaColumns() {
        return tableAliaColumns;
    }

    public void setTableAliaColumns(Map<String, List<String>> tableAliaColumns) {
        this.tableAliaColumns = tableAliaColumns;
    }


    public String getAliaName() {
        return aliaName;
    }

    public void setAliaName(String aliaName) {
        this.aliaName = aliaName;
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


    /**
     * 通过别名获取，表和字段名关联
     *
     * @param tableAliasmap
     * @return
     */
    public Map<String, List<String>> getTableColumnsByAlia(Map<String, String> tableAliasmap) {

        Map<String, List<String>> tableColumns = new HashMap<String, List<String>>();
        if (tableAliasmap == null || tableAliasmap.size() <= 0) {
            return tableColumns;
        }

        for (String tableAlias : tableAliaColumns.keySet()) {

            //通过别名获取表真名
            String tableName = tableAliasmap.get(tableAlias);
            if (StringUtils.isBlank(tableName)) {
                continue;
            }

            List<String> columns = tableAliaColumns.get(tableAlias);

            tableColumns.put(tableName, columns);
        }

        return tableColumns;
    }
}
