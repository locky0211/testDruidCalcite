package com.tmp;

/**
 * 插入语句，select 表字段
 */
public class SelectTableColumnTmp extends  SelectTableColumnTmpBase{


    private String tableAlias ="";//表别名

    private String columnNames;//字段名

    private String columnAlias;//字段别名

    public SelectTableColumnTmp(int type) {

       super(type);
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String columnNames) {
        this.columnNames = columnNames;
    }


    public String getColumnAlias() {
        return columnAlias;
    }

    public void setColumnAlias(String columnAlias) {
        this.columnAlias = columnAlias;
    }
}
