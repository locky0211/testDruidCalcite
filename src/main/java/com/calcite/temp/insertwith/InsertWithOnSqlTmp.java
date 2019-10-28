package com.calcite.temp.insertwith;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * insert into table2
 * with
 * s1 as (select rownum c1 from dual connect by rownum <= 10),
 */
public class InsertWithOnSqlTmp {

    private String tmpTableAlias;//临时表的别名，eg： a1

    private String columnName;//使用字段名，eg: a1.c1


    //该字段源信息<表名,List<字段名>>  eg:<dual,[rownum]>
    Map<String, List<String>> sourceTableInfo = new HashMap<String, List<String>>();

    public InsertWithOnSqlTmp() {

    }

    public InsertWithOnSqlTmp(String tmpTableAlias) {
        this.tmpTableAlias = tmpTableAlias;
    }

    public String getTmpTableAlias() {
        return tmpTableAlias;
    }

    public void setTmpTableAlias(String tmpTableAlias) {
        this.tmpTableAlias = tmpTableAlias;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Map<String, List<String>> getSourceTableInfo() {
        return sourceTableInfo;
    }

    public void setSourceTableInfo(Map<String, List<String>> sourceTableInfo) {
        this.sourceTableInfo = sourceTableInfo;
    }

    public void addSourceTableInfo(String sourceTableName, String sourceTableColumn) {


        List<String> columns = sourceTableInfo.get(sourceTableName);
        if (columns == null) {
            columns = new ArrayList<String>();
            sourceTableInfo.put(sourceTableName, columns);
        }

        columns.add(sourceTableColumn);
    }
}
