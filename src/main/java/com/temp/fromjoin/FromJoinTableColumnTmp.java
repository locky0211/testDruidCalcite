package com.temp.fromjoin;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * from tableA a
 * left join( select c.name name1,c.desc desc1 from tableC ) b
 * on a.id = b.name1
 */
public class FromJoinTableColumnTmp {

    private String tmpTableAlias;//关联表的别名，eg： b

    private String columnName;//使用字段名，eg: b.name1


    //该字段源信息<表名,List<字段名>>  eg:<tableC,[name]>
    Map<String, List<String>> sourceTableInfo = new HashMap<String, List<String>>();

    public FromJoinTableColumnTmp() {

    }

    public FromJoinTableColumnTmp(String tmpTableAlias) {
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
