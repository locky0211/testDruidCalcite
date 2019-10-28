package com.calcite.temp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TargetTableColumn implements Serializable {

    public static final int TYPE_1 = 1;//正常
    public static final int TYPE_2 = 2;//默认值
    public static final int TYPE_3 = 3;//未知

    private int type = TYPE_1;

    private String columnName;//字段名



    //源表和字段  <表明,List<字段名>>
    private Map<String,List<String>> sourceTableColumnMap = new HashMap<String, List<String>>();


    private String desc;


    public TargetTableColumn() {

    }

    public TargetTableColumn(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }


    public int getType() {
        return type;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public Map<String, List<String>> getSourceTableColumnMap() {
        return sourceTableColumnMap;
    }

    public void setSourceTableColumnMap(Map<String, List<String>> sourceTableColumnMap) {
        this.sourceTableColumnMap = sourceTableColumnMap;
    }

    public void setType(int type) {
        this.type = type;

        switch (type){

            case TYPE_1:
                desc ="正常";
                break;
            case TYPE_2:
                desc ="默认值";
                break;
            default:
                desc ="未知";
        }
    }


    /**
     * 添加源表、字段数据
     * @param sourceTableName
     * @param sourceTableColumn
     */
    public void addSourceTableInfo(String sourceTableName,String sourceTableColumn){

        List<String> tableColumns = sourceTableColumnMap.get(sourceTableName);
        if(tableColumns == null){
            tableColumns = new ArrayList<String>();
            sourceTableColumnMap.put(sourceTableName,tableColumns);
        }

        tableColumns.add(sourceTableColumn);
    }
}
