package com.tmp.targettable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TargetTableInfo implements Serializable{

    //目标表名称
    private String targetTableName;


    private List<String> tableColumn = new ArrayList<String>();//表字段

    //<表字段名，对应关系>
    private List<TargetTableColumn> tableColumnList = new ArrayList<TargetTableColumn>();

    public TargetTableInfo() {

    }

    public TargetTableInfo(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public List<TargetTableColumn> getTableColumnList() {
        return tableColumnList;
    }

    public void setTableColumnList(List<TargetTableColumn> tableColumnList) {
        this.tableColumnList = tableColumnList;
    }

    public List<String> getTableColumn() {
        return tableColumn;
    }

    public void setTableColumn(List<String> tableColumn) {
        this.tableColumn = tableColumn;
    }
}
