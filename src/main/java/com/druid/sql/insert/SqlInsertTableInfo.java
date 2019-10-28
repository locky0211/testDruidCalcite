package com.druid.sql.insert;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.tmp.targettable.TargetTableInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * 解析insert 表 字段信息
 */
public class SqlInsertTableInfo {


    /**
     * 获取insert 表，表名和表字段信息
     *
     * @param insertSqlState
     * @return
     * @throws Exception
     */
    public static TargetTableInfo parseInsertSql(SQLInsertStatement insertSqlState) throws Exception {

        //插入目标表信息
        SQLExprTableSource targetTableInfoSource = insertSqlState.getTableSource();
        if (targetTableInfoSource == null) {
            throw new RuntimeException("没有insert 表信息!");
        }

        String targetTableName = "";
        SQLExpr targetTableSQLExpr = targetTableInfoSource.getExpr();
        if (targetTableSQLExpr instanceof SQLPropertyExpr) {// insert into schema.tableA
            SQLPropertyExpr targetTableInfo = (SQLPropertyExpr) targetTableSQLExpr;
            String tableOwner = targetTableInfo.getOwnernName();
            String tableName = targetTableInfo.getName();

            targetTableName = StringUtils.isBlank(tableOwner) ? tableName : tableOwner + "." + tableName;

        } else if (targetTableSQLExpr instanceof SQLIdentifierExpr) {// insert into tableA

            SQLIdentifierExpr targetTableIdentifier = (SQLIdentifierExpr) targetTableSQLExpr;
            targetTableName = targetTableIdentifier.getName();
        } else {
            throw new RuntimeException("解析insert 信息! 未知的类型：[" + targetTableSQLExpr.getClass().toString() + "]");
        }

        //构建临时对象
        TargetTableInfo targetTableObj = new TargetTableInfo(targetTableName);

        //处理insert表的字段
        List<SQLExpr> columns = insertSqlState.getColumns();
        if (CollectionUtils.isEmpty(columns)) {
            //insert sql语句中没有写明表字段，默认表全部 eg: insert into table_test values (12,'aa');
            return targetTableObj;
        }

        for (SQLExpr tableFieldSQLExpr : columns) {
            //获取字段信息
            SQLIdentifierExpr tableField = (SQLIdentifierExpr) tableFieldSQLExpr;
            targetTableObj.getTableColumn().add(tableField.getName());
        }

        return targetTableObj;
    }
}
