package com.calcite.util;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.dialect.db2.parser.DB2StatementParser;
import com.alibaba.druid.util.JdbcConstants;
import com.tmp.targettable.TargetTableInfo;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class DruidUtil {


    /**
     * 获取sql中的insert部分
     *
     * @param insertSqlStr
     * @param sqlNo
     * @return
     * @throws Exception
     */
    public static TargetTableInfo parseInsertSql(String insertSqlStr, String sqlNo) throws Exception {


        System.out.println("解析的执行编号[" + sqlNo + "]的sql：[" + insertSqlStr + "]");
        //格式化输出
        String insertSqlStrFormat = SQLUtils.format(insertSqlStr, JdbcConstants.DB2);

        System.out.println("执行编号[" + sqlNo + "]的sql，解析的sql格式化：[" + insertSqlStrFormat + "]");

        System.out.println("===================开始解析insert===start=================");
        DB2StatementParser parser = new DB2StatementParser(insertSqlStr);

        SQLStatement statement = null;
        try {
            //解析sql
            statement = parser.parseStatement();
        } catch (Exception e) {
            // logger.error("执行编号[" + sqlNo + "]的sql，解析sql异常!sql语句格式异常!", e);
            System.out.println("执行编号[" + sqlNo + "]的sql，解析sql异常!sql语句格式异常!");
            throw e;
        }

        if (!(statement instanceof SQLInsertStatement)) {
            //判断sql中不包含insert
//            logger.debug("执行编号[" + sqlNo + "]的sql，不包含insert！");
//            logger.debug("===================开始解析insert===start=================");

            System.out.println("执行编号[" + sqlNo + "]的sql，不包含insert！");
//            logger.debug("===================开始解析insert===start=================");
            return null;
        }


        SQLInsertStatement insertStatement = (SQLInsertStatement) statement;

        //目标表明
        SQLName tableName = insertStatement.getTableName();

        TargetTableInfo targetTableInfo = new TargetTableInfo(tableName.getSimpleName());

        //表字段
        List<SQLExpr> tableFields = insertStatement.getColumns();
        if (CollectionUtils.isEmpty(tableFields)) {
            //insert sql语句中没有写明表字段，默认表全部 eg: insert into table_test values (12,'aa');

            //TODO 需要根据表名到指定的表里，获取表字段

            return targetTableInfo;
        }


        for (SQLExpr tableFieldSQLExpr : tableFields) {
            //获取字段信息
            SQLIdentifierExpr tableField = (SQLIdentifierExpr) tableFieldSQLExpr;
            targetTableInfo.getTableColumn().add(tableField.getName());
        }

        //      logger.debug("===================开始解析insert===start=================");
        return targetTableInfo;
    }
}
