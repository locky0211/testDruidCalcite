package com.druid.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.druid.sql.insert.SqlInsertTableInfo;
import com.druid.sql.select.SqlSelectInfo;
import com.tmp.SelectTableColumnTmpBase;
import com.tmp.targettable.TargetTableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * druid 解析sql
 */
public class SqlParserDruid {

    private static Logger log = LoggerFactory.getLogger(SqlParserDruid.class);

    /**
     * 解析处理表目标表、源数据表信息
     *
     * @param insertSqlStr
     * @param sqlNo
     * @throws Exception
     */
    public static void opSqlTargetResourceByDruid(String insertSqlStr, String sqlNo) throws Exception {

        log.info("解析的执行编号[" + sqlNo + "]的sql：[" + insertSqlStr + "]");
        //格式化输出
        String insertSqlStrFormat = SQLUtils.format(insertSqlStr, JdbcConstants.DB2);

        log.info("执行编号[" + sqlNo + "]的sql，解析的sql格式化：[" + insertSqlStrFormat + "]");

        log.info("===================开始解析insert===start=================");

        //sql解析
        List<SQLStatement> sqlStatementList = SQLUtils.parseStatements(insertSqlStr, JdbcConstants.DB2);
        //解析sql总数
        int stateNum = sqlStatementList.size();


        SQLStatement statement = sqlStatementList.get(0);
        if (!(statement instanceof SQLInsertStatement)) {
            //只有一个sql, 判断sql中不包含insert
            log.error("执行编号[" + sqlNo + "]的sql，不包含insert！解析结束");
            return;
        }
        SQLInsertStatement insertSqlState = (SQLInsertStatement) statement;

        //获取insert 表，表名和表字段信息
        TargetTableInfo targetTableInfo = SqlInsertTableInfo.parseInsertSql(insertSqlState);

        //select 字段内容
        List<SelectTableColumnTmpBase> selectTableColumnTmpBases = new ArrayList<SelectTableColumnTmpBase>();

        if (stateNum == 1) {//只有一个sql
            //解析select 部分
            selectTableColumnTmpBases.addAll(SqlSelectInfo.operateSqlSelect(insertSqlState.getQuery()));

        } else {

            int no = 0;
            for (SQLStatement statementSelect : sqlStatementList) {
                no++;

                if (no == 1) {
                    //跳过insert部分
                    continue;
                }

                if (!(statementSelect instanceof SQLSelectStatement)) {

                    log.error("执行编号[" + sqlNo + "]的sql，sql解析出来有多层，第一次insert 后 不是 select ！sql：[" + statement.toLowerCaseString() + "] ");
                    continue;
                }

                SQLSelectStatement selectSqlState = (SQLSelectStatement) statementSelect;

                selectTableColumnTmpBases.addAll(SqlSelectInfo.operateSqlSelect(selectSqlState.getSelect()));
            }
        }
        System.out.println();
    }



}
