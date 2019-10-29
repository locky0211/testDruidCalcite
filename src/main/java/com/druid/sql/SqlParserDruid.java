package com.druid.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.fastjson.JSONObject;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.calcite.temp.insertwith.InsertWithOnSqlTmp;
import com.druid.sql.from.SqlSelectFrom;
import com.druid.sql.insert.SqlInsertTableInfo;
import com.druid.sql.select.SqlSelectInfo;
import com.druid.sql.with.SqlInsertWith;
import com.tmp.SelectTableColumnTmpBase;
import com.tmp.targettable.TargetTableInfo;
import com.util.AtLastUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


        //================================================================================================
        //获取insert 表，表名和表字段信息
        TargetTableInfo targetTableInfo = SqlInsertTableInfo.parseInsertSql(insertSqlState);

        //select 字段内容
        List<SelectTableColumnTmpBase> selectTableColumnTmpBases = new ArrayList<SelectTableColumnTmpBase>();


        //解析源数据表和别名<别名,表名>
        Map<String, String> tableAliasmap = new HashMap<String, String>();

        //insert  with 临时表，管使用的真实表信息<临时表别名,<临时表使用字段名,对象信息>>
        Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap = new HashMap<String, Map<String, InsertWithOnSqlTmp>>();

        ///处理form关联（左/右）临时表  <关联,<字段名,对象>>
        Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap = new HashMap<String, Map<String, FromJoinTableColumnTmp>>();

        //=================================================================================================

        if (stateNum == 1) {//只有一个sql
            //解析select 字段部分
            selectTableColumnTmpBases.addAll(SqlSelectInfo.operateSqlSelect(insertSqlState.getQuery(), tableAliasmap, fromJoinTableColumnMap));

            // from 部分
            SqlSelectFrom.generateSelectTables(insertSqlState.getQuery(), tableAliasmap, fromJoinTableColumnMap);

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


                //解析select 字段部分
                selectTableColumnTmpBases.addAll(SqlSelectInfo.operateSqlSelect(selectSqlState.getSelect(), tableAliasmap, fromJoinTableColumnMap));

                //insert with 部分
                if (selectSqlState.getSelect().getWithSubQuery() != null) {
                    SqlInsertWith.insertWithAsOp(selectSqlState.getSelect().getWithSubQuery(),tableAliasmap, tmpTableRelationTableMap, fromJoinTableColumnMap);
                }

                // from 部分
                SqlSelectFrom.generateSelectTables(insertSqlState.getQuery(), tableAliasmap, fromJoinTableColumnMap);
            }
        }


        //最后处理，整理insert 表字段，来源关联
        AtLastUtil.atLastStep(targetTableInfo, selectTableColumnTmpBases, tableAliasmap, tmpTableRelationTableMap, fromJoinTableColumnMap);


        System.out.println(JSONObject.toJSONString(targetTableInfo));

    }


}
