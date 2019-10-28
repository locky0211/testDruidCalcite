package com.calcite.sql;

import com.tmp.SelectSqlCaseTmp;
import com.tmp.SelectSqlTmp;
import com.tmp.SelectTableColumnTmp;
import com.tmp.SelectTableColumnTmpBase;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class SqlBasicCallCalcite {


    private static Logger log = LoggerFactory.getLogger(SqlBasicCallCalcite.class);

    /**
     * 处理 insert .... select ....(select 部分 类型为SqlBasicCall)
     *
     * @param sqlBasicCall
     */
    public static void insertSelectSqlParseSqlBasicCall(SqlBasicCall sqlBasicCall, List<SelectTableColumnTmpBase> selectTableColumnTmps, Map<String, String> tableAliasmap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {


        SqlNode[] sqlOperands = sqlBasicCall.getOperands();
        if (sqlOperands.length <= 0) {
            return;
        }


        boolean except = false;
        String operator = sqlBasicCall.getOperator().toString();
        if (operator.equalsIgnoreCase("EXCEPT")) {
            except = true;
        }

        int forNum = 0;
        for (SqlNode sqlNode : sqlOperands) {

            forNum++;

            if (sqlNode instanceof SqlSelect) {


                SqlSelect sqlSelectList = (SqlSelect) sqlNode;

                //--select字段
                selectTableColumnTmps.addAll(SqlSelectTableColumns.selectTableColumns(sqlSelectList, fromJoinTableColumnMap));


                //解析源数据表和别名<别名,表名>
                tableAliasmap.putAll(SqlSelectFromCalcite.generateSelectTables(sqlSelectList, fromJoinTableColumnMap));


            } else if (sqlNode instanceof SqlBasicCall) {

                insertSelectSqlParseSqlBasicCall((SqlBasicCall) sqlNode, selectTableColumnTmps, tableAliasmap, fromJoinTableColumnMap);
            } else {

                log.error("insert ... select ...， operate parseSqlBasicCall  unKnow type!");
            }

            if (except && forNum > 0) {
                break;
            }
        }
    }

    /**
     * 处理 insert...select...，select部分，每个字段中 case when 这种选择
     *
     * @param sqlCase
     */
    public static SelectSqlCaseTmp insertSelectSqlCase(SqlCase sqlCase, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {


        /**
         * 举例:
         * CASE
         when B.OCCURDATE like '%0229'
         then last_day(substr(B.OCCURDATE,1,4) || '-02-01')
         WHEN LENGTH (B.OCCURDATE) IN (8, 10)
         THEN
         TO_DATE (B.OCCURDATE, 'YYYYMMDD')
         END AS KHRQ
         */

        SelectSqlCaseTmp columnTmp = new SelectSqlCaseTmp(SelectTableColumnTmp.TYPE_3);

        SqlNodeList thenList = sqlCase.getThenOperands();
        if (CollectionUtils.isEmpty(thenList.getList())) {
            return columnTmp;
        }

        for (SqlNode sqlNode : thenList.getList()) {

            if (sqlNode instanceof SqlSelect) {

                //then 部分，是select的sql语句

                //解析源数据表和别名<别名,表名>
                columnTmp.getTableAliasmap().putAll(SqlSelectFromCalcite.generateSelectTables((SqlSelect) sqlNode, fromJoinTableColumnMap));

                //选择字段
                List<SelectTableColumnTmpBase> selectColumns = SqlSelectTableColumns.selectTableColumns((SqlSelect) sqlNode, fromJoinTableColumnMap);

                for (SelectTableColumnTmpBase selectColumn : selectColumns) {

                    if (selectColumn instanceof SelectSqlTmp) {//字段为sql关联多个表字段
                        SelectSqlTmp sqlTmp = (SelectSqlTmp) selectColumn;

                        columnTmp.getTableAliaColumns().putAll(sqlTmp.getTableAliaColumns());

                    } else if (selectColumn instanceof SelectTableColumnTmp) {//select 表字段
                        SelectTableColumnTmp sqlTmp = (SelectTableColumnTmp) selectColumn;
                        columnTmp.addSourceTableInfo(sqlTmp.getTableAlias(), sqlTmp.getColumnNames());

                    } else if (selectColumn instanceof SelectSqlCaseTmp) {//sql中有case选择

                        SelectSqlCaseTmp sqlTmp = (SelectSqlCaseTmp) selectColumn;
                        columnTmp.getTableAliaColumns().putAll(sqlTmp.getTableAliaColumns());

                    } else {
                        log.error("insert ... select ...， operate insertSelectSqlCase ,SelectTableColumnTmpBase  unKnow type! sqlNode:[" + sqlNode.toString() + "]");
                    }
                }
                continue;
            }

            if (sqlNode instanceof SqlIdentifier) {
                SelectTableColumnTmp tableColumnTmp = SqlSelectTableColumns.selectTableColumnTmp(sqlNode);
                columnTmp.addSourceTableInfo(tableColumnTmp.getTableAlias(), tableColumnTmp.getColumnNames());
                continue;
            }

            if (sqlNode instanceof SqlCharStringLiteral ||
                    sqlNode instanceof SqlNumericLiteral) {//默认字符串/数值
                continue;
            }

            if (sqlNode instanceof SqlBasicCall) {//sql

                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode;
                SqlNode[] sqlNodes = sqlBasicCall.operands;

                SelectSqlTmp selectSqlTmp = new SelectSqlTmp(SelectSqlTmp.TYPE_3);

                SqlOperator sqlOperator = sqlBasicCall.getOperator();
                if (sqlOperator.getName().equalsIgnoreCase("AS")) { //定义别名
                    selectSqlTmp.setAliaName(sqlBasicCall.operands[1].toString());
                }
                try {

                    SqlSelectTableColumns.selectColumnBySql(sqlNodes, selectSqlTmp);
                    columnTmp.getTableAliaColumns().putAll(selectSqlTmp.getTableAliaColumns());
                } catch (Exception e) {
                    //TODO
                    log.error("insert select 部分解析异常! 内容：【" + sqlNode.toString() + "】", e);
                }

                continue;
            }
            log.error("insert ... select ...， operate insertSelectSqlCase sqlNode unKnow type! sqlNode:[" + sqlNode.toString() + "]");
        }

        return columnTmp;
    }
}
