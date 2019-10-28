package com.calcite.sql;


import com.alibaba.fastjson.JSONObject;
import com.tmp.SelectSqlCaseTmp;
import com.tmp.SelectSqlTmp;
import com.tmp.SelectTableColumnTmp;
import com.tmp.SelectTableColumnTmpBase;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.calcite.util.SqlSelectUtil;
import org.apache.calcite.sql.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 处理select ...from  中from部分
 */
public class SqlSelectFromCalcite {

    private static Logger log = LoggerFactory.getLogger(SqlSelectFromCalcite.class);

    /**
     * 解析源数据表和别名
     *
     * @param sqlSelect
     */
    public static Map<String, String> generateSelectTables(SqlSelect sqlSelect, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {


        //剔除select × from .... 的情况
        if (SqlSelectUtil.checkSelectAll(sqlSelect)) {

            if (sqlSelect.getFrom() instanceof SqlSelect) {
                //select × from(select ....)
                return generateSelectTables((SqlSelect) sqlSelect.getFrom(), fromJoinTableColumnMap);
            } else {
                //TODO
                return new HashMap<String, String>();
            }

        }


        //<别名,表名>
        Map<String, String> tableNameAlias = new HashMap<String, String>();

        //解析select表和别名关系
        SqlNode sqlNode1From = sqlSelect.getFrom();

        if (sqlNode1From instanceof SqlJoin) {

            //from .... left join
            selectSqlJoin((SqlJoin) sqlNode1From, tableNameAlias, fromJoinTableColumnMap);

        } else if (sqlNode1From instanceof SqlSelect) {

            //from 后面是sql,from (select ....)
            generateSelectTables((SqlSelect) sqlNode1From, fromJoinTableColumnMap);

        } else if (sqlNode1From instanceof SqlBasicCall) {

            // from 表名 as 别名
            generateFromTable((SqlBasicCall) sqlNode1From, tableNameAlias, fromJoinTableColumnMap);

        } else if (sqlNode1From instanceof SqlIdentifier) {

            // from 表名
            SqlIdentifier fromTable = (SqlIdentifier) sqlNode1From;
            tableNameAlias.put(fromTable.toString(), fromTable.toString());

        } else {

            //TODO
            throw new RuntimeException("未知类型：sqlNode1From");
        }

        return tableNameAlias;
    }


    /**
     * 处理from表
     *
     * @param sqlBasicCall
     * @param fromJoinTableColumnMap <关联别名,<字段名,对象>>
     */
    private static Map<String, String> generateFromTable(SqlBasicCall sqlBasicCall, Map<String, String> tableNameAlias, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {


        SqlOperator sqlOperator = sqlBasicCall.getOperator();
        if (sqlOperator.getName().equalsIgnoreCase("AS")) { //定义别名

            //表以及别名
            joinTableAlias(sqlBasicCall, tableNameAlias, fromJoinTableColumnMap);
        } else {
            log.error("select form unknow sqlBasicCall type! from sql:[" + sqlBasicCall.toString() + "]");
        }

        return tableNameAlias;
    }

    /**
     * 处理左关联右关联
     *
     * @param sqlNode1FromJoin
     * @param tableNameAlias
     * @param fromJoinTableColumnMap <关联别名,<字段名,对象>>
     */
    private static void selectSqlJoin(SqlJoin sqlNode1FromJoin, Map<String, String> tableNameAlias, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        //eg:
        /** FROM F_LN_LNLNSLNS A LEFT JOIN ODS.O_ODS_F_PRD_PDP_LOAN B ON A.BUS_TYP=B.LOAN_TYP
         LEFT JOIN T1 C ON A.ACCT_NO=C.LN_LN_ACCT_NO
         LEFT JOIN F_LN_BUSINESS_DUEBILL E ON A.ACCT_NO = E.ACCT_NO
         LEFT JOIN  FDS.F_LN_BUSINESS_CONTRACT D ON   E.CONT_NO=D.CONT_NO*/


        //最后一个left join
        // eg:  FDS.F_LN_BUSINESS_CONTRACT D ON   E.CONT_NO=D.CONT_NO
        SqlBasicCall sqlNode1FromJoinRight = (SqlBasicCall) sqlNode1FromJoin.getRight();

        joinTableAlias(sqlNode1FromJoinRight, tableNameAlias, fromJoinTableColumnMap);

        //前面其他jon集合体
        // eg: FROM F_LN_LNLNSLNS A LEFT JOIN ODS.O_ODS_F_PRD_PDP_LOAN B ON A.BUS_TYP=B.LOAN_TYP
        //          LEFT JOIN T1 C ON A.ACCT_NO=C.LN_LN_ACCT_NO
        //                 LEFT JOIN F_LN_BUSINESS_DUEBILL E ON A.ACCT_NO = E.ACCT_NO

        SqlNode sqlNode1FromJoinLeftNode = sqlNode1FromJoin.getLeft();


        if (sqlNode1FromJoinLeftNode instanceof SqlBasicCall) {
            // 一个join，取表别名和字段
            SqlBasicCall sqlNode1FromJoinLeft = (SqlBasicCall) sqlNode1FromJoinLeftNode;
            joinTableAlias(sqlNode1FromJoinLeft, tableNameAlias, fromJoinTableColumnMap);

        } else {
            //多个join的新式
            SqlJoin sqlNode1FromJoinLeft = (SqlJoin) sqlNode1FromJoinLeftNode;
            selectSqlJoin(sqlNode1FromJoinLeft, tableNameAlias, fromJoinTableColumnMap);
        }

    }

    /**
     * 左关联、右关联表别名
     *
     * @param sqlNode1FromJoinRight
     * @param tableNameAlias
     * @param fromJoinTableColumnMap <管理别名,<字段名,对象>>
     */
    private static void joinTableAlias(SqlBasicCall sqlNode1FromJoinRight, Map<String, String> tableNameAlias, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        SqlNode[] operands = sqlNode1FromJoinRight.operands;

        if (operands.length > 1) {
            if (operands[0] instanceof SqlSelect) { // 左关联、有关联，别名

                /** from tableA a
                 * left join(
                 *      select c.name as name1 ,d.desc as desc1
                 *          from tableC c
                 *          left join tableD d on c.id= d.id
                 * ) b on a.name = c.name
                 */

                //获取所有关联表、别名
                tableNameAlias.putAll(generateSelectTables((SqlSelect) operands[0], fromJoinTableColumnMap));
                //别名
                String joinTmpTableAlias = operands[1].toString();

                //记录别名信息,自定义查询集合
                tableNameAlias.put(joinTmpTableAlias, joinTmpTableAlias);

                //选择字段
                List<SelectTableColumnTmpBase> withAsColumns = SqlSelectTableColumns.selectTableColumns((SqlSelect) sqlNode1FromJoinRight.operands[0], fromJoinTableColumnMap);
                if (CollectionUtils.isNotEmpty(withAsColumns)) {

                    //关联表别名，使用字段
                    Map<String, FromJoinTableColumnTmp> columnMap = fromJoinTableColumnMap.get(joinTmpTableAlias);
                    if (columnMap == null) {
                        columnMap = new HashMap<String, FromJoinTableColumnTmp>();
                        fromJoinTableColumnMap.put(joinTmpTableAlias, columnMap);
                    }

                    //处理from、左右关联字段关连关系
                    fromJoinTableColumn(joinTmpTableAlias, withAsColumns, columnMap, tableNameAlias);
                }

            } else if (operands[0] instanceof SqlIdentifier) {
                tableNameAlias.put(operands[1].toString(), operands[0].toString());
            } else {
                log.error("joinTableAlias unKnow Type! str:[" + operands[0].toString() + "]");
            }

        } else {
            if (operands[0] instanceof SqlSelect) {
                tableNameAlias.putAll(generateSelectTables((SqlSelect) operands[0], fromJoinTableColumnMap));

            } else if (operands[0] instanceof SqlIdentifier) {

                String tableName = operands[0].toString();
                tableNameAlias.put(tableName, tableName);
            } else {
                log.error("joinTableAlias unKnow Type! str:[" + operands[0].toString() + "]");
            }
        }
    }


    /**
     * 处理from、左右关联字段关连关系
     *
     * @param tmpTableAlias
     * @param withAsColumns
     * @param columnMap
     * @param tableAliasmap
     */
    private static void fromJoinTableColumn(String tmpTableAlias, List<SelectTableColumnTmpBase> withAsColumns, Map<String, FromJoinTableColumnTmp> columnMap, Map<String, String> tableAliasmap) {
        for (SelectTableColumnTmpBase withAsColumn : withAsColumns) {


            //一个from/join 临时对象
            FromJoinTableColumnTmp fromJoinTmp = new FromJoinTableColumnTmp(tmpTableAlias);

            if (withAsColumn instanceof SelectSqlTmp) {

                SelectSqlTmp tableColumnTmp = (SelectSqlTmp) withAsColumn;
                if (tableColumnTmp.getTableAliaColumns().size() <= 0) {
                    continue;
                }

                fromJoinTmp.setColumnName(tableColumnTmp.getAliaName());//临时表使用字段名
                //<表别名,<字段名>>
                Map<String, List<String>> tableAliaColumns = tableColumnTmp.getTableAliaColumns();
                for (String tableAlia : tableAliaColumns.keySet()) {

                    String tableRealName = tableAliasmap.get(tableAlia);

                    tableRealName = tableRealName == null ? "" : tableRealName;

                    ////别名信息
                    fromJoinTmp.getSourceTableInfo().put(tableRealName, tableAliaColumns.get(tableAlia));
                }

                columnMap.put(fromJoinTmp.getColumnName(), fromJoinTmp);

            } else if (withAsColumn instanceof SelectTableColumnTmp) {

                SelectTableColumnTmp tableColumnTmp = (SelectTableColumnTmp) withAsColumn;

                if (StringUtils.isBlank(tableColumnTmp.getTableAlias())) {
                    //select 字段，没有表别名
                    continue;
                }

                String sqlUseColumnName = "";
                if (StringUtils.isBlank(tableColumnTmp.getColumnAlias())) {//取的字段是否用了别名
                    sqlUseColumnName = tableColumnTmp.getColumnNames();
                } else {
                    sqlUseColumnName = tableColumnTmp.getColumnAlias();
                }

                fromJoinTmp.setColumnName(sqlUseColumnName);//临时表使用字段名

                //临时表，select ... from 表 真实名字
                String tableRealName = tableAliasmap.get(tableColumnTmp.getTableAlias());

                tableRealName = tableRealName == null ? "" : tableRealName;

                //临时表使用字段，对应源数据表名，对应源数据表字段
                fromJoinTmp.addSourceTableInfo(tableRealName, tableColumnTmp.getColumnNames());

                columnMap.put(sqlUseColumnName, fromJoinTmp);

            } else if (withAsColumn instanceof SelectSqlCaseTmp) {


                SelectSqlCaseTmp tableColumnTmp = (SelectSqlCaseTmp) withAsColumn;
                if (tableColumnTmp.getTableAliaColumns().size() <= 0) {
                    continue;
                }

                fromJoinTmp.setColumnName(tableColumnTmp.getColumnAlias());//临时表使用字段名

                //<表别名,<字段名>>
                Map<String, List<String>> tableAliaColumns = tableColumnTmp.getTableAliaColumns();
                for (String tableAlia : tableAliaColumns.keySet()) {


                    String tableRealName = tableAliasmap.get(tableAlia);

                    tableRealName = tableRealName == null ? "" : tableRealName;

                    ////别名信息
                    fromJoinTmp.getSourceTableInfo().put(tableRealName, tableAliaColumns.get(tableAlia));
                }

                //别名信息
                columnMap.put(fromJoinTmp.getColumnName(), fromJoinTmp);
            } else {
                log.error("unKnow Type!fromJoinTableColumn() ! data: [" + JSONObject.toJSONString(withAsColumn) + "]");
            }
        }
    }
}
