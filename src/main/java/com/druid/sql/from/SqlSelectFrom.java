package com.druid.sql.from;


import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.fastjson.JSONObject;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.druid.sql.select.SqlSelectInfo;
import com.druid.util.SqlSelectUtil;
import com.tmp.SelectSqlCaseTmp;
import com.tmp.SelectSqlTmp;
import com.tmp.SelectTableColumnTmp;
import com.tmp.SelectTableColumnTmpBase;
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
public class SqlSelectFrom {

    private static Logger log = LoggerFactory.getLogger(SqlSelectFrom.class);


    public static void generateSelectTables(SQLSelect sqlSelect, Map<String, String> tableNameAlias, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();

        if (sqlSelectQuery instanceof SQLUnionQuery) {
            //insert .... select ... from... MINUS ......

            SQLUnionQuery sqlUnionQuery = (SQLUnionQuery) sqlSelectQuery;

            generateSelectTables((sqlUnionQuery).getLeft(), tableNameAlias, fromJoinTableColumnMap);//select 部分

            if (sqlUnionQuery.getOperator() == SQLUnionOperator.MINUS || //  去最小值
                    sqlUnionQuery.getOperator() == SQLUnionOperator.EXCEPT //排除
                    ) {
                return;
            }

            generateSelectTables((sqlUnionQuery).getRight(), tableNameAlias, fromJoinTableColumnMap);//MINUS 部分
            return;
        }

        //解析源数据表和别名
        generateSelectTables(sqlSelectQuery, tableNameAlias, fromJoinTableColumnMap);

    }


    /**
     * 解析源数据表和别名
     *
     * @param sqlSelectQuery
     * @param tableNameAlias         <别名,表名>
     * @param fromJoinTableColumnMap
     */
    private static void generateSelectTables(SQLSelectQuery sqlSelectQuery, Map<String, String> tableNameAlias, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {


        if (!(sqlSelectQuery instanceof SQLSelectQueryBlock)) {
            throw new RuntimeException("解析sql中sql部分，未知类型！[" + sqlSelectQuery.toString() + "]");
        }

        SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;


        //判断 select × from(select ....) 的情况
        if (SqlSelectUtil.checkSelectAll(sqlSelectQueryBlock.getSelectList())) {

            SQLSubqueryTableSource subQueryTableSource = (SQLSubqueryTableSource) sqlSelectQueryBlock.getFrom();
            generateSelectTables(subQueryTableSource.getSelect(), tableNameAlias, fromJoinTableColumnMap);
            return;
        }

        //解析select表和别名关系
        SQLTableSource sqlNode1From = sqlSelectQueryBlock.getFrom();

        if (sqlNode1From instanceof SQLJoinTableSource) {

            //from .... left join
            selectSqlJoin((SQLJoinTableSource) sqlNode1From, tableNameAlias, fromJoinTableColumnMap);

        } else if (sqlNode1From instanceof SQLSubqueryTableSource) {

            SQLSubqueryTableSource selectTableSource = (SQLSubqueryTableSource) sqlNode1From;

            //from 后面是sql,from (select ....)
            generateSelectTables(selectTableSource.getSelect(), tableNameAlias, fromJoinTableColumnMap);

        } else if (sqlNode1From instanceof SQLExprTableSource) {

            // from 表名 as 别名
            generateFromTable((SQLExprTableSource) sqlNode1From, tableNameAlias, fromJoinTableColumnMap);

        }
// else if (sqlNode1From instanceof SqlIdentifier) {
//
//            // from 表名
//            SqlIdentifier fromTable = (SqlIdentifier) sqlNode1From;
//            tableNameAlias.put(fromTable.toString(), fromTable.toString());
//
//        }

        else {

            //TODO
            throw new RuntimeException("未知类型：sqlNode1From");
        }

    }


    /**
     * 处理from表
     *
     * @param sqlBasicCall
     * @param fromJoinTableColumnMap <关联别名,<字段名,对象>>
     */
    private static Map<String, String> generateFromTable(SQLExprTableSource sqlBasicCall, Map<String, String> tableNameAlias, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        //别名名
        String aliasName = sqlBasicCall.getAlias();
        if (StringUtils.isNotBlank(aliasName)) { //定义别名

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
    private static void selectSqlJoin(SQLJoinTableSource sqlNode1FromJoin, Map<String, String> tableNameAlias, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        //eg:
        /** FROM F_LN_LNLNSLNS A LEFT JOIN ODS.O_ODS_F_PRD_PDP_LOAN B ON A.BUS_TYP=B.LOAN_TYP
         LEFT JOIN T1 C ON A.ACCT_NO=C.LN_LN_ACCT_NO
         LEFT JOIN F_LN_BUSINESS_DUEBILL E ON A.ACCT_NO = E.ACCT_NO
         LEFT JOIN  FDS.F_LN_BUSINESS_CONTRACT D ON   E.CONT_NO=D.CONT_NO*/


        //最后一个left join
        // eg:  FDS.F_LN_BUSINESS_CONTRACT D ON   E.CONT_NO=D.CONT_NO
        SQLTableSource sqlNode1FromJoinRight = sqlNode1FromJoin.getRight();

        joinTableAlias(sqlNode1FromJoinRight, tableNameAlias, fromJoinTableColumnMap);

        //前面其他jon集合体
        // eg: FROM F_LN_LNLNSLNS A LEFT JOIN ODS.O_ODS_F_PRD_PDP_LOAN B ON A.BUS_TYP=B.LOAN_TYP
        //          LEFT JOIN T1 C ON A.ACCT_NO=C.LN_LN_ACCT_NO
        //                 LEFT JOIN F_LN_BUSINESS_DUEBILL E ON A.ACCT_NO = E.ACCT_NO

        SQLTableSource sqlNode1FromJoinLeftNode = sqlNode1FromJoin.getLeft();


        if (sqlNode1FromJoinLeftNode instanceof SQLExprTableSource) {
            // 一个join，取表别名和字段
            SQLExprTableSource sqlNode1FromJoinLeft = (SQLExprTableSource) sqlNode1FromJoinLeftNode;
            joinTableAlias(sqlNode1FromJoinLeft, tableNameAlias, fromJoinTableColumnMap);

        } else if (sqlNode1FromJoinLeftNode instanceof SQLJoinTableSource) {
            //多个join的新式
            SQLJoinTableSource sqlNode1FromJoinLeft = (SQLJoinTableSource) sqlNode1FromJoinLeftNode;
            selectSqlJoin(sqlNode1FromJoinLeft, tableNameAlias, fromJoinTableColumnMap);
        } else {
            log.error("selectSqlJoin unKnow type! class:[" + sqlNode1FromJoinLeftNode.toString() + "]");
        }

    }

    /**
     * 左关联、右关联表别名
     *
     * @param sqlNode1FromJoinRight
     * @param tableNameAlias
     * @param fromJoinTableColumnMap <管理别名,<字段名,对象>>
     */
    private static void joinTableAlias(SQLTableSource sqlNode1FromJoinRight, Map<String, String> tableNameAlias, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {


        if (sqlNode1FromJoinRight instanceof SQLSubqueryTableSource) { // 左关联、右关联，别名

            /** from tableA a
             * left join(
             *      select c.name as name1 ,d.desc as desc1
             *          from tableC c
             *          left join tableD d on c.id= d.id
             * ) b on a.name = c.name
             */

            SQLSubqueryTableSource selectTableSource = (SQLSubqueryTableSource) sqlNode1FromJoinRight;

            //获取所有关联表、别名
            generateSelectTables(selectTableSource.getSelect(), tableNameAlias, fromJoinTableColumnMap);
            //别名
            String joinTmpTableAlias = sqlNode1FromJoinRight.getAlias();

            //记录别名信息,自定义查询集合
            tableNameAlias.put(joinTmpTableAlias, joinTmpTableAlias);

            //选择字段
            List<SelectTableColumnTmpBase> withAsColumns = SqlSelectInfo.operateSqlSelect(selectTableSource.getSelect(), tableNameAlias, fromJoinTableColumnMap);
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

        } else if (sqlNode1FromJoinRight instanceof SQLExprTableSource) {

            opTableInfo((SQLExprTableSource) sqlNode1FromJoinRight, tableNameAlias, fromJoinTableColumnMap);
        } else {
            log.error("joinTableAlias unKnow Type! class:[" + sqlNode1FromJoinRight.toString() + "]");
        }
    }


    private static void opTableInfo(SQLExprTableSource sqlNode1FromJoinRight, Map<String, String> tableNameAlias, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        SelectTableColumnTmpBase tableInfo = SqlSelectInfo.opSelectColumn(sqlNode1FromJoinRight.getAlias(), (sqlNode1FromJoinRight).getExpr(), tableNameAlias, fromJoinTableColumnMap);

        if (tableInfo instanceof SelectTableColumnTmp) {

            SelectTableColumnTmp columnTmp = (SelectTableColumnTmp) tableInfo;
            tableNameAlias.put(sqlNode1FromJoinRight.getAlias(), StringUtils.isBlank(columnTmp.getTableAlias()) ? columnTmp.getColumnNames() : columnTmp.getTableAlias() + "." + columnTmp.getColumnNames());

        } else {
            log.error("处理做表关联关系异常未知类型！");
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
