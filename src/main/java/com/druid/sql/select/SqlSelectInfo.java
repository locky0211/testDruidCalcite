package com.druid.sql.select;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.druid.sql.from.SqlSelectFrom;
import com.druid.util.SqlSelectUtil;
import com.tmp.SelectSqlTmp;
import com.tmp.SelectTableColumnTmp;
import com.tmp.SelectTableColumnTmpBase;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SqlSelectInfo {


    /**
     * 处理 select 部分
     *
     * @param sqlSelect
     * @return
     */
    public static List<SelectTableColumnTmpBase> operateSqlSelect(SQLSelect sqlSelect, Map<String, String> tableAliasmap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();

        if (!(sqlSelectQuery instanceof SQLSelectQueryBlock)) {
            throw new RuntimeException("解析sql中sql部分，未知类型！[" + sqlSelectQuery.toString() + "]");
        }

        //select部分 sql格式转换
        SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelectQuery;
        return selectColumn(sqlSelectQueryBlock, tableAliasmap, fromJoinTableColumnMap);

    }


    /**
     * 处理 select 部分
     *
     * @param sqlSelectQueryBlock
     * @return
     */
    private static List<SelectTableColumnTmpBase> selectColumn(SQLSelectQueryBlock sqlSelectQueryBlock, Map<String, String> tableAliasmap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        //判断 select × from(select ....) 的情况
        if (SqlSelectUtil.checkSelectAll(sqlSelectQueryBlock.getSelectList())) {

            SQLSubqueryTableSource subQueryTableSource = (SQLSubqueryTableSource) sqlSelectQueryBlock.getFrom();
            return operateSqlSelect(subQueryTableSource.getSelect(), tableAliasmap, fromJoinTableColumnMap);
        }


        //并非 select × from(select ....) 的情况
        List<SelectTableColumnTmpBase> selectColumnTmps = new ArrayList<SelectTableColumnTmpBase>();

        //遍历select每一个部分
        List<SQLSelectItem> selectItems = sqlSelectQueryBlock.getSelectList();
        for (SQLSelectItem sqlSelectItem : selectItems) {

            //字段别名
            String columnAlias = StringUtils.isBlank(sqlSelectItem.getAlias()) ? "" : sqlSelectItem.getAlias();

            //select单个内容部分
            SelectTableColumnTmpBase columnTmpBase = opSelectColumn(columnAlias, sqlSelectItem.getExpr(), tableAliasmap, fromJoinTableColumnMap);
            if (columnTmpBase != null) {
                selectColumnTmps.add(columnTmpBase);
            }
        }

        return selectColumnTmps;
    }


    /**
     * 处理select 单个部分
     *
     * @param columnAlias
     * @param sqlExpr     select内容部分
     * @return
     */
    public static SelectTableColumnTmpBase opSelectColumn(String columnAlias, SQLExpr sqlExpr, Map<String, String> tableAliasmap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        if (sqlExpr instanceof SQLIdentifierExpr) {// eg: tableA.column
            SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlExpr;

            SelectTableColumnTmp columnTmp = new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_1);
            columnTmp.setColumnAlias(columnAlias);//字段别名
            columnTmp.setColumnNames(sqlIdentifierExpr.getName());//字段名
            return columnTmp;


        }
        if (sqlExpr instanceof SQLPropertyExpr) {//eg:tableA.column as column

            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlExpr;
            SelectTableColumnTmp columnTmp = new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_1);
            columnTmp.setColumnAlias(columnAlias);//字段别名 eg:column
            columnTmp.setColumnNames(sqlPropertyExpr.getName());//字段名 eg:column
            columnTmp.setTableAlias(sqlPropertyExpr.getOwnernName());//表别名 tableA
            return columnTmp;
        }

        if (sqlExpr instanceof SQLMethodInvokeExpr) {//eg: function(tableA.column) as df

            SQLMethodInvokeExpr sqlMethodInvokeExpr = (SQLMethodInvokeExpr) sqlExpr;
            List<SQLExpr> sqlMethodExprs = sqlMethodInvokeExpr.getParameters();

            SelectSqlTmp selectSqlTmp = new SelectSqlTmp(SelectSqlTmp.TYPE_3);
            for (SQLExpr sqlMethodExpr : sqlMethodExprs) {
                //处理函数的，不记录常量默认值类型
                opSelectSqlTmp(sqlMethodExpr, selectSqlTmp, tableAliasmap, fromJoinTableColumnMap);
            }
            return selectSqlTmp;
        }

        if (sqlExpr instanceof SQLBinaryOpExpr) {//eg: A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD||GL_THIRD_LEVEL_LG_CD
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;

            SelectSqlTmp selectSqlTmp = new SelectSqlTmp(SelectSqlTmp.TYPE_3);
            selectColumnSQLBinaryOpExpr(sqlBinaryOpExpr, selectSqlTmp, tableAliasmap, fromJoinTableColumnMap);
            return selectSqlTmp;
        }
        if (sqlExpr instanceof SQLNumericLiteralExpr//数字常量
                || sqlExpr instanceof SQLTextLiteralExpr//字符串
                || sqlExpr instanceof SQLNullExpr//null
                ) {
            return new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_2);
        }
        if (sqlExpr instanceof SQLCaseExpr) {// select case when 类型

            return SqlSelectCaseWhen.opSqlSelectCaseWhen((SQLCaseExpr) sqlExpr, tableAliasmap, fromJoinTableColumnMap);

        }
        if (sqlExpr instanceof SQLQueryExpr) {// select 语句

            //select 嵌套 select
            SQLQueryExpr sqlQueryExpr = (SQLQueryExpr) sqlExpr;

            //获取select 字段
            List<SelectTableColumnTmpBase> selectTmpBases = operateSqlSelect(sqlQueryExpr.getSubQuery(), tableAliasmap, fromJoinTableColumnMap);

            //获取from表 部分内容
            SqlSelectFrom.generateSelectTables(sqlQueryExpr.getSubQuery(), tableAliasmap, fromJoinTableColumnMap);
            SelectSqlTmp selectSqlTmp = new SelectSqlTmp(SelectSqlTmp.TYPE_3);
            selectSqlTmp.addDataBySelectTableColumnTmpBase(selectTmpBases);

            return selectSqlTmp;
        }

        //未知
        return new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_0);

    }


    /**
     * select 部分的sql，为类似 A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD||GL_THIRD_LEVEL_LG_CD
     *
     * @param selectSqlTmp
     */
    public static void selectColumnSQLBinaryOpExpr(SQLBinaryOpExpr sqlBinaryOpExpr, SelectSqlTmp selectSqlTmp, Map<String, String> tableAliasmap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        //A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD
        SQLExpr sqlExprLeft = sqlBinaryOpExpr.getLeft();
        opSelectSqlTmp(sqlExprLeft, selectSqlTmp, tableAliasmap, fromJoinTableColumnMap);


        //GL_THIRD_LEVEL_LG_CD
        SQLExpr sqlExprRight = sqlBinaryOpExpr.getRight();
        opSelectSqlTmp(sqlExprRight, selectSqlTmp, tableAliasmap, fromJoinTableColumnMap);
    }


    /**
     * select 单个部分，SelectSqlTmp 记录信息
     *
     * @param sqlExpr
     * @param selectSqlTmp
     */
    public static void opSelectSqlTmp(SQLExpr sqlExpr, SelectSqlTmp selectSqlTmp, Map<String, String> tableAliasmap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {
        SelectTableColumnTmpBase columnTmpBase = opSelectColumn("", sqlExpr, tableAliasmap, fromJoinTableColumnMap);

        //添加数据到SelectSqlTmp
        selectSqlTmp.addDataBySelectTableColumnTmpBase(columnTmpBase);
    }
}
