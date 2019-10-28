package com.druid.sql.select;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.db2.ast.stmt.DB2SelectQueryBlock;
import com.druid.util.SqlSelectUtil;
import com.tmp.SelectSqlCaseTmp;
import com.tmp.SelectSqlTmp;
import com.tmp.SelectTableColumnTmp;
import com.tmp.SelectTableColumnTmpBase;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class SqlSelectInfo {


    /**
     * 处理 select 部分
     *
     * @param sqlSelect
     * @return
     */
    public static List<SelectTableColumnTmpBase> operateSqlSelect(SQLSelect sqlSelect) {


        SQLSelectQuery sqlSelectQuery = sqlSelect.getQuery();

        if (!(sqlSelectQuery instanceof DB2SelectQueryBlock)) {
            throw new RuntimeException("解析sql中sql部分，未知类型！[" + sqlSelectQuery.toString() + "]");
        }

        //select部分 sql格式转换
        DB2SelectQueryBlock db2SelectQueryBlock = (DB2SelectQueryBlock) sqlSelectQuery;

        return selectColumn(db2SelectQueryBlock);
    }


    /**
     * 处理 select 部分
     *
     * @param db2SelectQueryBlock
     * @return
     */
    private static List<SelectTableColumnTmpBase> selectColumn(DB2SelectQueryBlock db2SelectQueryBlock) {

        //判断 select × from(select ....) 的情况
        if (SqlSelectUtil.checkSelectAll(db2SelectQueryBlock.getSelectList())) {

            SQLSubqueryTableSource subQueryTableSource = (SQLSubqueryTableSource) db2SelectQueryBlock.getFrom();
            return operateSqlSelect(subQueryTableSource.getSelect());
        }


        //并非 select × from(select ....) 的情况
        List<SelectTableColumnTmpBase> selectColumnTmps = new ArrayList<SelectTableColumnTmpBase>();

        //遍历select每一个部分
        List<SQLSelectItem> selectItems = db2SelectQueryBlock.getSelectList();
        for (SQLSelectItem sqlSelectItem : selectItems) {

            //字段别名
            String columnAlias = StringUtils.isBlank(sqlSelectItem.getAlias()) ? "" : sqlSelectItem.getAlias();

            //select单个内容部分
            SelectTableColumnTmpBase columnTmpBase = opSelectColumn(columnAlias, sqlSelectItem.getExpr());
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
    private static SelectTableColumnTmpBase opSelectColumn(String columnAlias, SQLExpr sqlExpr) {

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
                opSelectSqlTmp(sqlMethodExpr, selectSqlTmp, false);
            }
            return selectSqlTmp;
        }

        if (sqlExpr instanceof SQLBinaryOpExpr) {//eg: A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD||GL_THIRD_LEVEL_LG_CD
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;

            SelectSqlTmp selectSqlTmp = new SelectSqlTmp(SelectSqlTmp.TYPE_3);
            selectColumnSQLBinaryOpExpr(sqlBinaryOpExpr, selectSqlTmp);
            return selectSqlTmp;
        }
        if (sqlExpr instanceof SQLNumericLiteralExpr//数字常量
                || sqlExpr instanceof SQLTextLiteralExpr//字符串
                || sqlExpr instanceof SQLNullExpr//null
                ) {
            return new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_2);

        }
        if (sqlExpr instanceof SQLCaseExpr) {// select case when 类型
            return new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_2);

        }

        //未知
        return new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_0);

    }


    /**
     * select 部分的sql，为类似 A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD||GL_THIRD_LEVEL_LG_CD
     *
     * @param selectSqlTmp
     */
    public static void selectColumnSQLBinaryOpExpr(SQLBinaryOpExpr sqlBinaryOpExpr, SelectSqlTmp selectSqlTmp) {

        //A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD
        SQLExpr sqlExprLeft = sqlBinaryOpExpr.getLeft();
        SelectTableColumnTmpBase tmpBase = opSelectColumn("", sqlExprLeft);
        selectSqlTmp.addDataBySelectTableColumnTmpBase(tmpBase);

        //GL_THIRD_LEVEL_LG_CD
        SQLExpr sqlExprRight = sqlBinaryOpExpr.getRight();


        opSelectSqlTmp(sqlExprRight, selectSqlTmp);
    }

    private static void opSelectSqlTmp(SQLExpr sqlExpr, SelectSqlTmp selectSqlTmp) {

        opSelectSqlTmp(sqlExpr, selectSqlTmp, true);
    }

    /**
     * @param sqlExpr
     * @param selectSqlTmp
     * @param needDefault
     */
    private static void opSelectSqlTmp(SQLExpr sqlExpr, SelectSqlTmp selectSqlTmp, boolean needDefault) {
        SelectTableColumnTmpBase columnTmpBase = opSelectColumn("", sqlExpr);

        if (!needDefault &&
                (columnTmpBase.getType() == SelectTableColumnTmpBase.TYPE_2
                        || columnTmpBase.getType() == SelectTableColumnTmpBase.TYPE_0)) {
            //默认值，不记录
            return;
        }

        //添加数据到SelectSqlTmp
        selectSqlTmp.addDataBySelectTableColumnTmpBase(columnTmpBase);
    }
}
