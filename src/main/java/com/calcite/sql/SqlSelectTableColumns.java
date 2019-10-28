package com.calcite.sql;

import com.tmp.SelectSqlCaseTmp;
import com.tmp.SelectSqlTmp;
import com.tmp.SelectTableColumnTmp;
import com.tmp.SelectTableColumnTmpBase;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.calcite.util.SqlSelectUtil;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 解析sql中 insert ... select 部分
 */
public class SqlSelectTableColumns {


    private static Logger log = LoggerFactory.getLogger(SqlSelectTableColumns.class);

    /**
     * 解析select 部分
     *
     * @param sqlSelectList
     * @return
     */
    public static List<SelectTableColumnTmpBase> selectTableColumns(SqlSelect sqlSelectList, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        //剔除select × from(select ....) 的情况
        if (SqlSelectUtil.checkSelectAll(sqlSelectList)) {
            return selectTableColumns((SqlSelect) sqlSelectList.getFrom(), fromJoinTableColumnMap);
        }

        //--select字段
        SqlNodeList sqlNodeList = sqlSelectList.getSelectList();
        List<SqlNode> list = sqlNodeList.getList();

        // 非select × from(select ....) 的情况
        List<SelectTableColumnTmpBase> selectColumnTmps = new ArrayList<SelectTableColumnTmpBase>();

        for (SqlNode sqlNode1 : list) {

            SelectTableColumnTmpBase columnTmp = null;

            if (sqlNode1 instanceof SqlBasicCall) {

                // 结果特殊处理
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode1;

                SqlOperator sqlOperator = sqlBasicCall.getOperator();
                if (sqlOperator.getName().equalsIgnoreCase("AS")) { //定义别名

                    //直接表的字段
                    columnTmp = selectTableColumnAliasTmp(sqlBasicCall, fromJoinTableColumnMap);

                } else { //其他额外处理

                    SqlNode[] sqlNodes = sqlBasicCall.operands;
                    try {
                        SelectSqlTmp selectSqlTmp = new SelectSqlTmp(SelectSqlTmp.TYPE_3);
                        // 对于insert 某个字段的select部分 为sql
                        selectColumnBySql(sqlNodes, selectSqlTmp);

                        columnTmp = selectSqlTmp;

                    } catch (Exception e) {
                        log.error("select 部分解析异常!", e);
                    }

                }
                if (columnTmp != null) {
                    columnTmp.setType(SelectTableColumnTmp.TYPE_3);
                }


            } else if (sqlNode1 instanceof SqlIdentifier) {

                //直接表的字段
                columnTmp = selectTableColumnTmp(sqlNode1);

            } else if (sqlNode1 instanceof SqlCharStringLiteral) {
                //--默认数值
                columnTmp = new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_2);
            } else {
                //未知
                columnTmp = new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_0);
            }

            if (columnTmp != null) {

                columnTmp.setStrs(sqlNode1.toString());

                selectColumnTmps.add(columnTmp);
            }
        }

        return selectColumnTmps;
    }


    /**
     * 对于insert 某个字段的select部分 为sql
     *
     * @param sqlNode1s
     * @param selectSqlTmp
     */
    public static void selectColumnBySql(SqlNode[] sqlNode1s, SelectSqlTmp selectSqlTmp) {
        for (SqlNode sqlNode1 : sqlNode1s) {

            if (sqlNode1 instanceof SqlBasicCall) {
                // 结果特殊处理
                SqlBasicCall sqlBasicCall = (SqlBasicCall) sqlNode1;

                SqlOperator sqlOperator = sqlBasicCall.getOperator();
                if (sqlOperator.getName().equalsIgnoreCase("AS")) { //定义别名

                    //直接表的字段
                    SelectTableColumnTmp columnTmp = selectTableColumnTmp(sqlBasicCall.operands[0]);
                    columnTmp.setStrs(sqlNode1.toString());
                    selectSqlTmp.addSourceTableInfo(columnTmp.getTableAlias(), columnTmp.getColumnNames());

                } else {// 其他处理

                    SqlNode[] sqlNodes = sqlBasicCall.operands;
                    log.info("-----insert select 部分解析----------" + sqlBasicCall.toString());
                    try {
                        selectColumnBySql(sqlNodes, selectSqlTmp);
                    } catch (Exception e) {
                        //TODO
                        log.error("insert select 部分解析异常! 内容：【" + sqlBasicCall.toString() + "】", e);
                    }
                }

            } else if (sqlNode1 instanceof SqlIdentifier) {

                //直接表的字段
                SelectTableColumnTmp columnTmp = selectTableColumnTmp(sqlNode1);
                columnTmp.setStrs(sqlNode1.toString());
                selectSqlTmp.addSourceTableInfo(columnTmp.getTableAlias(), columnTmp.getColumnNames());
            }
        }
    }


    /**
     * 处理字段名别名
     *
     * @param sqlBasicCall
     * @return
     */
    public static SelectTableColumnTmpBase selectTableColumnAliasTmp(SqlBasicCall sqlBasicCall, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        if (sqlBasicCall.operands[0] instanceof SqlCase) {
            // case when then

            // 处理 insert...select...，select部分，每个字段中 case when 这种选择
            SelectSqlCaseTmp selectSqlCaseTmp = SqlBasicCallCalcite.insertSelectSqlCase((SqlCase) sqlBasicCall.operands[0], fromJoinTableColumnMap);

            String columnAliasName = sqlBasicCall.operands[1].toString();
            selectSqlCaseTmp.setColumnAlias(columnAliasName);
            return selectSqlCaseTmp;

        }
        if (sqlBasicCall.operands[0] instanceof SqlBasicCall) {

            SqlNode[] sqlNodes = ((SqlBasicCall) sqlBasicCall.operands[0]).operands;


            SelectSqlTmp selectSqlTmp = new SelectSqlTmp(SelectSqlTmp.TYPE_3);

            SqlOperator sqlOperator = sqlBasicCall.getOperator();
            if (sqlOperator.getName().equalsIgnoreCase("AS")) { //定义别名
                selectSqlTmp.setAliaName(sqlBasicCall.operands[1].toString());
            }
            try {
                SqlSelectTableColumns.selectColumnBySql(sqlNodes, selectSqlTmp);
                return selectSqlTmp;
            } catch (Exception e) {
                log.error("insert select selectTableColumnAliasTmp 部分解析异常! 内容：【" + sqlBasicCall.operands[0].toString() + "】", e);
                return null;
            }

        } else {

            SelectTableColumnTmp columnTmp = selectTableColumnTmp(sqlBasicCall.operands[0]);
            if (columnTmp == null) {
                return null;
            }
            String columnAliasName = sqlBasicCall.operands[1].toString();
            columnTmp.setColumnAlias(columnAliasName);
            return columnTmp;
        }


    }

    /**
     * 直接表的字段
     *
     * @param sqlNode1
     * @return
     */
    public static SelectTableColumnTmp selectTableColumnTmp(SqlNode sqlNode1) {

        if (sqlNode1 instanceof SqlIdentifier) {
            //直接表的字段
            /**
             * 举例：
             * insert ......
             *  SELECT
             I_STATEDATE
             ,B.SERIALNO
             .....
             */
            SelectTableColumnTmp columnTmp = new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_1);

            SqlIdentifier sqlIdentifier = (SqlIdentifier) sqlNode1;

            //表别名和字段名 [别名,字段名]/[字段名]
            List<String> tableColumnDatas = sqlIdentifier.names;

            if (tableColumnDatas.size() > 1) {
                columnTmp.setTableAlias(tableColumnDatas.get(0));
                columnTmp.setColumnNames(tableColumnDatas.get(1));
            } else {
                columnTmp.setTableAlias("");
                columnTmp.setColumnNames(tableColumnDatas.get(0));
            }

            return columnTmp;

        } else if (sqlNode1 instanceof SqlCharStringLiteral) {//默认字符串
            //eg: '' AS KMH
            return new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_2);

        } else if (sqlNode1 instanceof SqlNumericLiteral) {//默认数值
            //0 AS DKJZ
            return new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_2);

        } else if (sqlNode1 instanceof SqlLiteral) {//默认字值
            //eg: '' AS KMH
            return new SelectTableColumnTmp(SelectTableColumnTmp.TYPE_2);

        } else {
            System.out.println();
            //TODO
            return null;
        }


    }
}
