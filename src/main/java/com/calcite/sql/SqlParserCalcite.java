package com.calcite.sql;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.calcite.temp.*;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.calcite.temp.insertwith.InsertWithOnSqlTmp;
import com.calcite.util.DruidUtil;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 处理sql
 */
public class SqlParserCalcite {


    public static TargetTableInfo opTargetTableInfoByDruid(String sql, String sqlNo) throws Exception {


        //获取目标表信息
        TargetTableInfo targetTableInfo = DruidUtil.parseInsertSql(sql, sqlNo);
        if (targetTableInfo == null) {
            throw new RuntimeException("不是insert sql!");
        }

        parserSql(sql, targetTableInfo);

        return targetTableInfo;
    }


    /**
     * 解析SQL
     *
     * @throws SqlParseException
     */
    public static void parserSql(String sql, TargetTableInfo targetTableInfo) throws SqlParseException {

        // 解析配置 - mysql设置
        SqlParser.Config mysqlConfig = SqlParser.configBuilder().setConformance(SqlConformanceEnum.BABEL).setLex(Lex.ORACLE).build();
        // 创建解析器
        SqlParser parser = SqlParser.create("", mysqlConfig);

        // 解析sql
        SqlNode sqlNode = parser.parseQuery(sql);

        if (!(sqlNode instanceof SqlInsert)) {
            throw new RuntimeException("不是insert sql!");
        }

        //--select字段
        List<SelectTableColumnTmpBase> selectTableColumnTmps = new ArrayList<SelectTableColumnTmpBase>();

        //解析源数据表和别名<别名,表名>
        Map<String, String> tableAliasmap = new HashMap<String, String>();


        //insert  with 临时表，管使用的真实表信息<临时表别名,<临时表使用字段名,对象信息>>
        Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap = new HashMap<String, Map<String, InsertWithOnSqlTmp>>();
        ///处理form关联（左/右）临时表  <关联,<字段名,对象>>
        Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap = new HashMap<String, Map<String, FromJoinTableColumnTmp>>();

        //insert语句对象体
        SqlInsert sqlInsert = (SqlInsert) sqlNode;

        SqlNode sqlNodeSource = sqlInsert.getSource();
        if (sqlNodeSource instanceof SqlWith) {

            // 有临时表 insert ..... with as...select ...
            insertWithAsOp(sqlNodeSource, selectTableColumnTmps, tableAliasmap, tmpTableRelationTableMap, fromJoinTableColumnMap);


        } else if (sqlNodeSource instanceof SqlSelect) {

            //insert .....select ... from 表
            insertSelectOp(sqlNodeSource, selectTableColumnTmps, tableAliasmap, fromJoinTableColumnMap);

        } else if (sqlNodeSource instanceof SqlBasicCall) {

            //处理 insert .... select ....(select 部分 类型为SqlBasicCall)
            SqlBasicCallCalcite.insertSelectSqlParseSqlBasicCall((SqlBasicCall) sqlNodeSource, selectTableColumnTmps, tableAliasmap, fromJoinTableColumnMap);

        } else {

            //TODO
            throw new RuntimeException("--------------sqlInsert.getSource--------unknow-type---------------------------------------");
        }


        //最后处理，整理insert 表字段，来源关联
        atLastStep(targetTableInfo, selectTableColumnTmps, tableAliasmap, tmpTableRelationTableMap, fromJoinTableColumnMap);


        System.out.println(JSONObject.toJSONString(targetTableInfo));

    }

    /**
     * 有临时表  insert ..... with as...select ...
     *
     * @param sqlNodeSource
     * @param selectTableColumnTmps
     * @param tableAliasmap
     * @param tmpTableRelationTableMap
     */
    private static void insertWithAsOp(SqlNode sqlNodeSource, List<SelectTableColumnTmpBase> selectTableColumnTmps, Map<String, String> tableAliasmap, Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        SqlWith sqlNodeSourceWith = (SqlWith) sqlNodeSource;

        //insert ..... with as部分
        SqlNodeList withAsNodeList = sqlNodeSourceWith.withList;

        if (withAsNodeList != null) {
            // 处理insert ..... with as部分
            SqlInsertWithAsCalcite.operateWithAs(withAsNodeList, tmpTableRelationTableMap, fromJoinTableColumnMap);
        }

        //TODO select部分，try catch
        SqlSelect sqlSelectList = (SqlSelect) sqlNodeSourceWith.body;

        //--select字段
        selectTableColumnTmps.addAll(SqlSelectTableColumns.selectTableColumns(sqlSelectList, fromJoinTableColumnMap));


        //解析源数据表和别名<别名,表名>
        tableAliasmap.putAll(SqlSelectFromCalcite.generateSelectTables(sqlSelectList, fromJoinTableColumnMap));

    }


    /**
     * insert .....select ... from 表
     *
     * @param sqlNodeSource
     * @param selectTableColumnTmps
     * @param tableAliasmap
     */
    private static void insertSelectOp(SqlNode sqlNodeSource, List<SelectTableColumnTmpBase> selectTableColumnTmps, Map<String, String> tableAliasmap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {


        SqlSelect sqlSelectList = (SqlSelect) sqlNodeSource;

        //--select字段
        selectTableColumnTmps.addAll(SqlSelectTableColumns.selectTableColumns(sqlSelectList, fromJoinTableColumnMap));


        //解析源数据表和别名<别名,表名>
        tableAliasmap.putAll(SqlSelectFromCalcite.generateSelectTables(sqlSelectList, fromJoinTableColumnMap));
    }


    /**
     * 最后处理，整理insert 表字段，来源关联
     *
     * @param targetTableInfo          目标表信息
     * @param selectTableColumnTmps    源表字段信息
     * @param tableAliasmap            源表别名
     * @param tmpTableRelationTableMap 临时表字段信息
     */
    private static void atLastStep(TargetTableInfo targetTableInfo, List<SelectTableColumnTmpBase> selectTableColumnTmps, Map<String, String> tableAliasmap, Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        if (targetTableInfo.getTableColumn().size() > 0 && targetTableInfo.getTableColumn().size() != selectTableColumnTmps.size()) {

            //TODO
            System.out.println(JSONArray.toJSONString(selectTableColumnTmps));

            System.out.println("==============================================");
            //TODO
            System.out.println(JSONObject.toJSONString(tableAliasmap));

            System.out.println("==============================================");
            //TODO
            System.out.println(JSONObject.toJSONString(tmpTableRelationTableMap));

            System.out.println("==============================================");

            throw new RuntimeException("insert 表字段数和 select 字段数不匹配! targetTable字段数【" + targetTableInfo.getTableColumn().size() + "】,select 字段数[" + selectTableColumnTmps.size() + "] ");
        }


        boolean insertTableColumnExist = targetTableInfo.getTableColumn().size() > 0;

        for (int i = 0; i < selectTableColumnTmps.size(); i++) {

            //目标表字段名
            String targetTableColumnStr = insertTableColumnExist ? targetTableInfo.getTableColumn().get(i) : "UNKNOW_COLUMN_" + (i + 1);


            SelectTableColumnTmpBase selectColumnTmpBase = selectTableColumnTmps.get(i);


            TargetTableColumn targetTableColumn = new TargetTableColumn(targetTableColumnStr);

            switch (selectColumnTmpBase.getType()) {
                case SelectTableColumnTmp.TYPE_1://直接表字段
                case SelectTableColumnTmp.TYPE_3://额外处理

                    if (selectColumnTmpBase instanceof SelectTableColumnTmp) {

                        //直接查询表字段
                        selectTableColumn(selectColumnTmpBase, tableAliasmap, tmpTableRelationTableMap, targetTableColumn, fromJoinTableColumnMap);

                    } else if (selectColumnTmpBase instanceof SelectSqlTmp) {
                        //表字段通过sql获取
                        selectTableColumnSql(selectColumnTmpBase, tableAliasmap, targetTableColumn);

                    } else if (selectColumnTmpBase instanceof SelectSqlCaseTmp) {

                        //插入语句，select 表字段为sql，sql中有case选择
                        selectTableColumnSqlCase(selectColumnTmpBase, tableAliasmap, targetTableColumn);
                    }

                    break;
                case SelectTableColumnTmp.TYPE_2://默认值
                    targetTableColumn.setType(TargetTableColumn.TYPE_2);
                    break;

                default:
                    targetTableColumn.setType(TargetTableColumn.TYPE_3);
                    break;
            }

            targetTableInfo.getTableColumnList().add(targetTableColumn);

        }

    }


    /**
     * 直接查询表字段
     *
     * @param selectColumnTmpBase
     * @param tableAliasmap       表名和别名管理
     * @param targetTableColumn   临时表名和临时中表、字段关联
     */
    private static void selectTableColumn(SelectTableColumnTmpBase selectColumnTmpBase, Map<String, String> tableAliasmap, Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap, TargetTableColumn targetTableColumn, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {
        SelectTableColumnTmp selectColumnTmp = (SelectTableColumnTmp) selectColumnTmpBase;


        //源表字段
        String sourceColumnName = selectColumnTmp.getColumnNames();

        //源表名
        String sourceTableNameStr = "";
        //源表信息
        String sourceTableName = tableAliasmap.get(selectColumnTmp.getTableAlias());

        if (StringUtils.isBlank(sourceTableName)) {
            //别名找不到
            sourceTableNameStr = "unKnow";
            targetTableColumn.setType(TargetTableColumn.TYPE_3);
            targetTableColumn.addSourceTableInfo(sourceTableNameStr, sourceColumnName);
            return;
        }

        if (opInsertWithOnSqlTmp(targetTableColumn, tmpTableRelationTableMap, sourceTableName, sourceColumnName)) {
            //处理insert...with时，临时表
            return;
        }
        if (opFromJoinTableColumnTmp(targetTableColumn, fromJoinTableColumnMap, sourceTableName, sourceColumnName)) {
            //处理form关联（左/右）临时表
            return;
        }

        sourceTableNameStr = sourceTableName;
        targetTableColumn.addSourceTableInfo(sourceTableNameStr, sourceColumnName);
    }


    /**
     * 处理form关联（左/右）临时表
     *
     * @param targetTableColumn
     * @param fromJoinTableColumnMap
     * @param sourceTableName
     * @param sourceColumnName
     * @return
     */
    private static boolean opFromJoinTableColumnTmp(TargetTableColumn targetTableColumn, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap, String sourceTableName, String sourceColumnName) {

        if (fromJoinTableColumnMap.size() <= 0) {
            //没有临时表信息
            return false;
        }

        //临时表判断
        Map<String, FromJoinTableColumnTmp> tmpTableColumnMap = fromJoinTableColumnMap.get(sourceTableName);
        if (tmpTableColumnMap == null) {
            //该表不是临时表
            return false;
        }


        //该表是临时表
        FromJoinTableColumnTmp tmpTableColumnObj = tmpTableColumnMap.get(sourceColumnName);
        if (tmpTableColumnObj == null) {
            //没有相关字段信息
            return false;
        }

        //从临时表中，取该字段真实源数据字段
        targetTableColumn.getSourceTableColumnMap().putAll(tmpTableColumnObj.getSourceTableInfo());
        return true;
    }

    /**
     * 处理insert...with时，临时表
     *
     * @param targetTableColumn
     * @param tmpTableRelationTableMap
     * @param sourceTableName
     * @param sourceColumnName
     * @return
     */
    private static boolean opInsertWithOnSqlTmp(TargetTableColumn targetTableColumn, Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap, String sourceTableName, String sourceColumnName) {

        if (tmpTableRelationTableMap.size() <= 0) {
            //没有临时表信息
            return false;
        }

        //临时表判断
        Map<String, InsertWithOnSqlTmp> tmpTableColumnMap = tmpTableRelationTableMap.get(sourceTableName);
        if (tmpTableColumnMap == null) {
            //该表不是临时表
            return false;
        }


        //该表是临时表
        InsertWithOnSqlTmp tmpTableColumnObj = tmpTableColumnMap.get(sourceColumnName);
        if (tmpTableColumnObj == null) {
            //没有相关字段信息
            return false;
        }

        //从临时表中，取该字段真实源数据字段
        targetTableColumn.getSourceTableColumnMap().putAll(tmpTableColumnObj.getSourceTableInfo());

        return true;
    }


    /**
     * 插入语句，select 表字段为sql，sql中有case选择
     *
     * @param selectColumnTmpBase
     * @param tableAliasmap
     * @param targetTableColumn
     */
    private static void selectTableColumnSqlCase(SelectTableColumnTmpBase selectColumnTmpBase, Map<String, String> tableAliasmap, TargetTableColumn targetTableColumn) {

        SelectSqlCaseTmp selectColumnTmp = (SelectSqlCaseTmp) selectColumnTmpBase;


        for (String tableAliaName : selectColumnTmp.getTableAliaColumns().keySet()) {


            List<String> columnNames = selectColumnTmp.getTableAliaColumns().get(tableAliaName);
            for (String sourceColumnName : columnNames) {

                //源表名
                String sourceTableNameStr = "";
                //源表信息
                String sourceTableName = tableAliasmap.get(tableAliaName);
                if (StringUtils.isBlank(sourceTableName)) {
                    sourceTableNameStr = "unKnow";
                    targetTableColumn.setType(TargetTableColumn.TYPE_3);
                } else {
                    sourceTableNameStr = sourceTableName;
                }

                targetTableColumn.addSourceTableInfo(sourceTableNameStr, sourceColumnName);
            }
        }
    }


    /**
     * 插入语句，select 表字段为sql关联多个表字段，处理
     *
     * @param selectColumnTmpBase
     * @param tableAliasmap
     * @param targetTableColumn
     */
    private static void selectTableColumnSql(SelectTableColumnTmpBase selectColumnTmpBase, Map<String, String> tableAliasmap, TargetTableColumn targetTableColumn) {

        SelectSqlTmp selectColumnTmp = (SelectSqlTmp) selectColumnTmpBase;


        for (String tableAliaName : selectColumnTmp.getTableAliaColumns().keySet()) {

            List<String> columnNames = selectColumnTmp.getTableAliaColumns().get(tableAliaName);
            for (String sourceColumnName : columnNames) {

                //源表名
                String sourceTableNameStr = "";
                //源表信息
                String sourceTableName = tableAliasmap.get(tableAliaName);
                if (StringUtils.isBlank(sourceTableName)) {
                    sourceTableNameStr = "unKnow";
                    targetTableColumn.setType(TargetTableColumn.TYPE_3);
                } else {
                    sourceTableNameStr = sourceTableName;
                }

                targetTableColumn.addSourceTableInfo(sourceTableNameStr, sourceColumnName);
            }
        }
    }
}
