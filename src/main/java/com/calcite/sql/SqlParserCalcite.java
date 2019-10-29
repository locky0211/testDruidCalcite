package com.calcite.sql;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.calcite.temp.insertwith.InsertWithOnSqlTmp;
import com.calcite.util.DruidUtil;
import com.tmp.*;
import com.tmp.targettable.TargetTableColumn;
import com.tmp.targettable.TargetTableInfo;
import com.util.AtLastUtil;
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
        AtLastUtil.atLastStep(targetTableInfo, selectTableColumnTmps, tableAliasmap, tmpTableRelationTableMap, fromJoinTableColumnMap);


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









}
