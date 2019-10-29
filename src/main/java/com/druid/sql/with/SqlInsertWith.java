package com.druid.sql.with;

import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.fastjson.JSONObject;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.calcite.temp.insertwith.InsertWithOnSqlTmp;
import com.druid.sql.from.SqlSelectFrom;
import com.druid.sql.select.SqlSelectInfo;
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

public class SqlInsertWith {

    private static Logger log = LoggerFactory.getLogger(SqlInsertWith.class);

    /**
     * 有临时表  insert ..... with as...select ...
     *
     * @param sqlNodeSourceWith
     * @param tableAliasmap
     * @param tmpTableRelationTableMap
     */
    public static void insertWithAsOp(SQLWithSubqueryClause sqlNodeSourceWith, Map<String, String> tableAliasmap, Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {


        //insert ..... with as部分
        List<SQLWithSubqueryClause.Entry> withAsNodeList = sqlNodeSourceWith.getEntries();

        if (CollectionUtils.isEmpty(withAsNodeList)) {
            return;
        }

        // 处理insert ..... with as部分
        operateWithAs(withAsNodeList,tableAliasmap, tmpTableRelationTableMap, fromJoinTableColumnMap);
    }


    /**
     * insert ..... with as部分
     *
     * @param withAdNodeList
     * @param tmpTableRelationTableMap ##insert  with 临时表，管使用的真实表信息<临时表别名,<临时表使用字段名,对象信息>>
     * @param fromJoinTableColumnMap   ##  <关联别名,<字段名,对象>>
     */
    private static void operateWithAs(List<SQLWithSubqueryClause.Entry> withAdNodeList, Map<String, String> tableAliasmap, Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        for (SQLWithSubqueryClause.Entry sqlNode : withAdNodeList) {
            SQLSelect withSqlSelect = sqlNode.getSubQuery();

            if (withSqlSelect.getQuery() instanceof SQLSelectQueryBlock) {

                //处理insert with as部分，单个select sql
                opSqlWithItem(sqlNode.getAlias(), withSqlSelect, tableAliasmap, tmpTableRelationTableMap, fromJoinTableColumnMap);
            } else {
                log.error("unknow type sqlNode in operateWithAs! sql:[" + sqlNode.toString() + "]");
            }
        }
    }

    /**
     * 处理insert with as部分，单个select sql
     *
     * @param tmpTableAlias            临时表别名
     * @param withSqlSelect
     * @param tmpTableRelationTableMap
     * @param fromJoinTableColumnMap
     */
    private static void opSqlWithItem(String tmpTableAlias, SQLSelect withSqlSelect, Map<String, String> tableAliasmap, Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        /**
         * insert into table2
         with
         s1 as (select rownum c1 from dual connect by rownum <= 10),
         s2 as (select rownum c2 from dual connect by rownum <= 10)
         select a.c1, b.c2 from s1 a, s2 b where...;

         sql 中   " s1 as (select rownum c1 from dual connect by rownum <= 10)"  部分
         */


        //解析源数据表和别名<别名,表名>
        SqlSelectFrom.generateSelectTables(withSqlSelect, tableAliasmap, fromJoinTableColumnMap);


        //选择字段
        List<SelectTableColumnTmpBase> withAsColumns = SqlSelectInfo.operateSqlSelect(withSqlSelect, tableAliasmap, fromJoinTableColumnMap);
        if (withAsColumns == null) {
            return;
        }

        Map<String, InsertWithOnSqlTmp> tempTableColumnList = tmpTableRelationTableMap.get(tmpTableAlias);
        if (tempTableColumnList == null) {
            tempTableColumnList = new HashMap<String, InsertWithOnSqlTmp>();
            tmpTableRelationTableMap.put(tmpTableAlias, tempTableColumnList);
        }

        //insert  with 临时表，管使用的真实表信息<临时表别名,对象新>

        for (SelectTableColumnTmpBase withAsColumn : withAsColumns) {


            //一个from/join 临时对象
            InsertWithOnSqlTmp fromJoinTmp = new InsertWithOnSqlTmp(tmpTableAlias);

            if (withAsColumn instanceof SelectSqlTmp) {

                SelectSqlTmp tableColumnTmp = (SelectSqlTmp) withAsColumn;
                if (tableColumnTmp.getTableAliaColumns().size() <= 0) {
                    continue;
                }

                fromJoinTmp.setColumnName(tableColumnTmp.getAliaName());//临时表使用字段名

                //别名信息
                fromJoinTmp.getSourceTableInfo().putAll(tableColumnTmp.getTableAliaColumns());
                tempTableColumnList.put(fromJoinTmp.getColumnName(), fromJoinTmp);

            } else if (withAsColumn instanceof SelectTableColumnTmp) {


                //一个select sql临时对象
                InsertWithOnSqlTmp insertWithTmp = new InsertWithOnSqlTmp(tmpTableAlias);

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

                insertWithTmp.setColumnName(sqlUseColumnName);//临时表使用字段名

                //临时表，select ... from 表 真实名字
                String tableRealName = tableAliasmap.get(tableColumnTmp.getTableAlias());
                tableRealName = tableRealName == null ? "" : tableRealName;

                //临时表使用字段，对应源数据表名，对应源数据表字段
                insertWithTmp.addSourceTableInfo(tableRealName, tableColumnTmp.getColumnNames());

                tempTableColumnList.put(sqlUseColumnName, insertWithTmp);

            } else if (withAsColumn instanceof SelectSqlCaseTmp) {


                SelectSqlCaseTmp tableColumnTmp = (SelectSqlCaseTmp) withAsColumn;
                if (tableColumnTmp.getTableAliaColumns().size() <= 0) {
                    continue;
                }

                fromJoinTmp.setColumnName(tableColumnTmp.getColumnAlias());//临时表使用字段名

                //别名信息
                fromJoinTmp.getSourceTableInfo().putAll(tableColumnTmp.getTableAliaColumns());
                tempTableColumnList.put(fromJoinTmp.getColumnName(), fromJoinTmp);
            } else {
                log.error("unKnow Type!fromJoinTableColumn() ! data: [" + JSONObject.toJSONString(withAsColumn) + "]");
            }
        }

    }

}
