package com.calcite.sql;

import com.alibaba.fastjson.JSONObject;
import com.calcite.temp.SelectSqlCaseTmp;
import com.calcite.temp.SelectSqlTmp;
import com.calcite.temp.SelectTableColumnTmp;
import com.calcite.temp.SelectTableColumnTmpBase;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.calcite.temp.insertwith.InsertWithOnSqlTmp;
import org.apache.calcite.sql.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * insert ..... with as部分
 */
public class SqlInsertWithAsCalcite {

    private static Logger log = LoggerFactory.getLogger(SqlInsertWithAsCalcite.class);

    /**
     * insert ..... with as部分
     *
     * @param withAdNodeList
     * @param tmpTableRelationTableMap ##insert  with 临时表，管使用的真实表信息<临时表别名,<临时表使用字段名,对象信息>>
     * @param fromJoinTableColumnMap   ##  <关联别名,<字段名,对象>>
     */
    public static void operateWithAs(SqlNodeList withAdNodeList, Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {


        List<SqlNode> sqlNodes = withAdNodeList.getList();
        if (CollectionUtils.isEmpty(sqlNodes)) {
            return;
        }

        for (SqlNode sqlNode : sqlNodes) {


            if (sqlNode instanceof SqlWithItem) {

                //处理insert with as部分，单个select sql
                opSqlWithItem((SqlWithItem) sqlNode, tmpTableRelationTableMap, fromJoinTableColumnMap);
            } else {
                log.error("unknow type sqlNode in operateWithAs! sql:[" + sqlNode.toString() + "]");
            }
        }

        return;
    }

    /**
     * 处理insert with as部分，单个select sql
     *
     * @param sqlWithItem
     */
    private static void opSqlWithItem(SqlWithItem sqlWithItem, Map<String, Map<String, InsertWithOnSqlTmp>> tmpTableRelationTableMap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        /**
         * insert into table2
         with
         s1 as (select rownum c1 from dual connect by rownum <= 10),
         s2 as (select rownum c2 from dual connect by rownum <= 10)
         select a.c1, b.c2 from s1 a, s2 b where...;

         sql 中   " s1 as (select rownum c1 from dual connect by rownum <= 10)"  部分
         */

        SqlIdentifier sqlName = sqlWithItem.name;//别名  eg: 上面的 a1,s2

        //临时表别名
        String tmpTableAlias = sqlName.toString();

        SqlNode withSqlSelectNode = sqlWithItem.query;


        //解析源数据表和别名<别名,表名>
        Map<String, String> tableAliasmap = SqlSelectFromCalcite.generateSelectTables((SqlSelect) withSqlSelectNode, fromJoinTableColumnMap);


        //选择字段
        List<SelectTableColumnTmpBase> withAsColumns = SqlSelectTableColumns.selectTableColumns((SqlSelect) withSqlSelectNode, fromJoinTableColumnMap);
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
