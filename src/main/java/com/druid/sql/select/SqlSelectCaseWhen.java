package com.druid.sql.select;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.calcite.temp.fromjoin.FromJoinTableColumnTmp;
import com.druid.sql.from.SqlSelectFrom;
import com.tmp.SelectSqlTmp;

import java.util.List;
import java.util.Map;

/**
 * 处理 select case when 类型
 */
public class SqlSelectCaseWhen {

    /**
     * 处理select case when 类型
     *
     * @param sqlCaseExpr
     * @return
     */
    public static SelectSqlTmp opSqlSelectCaseWhen(SQLCaseExpr sqlCaseExpr, Map<String, String> tableAliasmap, Map<String, Map<String, FromJoinTableColumnTmp>> fromJoinTableColumnMap) {

        //eg:
        /**
         * eg
         * insert ....
         *      select
         *          tableA.columnB
         *          case
         *          WHEN LENGTH (A.PUTOUTDATE) IN (8,10) THEN TO_DATE (A.PUTOUTDATE,'YYYYMMDD')
         *          WHEN LENGTH (A.PUTOUTDATE) IN (11,12) THEN SELECT CM_INTR1 FROM  ods.o_CBOD_CMIRTIRT WHERE  CM_INTR_TYP ='B5'\n" +
         *                                                              AND CM_IRT_STS = '0'
         *          END AS KHRQ,
         *          tableA.columnC
         */

        SelectSqlTmp selectSqlTmp = new SelectSqlTmp(SelectSqlTmp.TYPE_3);

        //多个when...then...部分
        List<SQLCaseExpr.Item> caseItemList = sqlCaseExpr.getItems();
        for (SQLCaseExpr.Item caseItem : caseItemList) {

            SQLExpr valueSQLExpr = caseItem.getValueExpr();

            //select 单个部分，SelectSqlTmp 记录信息
            SqlSelectInfo.opSelectSqlTmp(valueSQLExpr, selectSqlTmp, tableAliasmap, fromJoinTableColumnMap);

        }


        return selectSqlTmp;
    }
}
