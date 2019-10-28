package com.druid.util;

import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;

import java.util.List;

public class SqlSelectUtil {


    /**
     * 检验sql是否是 select *
     *
     * @param selectItems
     * @return
     */
    public static boolean checkSelectAll(List<SQLSelectItem> selectItems) {
        //--select字段
        if (selectItems.size() == 1) {
            //剔除select × from(select ....) 的情况
            SQLSelectItem sqlNode1One = selectItems.get(0);
            if (sqlNode1One.getExpr() instanceof SQLAllColumnExpr) {
                return true;
            }
        }

        return false;
    }
}
