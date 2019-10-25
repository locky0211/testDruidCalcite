package com.util;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class SqlSelectUtil {


    /**
     * 检验sql是否是 select *
     * @param sqlSelect
     * @return
     */
    public static boolean checkSelectAll(SqlSelect sqlSelect) {
        //--select字段
        SqlNodeList sqlNodeList = sqlSelect.getSelectList();
        List<SqlNode> list = sqlNodeList.getList();
        if (list.size() == 1) {
            //剔除select × from(select ....) 的情况
            SqlNode sqlNode1One = list.get(0);
            if (sqlNode1One instanceof SqlIdentifier) {
                SqlIdentifier sqlNode1OneIdentifier = (SqlIdentifier) sqlNode1One;
                if (StringUtils.equals(sqlNode1OneIdentifier.toString(), "*")) {
                    return true;
                }
            }
        }

        return false;
    }
}
