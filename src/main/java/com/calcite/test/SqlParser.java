package com.calcite.test;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.dialect.db2.visitor.DB2SchemaStatVisitor;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

public class SqlParser {
    private static List<SQLStatement> getSQLStatementList(String sql) {
        String dbType = JdbcConstants.DB2;
        return SQLUtils.parseStatements(sql, dbType);
    }

    public static void main(String[] args) {
        //String sql = "select age a,name n from student s inner join (select id,name from score where sex='女') temp on sex='男' and temp.id in(select id from score where sex='男') where student.name='zhangsan' group by student.age order by student.id ASC;";

        String sql = "SELECT   replace(brnname,'支行','') 机构名称,\n" +
                "DEC(CASE WHEN sum(CKXJ)=0 THEN 0 ELSE round(CAST(sum(DKXJ) AS DEC (31,10))/CAST(sum(CKXJ) AS DEC (31,5)),4) END *100,31,2) 存贷比, \n" +
                "DEC(round(sum(BZJ)/10000,2),31,2) 保证金,\n" +
                "DEC(round(sum(BBLC)/10000,2),31,2) 保本理财, \n" +
                "DEC(round(sum(TX)/10000,2),31,2) 贴现, \n" +
                "DEC(round(sum(YCCK)/10000,2),31,2) 银承敞口, \n" +
                "DEC(round(sum(CDK)/10000,2),31,2) 纯贷款, \n" +
                "DEC(round(sum(DQYCJE)/10000,2),31,2) S到期银承敞口,\n" +
                "'%,万元,万元,万元,万元,万元,万元' 单位\n" +
                "           FROM      MBI.S_MBI_DATA_LS_CDB_D A \n" +
                "           INNER JOIN (\n" +
                "                       SELECT   BRNNBR,\n" +
                "                                BRNNBR AS UPBRNNBR\n" +
                "                       FROM     MBI.DIM_BRANCH T \n" +
                "                      ) B \n" +
                "           ON         A.BRANCHID= B.BRNNBR \n" +
                "           INNER JOIN TABLE(MBI.DIM_BRANCH_RECU('$branchId') ) C \n" +
                "           ON         B.UPBRNNBR=C.BRNNBR\n" +
                "           WHERE      A.STATDATE ='$statDate' \n" +
                "           AND        substr(to_char(CURRENT TIMESTAMP ,'yyyy-mm-dd hh24:mi:ss'),1,10)<>'$statDate'\n" +
                "           AND   \t  C.UPPERBRN='$branchId'         \n" +
                "\t\t   AND        ORDERNO BETWEEN 1 AND 80\n" +
                "\t\t   AND\t\t  A.CCYNBR='$ccynbr'\n" +
                "           GROUP BY   C.BRNNAME, C.BRNNBR, C.ORDERNO";




        System.out.println("SQL语句为：" + sql);
        //格式化输出
        String result = SQLUtils.format(sql, JdbcConstants.DB2);
        System.out.println("格式化后输出：\n" + result);
        System.out.println("*********************");
        List<SQLStatement> sqlStatementList = getSQLStatementList(sql);
        //默认为一条sql语句
        SQLStatement stmt = sqlStatementList.get(0);

        DB2SchemaStatVisitor visitor = new DB2SchemaStatVisitor();
                //MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);


        List<SQLMethodInvokeExpr> sss =  visitor.getFunctions();





        System.out.println("数据库类型\t\t" + visitor.getDbType());

        //获取字段名称
        System.out.println("查询的字段\t\t" + visitor.getColumns());

        //获取表名称
        System.out.println("表名\t\t\t" + visitor.getTables().keySet());

        System.out.println("条件\t\t\t" + visitor.getConditions());

        System.out.println("group by\t\t" + visitor.getGroupByColumns());

        System.out.println("order by\t\t" + visitor.getOrderByColumns());


    }
}
