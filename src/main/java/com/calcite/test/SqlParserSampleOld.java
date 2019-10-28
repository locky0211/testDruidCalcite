package com.calcite.test;


import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * @author quxiucheng
 * @date 2019-04-22 11:45:00
 */
public class SqlParserSampleOld {

    public static void main(String[] args) throws SqlParseException {
        parserSql();
        parserExp();

    }

    /**
     * 解析SQL
     * @throws SqlParseException
     */
    public static void parserSql() throws SqlParseException {
        // 解析配置 - mysql设置
        SqlParser.Config mysqlConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        // 创建解析器
        SqlParser parser = SqlParser.create("", mysqlConfig);
        // Sql语句
      //  String sql = "select * from emps where id = 1";
       // String sql = "select a.id,a.name,b.gender from emps a left join test b on a.id= b.id where a.id in( select id from test_c )";


        String sql = "INSERT INTO S_GL_INN_ACCOUNT_D" +
                "(" +
                "STATDATE" +
                ",ACCTNBR" +
                ",CCYNBR" +
                ",BRNNBR" +
                ",ITEMCODE" +
                ",ITEMDR" +
                ",BUSNTYP" +
                ",ACCTNM" +
                ")" +
                "SELECT " +
                "I_STATEDATE STATDATE" +
                ",A.GL_ACCT_NO" +
                ",A.GL_CURR_COD_AUTHOR" +
                ",A.GL_OPUN_COD" +
                ",A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD||GL_THIRD_LEVEL_LG_CD" +
                ",A.GL_BAL_DIRE_FLG" +
                ",A.GL_BUSN_TYP" +
                ",A.GL_ACCT_NAME " +
                "FROM F_DEP_INN_GLACAACA A " +
                "LEFT JOIN TEST_TEST B ON A.GL_ACCT_NO=B.TEST_NO " +
                "WHERE A.GL_ACCT_NO_2 > 0 ";


        // 解析sql
        SqlNode sqlNode = parser.parseQuery(sql);

        SqlInsert sqlInsert = (SqlInsert) sqlNode;
        sqlInsert.getSource();

        // 还原某个方言的SQL
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }

    /**
     * 解析表达式
     * @throws SqlParseException
     */
    public static void parserExp() throws SqlParseException {
        // 解析配置 - mysql设置
        SqlParser.Config mysqlConfig = SqlParser.configBuilder().setLex(Lex.MYSQL).build();
        String exp = "id = 1 and name='1'";
        SqlParser expressionParser = SqlParser.create(exp, mysqlConfig);
        // Sql语句
        // 解析sql
        SqlNode sqlNode = expressionParser.parseExpression();
        // 还原某个方言的SQL
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }







}
