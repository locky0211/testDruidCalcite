package com.druid;

import com.druid.sql.SqlParserDruid;

public class DruidMain {

    public static void main(String[] args) throws Exception {

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
                "select * from ("+
                "SELECT " +
                "I_STATEDATE STATDATE" +
                ",A.GL_ACCT_NO def" +
                ",F_DEP_INN_GLACAACA.GL_CURR_COD_AUTHOR" +
                ",func(A.GL_OPUN_COD,123)" +
                ",A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD||GL_THIRD_LEVEL_LG_CD fd" +
                ",12312121212121" +
                ",'ave'" +
                ",null " +
                "FROM F_DEP_INN_GLACAACA A " +
                "LEFT JOIN TEST_TEST B ON A.GL_ACCT_NO=B.TEST_NO " +
                "WHERE A.GL_ACCT_NO_2 > 0 )";


        SqlParserDruid.opSqlTargetResourceByDruid(sql, "111");

    }
}
