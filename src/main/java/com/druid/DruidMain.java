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
                ",CASE  WHEN LENGTH (A.PUTOUTDATE) IN (8,10) THEN TO_DATE (A.PUTOUTDATE,'YYYYMMDD')  WHEN LENGTH (A.PUTOUTDATE) IN (11,12) THEN SELECT \n" +
                "                             CM_INTR1 FROM  ods.o_CBOD_CMIRTIRT WHERE  CM_INTR_TYP ='B5'\n" +
                "                         AND CM_IRT_STS = '0'  END AS KHRQ " +
                ",'ave'" +
                ",null " +
                "FROM DC.F_DEP_INN_GLACAACA A " +
                "LEFT JOIN TEST_TEST B ON A.GL_ACCT_NO=B.TEST_NO " +
                "LEFT JOIN  DF.TEST_2 C ON A.GL_ACCT_NO=C.TEST_NO " +
                "WHERE A.GL_ACCT_NO_2 > 0 )";


        SqlParserDruid.opSqlTargetResourceByDruid(sql, "111");

    }
}
