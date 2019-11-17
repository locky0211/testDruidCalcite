package com.druid;

import com.druid.sql.SqlParserDruid;

public class DruidMain {

    public static void main(String[] args) throws Exception {

//        String sql = "INSERT INTO S_GL_INN_ACCOUNT_D" +
//                "(" +
//                "STATDATE" +
//                ",ACCTNBR" +
//                ",CCYNBR" +
//                ",BRNNBR" +
//                ",ITEMCODE" +
//                ",ITEMDR" +
//                ",BUSNTYP" +
//                ",ACCTNM" +
//                ")" +
//                "select * from ("+
//                "SELECT " +
//                "I_STATEDATE STATDATE" +
//                ",A.GL_ACCT_NO def" +
//                ",F_DEP_INN_GLACAACA.GL_CURR_COD_AUTHOR" +
//                ",func(A.GL_OPUN_COD,123)" +
//                ",A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD||GL_THIRD_LEVEL_LG_CD fd" +
//                ",CASE  WHEN LENGTH (A.PUTOUTDATE) IN (8,10) THEN TO_DATE (A.PUTOUTDATE,'YYYYMMDD')  WHEN LENGTH (A.PUTOUTDATE) IN (11,12) THEN SELECT \n" +
//                "                             OO.CM_INTR1 FROM  ods.O_CBOD_CMIRTIRT OO WHERE  CM_INTR_TYP ='B5'\n" +
//                "                         AND CM_IRT_STS = '0'  END AS KHRQ " +
//                ",'ave'" +
//                ",null " +
//                "FROM DC.F_DEP_INN_GLACAACA A " +
//                "LEFT JOIN TEST_TEST B ON A.GL_ACCT_NO=B.TEST_NO " +
//                "LEFT JOIN  DF.TEST_2 C ON A.GL_ACCT_NO=C.TEST_NO " +
//                "WHERE A.GL_ACCT_NO_2 > 0 )";
//

//        String sql ="INSERT INTO IDS.I_LN_BUSINESS_INFO \n" +
//                "       SELECT\n" +
//
//                "           ,CASE WHEN A.BUSINESSTYPE IN('2060000005701','2060000005542','2060000005625','2060000005711') THEN 'WD' WHEN SUBSTR(A.CUSTOMERID,1,1)='1' THEN 'GR'WHEN SUBSTR(A.CUSTOMERID,1,1)='2' THEN 'GS' ELSE NULL END AS DKLX\n" +
//                "           ,NVL(A.BUSINESSTYPE,'9999') AS DKYT\n" +
//                "           ,F.ORGNATURE AS KHLX\n" +
//                "           ,DSP.FUN_XMDZ ('qygm',F.SCOPE,'9') AS QYGM\n" +
//                "           ,'' AS KGXS\n" +
//                "           ,F.INDUSTRYTYPE AS HYFL\n" +
//                "           ,F.INDUSTRYTYPE AS DKTX\n" +
//                "           ,CASE WHEN A.SUBJECTNO LIKE '1306%' THEN '1' ELSE DSP.FUN_XMDZ ('wjfl',SUBSTR (VALUE (C.FINALLYRESULT,B.APPLICATIONSORT),4,1),'0') END AS WJFL\n" +
//                "           ,CASE WHEN VALUE (A.BADBALANCE,0) <> 0 THEN '3' WHEN VALUE (A.DULLBALANCE,0) <> 0 THEN '2' WHEN VALUE (A.OVERDUEBALANCE,0) <> 0 THEN '1' ELSE '0' END AS SJFL4\n" +
//                "           ,CASE WHEN A.SUBJECTNO LIKE '1306%' THEN '11' ELSE DSP.FUN_XMDZ ('sjfl10',VALUE (C.FINALLYRESULT,B.APPLICATIONSORT),'00') END AS SJFL10\n" +
//                "           ,NVL(A.VOUCHTYPE,'') AS DBFS\n" +
//                "           ,D.SERIALNO AS SXH\n" +
//                "           ,RELATIVESERIALNO2 AS HTH\n" +
//                "           ,RELATIVESERIALNO1 AS JJH\n" +
//                "           ,0 AS JZZBJE\n" +
//                "           ,'' AS DKKH\n" +
//                "           ,E.LN_DEP_ACCT_NO AS CKZH\n" +
//                "           ,'' AS DKZLGS\n" +
//                "           ,'' AS GRDKYT\n" +
//                "           ,'' AS XGSYDKFL\n" +
//                "           ,CASE WHEN E.LN_LN_PURP = 'A08' THEN '1'  ELSE '2' END AS GJJDKBS\n" +
//                "           ,CASE WHEN SUBSTR (A.SUBJECTNO,1,4) IN ('1301','1302','1303') THEN '1' ELSE '2' END AS SNBS\n" +
//                "           ,CASE WHEN SUBSTR (A.SUBJECTNO,1,4) IN ('1303') THEN '3' WHEN SUBSTR (A.SUBJECTNO,1,4) IN ('1302') THEN '4' ELSE '' END AS SNZZLX\n" +
//                "           ,'' AS ZNDKYT\n" +
//                "           ,'' AS YTDKBS\n" +
//                "           ,CASE WHEN A.SUBJECTNO LIKE '1306%' THEN '2' ELSE DSP.FUN_XMDZ ('zffs',B.PAYTYPE,'1') END AS ZFFS\n" +
//                "           ,CASE WHEN E.LN_RFN_STY IN ('101','301','204') THEN '5' WHEN E.LN_RFN_STY IN ('201','202') THEN '1' ELSE '7' END  AS HBFS\n" +
//                "           ,CASE WHEN E.LN_RFN_STY = '01' THEN '6' WHEN E.LN_RFN_STY = '02' THEN '5' WHEN E.LN_COLI_CYCL_TOTL_MN_N BETWEEN 1 AND 2 THEN '1'  WHEN E.LN_COLI_CYCL_TOTL_MN_N BETWEEN 3 AND 5 THEN '2'\n" +
//                "                WHEN E.LN_COLI_CYCL_TOTL_MN_N BETWEEN 6 AND 11 THEN '3' WHEN E.LN_COLI_CYCL_TOTL_MN_N = 12 THEN '4' ELSE '6' END AS HXFS\n" +
//                "           ,DSP.FUN_XMDZ ('cztxbs',B.ISFINDIS,'3') AS CZTXBS\n" +
//                "           ,'' AS TSBZ\n" +
//                "           ,CASE WHEN DAYS (TO_DATE (I_STATEDATE, 'YYYYMMDD')) - DAYS (TO_DATE(A.MATURITY,'YYYY-MM-DD'))+1 > H.QBTS\n" +
//                "                      THEN DAYS (TO_DATE (I_STATEDATE, 'YYYYMMDD')) - DAYS (TO_DATE(A.MATURITY,'YYYY-MM-DD'))+1\n" +
//                "                    ELSE H.QBTS\n" +
//                "                END AS QBTS    --欠本天数\n" +
//                "           ,NVL(G.QXTS,0) AS QXTS    --欠息天数\n" +
//                "           ,CASE WHEN H.QBYE> (CASE WHEN E.LN_LN_BAL <> 0 AND TO_DATE(A.MATURITY,'YYYY-MM-DD') < TO_DATE (I_STATEDATE, 'YYYYMMDD') THEN E.LN_LN_BAL ELSE 0 end) \n" +
//                "            THEN (CASE WHEN E.LN_LN_BAL <> 0 AND TO_DATE(A.MATURITY,'YYYY-MM-DD') < TO_DATE (I_STATEDATE, 'YYYYMMDD') THEN E.LN_LN_BAL ELSE 0 END ) \n" +
//                "            ELSE H.QBYE END\n" +
//                "            AS QBYE    --欠本余额\n" +
//                "           ,NVL(G.BNQXYE,0) AS BNQXYE  --表内欠息余额\n" +
//                "           ,NVL(G.BWQXYE,0) AS BWQXYE  --表外欠息余额\n" +
//                "           ,0 AS BYHBJE\n" +
//                "           ,0 AS BYHXJE\n" +
//                "           ,NULL AS ZJYCHBRQ\n" +
//                "           ,0 AS ZJYCHBJE\n" +
//                "           ,NULL AS ZJYCHXRQ\n" +
//                "           ,0 AS ZJYHXJE\n" +
//                "           ,NULL AS XQHBRQ\n" +
//                "           ,0 AS XQHBJE\n" +
//                "           ,NULL AS XQHXRQ\n" +
//                "           ,0 AS XQHXJE\n" +
//                "           ,TO_DATE(CASE WHEN E.LN_FSTM_RFN_DT_N BETWEEN 10000101 AND 99991231 THEN E.LN_FSTM_RFN_DT_N ELSE '10000101' END,'YYYYMMDD') AS SCHBRQ\n" +
//                "           ,TO_DATE(CASE WHEN E.LN_FSTM_INTP_DT_N BETWEEN 10000101 AND 99991231 THEN E.LN_FSTM_INTP_DT_N ELSE '10000101' END,'YYYYMMDD') AS SCHXRQ\n" +
//                "           ,'' AS CYJGTZLX\n" +
//                "           ,'' AS GYZXSJBS\n" +
//                "           ,'' AS ZLXXCYLX\n" +
//                "           ,A.MANAGEUSERID AS YWGHR\n" +
//                "           ,A.MANAGEORGID AS YWGHJGDM\n" +
//                "           ,NULL AS ZXDKLX\n" +
//                "           ,NULL AS XXMC\n" +
//                "           ,NULL AS XXDZ\n" +
//                "           ,NULL AS XXXZQHDM\n" +
//                "           ,NULL AS XSZH\n" +
//                "           ,NULL AS DKSJTZZ\n" +
//                "           ,NULL AS DKSJTZZXZQHDM\n" +
//                "           ,NULL AS FDCDKLX\n" +
//                "           ,0 AS ZFJZMJ\n" +
//                "           ,0 AS ZFTS\n" +
//                "           ,0 AS DKJZB\n" +
//                "           ,0 AS CZSRB\n" +
//                "           ,DSP.FUN_XMDZ ('zfrzptbs',B.ISONEQ,'2') AS ZFRZPTBS\n" +
//                "           ,NULL AS ZFRZFLXZ\n" +
//                "           ,NULL AS ZFRZLSGX\n" +
//                "           ,NULL AS ZFRZLX\n" +
//                "           ,NULL AS ZFRZZJLY\n" +
//                "           ,NULL AS ZFRZPTTX\n" +
//                "       FROM FDS.F_LN_BUSINESS_DUEBILL_H A\n" +
//                "        LEFT JOIN FDS.F_LN_BUSINESS_CONTRACT_H B ON A.RELATIVESERIALNO2 = B.SERIALNO AND B.FLAG5 IN ('1000','3000') AND B.END_DT='9999-12-31'\n" +
//                "            LEFT JOIN FDS.F_LN_CLASSIFY_RESULT_H C ON A.RELATIVESERIALNO2 = C.OBJECTNO AND C.END_DT='9999-12-31' \n" +
//                "            LEFT JOIN FDS.F_LN_BUSINESS_CONTRACT_H D ON D.RELATIVESERIALNO = B.CREDITAGGREEMENT  AND D.FLAG5 IN ('1000','3000') AND D.END_DT='9999-12-31'\n" +
//                "            LEFT JOIN FDS.F_LN_LNLNSLNS_H E ON E.LN_LN_ACCT_NO = A.SERIALNO AND E.END_DT='9999-12-31'\n" +
//                "            LEFT JOIN ODS.O_CMIS_ENT_INFO F ON A.CUSTOMERID=F.CUSTOMERID\n" +
//                "            LEFT JOIN ( SELECT FK_LNLNS_KEY AS DKZH,\n" +
//                "              DAYS (TO_DATE (I_STATEDATE, 'YYYYMMDD')) -\n" +
//                "                 DAYS ( TO_DATE ( CASE\n" +
//                "                                    WHEN (SELECT MAX (LN_INTC_DAYS)\n" +
//                "                                            FROM ODS.O_CBOD_LNLNSUPY\n" +
//                "                                           WHERE FK_LNLNS_KEY = LN.FK_LNLNS_KEY\n" +
//                "                                           AND LN_INTC_CUTDT_N = MIN (LN.LN_INTC_CUTDT_N)) = 0\n" +
//                "                                    THEN (SELECT MAX (LN_INTC_CUTDT_N)\n" +
//                "                                            FROM ODS.O_CBOD_LNLNSUPY\n" +
//                "                                           WHERE LN_INTC_CUTDT_N < MIN (LN.LN_INTC_CUTDT_N)\n" +
//                "                                             AND LN_INTRBL = LN_ARFN_INT\n" +
//                "                                             AND FK_LNLNS_KEY = LN.FK_LNLNS_KEY)\n" +
//                "                                    ELSE MIN (LN.LN_INTC_CUTDT_N)\n" +
//                "                                  END, 'YYYYMMDD')) AS QXTS,\n" +
//                "              SUM ( CASE WHEN LN_INT_TYP IN ('2', '4')\n" +
//                "                           THEN LN_INTRBL - LN_ARFN_INT\n" +
//                "                         ELSE 0\n" +
//                "                     END) AS BNQXYE,\n" +
//                "              SUM ( CASE WHEN LN_INT_TYP IN ('2', '4')  --转贴现和系统外转贴现\n" +
//                "                           THEN 0\n" +
//                "                         ELSE LN_INTRBL - LN_ARFN_INT\n" +
//                "                     END) AS BWQXYE\n" +
//                "         FROM ODS.O_CBOD_LNLNSUPY LN\n" +
//                "        WHERE LN_INTRBL > LN_ARFN_INT \n" +
//                "          AND LN_INTC_CUTDT_N BETWEEN 10000101 AND I_STATEDATE\n" +
//                "        GROUP BY FK_LNLNS_KEY\n" +
//                "        ) G\n" +
//                "        ON A.SERIALNO=G.DKZH\n" +
//                "        LEFT JOIN (SELECT FK_LNLNS_KEY AS DKZH,\n" +
//                "              DAYS (TO_DATE (I_STATEDATE, 'YYYYMMDD')) - DAYS (TO_DATE (MIN (LN_PPRD_RFN_DAY_N), 'YYYYMMDD')) AS QBTS,\n" +
//                "              SUM (LN_CRNT_PRD_PR - LN_ARFN_PR) AS QBYE\n" +
//                "         FROM FDS.F_LN_LNLNSDUE_H\n" +
//                "        WHERE LN_PPRD_RFN_DAY_N BETWEEN '10000101'  AND I_STATEDATE\n" +
//                "        AND LN_CRNT_PRD_PR > LN_ARFN_PR AND END_DT='9999-12-31'\n" +
//                "        GROUP BY FK_LNLNS_KEY) H\n" +
//                "        ON A.SERIALNO=H.DKZH\n" +
//                "       WHERE (SUBSTR (A.SUBJECTNO,1,4) IN ('1301','1302','1303','1304','1305','1307','1308')) \n" +
//                "             AND A.END_DT = '9999-12-31'";


        String sql="INSERT INTO SESSION.N_F_LN_BUSINESS_PUTOUT_H(\n" +
                "        SERIALNO\n" +
                "        ,CONTRACTSERIALNO\n" +
                "        ,MFCUSTOMERID\n" +
                "        ,CUSTOMERID\n" +
                "       \n" +
                "    \n" +
                "       )  SELECT\n" +
                "      A.SERIALNO\n" +
                "      ,A.CONTRACTSERIALNO\n" +
                "      ,B.MFCUSTOMERID\n" +
                "      ,A.CUSTOMERID\n" +
                "     \n" +
                "      FROM ODS.O_CMIS_BUSINESS_PUTOUT A LEFT JOIN ODS.O_CMIS_CUSTOMER_INFO B ON A.CUSTOMERID=B.CUSTOMERID\n" +
                "      WHERE  A.ETL_DT = I_STATEDATE \n" +
                "      UNION  SELECT\n" +
                "        VC.SERIALNO_VC\n" +
                "        ,VC.CONTRACTSERIALNO_VC\n" +
                "        ,VC.MFCUSTOMERID_VC\n" +
                "        ,VC.CUSTOMERID_VC\n" +
                "      \n" +
                "      FROM  FDS.F_LN_BUSINESS_PUTOUT_H VC\n" +
                "     ";

        SqlParserDruid.opSqlTargetResourceByDruid(sql, "111");

    }
}
