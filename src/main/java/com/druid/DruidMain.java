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
//        String sql="INSERT INTO SESSION.N_F_LN_BUSINESS_PUTOUT_H(\n" +
//                "        SERIALNO\n" +
//                "        ,CONTRACTSERIALNO\n" +
//                "        ,MFCUSTOMERID\n" +
//                "        ,CUSTOMERID\n" +
//                "       \n" +
//                "    \n" +
//                "       )  SELECT\n" +
//                "      A.SERIALNO\n" +
//                "      ,A.CONTRACTSERIALNO\n" +
//                "      ,B.MFCUSTOMERID\n" +
//                "      ,A.CUSTOMERID\n" +
//                "     \n" +
//                "      FROM ODS.O_CMIS_BUSINESS_PUTOUT A LEFT JOIN ODS.O_CMIS_CUSTOMER_INFO B ON A.CUSTOMERID=B.CUSTOMERID\n" +
//                "      WHERE  A.ETL_DT = I_STATEDATE \n" +
//                "      UNION  SELECT\n" +
//                "        VC.SERIALNO_VC\n" +
//                "        ,VC.CONTRACTSERIALNO_VC\n" +
//                "        ,VC.MFCUSTOMERID_VC\n" +
//                "        ,VC.CUSTOMERID_VC\n" +
//                "      \n" +
//                "      FROM  FDS.F_LN_BUSINESS_PUTOUT_H VC\n" +
//                "     ";

        String sql ="INSERT INTO IDS.I_CI_ENT_INFO(\n" +
                "            TJRQ\n" +
                "            ,XDKHH\n" +
                "            ,HXKHH\n" +
                "            ,KHMC\n" +
                "            ,KHYWMC\n" +
                "            ,KHLX\n" +
                "            ,JWJGBS\n" +
                "            ,QYGM\n" +
                "            ,HYFL\n" +
                "            ,SYZDM\n" +
                "            ,KGXS\n" +
                "            ,ZJLX\n" +
                "            ,ZJHM\n" +
                "            ,ZJXXZHGXRQ\n" +
                "            ,DKKH\n" +
                "            ,DKKKHRQ\n" +
                "            ,DKKDQRQ\n" +
                "            ,DJZCDM\n" +
                "            ,ZCRQ\n" +
                "            ,ZCGJDM\n" +
                "            ,ZCGJMC\n" +
                "            ,ZCDZ\n" +
                "            ,XZQHDM\n" +
                "            ,ZCZBBZDM\n" +
                "            ,ZCZBJE\n" +
                "            ,ZCXXZHGXRQ\n" +
                "            ,SSZBBZDM\n" +
                "            ,SSZBJE\n" +
                "            ,SSBS\n" +
                "            ,JTKHBZ\n" +
                "            ,SYJTKHH\n" +
                "            ,JTMC\n" +
                "            ,YHGDBZ\n" +
                "            ,CGBL\n" +
                "            ,YHGLFBS\n" +
                "            ,NYCYHJB\n" +
                "            ,HJHSHFXFL\n" +
                "            ,FXYJXH\n" +
                "            ,GZSJ\n" +
                "            ,WYGL\n" +
                "            ,XYPJJG\n" +
                "            ,GHR\n" +
                "            ,JGDM\n" +
                "            ,GTQYBS\n" +
                "            ,LDMJXQYBS\n" +
                "            ,ZCZE\n" +
                "            ,FZZE\n" +
                "            ,SQLR\n" +
                "            ,ZYYWSR\n" +
                "            ,CH\n" +
                "            ,YSZK\n" +
                "            ,QTYSK\n" +
                "            ,LDZCHJ\n" +
                "            ,LDFZHJ\n" +
                "            ,CWBBLX\n" +
                "            ,CWBBRQ\n" +
                "            ,GNBGDZ\n" +
                "            ,GNBGDZXZQHDM\n" +
                "            ,GNBGDZGXRQ\n" +
                ")\n" +
                "       SELECT\n" +
                "        I_STATEDATE AS TLRQ,\n" +
                "        A.CUSTOMERID AS XDKHH,\n" +
                "        VALUE(C.HXKHH,B.MFCUSTOMERID) AS HXKHH,\n" +
                "          A.ENTERPRISENAME AS KHMC,\n" +
                "          A.ENGLISHNAME AS KHYWMC,\n" +
                "          DSP.FUN_XMDZ ('frkhlx',ORGNATURE,'21') AS KHLX,\n" +
                "          '2' AS JWJGBS,\n" +
                "          DSP.FUN_XMDZ ('qygm',SCOPE,'9') AS QYGM,\n" +
                "          A.INDUSTRYTYPE AS HYFL,\n" +
                "          DSP.FUN_XMDZ ('syzdm',ORGTYPE,'') AS SYZDM,\n" +
                "          DSP.FUN_XMDZ ('kgxs', DSP.FUN_XMDZ ('syzdm', ORGTYPE, ''), '')   AS KGXS,\n" +
                "          '1' AS ZJLX,\n" +
                "          case when A.corpid is not null then A.corpid when A.corpid is null and length(A.licenseno)=18 then substr(A.licenseno,9,9) end as zjhm,\n" +
                "          TO_DATE (CASE WHEN LENGTH (A.SRC_DT) IN (8,10) THEN A.SRC_DT WHEN LENGTH (A.INPUTDATE) IN (8,10) THEN A.INPUTDATE END, 'YYYYMMDD')   AS ZJXXZHGXRQ,\n" +
                "          A.LOANCARDNO AS DKKH,\n" +
                "          '' AS DKKKHRQ,\n" +
                "          '' AS DKKDQRQ,\n" +
                "          A.LICENSENO AS DJZCDM,\n" +
                "          CASE  WHEN LENGTH (A.LICENSECHECK) IN (8, 10)  THEN TO_DATE (A.LICENSECHECK,'YYYYMMDD')  END  AS ZCRQ,\n" +
                "          VALUE(A.COUNTRYCODE,'CHN') AS ZCGJDM,\n" +
                "          '' AS ZCGJMC,\n" +
                "          A.REGISTERADD AS ZCDZ,\n" +
                "          A.REGIONCODE AS XZQHDM,\n" +
                "          A.RCCURRENCY AS ZCZBBZDM,\n" +
                "          A.REGISTERCAPITAL AS ZCZBJE,\n" +
                "          TO_DATE (CASE WHEN length(A.SRC_DT) in (8,10) then A.SRC_DT WHEN LENGTH (A.INPUTDATE) IN (8,10) THEN A.INPUTDATE  END,'YYYYMMDD')  AS ZCXXZHGXRQ,\n" +
                "          A.PCCURRENCY AS SSZBBZDM,\n" +
                "          A.PAICLUPCAPITAL AS SSZBJE,\n" +
                "          CASE  WHEN A.LISTINGCORPORNOT IN ('A','B','F','H') THEN '1' ELSE '2'  END AS SSBS,\n" +
                "          CASE WHEN VALUE (BELONGGROUPID,'') = '' THEN '2' ELSE '1' END AS JTKHBZ,\n" +
                "          B.BELONGGROUPID AS SYJTKHH,\n" +
                "          '' AS JTMC,\n" +
                "         CASE WHEN B.MFCUSTOMERID=D.SH_CUST_NO OR C.HXKHH=D.SH_CUST_NO THEN '1' ELSE '2' END AS YHGDBZ,\n" +
                "         CASE WHEN B.MFCUSTOMERID=D.SH_CUST_NO OR C.HXKHH=D.SH_CUST_NO THEN  D.CGBL ELSE 0 END AS CGBL,\n" +
                "          '' AS YHGLFBS,\n" +
                "          CASE WHEN A.AGRLEVEL IS NOT NULL THEN A.AGRLEVEL END AS NYCYHJB,\n" +
                "          '' AS HJHSHFXFL,\n" +
                "          '' AS FXYJXH,\n" +
                "          '' AS GZSJ,\n" +
                "          '0' AS WYGL,\n" +
                "          A.CREDITLEVEL AS XYPJJG,\n" +
                "          CASE WHEN B.MANAGERUSERID IS NULL OR B.MANAGERORGID IS NULL THEN A.UPDATEUSERID ELSE B.MANAGERUSERID END AS GHR,\n" +
                "          CASE  WHEN B.MANAGERUSERID IS NULL OR B.MANAGERORGID IS NULL THEN A.UPDATEORGID  ELSE B.MANAGERORGID  END AS JGDM,\n" +
                "          '2' AS GTQYBS,\n" +
                "          '2' AS LDMJXQYBS,\n" +
                "          E.ZCZE AS ZCZE,\n" +
                "          E.FZZE AS FZZE,\n" +
                "          E.SQLR AS SQLR,\n" +
                "          E.ZYYWSR AS ZYYWSR,\n" +
                "          E.CH AS CH,\n" +
                "          STRCMP(E.YSZK,E.QTYSK) AS YSZK,\n" +
                "          E.QTYSK AS QTYSK,\n" +
                "          E.LDZCHJ AS LDZCHJ,\n" +
                "          E.LDFZHJ AS LDFZHJ,\n" +
                "          E.CWBBLX AS CWBBLX,\n" +
                "          E.CWBBRQ AS CWBBRQ,\n" +
                "          A.OFFICEADD AS GNBGDZ,\n" +
                "          A.REGIONCODE AS GNBGDZXZQHDM,\n" +
                "          '' AS GNBGDZGXRQ     \n" +
                "        FROM ODS.O_CMIS_ENT_INFO A\n" +
                "        LEFT JOIN ODS.O_CMIS_CUSTOMER_INFO B \n" +
                "        ON A.CUSTOMERID=B.CUSTOMERID \n" +
                "        AND B.CERTTYPE LIKE '200%'\n" +
                "        LEFT JOIN (SELECT CUSTOMERID,MAX(MFCUSTOMERID) AS HXKHH\n" +
                "                          FROM ODS.O_CMIS_BUSINESS_DUEBILL  \n" +
                "                          WHERE MFCUSTOMERID IS NOT NULL \n" +
                "                          GROUP BY CUSTOMERID) C \n" +
                "        ON A.CUSTOMERID=C.CUSTOMERID\n" +
                "        LEFT JOIN (SELECT A.SH_CUST_NO\n" +
                "                         ,CASE WHEN B.ZGJ = 0 THEN 0 ELSE ROUND (A.GJ / B.ZGJ * 100,4) END  AS CGBL         \n" +
                "                          FROM (SELECT SH_CUST_NO,DECIMAL (SUM (B.SH_ACCT_BAL),24,2) AS GJ \n" +
                "                                       FROM ODS.O_CBOD_SHACNACN A,ODS.O_CBOD_SHACNAMT B  \n" +
                "                                       WHERE A.SH_ACCT_NO = B.SH_ACCT_NO \n" +
                "                                       AND A.SH_DDP_ACCT_STS = '01'                \n" +
                "                                       GROUP BY SH_CUST_NO) A,\n" +
                "                               (SELECT DECIMAL (SUM (B.SH_ACCT_BAL),24,2) AS ZGJ                  \n" +
                "                                       FROM ODS.O_CBOD_SHACNACN A,ODS.O_CBOD_SHACNAMT B                 \n" +
                "                                       WHERE A.SH_ACCT_NO = B.SH_ACCT_NO  \n" +
                "                                       AND A.SH_DDP_ACCT_STS = '01') B\n" +
                "                   ) D \n" +
                "         ON B.MFCUSTOMERID=D.SH_CUST_NO OR C.HXKHH=D.SH_CUST_NO\n" +
                "         LEFT JOIN (SELECT CUSTOMERID,\n" +
                "                           VALUE (MAX (ZCZE),0) AS ZCZE,\n" +
                "                           VALUE (SUM (FZZE),0) AS FZZE,\n" +
                "                           VALUE (SUM (SQLR),0) AS SQLR,\n" +
                "                           VALUE (SUM (ZYYWSR),0) AS ZYYWSR,\n" +
                "                           VALUE (SUM (CH),0) AS CH,\n" +
                "                           VALUE (SUM (YSZK),0) AS YSZK,\n" +
                "                           VALUE (SUM (QTYSK),0) AS QTYSK,\n" +
                "                           VALUE (SUM (LDZCHJ),0) AS LDZCHJ,\n" +
                "                           VALUE (SUM (LDFZHJ),0) AS LDFZHJ,\n" +
                "                           CWBBLX,\n" +
                "                           CWBBRQ          \n" +
                "                           FROM (SELECT A.CUSTOMERID,\n" +
                "                                        CASE WHEN C.ROWSUBJECT = '165' THEN COL2VALUE ELSE 0 END AS ZCZE,\n" +
                "                                        CASE WHEN C.ROWSUBJECT = '246' THEN COL2VALUE ELSE 0 END AS FZZE,\n" +
                "                                        CASE WHEN C.ROWSUBJECT = '329' THEN COL2VALUE ELSE 0 END AS SQLR,\n" +
                "                                        CASE WHEN C.ROWSUBJECT = '301' THEN COL2VALUE ELSE 0 END AS ZYYWSR,\n" +
                "                                        CASE WHEN C.ROWSUBJECT = '117' THEN COL2VALUE ELSE 0 END AS CH,\n" +
                "                                        CASE WHEN C.ROWSUBJECT = '107' THEN COL2VALUE ELSE 0 END AS YSZK,\n" +
                "                                        CASE WHEN C.ROWSUBJECT = '115' THEN COL2VALUE ELSE 0 END AS QTYSK,\n" +
                "                                        CASE WHEN C.ROWSUBJECT = '123' THEN COL2VALUE ELSE 0 END AS LDZCHJ,\n" +
                "                                        CASE WHEN C.ROWSUBJECT = '224' THEN COL2VALUE ELSE 0 END AS LDFZHJ,\n" +
                "                                        DSP.FUN_XMDZ ('cwbblx',A.REPORTPERIOD,'9') AS CWBBLX,\n" +
                "                                        LAST_DAY (to_date(REPORTDATE||'01','yyyyMMdd')) AS CWBBRQ                  \n" +
                "                                        FROM (\n" +
                "                                              SELECT CUSTOMERID,\n" +
                "                                                     RECORDNO,\n" +
                "                                                     REPORTPERIOD,\n" +
                "                                                     REPORTDATE    \n" +
                "                                                     FROM (\n" +
                "                                                           SELECT CF.*,\n" +
                "                                                                  RANK () OVER ( PARTITION BY CUSTOMERID ORDER BY REPORTDATE DESC,RECORDNO DESC) AS RANK             \n" +
                "                                                                  FROM ODS.O_CMIS_CUSTOMER_FSRECORD CF           \n" +
                "                                                                  WHERE SUBSTR(I_STATEDATE,1,6) BETWEEN SUBSTR (RECORDNO,4,6) \n" +
                "                                                                  AND (SELECT SUBSTR ( REPORTNO,1,6)  FROM ODS.O_CMIS_REPORT_DATA  ORDER BY REPORTNO DESC FETCH FIRST 1 ROWS ONLY))    \n" +
                "                                                     WHERE RANK = 1)  A  \n" +
                "                                         LEFT  JOIN ODS.O_CMIS_REPORT_RECORD B     \n" +
                "                                         ON A.RECORDNO = B.OBJECTNO  \n" +
                "                                         LEFT  JOIN ODS.O_CMIS_REPORT_DATA C    \n" +
                "                                         ON C.REPORTNO = B.REPORTNO                 \n" +
                "                                         WHERE C.ROWSUBJECT IN ('165','246','329','301','117','107','115','123','224')) TMP        \n" +
                "                          GROUP BY CUSTOMERID,CWBBLX,CWBBRQ) E \n" +
                "          ON A.CUSTOMERID=E.CUSTOMERID;";

        SqlParserDruid.opSqlTargetResourceByDruid(sql, "111");

    }
}
