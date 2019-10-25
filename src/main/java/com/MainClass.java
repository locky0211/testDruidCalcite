package com;

import com.sql.SqlParserCalcite;

public class MainClass {


    public static void main(String[] args) throws Exception {

        // Sql语句
        //  String sql = "select a.id,a.name,b.gender from emps a left join test b on a.id= b.id where a.id in( select id from test_c )";

        //SELECT  * from app_user LEFT JOIN audit_log on user_key = AUTHOR_KEY;

//        String sql = "INSERT INTO FDS.F_LN_ACCT(\n" +
//                "      ACCT_NO\n" +
//                "      ,CUST_FLG\n" +
//                "      ,CUST_NO\n" +
//                "      ,CUST_NAME\n" +
//                "      ,CERT_TYP\n" +
//                "      ,CERT_NO\n" +
//                "      ,CONT_NO\n" +
//                "      ,BILL_NO\n" +
//                "      ,CURR_CODE\n" +
//                "      ,CURR_IDEN\n" +
//                "      ,CRLMT_NO\n" +
//                "      ,PDP_CODE\n" +
//                "      ,LOAN_TYP\n" +
//                "      ,ORG_NO\n" +
//                "      ,LOAN_DUE\n" +
//                "      ,LOAN_PURP\n" +
//                "      ,LOAN_STP\n" +
//                "      ,LOAN_FUND_RSUR\n" +
//                "      ,LOAN_SVR_BUS\n" +
//                "      ,ACCT_STS\n" +
//                "      ,RFN_TYP\n" +
//                "      ,RFN_ACCT_NO\n" +
//                "      ,LOAN_BUS_TYP\n" +
//                "      ,LOAN_SULT_TYP\n" +
//                "      ,GL_ACC_NO\n" +
//                "      ,RFN_USE_DUE\n" +
//                "      ,RFN_LAST_DT\n" +
//                "      ,ACCT_BAL\n" +
//                "      ,ACCT_NOR_BAL\n" +
//                "      ,ACCT_DUE_BAL\n" +
//                "      ,OPEN_DT\n" +
//                "      ,LOAN_ACT_DT\n" +
//                "      ,LOAN_DUE_DT\n" +
//                "      ,RATE_DT\n" +
//                "      ,RATE_TYP\n" +
//                "      ,LOAN_CLSD_TYP\n" +
//                "      ,LOAN_RATE\n" +
//                "      ,CLOSE_DT\n" +
//                "      ,TLR_NO\n" +
//                "      ,COMP_INT_END_DT\n" +
//                "      ,COMP_RATE\n" +
//                "      ,APCL_FLG\n" +
//                "      ,CRNT_YR_INTRBL\n" +
//                "      ,RMRK\n" +
//                "      ,GUAR_TYP\n" +
//                "      ,RFN_DUE\n" +
//                "      ,CRLMT_BAL\n" +
//                "      ,CRLMT_USE_BAL\n" +
//                "      ,LOAN_COG_LVL\n" +
//                "      ,COMP_INT_FLG\n" +
//                "      ,ACCU_COMP_INT\n" +
//                "      ,COMP_INT_AMT\n" +
//                "      ,LOAN_BUY_INT\n" +
//                "      ,ETL_DT\n" +
//                "   )WITH T1 AS (SELECT * FROM (SELECT LNS.LN_ENTR_DT_N a1,LNS.LN_LN_ACCT_NO b1,ROW_NUMBER() OVER(PARTITION BY LN_LN_ACCT_NO ORDER BY LN_ENTR_DT_N DESC) RN FROM ODS.O_CBOD_LNLNSJRN0 LNS) WHERE RN=1)\n" +
//                "      SELECT \n" +
//                //  "      A.ACCT_NO\n" +
//                "      ACCT_NO as ddf\n" +
//
//                "      ,A.CUST_TYP as dfdfd\n" +
//                "      ,A.CUST_NO\n" +
//                "      ,A.CUST_NAME\n" +
//                "      ,A.CERT_TYP\n" +
//                "      ,A.CERT_ID\n" +
//                "      ,E.CONT_NO\n" +
//                "      ,E.BILL_NO\n" +
//                "      ,A.CURR_CODE\n" +
//                "      ,A.CURR_IDEN\n" +
//                "      ,A.CRLMT_NO\n" +
//                "      ,A.PDP_CODE\n" +
//                "      ,A.LN_TYP\n" +
//                "      ,A.BELONG_INSTN_CODE\n" +
//                "      ,A.LN_MTHS\n" +
//                "      ,A.LN_PURP\n" +
//                "      ,A.LN_STY\n" +
//                "      ,A.LN_FUND_RSUR\n" +
//                "      ,D.DIRECTION\n" +
//                "      ,A.ACCT_STS\n" +
//                "      ,A.RFN_STY\n" +
//                "      ,A.DEP_ACCT_NO\n" +
//                "      ,E.BUS_TYP\n" +
//                "      ,E.LASTCLASSIFYRESULT\n" +
//                "      ,B.ACC_NO\n" +
//                "      ,A.ARFN_SCHD_PR\n" +
//                "      ,C.a1\n" +
//                "      ,A.LN_BAL\n" +
//                "      ,A.TOTL_LN_AMT_HYPO_AMT\n" +
//                "      ,A.DLAY_PR_TOTL\n" +
//                "      ,A.FLST_DT\n" +
//                "      ,A.FRST_ALFD_DT\n" +
//                "      ,A.DUE_DT\n" +
//                "      ,A.FRST_ALFD_DT\n" +
//                "      ,A.INTR_TYP\n" +
//                "      ,A.CLSD_INTC_TYP\n" +
//                "      ,A.FRST_INTC_INTR\n" +
//                "      ,A.CLSD_DT\n" +
//                "      ,A.LTST_MNTN_OPR_NO\n" +
//                "      ,A.INT_DIS_END_DT\n" +
//                "      ,A.INT_DIS_RATE\n" +
//                "      ,A.APCL_FLG\n" +
//                "      ,A.CRNT_YR_INTRBL\n" +
//                "      ,A.RMRK\n" +
//                "      ,D.VOUCHTYPE\n" +
//                "      ,E.PAYCYC\n" +
//                "      ,'0.00'\n" +
//                "      ,'0.00'\n" +
//                "      ,''\n" +
//                "      ,''\n" +
//                "      ,'0.00'\n" +
//                "      ,'0.00'\n" +
//                "      ,''\n" +
//                "      ,A.ETL_DT\n" +
//                "      FROM F_LN_LNLNSLNS A LEFT JOIN ODS.O_ODS_F_PRD_PDP_LOAN B ON A.BUS_TYP=B.LOAN_TYP\n" +
//                "      \t\t\t\t\t   LEFT JOIN T1 C ON A.ACCT_NO=C.LN_LN_ACCT_NO\n" +
//                "      \t\t\t\t\t   LEFT JOIN F_LN_BUSINESS_DUEBILL E ON A.ACCT_NO = E.ACCT_NO \n" +
//                "      \t\t\t\t\t   LEFT JOIN  FDS.F_LN_BUSINESS_CONTRACT D ON   E.CONT_NO=D.CONT_NO\n";


//        String sql = "INSERT INTO SESSION.N_F_LN_BUSINESS_PUTOUT_H(\n" +
//                "        SERIALNO\n" +
//                "        ,CONTRACTSERIALNO\n" +
//                "        ,MFCUSTOMERID\n" +
//                "        ,CUSTOMERID\n" +
//                "        ,CUSTOMERNAME\n" +
//                "        ,BUSINESSTYPE\n" +
//                "        ,BUSINESSCURRENCY\n" +
//                "        ,BUSINESSSUM\n" +
//                "      ,TERMMONTH\n" +
//                "      ,TERMDAY\n" +
//                "      ,PUTOUTDATE\n" +
//                "      ,MATURITY\n" +
//                "      ,BUSINESSRATE\n" +
//                "      ,PAYCYC\n" +
//                "      ,SUBJECTNO\n" +
//                "      ,OPERATEORGID\n" +
//                "      ,OPERATEUSERID\n" +
//                "      ,OPERATEDATE\n" +
//                "      ,INPUTORGID\n" +
//                "      ,INPUTUSERID\n" +
//                "      ,INPUTDATE\n" +
//                "      ,PIGEONHOLEDATE\n" +
//                "      ,REMARK\n" +
//                "      ,OCCURDATE\n" +
//                "      ,BASERATETYPE\n" +
//                "      ,BASERATE\n" +
//                "      ,RATEFLOATTYPE\n" +
//                "      ,RATEFLOAT\n" +
//                "      ,ARTIFICIALNO\n" +
//                "      ,LOANACCOUNTNO\n" +
//                "      ,PURPOSE\n" +
//                "      ,ADJUSTRATETYPE\n" +
//                "      ,RATEADJUSTCYC\n" +
//                "      ,RATETYPE\n" +
//                "      ,BAILCURRENCY\n" +
//                "      ,BAILRATIO\n" +
//                "      ,TEMPSAVEFLAG\n" +
//                "      ,PAYTYPE\n" +
//                "      ,BAILRATE\n" +
//                "      ,DEDUCTFLAG\n" +
//                "      ,PUTOUTORGID\n" +
//                "      ,PAYEFFECTDATE\n" +
//                "      ,LCNO\n" +
//                "      ,DIRECTION\n" +
//                "      ,OVERRATEMETHOD\n" +
//                "      ,PAYSOURCE\n" +
//                "      ,OCCURTYPE\n" +
//                "      ,CYCUSE\n" +
//                "      ,CHARGEMONTH\n" +
//                "      ,BAILSUM\n" +
//                "      ,PLUSMINUSNO\n" +
//                "      ,OVERPLUSMINUSNO\n" +
//                "      ,PLUSMINUS\n" +
//                "      ,OVERPLUSMINUS\n" +
//                "      ,LOANAUTHFLAG\n" +
//                "      ,BENEFICIARY\n" +
//                "      ,GUARANTEESORT\n" +
//                "      ,GUARANTEENO\n" +
//                "      ,DISCOUNTINTERESTSUM\n" +
//                "      ,DISCOUNTSUM\n" +
//                "      ,BILLNUM\n" +
//                "      ,FLAG5\n" +
//                "      ,BACURRENCY\n" +
//                "      ,OUGHTRATE\n" +
//                "      ,DISCOUNTRATIO\n" +
//                "      ,DEDATE\n" +
//                "      ,PDGRATIO\n" +
//                "      ,PURPOSEADD\n" +
//                "      ,SUBBILLTYPE\n" +
//                "      ,START_DT\n" +
//                "      ,END_DT\n" +
//                "       )  SELECT\n" +
//                "      A.SERIALNO\n" +
//                "      ,A.CONTRACTSERIALNO\n" +
//                "      ,B.MFCUSTOMERID\n" +
//                "      ,A.CUSTOMERID\n" +
//                "      ,A.CUSTOMERNAME\n" +
//                "      ,A.BUSINESSTYPE\n" +
//                "      ,A.BUSINESSCURRENCY\n" +
//                "      ,A.BUSINESSSUM\n" +
//                "      ,A.TERMMONTH\n" +
//                "      ,A.TERMDAY\n" +
//                "      ,A.PUTOUTDATE\n" +
//                "      ,A.MATURITY\n" +
//                "      ,A.BUSINESSRATE\n" +
//                "      ,A.PAYCYC\n" +
//                "      ,A.SUBJECTNO\n" +
//                "      ,A.OPERATEORGID\n" +
//                "      ,A.OPERATEUSERID\n" +
//                "      ,A.OPERATEDATE\n" +
//                "      ,A.INPUTORGID\n" +
//                "      ,A.INPUTUSERID\n" +
//                "      ,A.INPUTDATE\n" +
//                "      ,A.PIGEONHOLEDATE\n" +
//                "      ,A.REMARK\n" +
//                "      ,A.OCCURDATE\n" +
//                "      ,A.BASERATETYPE\n" +
//                "      ,A.BASERATE\n" +
//                "      ,A.RATEFLOATTYPE\n" +
//                "      ,A.RATEFLOAT\n" +
//                "      ,A.ARTIFICIALNO\n" +
//                "      ,A.LOANACCOUNTNO\n" +
//                "      ,A.PURPOSE\n" +
//                "      ,A.ADJUSTRATETYPE\n" +
//                "      ,A.RATEADJUSTCYC\n" +
//                "      ,A.RATETYPE\n" +
//                "      ,A.BAILCURRENCY\n" +
//                "      ,A.BAILRATIO\n" +
//                "      ,A.TEMPSAVEFLAG\n" +
//                "      ,A.PAYTYPE\n" +
//                "      ,A.BAILRATE\n" +
//                "      ,A.DEDUCTFLAG\n" +
//                "      ,A.PUTOUTORGID\n" +
//                "      ,A.PAYEFFECTDATE\n" +
//                "      ,A.LCNO\n" +
//                "      ,A.DIRECTION\n" +
//                "      ,A.OVERRATEMETHOD\n" +
//                "      ,A.PAYSOURCE\n" +
//                "      ,A.OCCURTYPE\n" +
//                "      ,A.CYCUSE\n" +
//                "      ,A.CHARGEMONTH\n" +
//                "      ,A.BAILSUM\n" +
//                "      ,A.PLUSMINUSNO\n" +
//                "      ,A.OVERPLUSMINUSNO\n" +
//                "      ,A.PLUSMINUS\n" +
//                "      ,A.OVERPLUSMINUS\n" +
//                "      ,A.LOANAUTHFLAG\n" +
//                "      ,A.BENEFICIARY\n" +
//                "      ,A.GUARANTEESORT\n" +
//                "      ,A.GUARANTEENO\n" +
//                "      ,A.DISCOUNTINTERESTSUM\n" +
//                "      ,A.DISCOUNTSUM\n" +
//                "      ,A.BILLNUM\n" +
//                "      ,A.FLAG5\n" +
//                "      ,A.BACURRENCY\n" +
//                "      ,A.OUGHTRATE\n" +
//                "      ,A.DISCOUNTRATIO\n" +
//                "      ,A.DEDATE\n" +
//                "      ,A.PDGRATIO\n" +
//                "      ,A.PURPOSEADD\n" +
//                "      ,A.SUBBILLTYPE\n" +
//                "      ,V_START_DT\n" +
//                "      ,V_END_DT\n" +
//                "      FROM ODS.O_CMIS_BUSINESS_PUTOUT A LEFT JOIN ODS.O_CMIS_CUSTOMER_INFO B ON A.CUSTOMERID=B.CUSTOMERID\n" +
//                "      WHERE  A.ETL_DT = I_STATEDATE \n" +
//                "      MINUS  SELECT\n" +
//                "        SERIALNO\n" +
//                "        ,CONTRACTSERIALNO\n" +
//                "        ,MFCUSTOMERID\n" +
//                "        ,CUSTOMERID\n" +
//                "        ,CUSTOMERNAME\n" +
//                "        ,BUSINESSTYPE\n" +
//                "        ,BUSINESSCURRENCY\n" +
//                "        ,BUSINESSSUM\n" +
//                "      ,TERMMONTH\n" +
//                "      ,TERMDAY\n" +
//                "      ,PUTOUTDATE\n" +
//                "      ,MATURITY\n" +
//                "      ,BUSINESSRATE\n" +
//                "      ,PAYCYC\n" +
//                "      ,SUBJECTNO\n" +
//                "      ,OPERATEORGID\n" +
//                "      ,OPERATEUSERID\n" +
//                "      ,OPERATEDATE\n" +
//                "      ,INPUTORGID\n" +
//                "      ,INPUTUSERID\n" +
//                "      ,INPUTDATE\n" +
//                "      ,PIGEONHOLEDATE\n" +
//                "      ,REMARK\n" +
//                "      ,OCCURDATE\n" +
//                "      ,BASERATETYPE\n" +
//                "      ,BASERATE\n" +
//                "      ,RATEFLOATTYPE\n" +
//                "      ,RATEFLOAT\n" +
//                "      ,ARTIFICIALNO\n" +
//                "      ,LOANACCOUNTNO\n" +
//                "      ,PURPOSE\n" +
//                "      ,ADJUSTRATETYPE\n" +
//                "      ,RATEADJUSTCYC\n" +
//                "      ,RATETYPE\n" +
//                "      ,BAILCURRENCY\n" +
//                "      ,BAILRATIO\n" +
//                "      ,TEMPSAVEFLAG\n" +
//                "      ,PAYTYPE\n" +
//                "      ,BAILRATE\n" +
//                "      ,DEDUCTFLAG\n" +
//                "      ,PUTOUTORGID\n" +
//                "      ,PAYEFFECTDATE\n" +
//                "      ,LCNO\n" +
//                "      ,DIRECTION\n" +
//                "      ,OVERRATEMETHOD\n" +
//                "      ,PAYSOURCE\n" +
//                "      ,OCCURTYPE\n" +
//                "      ,CYCUSE\n" +
//                "      ,CHARGEMONTH\n" +
//                "      ,BAILSUM\n" +
//                "      ,PLUSMINUSNO\n" +
//                "      ,OVERPLUSMINUSNO\n" +
//                "      ,PLUSMINUS\n" +
//                "      ,OVERPLUSMINUS\n" +
//                "      ,LOANAUTHFLAG\n" +
//                "      ,BENEFICIARY\n" +
//                "      ,GUARANTEESORT\n" +
//                "      ,GUARANTEENO\n" +
//                "      ,DISCOUNTINTERESTSUM\n" +
//                "      ,DISCOUNTSUM\n" +
//                "      ,BILLNUM\n" +
//                "      ,FLAG5\n" +
//                "      ,BACURRENCY\n" +
//                "      ,OUGHTRATE\n" +
//                "      ,DISCOUNTRATIO\n" +
//                "      ,DEDATE\n" +
//                "      ,PDGRATIO\n" +
//                "      ,PURPOSEADD\n" +
//                "      ,SUBBILLTYPE\n" +
//                "      ,V_START_DT\n" +
//                "      ,V_END_DT\n" +
//                "      FROM  FDS.F_LN_BUSINESS_PUTOUT_H \n" +
//                "      WHERE   TO_DATE(I_STATEDATE,'YYYY-MM-DD') BETWEEN START_DT AND END_DT ";


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
//                "SELECT " +
//                "I_STATEDATE STATDATE" +
//                ",A.GL_ACCT_NO" +
//                ",A.GL_CURR_COD_AUTHOR" +
//                ",A.GL_OPUN_COD" +
//                ",A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD||GL_THIRD_LEVEL_LG_CD" +
//                ",A.GL_BAL_DIRE_FLG" +
//                ",A.GL_BUSN_TYP" +
//                ",A.GL_ACCT_NAME " +
//                "FROM F_DEP_INN_GLACAACA A " +
//                "LEFT JOIN TEST_TEST B ON A.GL_ACCT_NO=B.TEST_NO " +
//                "WHERE A.GL_ACCT_NO_2 > 0 ";

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
//                "SELECT " +
//                "I_STATEDATE STATDATE" +
//                ",A.GL_ACCT_NO" +
//                ",A.GL_CURR_COD_AUTHOR" +
//                ",A.GL_OPUN_COD" +
//                ",A.GL_FIRST_LEVEL_LG_COD||GL_SECOND_LEVEL_LG_CD||GL_THIRD_LEVEL_LG_CD" +
//                ",A.GL_BAL_DIRE_FLG" +
//                ",A.GL_BUSN_TYP" +
//                ",A.GL_ACCT_NAME " +
//                "FROM F_DEP_INN_GLACAACA A ";





        String sql = "INSERT INTO IDS.I_LN_BUSINESS_INFO \n" +
                "       SELECT\n" +
                "           I_STATEDATE AS TJRQ\n" +
                "           ,B.SERIALNO AS DKZH\n" +
                "           ,'' AS JGDM\n" +
                "           ,B.BUSINESSCURRENCY AS BZDM\n" +
                "           ,'' AS KMH\n" +
                "           ,'' AS NBKMH\n" +
                "           ,'' AS HXKHH\n" +
                "           ,B.CUSTOMERID AS XDKHH\n" +
                "           ,B.CUSTOMERNAME AS KHM\n" +
                "           ,B.BUSINESSSUM AS DKJE  --授信额度\n" +
                "           ,C.LN_LN_BAL AS DKYE\n" +
                "           ,0 AS DKJZ\n" +
                "           ,0 AS LXTZJE\n" +
                "           ,'' AS ZHZT\n" +
                "           ,'' AS FFLX\n" +
                "           ,CASE\n" +
                "          when B.OCCURDATE like '%0229'\n" +
                "           then last_day(substr(B.OCCURDATE,1,4) || '-02-01')\n" +
                "           WHEN LENGTH (B.OCCURDATE) IN (8, 10)\n" +
                "          THEN\n" +
                "             TO_DATE (B.OCCURDATE, 'YYYYMMDD')\n" +
                "       END AS KHRQ\n" +
                "           ,CASE\n" +
                "          when B.MATURITY like '%0229'\n" +
                "           then last_day(substr(B.MATURITY,1,4) || '-02-01')\n" +
                "           WHEN LENGTH (B.MATURITY) IN (8, 10)\n" +
                "          THEN\n" +
                "             TO_DATE (B.MATURITY, 'YYYYMMDD')\n" +
                "       END AS DQRQ\n" +
                "           ,NULL AS ZQRQ\n" +
                "           ,NULL AS YSDQRQ\n" +
                "           ,'' AS QXDM\n" +
                "           ,NULL AS XYCLLBDRQ\n" +
                "           ,'' AS LLBDDW\n" +
                "           ,0 AS JZLL\n" +
                "           ,0 AS ZXLL\n" +
                "           ,'' AS ZHXZ\n" +
                "           ,'' AS DKZL\n" +
                "           ,B.CREDITCYCLE  AS DKLX   --授信额度循环标志\n" +
                "           ,NVL(B.BUSINESSTYPE,C.LN_LN_BAL,9999) AS DKYT   --授信分类\n" +
                "           ,'' AS KHLX\n" +
                "           ,'' AS QYGM\n" +
                "           ,'' AS KGXS\n" +
                "           ,'' AS HYFL\n" +
                "           ,'' AS DKTX\n" +
                "           ,'' AS WJFL\n" +
                "           ,'' AS SJFL4\n" +
                "           ,'' AS SJFL10\n" +
                "           ,'' AS DBFS\n" +
                "           ,'' AS SXH\n" +
                "           ,'' AS HTH\n" +
                "           ,'' AS JJH\n" +
                "           ,0 AS JZZBJE\n" +
                "           ,'' AS DKKH\n" +
                "           ,'' AS CKZH\n" +
                "           ,'' AS DKZLGS\n" +
                "           ,'' AS GRDKYT\n" +
                "           ,'' AS XGSYDKFL\n" +
                "           ,'' AS GJJDKBS\n" +
                "           ,'' AS SNBS\n" +
                "           ,'' AS SNZZLX\n" +
                "           ,'' AS ZNDKYT\n" +
                "           ,'' AS YTDKBS\n" +
                "           ,'' AS ZFFS\n" +
                "           ,'' AS HBFS\n" +
                "           ,'' AS HXFS\n" +
                "           ,'' AS CZTXBS\n" +
                "           ,'' AS TSBZ\n" +
                "           ,0 AS QBTS    --欠本天数\n" +
                "           ,0 AS QXTS    --欠息天数\n" +
                "           ,0 AS QBYE    --欠本余额\n" +
                "           ,0 AS BNQXYE  --表内欠息余额\n" +
                "           ,0 AS BWQXYE  --表外欠息余额\n" +
                "           ,0 AS BYHBJE\n" +
                "           ,0 AS BYHXJE\n" +
                "           ,NULL AS ZJYCHBRQ\n" +
                "           ,0 AS ZJYCHBJE\n" +
                "           ,NULL AS ZJYCHXRQ\n" +
                "           ,0 AS ZJYHXJE\n" +
                "           ,NULL AS XQHBRQ\n" +
                "           ,0 AS XQHBJE\n" +
                "           ,NULL AS XQHXRQ\n" +
                "           ,0 AS XQHXJE\n" +
                "           ,null AS SCHBRQ\n" +
                "           ,null AS SCHXRQ\n" +
                "           ,'' AS CYJGTZLX\n" +
                "           ,'' AS GYZXSJBS\n" +
                "           ,'' AS ZLXXCYLX\n" +
                "           ,B.MANAGEUSERID AS YWGHR\n" +
                "           ,B.MANAGEORGID AS YWGHJGDM\n" +
                "           ,NULL AS ZXDKLX\n" +
                "           ,NULL AS XXMC\n" +
                "           ,NULL AS XXDZ\n" +
                "           ,NULL AS XXXZQHDM\n" +
                "           ,NULL AS XSZH\n" +
                "           ,NULL AS DKSJTZZ\n" +
                "           ,NULL AS DKSJTZZXZQHDM\n" +
                "           ,NULL AS FDCDKLX\n" +
                "           ,0 AS ZFJZMJ\n" +
                "           ,0 AS ZFTS\n" +
                "           ,0 AS DKJZB\n" +
                "           ,0 AS CZSRB\n" +
                "           ,'' AS ZFRZPTBS\n" +
                "           ,NULL AS ZFRZFLXZ\n" +
                "           ,NULL AS ZFRZLSGX\n" +
                "           ,NULL AS ZFRZLX\n" +
                "           ,NULL AS ZFRZZJLY\n" +
                "           ,NULL AS ZFRZPTTX\n" +
                "       FROM FDS.F_LN_BUSINESS_CONTRACT_H B \n" +
                "       LEFT JOIN  (SELECT D.SERIALNO AS SERIALNO,sum(E.LN_LN_BAL) AS LN_LN_BAL FROM FDS.F_LN_BUSINESS_DUEBILL_H A\n" +
                "            INNER JOIN FDS.F_LN_BUSINESS_CONTRACT_H B ON A.RELATIVESERIALNO2 = B.SERIALNO AND B.FLAG5 IN ('1000','3000') AND B.END_DT='9999-12-31'\n" +
                "            INNER JOIN FDS.F_LN_BUSINESS_CONTRACT_H D ON D.RELATIVESERIALNO = B.CREDITAGGREEMENT AND D.FLAG5 IN ('1000','3000') AND D.END_DT='9999-12-31'\n" +
                "            INNER JOIN FDS.F_LN_LNLNSLNS_H E ON E.LN_LN_ACCT_NO = A.SERIALNO AND E.END_DT='9999-12-31'\n" +
                "                WHERE (SUBSTR (A.SUBJECTNO,1,4) IN ('1301','1302','1303','1304','1305','1307','1308')) AND A.END_DT='9999-12-31'\n" +
                "            GROUP BY  D.SERIALNO) C\n" +
                "       ON B.SERIALNO=C.SERIALNO    \n" +
                "       WHERE B.BUSINESSTYPE LIKE '3%' AND B.FLAG5 IN ('1000', '3000') AND B.END_DT='9999-12-31'";

        SqlParserCalcite.opTargetTableInfoByDruid(sql, "111");

    }
}
