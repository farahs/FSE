CREATE OR REPLACE PACKAGE             "IMP_STAGINGTABLE_XCSD"
IS
PROCEDURE RUN_IMPSTG_XCSD;
PROCEDURE IMP_STG_CASECURITIES;
PROCEDURE IMP_STG_INVESTORS;
PROCEDURE IMP_STG_INVESTORSEXT;
PROCEDURE IMP_STG_ACCOUNTS;
PROCEDURE IMP_STG_MEM_CONTACTPERSON;
PROCEDURE IMP_STG_MEM_ADDRESS;
PROCEDURE IMP_STG_MEM_CONTACTDETAILS;
PROCEDURE IMP_STG_MEM_PARTSUSPENSION;
PROCEDURE IMP_STG_MEMBERS;
PROCEDURE IMP_STG_BASIC_SECURITIES;
PROCEDURE IMP_STG_CURRENCIES;
PROCEDURE IMP_STG_SECURITIES;
PROCEDURE IMP_STG_WARRANTS;
PROCEDURE IMP_STG_CAMEMBERS;
PROCEDURE IMP_STG_APPROVALREQUEST;
PROCEDURE IMP_STG_ACCOUNT_INSTRUCTIONS;
PROCEDURE IMP_STG_INVS_SOURCEFUND;
PROCEDURE IMP_STG_INVS_CONTACTDETAILS;
PROCEDURE IMP_STG_ISSUER_CONTACTDETAIL;
PROCEDURE IMP_STG_PARTI_CONTACTDETAIL;
PROCEDURE IMP_STG_INVS_OBJCTV;
PROCEDURE IMP_STG_INVS_ADDRESS;
PROCEDURE IMP_STG_INVS_DIRECTSID;
PROCEDURE IMP_STG_INVS_DOCS;
PROCEDURE IMP_STG_INVS_IDENTIFIER;
PROCEDURE IMP_STG_INVS_INDCLASS1;
PROCEDURE IMP_STG_INVS_TAXCLASS;
PROCEDURE IMP_STG_INVS_MARITALSTA;
PROCEDURE IMP_STG_INVS_EDUCATION;
PROCEDURE IMP_STG_INVS_OCCUPATION;
PROCEDURE IMP_STG_INVS_INCOME;
PROCEDURE IMP_STG_INVS_INDCLASS2;
PROCEDURE IMP_STG_INVS_ASSET;
PROCEDURE IMP_STG_INVS_OPERATINGPROFIT;
PROCEDURE IMP_STG_INVS_INVCATEGORY;
PROCEDURE IMP_STG_POS_MOVMNT;
PROCEDURE IMP_STG_BALANCES;
PROCEDURE IMP_STG_SI_INSTRUCTIONTYPE;
PROCEDURE IMP_STG_SETTLEMENTINSTRUCTION;
PROCEDURE IMP_STG_SETTINSTRDETAILS;
PROCEDURE IMP_STG_CASHINSTRUCTION;
PROCEDURE IMP_STG_RESTRICTIONORDER;
PROCEDURE IMP_STG_SETTINSTRSTATUSLOG;
PROCEDURE IMP_STG_OTCBONDS;
PROCEDURE IMP_STG_PAIREDSYNCINSTRUCTIONS;
PROCEDURE IMP_STG_INSTRUCTIONS;
PROCEDURE IMP_STG_EXECFEE;
PROCEDURE IMP_STG_FEE;
PROCEDURE IMP_STG_FEEDEFINITION;
PROCEDURE IMP_STG_FEEBASIS;

END IMP_STAGINGTABLE_XCSD;
/


CREATE OR REPLACE PACKAGE BODY             "IMP_STAGINGTABLE_XCSD"
--staging table untuk transfer kebutuhan data ke DWH (REPORTINTRA)
is
  v_sql   VARCHAR2(32767);
  ERR_MSG VARCHAR2(100);
  ERR_CODE NUMBER;
  v_procdesc VARCHAR(100);
  tglDesc VARCHAR(50);
  endtglDesc VARCHAR(50);
  V_SCHEMAPROD VARCHAR2(20):='TOBA';

PROCEDURE RUN_IMPSTG_XCSD
IS
          jml number;
          CURSOR crsLog IS
                   SELECT * FROM ARCHIVE_DTC WHERE TO_CHAR(tgl,'YYYYMMDD')=TO_CHAR(SYSDATE,'YYYYMMDD') ORDER BY tgl;
          LogRow crsLog%ROWTYPE;

BEGIN
    --DBMS_OUTPUT.PUT_LINE('off day check '|| trunc(sysdate));
     SELECT COUNT(*) INTO jml FROM OFF_DAYS WHERE TO_CHAR(off_day,'DD-Mon-YYYY')=TO_CHAR(GET_NEXT_BUS_DATE(sysdate,-1),'DD-Mon-YYYY');
     IF jml=0 THEN
        DBMS_OUTPUT.PUT_LINE('Start Import to XCSD STG TABLE '|| TRUNC(SYSDATE));
        IMP_STG_CASECURITIES;
        IMP_STG_INVESTORS;
        IMP_STG_INVESTORSEXT;
        IMP_STG_ACCOUNTS;
        IMP_STG_MEM_CONTACTPERSON;
        IMP_STG_MEM_ADDRESS;
        IMP_STG_MEM_CONTACTDETAILS;
        IMP_STG_MEM_PARTSUSPENSION;
        IMP_STG_MEMBERS;
        IMP_STG_BASIC_SECURITIES;
        IMP_STG_CURRENCIES;
        IMP_STG_SECURITIES;
        IMP_STG_WARRANTS;
        IMP_STG_CAMEMBERS;
        IMP_STG_APPROVALREQUEST;
        IMP_STG_ACCOUNT_INSTRUCTIONS;
        IMP_STG_INVS_SOURCEFUND;
        IMP_STG_INVS_CONTACTDETAILS;
        IMP_STG_INVS_DIRECTSID;
        IMP_STG_ISSUER_CONTACTDETAIL;
        IMP_STG_PARTI_CONTACTDETAIL;
        IMP_STG_INVS_OBJCTV;
        IMP_STG_INVS_ADDRESS;
        IMP_STG_INVS_DIRECTSID;
        IMP_STG_INVS_DOCS;
        IMP_STG_INVS_IDENTIFIER;
        IMP_STG_INVS_INDCLASS1;
        IMP_STG_INVS_TAXCLASS;
        IMP_STG_INVS_MARITALSTA;
        IMP_STG_INVS_EDUCATION;
        IMP_STG_INVS_OCCUPATION;
        IMP_STG_INVS_INCOME;
        IMP_STG_INVS_INDCLASS2;
        IMP_STG_INVS_ASSET;
        IMP_STG_INVS_OPERATINGPROFIT;
        IMP_STG_INVS_INVCATEGORY;
        IMP_STG_SETTLEMENTINSTRUCTION;
        IMP_STG_SETTINSTRDETAILS;
        IMP_STG_CASHINSTRUCTION;
        IMP_STG_RESTRICTIONORDER;
        IMP_STG_SETTINSTRSTATUSLOG;
        IMP_STG_SI_INSTRUCTIONTYPE;
        IMP_STG_POS_MOVMNT;
        IMP_STG_BALANCES;
        IMP_STG_PAIREDSYNCINSTRUCTIONS;
        IMP_STG_INSTRUCTIONS;
        IMP_STG_OTCBONDS;
        IMP_STG_EXECFEE;
        IMP_STG_FEE;
        IMP_STG_FEEDEFINITION;
        IMP_STG_FEEBASIS;
        OPEN crsLog;
        FETCH crsLog INTO LogRow;
        IF crsLog%NOTFOUND THEN
           dbms_output.put_line('no log');
           CLOSE crsLog;
            DBMS_OUTPUT.PUT_LINE('End Import to XCSD STG TABLE '|| trunc(sysdate));
           RETURN;
        END IF;

        LOOP
            EXIT WHEN crsLog%NOTFOUND;
            --v_mail:=v_mail||TO_CHAR(LogRow.tgl,'DD-Mon-YYYY HH24:MI:SS')||' '||LogRow.Prosedur||','||LogRow.Proc_Desc||','||LogRow.Error_Message||CHR(13)||CHR(10);
            FETCH crsLog INTO LogRow;
        END LOOP;
        CLOSE crsLog;

         DBMS_OUTPUT.PUT_LINE('End Import to XCSD STG TABLE '|| trunc(sysdate));
     END IF;
END RUN_IMPSTG_XCSD;
--********************************************

procedure IMP_STG_CASECURITIES
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_CASECURITIES DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_TOBA_CASECURITIES (ID_CASEC_XCSD,
                    CODE_CALC_MET,
                    CODE_FRE_PAY,
                    DAT_CLO,
                    DAT_EAR_RED,
                    DAT_ISSUED,
                    DAT_OPE,
                    DAT_PREALI,
                    DAT_VAL,
                    LST_UPD_TS,
                    RATE_INT,
                    TYP_RATE)
                    select
                    instr.IDENT_INSTRUMENT AS ID_CASEC_XCSD,
                        CASE WHEN cainstr.IDENT_ACCRUALCONVENTION is null THEN null
                        WHEN cainstr.IDENT_ACCRUALCONVENTION = 1 THEN 1
                        WHEN cainstr.IDENT_ACCRUALCONVENTION = 4 THEN 2
                        WHEN cainstr.IDENT_ACCRUALCONVENTION = 5 THEN 3
                        WHEN cainstr.IDENT_ACCRUALCONVENTION = 6 THEN 5 END
                        AS CODE_CALC_MET,
                        CASE WHEN cainstr.IDENT_INTERESTPAYMENTFREQUENCY = 1 THEN 1
                        WHEN cainstr.IDENT_INTERESTPAYMENTFREQUENCY = 2 THEN 2
                        WHEN cainstr.IDENT_INTERESTPAYMENTFREQUENCY = 3 THEN 4
                        WHEN cainstr.IDENT_INTERESTPAYMENTFREQUENCY = 4 THEN 7
                        WHEN cainstr.IDENT_INTERESTPAYMENTFREQUENCY = 5 THEN 9
                        WHEN cainstr.IDENT_INTERESTPAYMENTFREQUENCY = 7 THEN 16 END
                        AS CODE_FRE_PAY,
                        cainstr.FIRSTINTERESTPERIODENDDATE AS DAT_CLO,
                        CAST(NULL AS DATE) AS DAT_EAR_RED,
                        instr.ISSUEDATE AS DAT_ISSUED,
                        cainstr.FIRSTINTERESTPERIODSTARTDATE AS DAT_OPE,
                        CAST(NULL AS DATE) AS DAT_PREALI,
                        CAST(NULL AS DATE) AS DAT_VAL,
                        instr.LASTUPDATETIME AS LST_UPD_TS,
                        cainstr.ANNUALINTERESTRATE AS RATE_INT,
                        cainstr.IDENT_RATETYPE AS TYP_RATE
                    FROM INSTRUMENT@'||v_SchemaProd||' instr
                        ,CAINSTRUMENTDEBT@'||v_SchemaProd||' cainstr 
                        WHERE
                        instr.IDENT_CAINSTRUMENT = cainstr.IDENT_CAINSTRUMENT(+) 
                        and instr.IDENT_MASTER is null';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_CASECURITIES', 'Y', 'Insert into IMP_STG_CASECURITIES');

     COMMIT;
      
     DBMS_OUTPUT.PUT_LINE('IMP_STG_CASECURITIES DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_CASECURITIES',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_CASECURITIES with error '||ERR_MSG);

      COMMIT;   
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_CASECURITIES with error '||ERR_MSG);
end;--IMP_STG_CASECURITIES

procedure IMP_STG_INVESTORS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_INVESTOR DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_TOBA_INVESTOR (IDENT_STAKEHOLDER, 
                            DISPLAYSID, IDENT_INVESTORTYPE, BIRTHDATE, DECEASEDDATE, COUNTRYOFRESIDENCE, 
                            DATEOFINCORPORATION, IDENT_ADMINISTRATOR, LONGNAME, FIRSTNAME, IDENT_ACTIVATIONTYPE, 
                            ACTIVATIONTYPE_NM, DATEOFINCEPTION, TAXPAYERIDENTIFIER, CODE, SHORTNAME, 
                            ENGLISHLONGNAME, COMMENTS, TAXDOMICILE, IDENT_MASTER, IDENT_REVISION, 
                            REQUESTED_ACTIVATION_DATE, REVISION_EFFECTIVE_FROM, REVISION_EFFECTIVE_TO, IDENT_LASTUPDATEUSER, LASTUPDATETIME, 
                            IDENT_INDUSTRYCLASSIFICATION1, INDUSTRYCLASSIFICATION1_CD, IDENT_INDUSTRYCLASSIFICATION2, INDUSTRYCLASSIFICATION2_CD, IDENT_REVISIONSTATE, 
                            IDENT_PREFCOMMUNICATIONMETHOD, IDENT_PREFCOMMUNICATIONLANG, IDENT_LEGALSTATUS, LEGALSTATUSDATE, ENGLISHCONDENSENAME, 
                            BILLINGREFERENCE, VALIDFROM, VALIDTO, INTERESTEDPARTY, AUTOMATICACCOUNTCREATION, 
                            NEWACCOUNTSONREACTIVATION, MAINID, IDENT_STAKEHOLDERIDTYPE_MAIN, DISPLAYNAME, SYSCREATED, 
                            SYSMODIFIED)
                        SELECT
                        invs.IDENT_STAKEHOLDER, iext.DISPLAYSID, invs.IDENT_INVESTORTYPE, invs.BIRTHDATE, invs.DECEASEDDATE, invs.COUNTRYOFRESIDENCE, invs.DATEOFINCORPORATION, 
                        invs.IDENT_ADMINISTRATOR, replace (invs.LONGNAME, CHR(10),'''') LONGNAME, replace (invs.FIRSTNAME, CHR(10),'''') FIRSTNAME, invs.IDENT_ACTIVATIONTYPE, atyp.TYPENAME ACTIVATIONTYPE_NM, invs.DATEOFINCEPTION, 
                        invs.TAXPAYERIDENTIFIER, invs.CODE, replace (invs.SHORTNAME, CHR(10),'''') SHORTNAME, replace (invs.ENGLISHLONGNAME, CHR(10),'''') ENGLISHLONGNAME, invs.COMMENTS, 
                        invs.TAXDOMICILE, invs.IDENT_MASTER, invs.IDENT_REVISION, invs.REQUESTED_ACTIVATION_DATE, invs.REVISION_EFFECTIVE_FROM, 
                        invs.REVISION_EFFECTIVE_TO, invs.IDENT_LASTUPDATEUSER, invs.LASTUPDATETIME, invs.IDENT_INDUSTRYCLASSIFICATION1, isc1.CODE INDUSTRYCLASSIFICATION1_CD,  invs.IDENT_INDUSTRYCLASSIFICATION2, 
                        isc2.CODE INDUSTRYCLASSIFICATION2_CD, invs.IDENT_REVISIONSTATE, invs.IDENT_PREFCOMMUNICATIONMETHOD, invs.IDENT_PREFCOMMUNICATIONLANG, invs.IDENT_LEGALSTATUS, invs.LEGALSTATUSDATE, 
                        invs.ENGLISHCONDENSENAME, invs.BILLINGREFERENCE, invs.VALIDFROM, invs.VALIDTO, invs. INTERESTEDPARTY, invs.AUTOMATICACCOUNTCREATION, invs.NEWACCOUNTSONREACTIVATION, 
                        invs.MAINID, invs.IDENT_STAKEHOLDERIDTYPE_MAIN, invs.DISPLAYNAME, invs.SYSCREATED, invs.SYSMODIFIED
                    FROM INVESTOR@'||v_SchemaProd||' invs,
                        INDUSTRYCLASSIFICATION1@'||v_SchemaProd||' isc1,
                        INDUSTRYCLASSIFICATION1@'||v_SchemaProd||' isc2,
                        ACTIVATIONTYPE@'||v_SchemaProd||' atyp,
                        INVESTOREXT@'||v_SchemaProd||' iext
                    WHERE 
                        invs.IDENT_ACTIVATIONTYPE = atyp.IDENT_ACTIVATIONTYPE(+)
                        and invs.IDENT_INDUSTRYCLASSIFICATION1 = isc1.ID(+)
                        and invs.IDENT_INDUSTRYCLASSIFICATION2 = isc2.ID(+)
                        and invs.IDENT_STAKEHOLDER = iext.IDENT_INVESTOR(+)
                        and invs.ident_master is null';

     execute immediate v_sql;

     COMMIT;     

     EXECUTE IMMEDIATE 'ANALYZE TABLE TMPSCHEMA.STG_TOBA_INVESTOR COMPUTE STATISTICS';

     COMMIT;
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVESTORS', 'Y', 'Insert into IMP_STG_INVESTORS');

      COMMIT;

     DBMS_OUTPUT.PUT_LINE('IMP_STG_INVESTORS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVESTORS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVESTORS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_INVESTORS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_INVESTORS

procedure IMP_STG_INVESTORSEXT
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_INVESTOREXT DROP STORAGE';

     v_sql := 'INSERT INTO TMPSCHEMA.STG_TOBA_INVESTOREXT 
                    (IDENT_INVESTOR, IDENT_PLACEOFESTABLISHMENT, PLACEOFESTABLISHMENT, LEGALDOMICILE, 
                    ASSOCIATIONARTICLE, IDENT_ASSETSLASTYEAR, IDENT_ASSETS2NDLASTYEAR, IDENT_ASSETS3RDLASTYEAR, 
                    IDENT_OPERPROFITLASTYEAR, IDENT_OPERPROFIT2NDLASTYEAR, IDENT_OPERPROFIT3RDLASTYEAR, MIDDLENAME, 
                    IDENT_BIRTHPLACE, BIRTHPLACE, IDENT_NATIONALITY, GENDER, IDENT_MARITALSTATUS, SPOUSENAME,
                    MOTHERSMAIDENNAME, HEIRNAME, HEIRRELATION, IDENT_EDUCATION, IDENT_OCCUPATION, 
                    OCCUPATION, BUSINESSNATURE, IDENT_INCOME, IDENT_CATEGORY, IDENT_TAXCLASSIFICATION, 
                    STATUSDATE, AKSESCARDHOLDER, BYPASSDOCUMENTS, IDENT_DIRECTINVESTOR, IDENT_IDNCLIENTCODE, 
                    ASSETOWNER, SYSCREATED, SYSMODIFIED, NONEUNIQUEIDENTIFIER, IDENT_DUPLICATE, MERGESOURCE, 
                    CBESTKEY, DISPLAYSID, UPLOADFILENAME, UPLOADSENDER, UPLOADTIMESTAMP, UPLOADEXTENDEDFILENAME)
                   SELECT 
                       IX.IDENT_INVESTOR, IX.IDENT_PLACEOFESTABLISHMENT, IX.PLACEOFESTABLISHMENT, IX.LEGALDOMICILE, 
                       IX.ASSOCIATIONARTICLE, IX.IDENT_ASSETSLASTYEAR, IX.IDENT_ASSETS2NDLASTYEAR, IX.IDENT_ASSETS3RDLASTYEAR, 
                       IX.IDENT_OPERPROFITLASTYEAR, IX.IDENT_OPERPROFIT2NDLASTYEAR, IX.IDENT_OPERPROFIT3RDLASTYEAR, 
                       IX.MIDDLENAME, IX.IDENT_BIRTHPLACE, IX.BIRTHPLACE, IX.IDENT_NATIONALITY, IX.GENDER, IX.IDENT_MARITALSTATUS, 
                       IX.SPOUSENAME, IX.MOTHERSMAIDENNAME, IX.HEIRNAME, IX.HEIRRELATION, IX.IDENT_EDUCATION, IX.IDENT_OCCUPATION, 
                       IX.OCCUPATION, IX.BUSINESSNATURE, IX.IDENT_INCOME, IX.IDENT_CATEGORY, IX.IDENT_TAXCLASSIFICATION, IX.STATUSDATE, 
                       IX.AKSESCARDHOLDER, IX.BYPASSDOCUMENTS, IX.IDENT_DIRECTINVESTOR, IX.IDENT_IDNCLIENTCODE, IX.ASSETOWNER, 
                       IX.SYSCREATED, IX.SYSMODIFIED, IX.NONEUNIQUEIDENTIFIER, IX.IDENT_DUPLICATE, IX.MERGESOURCE, IX.CBESTKEY, 
                       IX.DISPLAYSID, IX.UPLOADFILENAME, IX.UPLOADSENDER, IX.UPLOADTIMESTAMP, IX.UPLOADEXTENDEDFILENAME
                    FROM 
                        INVESTOREXT@'||v_SchemaProd||' IX
                        JOIN INVESTOR@'||v_SchemaProd||' I ON IX.IDENT_INVESTOR = I.IDENT_STAKEHOLDER
                    WHERE 
                        I.IDENT_MASTER IS NULL';

     execute immediate v_sql;

     commit;     

     EXECUTE IMMEDIATE 'ANALYZE TABLE TMPSCHEMA.STG_TOBA_INVESTOREXT COMPUTE STATISTICS';
     COMMIT;
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVESTORSEXT', 'Y', 'Insert into IMP_STG_INVESTORSEXT');

     COMMIT;

     DBMS_OUTPUT.PUT_LINE('IMP_STG_INVESTORSEXT DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVESTORSEXT',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVESTORSEXT with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_INVESTORSEXT with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_INVESTORSEXT

procedure IMP_STG_ACCOUNTS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_ACCOUNTS DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_TOBA_ACCOUNTS (CODE_ACCT_STA, 
                    CODE_ATA, DSC_ACCT, EXT_REF, ID_ACCT, LST_UPD_TS, 
                    TYP_ACCT, TYP_FLG_ACCT, MEM_ID_MEM_XCSD, MEM_PB_ID_MEM_CAPCO, CORR_ACCT, 
                    CODE_BR, MEM_IVS_ID_MEM_CAPCO, INV_ID_ISD_KSEI, ID_ACCT_XCSD,ID_MEM)
                    SELECT DISTINCT
                      CASE
                         WHEN AC_COMPOSITE.IDENT_ACTIVATIONTYPE = 1 AND (RESTR.RESTRICTWHOLEACCOUNT = 0 OR RESTR.RESTRICTWHOLEACCOUNT IS NULL) THEN 1
                        WHEN AC_COMPOSITE.IDENT_ACTIVATIONTYPE = 2 THEN 3
                        WHEN AC_COMPOSITE.IDENT_ACTIVATIONTYPE = 4 OR RESTR.RESTRICTWHOLEACCOUNT = 1 THEN 2
                        END AS CODE_ACCT_STA,
                      CASE
                      WHEN REG.CODE IS NOT NULL THEN 1003
                      WHEN AC_COMPOSITE.ACCOUNTIDENTIFIERVALUE = ''KSEPB000100103'' THEN 1003
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 1
                         THEN
                            1010
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 2
                         THEN
                            1013
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 3
                         THEN
                            1037
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 4
                         THEN
                            1012
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 5
                         THEN
                            1008
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 6
                         THEN
                            1019
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 7
                         THEN
                            1006
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 8
                         THEN
                            1015
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 9
                         THEN
                            1001
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND SUBSTR (AC_COMPOSITE.ACCOUNTIDENTIFIERVALUE, 1, 5) =
                                     ''KSEI1''
                         THEN
                            1076
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 2
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 IS NULL
                         THEN
                            1037
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 3
                         THEN
                            1007
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 4
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 2
                         THEN
                            1098
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 4
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 5
                         THEN
                            1067
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 4
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 3
                         THEN
                            1098
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 4
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 4
                         THEN
                            1099
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 4
                              AND HOLDER.IDENT_INDUSTRYCLASSIFICATION1 = 6
                         THEN
                            1100
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''ID''
                         THEN
                            1078
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''KW''
                         THEN
                            1090
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''KR''
                         THEN
                            1093
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''CA''
                         THEN
                            1095
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''GB''
                         THEN
                            1096
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''US''
                         THEN
                            1097
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''BN''
                         THEN
                            1101
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''SE''
                         THEN
                            1102
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''NO''
                         THEN
                            1249
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 5
                              AND HOLDER.TAXDOMICILE = ''SG''
                         THEN
                            1084
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 6
                         THEN
                            1006
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 7
                              AND HOLDER.TAXDOMICILE = ''BN''
                         THEN
                            1121
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 7
                              AND HOLDER.TAXDOMICILE = ''MY''
                         THEN
                            1122
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 7
                              AND HOLDER.TAXDOMICILE = ''AE''
                         THEN
                            1125
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 7
                              AND HOLDER.TAXDOMICILE = ''SG''
                         THEN
                            1152
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 8
                         THEN
                            1077
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 9
                              AND HOLDER.TAXDOMICILE != ''ID''
                         THEN
                            1004
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 10
                         THEN
                            1242
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''AT''
                         THEN
                            1107
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''AU''
                         THEN
                            1108
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''CA''
                         THEN
                            1113
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''DK''
                         THEN
                            1116
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''FR''
                         THEN
                            1119
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''DE''
                         THEN
                            1120
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''HK''
                         THEN
                            1126
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''IT''
                         THEN
                            1130
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''JP''
                         THEN
                            1131
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''LU''
                         THEN
                            1135
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''MY''
                         THEN
                            1136
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''NL''
                         THEN
                            1141
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''NO''
                         THEN
                            1143
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''CN''
                         THEN
                            1148
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''SG''
                         THEN
                            1153
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''KR''
                         THEN
                            1154
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''SE''
                         THEN
                            1160
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''CH''
                         THEN
                            1161
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''TW''
                         THEN
                            1163
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''TH''
                         THEN
                            1164
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''AE''
                         THEN
                            1167
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''GB''
                         THEN
                            1170
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''US''
                         THEN
                            1172
                         WHEN (HOLDEREXT.IDENT_TAXCLASSIFICATION = 11
                               OR HOLDEREXT.IDENT_TAXCLASSIFICATION = 12)
                              AND HOLDER.TAXDOMICILE = ''IN''
                         THEN
                            1129
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 13
                         THEN
                            1011
                         WHEN HOLDEREXT.IDENT_TAXCLASSIFICATION = 14
                         THEN
                            1017
                         ELSE
                            HOLDEREXT.IDENT_TAXCLASSIFICATION
                      END
                         AS CODE_ATA,
                      regexp_replace(replace(AC_COMPOSITE.ACCOUNTNAME, CHR(10),''''),''( ){2,}'','' '') AS DSC_ACCT,
                      SUBSTR (AC_COMPOSITE.ACCOUNTIDENTIFIERVALUE, 6, 4) AS EXT_REF,
                      AC_COMPOSITE.ACCOUNTIDENTIFIERVALUE AS ID_ACCT,
                      CASE
                         WHEN HOLDER.LASTUPDATETIME IS NOT NULL
                         THEN
                            GREATEST (
                               NVL (AC_COMPOSITE.LASTUPDATETIME,
                                    AC_COMPOSITE.REVISION_EFFECTIVE_FROM),
                               HOLDER.LASTUPDATETIME)
                         ELSE
                            NVL (AC_COMPOSITE.LASTUPDATETIME,
                                 AC_COMPOSITE.REVISION_EFFECTIVE_FROM)
                      END
                         AS LST_UPD_TS,
                      CASE
                         WHEN AC_SECURITY.IDENT_ACCOUNTTYPE = 92
                              OR AC_SECURITY.IDENT_ACCOUNTTYPE = 84
                         THEN
                            ''001''
                         ELSE
                            SUBSTR (AC_COMPOSITE.ACCOUNTIDENTIFIERVALUE, 10, 3)
                      END
                         AS TYP_ACCT,
                      CASE
                         WHEN AC_SECURITY.IDENT_ACCOUNTTYPE = 84 THEN 2
                         WHEN AC_CASH.IDENT_ACCOUNTTYPE = 1 THEN 2
                         ELSE 1
                      END
                         AS TYP_FLG_ACCT,
                      AC_COMPOSITE.IDENT_OPERATOR AS MEM_ID_MEM_XCSD,
                      PAYMENTBANK.CODE AS MEM_PB_ID_MEM_CAPCO,
                      CAST (NULL AS VARCHAR (40)) AS CORR_ACCT,
                      RESTR.IDENT_RESTRICTIONSUBTYPE AS CODE_BR,
                      CAST (NULL AS VARCHAR (40)) AS MEM_IVS_ID_MEM_CAPCO,
                      HOLDER.CODE AS INV_ID_ISD_KSEI,
                      AC_COMPOSITE.IDENT_ACCOUNTCOMPOSITE AS ID_ACCT_XCSD,
                      substr(AC_COMPOSITE.ACCOUNTIDENTIFIERVALUE,1,5) ID_MEM
                 FROM ACCOUNTCOMPOSITE@'||v_SchemaProd||'  AC_COMPOSITE
                LEFT JOIN SECURITIESACCOUNT@'||v_SchemaProd||'  AC_SECURITY ON AC_SECURITY.IDENT_ACCOUNTCOMPOSITE = AC_COMPOSITE.IDENT_ACCOUNTCOMPOSITE
                LEFT JOIN CASHACCOUNT@'||v_SchemaProd||'  AC_CASH ON AC_CASH.IDENT_ACCOUNTCOMPOSITE = AC_COMPOSITE.IDENT_ACCOUNTCOMPOSITE
                LEFT JOIN PAYMENTBANK@'||v_SchemaProd||'  ON PAYMENTBANK.IDENT_PAYMENTBANK = AC_CASH.IDENT_PAYMENTBANK
                LEFT JOIN PARTICIPANT@'||v_SchemaProd||'  ACCOUNTOPERATOR ON ACCOUNTOPERATOR.IDENT_STAKEHOLDER = AC_COMPOSITE.IDENT_OPERATOR
                LEFT JOIN (
                    SELECT REG.CODE, PRA.IDENT_CAPACITY
                    FROM PARTICIPANT@'||v_SchemaProd||'  REG
                    JOIN PARTICIPANTROLEASSIGNMENT@'||v_SchemaProd||'  PRA ON REG.IDENT_STAKEHOLDER = PRA.IDENT_STAKEHOLDER
                    WHERE PRA.IDENT_CAPACITY = 214
                ) REG ON SUBSTR(AC_COMPOSITE.ACCOUNTIDENTIFIERVALUE ,1,5) = REG.CODE
                LEFT JOIN INVESTOR@'||v_SchemaProd||'  HOLDER ON HOLDER.IDENT_STAKEHOLDER = AC_COMPOSITE.IDENT_HOLDER
                LEFT JOIN INVESTOREXT@'||v_SchemaProd||'  HOLDEREXT ON HOLDEREXT.IDENT_INVESTOR = HOLDER.IDENT_STAKEHOLDER
                LEFT JOIN (
                SELECT RESTR.*, ROW_NUMBER() OVER (PARTITION BY IDENT_ACCOUNTCOMPOSITE ORDER BY IDENT_RESTRICTIONORDER ASC) AS RROW FROM ACTIVEACCOUNTRESTRICTIONS@'||v_SchemaProd||'  RESTR) RESTR
                ON RESTR.IDENT_ACCOUNTCOMPOSITE = AC_COMPOSITE.IDENT_ACCOUNTCOMPOSITE AND RROW = 1
            WHERE ACCOUNTOPERATOR.IDENT_ACTIVATIONTYPE != 5
                         AND AC_COMPOSITE.IDENT_MASTER IS NULL
                     AND AC_COMPOSITE.IDENT_ACTIVATIONTYPE != 5
                     AND (AC_CASH.IDENT_ACCOUNTTYPE != 4 OR AC_CASH.IDENT_ACCOUNTTYPE IS NULL OR AC_COMPOSITE.ACCOUNTIDENTIFIERVALUE = ''KSEI1BSB100172'')';

     execute immediate v_sql;

     EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.ACCOUNT_BR drop storage';
     EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_CASHACCOUNT drop storage';
     EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT drop storage';

     v_sql := 'insert into TMPSCHEMA.ACCOUNT_BR
    SELECT 
        ID_OBJ ID_ACCT,
        CODE_BR MAPCODEBR_XC,
        BR ORICODEBR_XC,
        BR_DSC CODEBRDSC_XC
    FROM (SELECT CASE
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 13 THEN 1
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 17 THEN 2
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 9 THEN 3
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 5 THEN 4
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 4 THEN 1001
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 3 THEN 1002
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 11 THEN 1012
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 7 THEN 1006
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 8 THEN 1007
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 12 THEN 1008
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 1 THEN 1009
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 6 THEN 1010
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 10 THEN 1011
                  WHEN RO.IDENT_RESTRICTIONSUBTYPE = 16 THEN 1013
               END
                  AS CODE_BR,
               RO.VALIDTO DAT_END,
               RO.VALIDFROM DAT_START,
               TO_CHAR (RO.LASTUPDATETIME, ''YYYYMMDDHH24MISSFF3'')
                  LST_UPD_TS,
               NULL AS TYP_CODE_BR,
               AC.ACCOUNTIDENTIFIERVALUE ID_OBJ,
               ''ACC'' BR_OBJ,BRT.NAME BR_DSC, RO.IDENT_RESTRICTIONSUBTYPE BR
          FROM RESTRICTIONORDER@'||v_SchemaProd||'  RO, ACCOUNTCOMPOSITE@'||v_SchemaProd||'  AC,
            BLOCKINGREASON@'||v_SchemaProd||' BRT
         WHERE RO.IDENT_MASTER IS NULL
               AND RO.IDENT_ACCOUNTCOMPOSITE = AC.IDENT_ACCOUNTCOMPOSITE
               AND RO.RESTRICTWHOLEACCOUNT = 1
               AND RO.IDENT_RESTRICTIONSUBTYPE = BRT.IDENT_BLOCKINGREASON)';

     execute immediate v_sql;

      v_sql := 'insert into TMPSCHEMA.STG_TOBA_CASHACCOUNT select * from CASHACCOUNT@'||v_SchemaProd;

      execute immediate v_sql;

      v_sql := 'insert into TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT select * from SECURITIESACCOUNT@'||v_SchemaProd;

      execute immediate v_sql;
     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_ACCOUNTS', 'Y', 'Insert into IMP_STG_ACCOUNTS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_ACCOUNTS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_ACCOUNTS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_ACCOUNTS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_ACCOUNTS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_ACCOUNTS

procedure IMP_STG_MEM_CONTACTPERSON
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_MEM_CONTACTPERSON DROP STORAGE';

     V_SQL := 'insert into TMPSCHEMA.STG_MEM_CONTACTPERSON
                (IDENT_STAKEHOLDER, CON1, CON2, WEBSITE)
                SELECT CP.IDENT_STAKEHOLDER,
                MAX(CASE WHEN CP.NOS = 1 THEN CP.CONTACTNAME ELSE '''' END) CON1,
                MAX(CASE WHEN CP.NOS = 2 THEN CP.CONTACTNAME ELSE '''' END) CON2,
                MAX(CP.WEBSITE) WEBSITE
                FROM (
                SELECT C.ID, NVL(C.IDENT_PARTICIPANT, C.IDENT_ISSUER) IDENT_STAKEHOLDER, C.CONTACTNAME, C.TITLE, 
                C.PHONENUMBER, C.MOBILENUMBER, C.FAXNUMBER, C.EMAIL, C.PUBLICDETAILS, C.WEBSITE, 
                C.POSITION, C.SYSCREATED, C.SYSMODIFIED, 
                ROW_NUMBER() OVER (PARTITION BY NVL(IDENT_PARTICIPANT, IDENT_ISSUER) ORDER BY C.CONTACTNAME DESC NULLS LAST, C.ID) NOS
                FROM CONTACTDETAILS@'||v_SchemaProd||' C
                WHERE (C.IDENT_PARTICIPANT IS NOT NULL OR C.IDENT_ISSUER IS NOT NULL)
                ) CP
                GROUP BY CP.IDENT_STAKEHOLDER';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_MEM_CONTACTPERSON', 'Y', 'Insert into IMP_STG_MEM_CONTACTPERSON');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_MEM_CONTACTPERSON DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_MEM_CONTACTPERSON',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_MEM_CONTACTPERSON with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_MEM_CONTACTPERSON with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_MEM_CONTACTPERSON

procedure IMP_STG_MEM_ADDRESS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_MEM_ADDRESS DROP STORAGE';

     V_SQL := 'insert into TMPSCHEMA.STG_MEM_ADDRESS
                (IDENT_STAKEHOLDER, IDENT_ADDRESS, IDENT_ADDRESSTYPE, ADDRESS1, 
                ADDRESS2, POSTALCODE, CITY, STATEPROVINCE, COUNTRY, URL, SYSCREATED, SYSMODIFIED, NOS)
                SELECT NVL(AD.IDENT_PARTICIPANT, AD.IDENT_ISSUER) IDENT_STAKEHOLDER, AD.IDENT_ADDRESS, 
                AD.IDENT_ADDRESSTYPE, AD.ADDRESS1, AD.ADDRESS2, AD.POSTALCODE, AD.CITY, AD.STATEPROVINCE, 
                AD.COUNTRY, AD.URL, AD.SYSCREATED, AD.SYSMODIFIED,
                ROW_NUMBER() OVER (PARTITION BY NVL(IDENT_PARTICIPANT, IDENT_ISSUER), 
                    AD.IDENT_ADDRESSTYPE ORDER BY AD.IDENT_ADDRESS) NOS
                FROM ADDRESS@'||v_SchemaProd||' AD
                WHERE (AD.IDENT_PARTICIPANT IS NOT NULL OR AD.IDENT_ISSUER IS NOT NULL)';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_MEM_ADDRESS', 'Y', 'Insert into IMP_STG_MEM_ADDRESS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_MEM_ADDRESS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_MEM_ADDRESS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_MEM_ADDRESS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_MEM_ADDRESS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_MEM_ADDRESS

procedure IMP_STG_MEM_CONTACTDETAILS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_MEM_CONTACTDETAILS DROP STORAGE';

     V_SQL := 'insert into TMPSCHEMA.STG_MEM_CONTACTDETAILS
                (IDENT_STAKEHOLDER, PHONENBR, FAXNBR, EMAILADR)
                SELECT NVL(IDENT_PARTICIPANT, IDENT_ISSUER) IDENT_STAKEHOLDER,
                LISTAGG (PHONENUMBER, '';'') WITHIN GROUP (ORDER BY ID) AS PHONENBR,
                LISTAGG (FAXNUMBER, '';'') WITHIN GROUP (ORDER BY ID) AS FAXNBR,
                LISTAGG (EMAIL, '';'') WITHIN GROUP (ORDER BY ID) AS EMAILADR
                FROM CONTACTDETAILS@'||v_SchemaProd||'
                WHERE (IDENT_PARTICIPANT IS NOT NULL OR IDENT_ISSUER IS NOT NULL)
                GROUP BY NVL(IDENT_PARTICIPANT, IDENT_ISSUER)';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_MEM_CONTACTDETAILS', 'Y', 'Insert into IMP_STG_MEM_CONTACTDETAILS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_MEM_CONTACTDETAILS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_MEM_CONTACTDETAILS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_MEM_CONTACTDETAILS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_MEM_CONTACTDETAILS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_MEM_CONTACTDETAILS

procedure IMP_STG_MEM_PARTSUSPENSION
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_MEM_PARTSUSPENSION DROP STORAGE';

     V_SQL := 'insert into TMPSCHEMA.STG_MEM_PARTSUSPENSION
                (ID, IDENT_SUSPENDEDPARTICIPANT, IDENT_BLOCKINGREASON, CBEST_CODE_BR, 
                SYSCREATED, SYSMODIFIED, SUSPRANK)
                SELECT P.ID, P.IDENT_SUSPENDEDPARTICIPANT, P.IDENT_BLOCKINGREASON,
                CASE WHEN P.IDENT_BLOCKINGREASON = 4 THEN 1
                WHEN P.IDENT_BLOCKINGREASON = 11 THEN 2
                WHEN P.IDENT_BLOCKINGREASON = 7 THEN 3
                WHEN P.IDENT_BLOCKINGREASON = 12 THEN 4
                WHEN P.IDENT_BLOCKINGREASON = 2 THEN 1001
                WHEN P.IDENT_BLOCKINGREASON = 1 THEN 1002
                WHEN P.IDENT_BLOCKINGREASON = 8 THEN 1012
                WHEN P.IDENT_BLOCKINGREASON = 6 THEN 1007
                WHEN P.IDENT_BLOCKINGREASON = 5 THEN 1008
                WHEN P.IDENT_BLOCKINGREASON = 9 THEN 1009
                WHEN P.IDENT_BLOCKINGREASON = 10 THEN 1009 END CBEST_CODE_BR,
                P.SYSCREATED, P.SYSMODIFIED,
                ROW_NUMBER () OVER (PARTITION BY IDENT_SUSPENDEDPARTICIPANT ORDER BY P.IDENT_BLOCKINGREASON, P.ID) SUSPRANK
                FROM IDNPARTICIPANTSUSPENSION@'||v_SchemaProd||' P';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_MEM_PARTSUSPENSION', 'Y', 'Insert into IMP_STG_MEM_PARTSUSPENSION');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_MEM_PARTSUSPENSION DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_MEM_PARTSUSPENSION',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_MEM_PARTSUSPENSION with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_MEM_PARTSUSPENSION with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_MEM_PARTSUSPENSION

procedure IMP_STG_MEMBERS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_MEMBERS DROP STORAGE';
    
    -- PARTICIPANT
     v_sql := 'insert into TMPSCHEMA.STG_TOBA_MEMBERS (CON_PERS_1, 
                    CON_PERS_2, CODE_ZIP, CODE_COU, CODE_MEM_CAT, CODE_MRKT_RATE, 
                    CODE_MEM_STA, CITY_DSC, DSC_MEM, DSC_MEM_SHORT, EXT_REF, 
                    ID_MEM, ID_MEM_XCSD, ID_MEM_INTE, LST_UPD_TS, MEM_ADR_1, 
                    MEM_ADR_2, TYP_MEM, MEM_CLE_ID_MEM_CAPCO, TAX_NUM, FAX_NBR, 
                    PHONE_NBR, WEB_NM, EMAIL_ADR, CODE_BR, CODE_IVS, 
                    COR_CITY_DSC, COR_CODE_COU, COR_CODE_ZIP, COR_ADR_1, COR_ADR_2)
                    SELECT DISTINCT
                    CPR.CON1 AS CON_PERS_1,
                    CPR.CON2 AS CON_PERS_2,
                    ADR.POSTALCODE AS CODE_ZIP,
                    CASE WHEN ADR.COUNTRY = ''ID'' THEN 1001 END AS CODE_COU,
                    1 AS CODE_MEM_CAT,
                    CAST (NULL AS NUMBER) AS CODE_MRKT_RATE,
                    CASE
                    WHEN ST.IDENT_ACTIVATIONTYPE = 1 THEN 1
                    WHEN ST.IDENT_ACTIVATIONTYPE = 2 THEN 3 
                    WHEN ST.IDENT_ACTIVATIONTYPE = 4 THEN 2
                    END AS CODE_MEM_STA,
                    ADR.CITY AS CITY_DSC,
                    ST.LONGNAME AS DSC_MEM,
                    ST.ENGLISHCONDENSENAME AS DSC_MEM_SHORT,
                    CAST (NULL AS VARCHAR2 (30)) AS EXT_REF,
                    ST.CODE AS ID_MEM,
                    ST.IDENT_STAKEHOLDER ID_MEM_XCSD,
                    CAST (NULL AS VARCHAR2 (30)) AS ID_MEM_INTE,
                    NVL (ST.LASTUPDATETIME, ST.REVISION_EFFECTIVE_FROM) AS LST_UPD_TS,
                    ADR.ADDRESS1 AS MEM_ADR_1,
                    ADR.ADDRESS2 AS MEM_ADR_2,
                    CASE
                    WHEN PRA1.SORTVAL = 1 THEN 5                               -- CSD
                    WHEN PRA1.SORTVAL = 2 THEN 4    -- KSEPB AS PAYMENT BANK IN CBEST
                    WHEN PRA1.SORTVAL = 3 THEN 6                              -- KPEI
                    WHEN PRA1.SORTVAL = 4 THEN 1                   -- CLEARING MEMBER
                    WHEN PRA1.SORTVAL = 5 THEN 4  -- NOSTRO ACCOUNT MEMBER, REGISTRAR
                    WHEN PRA1.SORTVAL = 6 THEN 4 -- NOSTRO ACCOUNT MEMBER, PAYMENT BANK
                    WHEN PRA1.SORTVAL = 7 THEN 2               -- NON CLEARING MEMBER
                    ELSE 0
                    END AS TYP_MEM,
                    CAST (NULL AS VARCHAR2 (40)) AS MEM_CLE_ID_MEM_CAPCO,
                    NVL(ST.MAINID, ST.TAXPAYERIDENTIFIER) AS TAX_NUM,
                    REGEXP_REPLACE (CONT.FAXNBR, ''\+62-'', ''0'') AS FAX_NBR,
                    REGEXP_REPLACE (CONT.PHONENBR, ''\+62-'', ''0'') AS PHONE_NBR,
                    CPR.WEBSITE AS WEB_NM,
                    CONT.EMAILADR AS EMAIL_ADR,
                    PSUSP.CBEST_CODE_BR AS CODE_BR,
                    CAST (NULL AS VARCHAR2 (30)) AS CODE_IVS,
                    ADR2.CITY AS COR_CITY_DSC,
                    CASE WHEN ADR2.COUNTRY = ''ID'' THEN 1001 END AS COR_CODE_COU,
                    ADR2.POSTALCODE AS COR_CODE_ZIP,
                    ADR2.ADDRESS1 AS COR_ADR_1,
                    ADR2.ADDRESS2 AS COR_ADR_2
                    FROM PARTICIPANT@'||v_SchemaProd||' ST
                    LEFT JOIN TMPSCHEMA.STG_MEM_CONTACTPERSON CPR ON ST.IDENT_STAKEHOLDER = CPR.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_MEM_ADDRESS ADR ON ST.IDENT_STAKEHOLDER = ADR.IDENT_STAKEHOLDER AND ADR.IDENT_ADDRESSTYPE = 1 AND ADR.NOS = 1
                    LEFT JOIN TMPSCHEMA.STG_MEM_ADDRESS ADR2 ON ST.IDENT_STAKEHOLDER = ADR2.IDENT_STAKEHOLDER AND ADR2.IDENT_ADDRESSTYPE = 2 AND ADR2.NOS = 1
                    LEFT JOIN TMPSCHEMA.STG_MEM_CONTACTDETAILS CONT ON ST.IDENT_STAKEHOLDER = CONT.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_MEM_PARTSUSPENSION PSUSP ON ST.IDENT_STAKEHOLDER = PSUSP.IDENT_SUSPENDEDPARTICIPANT
                    LEFT JOIN (  SELECT IDENT_STAKEHOLDER,
                                      MIN (
                                         CASE IDENT_CAPACITY
                                            WHEN 1 THEN 1                       -- CSD
                                            WHEN 224 THEN 2            -- CENTRAL BANK
                                            WHEN 213 THEN 3          -- CLEARING HOUSE
                                            WHEN 216 THEN 4             -- KPEI MEMBER
                                            WHEN 214 THEN 5               -- REGISTRAR
                                            WHEN 209 THEN 6            -- PAYMENT BANK
                                            WHEN 202 THEN 7        -- ACCOUNT OPERATOR
                                         END)
                                         AS SORTVAL
                                 FROM PARTICIPANTROLEASSIGNMENT@'||v_SchemaProd||'
                             GROUP BY IDENT_STAKEHOLDER
                             ORDER BY SORTVAL) PRA1 ON ST.IDENT_STAKEHOLDER = PRA1.IDENT_STAKEHOLDER
                    WHERE ST.IDENT_MASTER IS NULL
                    AND ST.IDENT_ACTIVATIONTYPE != 5
                    AND (PSUSP.SUSPRANK IS NULL OR PSUSP.SUSPRANK = 1)';

    EXECUTE IMMEDIATE V_SQL;
    
    -- ISSUER
     v_sql := 'insert into TMPSCHEMA.STG_TOBA_MEMBERS (CON_PERS_1, 
                    CON_PERS_2, CODE_ZIP, CODE_COU, CODE_MEM_CAT, CODE_MRKT_RATE, 
                    CODE_MEM_STA, CITY_DSC, DSC_MEM, DSC_MEM_SHORT, EXT_REF, 
                    ID_MEM, ID_MEM_XCSD, ID_MEM_INTE, LST_UPD_TS, MEM_ADR_1, 
                    MEM_ADR_2, TYP_MEM, MEM_CLE_ID_MEM_CAPCO, TAX_NUM, FAX_NBR, 
                    PHONE_NBR, WEB_NM, EMAIL_ADR, CODE_BR, CODE_IVS, 
                    COR_CITY_DSC, COR_CODE_COU, COR_CODE_ZIP, COR_ADR_1, COR_ADR_2)
                    SELECT CPR.CON1 AS CON_PERS_1,
                    CPR.CON2 AS CON_PERS_2,
                    ADR.POSTALCODE AS CODE_ZIP,
                    CASE WHEN ADR.COUNTRY = ''ID'' THEN 1001 END AS CODE_COU,
                    1 AS CODE_MEM_CAT,
                    CAST (NULL AS NUMBER) AS CODE_MRKT_RATE,
                    CASE
                    WHEN ISS.IDENT_ACTIVATIONTYPE = 1 THEN 1
                    WHEN ISS.IDENT_ACTIVATIONTYPE = 2 THEN 3
                    WHEN ISS.IDENT_ACTIVATIONTYPE = 4 THEN 2
                    END AS CODE_MEM_STA,
                    ADR.CITY AS CITY_DSC,
                    ISS.LONGNAME AS DSC_MEM,
                    CAST (NULL AS VARCHAR2 (30)) AS DSC_MEM_SHORT,
                    CAST (NULL AS VARCHAR2 (30)) AS EXT_REF,
                    ISS.CODE AS ID_MEM,
                    ISS.IDENT_STAKEHOLDER ID_MEM_XCSD,
                    CAST (NULL AS VARCHAR2 (30)) AS ID_MEM_INTE,
                    NVL (ISS.LASTUPDATETIME, ISS.REVISION_EFFECTIVE_FROM) AS LST_UPD_TS,
                    ADR.ADDRESS1 AS MEM_ADR_1,
                    ADR.ADDRESS2 AS MEM_ADR_2,
                    3 AS TYP_MEM,                                              -- ISSUER
                    CAST (NULL AS VARCHAR2 (40)) AS MEM_CLE_ID_MEM_CAPCO,
                    NVL(ISS.MAINID, ISS.TAXPAYERIDENTIFIER) AS TAX_NUM,
                    REGEXP_REPLACE (CONT.FAXNBR, ''\+62-'', ''0'') AS FAX_NBR,
                    REGEXP_REPLACE (CONT.PHONENBR, ''\+62-'', ''0'') AS PHONE_NBR,
                    CPR.WEBSITE AS WEB_NM,
                    CONT.EMAILADR AS EMAIL_ADR,
                    PSUSP.CBEST_CODE_BR AS CODE_BR,
                    CAST (NULL AS VARCHAR2 (30)) AS CODE_IVS,
                    ADR2.CITY AS COR_CITY_DSC,
                    CASE WHEN ADR2.COUNTRY = ''ID'' THEN 1001 END AS COR_CODE_COU,
                    ADR2.POSTALCODE AS COR_CODE_ZIP,
                    ADR2.ADDRESS1 AS COR_ADR_1,
                    ADR2.ADDRESS2 AS COR_ADR_2
                    FROM ISSUER@'||v_SchemaProd||' ISS
                    LEFT JOIN TMPSCHEMA.STG_MEM_CONTACTPERSON CPR ON ISS.IDENT_STAKEHOLDER = CPR.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_MEM_ADDRESS ADR ON ISS.IDENT_STAKEHOLDER = ADR.IDENT_STAKEHOLDER AND ADR.IDENT_ADDRESSTYPE = 1 AND ADR.NOS = 1
                    LEFT JOIN TMPSCHEMA.STG_MEM_ADDRESS ADR2 ON ISS.IDENT_STAKEHOLDER = ADR2.IDENT_STAKEHOLDER AND ADR2.IDENT_ADDRESSTYPE = 2 AND ADR2.NOS = 1
                    LEFT JOIN TMPSCHEMA.STG_MEM_CONTACTDETAILS CONT ON ISS.IDENT_STAKEHOLDER = CONT.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_MEM_PARTSUSPENSION PSUSP ON ISS.IDENT_STAKEHOLDER = PSUSP.IDENT_SUSPENDEDPARTICIPANT
                    WHERE ISS.IDENT_MASTER IS NULL
                    AND ISS.IDENT_ACTIVATIONTYPE != 5
                    AND (PSUSP.SUSPRANK IS NULL OR PSUSP.SUSPRANK = 1)';

    EXECUTE IMMEDIATE V_SQL;
    
    -- BICOUNTERPARTY
     v_sql := 'insert into TMPSCHEMA.STG_TOBA_MEMBERS (CON_PERS_1, 
                    CON_PERS_2, CODE_ZIP, CODE_COU, CODE_MEM_CAT, CODE_MRKT_RATE, 
                    CODE_MEM_STA, CITY_DSC, DSC_MEM, DSC_MEM_SHORT, EXT_REF, 
                    ID_MEM, ID_MEM_XCSD, ID_MEM_INTE, LST_UPD_TS, MEM_ADR_1, 
                    MEM_ADR_2, TYP_MEM, MEM_CLE_ID_MEM_CAPCO, TAX_NUM, FAX_NBR, 
                    PHONE_NBR, WEB_NM, EMAIL_ADR, CODE_BR, CODE_IVS, 
                    COR_CITY_DSC, COR_CODE_COU, COR_CODE_ZIP, COR_ADR_1, COR_ADR_2)
                    SELECT 
                    NULL AS CON_PERS_1, NULL AS CON_PERS_2, NULL AS CODE_ZIP, 1001 AS CODE_COU, 1 AS CODE_MEM_CAT,
                    NULL AS CODE_MRKT_RATE, 
                    DECODE (ST.IDENT_ACTIVATIONTYPE, 1,1, 2,3, 4,2) AS CODE_MEM_STA,
                    ''Other'' AS CITY_DSC, ST.LONGNAME AS DSC_MEM, NULL AS DSC_MEM_SHORT, NULL AS EXT_REF,
                    ST.CODE AS ID_MEM, 90000+ST.IDENT_BICOUNTERPARTY AS ID_MEM_XCSD, NULL AS ID_MEM_INTE,
                    ST.LASTUPDATETIME AS LST_UPD_TS, NULL AS MEM_ADR_1, NULL AS MEM_ADR_2, 9 AS TYP_MEM,
                    NULL AS MEM_CLE_ID_MEM_CAPCO, NULL AS TAX_NUM, NULL AS FAX_NBR, NULL AS PHONE_NBR,
                    NULL AS WEB_NM, NULL AS EMAIL_ADR, NULL AS CODE_BR, NULL AS CODE_IVS, NULL AS COR_CITY_DSC,
                    NULL AS COR_CODE_COU, NULL AS COR_CODE_ZIP, NULL AS COR_ADR_1, NULL AS COR_ADR_2
                    FROM BICOUNTERPARTY@'||v_SchemaProd||' ST
                    WHERE IDENT_MASTER IS NULL';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_MEMBERS', 'Y', 'Insert into IMP_STG_MEMBERS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_MEMBERS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_MEMBERS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_MEMBERS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_MEMBERS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_MEMBERS

procedure IMP_STG_BASIC_SECURITIES
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_BASIC_SECURITIES DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_TOBA_BASIC_SECURITIES (AMT_MIN_TRADE, 
                        CODE_BASE_SEC, CODE_PHYS, CODE_REG, CODE_SEC_RATE, DATE_ACTI, 
                        DAT_MAT, EXT_REF, SEC_DENM, SEC_DSC, TYP_SEC, 
                        SEC_NUM, EXE_PRI, TYP_EXE, TYP_OPT, WAR_RATIO, 
                        AMT_ISSUED, CODE_ACTI_SEC, ID_INS_CAPCO, LST_UPD_TS, CODE_STA, 
                        CASEC_ID_CASEC_CAPCO, CUR_ID_INS_CAPCO, INS_ID_INS_CAPCO, TYP_BS, CODE_BR, 
                        DATE_ISSUED,IDENT_INSTRUMENT,NOM_VALUE)
                    SELECT INSTRUMENT.MINIMUMSETTLEMENTQUANTITY AS AMT_MIN_TRADE,
                      SUBSTR (INSTRUMENT.NAME, 1, LEAST (30, LENGTH (INSTRUMENT.NAME)))
                         AS CODE_BASE_SEC,
                      CASE
                         WHEN INSTRUMENT.IDENT_INSTRUMENTFORM = 2 THEN 4
                         WHEN INSTRUMENT.IDENT_INSTRUMENTFORM = 3 THEN 1
                         WHEN INSTRUMENT.IDENT_INSTRUMENTFORM = 1 THEN 0
                      END
                         AS CODE_PHYS,
                      CASE WHEN INSTRUMENT.ISBEARER = 1 THEN 1 ELSE 2 END AS CODE_REG,
                      CAST (DECODE (CAINSTR.RATING,
                                   ''idAAA'', 1001,
                                   ''idAA+'', 1002,
                                   ''idAA'', 1003,
                                   ''idAA-'', 1004,
                                   ''idA+'', 1005,
                                   ''idA'', 1006,
                                   ''idA-'', 1007,
                                   ''idBBB+'', 1008,
                                   ''idBBB'', 1009,
                                   ''idBBB-'', 1010,
                                   ''idBB+'', 1011,
                                   ''idBB'', 1012,
                                   ''idBB-'', 1013,
                                   ''idB+'', 1014,
                                   ''idB'', 1015,
                                   ''idB-'', 1016,
                                   ''idCCC'', 1017,
                                   ''idD'', 1018,
                                   ''Aaa.id'', 1019,
                                   ''Aa1.id'', 1020,
                                   ''Aa2.id'', 1021,
                                   ''Aa3.id'', 1022,
                                   ''A1.id'', 1023,
                                   ''A2.id'', 1024,
                                   ''A3.id'', 1025,
                                   ''Baa1.id'', 1026,
                                   ''Baa2.id'', 1027,
                                   ''Baa3.id'', 1028,
                                   ''Ba1.id'', 1029,
                                   ''Ba2.id'', 1030,
                                   ''Ba3.id'', 1031,
                                   ''B1.id'', 1032,
                                   ''B2.id'', 1033,
                                   ''B3.id'', 1034,
                                   ''Caa1.id'', 1035,
                                   ''Caa2.id'', 1036,
                                   ''Caa3.id'', 1037,
                                   ''Ca.id'', 1038,
                                   ''C.id'', 1039,
                                   ''idCC'', 1040)
                              AS NUMBER(5)) AS CODE_SEC_RATE,
                      INSTRUMENT.REGISTRATIONDATE AS DATE_ACTI,
                      INSTRUMENT.MATURITYDATE AS DAT_MAT,
                      CAST (NULL AS VARCHAR2 (145)) AS EXT_REF,
                      INSTRUMENT.SETTLEMENTLOT AS SEC_DENM,
                      INSTRUMENT.LONGNAME AS SEC_DSC,
                      CASE
                         WHEN INSTRUMENT.IDENT_ASSETCLASS IN (1001, 1029, 1030) THEN 2
                         WHEN INSTRUMENT.IDENT_ASSETCLASS IN (1002, 1028) THEN 1
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1003 THEN 14
                         WHEN INSTRUMENT.IDENT_ASSETCLASS IN (1004, 1031) THEN 7
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1005 THEN 4
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1006 THEN 5
                         WHEN INSTRUMENT.IDENT_ASSETCLASS IN (1007, 1027) THEN 16
                         WHEN INSTRUMENT.IDENT_ASSETCLASS IN (1008, 1017) THEN 3
                         WHEN INSTRUMENT.IDENT_ASSETCLASS IN (1009, 1018, 1019) THEN 13
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1010 THEN 11
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1011 THEN 17
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1012 THEN 8
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1013 THEN 18
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1014 THEN 9
                         WHEN INSTRUMENT.IDENT_ASSETCLASS IN (1015,1032) THEN 19
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1016 THEN 15
                         WHEN INSTRUMENT.IDENT_ASSETCLASS = 1033 THEN 20
                         ELSE 0
                      END
                         AS TYP_SEC,
                      CASE
                         WHEN ac.IDENT_ASSETCLASSTYPE != 5
                         THEN
                            INSTRUMENT.TOTALALLOWEDQUANTITY
                         ELSE
                            CAST (NULL AS NUMBER)
                      END
                         AS SEC_NUM,
                      RA.EXERCISEPRICE AS EXE_PRI,
                      CASE
                         WHEN RA.IDENT_EXERCISESTYLE = 1 THEN 2
                         WHEN RA.IDENT_EXERCISESTYLE = 2 THEN 1
                      END
                         AS TYP_EXE,
                      CASE
                         WHEN RA.IDENT_WARRANTTYPE = 0 THEN 1
                         WHEN RA.IDENT_WARRANTTYPE = 1 THEN 2
                      END
                         AS TYP_OPT,
                      CAST (NULL AS VARCHAR2 (145)) AS WAR_RATIO,
                      CASE
                         WHEN AC.IDENT_ASSETCLASSTYPE = 5 THEN INSTRUMENT.TOTALALLOWEDQUANTITY
                         ELSE INSTRUMENT.ISSUEDQUANTITY
                      END
                         AS AMT_ISSUED,
                      CD.CODE_VAL AS CODE_ACTI_SEC,
                      INSTRUMENT.ISIN AS ID_INS_CAPCO,
                      INSTRUMENT.LASTUPDATETIME AS LST_UPD_TS,
                      CASE
                         WHEN INSTRUMENT.IDENT_ACTIVATIONTYPE = 1 THEN 1
                         WHEN INSTRUMENT.IDENT_ACTIVATIONTYPE = 4 THEN 2
                         WHEN INSTRUMENT.IDENT_ACTIVATIONTYPE = 2 THEN 3
                      END
                         AS CODE_STA,
                      INSTRUMENT.ISIN AS CASEC_ID_CASEC_CAPCO,
                      CURRENCY.CODE AS CUR_ID_INS_CAPCO,
                      CASE
                         WHEN INSTRUMENT.IDENT_ASSETCLASS IN (1005, 1006)
                         THEN
                            REFINS.ISIN
                         ELSE
                            CAST (NULL AS VARCHAR2 (30))
                      END
                         AS INS_ID_INS_CAPCO,
                      CASE WHEN ACT.IDENT_ASSETCLASSTYPE = 10  THEN 2 ELSE 1 END AS TYP_BS,
                      CASE WHEN SUSP.IDENT_BLOCKINGREASON = 4 THEN 1
                     WHEN SUSP.IDENT_BLOCKINGREASON = 11 THEN 2
                     WHEN SUSP.IDENT_BLOCKINGREASON = 7 THEN 3
                     WHEN SUSP.IDENT_BLOCKINGREASON = 12 THEN 4
                     WHEN SUSP.IDENT_BLOCKINGREASON = 2 THEN 1001
                     WHEN SUSP.IDENT_BLOCKINGREASON = 1 THEN 1002
                     WHEN SUSP.IDENT_BLOCKINGREASON = 8 THEN 1012
                     WHEN SUSP.IDENT_BLOCKINGREASON = 6 THEN 1007
                     WHEN SUSP.IDENT_BLOCKINGREASON = 5 THEN 1008
                     WHEN SUSP.IDENT_BLOCKINGREASON = 9 THEN 1009
                     WHEN SUSP.IDENT_BLOCKINGREASON = 10 THEN 1009 END CODE_BR,
                      INSTRUMENT.ISSUEDATE AS DATE_ISSUED,INSTRUMENT.IDENT_INSTRUMENT,
                      INSTRUMENT.NOMINALVALUE
                    FROM INSTRUMENT@'||v_SchemaProd||' INSTRUMENT
                    LEFT JOIN CURRENCY@'||v_SchemaProd||' ON INSTRUMENT.IDENT_CURRENCY = CURRENCY.IDENT_CURRENCY
                    LEFT JOIN RIGHTSATTRIBUTES@'||v_SchemaProd||' RA ON INSTRUMENT.IDENT_RIGHTSATTRIBUTES = RA.IDENT_RIGHTSATTRIBUTES
                    LEFT JOIN (
                        SELECT SUSP.*,
                        ROW_NUMBER () OVER (PARTITION BY IDENT_INSTRUMENTEXT ORDER BY SUSP.IDENT_BLOCKINGREASON ASC, IDENT ASC) AS SUSPROW
                        FROM INSTRUMENTSUSPENSION@'||v_SchemaProd||' SUSP
                    ) SUSP ON INSTRUMENT.IDENT_INSTRUMENT = SUSP.IDENT_INSTRUMENTEXT AND SUSP.SUSPROW = 1
                    LEFT JOIN INSTRUMENT@'||v_SchemaProd||' REFINS ON RA.IDENT_UNDERLYINGINSTRUMENT = REFINS.IDENT_INSTRUMENT
                    LEFT JOIN ASSETCLASS@'||v_SchemaProd||' AC ON INSTRUMENT.IDENT_ASSETCLASS = AC.IDENT_ASSETCLASS
                    LEFT JOIN ASSETCLASSTYPE@'||v_SchemaProd||' ACT ON AC.IDENT_ASSETCLASSTYPE = ACT.IDENT_ASSETCLASSTYPE
                    LEFT JOIN ISSUEREXT@'||v_SchemaProd||' ISS ON INSTRUMENT.IDENT_ISSUER = ISS.IDENT_ISSUER
                    LEFT JOIN IDNISSUERCLASSIFICATION@'||v_SchemaProd||' IIC ON ISS.IDENT_ISSUERCLASSIFICATION = IIC.ID
                    LEFT JOIN CAINSTRUMENTDEBT@'||v_SchemaProd||' CAINSTR ON INSTRUMENT.IDENT_CAINSTRUMENT = CAINSTR.IDENT_CAINSTRUMENT
                    LEFT JOIN CODES CD ON CD.COL_NM = ''CODE_ACTI_SEC'' AND IIC.CODE = CD.CODE_ORDER
                    WHERE INSTRUMENT.IDENT_MASTER IS NULL';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_BASIC_SECURITIES', 'Y', 'Insert into IMP_STG_BASIC_SECURITIES');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_BASIC_SECURITIES DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_BASIC_SECURITIES',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_BASIC_SECURITIES with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_BASIC_SECURITIES with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_BASIC_SECURITIES

procedure IMP_STG_CURRENCIES
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_CURRENCIES DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_TOBA_CURRENCIES (ID_INS_CAPCO, 
                    LST_UPD_TS, CODE_STA, CODE_BASE_CURR, DSC_CURR, NBR_DECIMAL, 
                    CODE_BR)
                       SELECT IDENT_CURRENCY ID_INS_CAPCO,
                          LASTUPDATETIME AS LST_UPD_TS,
                          CASE
                             WHEN CURRENCY.IDENT_ACTIVATIONTYPE = 1 THEN 1
                             WHEN CURRENCY.IDENT_ACTIVATIONTYPE = 4 THEN 2
                             WHEN CURRENCY.IDENT_ACTIVATIONTYPE = 2 THEN 3
                          END
                             AS CODE_STA,
                          CODE AS CODE_BASE_CURR,
                          NAME AS DSC_CURR,
                          DECIMALS AS NBR_DECIMAL,
                          CAST (NULL AS NUMBER) CODE_BR
                     FROM CURRENCY@'||v_SchemaProd||'
                    WHERE IDENT_MASTER IS NULL';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_CURRENCIES', 'Y', 'Insert into IMP_STG_CURRENCIES');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_CURRENCIES DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_CURRENCIES',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_CURRENCIES with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_CURRENCIES with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_CURRENCIES

procedure IMP_STG_SECURITIES
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_SECURITIES DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_TOBA_SECURITIES (ID_INS_XCSD, 
                    LOCAL_CODE, LST_UPD_TS, CODE_STA, AMT_MIN_TRADE, CODE_BASE_SEC, 
                    CODE_PHYS, CODE_REG, CODE_SEC_RATE, DATE_ACTI, DAT_MAT, 
                    EXT_REF, SEC_DENM, SEC_DSC, SEC_NUM, TYP_SEC, 
                    AMT_ISSUED, CODE_ACTI_SEC, CASEC_ID_CASEC_XCSD, CODE_CURR, CODE_BR, 
                    ID_REGISTRAR_XCSD, ID_ISSUER_XCSD)
                       SELECT INSTR.IDENT_INSTRUMENT AS ID_INS_XCSD,
                          INSTR.NAME AS LOCAL_CODE,
                          INSTR.LASTUPDATETIME AS LST_UPD_TS,
                          CASE
                             WHEN INSTR.IDENT_ACTIVATIONTYPE = 1 THEN 1
                             WHEN INSTR.IDENT_ACTIVATIONTYPE = 4 THEN 2
                             WHEN INSTR.IDENT_ACTIVATIONTYPE = 2 THEN 3
                          END
                             AS CODE_STA,
                          INSTR.MINIMUMSETTLEMENTQUANTITY AS AMT_MIN_TRADE,
                          INSTR.NAME AS CODE_BASE_SEC,
                          CASE
                             WHEN INSTR.IDENT_INSTRUMENTFORM = 2 THEN 4
                             WHEN INSTR.IDENT_INSTRUMENTFORM = 3 THEN 1
                             WHEN INSTR.IDENT_INSTRUMENTFORM = 1 THEN 0
                          END
                             AS CODE_PHYS,
                          CASE WHEN INSTR.ISBEARER = 1 THEN 1 ELSE 2 END AS CODE_REG,
                          CAST (NULL AS NUMBER) AS CODE_SEC_RATE,
                          INSTR.ISSUEDATE AS DATE_ACTI,
                          INSTR.MATURITYDATE AS DAT_MAT,
                          CAST (NULL AS VARCHAR (10)) AS EXT_REF,
                          INSTR.SETTLEMENTLOT AS SEC_DENM,
                          INSTR.LONGNAME AS SEC_DSC,
                          CASE
                             WHEN ac.IDENT_ASSETCLASSTYPE != 5
                             THEN
                                INSTR.TOTALALLOWEDQUANTITY
                             ELSE
                                CAST (NULL AS NUMBER)
                          END
                             AS SEC_NUM,
                          CASE
                             WHEN INSTR.IDENT_ASSETCLASS IN (1001, 1029, 1030) THEN 2
                             WHEN INSTR.IDENT_ASSETCLASS IN (1002, 1028) THEN 1
                             WHEN INSTR.IDENT_ASSETCLASS = 1003 THEN 14
                             WHEN INSTR.IDENT_ASSETCLASS IN (1004, 1031) THEN 7
                             WHEN INSTR.IDENT_ASSETCLASS = 1005 THEN 4
                             WHEN INSTR.IDENT_ASSETCLASS = 1006 THEN 5
                             WHEN INSTR.IDENT_ASSETCLASS IN (1007, 1027) THEN 16
                             WHEN INSTR.IDENT_ASSETCLASS IN (1008, 1017) THEN 3
                             WHEN INSTR.IDENT_ASSETCLASS IN (1009, 1018, 1019) THEN 13
                             WHEN INSTR.IDENT_ASSETCLASS = 1010 THEN 11
                             WHEN INSTR.IDENT_ASSETCLASS = 1011 THEN 17
                             WHEN INSTR.IDENT_ASSETCLASS = 1012 THEN 8
                             WHEN INSTR.IDENT_ASSETCLASS = 1013 THEN 18
                             WHEN INSTR.IDENT_ASSETCLASS = 1014 THEN 9
                             WHEN INSTR.IDENT_ASSETCLASS = 1015 THEN 19
                             WHEN INSTR.IDENT_ASSETCLASS = 1016 THEN 15
                             WHEN INSTR.IDENT_ASSETCLASS = 1033 THEN 20
                             ELSE 0
                          END
                             AS TYP_SEC,
                          CASE
                             WHEN ac.IDENT_ASSETCLASSTYPE = 5 THEN INSTR.TOTALALLOWEDQUANTITY
                             ELSE CAST (NULL AS NUMBER)
                          END
                             AS AMT_ISSUED,
                          CAST (NULL AS NUMBER) AS CODE_ACTI_SEC,
                          INSTR.IDENT_INSTRUMENT AS CASEC_ID_CASEC_XCSD,
                          cu.CODE AS CODE_CURR,
                          SUSP.IDENT_BLOCKINGREASON AS CODE_BR,
                          INSTR.IDENT_ISSUERCSD AS ID_REGISTRAR_XCSD,
                          INSTR.IDENT_ISSUER AS ID_ISSUER_XCSD
                     FROM INSTRUMENT@'||v_SchemaProd||' INSTR, ASSETCLASS@'||v_SchemaProd||' ac, ASSETCLASSTYPE@'||v_SchemaProd||' act, CURRENCY@'||v_SchemaProd||' cu,
                             (SELECT SUSP.*,
                                            ROW_NUMBER ()
                                            OVER (PARTITION BY IDENT_INSTRUMENTEXT
                                                  ORDER BY IDENT ASC)
                                               AS SUSPROW
                                       FROM INSTRUMENTSUSPENSION@'||v_SchemaProd||' SUSP) SUSP
                    WHERE INSTR.IDENT_MASTER IS NULL AND ACT.IDENT_ASSETCLASSTYPE != 10
                    AND INSTR.IDENT_ASSETCLASS = ac.IDENT_ASSETCLASS(+)
                    AND ac.IDENT_ASSETCLASSTYPE = act.IDENT_ASSETCLASSTYPE(+)
                    AND INSTR.IDENT_CURRENCY = cu.IDENT_CURRENCY(+) 
                    AND  INSTR.IDENT_INSTRUMENT = SUSP.IDENT_INSTRUMENTEXT(+)
                    AND  SUSP.SUSPROW(+) = 1';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_SECURITIES', 'Y', 'Insert into IMP_STG_SECURITIES');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_SECURITIES DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_SECURITIES',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_SECURITIES with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_SECURITIES with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_SECURITIES

procedure IMP_STG_WARRANTS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_WARRANTS DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_TOBA_WARRANTS (ID_INS_XCSD, 
                    LOCAL_CODE, LST_UPD_TS, CODE_STA, AMT_MIN_TRADE, CODE_BASE_SEC, 
                    CODE_PHYS, CODE_REG, CODE_SEC_RATE, DATE_ACTI, DAT_MAT, 
                    EXT_REF, SEC_DENM, SEC_DSC, SEC_NUM, TYP_SEC, 
                    EXE_PRI, TYP_OPT, TYP_EXE, WAR_RATIO, INS_ID_INS_XCSD, 
                    CASEC_ID_CASEC_XCSD, CODE_CURR, CODE_BR, ID_REGISTRAR_XCSD, ID_ISSUER_XCSD)
                      SELECT INSTR.ident_instrument AS ID_INS_XCSD,
                          INSTR.NAME AS LOCAL_CODE,
                          INSTR.LASTUPDATETIME AS LST_UPD_TS,
                          CASE
                             WHEN INSTR.IDENT_ACTIVATIONTYPE = 1 THEN 1
                             WHEN INSTR.IDENT_ACTIVATIONTYPE = 4 THEN 2
                             WHEN INSTR.IDENT_ACTIVATIONTYPE = 2 THEN 3
                          END
                             AS CODE_STA,
                          INSTR.MINIMUMSETTLEMENTQUANTITY AS AMT_MIN_TRADE,
                          INSTR.NAME AS CODE_BASE_SEC,
                          CASE
                             WHEN INSTR.IDENT_INSTRUMENTFORM = 2 THEN 4
                             WHEN INSTR.IDENT_INSTRUMENTFORM = 3 THEN 1
                             WHEN INSTR.IDENT_INSTRUMENTFORM = 1 THEN 0
                          END
                             AS CODE_PHYS,
                          CASE WHEN INSTR.ISBEARER = 1 THEN 1 ELSE 2 END AS CODE_REG,
                          CAST (NULL AS NUMBER) AS CODE_SEC_RATE,
                          INSTR.ISSUEDATE AS DATE_ACTI,
                          INSTR.MATURITYDATE AS DAT_MAT,
                          CAST (NULL AS VARCHAR2 (30)) AS EXT_REF,
                          INSTR.SETTLEMENTLOT AS SEC_DENM,
                          INSTR.LONGNAME AS SEC_DSC,
                          INSTR.TOTALALLOWEDQUANTITY AS SEC_NUM,
                          CASE
                             WHEN INSTR.IDENT_ASSETCLASS IN (1001, 1029, 1030) THEN 2
                             WHEN INSTR.IDENT_ASSETCLASS IN (1002, 1028) THEN 1
                             WHEN INSTR.IDENT_ASSETCLASS = 1003 THEN 14
                             WHEN INSTR.IDENT_ASSETCLASS IN (1004, 1031) THEN 7
                             WHEN INSTR.IDENT_ASSETCLASS = 1005 THEN 4
                             WHEN INSTR.IDENT_ASSETCLASS = 1006 THEN 5
                             WHEN INSTR.IDENT_ASSETCLASS IN (1007, 1027) THEN 16
                             WHEN INSTR.IDENT_ASSETCLASS IN (1008, 1017) THEN 3
                             WHEN INSTR.IDENT_ASSETCLASS IN (1009, 1018, 1019) THEN 13
                             WHEN INSTR.IDENT_ASSETCLASS = 1010 THEN 11
                             WHEN INSTR.IDENT_ASSETCLASS = 1011 THEN 17
                             WHEN INSTR.IDENT_ASSETCLASS = 1012 THEN 8
                             WHEN INSTR.IDENT_ASSETCLASS = 1013 THEN 18
                             WHEN INSTR.IDENT_ASSETCLASS = 1014 THEN 9
                             WHEN INSTR.IDENT_ASSETCLASS = 1015 THEN 19
                             WHEN INSTR.IDENT_ASSETCLASS = 1016 THEN 15
                             WHEN INSTR.IDENT_ASSETCLASS = 1033 THEN 20
                             ELSE 0
                          END
                             AS TYP_SEC,
                          ra.EXERCISEPRICE AS EXE_PRI,
                          CASE WHEN ra.IDENT_WARRANTTYPE = 0 THEN 1 ELSE 1 END AS TYP_OPT,
                          CASE WHEN ra.IDENT_EXERCISESTYLE = 1 THEN 2 ELSE 1 END AS TYP_EXE,
                          NVL (ra.UNDERLYINGASSETS, 1) AS WAR_RATIO,
                          ra.IDENT_UNDERLYINGINSTRUMENT AS INS_ID_INS_XCSD,
                          INSTR.IDENT_INSTRUMENT AS CASEC_ID_CASEC_XCSD,
                          cu.CODE AS CODE_CURR,
                          SUSP.IDENT_BLOCKINGREASON AS CODE_BR,
                          INSTR.IDENT_ISSUERCSD AS ID_REGISTRAR_XCSD,
                          INSTR.IDENT_ISSUER AS ID_ISSUER_XCSD
                        FROM  INSTRUMENT@'||v_SchemaProd||' INSTR,
                                   (SELECT SUSP.*,
                                           ROW_NUMBER ()
                                           OVER (PARTITION BY IDENT_INSTRUMENTEXT
                                                 ORDER BY IDENT ASC)
                                              AS SUSPROW
                                      FROM INSTRUMENTSUSPENSION@'||v_SchemaProd||' SUSP) SUSP,
                                ASSETCLASS@'||v_SchemaProd||' ac, ASSETCLASSTYPE@'||v_SchemaProd||' act,
                          RIGHTSATTRIBUTES@'||v_SchemaProd||' ra,
                          CURRENCY@'||v_SchemaProd||' cu
                        WHERE     INSTR.IDENT_MASTER IS NULL
                          AND cu.IDENT_CURRENCY = INSTR.IDENT_CURRENCY
                          AND act.IDENT_ASSETCLASSTYPE = 10
                          AND INSTR.IDENT_RIGHTSATTRIBUTES = ra.IDENT_RIGHTSATTRIBUTES
                          AND SUSP.SUSPROW(+) = 1
                          AND INSTR.IDENT_INSTRUMENT = SUSP.IDENT_INSTRUMENTEXT(+)
                          AND ac.IDENT_ASSETCLASS = INSTR.IDENT_ASSETCLASS(+) 
                          AND ac.IDENT_ASSETCLASSTYPE = act.IDENT_ASSETCLASSTYPE(+)';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_WARRANTS', 'Y', 'Insert into IMP_STG_WARRANTS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_WARRANTS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_WARRANTS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_WARRANTS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_WARRANTS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_WARRANTS

procedure IMP_STG_CAMEMBERS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_CAMEMBERS DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_TOBA_CAMEMBERS (ID_CAM, 
                        ACCT_ID_ACCT, LST_UPD_TS)
                   SELECT PART.CODE AS ID_CAM,
                      AC.ACCOUNTIDENTIFIERVALUE AS ACCT_ID_ACCT,
                      NVL (AC.REVISION_EFFECTIVE_FROM, AC.LASTUPDATETIME) AS LST_UPD_TS
                 FROM PARTICIPANT@'||v_SchemaProd||' PART,
                         ACCOUNTCOMPOSITE@'||v_SchemaProd||' AC,
                         ACCOUNTCOMPOSITEEXT@'||v_SchemaProd||' ACE
                WHERE     AC.IDENT_OPERATOR = PART.IDENT_STAKEHOLDER
                      AND AC.IDENT_MASTER IS NULL
                      AND ACE.CORPORATEACTION = 1
                      AND AC.IDENT_ACCOUNTCOMPOSITE = ACE.IDENT_ACCOUNTCOMPOSITE(+)';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_CAMEMBERS', 'Y', 'Insert into IMP_STG_CAMEMBERS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_CAMEMBERS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_CAMEMBERS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_CAMEMBERS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_CAMEMBERS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_WARRANTS

procedure IMP_STG_APPROVALREQUEST
is
begin
    
    select to_char(get_next_bus_date(sysdate,-1),'DD-MON-YYYY') into tglDesc from dual;  

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_APPROVALREQUEST DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_TOBA_APPROVALREQUEST (IDENT_REQUEST, 
                            REQUESTDATE, FIRSTAPPROVAL, IDENT_REQUESTER, IDENT_FIRSTAPPROVER, PERMISSIONFUNCTION, 
                            METHODNAME, BEANNAME, BEANIDENTIFIER, EVENT, IDENT_APPROVALSTATE, IDENT_FINAL_APPROVALSTATE, 
                            IDENT_REVISION, NUMBEROFCONFIRMATIONSREQUIRED, APPROVALCOMMENT, DATAOWNER, ISENTITYREVISIONED, 
                            FINALCSDAPPROVALREQUIRED, IDENT_APPROVER, IDENT_REVISION_MASTER, ENTITYNAME, ENTITYLONGNAME, 
                            SYSCREATED, SYSMODIFIED, IDENT_REVISION_LIFECYCLEENTITY)
                   SELECT IDENT_REQUEST, 
                            REQUESTDATE, FIRSTAPPROVAL, IDENT_REQUESTER, IDENT_FIRSTAPPROVER, PERMISSIONFUNCTION, 
                            METHODNAME, BEANNAME, BEANIDENTIFIER, EVENT, IDENT_APPROVALSTATE, 
                            IDENT_FINAL_APPROVALSTATE, IDENT_REVISION, NUMBEROFCONFIRMATIONSREQUIRED, APPROVALCOMMENT, 
                            DATAOWNER, ISENTITYREVISIONED, FINALCSDAPPROVALREQUIRED, IDENT_APPROVER, IDENT_REVISION_MASTER, 
                            ENTITYNAME, ENTITYLONGNAME, SYSCREATED, SYSMODIFIED, IDENT_REVISION_LIFECYCLEENTITY
                 FROM APPROVALREQUEST@'||v_SchemaProd||'
                 WHERE TRUNC(REQUESTDATE) = '''||tglDesc||'''
                 AND PERMISSIONFUNCTION IN (''SecuritiesAccount'', ''CashAccount'')';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_APPROVALREQUEST', 'Y', 'Insert into IMP_STG_APPROVALREQUEST');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_TOBA_APPROVALREQUEST DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_APPROVALREQUEST',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_APPROVALREQUEST with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_APPROVALREQUEST with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_APPROVALREQUEST

procedure IMP_STG_ACCOUNT_INSTRUCTIONS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_ACCOUNT_INSTRUCTIONS DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_ACCOUNT_INSTRUCTIONS (EVENT, 
                ACCOUNTIDENTIFIERVALUE, MEMCODE, USERNAME, REQUESTDATE, APPROVALSTATE_DSC, APPROVALSTATE)
                   select EVENT, ACCOUNTIDENTIFIERVALUE, MEMCODE, USERNAME, REQUESTDATE,IDENT_FINAL_APPROVALSTATE_DSC APPROVALSTATE_DSC,IDENT_APPROVALSTATE APPROVALSTATE
                    from 
                    (
                        SELECT 
                            CASE WHEN EVENT = ''CREATE'' THEN ''ACCOUNT CREATE''
                                WHEN EVENT = ''UPDATE'' AND ACCOUNTSTATUS = 2 THEN ''ACCOUNT CLOSE''
                                WHEN EVENT = ''UPDATE'' AND ACCOUNTSTATUS <> 2 THEN ''ACCOUNT UPDATE''
                            ELSE ''ACCOUNT'' || EVENT ||'' '' || APPROVALNUMBER END AS EVENT
                            , ACCOUNTIDENTIFIERVALUE
                            , CODE AS MEMCODE
                            , USERNAME
                            , REQUESTDATE
                            , IDENT_FINAL_APPROVALSTATE_DSC
                            , IDENT_APPROVALSTATE
                        FROM (
                            SELECT AR.PERMISSIONFUNCTION, AR.EVENT
                            , 0 AS APPROVALNUMBER
                            , AC.ACCOUNTIDENTIFIERVALUE, AC.IDENT_ACTIVATIONTYPE ACCOUNTSTATUS
                            , P.CODE, UA.USERNAME, AR.REQUESTDATE
                            , case when APS.NAME is null then APS2.NAME else APS.NAME end IDENT_FINAL_APPROVALSTATE_DSC
                            , case when APS.NAME is null then AR.IDENT_APPROVALSTATE else AR.IDENT_FINAL_APPROVALSTATE end IDENT_APPROVALSTATE
                            FROM TMPSCHEMA.STG_TOBA_APPROVALREQUEST AR
                            LEFT JOIN USERACCOUNT@'||v_SchemaProd||' UA ON AR.IDENT_REQUESTER = UA.IDENT_USERACCOUNT
                            LEFT JOIN ACCOUNTCOMPOSITE@'||v_SchemaProd||' AC ON AR.BEANIDENTIFIER = AC.ACCOUNTIDENTIFIERVALUE
                            LEFT JOIN PARTICIPANT@'||v_SchemaProd||' P ON UA.IDENT_STAKEHOLDER = P.IDENT_STAKEHOLDER
                            LEFT JOIN APPROVALSTATE@'||v_SchemaProd||' APS ON AR.IDENT_FINAL_APPROVALSTATE = APS.IDENT_APPROVALSTATE
                            LEFT JOIN APPROVALSTATE@'||v_SchemaProd||' APS2 ON AR.IDENT_APPROVALSTATE = APS2.IDENT_APPROVALSTATE
                        WHERE 1=1
                            AND AR.PERMISSIONFUNCTION IN (
                                ''SecuritiesAccount''
                                , ''CashAccount''
                            )
                            AND AR.IDENT_APPROVALSTATE = 1
                            AND AC.IDENT_MASTER IS NULL    
                        UNION
                        SELECT PERMISSIONFUNCTION,
                        EVENT,
                        APPROVALNUMBER,
                        ACCOUNTIDENTIFIERVALUE,
                        ACCOUNTSTATUS,
                        CODE,
                        USERNAME,
                        REQUESTDATE,
                        IDENT_FINAL_APPROVALSTATE_DSC,
                        IDENT_APPROVALSTATE
                        FROM (
                            SELECT AR.PERMISSIONFUNCTION
                                , CASE WHEN AR.IDENT_APPROVALSTATE = 5 THEN ''APPROVE''
                                  WHEN AR.IDENT_APPROVALSTATE = 6 THEN ''REJECT''
                                  ELSE AR.EVENT END EVENT
                                , 1 AS APPROVALNUMBER
                                , AC.ACCOUNTIDENTIFIERVALUE, AC.IDENT_ACTIVATIONTYPE ACCOUNTSTATUS
                                , P.CODE, UA.USERNAME, AR.REQUESTDATE
                                , case when APS.NAME is null then APS2.NAME else APS.NAME end IDENT_FINAL_APPROVALSTATE_DSC
                                , case when APS.NAME is null then AR.IDENT_APPROVALSTATE else AR.IDENT_FINAL_APPROVALSTATE end IDENT_APPROVALSTATE
                            FROM TMPSCHEMA.STG_TOBA_APPROVALREQUEST AR
                            LEFT JOIN USERACCOUNT@'||v_SchemaProd||' UA ON AR.IDENT_REQUESTER = UA.IDENT_USERACCOUNT
                            LEFT JOIN ACCOUNTCOMPOSITE@'||v_SchemaProd||' AC ON AR.BEANIDENTIFIER = AC.ACCOUNTIDENTIFIERVALUE
                            LEFT JOIN PARTICIPANT@'||v_SchemaProd||' P ON UA.IDENT_STAKEHOLDER = P.IDENT_STAKEHOLDER
                            LEFT JOIN APPROVALSTATE@'||v_SchemaProd||' APS ON AR.IDENT_FINAL_APPROVALSTATE = APS.IDENT_APPROVALSTATE
                            LEFT JOIN APPROVALSTATE@'||v_SchemaProd||' APS2 ON AR.IDENT_APPROVALSTATE = APS2.IDENT_APPROVALSTATE
                        WHERE 1=1
                            AND AR.PERMISSIONFUNCTION IN (
                               ''SecuritiesAccount''
                                , ''CashAccount''
                            )
                            AND AR.IDENT_APPROVALSTATE IN (5,6)
                            AND NUMBEROFCONFIRMATIONSREQUIRED = 1
                            AND AC.IDENT_MASTER IS NULL
                        UNION
                        SELECT AR.PERMISSIONFUNCTION
                            , CASE WHEN AR.IDENT_APPROVALSTATE = 5 THEN ''APPROVE'' 
                              WHEN AR.IDENT_APPROVALSTATE = 6 THEN ''REJECT''
                              ELSE AR.EVENT END EVENT
                            , ROW_NUMBER() OVER (PARTITION BY PERMISSIONFUNCTION, AR.EVENT, ACCOUNTIDENTIFIERVALUE, NUMBEROFCONFIRMATIONSREQUIRED ORDER BY REQUESTDATE ASC) AS APPROVALNUMBER
                            , AC.ACCOUNTIDENTIFIERVALUE, AC.IDENT_ACTIVATIONTYPE ACCOUNTSTATUS
                            , P.CODE, UA.USERNAME, AR.REQUESTDATE
                            , case when APS.NAME is null then APS2.NAME else APS.NAME end IDENT_FINAL_APPROVALSTATE_DSC
                            , case when APS.NAME is null then AR.IDENT_APPROVALSTATE else AR.IDENT_FINAL_APPROVALSTATE end IDENT_APPROVALSTATE
                        FROM TMPSCHEMA.STG_TOBA_APPROVALREQUEST AR
                            LEFT JOIN USERACCOUNT@'||v_SchemaProd||' UA ON AR.IDENT_REQUESTER = UA.IDENT_USERACCOUNT
                            LEFT JOIN ACCOUNTCOMPOSITE@'||v_SchemaProd||' AC ON AR.BEANIDENTIFIER = AC.ACCOUNTIDENTIFIERVALUE
                            LEFT JOIN PARTICIPANT@'||v_SchemaProd||' P ON UA.IDENT_STAKEHOLDER = P.IDENT_STAKEHOLDER
                            LEFT JOIN APPROVALSTATE@'||v_SchemaProd||' APS ON AR.IDENT_FINAL_APPROVALSTATE = APS.IDENT_APPROVALSTATE
                            LEFT JOIN APPROVALSTATE@'||v_SchemaProd||' APS2 ON AR.IDENT_APPROVALSTATE = APS2.IDENT_APPROVALSTATE
                        WHERE 1=1
                            AND AR.PERMISSIONFUNCTION IN (
                            ''SecuritiesAccount''
                            , ''CashAccount''
                            )
                            AND AR.IDENT_APPROVALSTATE IN (5,6)
                            AND NUMBEROFCONFIRMATIONSREQUIRED = 2
                            AND AC.IDENT_MASTER IS NULL
                        )
                    ))';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_ACCOUNT_INSTRUCTIONS', 'Y', 'Insert into IMP_STG_ACCOUNT_INSTRUCTIONS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_ACCOUNT_INSTRUCTIONS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_ACCOUNT_INSTRUCTIONS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_ACCOUNT_INSTRUCTIONS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_ACCOUNT_INSTRUCTIONS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_APPROVALREQUEST

procedure IMP_STG_INVS_SOURCEFUND
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_SOURCEFUND DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_INVS_SOURCEFUND (IDENT_INVESTOR, 
                            FUNDSOURCE, FUNDSOURCE_DSC, RNK)
                    SELECT IDENT_INVESTOR, ID FUNDSOURCE, NARRATIVES FUNDSOURCE_DSC, 1 RNK
                    FROM (
                    SELECT IDENT_INVESTOR
                    , LISTAGG(DECODE (CODES.ID, 
                           1,2,
                           2,3,
                           3,4,
                           4,5,
                           5,6,
                           6,7,
                           7,8,
                           8,9,
                           9,10,
                           10,11,
                           11,12,
                           12,13,
                           13,1), '','') WITHIN GROUP (ORDER BY CODES.ID) ID
                    , LISTAGG(SOURCES.NARRATIVE, '','') WITHIN GROUP (ORDER BY CODES.ID) NARRATIVES
                    FROM IDNFUNDSOURCESINDIVIDUAL@'||v_SchemaProd||' SOURCES
                    JOIN IDNFUNDSOURCEINDIVIDUAL@'||v_SchemaProd||' CODES ON SOURCES.IDENT_FUNDSOURCEINDIVIDUAL = CODES.ID
                    GROUP BY IDENT_INVESTOR
                    UNION ALL
                    SELECT IDENT_INVESTOR
                    , LISTAGG(DECODE (CODES.ID,
                                 1,3,
                                 2,4,
                                 3,8,
                                 4,9,
                                 5,10,
                                 6,11,
                                 7,12,
                                 8,13,
                                 9,1), '','') WITHIN GROUP (ORDER BY CODES.ID) ID
                    , LISTAGG(SOURCES.NARRATIVE, '','') WITHIN GROUP (ORDER BY CODES.ID) NARRATIVES
                    FROM IDNFUNDSOURCE@'||v_SchemaProd||' SOURCES
                    JOIN IDNFUNDSOURCECOMPANY@'||v_SchemaProd||' CODES ON SOURCES.IDENT_FUNDSOURCECOMPANY = CODES.ID
                    GROUP BY IDENT_INVESTOR
                    )';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_SOURCEFUND', 'Y', 'Insert into IMP_STG_INVS_SOURCEFUND');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_INVS_SOURCEFUND DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_SOURCEFUND',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_SOURCEFUND with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_INVS_SOURCEFUND with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_INVS_SOURCEFUND

procedure IMP_STG_INVS_CONTACTDETAILS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_CONTACTDETAIL DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_INVS_CONTACTDETAIL (ID, 
                            IDENT_INVESTOR, PHONENUMBER, MOBILENUMBER, FAXNUMBER, EMAIL, 
                            PUBLICDETAILS, WEBSITE, POSITION, SYSCREATED, SYSMODIFIED, 
                            RNK)
                        select
                            ID, IDENT_INVESTOR, PHONENUMBER,MOBILENUMBER,FAXNUMBER,EMAIL,
                            PUBLICDETAILS,WEBSITE,POSITION,SYSCREATED,SYSMODIFIED,
                            ROW_NUMBER() OVER (PARTITION BY IDENT_INVESTOR ORDER BY ID ASC) RNK
                        from CONTACTDETAILS@'||v_SchemaProd||'
                        where IDENT_INVESTOR is not null';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_CONTACTDETAILS', 'Y', 'Insert into IMP_STG_INVS_CONTACTDETAILS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_INVS_CONTACTDETAILS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_CONTACTDETAILS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_CONTACTDETAILS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_INVS_CONTACTDETAILS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_INVS_CONTACTDETAILS

procedure IMP_STG_ISSUER_CONTACTDETAIL
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_ISSUER_CONTACTDETAIL DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_ISSUER_CONTACTDETAIL (ID, 
                            IDENT_ISSUER, CONTACTNAME, TITLE, PHONENUMBER, MOBILENUMBER, 
                            FAXNUMBER, EMAIL, PUBLICDETAILS, WEBSITE, POSITION, 
                            SYSCREATED, SYSMODIFIED, RNK)
                        select
                            ID, IDENT_ISSUER, CONTACTNAME, TITLE, PHONENUMBER,MOBILENUMBER,FAXNUMBER,EMAIL,
                            PUBLICDETAILS,WEBSITE,POSITION,SYSCREATED,SYSMODIFIED,
                            ROW_NUMBER() OVER (PARTITION BY IDENT_ISSUER ORDER BY ID ASC) RNK
                        from CONTACTDETAILS@'||v_SchemaProd||'
                        where IDENT_ISSUER is not null';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_ISSUER_CONTACTDETAIL', 'Y', 'Insert into IMP_STG_ISSUER_CONTACTDETAIL');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_ISSUER_CONTACTDETAIL DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_ISSUER_CONTACTDETAIL',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_ISSUER_CONTACTDETAIL with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_ISSUER_CONTACTDETAIL with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_ISSUER_CONTACTDETAIL

procedure IMP_STG_PARTI_CONTACTDETAIL
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_PARTICIPANT_CONTACTDETAIL DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_PARTICIPANT_CONTACTDETAIL (ID, 
                            IDENT_PARTICIPANT, CONTACTNAME, TITLE, PHONENUMBER, MOBILENUMBER, 
                            FAXNUMBER, EMAIL, PUBLICDETAILS, WEBSITE, POSITION, 
                            SYSCREATED, SYSMODIFIED, RNK)
                        select
                            ID, IDENT_PARTICIPANT, CONTACTNAME, TITLE, PHONENUMBER,MOBILENUMBER,FAXNUMBER,EMAIL,
                            PUBLICDETAILS,WEBSITE,POSITION,SYSCREATED,SYSMODIFIED,
                            ROW_NUMBER() OVER (PARTITION BY IDENT_PARTICIPANT ORDER BY ID ASC) RNK
                        from CONTACTDETAILS@'||v_SchemaProd||'
                        where IDENT_PARTICIPANT is not null';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_PARTI_CONTACTDETAIL', 'Y', 'Insert into IMP_STG_PARTI_CONTACTDETAIL');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_PARTI_CONTACTDETAIL DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_PARTI_CONTACTDETAIL',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_PARTI_CONTACTDETAIL with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_PARTI_CONTACTDETAIL with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_PARTI_CONTACTDETAIL

procedure IMP_STG_INVS_OBJCTV
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_INVMT_OBJCTV DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_INVS_INVMT_OBJCTV (IDENT_INVESTOR, 
                            IDENT_INVESTMENTOBJECTIVE, INVESTMENTOBJECTIVE_DSC, RNK)
                    SELECT A.IDENT_INVESTOR, 
                    LISTAGG(DECODE(A.IDENT_INVESTMENTOBJECTIVE, 5,1,1,2,2,3,3,4,4,5), '','') WITHIN GROUP (ORDER BY A.IDENT_INVESTMENTOBJECTIVE) IDENT_INVESTMENTOBJECTIVE,
                    LISTAGG(AB.DESCRIPTION, '','') WITHIN GROUP (ORDER BY AB.ID) INVESTMENTOBJECTIVE_DSC
                    , 1 RNK
                    FROM IDNINVESTMENTOBJECTIVES@TOBA A
                    LEFT JOIN IDNINVESTMENTOBJECTIVE@TOBA AB ON A.IDENT_INVESTMENTOBJECTIVE = AB.ID
                    GROUP BY IDENT_INVESTOR';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_OBJCTV', 'Y', 'Insert into IMP_STG_INVS_OBJCTV');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_INVS_OBJCTV DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_OBJCTV',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_OBJCTV with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_INVS_OBJCTV with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_INVS_OBJCTV

procedure IMP_STG_INVS_ADDRESS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_ADDRESS DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_INVS_ADDRESS (IDENT_ADDRESS, 
                        IDENT_INVESTOR, IDENT_ADDRESSTYPE, ADDRESSTYPE_DSC, ADDRESS1, ADDRESS2, 
                        ADDRESS3, POSTALCODE, CITY_CODE, CITY, CITY_FREE, PROVINCE_CODE, PROVINCE,
                        PROVINCE_FREE, COUNTRY, URL, SYSCREATED, SYSMODIFIED)
                    select
                        AX.IDENT_ADDRESS, AX.IDENT_INVESTOR,
                        AX.IDENT_ADDRESSTYPE, AX2.NAME ADDRESSTYPE_DSC, AX.ADDRESS1, AX.ADDRESS2, AX.ADDRESS3, AX.POSTALCODE, IC.CODE CITY_CODE,
                        IC.NAME CITY, DECODE (IC.CODE,NULL,AX.CITY,NULL) CITY_FREE, IP.NUMERIC_CODE PROVINCE_CODE, IP.NAME PROVINCE,
                        DECODE (IP.NUMERIC_CODE,NULL,AX.STATEPROVINCE,NULL) PROVINCE_FREE,
                        ax.COUNTRY, ax.URL,ax.SYSCREATED, ax.SYSMODIFIED
                    FROM 
                        address@'||v_SchemaProd||' ax,
                        ADDRESSTYPE@'||v_SchemaProd||'  AX2,
                        IDNCITY@'||v_SchemaProd||'  IC,
                        IDNPROVINCE@'||v_SchemaProd||'  IP
                    where
                        AX.IDENT_INVESTOR IS NOT NULL
                        AND AX.IDENT_ADDRESSTYPE = AX2.IDENT_ADDRESSTYPE(+)
                        AND AX.CITY = IC.NAME(+)
                        AND IC.IDENT_PROVINCE = IP.ID(+)';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_ADDRESS', 'Y', 'Insert into IMP_STG_INVS_ADDRESS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_INVS_ADDRESS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_ADDRESS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_ADDRESS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_INVS_ADDRESS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_INVS_ADDRESS

procedure IMP_STG_INVS_DIRECTSID
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_DIRECTSID DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_INVS_DIRECTSID (INV_CODE, INV_ACCT, DIRECT_SID)
                    SELECT
                        INV.CODE INV_CODE, AC.ACCOUNTIDENTIFIERVALUE INV_ACCT, SID.IDENTIFIER DIRECT_SID
                    FROM 
                        ACCOUNTCOMPOSITE@'||v_SchemaProd||' AC
                        JOIN INVESTOR@'||v_SchemaProd||' INV ON AC.IDENT_HOLDER = INV.IDENT_STAKEHOLDER
                        JOIN INVESTOREXT@'||v_SchemaProd||' IXT ON INV.IDENT_STAKEHOLDER = IXT.IDENT_INVESTOR
                        JOIN ACCOUNTRELATIONSHIP@'||v_SchemaProd||' IND ON AC.IDENT_ACCOUNTCOMPOSITE = IND.IDENT_ACCOUNTCOMPOSITE AND IND.IDENT_ACCOUNTRELATIONSHIPTYPE = 1
                        JOIN ACCOUNTRELATIONSHIP@'||v_SchemaProd||' DIR ON IND.IDENT_ACCOUNTCOMPOSITE = DIR.IDENT_ACCOUNTCOMPOSITE AND DIR.IDENT_ACCOUNTRELATIONSHIPTYPE = 4
                        JOIN STAKEHOLDERIDENTIFIER@'||v_SchemaProd||' SID ON DIR.IDENT_INVESTOR = SID.IDENT_INVESTOR AND SID.IDENT_STAKEHOLDERIDTYPE = 5
                    WHERE 
                        INV.IDENT_MASTER IS NULL
                        AND AC.IDENT_MASTER IS NULL';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_DIRECTSID', 'Y', 'Insert into IMP_STG_INVS_DIRECTSID');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_INVS_DIRECTSID DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_DIRECTSID',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_DIRECTSID with error '||ERR_MSG);

      COMMIT;
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_INVS_DIRECTSID with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_DIRECTSID

procedure IMP_STG_INVS_DOCS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_DOCS DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_INVS_DOCS
                           (IDENT_INVESTOREXT, IDENT_DOCUMENTDATA, RNK, IDENT_DOCUMENTTYPE, DOCUMENTTYPE_DSC, 
                           VALIDFROM, VALIDTO, STATUS, EXTKEY, LINKEDCAEVENTID, DOCUMENTID, DOCUMENTNAME, 
                           SYSCREATED, SYSMODIFIED)
                       select 
                            ax.IDENT_INVESTOREXT, ax.IDENT_DOCUMENTDATA,
                            ROW_NUMBER() OVER (PARTITION BY ax.IDENT_INVESTOREXT ORDER BY ax.SYSCREATED ASC) RNK,
                            ax2.IDENT_DOCUMENTTYPE, ax3.NAME DOCUMENTTYPE_DSC, ax2.VALIDFROM, ax2.VALIDTO, ax2.STATUS,
                            ax2.EXTKEY, ax2.LINKEDCAEVENTID, ax2.DOCUMENTID, ax2.DOCUMENTNAME, ax.SYSCREATED, ax2.SYSMODIFIED
                        from 
                            INVESTOREXTDOCUMENTDATA@'||v_SchemaProd||' ax,
                            DOCUMENTDATA@'||v_SchemaProd||' ax2,
                            DOCUMENTTYPE@'||v_SchemaProd||' ax3
                        where 
                            ax.IDENT_DOCUMENTDATA  = ax2.IDENT_DOCUMENTDATA
                            and ax2.IDENT_DOCUMENTTYPE = ax3.IDENT_DOCUMENTTYPE';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_DOCS', 'Y', 'Insert into IMP_STG_INVS_DOCS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_INVS_DOCS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_DOCS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_DOCS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_INVS_DOCS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_INVS_DOCS

procedure IMP_STG_INVS_IDENTIFIER
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_IDENTIFIER DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_IDENTIFIER
                SELECT IVS.IDENT_STAKEHOLDER IDENT_INVESTOR
                , SID.IDENTIFIER SID, BIC.IDENTIFIER BIC
                , NPWP.IDENTIFIER NPWP, NPWP.EXPIRYDATE NPWP_EXPDATE, NPWP.REGISTRATIONDATE NPWP_REGDATE
                , PASSPORT.IDENTIFIER PASSPORT, PASSPORT.EXPIRYDATE PASSPORT_EXPDATE, PASSPORT.REGISTRATIONDATE PASSPORT_REGDATE
                , KTP.IDENTIFIER KTP, KTP.EXPIRYDATE KTP_EXPDATE, KTP.REGISTRATIONDATE KTP_REGDATE
                , KITAS.IDENTIFIER KITAS, KITAS.EXPIRYDATE KITAS_EXPDATE, KITAS.REGISTRATIONDATE KITAS_REGDATE
                , BUSREG.IDENTIFIER BUSREG, BUSREG.EXPIRYDATE BUSREG_EXPDATE, BUSREG.REGISTRATIONDATE BUSREG_REGDATE
                FROM INVESTOR@'||v_SchemaProd||' IVS
                LEFT JOIN (
                SELECT IDENT_INVESTOR, IDENTIFIER, SI.EXPIRYDATE, SI.REGISTRATIONDATE
                FROM STAKEHOLDERIDENTIFIER@'||v_SchemaProd||' SI 
                WHERE IDENT_STAKEHOLDERIDTYPE = 5 -- SID
                ) SID ON IVS.IDENT_STAKEHOLDER = SID.IDENT_INVESTOR
                LEFT JOIN (
                SELECT IDENT_INVESTOR, IDENTIFIER, SI.EXPIRYDATE, SI.REGISTRATIONDATE 
                FROM STAKEHOLDERIDENTIFIER@'||v_SchemaProd||' SI
                WHERE IDENT_STAKEHOLDERIDTYPE = 6 -- NPWP
                ) NPWP ON IVS.IDENT_STAKEHOLDER = NPWP.IDENT_INVESTOR
                LEFT JOIN (
                SELECT IDENT_INVESTOR, IDENTIFIER, SI.EXPIRYDATE, SI.REGISTRATIONDATE 
                FROM STAKEHOLDERIDENTIFIER@'||v_SchemaProd||' SI
                WHERE IDENT_STAKEHOLDERIDTYPE = 7 -- PASSPORT
                 ) PASSPORT ON IVS.IDENT_STAKEHOLDER = PASSPORT.IDENT_INVESTOR
                LEFT JOIN ( 
                SELECT IDENT_INVESTOR, IDENTIFIER, SI.EXPIRYDATE, SI.REGISTRATIONDATE
                FROM STAKEHOLDERIDENTIFIER@'||v_SchemaProd||' SI
                WHERE IDENT_STAKEHOLDERIDTYPE = 8 -- KTP
                ) KTP ON IVS.IDENT_STAKEHOLDER = KTP.IDENT_INVESTOR
                LEFT JOIN (
                SELECT IDENT_INVESTOR, IDENTIFIER, SI.EXPIRYDATE, SI.REGISTRATIONDATE
                FROM STAKEHOLDERIDENTIFIER@'||v_SchemaProd||' SI 
                WHERE IDENT_STAKEHOLDERIDTYPE = 9 -- BIC
                ) BIC ON IVS.IDENT_STAKEHOLDER = BIC.IDENT_INVESTOR
                LEFT JOIN (
                SELECT IDENT_INVESTOR, IDENTIFIER, SI.EXPIRYDATE, SI.REGISTRATIONDATE
                FROM STAKEHOLDERIDENTIFIER@'||v_SchemaProd||' SI
                WHERE IDENT_STAKEHOLDERIDTYPE = 14 -- KITAS/SKD
                ) KITAS ON IVS.IDENT_STAKEHOLDER = KITAS.IDENT_INVESTOR
                LEFT JOIN (
                SELECT IDENT_INVESTOR, IDENTIFIER, SI.EXPIRYDATE, SI.REGISTRATIONDATE 
                FROM STAKEHOLDERIDENTIFIER@'||v_SchemaProd||' SI
                WHERE IDENT_STAKEHOLDERIDTYPE = 15 -- BUSREG
                 ) BUSREG ON IVS.IDENT_STAKEHOLDER = BUSREG.IDENT_INVESTOR';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_IDENTIFIER', 'Y', 'Insert into IMP_STG_INVS_IDENTIFIER');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_INVS_IDENTIFIER DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_IDENTIFIER',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_IDENTIFIER with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_INVS_IDENTIFIER with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_IDENTIFIER

procedure IMP_STG_INVS_INDCLASS1
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_INDCLASS1 DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_INDCLASS1
                    SELECT ID, CODE, DESCRIPTION, SYSCREATED, SYSMODIFIED
                    FROM INDUSTRYCLASSIFICATION1@'||v_SchemaProd||' IC1';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_INDCLASS1', 'Y', 'Insert into IMP_STG_INVS_INDCLASS1');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_INDCLASS1 DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_INDCLASS1',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_INDCLASS1 with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_INDCLASS1 with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_INDCLASS1

procedure IMP_STG_INVS_TAXCLASS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_TAXCLASS DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_TAXCLASS
                    SELECT ID, DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED, APPLICABILITY
                    FROM IDNTAXCLASSIFICATION@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_TAXCLASS', 'Y', 'Insert into IMP_STG_INVS_TAXCLASS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_TAXCLASS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_TAXCLASS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_TAXCLASS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_TAXCLASS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_TAXCLASS

procedure IMP_STG_INVS_MARITALSTA
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_MARITALSTA DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_MARITALSTA
                    SELECT ID, DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED
                    FROM IDNMARITALSTATUS@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_MARITALSTA', 'Y', 'Insert into IMP_STG_INVS_MARITALSTA');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_MARITALSTA DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_MARITALSTA',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_MARITALSTA with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_MARITALSTA with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_MARITALSTA

procedure IMP_STG_INVS_EDUCATION
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_EDUCATION DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_EDUCATION
                    (ID, CBEST_CODE, DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED)
                    SELECT ID, DECODE(ID,8, 1, ID+1) CBEST_CODE, 
                    DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED
                    FROM IDNEDUCATION@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_EDUCATION', 'Y', 'Insert into IMP_STG_INVS_EDUCATION');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_EDUCATION DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_EDUCATION',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_EDUCATION with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_EDUCATION with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_EDUCATION

procedure IMP_STG_INVS_OCCUPATION
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_OCCUPATION DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_OCCUPATION
                    (ID, CBEST_CODE, DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED)
                    SELECT ID, DECODE(ID,9,1,
                    1,2,
                    2,3,
                    3,4,
                    4,6,
                    5,7,
                    6,8,
                    7,9,
                    8,5) CBEST_CODE,
                    DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED
                    FROM IDNOCCUPATION@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_OCCUPATION', 'Y', 'Insert into IMP_STG_INVS_OCCUPATION');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_OCCUPATION DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_OCCUPATION',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_OCCUPATION with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_OCCUPATION with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_OCCUPATION

procedure IMP_STG_INVS_INCOME
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_INCOME DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_INCOME
                    SELECT ID, DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED
                    FROM IDNINCOME@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_INCOME', 'Y', 'Insert into IMP_STG_INVS_INCOME');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_INCOME DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_INCOME',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_INCOME with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_INCOME with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_INCOME

procedure IMP_STG_INVS_INDCLASS2
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_INDCLASS2 DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_INDCLASS2
                    SELECT ID, CODE, DESCRIPTION, INVESTOR, ISSUER, SYSCREATED, SYSMODIFIED
                    FROM INDUSTRYCLASSIFICATION2@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_INDCLASS2', 'Y', 'Insert into IMP_STG_INVS_INDCLASS2');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_INDCLASS2 DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_INDCLASS2',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_INDCLASS2 with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_INDCLASS2 with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_INDCLASS2

procedure IMP_STG_INVS_ASSET
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_ASSET DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_ASSET
                    SELECT ID, DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED
                    FROM IDNASSET@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_ASSET', 'Y', 'Insert into IMP_STG_INVS_ASSET');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_ASSET DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_ASSET',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_ASSET with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_ASSET with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
END;--IMP_STG_INVS_ASSET

procedure IMP_STG_INVS_OPERATINGPROFIT
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_OPERATINGPROFIT DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_OPERATINGPROFIT
                    SELECT ID, DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED
                    FROM IDNOPERATINGPROFIT@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_OPERATINGPROFIT', 'Y', 'Insert into IMP_STG_INVS_OPERATINGPROFIT');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_OPERATINGPROFIT DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_OPERATINGPROFIT',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_OPERATINGPROFIT with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_OPERATINGPROFIT with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_OPERATINGPROFIT

procedure IMP_STG_INVS_INVCATEGORY
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INVS_INVCATEGORY DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_INVS_INVCATEGORY
                    SELECT ID, DESCRIPTION, CODE, SYSCREATED, SYSMODIFIED
                    FROM IDNINVESTORCATEGORY@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INVS_INVCATEGORY', 'Y', 'Insert into IMP_STG_INVS_INVCATEGORY');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INVS_INVCATEGORY DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INVS_INVCATEGORY',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INVS_INVCATEGORY with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INVS_INVCATEGORY with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INVS_INVCATEGORY

procedure IMP_STG_POS_MOVMNT
is
begin

    select to_char(get_next_bus_date(sysdate,-1),'DD-MON-YYYY') into tglDesc from dual;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_POSITION_MOVEMENTS DROP STORAGE';

     v_sql := ' INSERT INTO TMPSCHEMA.STG_POSITION_MOVEMENTS (AMT_OPER, CODE_ACT_STA, DAT_EXEC, ID_ACT_CAPCO, 
                    LST_UPD_TS, TYP_MVT, BLNC_ID_BLNC_CAPCO, INST_ID_INST_XCSD, EXT_REF, ID_ACCT, CODE_BASE_SEC, TYP_BLNC, 
                    POOLIDENTIFIER, INSTR_TYP, TRADE_DATE)
                    SELECT AMT_OPER,
                    CODE_ACT_STA,
                    TO_CHAR(DAT_EXEC, ''yyyymmddhh24missff3'') DAT_EXEC, INST_ID_INST_XCSD || ''_'' || TYP_MVT ID_ACT_CAPCO,
                    TO_CHAR(LST_UPD_TS, ''yyyymmddhh24missff3'') LST_UPD_TS, 
                    TYP_MVT, BLNC_ID_BLNC_CAPCO, INST_ID_INST_XCSD, 
                    EXT_REF, ID_ACCT, SECURITY AS CODE_BASE_SEC, TYP_BLNC, POOLIDENTIFIER, INSTR_TYP,
                    TO_CHAR(TRADEDATE, ''YYYYMMDD'') TRADE_DATE
                    FROM (
                    SELECT SI.VOLUME AS AMT_OPER, 1 AS CODE_ACT_STA, NVL(SI.EFFECTIVESETTLEMENTDATE,SI.UPDATEDATE) AS DAT_EXEC,
                    NULL AS ID_ACT_CAPCO, SI.TRADEDATE, NVL(SI.EFFECTIVESETTLEMENTDATE,SI.UPDATEDATE) AS LST_UPD_TS,
                    SI.IDENT_SECURITIESMOVEMENT AS TYP_MVT, 
                    ''BLNC_''||TRIM(AC.ACCOUNTIDENTIFIERVALUE)|| TRIM(INSTR.NAME) AS BLNC_ID_BLNC_CAPCO,
                    SI.IDENT_SETTLEMENTINSTRUCTION AS INST_ID_INST_XCSD,
                    CASE WHEN SI.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SI.COMMONREFERENCE
                    ELSE SI.INSTRUCTIONREFERENCE END AS EXT_REF,
                    AC.ACCOUNTIDENTIFIERVALUE ID_ACCT, INSTR.NAME AS SECURITY, 1 AS TYP_BLNC, 
                    SI.POOLIDENTIFIER, 
                    CASE WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE LIKE ''VCA%'' THEN ''VCA''
                    WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NOT NULL THEN ''TECH'' 
                    WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NULL THEN ''CORP''
                    WHEN IT.CODE = ''SECD'' THEN ''DCONF''
                    ELSE IT.CODE END INSTR_TYP
                    FROM SETTLEMENTINSTRUCTION@'||v_SchemaProd||' SI
                    LEFT JOIN SECURITIESACCOUNT@'||v_SchemaProd||' SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT             
                    LEFT JOIN ACCOUNTCOMPOSITE@'||v_SchemaProd||' AC ON SA.IDENT_ACCOUNTCOMPOSITE = AC.IDENT_ACCOUNTCOMPOSITE
                    LEFT JOIN INSTRUMENT@'||v_SchemaProd||' INSTR ON SI.IDENT_INSTRUMENT = INSTR.IDENT_INSTRUMENT
                    LEFT JOIN SETTLEMENTINSTRUCTIONDETAILS@'||v_SchemaProd||' DE ON SI.IDENT_SETTLEMENTINSTRUCTION = DE.IDENT_SETTLEMENTINSTRUCTION
                    LEFT JOIN INSTRUCTIONTYPE@'||v_SchemaProd||' IT ON SI.IDENT_INSTRUCTIONTYPE  = IT.IDENT_INSTRUCTIONTYPE
                    WHERE SI.INSTRUCTIONSTATUS = ''SETTLED''
                    AND SI.PARTYONHOLD = 0
                    AND SI.IDENT_SECURITIESMOVEMENT != 3
                    AND SI.VOLUME > 0
                    AND TRUNC(SI.EFFECTIVESETTLEMENTDATE) = '''||tglDesc||'''
                    UNION ALL
                    SELECT SI.AMOUNT AS AMT_OPER, 1 AS CODE_ACT_STA, NVL(SI.EFFECTIVESETTLEMENTDATE,SI.UPDATEDATE) AS DAT_EXEC,
                    NULL AS ID_ACT_CAPCO, SI.TRADEDATE, NVL(SI.EFFECTIVESETTLEMENTDATE,SI.UPDATEDATE) AS LST_UPD_TS,
                    SI.IDENT_PAYMENTDIRECTION AS TYP_MVT, 
                    ''BLNC_''||TRIM(AC.ACCOUNTIDENTIFIERVALUE)|| TRIM(CURR.CODE) AS BLNC_ID_BLNC_CAPCO,
                    SI.IDENT_SETTLEMENTINSTRUCTION AS INST_ID_INST_XCSD,
                    CASE WHEN SI.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SI.COMMONREFERENCE
                    ELSE SI.INSTRUCTIONREFERENCE END AS EXT_REF,
                    AC.ACCOUNTIDENTIFIERVALUE ID_ACCT, CURR.CODE AS SECURITY, 1 AS TYP_BLNC, 
                    SI.POOLIDENTIFIER, 
                    CASE WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE LIKE ''VCA%'' THEN ''VCA''
                    WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NOT NULL THEN ''TECH'' 
                    WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NULL THEN ''CORP''
                    WHEN IT.CODE = ''SECD'' THEN ''DCONF''
                    ELSE IT.CODE END INSTR_TYP
                    FROM SETTLEMENTINSTRUCTION@'||v_SchemaProd||' SI
                    LEFT JOIN SETTLEMENTINSTRUCTIONDETAILS@'||v_SchemaProd||' DE ON SI.IDENT_SETTLEMENTINSTRUCTION = DE.IDENT_SETTLEMENTINSTRUCTION
                    LEFT JOIN CASHACCOUNT@'||v_SchemaProd||' CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT
                    LEFT JOIN ACCOUNTCOMPOSITE@'||v_SchemaProd||' AC ON AC.IDENT_ACCOUNTCOMPOSITE = CA.IDENT_ACCOUNTCOMPOSITE 
                    LEFT JOIN CURRENCY@'||v_SchemaProd||' CURR ON SI.IDENT_CURRENCY = CURR.IDENT_CURRENCY
                    LEFT JOIN INSTRUCTIONTYPE@'||v_SchemaProd||' IT ON SI.IDENT_INSTRUCTIONTYPE  = IT.IDENT_INSTRUCTIONTYPE
                    WHERE SI.INSTRUCTIONSTATUS = ''SETTLED''
                    AND SI.PARTYONHOLD = 0
                    AND SI.IDENT_CFPMETHOD != 2
                    AND SI.AMOUNT > 0
                    AND TRUNC(SI.EFFECTIVESETTLEMENTDATE) = '''||tglDesc||'''
                    UNION ALL
                    SELECT SI.SETTLEDQUANTITY AMT_OPER, 1 CODE_ACT_STA, NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED) DAT_EXEC,
                    NULL AS ID_ACT_CAPCO, CAST(NULL AS DATE) TRADE_DATE, NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED) LST_UPD_TS,
                    1 TYP_MVT , ''BLNC_''||TRIM(AC.ID_ACCT)|| TRIM(INS.NAME) AS BLNC_ID_BLNC_CAPCO, SI.IDENT_SETTLEMENTINSTRUCTION INST_ID_INST_XCSD,
                    RO.USERREFERENCE EXT_REF, AC.ID_ACCT, INS.NAME SECURITY, 1 TYP_BLNC, SI.POOLIDENTIFIER, ''BLOACINSTR'' INSTR_TYP
                    FROM TMPSCHEMA.STG_TOBA_RESTRICTIONORDER RO
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AC ON RO.IDENT_ACCOUNTCOMPOSITE = AC.ID_ACCT_XCSD
                    LEFT JOIN STATIC_INSTRUMENT INS ON RO.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI ON RO.IDENT_RESTRICTIONINSTRUCTION = SI.IDENT_SETTLEMENTINSTRUCTION
                    WHERE 1=1
                    AND RO.RESTRICTWHOLEACCOUNT = 0
                    AND RO.IDENT_MASTER IS NULL
                    AND SI.INSTRUCTIONSTATUS IN (''PENDING'')
                    AND TRUNC(RO.REVISION_EFFECTIVE_FROM) = '''||tglDesc||'''
                    UNION ALL
                    SELECT SI.SETTLEDQUANTITY AMT_OPER, 1 CODE_ACT_STA, NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED) DAT_EXEC,
                    NULL AS ID_ACT_CAPCO, CAST(NULL AS DATE) TRADE_DATE, NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED) LST_UPD_TS,
                    2 TYP_MVT , ''BLNC_''||TRIM(AC.ID_ACCT)|| TRIM(INS.NAME) AS BLNC_ID_BLNC_CAPCO, SI.IDENT_SETTLEMENTINSTRUCTION INST_ID_INST_XCSD,
                    RO.USERREFERENCE EXT_REF, AC.ID_ACCT, INS.NAME SECURITY, 2 TYP_BLNC, SI.POOLIDENTIFIER, ''BLOACINSTR'' INSTR_TYP
                    FROM TMPSCHEMA.STG_TOBA_RESTRICTIONORDER RO
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AC ON RO.IDENT_ACCOUNTCOMPOSITE = AC.ID_ACCT_XCSD
                    LEFT JOIN STATIC_INSTRUMENT INS ON RO.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI ON RO.IDENT_RESTRICTIONINSTRUCTION = SI.IDENT_SETTLEMENTINSTRUCTION
                    WHERE 1=1
                    AND RO.RESTRICTWHOLEACCOUNT = 0
                    AND RO.IDENT_MASTER IS NULL
                    AND SI.INSTRUCTIONSTATUS IN (''PENDING'')
                    AND TRUNC(RO.REVISION_EFFECTIVE_FROM) = '''||tglDesc||'''
                    UNION ALL
                    SELECT DISTINCT SL.SETTLEDQUANTITY AMT_OPER, 1 CODE_ACT_STA, NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED) DAT_EXEC,
                    NULL AS ID_ACT_CAPCO, CAST(NULL AS DATE) TRADE_DATE, NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED) LST_UPD_TS,
                    1 TYP_MVT , ''BLNC_''||TRIM(AC.ID_ACCT)|| TRIM(INS.NAME) AS BLNC_ID_BLNC_CAPCO, SI.IDENT_SETTLEMENTINSTRUCTION INST_ID_INST_XCSD,
                    RO.USERREFERENCE EXT_REF, AC.ID_ACCT, INS.NAME SECURITY, 2 TYP_BLNC, SI.POOLIDENTIFIER, ''UNBLOINSTR'' INSTR_TYP
                    FROM TMPSCHEMA.STG_TOBA_RESTRICTIONORDER RO
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AC ON RO.IDENT_ACCOUNTCOMPOSITE = AC.ID_ACCT_XCSD
                    LEFT JOIN STATIC_INSTRUMENT INS ON RO.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI ON RO.IDENT_RESTRICTIONINSTRUCTION = SI.IDENT_SETTLEMENTINSTRUCTION
                    LEFT JOIN STG_TOBA_SETTINSTRSTATUSLOG SL ON SI.IDENT_SETTLEMENTINSTRUCTION = SL.IDENT_SETTLEMENTINSTRUCTION AND SL.INSTRUCTIONSTATUS = ''PENDING''
                    WHERE 1=1
                    AND RO.RESTRICTWHOLEACCOUNT = 0
                    AND RO.IDENT_MASTER IS NULL
                    AND SI.INSTRUCTIONSTATUS IN (''SETTLED'')
                    AND TRUNC(RO.REVISION_EFFECTIVE_FROM) = '''||tglDesc||'''
                    UNION ALL
                    SELECT DISTINCT SL.SETTLEDQUANTITY AMT_OPER, 1 CODE_ACT_STA, NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED) DAT_EXEC,
                    NULL AS ID_ACT_CAPCO, CAST(NULL AS DATE) TRADE_DATE, NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED) LST_UPD_TS,
                    2 TYP_MVT , ''BLNC_''||TRIM(AC.ID_ACCT)|| TRIM(INS.NAME) AS BLNC_ID_BLNC_CAPCO, SI.IDENT_SETTLEMENTINSTRUCTION INST_ID_INST_XCSD,
                    RO.USERREFERENCE EXT_REF, AC.ID_ACCT, INS.NAME SECURITY, 1 TYP_BLNC, SI.POOLIDENTIFIER, ''UNBLOINSTR'' INSTR_TYP
                    FROM TMPSCHEMA.STG_TOBA_RESTRICTIONORDER RO
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AC ON RO.IDENT_ACCOUNTCOMPOSITE = AC.ID_ACCT_XCSD
                    LEFT JOIN STATIC_INSTRUMENT INS ON RO.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI ON RO.IDENT_RESTRICTIONINSTRUCTION = SI.IDENT_SETTLEMENTINSTRUCTION
                    LEFT JOIN STG_TOBA_SETTINSTRSTATUSLOG SL ON SI.IDENT_SETTLEMENTINSTRUCTION = SL.IDENT_SETTLEMENTINSTRUCTION AND SL.INSTRUCTIONSTATUS = ''PENDING''
                    WHERE 1=1
                    AND RO.RESTRICTWHOLEACCOUNT = 0
                    AND RO.IDENT_MASTER IS NULL
                    AND SI.INSTRUCTIONSTATUS IN (''SETTLED'')
                    AND TRUNC(RO.REVISION_EFFECTIVE_FROM) = '''||tglDesc||'''
                    )';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_POS_MOVMNT', 'Y', 'Insert into IMP_STG_POS_MOVMNT');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_POS_MOVMNT DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_POS_MOVMNT',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_POS_MOVMNT with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_POS_MOVMNT with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_POS_MOVMNT

procedure IMP_STG_BALANCES
is
begin
    
    SELECT TO_CHAR(GET_NEXT_BUS_DATE(SYSDATE,-1),'DD-MON-YYYY') INTO TGLDESC FROM DUAL; 
     
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_BALANCES DROP STORAGE';

     v_sql := 'insert into TMPSCHEMA.STG_BALANCES (CODE_BASE_SEC,  
                            SEC_DSC, ID_MEM, DSC_MEM, ID_ACCT, DSC_ACCT,  
                            TAX_ID, LONG_DESC, TAX_EQUI, TAX_CORP, AMT_BLNC,  
                            TYP_BLNC, ACCT_STA, SEC_STA, MEM_STA ,BLNC_ID)
                        select 
                            CODE_BASE_SEC,  
                            SEC_DSC, ID_MEM, DSC_MEM, ID_ACCT, DSC_ACCT,  
                            CODE_ATA TAX_ID, LONG_DESC, TAX_EQUI, TAX_CORP, AMT_BLNC,  
                            TYP_BLNC, ACCT_STA, SEC_STA, MEM_STA ,BLNC_ID
                        from( 
                                SELECT  
                                        ah.IDENT_ACCOUNTHOLDING AS ID_BLNC_XCSD, 
                                        to_char(ah.VALIDFROM,''yyyymmddhh24missff3'') AS LST_UPD_TS,  
                                        ABS(ah.SETTLEDQUANTITY - ah.RESTRICTEDQUANTITY) AS AMT_BLNC, 
                                        CASE WHEN sa.IDENT_ACCOUNTTYPE NOT IN (1002, 1010, 1013) THEN 1 ELSE 9 END AS TYP_BLNC, 
                                        CASE WHEN sa.IDENT_ACCOUNTTYPE = 84 THEN 2 WHEN sa.IDENT_ACCOUNTTYPE = 1 THEN 2 ELSE 1 END AS TYP_FLG_ACCT, 
                                        ac.ID_ACCT, 
                                        ac.DSC_ACCT,  
                                        ac.CODE_ACCT_STA ACCT_STA, 
                                        instm.CODE_BASE_SEC, 
                                        instm.SEC_DSC, 
                                        instm.CODE_STA SEC_STA, 
                                        prt.CODE ID_MEM, 
                                        prt.LONGNAME DSC_MEM, 
                                        prt.IDENT_ACTIVATIONTYPE MEM_STA, 
                                        ac.CODE_ATA, 
                                        txi.CODE_LONG_DSC LONG_DESC 
                                        ,txs.TAX_VAL TAX_EQUI 
                                        ,txs2.TAX_VAL TAX_CORP 
                                        ,''ACCTHLDG_''||ah.IDENT_ACCOUNTHOLDING BLNC_ID
                                    FROM ACCOUNTHOLDING@'||v_SchemaProd||' ah, 
                                        SECURITIESACCOUNT@'||v_SchemaProd||' sa, 
                                        TMPSCHEMA.STG_TOBA_ACCOUNTS ac, 
                                        TMPSCHEMA.STG_TOBA_BASIC_SECURITIES instm, 
                                        STATIC_PARTICIPANT prt, 
                                        codes txi, 
                                        TAX_ID txs, 
                                        TAX_ID txs2 
                                    WHERE ah.IDENT_ACCOUNT = sa.IDENT_ACCOUNT 
                                        AND SA.IDENT_ACCOUNTCOMPOSITE = AC.ID_ACCT_XCSD(+) 
                                        AND ah.IDENT_INSTRUMENT = instm.IDENT_INSTRUMENT(+) 
                                        AND ac.ID_MEM = prt.CODE(+) 
                                        AND (ac.CODE_ATA = txi.CODE_VAL(+) AND txi.COL_NM(+) = ''CODE_ATA'')     
                                        AND (ac.CODE_ATA = txs.CODE_VAL(+) AND txs.TYP_SEC(+) = 1) 
                                        AND (ac.CODE_ATA = txs2.CODE_VAL(+) AND txs2.TYP_SEC(+) = 2) 
                                        AND prt.IDENT_ACTIVATIONTYPE(+) <> 5 
                                        AND ABS(ah.SETTLEDQUANTITY) > 0 
                                        AND TRUNC(AH.VALIDFROM) <= '''||tglDesc||'''
                                        AND (TRUNC(AH.VALIDUNTIL) IS NULL OR TRUNC(AH.VALIDUNTIL) > '''||tglDesc||''')        
                                    union 
                                    SELECT  
                                        ah.IDENT_ACCOUNTHOLDING AS ID_BLNC_XCSD, 
                                        to_char(ah.VALIDFROM,''yyyymmddhh24missff3'') AS LST_UPD_TS,  
                                        ABS (ah.RESTRICTEDQUANTITY) AS AMT_BLNC, 
                                        CASE WHEN sa.IDENT_ACCOUNTTYPE NOT IN (1002, 1010, 1013) THEN 2 ELSE 9 END AS TYP_BLNC,     
                                        CASE WHEN sa.IDENT_ACCOUNTTYPE = 84 THEN 2 WHEN sa.IDENT_ACCOUNTTYPE = 1 THEN 2 ELSE 1 END AS TYP_FLG_ACCT, 
                                        ac.ID_ACCT, 
                                        ac.DSC_ACCT,  
                                        ac.CODE_ACCT_STA ACCT_STA, 
                                        instm.CODE_BASE_SEC, 
                                        instm.SEC_DSC, 
                                        instm.CODE_STA SEC_STA, 
                                        prt.CODE ID_MEM, 
                                        prt.LONGNAME DSC_MEM, 
                                        prt.IDENT_ACTIVATIONTYPE MEM_STA, 
                                        ac.CODE_ATA, 
                                        txi.CODE_LONG_DSC LONG_DESC 
                                        ,txs.TAX_VAL TAX_EQUI 
                                        ,txs2.TAX_VAL TAX_CORP 
                                        ,''ACCTHLDG_''||ah.IDENT_ACCOUNTHOLDING BLNC_ID
                                    FROM ACCOUNTHOLDING@'||v_SchemaProd||' ah, 
                                        SECURITIESACCOUNT@'||v_SchemaProd||' sa, 
                                        TMPSCHEMA.STG_TOBA_ACCOUNTS ac, 
                                        TMPSCHEMA.STG_TOBA_BASIC_SECURITIES instm, 
                                        STATIC_PARTICIPANT prt, 
                                        codes txi, 
                                        TAX_ID txs, 
                                        TAX_ID txs2 
                                    WHERE ah.IDENT_ACCOUNT = sa.IDENT_ACCOUNT 
                                        AND SA.IDENT_ACCOUNTCOMPOSITE = AC.ID_ACCT_XCSD 
                                        AND ah.IDENT_INSTRUMENT = instm.IDENT_INSTRUMENT(+) 
                                        AND ac.ID_MEM = prt.CODE(+) 
                                        AND prt.IDENT_ACTIVATIONTYPE(+) <> 5 
                                        AND (ac.CODE_ATA = txi.CODE_VAL(+) AND txi.COL_NM(+) = ''CODE_ATA'')     
                                        AND (ac.CODE_ATA = txs.CODE_VAL(+) AND txs.TYP_SEC(+) = 1) 
                                        AND (ac.CODE_ATA = txs2.CODE_VAL(+) AND txs2.TYP_SEC(+) = 2) 
                                        AND ABS (ah.RESTRICTEDQUANTITY) > 0     
                                        AND TRUNC(AH.VALIDFROM) <= '''||tglDesc||'''
                                        AND (TRUNC(AH.VALIDUNTIL) IS NULL OR TRUNC(AH.VALIDUNTIL) > '''||tglDesc||''') 
                                    union     
                                    SELECT  
                                        cab.IDENT_CASHACCOUNTBALANCE AS ID_BLNC_CAPCO, 
                                        to_char(cab.VALIDFROM,''yyyymmddhh24missff3'') AS LST_UPD_TS, 
                                        ABS (cab.BALANCE - CAB.RESTRICTEDBALANCE) AS AMT_BLNC, 
                                        1 AS TYP_BLNC, 
                                        CASE WHEN ca.IDENT_ACCOUNTTYPE = 1 THEN 2 ELSE 1 END AS TYP_FLG_ACCT, 
                                        ac.ID_ACCT, 
                                        ac.DSC_ACCT,  
                                        ac.CODE_ACCT_STA ACCT_STA, 
                                        curr.CODE CODE_BASE_SEC, 
                                        curr.NAME SEC_DSC, 
                                        curr.IDENT_ACTIVATIONTYPE SEC_STA, 
                                        prt.CODE ID_MEM, 
                                        prt.LONGNAME DSC_MEM, 
                                        prt.IDENT_ACTIVATIONTYPE MEM_STA, 
                                        ac.CODE_ATA, 
                                        txi.CODE_LONG_DSC LONG_DESC 
                                        ,txs.TAX_VAL TAX_EQUI 
                                        ,txs2.TAX_VAL TAX_CORP 
                                        ,''CASHACTBLNC_''||cab.IDENT_CASHACCOUNTBALANCE AS BLNC_ID
                                    FROM  
                                        CASHACCOUNTBALANCE@'||v_SchemaProd||' cab, 
                                        TMPSCHEMA.STG_TOBA_ACCOUNTS ac, 
                                        CASHACCOUNT@'||v_SchemaProd||' ca, 
                                        CURRENCY@'||v_SchemaProd||' curr, 
                                        STATIC_PARTICIPANT prt, 
                                        codes txi, 
                                        TAX_ID txs, 
                                        TAX_ID txs2     
                                    WHERE      
                                        ca.IDENT_ACCOUNTCOMPOSITE = ac.ID_ACCT_XCSD(+) 
                                        AND CAB.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT 
                                        AND cab.IDENT_CURRENCY = curr.IDENT_CURRENCY(+) 
                                        AND ac.ID_MEM = prt.CODE(+) 
                                        AND (ac.CODE_ATA = txi.CODE_VAL(+) AND txi.COL_NM(+) = ''CODE_ATA'')     
                                        AND (ac.CODE_ATA = txs.CODE_VAL(+) AND txs.TYP_SEC(+) = 1) 
                                        AND (ac.CODE_ATA = txs2.CODE_VAL(+) AND txs2.TYP_SEC(+) = 2) 
                                        AND prt.IDENT_ACTIVATIONTYPE(+) <> 5     
                                        AND ABS (CAB.BALANCE) > 0 
                                        AND TRUNC(CAB.VALIDFROM) <= '''||tglDesc||'''
                                        AND (TRUNC(CAB.VALIDUNTIL) IS NULL OR TRUNC(CAB.VALIDUNTIL) > '''||tglDesc||''')
                                    union 
                                    SELECT  
                                        cab.IDENT_CASHACCOUNTBALANCE AS ID_BLNC_CAPCO, 
                                        to_char(cab.VALIDFROM,''yyyymmddhh24missff3'') AS LST_UPD_TS, 
                                        ABS (RESTRICTEDBALANCE) AS AMT_BLNC, 
                                        2 AS TYP_BLNC, 
                                        CASE WHEN ca.IDENT_ACCOUNTTYPE = 1 THEN 2 ELSE 1 END AS TYP_FLG_ACCT, 
                                        ac.ID_ACCT, 
                                        ac.DSC_ACCT,  
                                        ac.CODE_ACCT_STA ACCT_STA, 
                                        curr.CODE CODE_BASE_SEC, 
                                        curr.NAME SEC_DSC, 
                                        curr.IDENT_ACTIVATIONTYPE SEC_STA, 
                                        prt.CODE ID_MEM, 
                                        prt.LONGNAME DSC_MEM, 
                                        prt.IDENT_ACTIVATIONTYPE MEM_STA, 
                                        ac.CODE_ATA, 
                                        txi.CODE_LONG_DSC LONG_DESC 
                                        ,txs.TAX_VAL TAX_EQUI 
                                        ,txs2.TAX_VAL TAX_CORP 
                                        ,''CASHACTBLNC_''||cab.IDENT_CASHACCOUNTBALANCE AS BLNC_ID
                                    FROM  
                                        CASHACCOUNTBALANCE@'||v_SchemaProd||' cab, 
                                        TMPSCHEMA.STG_TOBA_ACCOUNTS ac, 
                                        CASHACCOUNT@'||v_SchemaProd||' ca, 
                                        CURRENCY@'||v_SchemaProd||' curr, 
                                        STATIC_PARTICIPANT prt, 
                                        codes txi, 
                                        TAX_ID txs, 
                                        TAX_ID txs2     
                                    WHERE      
                                        ca.IDENT_ACCOUNTCOMPOSITE = ac.ID_ACCT_XCSD(+) 
                                        AND CAB.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT 
                                        AND cab.IDENT_CURRENCY = curr.IDENT_CURRENCY(+) 
                                        AND ac.ID_MEM = prt.CODE(+) 
                                        AND (ac.CODE_ATA = txi.CODE_VAL(+) AND txi.COL_NM(+) = ''CODE_ATA'')     
                                        AND (ac.CODE_ATA = txs.CODE_VAL(+) AND txs.TYP_SEC(+) = 1) 
                                        AND (ac.CODE_ATA = txs2.CODE_VAL(+) AND txs2.TYP_SEC(+) = 2) 
                                        AND prt.IDENT_ACTIVATIONTYPE(+) <> 5     
                                        AND ABS (CAB.RESTRICTEDBALANCE) > 0 
                                        AND TRUNC(CAB.VALIDFROM) <= '''||tglDesc||'''
                                        AND (TRUNC(CAB.VALIDUNTIL) IS NULL OR TRUNC(CAB.VALIDUNTIL) > '''||tglDesc||''')  
                                    ) where ID_ACCT is not null';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_BALANCES', 'Y', 'Insert into IMP_STG_BALANCES');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_BALANCES DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_BALANCES',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_BALANCES with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_BALANCES with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(v_sql);
end;--IMP_STG_BALANCES

procedure IMP_STG_SI_INSTRUCTIONTYPE
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE
                    SELECT IDENT_INSTRUCTIONTYPE, CODE, NAME, APPLICABLETOSI, APPLICABLETOCI, SYSCREATED, SYSMODIFIED 
                    FROM INSTRUCTIONTYPE@'||v_SchemaProd||'';

     execute immediate v_sql;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_SI_INSTRUCTIONTYPE', 'Y', 'Insert into IMP_STG_SI_INSTRUCTIONTYPE');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_TOBA_SI_INSTRUCTIONTYPE DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_SI_INSTRUCTIONTYPE',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_SI_INSTRUCTIONTYPE with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_TOBA_SI_INSTRUCTIONTYPE with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_SI_INSTRUCTIONTYPE

procedure IMP_STG_SETTLEMENTINSTRUCTION
is
begin

    select to_char(get_next_bus_date(sysdate,-1),'DD-MON-YYYY') into tglDesc from dual;     
        
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION
                    SELECT SI.IDENT_SETTLEMENTINSTRUCTION, SI.IDENT_ACCOUNT, SI.IDENT_INSTRUMENT, SI.VOLUME, SI.AMOUNT, SI.SETTLEMENTDATE, SI.INSTRUCTIONREFERENCE, 
                    SI.IDENT_COUNTERPART, SI.IDENT_CURRENCY, SI.TRADEDATE, SI.SOURCEID, SI.TRADEREFERENCE, SI.MARKETINFRASTRUCTUREREF, SI.ID_MATCHEDSETTLEMENTCURR, 
                    SI.MATCHEDSETTLEMENTAMOUNT, SI.SETTLEDQUANTITY, SI.EFFECTIVESETTLEMENTDATE, SI.IDENT_SECURITIESMOVEMENT, SI.IDENT_TRANSACTIONTYPE, 
                    SI.IDENT_CUMEXTYPE, SI.IDENT_SETTLEMENTMETHOD, SI.IDENT_CFPMETHOD, SI.IDENT_PAYMENTDIRECTION, SI.IDENT_PARTIALSETTLEMENTTYPE, 
                    SI.IDENT_SETTLEMENTPRIORITY, SI.IDENT_SOURCE, SI.INSTRUCTIONSTATUS, SI.IDENT_SETTLEMENTSTATUS, SI.IDENT_CANCELLATIONSTATUS, SI.IDENT_INSTRUCTINGPARTY, 
                    SI.SETTLEDAMOUNT, SI.IDENT_MATCHINGSTATUS, SI.MATCHINGREFERENCE, SI.IDENT_INSTRUCTIONSTATUSREASON, SI.NETTINGREFERENCE, SI.IDENT_ACCOUNTSERVICING, 
                    SI.SETTLPARTYCLIENT, SI.SETTLPARTYCLIENTSECACCOUNT, SI.COUNTPARTYCLIENT, SI.COUNTPARTYCLIENTSECACCOUNT, SI.TRADEPRICE, SI.OPTOUTINDICATOR, 
                    SI.IDENT_CSDOFCOUNTERPARTY, SI.CASHCLEARINGPARTICIPANT, SI.CASHACCOUNT, SI.COMMONREFERENCE, SI.IDENT_CASHACCOUNT, SI.LINKREFERENCE, 
                    SI.POOLIDENTIFIER, SI.COUNTERPARTYSECURITIESACC, SI.PLACEOFTRADE, SI.CREATEDATE, SI.UPDATEDATE, SI.IDENT_SETTLINGCAPACITY, SI.IDENT_MARKETTYPE, 
                    SI.COUNTERPARTBIC, SI.ACCOUNTSERVICINGBIC, SI.IDENT_CSDOFACCOUNTSERVICING, SI.POOLCOUNTER, SI.IDENT_EXTERNALSETTLEMENTSTATE, 
                    SI.VOLUMETOPARTIALSETTLE, SI.AMOUNTTOPARTIALSETTLE, SI.IDENT_SETTLEMENTCAP1, SI.IDENT_SETTLEMENTCAP2, SI.ULT_DEBTOR_OR_CREDITOR, 
                    SI.ULT_DEBTOR_OR_CREDITOR_ACC, SI.ONHOLDBYCSDVALIDATION, SI.TRANSFORMATIONINDICATOR, SI.MODIFICATIONANDCANCELALLOWED, 
                    SI.PROCESSINGREFERENCE, SI.ONHOLDBYCOSD, SI.IDENT_CASHAGENT, SI.CSDONHOLD, SI.PARTYONHOLD, SI.IDENT_GENERATEDREASON, 
                    SI.IDENT_TRADEPRICECURRENCY, SI.SETTLEMENTELIGIBILITY, SI.EXTERNALMATCHINGREF, SI.SETTLEMENTPATTERN, SI.IDENT_PAYMENTSYSTEM, 
                    SI.EXPECTEDSETTLEMENTDATE, SI.BYPASSELIGIBILITYCHECK, SI.CORPORATEACTIONREFERENCE, SI.ENDTOENDREFERENCE, SI.PREMATCHED, 
                    SI.CORPORATEACTIONTYPE, SI.IDENT_INSTRUCTIONTYPE, SI.ACCRUEDINTERESTAMOUNT, SI.ACCOUNTBLOCKED, SI.TRADEPRICETYPE, 
                    SI.SETTLEMENTELIGIBILITYFLAGS, SI.REPOREFERENCE, SI.SYSCREATED, SI.SYSMODIFIED, SI.IDENT_COUNTERPARTYSECURITIESAC, DE.DESCRIPTION
                    FROM SETTLEMENTINSTRUCTION@'||v_SchemaProd||' SI
                    LEFT JOIN SETTLEMENTINSTRUCTIONDETAILS@'||v_SchemaProd||' DE ON SI.IDENT_SETTLEMENTINSTRUCTION = DE.IDENT_SETTLEMENTINSTRUCTION
                    WHERE (TRUNC(SI.EFFECTIVESETTLEMENTDATE) = '''||tglDesc||''' OR TRUNC(SI.SYSMODIFIED) = '''||tglDesc||''')';

     EXECUTE IMMEDIATE V_SQL;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_SETTLEMENTINSTRUCTION', 'Y', 'Insert into IMP_STG_SETTLEMENTINSTRUCTION');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_TOBA_SETTLEMENTINSTRUCTION DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_SETTLEMENTINSTRUCTION',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_SETTLEMENTINSTRUCTION with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_TOBA_SETTLEMENTINSTRUCTION with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_SETTLEMENTINSTRUCTION

procedure IMP_STG_SETTINSTRDETAILS
is
begin

    select to_char(get_next_bus_date(sysdate,-1),'DD-MON-YYYY') into tglDesc from dual;     
        
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_SETTINSTRDETAILS DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_TOBA_SETTINSTRDETAILS
                    SELECT DE.IDENT_SETTLEMENTINSTRUCTION, DE.DESCRIPTION, DE.TRADEAMOUNT, DE.IDENT_TRADEAMOUNTCURRENCY, 
                    DE.IDENT_TRADEPAYMENTDIRECTION, DE.NUMBEROFDAYSACCRUED, DE.WITHHOLDINGTAX, DE.TRANSACTIONTAX, 
                    DE.YIELD, DE.CHANGEOFBENEFICIALOWNERSHIP, DE.INTERESTRATE, DE.WITHDRAWALADDRESS, DE.REGISTRARREFERENCE, 
                    DE.EXTINTERFACEATTRIBUTE, DE.SYSCREATED, DE.SYSMODIFIED, DE.UPLOADFILENAME, DE.UPLOADSENDER, 
                    DE.UPLOADTIMESTAMP, DE.UPLOADEXTENDEDFILENAME, DE.IDENT_TRANSFERTYPE
                    FROM SETTLEMENTINSTRUCTIONDETAILS@'||v_SchemaProd||' DE
                    WHERE TRUNC(DE.SYSMODIFIED) = '''||tglDesc||'''';

     EXECUTE IMMEDIATE V_SQL;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_SETTINSTRDETAILS', 'Y', 'Insert into IMP_STG_SETTINSTRDETAILS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_TOBA_SETTINSTRDETAILS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_SETTINSTRDETAILS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_SETTINSTRDETAILS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_TOBA_SETTINSTRDETAILS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_SETTINSTRDETAILS

procedure IMP_STG_CASHINSTRUCTION
is
begin

    select to_char(get_next_bus_date(sysdate,-1),'DD-MON-YYYY') into tglDesc from dual;     
        
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_CASHINSTRUCTION DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_TOBA_CASHINSTRUCTION
                    SELECT CI.IDENT_CASHINSTRUCTION, CI.IDENT_INSTRUCTINGPARTY, CI.TRANSACTIONREF, CI.CREATIONTIME, CI.AMOUNT, 
                    CI.IDENT_CURRENCY, CI.ONHOLD, CI.SETTLEMENTDATE, CI.IDENT_CASHINSTRUCTIONSTATUS, CI.IDENT_DEBTOR, CI.IDENT_DEBTORACCOUNT, 
                    CI.IDENT_CREDITOR, CI.IDENT_CREDITORACCOUNT, CI.SETTLEMENTEFFECTIVEDATE, CI.IDENT_DEBITINSTRUCTION, CI.IDENT_CREDITINSTRUCTION, 
                    CI.CORPORATEACTIONREFERENCE, CI.ENDTOENDREFERENCE, CI.IDENT_INSTRUCTIONTYPE, CI.IDENT_ACCOUNT_SERVICING, CI.BENEFICIARYINSTITUTIONCODE, 
                    CI.BENEFICIARYINSTITUTIONBIC, CI.BENEFICIARYACCOUNTTYPE, CI.BENEFICIARYACCOUNTNUMBER, CI.BENEFICIARYACCOUNTNAME, CI.DESCRIPTION, 
                    CI.DEBTOR, CI.DEBTORACCOUNT, CI.CREDITOR, CI.CREDITORACCOUNT, CI.INTERMEDIARY_PAYMENTBANK, CI.SYSCREATED, CI.SYSMODIFIED, 
                    CI.BENEFICIARYINSTITUTIONACCNAME, CI.BENEFICIARYINSTITUTIONACCNB
                    FROM CASHINSTRUCTION@'||v_SchemaProd||' CI
                    WHERE (TRUNC(CI.SETTLEMENTEFFECTIVEDATE) = '''||tglDesc||''' OR TRUNC(CI.SYSMODIFIED) = '''||tglDesc||''')';

     EXECUTE IMMEDIATE V_SQL;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_CASHINSTRUCTION', 'Y', 'Insert into IMP_STG_CASHINSTRUCTION');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_TOBA_CASHINSTRUCTION DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_CASHINSTRUCTION',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_CASHINSTRUCTION with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_TOBA_CASHINSTRUCTION with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_CASHINSTRUCTION

procedure IMP_STG_RESTRICTIONORDER
is
begin

    select to_char(get_next_bus_date(sysdate,-1),'DD-MON-YYYY') into tglDesc from dual;     
        
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_RESTRICTIONORDER DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_TOBA_RESTRICTIONORDER
                    SELECT RO.IDENT_RESTRICTIONORDER, RO.RESTRICTIONTYPE, RO.IDENT_RESTRICTIONSUBTYPE, 
                    RO.USERREFERENCE, RO.CSDCOMMENT, RO.RESTRICTWHOLEACCOUNT, RO.VALIDFROM, 
                    RO.VALIDTO, RO.IDENT_MASTER, RO.IDENT_REVISION, RO.REQUESTED_ACTIVATION_DATE, 
                    RO.REVISION_EFFECTIVE_FROM, RO.REVISION_EFFECTIVE_TO, RO.IDENT_APPROVALSTATE, 
                    RO.IDENT_REVISIONSTATE, RO.IDENT_LASTUPDATEUSER, RO.LASTUPDATETIME, 
                    RO.IDENT_ACTIVATIONTYPE, RO.NOTICE, RO.IDENT_ACCOUNTCOMPOSITE, RO.SYSCREATED, 
                    RO.SYSMODIFIED, RO.CLOSED, 
                    RD.IDENT_RESTRICTIONDETAIL, RD.IDENT_INSTRUMENT, 
                    RD.IDENT_CURRENCY, RD.REQUESTEDQUANTITY, RD.IDENT_RESTRICTIONINSTRUCTION
                    FROM RESTRICTIONORDER@'||v_SchemaProd||' RO
                    LEFT JOIN RESTRICTIONDETAIL@'||v_SchemaProd||' RD ON RO.IDENT_RESTRICTIONORDER = RD.IDENT_RESTRICTIONORDER
                    WHERE RO.IDENT_MASTER IS NULL
                    AND TRUNC(RO.REVISION_EFFECTIVE_FROM) = '''||tglDesc||'''';

     EXECUTE IMMEDIATE V_SQL;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_RESTRICTIONORDER', 'Y', 'Insert into IMP_STG_RESTRICTIONORDER');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_TOBA_RESTRICTIONORDER DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_RESTRICTIONORDER',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_RESTRICTIONORDER with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_TOBA_RESTRICTIONORDER with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_RESTRICTIONORDER

procedure IMP_STG_SETTINSTRSTATUSLOG
is
begin

    select to_char(get_next_bus_date(sysdate,-1),'DD-MON-YYYY') into tglDesc from dual;     
        
    V_SQL := 'DELETE FROM STG_TOBA_SETTINSTRSTATUSLOG SL
                     WHERE TRUNC(SL.SYSMODIFIED) = '''||tglDesc||'''';
    
    EXECUTE IMMEDIATE V_SQL;
    COMMIT;

    V_SQL := 'INSERT INTO STG_TOBA_SETTINSTRSTATUSLOG
                    SELECT SL.ID, SL.IDENT_SETTLEMENTINSTRUCTION, SL.INSTRUCTIONSTATUS, 
                    SL.IDENT_INSTRUCTIONSTATUSREASON, SL.UPDATEDTIME, SL.IDENT_CANCELLATIONSTATUS, 
                    SL.EFFECTIVESETTLEMENTDATE, SL.IDENT_SETTLEMENTSTATUS, SL.NETTINGREFERENCE, 
                    SL.SETTLEDAMOUNT, SL.SETTLEDQUANTITY, SL.IDENT_EXTERNALSETTLEMENTSTATE, 
                    SL.CSDONHOLD, SL.PARTYONHOLD, SL.IDENT_MATCHINGSTATUS, SL.IDENT_CURRENCY, 
                    SL.ONHOLDBYCSDVALIDATION, SL.ONHOLDBYCOSD, SL.ACCOUNTBLOCKED, 
                    SL.EXPECTEDSETTLEMENTDATE, SL.SYSCREATED, SL.SYSMODIFIED, SL.HAVEADVICEMENT, 
                    SL.IDENT_ADVICESTATUS
                    FROM SETTLEMENTINSTRUCTIONSTATUSLOG@'||v_SchemaProd||' SL
                    WHERE SL.INSTRUCTIONSTATUS = ''PENDING''
                    AND SL.IDENT_INSTRUCTIONSTATUSREASON = 38
                    AND TRUNC(SL.SYSMODIFIED) = '''||tglDesc||'''';

     EXECUTE IMMEDIATE V_SQL;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_SETTINSTRSTATUSLOG', 'Y', 'Insert into IMP_STG_SETTINSTRSTATUSLOG');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_TOBA_SETTINSTRSTATUSLOG DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_SETTINSTRSTATUSLOG',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_SETTINSTRSTATUSLOG with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_TOBA_SETTINSTRSTATUSLOG with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_SETTINSTRSTATUSLOG

procedure IMP_STG_PAIREDSYNCINSTRUCTIONS
IS
begin

    SELECT TO_CHAR(GET_NEXT_BUS_DATE(sysdate,-1),'yyyymmdd') INTO TGLDESC FROM DUAL;     

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_SYNC_INSTRUCTIONS2 DROP STORAGE';

    v_sql := 'INSERT INTO TMPSCHEMA.STG_SYNC_INSTRUCTIONS2
                    (INST_TYP, EXT_REF, CODE_BASE_SEC, SEC_DSC, DAT_TRADE, ID_ACCT, CACCT, 
                    ID_MEM, DSC_MEM, TYP_MEM, AMT_INST_SEC, AMT_INST_CASH, DAT_SETTLE, 
                    CODE_STA, LST_UPD_TS, FREE_DSC, ID_INST_CAPCO, INST_ID_INST_CAPCO, TYP_INS, 
                    EXT_BAN_ACCT, CASH_CURR_CODE, PURPOSE, PURPOSE_DSC, TRADE_REF, SETTLEMENT_REASON, 
                    SETTLEMENT_REASON_DSC, CODE_BR, CODE_BR_DSC, INST_ID_INST_CAPCO_SETTINSTR)
                    SELECT IT.CODE INST_TYP, 
                    CASE WHEN SI.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SI.COMMONREFERENCE
                    ELSE SI.INSTRUCTIONREFERENCE END AS EXT_REF,
                    NVL(INS.NAME, CU.CODE_BASE_CURR) CODE_BASE_SEC,
                    NVL(INS.LONGNAME, CU.DSC_CURR) SEC_DSC,
                    SI.TRADEDATE DAT_TRADE,
                    NVL(AG.ID_ACCT, AG1.ID_ACCT) ID_ACCT, 
                    NVL(AG2.ID_ACCT, AG21.ID_ACCT) CACCT, 
                    P2.CODE ID_MEM,
                    P2.LONGNAME DSC_MEM,
                    M2.TYP_MEM TYP_MEM,
                    NVL(SI.VOLUME,0) AMT_INST_SEC, 
                    NVL(SI.AMOUNT,0) AMT_INST_CASH,   
                    SI.SETTLEMENTDATE DAT_SETTLE,
                    CASE WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' 
                    AND (SI.IDENT_INSTRUCTIONTYPE NOT IN (12,14,19,21,2,22) OR SI.IDENT_INSTRUCTIONTYPE IS NULL) THEN 116
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED''  AND SI.IDENT_INSTRUCTIONSTATUSREASON = 43 THEN 149
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONSTATUSREASON = 35 THEN 107
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE IN (1, 21) AND SI.IDENT_INSTRUCTIONSTATUSREASON = 45 THEN 113  
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE NOT IN  (1, 21) AND  SI.IDENT_INSTRUCTIONSTATUSREASON <> 45 THEN 112     
                    WHEN SI.INSTRUCTIONSTATUS = ''PENDING'' AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (1,2,9,10)  THEN 108 
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN  (1, 19, 20, 21) AND SI.INSTRUCTIONSTATUS NOT IN (''SETTLED'', ''CANCELLED'') AND  SI.IDENT_INSTRUCTIONSTATUSREASON IN (7) THEN  109 
                    WHEN (SELECT OPERATINGDATE FROM OPERATINGDATE@'||v_SchemaProd||' WHERE IDENT_OPERATINGDATE = (SELECT MAX(IDENT_OPERATINGDATE) FROM OPERATINGDATE@'||v_SchemaProd||')) >= SI.SETTLEMENTDATE THEN 108    
                    WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'')  AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (14,16) THEN 101 
                    ELSE 102   
                    END CODE_STA,
                    TO_CHAR(NVL(SI.EFFECTIVESETTLEMENTDATE, SI.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS,
                    SI.DESCRIPTION FREE_DSC,
                    TO_CHAR(SI.IDENT_SETTLEMENTINSTRUCTION) ID_INST_CAPCO,
                    TO_CHAR(SIM.IDENT_SETTLEMENTINSTRUCTION) INST_ID_INST_CAPCO,
                    NVL(BS.TYP_SEC,0) TYP_INS,
                    '''' EXT_BAN_ACCT,
                    CU.CODE_BASE_CURR CASH_CURR_CODE,
                    0 PURPOSE,
                    '''' PURPOSE_DSC,
                    '''' TRADE_REF,
                    0 SETTLEMENT_REASON,
                    '''' SETTLEMENT_REASON_DSC,
                    '''' CODE_BR,
                    '''' CODE_BR_DSC,
                    '''' INST_ID_INST_CAPCO_SETTINSTR
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CURRENCIES CU ON SI.IDENT_CURRENCY = CU.ID_INS_CAPCO 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT  
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG ON SA.IDENT_ACCOUNTCOMPOSITE = AG.ID_ACCT_XCSD
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT    
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG1 ON CA.IDENT_ACCOUNTCOMPOSITE = AG1.ID_ACCT_XCSD    
                    LEFT JOIN (  
                    SELECT SIM.* 
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SIM WHERE SIM.MATCHINGREFERENCE != 0
                    AND (SIM.IDENT_PAYMENTDIRECTION = 2 OR SIM.IDENT_SECURITIESMOVEMENT = 2)
                    ) SIM ON SI.MATCHINGREFERENCE = SIM.MATCHINGREFERENCE
                    LEFT JOIN STATIC_PARTICIPANT P2 ON SI.IDENT_COUNTERPART = P2.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA2 ON SIM.IDENT_ACCOUNT = SA2.IDENT_ACCOUNT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG2 ON SA2.IDENT_ACCOUNTCOMPOSITE = AG2.ID_ACCT_XCSD   
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA2 ON SIM.IDENT_CASHACCOUNT = CA2.IDENT_ACCOUNT 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG21 ON CA2.IDENT_ACCOUNTCOMPOSITE = AG21.ID_ACCT_XCSD 
                    LEFT JOIN STATIC_INSTRUMENT INS ON SI.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M2 ON P2.CODE = M2.ID_MEM
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE IT ON SI.IDENT_INSTRUCTIONTYPE = IT.IDENT_INSTRUCTIONTYPE 
                    WHERE IT.CODE IN (
                    ''SYNC''
                    )  
                    AND (
                    SI.MATCHINGREFERENCE = 0 OR
                    SI.IDENT_SETTLEMENTINSTRUCTION != SIM.IDENT_SETTLEMENTINSTRUCTION
                    )  
                    AND INS.IDENT_MASTER IS NULL 
                    AND (SI.IDENT_PAYMENTDIRECTION = 1 OR SI.IDENT_SECURITIESMOVEMENT = 1)';

    EXECUTE IMMEDIATE V_SQL;

    COMMIT;  
    
    INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_PAIREDSYNCINSTRUCTIONS', 'Y', 'Insert into STG_SYNC_INSTRUCTIONS2');

     COMMIT;
        
    DBMS_OUTPUT.PUT_LINE('STG_SYNC_INSTRUCTIONS2 DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_PAIREDSYNCINSTRUCTIONS',
                      'N',
                      ERR_MSG,
                      'Error STG_SYNC_INSTRUCTIONS2 with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE(v_sql);
      DBMS_OUTPUT.PUT_LINE('Error STG_SYNC_INSTRUCTIONS2 with error '||ERR_MSG);
END;--IMP_STG_PAIREDSYNCINSTRUCTIONS

procedure IMP_STG_OTCBONDS
IS
begin

    SELECT TO_CHAR(GET_NEXT_BUS_DATE(sysdate,-1),'yyyymmdd') INTO TGLDESC FROM DUAL;     

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_OTCBONDS DROP STORAGE';

    v_sql := 'INSERT INTO TMPSCHEMA.STG_TOBA_OTCBONDS
                    (ID_OTCBOND_XCSD, EXT_REF, TYP_INST, ID_MEM, ID_ACCT, ID_ACCT_CASH,
                    ID_ACCT_CP, ID_MEM_CP, COUNTERPART_TYPE, INS_TYP_SEC, CODE_BASE_SEC, CODE_BASE_CUR, 
                    INST_ID_INST_XCSD, DAT_MATURITY, DAT_TRADE, DAT_SETTLE, DAT_PURCHASE, FACE_VALUE, 
                    PRICE_PERCENTAGE, PRICE, PROCEED, INTEREST_RATE, ACCRUED_DAYS, 
                    ACCRUED_INTEREST, YIELD, HOLDING_PERIOD_TAX, MISC_AMOUNT, NET_PROCEEDS, 
                    REPORT_TO_EXCHANGE, CAPITAL_GAIN_TAX, FREE_DSC, AGREEMENT_CODE, FLG_CAN, FLG_MATCH, 
                    CODE_INST_PRIO, FLG_TAX, LST_UPD_TS, CODE_STA)
                    SELECT 
                    OB.ID_OTCBOND_XCSD, OB.EXT_REF, C.CODE_SHORT_DSC TYP_INST, OB.ID_MEM, OB.ID_ACCT,
                    OB.ID_ACCT_CASH, OB.ID_ACCT_CP, OB.ID_MEM_CP, C2.CODE_SHORT_DSC COUNTERPART_TYPE,
                    OB.INS_TYP_SEC, OB.CODE_BASE_SEC, OB.CODE_BASE_CUR, OB.INST_ID_INST_XCSD,
                    OB.DAT_MATURITY, OB.DAT_TRADE, OB.DAT_SETTLE, OB.DAT_PURCHASE, OB.FACE_VALUE,
                    OB.PRICE_PERCENTAGE, OB.PRICE, OB.PROCEED, OB.INTEREST_RATE, OB.ACCRUED_DAYS,
                    OB.ACCRUED_INTEREST, OB.YIELD, OB.HOLDING_PERIOD_TAX, OB.MISC_AMOUNT,
                    OB.NET_PROCEEDS, OB.REPORT_TO_EXCHANGE, OB.CAPITAL_GAIN_TAX, OB.FREE_DSC,
                    OB.AGREEMENT_CODE, C3.CODE_SHORT_DSC FLG_CAN, C4.CODE_SHORT_DSC FLG_MATCH,
                    C5.CODE_SHORT_DSC CODE_INST_PRIO, OB.FLG_TAX, OB.LST_UPD_TS, C6.CODE_SHORT_DSC CODE_STA
                    FROM (
                    SELECT 
                    SI.IDENT_SETTLEMENTINSTRUCTION AS ID_OTCBOND_XCSD,
                    CASE WHEN SI.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SI.COMMONREFERENCE
                    ELSE SI.INSTRUCTIONREFERENCE END AS EXT_REF,
                    CASE
                    WHEN SI.IDENT_INSTRUCTIONTYPE = 4 THEN 25 -- DFOPBOND
                    WHEN SI.IDENT_INSTRUCTIONTYPE = 6 THEN 23 -- DVPBOND
                    WHEN SI.IDENT_INSTRUCTIONTYPE = 9 THEN 26 -- RFOPBOND
                    WHEN SI.IDENT_INSTRUCTIONTYPE = 11 THEN 24 -- RVPBOND
                    END AS TYP_INST,
                    P.ID_MEM AS ID_MEM,
                    NVL(AG.ID_ACCT, AG2.ID_ACCT) AS ID_ACCT,
                    CASE WHEN SI.IDENT_ACCOUNT IS NOT NULL THEN AG2.ID_ACCT
                    ELSE NULL END AS ID_ACCT_CASH,
                    CAST (NULL AS VARCHAR2(10)) AS ID_ACCT_CP,
                    CASE WHEN CSDOFCOUNTERPARTY.ID_MEM = ''KSEPB'' THEN EXTP2.CODE ELSE P2.ID_MEM END AS ID_MEM_CP,
                    CASE WHEN  (CSDOFCOUNTERPARTY.ID_MEM = ''KSEPB'' OR CSDOFACCOUNTSERVICING.ID_MEM = ''KSEPB'') THEN 2
                    ELSE 1 END AS COUNTERPART_TYPE,
                    1 AS INS_TYP_SEC,
                    INS.NAME AS CODE_BASE_SEC,
                    NVL(CU.CODE_BASE_CURR, ''IDR'') AS CODE_BASE_CUR,
                    SIM.IDENT_SETTLEMENTINSTRUCTION AS INST_ID_INST_XCSD,
                    INS.MATURITYDATE AS DAT_MATURITY,
                    SI.TRADEDATE AS DAT_TRADE,
                    SI.SETTLEMENTDATE AS DAT_SETTLE,
                    CAST(NULL AS DATE) AS DAT_PURCHASE,
                    SI.VOLUME AS FACE_VALUE,
                    SI.TRADEPRICE PRICE_PERCENTAGE,
                    0 AS PRICE,
                    NVL(SI.AMOUNT,0) AS PROCEED,
                    NVL(DE.INTERESTRATE,0) INTEREST_RATE,
                    NVL(DE.NUMBEROFDAYSACCRUED,0) ACCRUED_DAYS,
                    NVL(SI.ACCRUEDINTERESTAMOUNT,0) ACCRUED_INTEREST,
                    NVL(DE.YIELD,0) YIELD,
                    NVL(DE.WITHHOLDINGTAX,0) AS HOLDING_PERIOD_TAX,
                    0 AS MISC_AMOUNT, 
                    NVL(SI.AMOUNT,0) AS NET_PROCEEDS,
                    1 AS REPORT_TO_EXCHANGE,
                    NVL(DE.TRANSACTIONTAX,0) AS CAPITAL_GAIN_TAX,
                    CASE WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''
                    AND (SI.IDENT_INSTRUCTIONTYPE NOT IN (12,14) OR SI.IDENT_INSTRUCTIONTYPE IS NULL) THEN 116 -- SETTLED
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 12 THEN 104 -- DEPOSITED
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 14 THEN 114 -- WITHDRAWN
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED''  AND SI.IDENT_INSTRUCTIONSTATUSREASON = 43 THEN 149 -- DROPPED
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONSTATUSREASON = 35 THEN 107 -- OVERDUE
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' THEN 112 -- CANCELLED
                    WHEN SI.INSTRUCTIONSTATUS = ''PENDING'' AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (1,2,9,10)  THEN 108 -- FAILED (LACK OF BALANCES)
                    WHEN SI.INSTRUCTIONSTATUS = ''ACCEPTED'' AND SI.IDENT_INSTRUCTIONSTATUSREASON = 40
                    AND (CSDOFCOUNTERPARTY.ID_MEM = ''KSEPB'' OR CSDOFACCOUNTSERVICING.ID_MEM = ''KSEPB'') THEN 147 -- ''WAITING FOR BI CONFIRMATION''
                    WHEN SI.INSTRUCTIONSTATUS NOT IN (''ACCEPTED'',  ''SETTLED'', ''CANCELLED'', ''FAILED'') AND SI.SETTLEMENTPATTERN = 8
                    AND (CSDOFCOUNTERPARTY.ID_MEM = ''KSEPB'' OR CSDOFACCOUNTSERVICING.ID_MEM = ''KSEPB'') THEN 147 -- ''WAITING FOR BI CONFIRMATION''
                    WHEN  SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'') AND SI.PARTYONHOLD = 1 AND SI.IDENT_MATCHINGSTATUS = 2  THEN 101 -- MATCHED PENDING APPROVE
                    WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'') AND SI.PARTYONHOLD = 0 AND SI.IDENT_MATCHINGSTATUS = 2  THEN 101 -- ''MATCHED''
                    WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'') AND SI.IDENT_MATCHINGSTATUS IN (0,1) AND SI.PARTYONHOLD = 1 THEN 101 -- PENDING APPROVE
                    WHEN SI.INSTRUCTIONSTATUS = ''ACCEPTED'' AND (SI.IDENT_MATCHINGSTATUS IN (0, 1)
                    OR SI.IDENT_MATCHINGSTATUS IS NULL) THEN 121 -- UNMATCHED
                    WHEN SI.INSTRUCTIONSTATUS IN (''ACCEPTED'', ''MATCHED'') AND SI.PARTYONHOLD = 0 AND (SELECT OPERATINGDATE FROM OPERATINGDATE@'||v_SchemaProd||' WHERE IDENT_OPERATINGDATE = (SELECT MAX(IDENT_OPERATINGDATE) FROM OPERATINGDATE@'||v_SchemaProd||')) < SI.SETTLEMENTDATE THEN 102 -- READY FOR POSITIONING (SETT DATE IN THE FUTURE)
                    WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'')  AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (14,16) THEN 101 -- VALIDATED
                    ELSE 0 END AS CODE_STA,    
                    SI.DESCRIPTION AS FREE_DSC,
                    CASE WHEN SI.IDENT_CANCELLATIONSTATUS = 2 THEN 1
                    ELSE 0 END AS FLG_CAN,
                    CASE WHEN SI.IDENT_MATCHINGSTATUS IN (0,1) THEN 0
                    ELSE 1 END AS FLG_MATCH,
                    2 AS CODE_INST_PRIO,
                    SI.COMMONREFERENCE AGREEMENT_CODE,
                    0 AS FLG_TAX,
                    TO_CHAR(NVL(SI.EFFECTIVESETTLEMENTDATE, SI.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS,
                    CASE WHEN SI.TRADEREFERENCE IS NOT NULL THEN 1
                    ELSE 2 END AS PURPOSE,
                    SI.TRADEREFERENCE AS TRADE_REF,
                    CASE WHEN SI.IDENT_INSTRUCTIONTYPE IN (6,11) THEN 0
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 52 THEN 1004 -- BART
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE IN (26, 27) THEN 1013 -- COLL
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 28 THEN 1023 -- CONV
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 53 THEN 1008 -- GIFT
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 50 THEN 1002 -- GRAN
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 51 THEN 1003 -- HERI
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 4 THEN 1021 -- IPO
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 55 THEN 1020 -- MSOP
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 56 THEN 1010 -- OTHR
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE IN (7,8,2) THEN 1016 -- OWNE
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 5 THEN 1006 -- REDM
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE IN (12,13) THEN 1022 -- REPO
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE IN (40, 41) THEN 1007 -- SELB
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 14 THEN 1005 -- SUBS
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 1 THEN 1001 -- TRAD
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 54 THEN 1009 -- VERD
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN (4,9) AND SI.IDENT_TRANSACTIONTYPE = 29 THEN 1024 -- ETFT
                    END AS SETTLEMENT_REASON
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS CSDOFCOUNTERPARTY ON SI.IDENT_CSDOFCOUNTERPARTY = CSDOFCOUNTERPARTY.ID_MEM_XCSD
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS CSDOFACCOUNTSERVICING ON SI.IDENT_CSDOFACCOUNTSERVICING = CSDOFACCOUNTSERVICING.ID_MEM_XCSD
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SETTINSTRDETAILS DE ON SI.IDENT_SETTLEMENTINSTRUCTION = DE.IDENT_SETTLEMENTINSTRUCTION
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS P ON SI.IDENT_ACCOUNTSERVICING = P.ID_MEM_XCSD
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CURRENCIES CU ON SI.IDENT_CURRENCY = CU.ID_INS_CAPCO
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG ON SA.IDENT_ACCOUNTCOMPOSITE = AG.ID_ACCT_XCSD
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG2 ON CA.IDENT_ACCOUNTCOMPOSITE = AG2.ID_ACCT_XCSD
                    LEFT JOIN (
                    SELECT SIM.*
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SIM WHERE SIM.MATCHINGREFERENCE <> 0
                    ) SIM ON SI.MATCHINGREFERENCE = SIM.MATCHINGREFERENCE
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS P2 ON SI.IDENT_COUNTERPART = P2.ID_MEM_XCSD
                    LEFT JOIN STATIC_BICOUNTERPARTY EXTP2 ON SI.COUNTERPARTBIC =  EXTP2.BICCODE
                    LEFT JOIN STATIC_INSTRUMENT INS ON SI.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    WHERE 1=1
                    AND SI.IDENT_INSTRUCTIONTYPE IN (4,6,9,11)
                    AND ( 
                    SI.MATCHINGREFERENCE = 0 OR 
                    SI.IDENT_SETTLEMENTINSTRUCTION != SIM.IDENT_SETTLEMENTINSTRUCTION
                    )
                    AND INS.IDENT_MASTER IS NULL
                    AND (SI.INSTRUCTIONREFERENCE NOT LIKE ''CRB%'')
                    ) OB
                    LEFT JOIN CODES C ON C.COL_NM = ''TYP_INST'' AND OB.TYP_INST = C.CODE_VAL
                    LEFT JOIN CODES C2 ON C2.COL_NM = ''COUNTERPART_TYPE'' AND OB.COUNTERPART_TYPE = C2.CODE_VAL 
                    LEFT JOIN CODES C3 ON C3.COL_NM = ''FLG_CAN'' AND OB.FLG_CAN = C3.CODE_VAL
                    LEFT JOIN CODES C4 ON C4.COL_NM = ''FLG_MATCH'' AND OB.FLG_MATCH = C4.CODE_VAL
                    LEFT JOIN CODES C5 ON C5.COL_NM = ''CODE_INST_PRIO'' AND OB.CODE_INST_PRIO = C5.CODE_VAL
                    LEFT JOIN CODES C6 ON C6.COL_NM = ''CODE_INST_STA'' AND OB.CODE_STA = C6.CODE_VAL
                    WHERE CODE_STA IN (     
                        103,104,105,107,112,113,114,116,124,125,129,130,132,133,135,137,139,149     
                    )';

    EXECUTE IMMEDIATE V_SQL;

    COMMIT;     
    
    INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_OTCBONDS', 'Y', 'Insert into IMP_STG_OTCBONDS');

     COMMIT;
     
    DBMS_OUTPUT.PUT_LINE('IMP_STG_OTCBONDS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_OTCBONDS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_OTCBONDS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE(v_sql);
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_OTCBONDS with error '||ERR_MSG);
END;--IMP_STG_OTCBONDS

procedure IMP_STG_INSTRUCTIONS
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_INSTRUCTIONS2 DROP STORAGE';
    
-- OTC
    V_SQL := 'INSERT INTO  TMPSCHEMA.STG_INSTRUCTIONS2(INST_TYP, EXT_REF, CODE_BASE_SEC, SEC_DSC, DAT_TRADE, ID_ACCT, 
                    CACCT, ID_MEM, DSC_MEM, TYP_MEM, AMT_INST_SEC, AMT_INST_CASH, DAT_SETTLE, CODE_STA, LST_UPD_TS,
                    FREE_DSC, ID_INST_CAPCO, INST_ID_INST_CAPCO, TYP_INS, EXT_BAN_ACCT, CASH_CURR_CODE, PURPOSE, PURPOSE_DSC,
                    TRADE_REF, SETTLEMENT_REASON, SETTLEMENT_REASON_DSC, CODE_BR, CODE_BR_DSC, INST_ID_INST_CAPCO_SETTINSTR)
                    SELECT CASE WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE LIKE ''VCA%'' THEN ''VCA''
                     WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NOT NULL THEN ''TECH'' 
                     WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NULL THEN ''CORP''
                     WHEN IT.CODE = ''SECD'' THEN ''DCONF''
                     ELSE IT.CODE END INST_TYP, 
                     CASE WHEN SI.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SI.COMMONREFERENCE
                     ELSE SI.INSTRUCTIONREFERENCE END AS EXT_REF,
                     NVL(INS.NAME, '''') CODE_BASE_SEC,
                     NVL(INS.LONGNAME,'''') SEC_DSC,
                     SI.TRADEDATE DAT_TRADE,
                     NVL(AG.ID_ACCT, AG2.ID_ACCT) AS ID_ACCT,    
                     P2.CODE AS CACCT,
                     P2.CODE AS ID_MEM,
                     P2.LONGNAME DSC_MEM,
                     M2.TYP_MEM,
                     NVL(SI.SETTLEDQUANTITY,0) AS AMT_INST_SEC, 
                     NVL(SI.SETTLEDAMOUNT,0) AS AMT_INST_CASH,
                     SI.SETTLEMENTDATE DAT_SETTLE,
                     CASE WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' 
                     AND (SI.IDENT_INSTRUCTIONTYPE NOT IN (12,14,19,21,2,22) OR SI.IDENT_INSTRUCTIONTYPE IS NULL) THEN 116
                     WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 19 THEN 137   
                     WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 21 THEN 132
                     WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE IN (2,12) THEN 104   
                     WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 14 THEN 114   
                     WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 22 THEN 135
                     WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONSTATUSREASON = 43 THEN 149
                     WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.PARTYONHOLD = 1 AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (13,37,35) THEN 105
                     WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONSTATUSREASON = 35 AND SI.PARTYONHOLD = 0 THEN 107
                     WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE IN (1, 21) AND SI.IDENT_INSTRUCTIONSTATUSREASON = 45 THEN 113
                     WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE NOT IN  (1, 21) AND  SI.IDENT_INSTRUCTIONSTATUSREASON <> 45 THEN 112 
                     WHEN SI.INSTRUCTIONSTATUS = ''PENDING'' AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (1,2,9,10)  THEN 108   
                     WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'') AND SI.PARTYONHOLD = 1 AND SI.IDENT_MATCHINGSTATUS = 2  THEN 101
                     WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'') AND SI.PARTYONHOLD = 0 AND SI.IDENT_MATCHINGSTATUS = 2  THEN 101 
                     WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'') AND SI.IDENT_MATCHINGSTATUS IN (0,1) AND SI.PARTYONHOLD = 1 THEN 101   
                     WHEN SI.INSTRUCTIONSTATUS = ''ACCEPTED'' AND SI.PARTYONHOLD = 0 AND (SI.IDENT_MATCHINGSTATUS IN (0, 1)
                     OR SI.IDENT_MATCHINGSTATUS IS NULL) THEN 121   
                     WHEN SI.IDENT_INSTRUCTIONTYPE IN  (1, 19, 20, 21) AND SI.INSTRUCTIONSTATUS NOT IN (''SETTLED'', ''CANCELLED'') AND  SI.IDENT_INSTRUCTIONSTATUSREASON IN (7) THEN  109 
                     WHEN (SELECT OPERATINGDATE FROM OPERATINGDATE@'||v_SchemaProd||' WHERE IDENT_OPERATINGDATE = (SELECT MAX(IDENT_OPERATINGDATE) FROM OPERATINGDATE@'||v_SchemaProd||')) >= SI.SETTLEMENTDATE THEN 108 
                     WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'')  AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (14,16) THEN 101   
                     ELSE 102
                     END CODE_STA,
                     TO_CHAR(NVL(SI.EFFECTIVESETTLEMENTDATE, SI.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS,
                     SI.DESCRIPTION FREE_DSC,
                     TO_CHAR(SI.IDENT_SETTLEMENTINSTRUCTION) ID_INST_CAPCO,
                     CASE WHEN SIM.IDENT_SETTLEMENTINSTRUCTION = SI.IDENT_SETTLEMENTINSTRUCTION AND SI.INSTRUCTIONSTATUS = ''CANCELLED'' 
                     THEN SI.INSTRUCTIONREFERENCE
                     ELSE NVL(TO_CHAR(SIM.IDENT_SETTLEMENTINSTRUCTION), SI.INSTRUCTIONREFERENCE) END INST_ID_INST_CAPCO,
                     NVL(BS.TYP_SEC,0) TYP_INS,
                     '''' EXT_BAN_ACCT,
                     CU.CODE_BASE_CURR CASH_CURR_CODE,
                     CASE WHEN SI.IDENT_MARKETTYPE = 2 AND IT.CODE LIKE ''_FOP'' THEN 1
                     WHEN SI.IDENT_MARKETTYPE = 1 AND IT.CODE LIKE ''_FOP'' THEN 2 END AS PURPOSE,
                     '''' PURPOSE_DSC,
                     SI.TRADEREFERENCE TRADE_REF, 
                    CASE WHEN IT.CODE LIKE ''_VP'' THEN 0
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 52 THEN 1004 -- BART
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE IN (26, 27) THEN 1013 -- COLL
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 28 THEN 1023 -- CONV
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 53 THEN 1008 -- GIFT
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 50 THEN 1002 -- GRAN
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 51 THEN 1003 -- HERI
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 4 THEN 1021 -- IPO
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 55 THEN 1020 -- MSOP
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 56 THEN 1010 -- OTHR
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE IN (7,8,2) THEN 1016 -- OWNE
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 5 THEN 1006 -- REDM
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE IN (12,13) THEN 1022 -- REPO
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE IN (40, 41) THEN 1007 -- SELB
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 14 THEN 1005 -- SUBS
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 1 THEN 1001 -- TRAD
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 54 THEN 1009 -- VERD
                    WHEN IT.CODE LIKE ''_FOP'' AND SI.IDENT_TRANSACTIONTYPE = 29 THEN 1024 -- ETFT
                    END AS SETTLEMENT_REASON,
                     '''' SETTLEMENT_REASON_DSC,
                     '''' CODE_BR,
                     '''' CODE_BR_DSC,
                     '''' INST_ID_INST_CAPCO_SETTINSTR
                     FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI
                     LEFT JOIN STATIC_PARTICIPANT P ON  SI.IDENT_ACCOUNTSERVICING = P.IDENT_STAKEHOLDER   
                     LEFT JOIN TMPSCHEMA.STG_TOBA_CURRENCIES CU ON SI.IDENT_CURRENCY = CU.ID_INS_CAPCO   
                     LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT     
                     LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG ON SA.IDENT_ACCOUNTCOMPOSITE = AG.ID_ACCT_XCSD
                     LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT 
                     LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG2 ON CA.IDENT_ACCOUNTCOMPOSITE = AG2.ID_ACCT_XCSD
                     LEFT JOIN (   
                         SELECT SIM.*  
                         FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SIM WHERE SIM.MATCHINGREFERENCE != 0   
                     ) SIM ON SI.MATCHINGREFERENCE = SIM.MATCHINGREFERENCE 
                     LEFT JOIN STATIC_PARTICIPANT P2 ON SI.IDENT_COUNTERPART = P2.IDENT_STAKEHOLDER   
                     LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M2 ON P2.CODE = M2.ID_MEM
                     LEFT JOIN STATIC_INSTRUMENT INS ON SI.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT    
                     LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC
                     LEFT JOIN TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE IT ON SI.IDENT_INSTRUCTIONTYPE = IT.IDENT_INSTRUCTIONTYPE 
                     WHERE IT.CODE IN (
                     ''DFOP'', ''DVP'', ''RFOP'', ''RVP'' 
                     ) 
                     AND (     
                     SI.MATCHINGREFERENCE = 0 OR  
                     (SI.IDENT_SETTLEMENTINSTRUCTION != SIM.IDENT_SETTLEMENTINSTRUCTION AND SI.INSTRUCTIONSTATUS != ''CANCELLED'')
                     OR (SI.IDENT_SETTLEMENTINSTRUCTION = SIM.IDENT_SETTLEMENTINSTRUCTION AND SI.INSTRUCTIONSTATUS = ''CANCELLED'')
                     )  
                     AND INS.IDENT_MASTER IS NULL ';

     execute immediate v_sql;

-- SECD
    V_SQL := 'INSERT INTO  TMPSCHEMA.STG_INSTRUCTIONS2(INST_TYP, EXT_REF, CODE_BASE_SEC, SEC_DSC, DAT_TRADE, ID_ACCT, 
                CACCT, ID_MEM, DSC_MEM, TYP_MEM, AMT_INST_SEC, AMT_INST_CASH, DAT_SETTLE, CODE_STA, LST_UPD_TS,
                FREE_DSC, ID_INST_CAPCO, INST_ID_INST_CAPCO, TYP_INS, EXT_BAN_ACCT, CASH_CURR_CODE, PURPOSE, PURPOSE_DSC,
                TRADE_REF, SETTLEMENT_REASON, SETTLEMENT_REASON_DSC, CODE_BR, CODE_BR_DSC, INST_ID_INST_CAPCO_SETTINSTR)
                SELECT CASE WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE LIKE ''VCA%'' THEN ''VCA''
                 WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NOT NULL THEN ''TECH'' 
                 WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NULL THEN ''CORP''
                 WHEN IT.CODE = ''SECD'' THEN ''DCONF''
                 ELSE IT.CODE END INST_TYP, 
                 CASE WHEN SI.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SI.COMMONREFERENCE
                 ELSE SI.INSTRUCTIONREFERENCE END AS EXT_REF,
                 NVL(INS.NAME, CU.CODE_BASE_CURR) CODE_BASE_SEC,
                 NVL(INS.LONGNAME, CU.DSC_CURR) SEC_DSC,
                 SI.TRADEDATE DAT_TRADE,
                 NVL(AG.ID_ACCT, AG1.ID_ACCT) ID_ACCT,
                 NVL(AG2.ID_ACCT, AG21.ID_ACCT) CACCT,
                 P2.CODE ID_MEM,
                 P2.LONGNAME DSC_MEM,
                 MEM.TYP_MEM,
                 NVL(SI.VOLUME,0) AMT_INST_SEC, 
                 NVL(SI.AMOUNT,0) AMT_INST_CASH,
                 SI.SETTLEMENTDATE DAT_SETTLE,
                 CASE WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' 
                 AND (SI.IDENT_INSTRUCTIONTYPE NOT IN (12,14,19,21,2,22) OR SI.IDENT_INSTRUCTIONTYPE IS NULL) THEN 116
                 WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 19 THEN 137 
                 WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 21 THEN 132
                 WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE IN (2,12) THEN 104 
                 WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 14 THEN 114 
                 WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 22 THEN 135
                 WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED''  AND SI.IDENT_INSTRUCTIONSTATUSREASON = 43 THEN 149
                 WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONSTATUSREASON = 35 THEN 107
                 WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE IN (1, 21) AND SI.IDENT_INSTRUCTIONSTATUSREASON = 45 THEN 113  
                 WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE NOT IN  (1, 21) AND  SI.IDENT_INSTRUCTIONSTATUSREASON <> 45 THEN 112     
                 WHEN SI.INSTRUCTIONSTATUS = ''PENDING'' AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (1,2,9,10)  THEN 108 
                 WHEN SI.INSTRUCTIONSTATUS = ''ACCEPTED'' AND SI.PARTYONHOLD = 0 AND (SI.IDENT_MATCHINGSTATUS IN (0, 1)
                 OR SI.IDENT_MATCHINGSTATUS IS NULL) THEN 121     
                 WHEN SI.IDENT_INSTRUCTIONTYPE IN  (1, 19, 20, 21) AND SI.INSTRUCTIONSTATUS NOT IN (''SETTLED'', ''CANCELLED'') AND  SI.IDENT_INSTRUCTIONSTATUSREASON IN (7) THEN  109 
                 WHEN (SELECT OPERATINGDATE FROM OPERATINGDATE@'||v_SchemaProd||' WHERE IDENT_OPERATINGDATE = (SELECT MAX(IDENT_OPERATINGDATE) FROM OPERATINGDATE@'||v_SchemaProd||')) >= SI.SETTLEMENTDATE THEN 108    
                 WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'')  AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (14,16) THEN 101 
                 ELSE 102   
                 END CODE_STA,
                 TO_CHAR(NVL(SI.EFFECTIVESETTLEMENTDATE, SI.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS,
                 SI.DESCRIPTION FREE_DSC,
                 TO_CHAR(SI.IDENT_SETTLEMENTINSTRUCTION) ID_INST_CAPCO,
                 TO_CHAR(SIM.IDENT_SETTLEMENTINSTRUCTION) INST_ID_INST_CAPCO,
                 NVL(BS.TYP_SEC,0) TYP_INS,
                 CAST (NULL AS VARCHAR2(10)) AS EXT_BAN_ACCT,
                 CU.CODE_BASE_CURR CASH_CURR_CODE,
                 0 PURPOSE,
                 '''' PURPOSE_DSC,
                 '''' TRADE_REF,
                 0 SETTLEMENT_REASON,
                 '''' SETTLEMENT_REASON_DSC,
                 '''' CODE_BR,
                 '''' CODE_BR_DSC,
                 '''' INST_ID_INST_CAPCO_SETTINSTR
                   FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI
                 LEFT JOIN STATIC_PARTICIPANT P ON  SI.IDENT_ACCOUNTSERVICING = P.IDENT_STAKEHOLDER 
                 LEFT JOIN TMPSCHEMA.STG_TOBA_CURRENCIES CU ON SI.IDENT_CURRENCY = CU.ID_INS_CAPCO 
                 LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT  
                 LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG ON SA.IDENT_ACCOUNTCOMPOSITE = AG.ID_ACCT_XCSD
                 LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT    
                 LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG1 ON CA.IDENT_ACCOUNTCOMPOSITE = AG1.ID_ACCT_XCSD    
                 LEFT JOIN (  
                 SELECT SIM.* 
                 FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SIM WHERE SIM.MATCHINGREFERENCE != 0
                 AND SIM.IDENT_SECURITIESMOVEMENT = 1
                 ) SIM ON SI.MATCHINGREFERENCE = SIM.MATCHINGREFERENCE
                LEFT JOIN STATIC_PARTICIPANT P2 ON SI.IDENT_COUNTERPART = P2.IDENT_STAKEHOLDER
                LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA2 ON SIM.IDENT_ACCOUNT = SA2.IDENT_ACCOUNT
                LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG2 ON SA2.IDENT_ACCOUNTCOMPOSITE = AG2.ID_ACCT_XCSD   
                LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA2 ON SIM.IDENT_CASHACCOUNT = CA2.IDENT_ACCOUNT 
                LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG21 ON CA2.IDENT_ACCOUNTCOMPOSITE = AG21.ID_ACCT_XCSD 
                LEFT JOIN STATIC_INSTRUMENT INS ON SI.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS MEM ON P2.CODE = MEM.ID_MEM
                LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC 
                LEFT JOIN TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE IT ON SI.IDENT_INSTRUCTIONTYPE = IT.IDENT_INSTRUCTIONTYPE
                 WHERE IT.CODE IN (
                  ''SECD''
                 )  
                 AND (
                 SI.MATCHINGREFERENCE = 0 OR
                 SI.IDENT_SETTLEMENTINSTRUCTION != SIM.IDENT_SETTLEMENTINSTRUCTION
                 )
                 AND INS.IDENT_MASTER IS NULL 
                 AND SI.IDENT_SECURITIESMOVEMENT = 2';
         
     execute immediate v_sql;
    
 -- PAIRED INSTRUCTIONS EXCLUDE SECD AND SECW
 
    V_SQL := '    INSERT INTO  TMPSCHEMA.STG_INSTRUCTIONS2(INST_TYP, EXT_REF, CODE_BASE_SEC, SEC_DSC, DAT_TRADE, ID_ACCT, 
                    CACCT, ID_MEM, DSC_MEM, TYP_MEM, AMT_INST_SEC, AMT_INST_CASH, DAT_SETTLE, CODE_STA, LST_UPD_TS,
                    FREE_DSC, ID_INST_CAPCO, INST_ID_INST_CAPCO, TYP_INS, EXT_BAN_ACCT, CASH_CURR_CODE, PURPOSE, PURPOSE_DSC,
                    TRADE_REF, SETTLEMENT_REASON, SETTLEMENT_REASON_DSC, CODE_BR, CODE_BR_DSC, INST_ID_INST_CAPCO_SETTINSTR)
                       SELECT CASE WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE LIKE ''VCA%'' THEN ''VCA''
                        WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NOT NULL THEN ''TECH'' 
                        WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NULL THEN ''CORP''
                        WHEN IT.CODE = ''REALGN'' THEN ''REALIGN''
                        WHEN IT.CODE = ''SECD'' THEN ''DCONF''
                        ELSE IT.CODE END INST_TYP, 
                        CASE WHEN SI.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SI.COMMONREFERENCE
                        ELSE SI.INSTRUCTIONREFERENCE END AS EXT_REF,
                        NVL(INS.NAME, CU.CODE_BASE_CURR) CODE_BASE_SEC,
                        NVL(INS.LONGNAME, CU.DSC_CURR) SEC_DSC,
                        SI.TRADEDATE DAT_TRADE,
                        NVL(AG.ID_ACCT, AG1.ID_ACCT) ID_ACCT, 
                        NVL(AG2.ID_ACCT, AG21.ID_ACCT) CACCT, 
                        CASE WHEN IT.CODE = ''WT'' AND (AG2.ID_ACCT = ''KSEPB000000170'' OR AG21.ID_ACCT = ''KSEPB000000170'') THEN CI.BENEFICIARYINSTITUTIONCODE
                        WHEN IT.CODE IN (''WT'', ''SWPAI'') THEN NVL(SUBSTR(AG2.ID_ACCT,1,5), SUBSTR(AG21.ID_ACCT,1,5))
                        ELSE P2.CODE END ID_MEM,
                        CASE WHEN IT.CODE = ''WT'' AND (AG2.ID_ACCT = ''KSEPB000000170'' OR AG21.ID_ACCT = ''KSEPB000000170'') THEN CI.BENEFICIARYINSTITUTIONACCNAME
                        WHEN IT.CODE IN (''WT'', ''SWPAI'') THEN M4.DSC_MEM
                        ELSE P2.LONGNAME END DSC_MEM,
                        CASE WHEN IT.CODE = ''WT'' AND (AG2.ID_ACCT = ''KSEPB000000170'' OR AG21.ID_ACCT = ''KSEPB000000170'') THEN M3.TYP_MEM
                        WHEN IT.CODE IN (''WT'', ''SWPAI'') THEN M4.TYP_MEM
                        ELSE M2.TYP_MEM END TYP_MEM,
                        NVL(SI.VOLUME,0) AMT_INST_SEC, 
                        NVL(SI.AMOUNT,0) AMT_INST_CASH,   
                        SI.SETTLEMENTDATE DAT_SETTLE,
                        CASE WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' 
                        AND (SI.IDENT_INSTRUCTIONTYPE NOT IN (12,14,19,21,2,22) OR SI.IDENT_INSTRUCTIONTYPE IS NULL) THEN 116
                        WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 19 THEN 137 
                        WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 21 THEN 132
                        WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE IN (2,12) THEN 104 
                        WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 14 THEN 114
                        WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 22 THEN 135
                        WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED''  AND SI.IDENT_INSTRUCTIONSTATUSREASON = 43 THEN 149
                        WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONSTATUSREASON = 35 THEN 107
                        WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE IN (1, 21) AND SI.IDENT_INSTRUCTIONSTATUSREASON = 45 THEN 113  
                        WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE NOT IN  (1, 21) AND  SI.IDENT_INSTRUCTIONSTATUSREASON <> 45 THEN 112     
                        WHEN SI.INSTRUCTIONSTATUS = ''PENDING'' AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (1,2,9,10)  THEN 108 
                        WHEN SI.INSTRUCTIONSTATUS = ''ACCEPTED'' AND SI.PARTYONHOLD = 0 AND (SI.IDENT_MATCHINGSTATUS IN (0, 1)
                        OR SI.IDENT_MATCHINGSTATUS IS NULL) THEN 121     
                        WHEN SI.IDENT_INSTRUCTIONTYPE IN  (1, 19, 20, 21) AND SI.INSTRUCTIONSTATUS NOT IN (''SETTLED'', ''CANCELLED'') AND  SI.IDENT_INSTRUCTIONSTATUSREASON IN (7) THEN  109 
                        WHEN (SELECT OPERATINGDATE FROM OPERATINGDATE@'||v_SchemaProd||' WHERE IDENT_OPERATINGDATE = (SELECT MAX(IDENT_OPERATINGDATE) FROM OPERATINGDATE@'||v_SchemaProd||')) >= SI.SETTLEMENTDATE THEN 108    
                        WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'')  AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (14,16) THEN 101 
                        ELSE 102   
                        END CODE_STA,
                        TO_CHAR(NVL(SI.EFFECTIVESETTLEMENTDATE, SI.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS,
                        SI.DESCRIPTION FREE_DSC,
                        TO_CHAR(SI.IDENT_SETTLEMENTINSTRUCTION) ID_INST_CAPCO,
                        TO_CHAR(SIM.IDENT_SETTLEMENTINSTRUCTION) INST_ID_INST_CAPCO,
                        NVL(BS.TYP_SEC,0) TYP_INS,
                        CAST (NULL AS VARCHAR2(10)) AS EXT_BAN_ACCT,
                        CU.CODE_BASE_CURR CASH_CURR_CODE,
                        0 PURPOSE,
                        '''' PURPOSE_DSC,
                        '''' TRADE_REF,
                        0 SETTLEMENT_REASON,
                        '''' SETTLEMENT_REASON_DSC,
                        '''' CODE_BR,
                        '''' CODE_BR_DSC,
                        '''' INST_ID_INST_CAPCO_SETTINSTR
                        FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI
                        LEFT JOIN TMPSCHEMA.STG_TOBA_CASHINSTRUCTION CI ON SI.IDENT_SETTLEMENTINSTRUCTION = CI.IDENT_DEBITINSTRUCTION 
                        LEFT JOIN STATIC_PARTICIPANT P ON  SI.IDENT_ACCOUNTSERVICING = P.IDENT_STAKEHOLDER 
                        LEFT JOIN TMPSCHEMA.STG_TOBA_CURRENCIES CU ON SI.IDENT_CURRENCY = CU.ID_INS_CAPCO 
                        LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT  
                        LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG ON SA.IDENT_ACCOUNTCOMPOSITE = AG.ID_ACCT_XCSD
                        LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT    
                        LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG1 ON CA.IDENT_ACCOUNTCOMPOSITE = AG1.ID_ACCT_XCSD    
                        LEFT JOIN (  
                        SELECT SIM.* 
                        FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SIM WHERE SIM.MATCHINGREFERENCE != 0
                        AND (SIM.IDENT_PAYMENTDIRECTION = 2 OR SIM.IDENT_SECURITIESMOVEMENT = 2)
                        ) SIM ON SI.MATCHINGREFERENCE = SIM.MATCHINGREFERENCE
                        LEFT JOIN STATIC_PARTICIPANT P2 ON SI.IDENT_COUNTERPART = P2.IDENT_STAKEHOLDER
                        LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA2 ON SIM.IDENT_ACCOUNT = SA2.IDENT_ACCOUNT
                        LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG2 ON SA2.IDENT_ACCOUNTCOMPOSITE = AG2.ID_ACCT_XCSD   
                        LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA2 ON SIM.IDENT_CASHACCOUNT = CA2.IDENT_ACCOUNT 
                        LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG21 ON CA2.IDENT_ACCOUNTCOMPOSITE = AG21.ID_ACCT_XCSD 
                        LEFT JOIN STATIC_INSTRUMENT INS ON SI.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                        LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M2 ON P2.CODE = M2.ID_MEM
                        LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M3 ON CI.BENEFICIARYINSTITUTIONCODE = M3.ID_MEM
                        LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M4 ON SUBSTR(AG21.ID_ACCT,1,5) = M4.ID_MEM
                        LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC 
                        LEFT JOIN TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE IT ON SI.IDENT_INSTRUCTIONTYPE = IT.IDENT_INSTRUCTIONTYPE 
                        WHERE IT.CODE IN (
                        ''REALGN'', ''COLDS'', ''LENDDS'', ''SECTRS'', ''CSTR'',''BTS'', ''CNF'', ''SWPAI'', ''WT'' 
                        )  
                        AND (
                        SI.MATCHINGREFERENCE = 0 OR
                        SI.IDENT_SETTLEMENTINSTRUCTION != SIM.IDENT_SETTLEMENTINSTRUCTION
                        )  
                        AND INS.IDENT_MASTER IS NULL 
                        AND (SI.IDENT_PAYMENTDIRECTION = 1 OR SI.IDENT_SECURITIESMOVEMENT = 1)';
    
    execute immediate v_sql;
    
    -- secw + wconf
    
    V_SQL := 'INSERT INTO  TMPSCHEMA.STG_INSTRUCTIONS2(INST_TYP, EXT_REF, CODE_BASE_SEC, SEC_DSC, DAT_TRADE, ID_ACCT, 
                    CACCT, ID_MEM, DSC_MEM, TYP_MEM, AMT_INST_SEC, AMT_INST_CASH, DAT_SETTLE, CODE_STA, LST_UPD_TS,
                    FREE_DSC, ID_INST_CAPCO, INST_ID_INST_CAPCO, TYP_INS, EXT_BAN_ACCT, CASH_CURR_CODE, PURPOSE, PURPOSE_DSC,
                    TRADE_REF, SETTLEMENT_REASON, SETTLEMENT_REASON_DSC, CODE_BR, CODE_BR_DSC, INST_ID_INST_CAPCO_SETTINSTR)
                    SELECT CASE WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE LIKE ''VCA%'' THEN ''VCA''
                    WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NOT NULL THEN ''TECH'' 
                    WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NULL THEN ''CORP''
                    WHEN IT.CODE = ''REALGN'' THEN ''REALIGN''
                    WHEN IT.CODE = ''SECD'' THEN ''DCONF''
                    ELSE IT.CODE END INST_TYP, 
                    CASE WHEN SI.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SI.COMMONREFERENCE
                    ELSE SI.INSTRUCTIONREFERENCE END AS EXT_REF,
                    NVL(INS.NAME, CU.CODE_BASE_CURR) CODE_BASE_SEC,
                    NVL(INS.LONGNAME, CU.DSC_CURR) SEC_DSC,
                    SI.TRADEDATE DAT_TRADE,
                    NVL(AG.ID_ACCT, AG1.ID_ACCT) ID_ACCT, 
                    NVL(NVL(AG2.ID_ACCT, AG21.ID_ACCT), ACM2.ID_ACCT) CACCT, 
                    CASE WHEN IT.CODE = ''WT'' AND (AG2.ID_ACCT = ''KSEPB000000170'' OR AG21.ID_ACCT = ''KSEPB000000170'') THEN CI.BENEFICIARYINSTITUTIONCODE
                    WHEN IT.CODE IN (''WT'', ''SWPAI'') THEN NVL(SUBSTR(AG2.ID_ACCT,1,5), SUBSTR(AG21.ID_ACCT,1,5))
                    ELSE P2.CODE END ID_MEM,
                    CASE WHEN IT.CODE = ''WT'' AND (AG2.ID_ACCT = ''KSEPB000000170'' OR AG21.ID_ACCT = ''KSEPB000000170'') THEN CI.BENEFICIARYINSTITUTIONACCNAME
                    WHEN IT.CODE IN (''WT'', ''SWPAI'') THEN M4.DSC_MEM
                    ELSE P2.LONGNAME END DSC_MEM,
                    CASE WHEN IT.CODE = ''WT'' AND (AG2.ID_ACCT = ''KSEPB000000170'' OR AG21.ID_ACCT = ''KSEPB000000170'') THEN M3.TYP_MEM
                    WHEN IT.CODE IN (''WT'', ''SWPAI'') THEN M4.TYP_MEM
                    ELSE M2.TYP_MEM END TYP_MEM,
                    NVL(SI.VOLUME,0) AMT_INST_SEC, 
                    NVL(SI.AMOUNT,0) AMT_INST_CASH,   
                    SI.SETTLEMENTDATE DAT_SETTLE,
                    CASE WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' 
                    AND (SI.IDENT_INSTRUCTIONTYPE NOT IN (12,14,19,21,2,22) OR SI.IDENT_INSTRUCTIONTYPE IS NULL) THEN 116
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 19 THEN 137 
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 21 THEN 132
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE IN (2,12) THEN 104 
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 14 THEN 114
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 22 THEN 135
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED''  AND SI.IDENT_INSTRUCTIONSTATUSREASON = 43 THEN 149
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONSTATUSREASON = 35 THEN 107
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE IN (1, 21) AND SI.IDENT_INSTRUCTIONSTATUSREASON = 45 THEN 113  
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE NOT IN  (1, 21) AND  SI.IDENT_INSTRUCTIONSTATUSREASON <> 45 THEN 112     
                    WHEN SI.INSTRUCTIONSTATUS = ''PENDING'' AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (1,2,9,10)  THEN 108 
                    WHEN SI.INSTRUCTIONSTATUS = ''ACCEPTED'' AND SI.PARTYONHOLD = 0 AND (SI.IDENT_MATCHINGSTATUS IN (0, 1)
                    OR SI.IDENT_MATCHINGSTATUS IS NULL) THEN 121     
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN  (1, 19, 20, 21) AND SI.INSTRUCTIONSTATUS NOT IN (''SETTLED'', ''CANCELLED'') AND  SI.IDENT_INSTRUCTIONSTATUSREASON IN (7) THEN  109 
                    WHEN (SELECT OPERATINGDATE FROM OPERATINGDATE@'||v_SchemaProd||' WHERE IDENT_OPERATINGDATE = (SELECT MAX(IDENT_OPERATINGDATE) FROM OPERATINGDATE@'||v_SchemaProd||')) >= SI.SETTLEMENTDATE THEN 108    
                    WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'')  AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (14,16) THEN 101 
                    ELSE 102   
                    END CODE_STA,
                    TO_CHAR(NVL(SI.EFFECTIVESETTLEMENTDATE, SI.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS,
                    SI.DESCRIPTION FREE_DSC,
                    TO_CHAR(SI.IDENT_SETTLEMENTINSTRUCTION) ID_INST_CAPCO,
                    NVL(TO_CHAR(SIM.IDENT_SETTLEMENTINSTRUCTION), SI.INSTRUCTIONREFERENCE) INST_ID_INST_CAPCO,
                    NVL(BS.TYP_SEC,0) TYP_INS,
                    CAST (NULL AS VARCHAR2(10)) AS EXT_BAN_ACCT,
                    CU.CODE_BASE_CURR CASH_CURR_CODE,
                    0 PURPOSE,
                    '''' PURPOSE_DSC,
                    '''' TRADE_REF,
                    0 SETTLEMENT_REASON,
                    '''' SETTLEMENT_REASON_DSC,
                    '''' CODE_BR,
                    '''' CODE_BR_DSC,
                    '''' INST_ID_INST_CAPCO_SETTINSTR
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHINSTRUCTION CI ON SI.IDENT_SETTLEMENTINSTRUCTION = CI.IDENT_DEBITINSTRUCTION 
                    LEFT JOIN STATIC_PARTICIPANT P ON  SI.IDENT_ACCOUNTSERVICING = P.IDENT_STAKEHOLDER 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CURRENCIES CU ON SI.IDENT_CURRENCY = CU.ID_INS_CAPCO 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT  
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG ON SA.IDENT_ACCOUNTCOMPOSITE = AG.ID_ACCT_XCSD
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT    
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG1 ON CA.IDENT_ACCOUNTCOMPOSITE = AG1.ID_ACCT_XCSD    
                    LEFT JOIN (  
                    SELECT SIM.* 
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SIM WHERE SIM.MATCHINGREFERENCE != 0
                    AND (SIM.IDENT_PAYMENTDIRECTION = 2 OR SIM.IDENT_SECURITIESMOVEMENT = 2)
                    ) SIM ON SI.MATCHINGREFERENCE = SIM.MATCHINGREFERENCE
                    LEFT JOIN STATIC_PARTICIPANT P2 ON SI.IDENT_COUNTERPART = P2.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA2 ON SIM.IDENT_ACCOUNT = SA2.IDENT_ACCOUNT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG2 ON SA2.IDENT_ACCOUNTCOMPOSITE = AG2.ID_ACCT_XCSD   
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA2 ON SIM.IDENT_CASHACCOUNT = CA2.IDENT_ACCOUNT 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG21 ON CA2.IDENT_ACCOUNTCOMPOSITE = AG21.ID_ACCT_XCSD 
                    LEFT JOIN STATIC_INSTRUMENT INS ON SI.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M2 ON P2.CODE = M2.ID_MEM
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS ACM2 ON M2.ID_MEM = ACM2.ID_MEM
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M3 ON CI.BENEFICIARYINSTITUTIONCODE = M3.ID_MEM
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M4 ON SUBSTR(AG21.ID_ACCT,1,5) = M4.ID_MEM
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE IT ON SI.IDENT_INSTRUCTIONTYPE = IT.IDENT_INSTRUCTIONTYPE 
                    WHERE IT.CODE IN (
                    ''SECW''
                    )  
                    AND (
                    SI.MATCHINGREFERENCE = 0 OR
                    SI.IDENT_SETTLEMENTINSTRUCTION != SIM.IDENT_SETTLEMENTINSTRUCTION
                    )  
                    AND INS.IDENT_MASTER IS NULL 
                    AND (SI.IDENT_PAYMENTDIRECTION = 1 OR SI.IDENT_SECURITIESMOVEMENT = 1)
                    AND SI.INSTRUCTIONREFERENCE NOT LIKE ''CRB%''
                    UNION ALL
                    SELECT ''WCONF'' INST_TYP, 
                    SIM.INSTRUCTIONREFERENCE EXT_REF,
                    NVL(INS.NAME, CU.CODE_BASE_CURR) CODE_BASE_SEC,
                    NVL(INS.LONGNAME, CU.DSC_CURR) SEC_DSC,
                    SI.TRADEDATE DAT_TRADE,
                    NVL(AG2.ID_ACCT, AG21.ID_ACCT) ID_ACCT, 
                    NVL(AG.ID_ACCT, AG1.ID_ACCT) CACCT, 
                    P2.CODE ID_MEM,
                    P2.LONGNAME DSC_MEM,
                    M2.TYP_MEM TYP_MEM,
                    NVL(SI.VOLUME,0) AMT_INST_SEC, 
                    NVL(SI.AMOUNT,0) AMT_INST_CASH,   
                    SI.SETTLEMENTDATE DAT_SETTLE,
                    114 CODE_STA,
                    TO_CHAR(NVL(SI.EFFECTIVESETTLEMENTDATE, SI.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS,
                    SI.DESCRIPTION FREE_DSC,
                    TO_CHAR(SI.IDENT_SETTLEMENTINSTRUCTION) ID_INST_CAPCO,
                    TO_CHAR(SIM.IDENT_SETTLEMENTINSTRUCTION) INST_ID_INST_CAPCO,
                    NVL(BS.TYP_SEC,0) TYP_INS,
                    '''' EXT_BAN_ACCT,
                    CU.CODE_BASE_CURR CASH_CURR_CODE,
                    0 PURPOSE,
                    '''' PURPOSE_DSC,
                    '''' TRADE_REF,
                    0 SETTLEMENT_REASON,
                    '''' SETTLEMENT_REASON_DSC,
                    '''' CODE_BR,
                    '''' CODE_BR_DSC,
                    '''' INST_ID_INST_CAPCO_SETTINSTR
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI
                    LEFT JOIN (  
                    SELECT SIM.* 
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SIM WHERE SIM.MATCHINGREFERENCE != 0
                    AND (SIM.IDENT_PAYMENTDIRECTION = 2 OR SIM.IDENT_SECURITIESMOVEMENT = 2)
                    ) SIM ON SI.MATCHINGREFERENCE = SIM.MATCHINGREFERENCE
                    LEFT JOIN STATIC_PARTICIPANT P ON  SI.IDENT_ACCOUNTSERVICING = P.IDENT_STAKEHOLDER 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CURRENCIES CU ON SI.IDENT_CURRENCY = CU.ID_INS_CAPCO 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT  
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG ON SA.IDENT_ACCOUNTCOMPOSITE = AG.ID_ACCT_XCSD
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT    
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG1 ON CA.IDENT_ACCOUNTCOMPOSITE = AG1.ID_ACCT_XCSD    
                    LEFT JOIN STATIC_PARTICIPANT P2 ON SIM.IDENT_COUNTERPART = P2.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA2 ON SIM.IDENT_ACCOUNT = SA2.IDENT_ACCOUNT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG2 ON SA2.IDENT_ACCOUNTCOMPOSITE = AG2.ID_ACCT_XCSD   
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA2 ON SIM.IDENT_CASHACCOUNT = CA2.IDENT_ACCOUNT 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG21 ON CA2.IDENT_ACCOUNTCOMPOSITE = AG21.ID_ACCT_XCSD 
                    LEFT JOIN STATIC_INSTRUMENT INS ON SI.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M2 ON P2.CODE = M2.ID_MEM
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE IT ON SI.IDENT_INSTRUCTIONTYPE = IT.IDENT_INSTRUCTIONTYPE 
                    WHERE IT.CODE IN (
                    ''SECW''
                    )  
                    AND (
                    SI.MATCHINGREFERENCE = 0 OR
                    SI.IDENT_SETTLEMENTINSTRUCTION != SIM.IDENT_SETTLEMENTINSTRUCTION
                    )  
                    AND INS.IDENT_MASTER IS NULL 
                    AND SI.INSTRUCTIONREFERENCE LIKE ''CRB%''
                    AND SI.INSTRUCTIONSTATUS = ''SETTLED''
                    AND (SI.IDENT_PAYMENTDIRECTION = 1 OR SI.IDENT_SECURITIESMOVEMENT = 1)';
    
     EXECUTE IMMEDIATE V_SQL;
     
    -- otc bond
V_SQL := 'INSERT INTO  TMPSCHEMA.STG_INSTRUCTIONS2(INST_TYP, EXT_REF, CODE_BASE_SEC, SEC_DSC, DAT_TRADE, ID_ACCT, 
                    CACCT, ID_MEM, DSC_MEM, TYP_MEM, AMT_INST_SEC, AMT_INST_CASH, DAT_SETTLE, CODE_STA, LST_UPD_TS,
                    FREE_DSC, ID_INST_CAPCO, INST_ID_INST_CAPCO, TYP_INS, EXT_BAN_ACCT, CASH_CURR_CODE, PURPOSE, PURPOSE_DSC,
                    TRADE_REF, SETTLEMENT_REASON, SETTLEMENT_REASON_DSC, CODE_BR, CODE_BR_DSC, INST_ID_INST_CAPCO_SETTINSTR)
                    SELECT CASE WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE LIKE ''VCA%'' THEN ''VCA''
                    WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NOT NULL THEN ''TECH'' 
                    WHEN SI.IDENT_TRANSACTIONTYPE = 3 AND SI.CORPORATEACTIONREFERENCE IS NULL THEN ''CORP''
                    WHEN IT.CODE = ''SECD'' THEN ''DCONF''
                    ELSE IT.CODE END INST_TYP, 
                    CASE WHEN SI.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SI.COMMONREFERENCE
                    ELSE SI.INSTRUCTIONREFERENCE END AS EXT_REF,
                    NVL(INS.NAME, '''') CODE_BASE_SEC,
                    NVL(INS.LONGNAME,'''') SEC_DSC,
                    SI.TRADEDATE DAT_TRADE,
                    NVL(AG.ID_ACCT, AG2.ID_ACCT) AS ID_ACCT,
                    CASE WHEN  (CSDOFCOUNTERPARTY.CODE = ''KSEPB'' OR CSDOFACCOUNTSERVICING.CODE = ''KSEPB'')  THEN BICN.CODE 
                    ELSE P2.CODE END AS CACCT,
                    CASE WHEN  (CSDOFCOUNTERPARTY.CODE = ''KSEPB'' OR CSDOFACCOUNTSERVICING.CODE = ''KSEPB'')  THEN BICN.CODE 
                    ELSE P2.CODE END AS ID_MEM,
                    CASE WHEN  (CSDOFCOUNTERPARTY.CODE = ''KSEPB'' OR CSDOFACCOUNTSERVICING.CODE = ''KSEPB'')  THEN BICN.LONGNAME 
                    ELSE P2.LONGNAME END DSC_MEM,
                    M2.TYP_MEM TYP_MEM, 
                    NVL(SI.VOLUME,0) AS AMT_INST_SEC, 
                    NVL(SI.AMOUNT,0) AS AMT_INST_CASH, 
                    SI.SETTLEMENTDATE DAT_SETTLE,
                    CASE WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' 
                    AND (SI.IDENT_INSTRUCTIONTYPE NOT IN (12,14,19,21,2,22) OR SI.IDENT_INSTRUCTIONTYPE IS NULL) THEN 116
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 19 THEN 137
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED'' AND SI.IDENT_INSTRUCTIONTYPE = 21 THEN 132
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE IN (2,12) THEN 104
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 14 THEN 114
                    WHEN SI.INSTRUCTIONSTATUS = ''SETTLED''  AND SI.IDENT_INSTRUCTIONTYPE = 22 THEN 135
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED''  AND SI.IDENT_INSTRUCTIONSTATUSREASON = 43 THEN 149
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONSTATUSREASON = 35 AND SI.PARTYONHOLD = 0 THEN 107
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.PARTYONHOLD = 1 AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (13,37,35) THEN 105
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE IN (1, 21) AND SI.IDENT_INSTRUCTIONSTATUSREASON = 45 THEN 113
                    WHEN SI.INSTRUCTIONSTATUS = ''CANCELLED'' AND SI.IDENT_INSTRUCTIONTYPE NOT IN  (1, 21) AND  SI.IDENT_INSTRUCTIONSTATUSREASON <> 45 THEN 112 
                    WHEN SI.INSTRUCTIONSTATUS = ''PENDING'' AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (1,2,9,10)  THEN 108
                    WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'') AND SI.PARTYONHOLD = 1 AND SI.IDENT_MATCHINGSTATUS = 2  THEN 101
                    WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'') AND SI.PARTYONHOLD = 0 AND SI.IDENT_MATCHINGSTATUS = 2  THEN 101 
                    WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'') AND SI.IDENT_MATCHINGSTATUS IN (0,1) AND SI.PARTYONHOLD = 1 THEN 101
                    WHEN SI.INSTRUCTIONSTATUS = ''ACCEPTED'' AND SI.PARTYONHOLD = 0 AND (SI.IDENT_MATCHINGSTATUS IN (0, 1)
                    OR SI.IDENT_MATCHINGSTATUS IS NULL) THEN 121
                    WHEN SI.IDENT_INSTRUCTIONTYPE IN  (1, 19, 20, 21) AND SI.INSTRUCTIONSTATUS NOT IN (''SETTLED'', ''CANCELLED'') AND  SI.IDENT_INSTRUCTIONSTATUSREASON IN (7) THEN  109 
                    WHEN (SELECT OPERATINGDATE FROM OPERATINGDATE@'||v_SchemaProd||' WHERE IDENT_OPERATINGDATE = (SELECT MAX(IDENT_OPERATINGDATE) FROM OPERATINGDATE@'||v_SchemaProd||')) >= SI.SETTLEMENTDATE THEN 108 
                    WHEN SI.INSTRUCTIONSTATUS IN (''PENDING'', ''ACCEPTED'', ''MATCHED'')  AND SI.IDENT_INSTRUCTIONSTATUSREASON IN (14,16) THEN 101
                    ELSE 102
                    END CODE_STA,
                    TO_CHAR(NVL(SI.EFFECTIVESETTLEMENTDATE, SI.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS, 
                    SI.DESCRIPTION FREE_DSC,
                    TO_CHAR(SI.IDENT_SETTLEMENTINSTRUCTION)  ID_INST_CAPCO,
                    CASE WHEN (CSDOFCOUNTERPARTY.CODE = ''KSEPB'' OR CSDOFACCOUNTSERVICING.CODE = ''KSEPB'')
                    THEN  SI.INSTRUCTIONREFERENCE
                    WHEN SIM.IDENT_SETTLEMENTINSTRUCTION = SI.IDENT_SETTLEMENTINSTRUCTION AND SI.INSTRUCTIONSTATUS = ''CANCELLED'' 
                    THEN SI.INSTRUCTIONREFERENCE
                    ELSE NVL(TO_CHAR(SIM.IDENT_SETTLEMENTINSTRUCTION), SI.INSTRUCTIONREFERENCE) END INST_ID_INST_CAPCO,
                    NVL(BS.TYP_SEC, 0) TYP_INS, 
                    '''' EXT_BAN_ACCT,
                    CU.CODE_BASE_CURR CASH_CURR_CODE, 
                    CASE WHEN SI.IDENT_MARKETTYPE = 2 AND IT.CODE LIKE ''_FOPBOND'' THEN 1
                    WHEN SI.IDENT_MARKETTYPE = 1 AND IT.CODE LIKE ''_FOPBOND'' THEN 2 END AS PURPOSE,
                    '''' PURPOSE_DSC,
                    SI.TRADEREFERENCE AS TRADE_REF,
                    CASE WHEN IT.CODE LIKE ''_VPBOND'' THEN 0
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 52 THEN 1004 -- BART
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE IN (26, 27) THEN 1013 -- COLL
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 28 THEN 1023 -- CONV
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 53 THEN 1008 -- GIFT
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 50 THEN 1002 -- GRAN
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 51 THEN 1003 -- HERI
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 4 THEN 1021 -- IPO
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 55 THEN 1020 -- MSOP
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 56 THEN 1010 -- OTHR
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE IN (7,8,2) THEN 1016 -- OWNE
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 5 THEN 1006 -- REDM
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE IN (12,13) THEN 1022 -- REPO
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE IN (40, 41) THEN 1007 -- SELB
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 14 THEN 1005 -- SUBS
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 1 THEN 1001 -- TRAD
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 54 THEN 1009 -- VERD
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 29 THEN 1024 -- ETFT
                    END AS SETTLEMENT_REASON,
                    '''' SETTLEMENT_REASON_DSC,
                    0 CODE_BR,
                    '''' CODE_BR_DSC,
                    '''' INST_ID_INST_CAPCO_SETTINSTR
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI 
                    LEFT JOIN STATIC_PARTICIPANT CSDOFCOUNTERPARTY ON SI.IDENT_CSDOFCOUNTERPARTY = CSDOFCOUNTERPARTY.IDENT_STAKEHOLDER 
                    LEFT JOIN STATIC_PARTICIPANT CSDOFACCOUNTSERVICING ON SI.IDENT_CSDOFACCOUNTSERVICING = CSDOFACCOUNTSERVICING.IDENT_STAKEHOLDER 
                    LEFT JOIN STATIC_PARTICIPANT P ON  SI.IDENT_ACCOUNTSERVICING = P.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CURRENCIES CU ON SI.IDENT_CURRENCY = CU.ID_INS_CAPCO 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG ON SA.IDENT_ACCOUNTCOMPOSITE = AG.ID_ACCT_XCSD 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT  
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG2 ON CA.IDENT_ACCOUNTCOMPOSITE = AG2.ID_ACCT_XCSD
                    LEFT JOIN (
                    SELECT SIM.*
                    FROM  TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION  SIM WHERE SIM.MATCHINGREFERENCE != 0
                    ) SIM ON SI.MATCHINGREFERENCE = SIM.MATCHINGREFERENCE 
                    LEFT JOIN STATIC_PARTICIPANT P2 ON SI.IDENT_COUNTERPART = P2.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M2 ON P.CODE = M2.ID_MEM
                    LEFT JOIN STATIC_BICOUNTERPARTY BICN ON SI.COUNTERPARTBIC = BICN.BICCODE 
                    LEFT JOIN STATIC_INSTRUMENT INS ON SI.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE IT ON SI.IDENT_INSTRUCTIONTYPE = IT.IDENT_INSTRUCTIONTYPE
                    WHERE IT.CODE IN ( 
                    ''DFOPBOND'', ''DVPBOND'', ''RFOPBOND'', ''RVPBOND'' 
                    )
                    AND (
                    SI.MATCHINGREFERENCE = 0 OR
                    SI.IDENT_SETTLEMENTINSTRUCTION != SIM.IDENT_SETTLEMENTINSTRUCTION 
                    )
                    AND INS.IDENT_MASTER IS NULL
                    AND (SI.INSTRUCTIONREFERENCE NOT LIKE ''CRB%'')
                    UNION ALL
                    SELECT ''S4CNF'' INST_TYP, 
                    CASE WHEN SIM.INSTRUCTIONREFERENCE LIKE ''CI%'' AND SI.IDENT_INSTRUMENT = 0 THEN SIM.COMMONREFERENCE
                    ELSE SIM.INSTRUCTIONREFERENCE END AS EXT_REF,
                    NVL(INS.NAME, '''') CODE_BASE_SEC,
                    NVL(INS.LONGNAME,'''') SEC_DSC,
                    SI.TRADEDATE DAT_TRADE,
                    AG22.ID_ACCT AS ID_ACCT,    
                    CASE WHEN  (CSDOFCOUNTERPARTY.CODE = ''KSEPB'' OR CSDOFACCOUNTSERVICING.CODE = ''KSEPB'')  THEN BICN.CODE 
                    ELSE P2.CODE END AS CACCT,
                    CASE WHEN  (CSDOFCOUNTERPARTY.CODE = ''KSEPB'' OR CSDOFACCOUNTSERVICING.CODE = ''KSEPB'')  THEN BICN.CODE 
                    ELSE P2.CODE END AS ID_MEM,
                    CASE WHEN  (CSDOFCOUNTERPARTY.CODE = ''KSEPB'' OR CSDOFACCOUNTSERVICING.CODE = ''KSEPB'')  THEN BICN.LONGNAME 
                    ELSE P2.LONGNAME END DSC_MEM,
                    M2.TYP_MEM TYP_MEM, 
                    NVL(SI.VOLUME,0) AS AMT_INST_SEC, 
                    NVL(SI.AMOUNT,0) AS AMT_INST_CASH, 
                    SI.SETTLEMENTDATE DAT_SETTLE,
                    139 CODE_STA,
                    TO_CHAR(NVL(SI.EFFECTIVESETTLEMENTDATE, SI.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS, 
                    SI.DESCRIPTION FREE_DSC,
                    TO_CHAR(SI.IDENT_SETTLEMENTINSTRUCTION)  ID_INST_CAPCO,
                    NVL(TO_CHAR(SIM.IDENT_SETTLEMENTINSTRUCTION), SI.INSTRUCTIONREFERENCE) INST_ID_INST_CAPCO,
                    NVL(BS.TYP_SEC, 0) TYP_INS, 
                    '''' EXT_BAN_ACCT,
                    CU.CODE_BASE_CURR CASH_CURR_CODE, 
                    CASE WHEN SI.IDENT_MARKETTYPE = 2 AND IT.CODE LIKE ''_FOPBOND'' THEN 1
                    WHEN SI.IDENT_MARKETTYPE = 1 AND IT.CODE LIKE ''_FOPBOND'' THEN 2 END AS PURPOSE,
                    '''' PURPOSE_DSC,
                    SI.TRADEREFERENCE AS TRADE_REF,
                    CASE WHEN IT.CODE LIKE ''_VPBOND'' THEN 0
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 52 THEN 1004 -- BART
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE IN (26, 27) THEN 1013 -- COLL
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 28 THEN 1023 -- CONV
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 53 THEN 1008 -- GIFT
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 50 THEN 1002 -- GRAN
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 51 THEN 1003 -- HERI
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 4 THEN 1021 -- IPO
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 55 THEN 1020 -- MSOP
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 56 THEN 1010 -- OTHR
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE IN (7,8,2) THEN 1016 -- OWNE
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 5 THEN 1006 -- REDM
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE IN (12,13) THEN 1022 -- REPO
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE IN (40, 41) THEN 1007 -- SELB
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 14 THEN 1005 -- SUBS
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 1 THEN 1001 -- TRAD
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 54 THEN 1009 -- VERD
                    WHEN IT.CODE LIKE ''_FOPBOND'' AND SI.IDENT_TRANSACTIONTYPE = 29 THEN 1024 -- ETFT
                    END AS SETTLEMENT_REASON,
                    '''' SETTLEMENT_REASON_DSC,
                    0 CODE_BR,
                    '''' CODE_BR_DSC,
                    '''' INST_ID_INST_CAPCO_SETTINSTR
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI 
                    LEFT JOIN (
                    SELECT SIM.*
                    FROM TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SIM WHERE SIM.MATCHINGREFERENCE != 0
                    ) SIM ON SI.MATCHINGREFERENCE = SIM.MATCHINGREFERENCE 
                    LEFT JOIN STATIC_PARTICIPANT CSDOFCOUNTERPARTY ON SI.IDENT_CSDOFCOUNTERPARTY = CSDOFCOUNTERPARTY.IDENT_STAKEHOLDER 
                    LEFT JOIN STATIC_PARTICIPANT CSDOFACCOUNTSERVICING ON SI.IDENT_CSDOFACCOUNTSERVICING = CSDOFACCOUNTSERVICING.IDENT_STAKEHOLDER 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CURRENCIES CU ON SI.IDENT_CURRENCY = CU.ID_INS_CAPCO 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA ON SI.IDENT_ACCOUNT = SA.IDENT_ACCOUNT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG ON SA.IDENT_ACCOUNTCOMPOSITE = AG.ID_ACCT_XCSD 
                    LEFT JOIN TMPSCHEMA.STG_TOBA_CASHACCOUNT CA ON SI.IDENT_CASHACCOUNT = CA.IDENT_ACCOUNT  
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG2 ON CA.IDENT_ACCOUNTCOMPOSITE = AG2.ID_ACCT_XCSD
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SECURITIESACCOUNT SA2 ON SIM.IDENT_ACCOUNT = SA2.IDENT_ACCOUNT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AG22 ON SA2.IDENT_ACCOUNTCOMPOSITE = AG22.ID_ACCT_XCSD
                    LEFT JOIN STATIC_PARTICIPANT P2 ON SIM.IDENT_ACCOUNTSERVICING = P2.IDENT_STAKEHOLDER
                    LEFT JOIN TMPSCHEMA.STG_TOBA_MEMBERS M2 ON P2.CODE = M2.ID_MEM
                    LEFT JOIN STATIC_BICOUNTERPARTY BICN ON SIM.COUNTERPARTBIC = BICN.BICCODE 
                    LEFT JOIN STATIC_INSTRUMENT INS ON SI.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SI_INSTRUCTIONTYPE IT ON SI.IDENT_INSTRUCTIONTYPE = IT.IDENT_INSTRUCTIONTYPE
                    WHERE IT.CODE IN ( 
                    ''DFOPBOND'', ''DVPBOND'', ''RFOPBOND'', ''RVPBOND'' 
                    )
                    AND (
                    SI.MATCHINGREFERENCE = 0 OR
                    (SI.IDENT_SETTLEMENTINSTRUCTION != SIM.IDENT_SETTLEMENTINSTRUCTION AND SI.INSTRUCTIONSTATUS != ''CANCELLED'')
                    OR (SI.IDENT_SETTLEMENTINSTRUCTION = SIM.IDENT_SETTLEMENTINSTRUCTION AND SI.INSTRUCTIONSTATUS = ''CANCELLED'')
                    )
                    AND INS.IDENT_MASTER IS NULL
                    AND SI.INSTRUCTIONREFERENCE LIKE ''CRB%''
                    AND SI.INSTRUCTIONSTATUS = ''SETTLED''';

    EXECUTE IMMEDIATE V_SQL;
     
     -- BLOCK UNBLOCK BALANCE
     V_SQL := 'INSERT INTO  TMPSCHEMA.STG_INSTRUCTIONS2(INST_TYP, EXT_REF, CODE_BASE_SEC, SEC_DSC, DAT_TRADE, ID_ACCT, 
                    CACCT, ID_MEM, DSC_MEM, TYP_MEM, AMT_INST_SEC, AMT_INST_CASH, DAT_SETTLE, CODE_STA, LST_UPD_TS,
                    FREE_DSC, ID_INST_CAPCO, INST_ID_INST_CAPCO, TYP_INS, EXT_BAN_ACCT, CASH_CURR_CODE, PURPOSE, PURPOSE_DSC,
                    TRADE_REF, SETTLEMENT_REASON, SETTLEMENT_REASON_DSC, CODE_BR, CODE_BR_DSC, INST_ID_INST_CAPCO_SETTINSTR)
                    SELECT ''BLOACINSTR'' INST_TYP, RO.USERREFERENCE EXT_REF, INS.NAME CODE_BASE_SEC, INS.LONGNAME SEC_DSC, 
                    CAST(NULL AS DATE) DAT_TRADE, 
                    AC.ID_ACCT, '''' CACCT, '''' ID_MEM, '''' DSC_MEM, '''' TYP_MEM, 
                    SI.SETTLEDQUANTITY AMT_INST_SEC, '''' AMT_INST_CASH, TRUNC(RO.REVISION_EFFECTIVE_FROM) DAT_SETTLE,
                    116 CODE_STA, 
                    TO_CHAR(NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS,
                    CASE WHEN RO.IDENT_RESTRICTIONSUBTYPE = 15
                    THEN ''BALANCE BLOCKED BEFORE MIGRATION'' 
                    ELSE '''' END FREE_DSC,
                    SI.IDENT_SETTLEMENTINSTRUCTION ID_INST_CAPCO, '''' INST_ID_INST_CAPCO, BS.TYP_SEC TYP_INS, '''' EXT_BAN_ACCT, 
                    '''' CASH_CURR_CODE, 0 PURPOSE, '''' PURPOSE_DSC, '''' TRADE_REF, 0 SETTLEMENT_REASON, '''' SETTLEMENT_REASON_DSC,
                    CASE WHEN RO.IDENT_RESTRICTIONSUBTYPE = 13 THEN 1
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 17 THEN 2
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 9 THEN 3
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 5 THEN 4
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 4 THEN 1001
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 3 THEN 1002
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 11 THEN 1012
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 7 THEN 1006
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 8 THEN 1007
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 12 THEN 1008
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 1 THEN 1009
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 6 THEN 1010
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 10 THEN 1011
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 16 THEN 1013
                    END AS CODE_BR,
                    '''' CODE_BR_DSC, 
                    '''' INST_ID_INST_CAPCO_SETTINSTR
                    FROM TMPSCHEMA.STG_TOBA_RESTRICTIONORDER RO
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AC ON RO.IDENT_ACCOUNTCOMPOSITE = AC.ID_ACCT_XCSD
                    LEFT JOIN STATIC_INSTRUMENT INS ON RO.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI ON RO.IDENT_RESTRICTIONINSTRUCTION = SI.IDENT_SETTLEMENTINSTRUCTION
                    WHERE 1=1
                    AND RO.RESTRICTWHOLEACCOUNT = 0
                    AND RO.IDENT_MASTER IS NULL
                    AND SI.INSTRUCTIONSTATUS IN (''PENDING'')
                    UNION ALL
                    SELECT DISTINCT ''UNBLOINSTR'' INST_TYP, RO.USERREFERENCE, INS.NAME, INS.LONGNAME, CAST(NULL AS DATE) DAT_TRADE, 
                    AC.ID_ACCT, '''' CACCT, '''' ID_MEM, '''' DSC_MEM, '''' TYP_MEM, 
                    SL.SETTLEDQUANTITY AMT_INST_SEC, '''' AMT_INST_CASH, TRUNC(RO.REVISION_EFFECTIVE_FROM) DAT_SETTLE,
                    116 CODE_STA, 
                    TO_CHAR(NVL(RO.REVISION_EFFECTIVE_FROM, RO.SYSMODIFIED),''yyyymmddhh24missff3'') LST_UPD_TS,
                    CASE WHEN RO.IDENT_RESTRICTIONSUBTYPE = 15
                    THEN ''BALANCE BLOCKED BEFORE MIGRATION'' 
                    ELSE '''' END FREE_DSC,
                    SI.IDENT_SETTLEMENTINSTRUCTION ID_INST_CAPCO, '''' INST_ID_INST_CAPCO, BS.TYP_SEC TYP_INS, '''' EXT_BAN_ACCT, 
                    '''' CASH_CURR_CODE, 0 PURPOSE, '''' PURPOSE_DSC, '''' TRADE_REF, 0 SETTLEMENT_REASON, '''' SETTLEMENT_REASON_DSC,
                    CASE WHEN RO.IDENT_RESTRICTIONSUBTYPE = 13 THEN 1
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 17 THEN 2
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 9 THEN 3
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 5 THEN 4
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 4 THEN 1001
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 3 THEN 1002
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 11 THEN 1012
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 7 THEN 1006
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 8 THEN 1007
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 12 THEN 1008
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 1 THEN 1009
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 6 THEN 1010
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 10 THEN 1011
                    WHEN RO.IDENT_RESTRICTIONSUBTYPE = 16 THEN 1013
                    END AS CODE_BR,
                    '''' CODE_BR_DSC, 
                    '''' INST_ID_INST_CAPCO_SETTINSTR
                    FROM TMPSCHEMA.STG_TOBA_RESTRICTIONORDER RO
                    LEFT JOIN TMPSCHEMA.STG_TOBA_ACCOUNTS AC ON RO.IDENT_ACCOUNTCOMPOSITE = AC.ID_ACCT_XCSD
                    LEFT JOIN STATIC_INSTRUMENT INS ON RO.IDENT_INSTRUMENT = INS.IDENT_INSTRUMENT
                    LEFT JOIN TMPSCHEMA.STG_TOBA_BASIC_SECURITIES BS ON INS.NAME = BS.CODE_BASE_SEC
                    LEFT JOIN TMPSCHEMA.STG_TOBA_SETTLEMENTINSTRUCTION SI ON RO.IDENT_RESTRICTIONINSTRUCTION = SI.IDENT_SETTLEMENTINSTRUCTION
                    LEFT JOIN STG_TOBA_SETTINSTRSTATUSLOG SL ON SI.IDENT_SETTLEMENTINSTRUCTION = SL.IDENT_SETTLEMENTINSTRUCTION AND SL.INSTRUCTIONSTATUS = ''PENDING''
                    WHERE 1=1
                    AND RO.RESTRICTWHOLEACCOUNT = 0
                    AND RO.IDENT_MASTER IS NULL
                    AND SI.INSTRUCTIONSTATUS IN (''SETTLED'')';
      
     EXECUTE IMMEDIATE V_SQL;
        
     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_INSTRUCTIONS', 'Y', 'Insert into IMP_STG_INSTRUCTIONS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('STG_INSTRUCTIONS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_INSTRUCTIONS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_INSTRUCTIONS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error STG_INSTRUCTIONS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_INSTRUCTIONS

procedure IMP_STG_EXECFEE
is
begin
   
    select to_char((get_last_bus_day(sysdate-1)+1),'DD-MON-YYYY'), to_char(sysdate-1, 'DD-MON-YYYY')  INTO TGLDESC, ENDTGLDESC  from dual;
    
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_EXECFEE DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_TOBA_EXECFEE
                    SELECT IDENT_EXECFEE, IDENT_FEEDEFINITION, EXECDATE, EXECCOMMENT, PERIODSTART, PERIODEND, GENERATEDTYPE
                    FROM EXECFEE@'||v_SchemaProd||' fee
                    WHERE TRUNC(FEE.PERIODSTART) BETWEEN '''||tglDesc||''' AND '''||endtglDesc||'''';

     EXECUTE IMMEDIATE V_SQL;
     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_EXECFEE', 'Y', 'Insert into IMP_STG_EXECFEE');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_EXECFEE DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_EXECFEE',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_EXECFEE with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_EXECFEE with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_EXECFEE

procedure IMP_STG_FEE
is
begin

    select to_char((get_last_bus_day(sysdate-1)+1),'DD-MON-YYYY'), to_char(sysdate-1, 'DD-MON-YYYY')  INTO TGLDESC, ENDTGLDESC  from dual;
        
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_FEE DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_TOBA_FEE
                    SELECT F.IDENT_EXECFEE, F.IDENT_FEE, F.IDENT_INVOICESUBITEM, F.ACCOUNTID, 
                    F.PAYMENTDIRECTION, F.FEEAMOUNT, F.INVOICEDATE, F.FEECOMMENT, 
                    F.INSTRUMENTID, F.SYSTEMGENERATED, F.VAT, F.IDENT_CUSTOMER
                    , C.CODE, C.NAME, C.TYPE
                    FROM FEE@'||v_SchemaProd||' F
                    LEFT JOIN EXECFEE@'||v_SchemaProd||' EF ON F.IDENT_EXECFEE = EF.IDENT_EXECFEE
                    LEFT JOIN CUSTOMER@'||v_SchemaProd||' C ON F.IDENT_CUSTOMER = C.IDENT_CUSTOMER
                    WHERE TRUNC(EF.PERIODSTART) BETWEEN '''||tglDesc||''' AND '''||endtglDesc||'''';

     EXECUTE IMMEDIATE V_SQL;

     commit;     
     
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_FEE', 'Y', 'Insert into IMP_STG_FEE');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_FEE DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_FEE',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_FEE with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_FEE with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_FEE

procedure IMP_STG_FEEDEFINITION
is
begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_FEEDEFINITION DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_TOBA_FEEDEFINITION 
                    SELECT IDENT_FEEDEFINITION, FEEDEFINITIONNAME, FEETYPE, CALCULATIONFREQUENCY, 
                    CALCULATIONDAYS, IDENT_ACTIVATIONTYPE, INCLUDEDELISTEDINSTRUMENTS, 
                    VAT, INITIALDATE, PRESERVEFEEBASIS
                    FROM FEEDEFINITION@'||v_SchemaProd||'';

     EXECUTE IMMEDIATE V_SQL;

     commit;     
     
     INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
           VALUES (SYSDATE, 'IMP_STG_FEEDEFINITION', 'Y', 'Insert into IMP_STG_FEEDEFINITION');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_FEEDEFINITION DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_FEEDEFINITION',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_FEEDEFINITION with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_FEEDEFINITION with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_FEEDEFINITION

procedure IMP_STG_FEEBASIS
is
begin

    select to_char((get_last_bus_day(sysdate-1)+1),'DD-MON-YYYY'), to_char(sysdate-1, 'DD-MON-YYYY')  INTO TGLDESC, ENDTGLDESC  from dual;
        
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPSCHEMA.STG_TOBA_FEEBASIS DROP STORAGE';

    V_SQL := 'INSERT INTO TMPSCHEMA.STG_TOBA_FEEBASIS
                    SELECT FB.IDENT_FEEBASIS, FB.IDENT_FEE, FB.IDENT_EXECFEE, FB.CLOSINGPRICE, FB.EXCHANGERATE, 
                    FB.SECURITYQUANTITY, FB.ACCOUNTID, FB.INSTRUMENTID, FB.INSTRUCTIONREFERENCE, 
                    FB.INSTRUCTIONTYPE, FB.CHARGEABLERANGEBEGIN, FB.CHARGEABLERANGEEND, FB.CALCULATEDAMOUNT
                    FROM FEEBASIS@'||v_SchemaProd||' FB
                    LEFT JOIN EXECFEE@'||v_SchemaProd||' EF ON FB.IDENT_EXECFEE = EF.IDENT_EXECFEE
                    WHERE TRUNC(EF.PERIODSTART) BETWEEN '''||tglDesc||''' AND '''||endtglDesc||'''';

     EXECUTE IMMEDIATE V_SQL;

     commit;     
     
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, proc_desc)
       VALUES (SYSDATE, 'IMP_STG_FEEBASIS', 'Y', 'Insert into IMP_STG_FEEBASIS');

     COMMIT;
     
     DBMS_OUTPUT.PUT_LINE('IMP_STG_FEEBASIS DONE');
EXCEPTION
WHEN OTHERS
   THEN
      ROLLBACK;
      ERR_MSG := SUBSTR(SQLERRM,1,100);
      ERR_CODE := SQLCODE;
      
      INSERT INTO ARCHIVE_LOG (tgl, prosedur, sukses, ERROR_MESSAGE, proc_desc)
              VALUES (SYSDATE,
                      'IMP_STG_FEEBASIS',
                      'N',
                      ERR_MSG,
                      'Error IMP_STG_FEEBASIS with error '||ERR_MSG);

      COMMIT; 
      
      DBMS_OUTPUT.PUT_LINE('Error IMP_STG_FEEBASIS with error '||ERR_MSG);
      DBMS_OUTPUT.PUT_LINE(V_SQL);
end;--IMP_STG_FEEBASIS
end IMP_STAGINGTABLE_XCSD;
/
