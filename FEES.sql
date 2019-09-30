CREATE OR REPLACE VIEW FEES AS
SELECT ID_FEE_CAPCO, LST_UPD_TS, TYP_FEE, XCSD_FEETYPE, INST_NBR, TYP_MVT,
FEERATE, AMT_NET, CALC_DAT, FEE_PERIOD_START, FEE_PERIOD_END, FEE_DSC, INV_ID_INV_CAPCO,
ACCT_ID_ACCT_CAPCO, PARTICIPANT, ISSUER, CRE_DAT
FROM (
SELECT DISTINCT F.IDENT_FEE ID_FEE_CAPCO
, EF.EXECDATE LST_UPD_TS
, CASE
WHEN FD.FEETYPE IN ('OTC_INSTRUCTION','EXTERNAL_OTC_BOND','CANCELLED_SETTLEMENT_INSTRUCTION','SETTLEMENT_INSTRUCTIONS_CANCELLED_FROM_ONHOLD','SECURITIES_WITHDRAWAL') THEN 1
WHEN FD.FEETYPE = 'SAFE_KEEPING' THEN 2
WHEN FD.FEETYPE = 'ISSUER_JOINING' THEN 3
WHEN FD.FEETYPE IN ('SECURITIES_ANNUAL', 'SECURITIES_REGISTRATION') THEN 4
WHEN FD.FEETYPE IN ('LATE_PENALTY','LATE_PENALTY_FOR_ISSUER') THEN 8
WHEN FD.FEETYPE = 'MISCELLANEOUS' AND (F.VAT = 0 OR F.VAT IS NULL) THEN 8
WHEN FD.FEETYPE = 'MISCELLANEOUS' AND F.VAT = 1 THEN 9
WHEN FD.FEETYPE = 'DORMANT_ACCOUNT' THEN 26
WHEN FD.FEETYPE IN ('RETURN_LIQUIDITY_TO_BI','FROZEN_BALANCE','FROZEN_ACCOUNT') THEN 8826
WHEN FD.FEETYPE = 'ORCHID' THEN 999
END AS TYP_FEE
, FD.FEETYPE XCSD_FEETYPE
, CASE
WHEN FD.FEETYPE IN  ('OTC_INSTRUCTION', 'EXTERNAL_OTC_BOND', 'CANCELLED_SETTLEMENT_INSTRUCTION','SETTLEMENT_INSTRUCTIONS_CANCELLED_FROM_ONHOLD', 'SECURITIES_WITHDRAWAL')
 AND F.SYSTEMGENERATED = 1 THEN FB.INST_NBR
ELSE 0 END AS INST_NBR
, CASE
WHEN F.PAYMENTDIRECTION = 'DEBIT' THEN 1
ELSE 2 END AS TYP_MVT
, FRB.FEERATE
, CASE
WHEN FD.FEETYPE IN  ('OTC_INSTRUCTION', 'EXTERNAL_OTC_BOND', 'CANCELLED_SETTLEMENT_INSTRUCTION','SETTLEMENT_INSTRUCTIONS_CANCELLED_FROM_ONHOLD')
 AND F.SYSTEMGENERATED = 1 THEN (FB.INST_NBR * FRB.FEERATE)
WHEN FD.FEETYPE IN  ('OTC_INSTRUCTION', 'EXTERNAL_OTC_BOND', 'CANCELLED_SETTLEMENT_INSTRUCTION','SETTLEMENT_INSTRUCTIONS_CANCELLED_FROM_ONHOLD')
 AND F.SYSTEMGENERATED = 0 THEN ABS(F.FEEAMOUNT)
ELSE ABS(F.FEEAMOUNT) END AS AMT_NET
, TRUNC(EF.PERIODSTART) CALC_DAT
, TRUNC(EF.PERIODSTART) FEE_PERIOD_START
, TRUNC(EF.PERIODEND) FEE_PERIOD_END
, CASE
WHEN FD.FEETYPE IN ('OTC_INSTRUCTION', 'EXTERNAL_OTC_BOND', 'CANCELLED_SETTLEMENT_INSTRUCTION','SETTLEMENT_INSTRUCTIONS_CANCELLED_FROM_ONHOLD')
AND F.SYSTEMGENERATED = 1 THEN FB.NAME
WHEN FD.FEETYPE IN ('OTC_INSTRUCTION', 'EXTERNAL_OTC_BOND', 'CANCELLED_SETTLEMENT_INSTRUCTION','SETTLEMENT_INSTRUCTIONS_CANCELLED_FROM_ONHOLD')
AND F.SYSTEMGENERATED = 0 THEN FB.NAME
WHEN FD.FEETYPE = 'SECURITIES_WITHDRAWAL' THEN 'Instruction fee - Security Withdrawal (' || INSSECW.NAME || ')' 
WHEN FD.FEETYPE = 'SAFE_KEEPING' THEN FD.FEEDEFINITIONNAME || ' for account ' || F.ACCOUNTID
WHEN FD.FEETYPE = 'ISSUER_JOINING'  THEN FD.FEEDEFINITIONNAME || ' for ' || ISS.CODE || ' (' || ISS.NAME || ')'
WHEN FD.FEETYPE IN ('SECURITIES_ANNUAL', 'SECURITIES_REGISTRATION') THEN 'Annual fee for security ' || INS.NAME
ELSE FD.FEEDEFINITIONNAME END FEE_DSC
, CAST(NULL AS VARCHAR2(40)) AS INV_ID_INV_CAPCO
, F.ACCOUNTID ACCT_ID_ACCT_CAPCO
, PART.CODE PARTICIPANT
, ISS.CODE ISSUER
, CAST(NULL AS DATE) CRE_DAT
FROM FEE F
LEFT JOIN EXECFEE EF ON F.IDENT_EXECFEE = EF.IDENT_EXECFEE
LEFT JOIN (
SELECT FB.IDENT_FEE, FB.IDENT_EXECFEE, FB.ACCOUNTID, IT.CODE
, CASE WHEN IT.CODE = 'DFOP' THEN 'Instruction fee - DELIVERY'
 WHEN IT.CODE = 'RFOP' THEN 'Instruction fee - RECEIPT'
 WHEN IT.CODE = 'DVP' THEN 'Instruction fee - DELIVERY VERSUS PAYMENT'
 WHEN IT.CODE = 'RVP' THEN 'Instruction fee - RECEIPT VERSUS PAYMENT' 
 WHEN IT.CODE = 'DFOPBOND' THEN 'Instruction fee - DELIVERY BOND'
 WHEN IT.CODE = 'RFOPBOND' THEN 'Instruction fee - RECEIPT BOND'
 WHEN IT.CODE = 'DVPBOND' THEN 'Instruction fee - DELIVERY VERSUS PAYMENT BOND'
 WHEN IT.CODE = 'RVPBOND' THEN 'Instruction fee - RECEIPT VERSUS PAYMENT BOND' 
 END NAME
, COUNT(*) INST_NBR
FROM FEEBASIS FB
LEFT JOIN INSTRUCTIONTYPE IT ON FB.INSTRUCTIONTYPE = TO_CHAR(IT.IDENT_INSTRUCTIONTYPE)
GROUP BY FB.IDENT_FEE, FB.IDENT_EXECFEE, FB.ACCOUNTID, IT.CODE, IT.NAME
) FB ON F.IDENT_FEE = FB.IDENT_FEE AND FB.ACCOUNTID = F.ACCOUNTID
LEFT JOIN (
SELECT FB.IDENT_FEEBASIS, FB.IDENT_FEE, FB.SECURITYQUANTITY, FB.ACCOUNTID, 
FB.CALCULATEDAMOUNT, TRUNC(EF.PERIODSTART) PERIODSTART, SI.IDENT_INSTRUMENT, SI.INSTRUCTIONSTATUS
FROM FEEBASIS FB 
JOIN EXECFEE EF ON FB.IDENT_EXECFEE = EF.IDENT_EXECFEE
JOIN SETTLEMENTINSTRUCTION SI ON FB.INSTRUCTIONREFERENCE = SI.INSTRUCTIONREFERENCE AND FB.INSTRUCTIONTYPE = '14'
    AND FB.INSTRUCTIONTYPE = TO_CHAR(SI.IDENT_INSTRUCTIONTYPE)
WHERE FB.INSTRUCTIONTYPE = '14'
AND EF.IDENT_FEEDEFINITION = 1400
) FB2 ON F.IDENT_FEE = FB2.IDENT_FEE
LEFT JOIN INSTRUMENT INSSECW ON FB2.IDENT_INSTRUMENT = INSSECW.IDENT_INSTRUMENT 
    AND INSSECW.IDENT_MASTER IS NULL 
LEFT JOIN FEEDEFINITION FD ON EF.IDENT_FEEDEFINITION = FD.IDENT_FEEDEFINITION
LEFT JOIN CUSTOMER PART ON PART.IDENT_CUSTOMER = F.IDENT_CUSTOMER AND PART.TYPE = 'PARTICIPANT'
LEFT JOIN CUSTOMER ISS ON ISS.IDENT_CUSTOMER = F.IDENT_CUSTOMER AND ISS.TYPE = 'ISSUER'
LEFT JOIN FEERATE FR ON FD.IDENT_FEEDEFINITION = FR.IDENT_FEEDEFINITION
LEFT JOIN FEERATEBANDVALUE FRB ON FR.IDENT_FEERATE = FRB.IDENT_FEERATE
LEFT JOIN INSTRUMENT INS ON INS.ISIN = F.INSTRUMENTID AND INS.IDENT_MASTER IS NULL
LEFT JOIN (
SELECT ISS.CODE FROM ISSUER ISS 
JOIN INSTRUMENT INS ON ISS.IDENT_STAKEHOLDER = INS.IDENT_ISSUER AND INS.IDENT_MASTER IS NULL 
WHERE ISS.IDENT_MASTER IS NULL
AND (INS.IDENT_ASSETCLASS IN (1009, 1019) OR ISS.COMMENTS LIKE '%NOFEE%')
) ISSR ON ISS.CODE = ISSR.CODE -- exclusion of issuer from issuer registration fee
JOIN OPERATINGDATE OD ON OD.IDENT_OPERATINGDATE = (SELECT MAX(IDENT_OPERATINGDATE) FROM OPERATINGDATE)
WHERE 1=1
AND TRUNC(EF.PERIODSTART) = TRUNC(EF.PERIODEND)
AND (PART.CODE != 'KSEI1' OR PART.CODE IS NULL)
AND (PART.CODE NOT LIKE '__009' OR PART.CODE IS NULL)
AND (INS.IDENT_ASSETCLASS IS NULL 
    OR INS.IDENT_ASSETCLASS NOT IN (1003,1005,1006,1007,1008,1009,1016,1017,1019,1020,1021,1022,1023,1024,1025,1026)
) -- exclusion of instrument from securities registration fee
AND ISSR.CODE IS NULL
AND (INSSECW.IDENT_ASSETCLASS IS NULL OR INSSECW.IDENT_ASSETCLASS NOT IN (1018)) -- exclude ETF from securities withdrawal fee
AND (FB2.INSTRUCTIONSTATUS IS NULL OR FB2.INSTRUCTIONSTATUS = 'SETTLED')
AND (
TRUNC(EF.PERIODSTART) BETWEEN GET_LAST_BUS_DAY(OD.OPERATINGDATE)+1 AND OD.OPERATINGDATE
OR (FD.FEETYPE = 'ORCHID' AND TRUNC(EF.PERIODSTART) BETWEEN GET_LAST_BUS_DAY(OD.OPERATINGDATE) AND OD.OPERATINGDATE))
AND FD.FEETYPE NOT IN ('ISSUER_JOINING','SECURITIES_ANNUAL', 'SECURITIES_REGISTRATION') -- exclude issuer related fees
)
WHERE AMT_NET > 0
;
