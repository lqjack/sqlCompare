----GMR_TRANSACTION
select tr.TRAN_ID,
(select p.AREA_UNIT from gmrdba.GMR_AREA p 
    where p.AREA_KEY=tr.TRAN_AREA_KEY
    and p.AREA_RECORD_STATUS = 'A' AND p.AREA_SRC_SYS = 'WSS' ) as
    TRAN_AREA_KEY,
tr.TRAN_ASSET_CLASS,
decode(TRAN_CCY_BOUGHT_KEY, null, null, (select p.CCCY_CODE from gmrdba.GMR_CURRENCY p 
    where p.CCCY_KEY=tr.TRAN_CCY_BOUGHT_KEY
    and p.CCCY_RECORD_STATUS = 'A' AND p.CCCY_SRC_SYS = 'WSS')) as
    TRAN_CCY_BOUGHT_KEY,
tr.TRAN_AMT_BOUGHT,
 decode(TRAN_CCY_SOLD_KEY, null, null, (select p.CCCY_CODE from gmrdba.GMR_CURRENCY p 
    where p.CCCY_KEY=tr.TRAN_CCY_SOLD_KEY
    and p.CCCY_RECORD_STATUS = 'A' AND p.CCCY_SRC_SYS = 'WSS')) as
    TRAN_CCY_SOLD_KEY,
  tr.TRAN_AMT_SOLD,
  tr.TRAN_AMTSTRNG,
  tr.TRAN_AMTWEAK,
  (select p.CCCY_CODE from gmrdba.GMR_CURRENCY p 
    where p.CCCY_KEY=tr.TRAN_CCY_KEY
    and p.CCCY_RECORD_STATUS = 'A' AND p.CCCY_SRC_SYS = 'WSS') as
    TRAN_CCY_KEY,
  tr.TRAN_BASE_EQUIV_AMT,
  tr.TRAN_BASE_EQUIV_RATE,
  tr.TRAN_BROKER_CODE,
 tr.TRAN_CUSTOMER_REF, 
  tr.TRAN_DEAL_NUM,
 tr.TRAN_DI_FLAG,
 tr.TRAN_DEAL_CODE,
 tr.TRAN_ENTRY_DATE_TIME,
 tr.TRAN_FULLY_APP,
 tr.TRAN_GDP_TIME,
 (select p.CTRP_ID from gmrdba.GMR_COUNTERPARTY p 
    where p.CTRP_KEY=tr.TRAN_CUSTID_KEY
    and p.CTRP_RECORD_STATUS = 'A' AND p.CTRP_SRC_SYS = 'WSS') as
    TRAN_CUSTID_KEY,
    tr.TRAN_EXTN_DEAL_NUM,
	tr.TRAN_FSI_TID,
	tr.TRAN_FSI_REC_TID,
	tr.TRAN_ORIG_TID,
  tr.TRAN_OTC_CODE,
  tr.TRAN_OTC_PREM,
  (select p.CCCY_CODE from gmrdba.GMR_CURRENCY p 
    where p.CCCY_KEY=tr.TRAN_OTC_PREM_CCY_KEY
    and p.CCCY_RECORD_STATUS = 'A' AND p.CCCY_SRC_SYS = 'WSS') as
    TRAN_OTC_PREM_CCY_KEY,
  tr.TRAN_OTC_TYPE,
  tr.TRAN_OTC_TYPE_CODE,
  tr.TRAN_PAY_NOSTRO,
  (select p.PRDT_PRODUCT from gmrdba.GMR_PRODUCT p 
    where p.PRDT_KEY=tr.TRAN_PRDT_KEY
    and p.PRDT_RECORD_STATUS = 'A' AND p.PRDT_SRC_SYS = 'WSS') as
    TRAN_PRDT_KEY,
	tr.TRAN_PAY_METHOD,
	tr.TRAN_RECEIVE_METHOD,
	tr.TRAN_REC_NOSTRO,
	tr.TRAN_REMARK1,
    tr.TRAN_REMARK2,
    tr.TRAN_REMARK3,
	tr.TRAN_RATE,
	tr.TRAN_SPOT_DATE,
  tr.TRAN_STATUS_FLAG,
  decode(TRAN_CCYSTRNG_KEY, null, null, (select p.CCCY_CODE from gmrdba.GMR_CURRENCY p 
    where p.CCCY_KEY=tr.TRAN_CCYSTRNG_KEY
    and p.CCCY_RECORD_STATUS = 'A' AND p.CCCY_SRC_SYS = 'WSS')) as
    TRAN_CCYSTRNG_KEY,
	tr.TRAN_SWAP_SF,
	tr.TRAN_SWIFT_SETTLE_2,
  tr.TRAN_TICKET_AREA,
  tr.TRAN_TRADE_DATE,
  tr.TRAN_TRADER,
  tr.TRAN_TRAN_TYPE,
  tr.TRAN_VALUE_DATE,
  decode(TRAN_CCYWEAK_KEY, null, null, (select p.CCCY_CODE from gmrdba.GMR_CURRENCY p 
    where p.CCCY_KEY=tr.TRAN_CCYWEAK_KEY
    and p.CCCY_RECORD_STATUS = 'A' AND p.CCCY_SRC_SYS = 'WSS')) as
    TRAN_CCYWEAK_KEY,
 (select p.CTRP_ID from gmrdba.GMR_COUNTERPARTY p 
    where p.CTRP_KEY=tr.TRAN_DEAL_IM_KEY
    and p.CTRP_RECORD_STATUS = 'A' AND p.CTRP_SRC_SYS = 'WSS') as
 TRAN_DEAL_IM_KEY,
   tr.TRAN_CANCEL_REASON,
  tr.TRAN_SWIFT_SETTLE_3,
  tr.TRAN_TRADER_NAME,
  tr.TRAN_CANCEL_DATE,
 tr.TRAN_OTC_PREM_DD,
  tr.TRAN_OTC_EXO_BARR1,
  tr.TRAN_OTC_EXO_BARR2,
  tr.TRAN_OTC_EXO_FREE1,
  tr.TRAN_OTC_EXO_FREE3,
  tr.TRAN_OTC_EXO_FREE4,
  tr.TRAN_POINTS,
 tr.TRAN_INVMAN,
  tr.TRAN_NDF_STCCY,
  tr.TRAN_BK_TYPE,
  TRAN_BROKER_CTRP_KEY,
  tr.TRAN_NDF_FIX_DATE,
  tr.TRAN_SWAP_TID,
  tr.TRAN_NDF_ORID_TID,
  tr.TRAN_INVERT,
  (select p.PORT_TID from gmrdba.GMR_PORTFOLIO p 
    where p.PORT_KEY=tr.TRAN_PORT_KEY
    and p.PORT_RECORD_STATUS = 'A' AND p.PORT_SRC_SYS = 'WSS') as
    TRAN_PORT_KEY,
	(select p.CCCY_CODE from gmrdba.GMR_CURRENCY p 
    where p.CCCY_KEY=tr.TRAN_PUT_CCY_KEY
    and p.CCCY_RECORD_STATUS = 'A' AND p.CCCY_SRC_SYS = 'WSS') as
   TRAN_PUT_CCY_KEY,
  tr.TRAN_AMT_BOUGHT_EURO,
   tr.TRAN_AMT_SOLD_EURO,
   tr.TRAN_ARB_TID1,
   tr.TRAN_ARB_TID2,
   tr.TRAN_ARBITRAGE_FLAG,
   tr.TRAN_AREA_INT_NO,
   tr.TRAN_AVG_TO_DATE,
   tr.TRAN_BACK_DATED,
   tr.TRAN_BREAKOUT_TID,
   tr.TRAN_CALL_CCY_AMOUNT,
   (select p.CCCY_CODE from gmrdba.GMR_CURRENCY p 
    where p.CCCY_KEY=tr.TRAN_CALL_CCY_KEY
    and p.CCCY_RECORD_STATUS = 'A' AND p.CCCY_SRC_SYS = 'WSS') as
    TRAN_CALL_CCY_KEY,
	tr.TRAN_CASH_ADJ,
	tr.TRAN_CCY1_TRADE_RATE,
    tr.TRAN_CCY2_TRADE_RATE,
	tr.TRAN_CLS_FLAG,
	tr.TRAN_CP_AMOUNT,
	tr.TRAN_CP_TYPE,
	tr.TRAN_CUR_SPOT_REV,
    tr.TRAN_CUR_SPOT_TRD,
    tr.TRAN_CUR_VAL,
    tr.TRAN_CUR_VAL_DIS,
	tr.TRAN_FWD_FLAG,
    tr.TRAN_FWD_SPOT,
	tr.TRAN_MARG_NPV,
	tr.TRAN_OTC_DEL_DATE,
    tr.TRAN_OTC_ET,
    tr.TRAN_OTC_EXC_DATE,
	tr.TRAN_OTC_EXO_DATE1,
    tr.TRAN_OTC_EXO_DATE2,
	tr.TRAN_OTC_EXO_FREE2,
	tr.TRAN_OTC_EXP_TIME,
	tr.TRAN_OTC_FDC1,
	tr.TRAN_OTC_FDC2,
	tr.TRAN_OTC_PREM_CNV,
	tr.TRAN_OTC_PREM_NO,
	tr.TRAN_OTC_PREM_TYPE,
	tr.TRAN_OTC_SDC1,
	tr.TRAN_OTC_SDC2,
	tr.TRAN_OTC_SPOT_RATE,
	tr.TRAN_PUT_CCY_AMOUNT,
	tr.TRAN_REC_INTL,
	tr.TRAN_REUTERS_DEAL_NO,
	tr.TRAN_REUTERS_NODE,
	tr.TRAN_SWAP_POINTS,
	tr.TRAN_TIME_OP,
	tr.TRAN_TO_ORIG_TID,
	tr.TRAN_START_DATE,
	tr.TRAN_REASON,
	tr.TRAN_CITY,
	tr.TRAN_COMMISSION_AMT,
	(select p.CTRP_ID from gmrdba.GMR_COUNTERPARTY p 
    where p.CTRP_KEY=tr.TRAN_INVMAN_KEY
    and p.CTRP_RECORD_STATUS = 'A' AND p.CTRP_SRC_SYS = 'WSS') as
	TRAN_INVMAN_KEY,
	tr.TRAN_MARG_PTS1,
	tr.TRAN_OPTION_FLAG,
	tr.TRAN_MARG_AMT
FROM GMRDBA.GMR_TRANSACTION tr 
       left join gmrdba.gmr_area a on a.area_key = tran_area_key
where tr.tran_src_sys_date = to_date('2015-02-24 00:00:00', 'YYYY-MM-DD hh24:mi:ss') 
  and tran_asset_class = 'FX'
  and a.area_src_sys = 'WSS'
  and tr.tran_src_sys = 'WSS'
  and a.AREA_TRADESITE = 'BOS';