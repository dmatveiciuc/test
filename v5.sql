# test
spool otc_wh_p-b-cfd_cash_reports_v5_pck.pkg.out;

SET DEFINE OFF;
WHENEVER sqlerror EXIT sql.sqlcode;

CREATE OR REPLACE PACKAGE BODY OTC_WRHS_POSITION.CFD_CASH_REPORTS_V5_PCK IS
--------
FUNCTION getversion RETURN VARCHAR2
IS
BEGIN
       RETURN( '4.2015.09.29.16-47.ID-156.1'); /*  Report id #712 FDR Dividend Receivable/Payable extract - should only populate Open Entitlement. DB: Dmitri Matveiciuc. */
     --RETURN( '3.2015.09.29.16-47.ID-153.1'); /*  FDR-PAS - Report id 712 FDR Dividend Receivable and Payable extract - fdr_post_fund change. DB: Dmitri Matveiciuc. */
     --RETURN( '2.2014.09.29.16-47.ID-9.1'); /*  CF-1005 - Decommision report id #373 and send 320 to FDR - Prototype version. DB: Andrei Cataev. */
     --RETURN( '1.2014.09.15.18-00.CF-1005.1'); /*  New: Create new package for report 320+373*. DB Dmitri Matveiciuc. */
END;

-- start section of procedures, functions and types for FDR Extract
FUNCTION p$_get_VT_tmp_fund_list RETURN TypeFundListTable PIPELINED
IS
BEGIN
  FOR l_n IN 1..VT_tmp_fund_list.COUNT
  LOOP
      PIPE ROW( VT_tmp_fund_list( l_n ) );
  END LOOP;
  RETURN;
END;

PROCEDURE init_fund_list
IS
BEGIN
  VT_tmp_fund_list.DELETE;
END;

PROCEDURE add_fund_list( p_fund_seqno IN INTEGER )
IS
BEGIN
  VT_tmp_fund_list.EXTEND;
  VT_tmp_fund_list( VT_tmp_fund_list.COUNT ).fund_seqno := p_fund_seqno;
END;
-- end section of procedures, functions and types for FDR Extract

FUNCTION PARSE_STRING(P_STR IN VARCHAR2) RETURN TypeStringTable PIPELINED IS
    l_comma_index  PLS_INTEGER;
    l_index        PLS_INTEGER := 1;
    l_string       VARCHAR2(4000) := P_STR||',';
    BEGIN
      LOOP
          l_comma_index := INSTR(l_string, ',', l_index);
      EXIT WHEN l_comma_index = 0;
          PIPE ROW ( SUBSTR(l_string, l_index, l_comma_index - l_index) );
          l_index := l_comma_index + 1;
      END LOOP;
    RETURN;
  END PARSE_STRING;


  FUNCTION cashflow_report(p_client_id       IN VARCHAR2,
                           p_fund_group      IN VARCHAR2,
                           p_fund            IN VARCHAR2,
                           p_date_end        IN VARCHAR2,
                           p_cash_type       IN VARCHAR2 DEFAULT NULL,
                           p_period_ind      IN VARCHAR2 DEFAULT NULL,
                           p_trade_source_nm IN VARCHAR2 DEFAULT NULL)
    RETURN  TypeCashFlowTable PIPELINED IS
    rec_out TypeCashFlow;
  BEGIN 
    FOR rec IN (SELECT MAX(asset_id)                  asset_id,
                       MAX(jurisdiction)              jurisdiction,
                       ex_date,
                       MAX(pay_date)                  pay_date,
                       SUM(contracts)                 contracts,
                       MAX(dividend_rate)             dividend_rate,
                       SUM(gross_dividend_local)      gross_dividend_local,
                       SUM(gross_dividend_base)       gross_dividend_base,
                       MAX(pay_out_ratio)             pay_out_ratio,
                       SUM(dividend_income_local)     dividend_income_local,
                       SUM(dividend_income_base)      dividend_income_base,
                       SUM(dividend_expense_local)    dividend_expense_local,
                       SUM(dividend_expense_base)     dividend_expense_base,
                       MAX(long_short_indicator)      long_short_indicator,
                       rec_pay_flg,
                       MAX(income_expense_indicator)  income_expense_indicator,
                       MAX(ex_date_fx_rate)           ex_date_fx_rate,
                       MAX(product_type)              product_type,
                       MAX(prime_broker)              prime_broker,
                       Currency,
                       SUM(unrealized_gain_loss)      unrealized_gain_loss,
                       MAX(client_long_nm)            client_long_nm,
                       MAX(crncy_lng_nm)              crncy_lng_nm,
                       MAX(exchange_rate_source)      exchange_rate_source,
                       MAX(fisc_ye)                   fisc_ye,
                       MAX(fisc_ye_nm)                fisc_ye_nm,
                       MAX(custody_fund_nm)           custody_fund_nm,
                       MAX(client_id)                 client_id,
                       MAX(im_acct_id)                im_acct_id,
                       MAX(base_currency_code)        base_currency_code,
                       MAX(custodian_acct_id)         custodian_acct_id,
                       SUM(prncpl_lcl)                prncpl_lcl,
                       SUM(intrt_lcl)                 intrt_lcl,
                       SUM(lcl_amt)                   lcl_amt,
                       SUM(base_amt)                  base_amt,
                       MAX(undrg_isin)                undrg_isin,
                       MAX(undrg_cusip)               undrg_cusip,
                       MAX(undrg_sedol)               undrg_sedol,
                       MAX(trans_type)                trans_type,
                       MAX(long_short_ind_desc)       long_short_ind_desc,
                       MAX(cash_type)                 cash_type,
                       MAX(issue_longname)            issue_longname,
                       MAX(days_aged)                 days_aged,
                       MAX(trade_price)               trade_price,
                       SUM(commission)                commission,
                       MAX(cashprocessing_flg)        cashprocessing_flg,
                       SUM(current_value_base)        current_value_base,
                       MAX(fx_rate)                   fx_rate,
                       MAX(accounting_date)           accounting_date,
                       SUM(local_withholding)         local_withholding,
                       SUM(base_withholding)          base_withholding,
                       MAX(local_crncy)               local_crncy,
                       MAX(local_crncy_descr)         local_crncy_descr,
                       Fund_id,
                       pbrc_pymnt_nmb,
                       trade_source_seqno             trade_source_seqno,
                       MAX(trade_source_nm)           trade_source_nm,
                       MAX(nra_tax_country)           nra_tax_country,
                       MAX(mch_post_acct)             mch_post_acct,
                       MAX(prcsng_cntr)               prcsng_cntr,
                       MAX(position_dt)               position_dt,
                       MAX(client_short_nm)           client_short_nm,
                       MAX(custodian_acct_nm)         custodian_acct_nm,
                       MAX(custodian_nm)              custodian_nm,
                       MAX(party_1_id)                party_1_id,
                       MAX(product_family)            product_family,
                       MAX(ssc_id)                    ssc_id,
                       MAX(fdr_post_fund)             fdr_post_fund
                  FROM 
                        TABLE(load_raw_table( p_client_id,
                                              p_fund_group,
                                              p_fund,
                                              p_date_end,
                                              p_cash_type,
                                              p_period_ind,
                                              p_trade_source_nm))
                 GROUP BY 
                          Fund_id,
                          rec_pay_flg,
                          Currency,
                          Asset_id,
                          Ex_Date,
                          pbrc_pymnt_nmb,
                          trade_source_seqno
                 ORDER BY 
                          trade_source_nm,
                          Fund_id,
                          rec_pay_flg desc,
                          Currency,
                          pay_Date,
                          Asset_id,
                          pbrc_pymnt_nmb
              )
  LOOP 
      rec_out.asset_id                 := rec.asset_id;
      rec_out.jurisdiction             := rec.jurisdiction;
      rec_out.ex_date                  := rec.ex_date;
      rec_out.pay_date                 := rec.pay_date;
      rec_out.contracts                := rec.contracts;
      rec_out.dividend_rate            := rec.dividend_rate;
      rec_out.gross_dividend_local     := rec.gross_dividend_local;
      rec_out.gross_dividend_base      := rec.gross_dividend_base;
      rec_out.pay_out_ratio            := rec.pay_out_ratio;
      rec_out.dividend_income_local    := rec.dividend_income_local;
      rec_out.dividend_income_base     := rec.dividend_income_base;
      rec_out.dividend_expense_local   := rec.dividend_expense_local;
      rec_out.dividend_expense_base    := rec.dividend_expense_base;
      rec_out.long_short_indicator     := rec.long_short_indicator;
      rec_out.income_expense_indicator := rec.income_expense_indicator;
      rec_out.ex_date_fx_rate          := rec.ex_date_fx_rate;
      rec_out.product_type             := rec.product_type;
      rec_out.prime_broker             := rec.prime_broker;
      rec_out.currency                 := rec.currency;
      rec_out.unrealized_gain_loss     := rec.unrealized_gain_loss;
      rec_out.client_long_nm           := rec.client_long_nm;
      rec_out.crncy_lng_nm             := rec.crncy_lng_nm;
      rec_out.exchange_rate_source     := rec.exchange_rate_source;
      rec_out.fisc_ye                  := rec.fisc_ye;
      rec_out.fisc_ye_nm               := rec.fisc_ye_nm;
      rec_out.custody_fund_nm          := rec.custody_fund_nm;
      rec_out.client_id                := rec.client_id;
      rec_out.im_acct_id               := rec.im_acct_id;
      rec_out.base_currency_code       := rec.base_currency_code;
      rec_out.custodian_acct_id        := rec.custodian_acct_id;
      rec_out.prncpl_lcl               := rec.prncpl_lcl;
      rec_out.intrt_lcl                := rec.intrt_lcl;
      rec_out.lcl_amt                  := rec.lcl_amt;
      rec_out.base_amt                 := rec.base_amt;
      rec_out.undrg_isin               := rec.undrg_isin;
      rec_out.undrg_cusip              := rec.undrg_cusip;
      rec_out.undrg_sedol              := rec.undrg_sedol;
      rec_out.trans_type               := rec.trans_type;
      rec_out.long_short_ind_desc      := rec.long_short_ind_desc;
      rec_out.cash_type                := rec.cash_type;
      rec_out.issue_longname           := rec.issue_longname;
      rec_out.days_aged                := rec.days_aged;
      rec_out.trade_price              := rec.trade_price;
      rec_out.commission               := rec.commission;
      rec_out.cashprocessing_flg       := rec.cashprocessing_flg;
      rec_out.current_value_base       := rec.current_value_base;
      rec_out.rec_pay_flg              := rec.rec_pay_flg;
      rec_out.fx_rate                  := rec.fx_rate;
      rec_out.accounting_date          := rec.accounting_date;
      rec_out.local_crncy              := rec.local_crncy;
      rec_out.local_crncy_descr        := rec.local_crncy_descr;
      rec_out.fund_id                  := rec.fund_id;
      rec_out.trade_source_seqno       := rec.trade_source_seqno;
      rec_out.trade_source_nm          := rec.trade_source_nm;
      rec_out.nra_tax_country          := rec.nra_tax_country;
      rec_out.mch_post_acct            := rec.mch_post_acct;
      rec_out.prcsng_cntr              := rec.prcsng_cntr;
      rec_out.pbrc_pymnt_nmb           := rec.pbrc_pymnt_nmb;
      rec_out.position_dt              := rec.position_dt;
      rec_out.client_short_nm          := rec.client_short_nm;
      rec_out.custodian_acct_nm        := rec.custodian_acct_nm;
      rec_out.custodian_nm             := rec.custodian_nm;
      rec_out.party_1_id               := rec.party_1_id;
      rec_out.product_family           := rec.product_family;
      rec_out.ssc_id                   := rec.ssc_id;
      -->ID-153
      rec_out.fdr_post_fund            := rec.fdr_post_fund;
      --<ID-153
      
      
      PIPE ROW(rec_out);
  END LOOP;
    RETURN;
END CashFlow_report;
--------  
FUNCTION load_raw_table( p_client_id        IN VARCHAR2,
                         p_fund_group       IN VARCHAR2,
                         p_fund             IN VARCHAR2,
                         p_date_end         IN VARCHAR2,
                         p_cash_type        IN VARCHAR2 DEFAULT NULL,
                         p_period_ind       IN VARCHAR2 DEFAULT NULL,
                         p_trade_source_nm  IN VARCHAR2 DEFAULT NULL)
    RETURN TypeCashFlowRawTable PIPELINED IS
    rec_out    TypeCashFlowRaw;
    v_end_date DATE;
    l_period_ind CHAR(1);
  BEGIN
    IF p_client_id IS NULL AND p_fund_group IS NULL AND p_fund IS NULL THEN
       raise_application_error(-20000, 'CLIENT or ACCOUNT or FUND GROUP is required');
    END IF;
    --
    v_end_date := NVL(TO_DATE(p_date_end, 'YYYY/MM/DD'), SYSDATE);
    l_period_ind := NVL(p_period_ind, 'D');
    --
    FOR rec IN
    ( WITH
      --
      fund_gtt AS
      --
      ( SELECT  --+ MATERIALIZE
                DISTINCT 
                c.client_id,
                f.custodian_acct_id,
                f.base_currency_code,
                f.exchange_rate_source,
                f.custody_fund_nm,
                f.im_acct_id,
                f.custodian_nm,
                f.fisc_ye,
                f.custodian_acct_nm,
                c.client_short_nm,
                c.client_long_nm,
                f.acctg_cnvrn_dt,
                s.source_seqno,
                s.source_nm,
                px.wrhs_seqno         source_wrhs_seqno,
                f.mch_post_acct,
                f.prcsng_cntr,
                f.fdr_post_fund
          FROM  otc_wrhs_reference.fund                        f
                INNER JOIN otc_wrhs_reference.client           c   ON f.client_seqno  = c.client_seqno
                INNER JOIN otc_hub_rules.source                s   ON s.source_seqno  = f.source_seqno 
                INNER JOIN otc_wrhs_position.position_xrfrne  px   ON px.source_seqno = f.source_seqno
                                                                  AND px.acct_id      = f.im_acct_id
         WHERE (
                 (
                  NVL(p_fund,'N') != 'LIST:'
             AND (p_client_id       IS NULL OR p_client_id = 'ALL' OR c.client_id         = p_client_id)
             AND (p_fund            IS NULL OR p_fund = 'ALL'      OR f.custodian_acct_id = p_fund)
             AND (p_trade_source_nm IS NULL OR s.source_nm = p_trade_source_nm)
             AND (p_fund_group IS NULL OR EXISTS
                                            (SELECT '*'
                                               FROM otc_wrhs_reference.fund_list      fl,
                                                    otc_wrhs_reference.fund_list_item fli
                                              WHERE fl.fund_list_short_nm = p_fund_group
                                                AND fli.fund_list_seqno   = fl.fund_list_seqno
                                                AND fli.fund_item_seqno   = f.fund_seqno))
                   )
               OR (
                   NVL(p_fund,'N') = 'LIST:'
              AND (f.fund_seqno IN (SELECT fund_seqno FROM TABLE(p$_get_VT_tmp_fund_list)))
                  )
                 )
      ),
      --
      acct_pybl_rcvbl_gtt AS 
      -- 
      ( SELECT  --+ MATERIALIZE
                pbrc.acct_pybl_rcvbl_seqno,
                pbrc.im_acct_id,
                pbrc.custodian_acct_id,
                pbrc.asset,
                pbrc.trade_id,
                pbrc.ssc_trx_id,
                pbrc.setle_dt,
                pbrc.cash_type,
                pbrc.cash_source,
                pbrc.lcl_amt,
                pbrc.lcl_crncy,
                pbrc.rec_pay_flg,
                pbrc.xrate,
                pbrc.base_amt,
                pbrc.base_crncy,
                pbrc.acctg_dt,
                pbrc.wrhs_seqno,
                pbrc.wrhs_dt,
                pbrc.upd_login,
                pbrc.upd_dt,
                pbrc.alt_acctg_dt,
                pbrc.intrt_lcl,
                pbrc.settle_crncy,
                pbrc.trade_dt,
                pbrc.notnl_amt,
                pbrc.prncpl_lcl,
                pbrc.setle_amt,
                pbrc.trade_dt_fx_rate,
                pbrc.setle_base_amt,
                pbrc.brkge_amt,
                pbrc.trade_type,
                pbrc.actvy_flg,
                pbrc.long_short_ind,
                pbrc.payout_ratio,
                pbrc.gross_cashdiv_lcl,
                pbrc.dvdnd_rate,
                pbrc.gross_cashdiv_bse,
                pbrc.cfd_price            trade_price,
                pbrc.cmsn_amt             commission,
                pbrc.cash_processing      cashprocessing_flg,
                pbrc.pymnt_nmb,
                pbrc.cancel_flg,
                pbrc.rate_absnt_flg
          FROM  fund_gtt                                  fg 
                INNER JOIN otc_wrhs_cash.acct_pybl_rcvbl  pbrc  ON pbrc.im_acct_id       = fg.im_acct_id
                                                               AND fg.source_wrhs_seqno  = pbrc.wrhs_seqno
                INNER JOIN otc_wrhs_reference.crncy       cc    ON fg.base_currency_code = cc.crncy_cd
         WHERE (p_cash_type    IS NULL          OR 
               (p_cash_type    IS NOT NULL      AND
                pbrc.cash_type IN (SELECT * FROM TABLE(PARSE_STRING(p_cash_type)))))
           AND  acctg_dt       <= TO_DATE(p_date_end, 'YYYY/MM/DD')
           AND ((fg.acctg_cnvrn_dt IS NOT NULL  AND  pbrc.acctg_dt >= fg.acctg_cnvrn_dt)
            OR  (fg.acctg_cnvrn_dt IS NULL      AND  pbrc.acctg_dt <= TO_DATE(p_date_end, 'YYYY/MM/DD')))
            -->ID-156
           AND  pbrc.actvy_flg   = 'Y'
           AND  pbrc.cancel_flg != 'Y'
           --<ID-156
      ),
      --
      mchcash_gtt AS
      --
     (  SELECT  --+ MATERIALIZE
                mc.mchcash_seqno,
                mc.fund,
                mc.asset,
                mc.trade_id,
                mc.lcl_crncy,
                mc.lcl_amt,
                mc.acct_dt,
                mc.setle_dt,
                mc.base_crncy,
                mc.base_amt,
                mc.xrate,
                mc.post_dscrn,
                mc.rec_pay_flg,
                mc.mch_stts,
                mc.mch_err_msg,
                mc.post_tm,
                mc.upd_login,
                mc.upd_dt,
                mc.setle_crncy,
                mc.trans_type,
                mc.setle_amt,
                mc.otc_hub_stts,
                mc.mch_post_nmbr,
                mc.mch_error_nmbr,
                mc.mchcash_original_seqno,
                mc.ldgr_cash_offset_subacct,
                mc.ldgr_cash_offset_acct,
                mc.orgn,
                mc.cash_srce,
                mc.ldgr_cash_acct,
                mc.offset_ldgr_acct,
                mc.basis,
                mc.altve_acct_dt,
                mc.im_acct_id,
                mc.ssc_trx_id,
                mc.wrhs_seqno,
                mc.wrhs_dt,
                mc.trade_dt,
                mc.notnl_amt,
                mc.prncpl_lcl,
                mc.intrt_lcl,
                mc.acct_pybl_rcvbl_seqno,
                mc.prncpl_base,
                mc.intrt_base,
                mc.trade_dt_fx_rate,
                mc.rlzd_crncy_gain,
                mc.rlzd_crncy_loss,
                mc.setle_base_amt,
                mc.brkge_amt,
                mc.trade_type,
                mc.brkge_amt_base,
                mc.unwind_flg,
                mc.actvy_flg,
                mc.long_short_ind,
                mc.payout_ratio,
                mc.gross_cashdiv_lcl,
                mc.cash_processing,
                mc.cfd_price,
                mc.cmsn_amt,
                mc.pymnt_nmb
          FROM  fund_gtt                            fg
                INNER JOIN  otc_wrhs_cash.mchcash   mc   ON mc.im_acct_id         = fg.im_acct_id 
                                                        AND fg.source_wrhs_seqno  = mc.wrhs_seqno
                INNER JOIN otc_wrhs_reference.crncy cc   ON fg.base_currency_code = cc.crncy_cd
         WHERE (p_cash_type   IS NULL            OR
               (p_cash_type   IS NOT NULL        AND 
                mc.trans_type IN (SELECT * FROM TABLE(PARSE_STRING(p_cash_type)))))
           AND  mc.acct_dt    <= TO_DATE(p_date_end,'YYYY/MM/DD')
           AND ((fg.acctg_cnvrn_dt IS NOT NULL   AND  mc.acct_dt >= fg.acctg_cnvrn_dt)
            OR  (fg.acctg_cnvrn_dt IS NULL       AND  mc.acct_dt <= TO_DATE(p_date_end, 'YYYY/MM/DD')))
          -->ID-156
           AND mc.actvy_flg   = 'Y'
           AND mc.cancel_flg != 'Y'
          --<ID-156 
      ),
      --
      trd_activity_gtt AS
      --
      ( SELECT  --+ MATERIALIZE
                DISTINCT
                ta.party_1_id,
                ta.ssc_id,
                ta.ssc_trx_id
          FROM  acct_pybl_rcvbl_gtt                        pbrc
                INNER JOIN otc_wrhs_position.trd_activity    ta  ON   pbrc.ssc_trx_id = ta.ssc_trx_id
         WHERE 1=1
           AND ta.trd_status  = 0
           AND ta.applied_flg = 1
           AND ta.acct_dt    <= TO_DATE(p_date_end, 'YYYY/MM/DD')
      ),
      --
      exchrate_gtt as 
      --  
     (  SELECT  --+ MATERIALIZE INDEX(rt exchrate_uk1)
                DISTINCT
                ex.exchrate_dt    exchrate_dt,
                ex.currency_from  currency_from,
                ex.currency_to    currency_to,
                ex.rate_last ,
                sr.source_nm      source_nm
          FROM  otc_wrhs_reference.exchrate           ex,
                otc_wrhs_reference.wrhs_identifier    wi,
                otc_hub_rules.data_ctgry              rtctgr,
                otc_hub_rules.source                  sr,
                acct_pybl_rcvbl_gtt                   pbrc,
                fund_gtt                              fg
         WHERE ex.currency_to          = fg.base_currency_code
           AND sr.source_nm            = fg.exchange_rate_source
           AND ex.currency_from        = pbrc.lcl_crncy
           AND ex.exchrate_dt          = TO_DATE(p_date_end,'YYYY/MM/DD')
           AND ex.wrhs_seqno           = wi.wrhs_seqno
           AND rtctgr.data_ctgry_seqno = wi.data_ctgry_seqno
           AND rtctgr.data_ctgry_nm    = 'REFERENCE'
           AND wi.source_seqno         = sr.source_seqno
     )
    -- 
    -- base select
    --
   	 SELECT asset_id,
            jurisdiction,
            ex_date,
            pay_date,
            contracts,
            dividend_rate,
            gross_dividend_local,
            gross_dividend_base,
            pay_out_ratio,
            dividend_income_local,
            dividend_income_base,
            dividend_expense_local,
            dividend_expense_base,
            long_short_indicator,
            income_expense_indicator,
            ROUND(ex_date_fx_rate, 6)         ex_date_fx_rate,
            product_type,
            prime_broker,
            currency,
            fx_rate,
            notnl_amt,
            rec_pay_flg,
            fund_id,
            setle_amt,
            setle_base_amt,
            client_long_nm,
            crncy_lng_nm,
            exchange_rate_source,
            fisc_ye,
            fisc_ye_nm,
            custody_fund_nm,
            client_id,
            im_acct_id,
            base_currency_code,
            custodian_acct_id,
            prncpl_lcl,
            intrt_lcl,
            lcl_amt,
            base_amt,
            undrg_isin,
            undrg_cusip,
            undrg_sedol,
            trans_type,
            long_short_ind_desc,
            cash_type,
            issue_longname,
            days_aged,
            trade_price,
            commission,
            cashprocessing_flg,
            accounting_date,
            ssc_trx_id,
            mcsh_lcl_amt,
            local_crncy,
            local_crncy_descr,
            pbrc_pymnt_nmb,
            mcsh_pymnt_nmb,
            CASE fx_rate
              WHEN 0 THEN 0
              ELSE  CASE long_short_indicator
                      WHEN 'S' THEN -1 * ROUND((lcl_amt / fx_rate) - base_amt,2)
                      ELSE ROUND((lcl_amt / fx_rate) - base_amt,2)
                    END
            END                                                                   unrealized_gain_loss,
            CASE fx_rate
              WHEN 0 THEN 0
              ELSE ROUND((lcl_amt / fx_rate),2) 
            END                                                                   current_value_base,
            trade_source_seqno,
            trade_source_nm,
            nra_tax_country,
            --373
            position_dt,
            client_short_nm,
            custodian_acct_nm,
            custodian_nm,
            party_1_id,
            product_family,
            ssc_id,
            mch_post_acct,
            prcsng_cntr,
            cancel_flg,
            fdr_post_fund,
            -->ID-156
            mcsh_setle_amt,
            mcsh_brkge_amt,
            mcsh_rec_pay_flg,
            mcsh_base_amt
            --<ID-156
      FROM (
            SELECT  pbrc.asset_id,
                    pbrc.jurisdiction,
                    pbrc.ex_date,
                    pbrc.pay_date,
                    pbrc.contracts,
                    pbrc.dividend_rate,
                    pbrc.gross_dividend_local,
                    pbrc.gross_dividend_base,
                    pbrc.pay_out_ratio,
                    pbrc.dividend_income_local,
                    pbrc.dividend_income_base,
                    pbrc.dividend_expense_local,
                    pbrc.dividend_expense_base,
                    CASE pbrc.long_short_indicator
                      WHEN 'L' THEN 'Long Intent'
                      WHEN 'S' THEN 'Short Intent'
                      ELSE NULL
                    END                                   long_short_ind_desc,
                    pbrc.long_short_indicator,
                    pbrc.income_expense_indicator,
                    pbrc.ex_date_fx_rate,
                    pbrc.product_type,
                    pbrc.prime_broker,
                    pbrc.currency,
                    pbrc.fx_rate,
                    pbrc.notnl_amt,
                    CASE TRIM(pbrc.rec_pay_flg)
                      WHEN 'Receipt'      THEN 'Receivable'
                      WHEN 'Disbursement' THEN 'Payable'
                      ELSE pbrc.rec_pay_flg
                    end                                    rec_pay_flg,
                    pbrc.fund_id,
                    pbrc.setle_amt,
                    pbrc.setle_base_amt,
                    pbrc.ssc_trx_id,
                    pbrc.im_acct_id,
                    pbrc.lcl_amt,
                    pbrc.client_long_nm,
                    pbrc.crncy_lng_nm,
                    pbrc.exchange_rate_source,
                    pbrc.fisc_ye,
                    pbrc.fisc_ye_nm,
                    pbrc.custody_fund_nm,
                    pbrc.client_id,
                    pbrc.base_currency_code,
                    pbrc.custodian_acct_id,
                    pbrc.prncpl_lcl,
                    pbrc.intrt_lcl,
                    pbrc.base_amt,
                    pbrc.undrg_isin,
                    pbrc.undrg_cusip,
                    pbrc.undrg_sedol,
                    pbrc.cash_type,
                    pbrc.issue_longname,
                    CASE 
                      WHEN pbrc.days_aged >= 0 
                      THEN pbrc.days_aged
                      ELSE 0 
                    END                                     days_aged,
                    pbrc.trade_price,
                    pbrc.commission,
                    cashprocessing_flg,
                    accounting_date,
                    local_crncy,
                    local_crncy_descr,
                    pbrc.pbrc_pymnt_nmb,
                    mcsh.pymnt_nmb                         mcsh_pymnt_nmb,
                    NVL(mcsh.trans_type, pbrc.cash_type)   trans_type,
                    NVL(mcsh.mcsh_lcl_amt, 0)              mcsh_lcl_amt,
                    pbrc.trade_source_seqno,
                    pbrc.trade_source_nm,
                    nra_tax_country,
                    --373
                    NVL(v_end_date, accounting_date)      position_dt,
                    client_short_nm,
                    custodian_acct_nm,
                    custodian_nm,
                    party_1_id,
                    product_family,
                    ssc_id,
                    mch_post_acct,
                    prcsng_cntr,
                    cancel_flg,
                    pbrc.fdr_post_fund,
                    -->ID-156
                    NVL(mcsh.setle_amt, 0)        mcsh_setle_amt,
                    NVL(mcsh.brkge_amt, 0)        mcsh_brkge_amt,
                    mcsh.rec_pay_flg              mcsh_rec_pay_flg,
                    NVL(mcsh.base_amt, 0)         mcsh_base_amt
                    --<ID-156
                    
              FROM (
                    SELECT pb.asset                                 asset_id,
                           MAX(otsc.cntry)                          jurisdiction,
                           MAX(pb.trade_dt)                         ex_date,
                           pb.setle_dt                              pay_date,
                           SUM(round(pb.notnl_amt, 2))              contracts,
                           MAX(pb.dvdnd_rate)                       dividend_rate,
                           SUM(round(pb.gross_cashdiv_lcl, 2))      gross_dividend_local,
                           SUM(round(pb.gross_cashdiv_bse, 2))      gross_dividend_base,
                           MAX(CASE 
                                WHEN pb.cancel_flg = 'Y' 
                                THEN -1 * pb.payout_ratio
                                ELSE pb.payout_ratio
                               END)                                 pay_out_ratio,
                           SUM(CASE
                                WHEN pb.long_short_ind = 'L'
                                THEN round(pb.lcl_amt, 2)
                                ELSE 0
                               END)                                 dividend_income_local,
                           SUM(CASE
                                WHEN pb.long_short_ind = 'L' 
                                THEN round(pb.base_amt, 2)
                                ELSE 0
                               END)                                 dividend_income_base,
                           SUM(CASE
                                WHEN pb.long_short_ind = 'S' 
                                THEN round(pb.lcl_amt, 2)
                                ELSE 0
                               END)                                 dividend_expense_local,
                           SUM(CASE
                                WHEN pb.long_short_ind = 'S' 
                                THEN round(pb.base_amt, 2)
                                ELSE 0
                               END)                                 dividend_expense_base,
                           MAX(pb.long_short_ind)                   long_short_indicator,
                           MAX(pb.rec_pay_flg)                      income_expense_indicator,
                           MAX(pb.trade_dt_fx_rate)                 ex_date_fx_rate,
                           MAX(otsc.product_type)                   product_type,
                           NULL                                     prime_broker,
                           MAX(pb.lcl_crncy)                        currency,
                           MAX(NVL(ex.rate_last, 0))                fx_rate,
                           SUM(round(pb.notnl_amt, 2))              notnl_amt,
                           pb.rec_pay_flg,
                           MAX(fg.custodian_acct_id)                fund_id,
                           SUM(round(pb.setle_amt, 2))              setle_amt,
                           SUM(round(pb.setle_base_amt, 2))         setle_base_amt,
                           pb.ssc_trx_id                            ssc_trx_id,
                           pb.im_acct_id                            im_acct_id,
                           SUM(round(pb.lcl_amt, 2))                lcl_amt,
                           MAX(fg.client_long_nm)                   client_long_nm,
                           MAX(cc.crncy_lng_nm)                     crncy_lng_nm,
                           MAX(fg.exchange_rate_source)             exchange_rate_source,
                           MAX(fg.fisc_ye)                          fisc_ye,
                           MAX(DECODE(fg.fisc_ye, 131, 'January 31',
                                                  228, 'February 28',
                                                  229, 'February 29',
                                                  331, 'March 31',
                                                  430, 'April 30',
                                                  531, 'May 31',
                                                  630, 'June 30',
                                                  731, 'July 31',
                                                  831, 'August 31',
                                                  930, 'September 30',
                                                 1031, 'October 31',
                                                 1130, 'November 30',
                                                 1231, 'December 31',
                                                       'Unknown'))  fisc_ye_nm,
                           MAX(fg.custody_fund_nm)                  custody_fund_nm,
                           MAX(fg.client_id)                        client_id,
                           MAX(fg.base_currency_code)               base_currency_code,
                           MAX(fg.custodian_acct_id)                custodian_acct_id,
                           SUM(round(pb.prncpl_lcl, 2))             prncpl_lcl,
                           SUM(round(pb.intrt_lcl, 2))              intrt_lcl,
                           SUM(round(pb.base_amt, 2))               base_amt,
                           MAX(otsc.undrg_isin)                     undrg_isin,
                           MAX(otsc.undrg_cusip)                    undrg_cusip,
                           MAX(otsc.undrg_sedol)                    undrg_sedol,
                           MAX(pb.cash_type)                        cash_type,
                           MAX(otsc.issue_longname)                 issue_longname,
                           MAX(v_end_date - pb.setle_dt)            days_aged,
                           MAX(pb.trade_price)                      trade_price,
                           MAX(pb.commission)                       commission,
                           MAX(pb.cashprocessing_flg)               cashprocessing_flg,
                           MAX(pb.acctg_dt)                         accounting_date,
                           pb.settle_crncy                          local_crncy,
                           MAX(cc1.crncy_lng_nm)                    local_crncy_descr,
                           pb.pymnt_nmb                             pbrc_pymnt_nmb,
                           fg.source_seqno                          trade_source_seqno,
                           MAX(fg.source_nm)                        trade_source_nm,
                           MAX(otsc.trade_country_cd)               nra_tax_country,
                           --373
                           MAX(fg.client_short_nm)                  client_short_nm,
                           MAX(fg.custodian_acct_nm)                custodian_acct_nm,
                           MAX(fg.custodian_nm)                     custodian_nm,
                           MAX(ta.party_1_id)                       party_1_id,
                           MAX(otsc.product_family)                 product_family,
                           MAX(ta.ssc_id)                           ssc_id,
                           MAX(fg.mch_post_acct)                    mch_post_acct,
                           MAX(fg.prcsng_cntr)                      prcsng_cntr,
                           MAX(pb.cancel_flg)                       cancel_flg,
                           MAX(fg.fdr_post_fund)                    fdr_post_fund
                      FROM acct_pybl_rcvbl_gtt                           pb
                           INNER JOIN otc_wrhs_reference.otc_security  otsc  ON pb.asset              = otsc.otc_id
                           INNER JOIN fund_gtt                           fg  ON pb.im_acct_id         = fg.im_acct_id 
                                                                            AND fg.source_wrhs_seqno  = pb.wrhs_seqno
                           INNER JOIN otc_wrhs_reference.crncy           cc  ON fg.base_currency_code = cc.crncy_cd
                           INNER JOIN otc_wrhs_reference.crncy          cc1  ON pb.settle_crncy       = cc1.crncy_cd
                           LEFT OUTER JOIN exchrate_gtt                  ex  ON ex.currency_from      = pb.lcl_crncy
                                                                            AND ex.currency_to        = fg.base_currency_code
                                                                            AND ex.exchrate_dt        = v_end_date
                                                                            AND ex.source_nm          = fg.exchange_rate_source
                           LEFT OUTER JOIN trd_activity_gtt              ta  ON pb.ssc_trx_id         = ta.ssc_trx_id  
                     GROUP BY pb.asset,
                              pb.ssc_trx_id,
                              pb.im_acct_id,
                              pb.setle_dt,
                              pb.rec_pay_flg,
                              pb.pymnt_nmb,
                              pb.settle_crncy,
                              fg.source_seqno
                    )                                                 pbrc
                      
                    LEFT OUTER JOIN mchcash_gtt                       mcsh   ON  mcsh.acct_pybl_rcvbl_seqno = pbrc.acct_pybl_rcvbl_seqno
                                                                            
              )                                                  pybl_rcvbl
         WHERE pybl_rcvbl.lcl_amt <> pybl_rcvbl.mcsh_lcl_amt
           -->ID-156
           AND (
                NVL(pybl_rcvbl.mcsh_setle_amt + pybl_rcvbl.mcsh_brkge_amt, 0) != pybl_rcvbl.lcl_amt 
            OR  NVL(pybl_rcvbl.mcsh_rec_pay_flg, 'X')                         != pybl_rcvbl.rec_pay_flg 
            OR  NVL(pybl_rcvbl.mcsh_base_amt, 0)                              != NVL(pybl_rcvbl.base_amt, 0)
                )
           --<ID-156
           AND (l_period_ind != 'M' 
            OR (l_period_ind  = 'M' AND NVL(pybl_rcvbl.cancel_flg,'N') != 'Y')) 
    )
  LOOP
      rec_out.asset_id                 := rec.asset_id;
      rec_out.jurisdiction             := rec.jurisdiction;
      rec_out.ex_date                  := rec.ex_date;
      rec_out.pay_date                 := rec.pay_date;
      rec_out.contracts                := rec.contracts;
      rec_out.dividend_rate            := rec.dividend_rate;
      rec_out.gross_dividend_local     := rec.gross_dividend_local;
      rec_out.gross_dividend_base      := rec.gross_dividend_base;
      rec_out.pay_out_ratio            := rec.pay_out_ratio;
      rec_out.dividend_income_local    := rec.dividend_income_local;
      rec_out.dividend_income_base     := rec.dividend_income_base;
      rec_out.dividend_expense_local   := rec.dividend_expense_local;
      rec_out.dividend_expense_base    := rec.dividend_expense_base;
      rec_out.long_short_indicator     := rec.long_short_indicator;
      rec_out.income_expense_indicator := rec.income_expense_indicator;
      rec_out.ex_date_fx_rate          := rec.ex_date_fx_rate;
      rec_out.product_type             := rec.product_type;
      rec_out.prime_broker             := rec.prime_broker;
      rec_out.currency                 := rec.currency;
      rec_out.fx_rate                  := rec.fx_rate;
      rec_out.rec_pay_flg              := rec.rec_pay_flg;
      rec_out.fund_id                  := rec.fund_id;
      rec_out.setle_amt                := rec.setle_amt;
      rec_out.setle_base_amt           := rec.setle_base_amt;
      rec_out.client_long_nm           := rec.client_long_nm;
      rec_out.crncy_lng_nm             := rec.crncy_lng_nm;
      rec_out.exchange_rate_source     := rec.exchange_rate_source;
      rec_out.fisc_ye                  := rec.fisc_ye;
      rec_out.fisc_ye_nm               := rec.fisc_ye_nm;
      rec_out.custody_fund_nm          := rec.custody_fund_nm;
      rec_out.client_id                := rec.client_id;
      rec_out.im_acct_id               := rec.im_acct_id;
      rec_out.base_currency_code       := rec.base_currency_code;
      rec_out.custodian_acct_id        := rec.custodian_acct_id;
      rec_out.prncpl_lcl               := rec.prncpl_lcl;
      rec_out.intrt_lcl                := rec.intrt_lcl;
      rec_out.lcl_amt                  := rec.lcl_amt;
      rec_out.base_amt                 := rec.base_amt;
      rec_out.undrg_isin               := rec.undrg_isin;
      rec_out.undrg_cusip              := rec.undrg_cusip;
      rec_out.undrg_sedol              := rec.undrg_sedol;
      rec_out.trans_type               := rec.trans_type;
      rec_out.long_short_ind_desc      := rec.long_short_ind_desc;
      rec_out.cash_type                := rec.cash_type;
      rec_out.issue_longname           := rec.issue_longname;
      rec_out.days_aged                := rec.days_aged;
      rec_out.trade_price              := rec.trade_price;
      rec_out.commission               := rec.commission;
      rec_out.cashprocessing_flg       := rec.cashprocessing_flg;
      rec_out.accounting_date          := rec.accounting_date;
      rec_out.ssc_trx_id               := rec.ssc_trx_id;
      rec_out.local_withholding        := ABS(rec.gross_dividend_local -
                                              rec.dividend_income_local);
      rec_out.base_withholding         := ABS(rec.gross_dividend_base -
                                              rec.dividend_income_base);
      rec_out.pbrc_pymnt_nmb           := rec.pbrc_pymnt_nmb;
      rec_out.mcsh_pymnt_nmb           := rec.mcsh_pymnt_nmb;
      rec_out.local_crncy              := rec.local_crncy;
      rec_out.local_crncy_descr        := rec.local_crncy_descr;
      rec_out.current_value_base       := rec.current_value_base;
      rec_out.unrealized_gain_loss     := rec.unrealized_gain_loss;
      rec_out.trade_source_seqno       := rec.trade_source_seqno;
      rec_out.trade_source_nm          := rec.trade_source_nm;
      rec_out.nra_tax_country          := rec.nra_tax_country;
      --373
      rec_out.client_short_nm          := rec.client_short_nm;
      rec_out.custodian_acct_nm        := rec.custodian_acct_nm;
      rec_out.custodian_nm             := rec.custodian_nm;
      rec_out.party_1_id               := rec.party_1_id;
      rec_out.product_family           := rec.product_family;
      rec_out.ssc_id                   := rec.ssc_id;
      rec_out.mch_post_acct            := rec.mch_post_acct;
      rec_out.prcsng_cntr              := rec.prcsng_cntr;
      rec_out.position_dt              := rec.position_dt;
      -->ID-153
      rec_out.fdr_post_fund            := rec.fdr_post_fund;
      --<ID-153
      
      PIPE ROW(rec_out);
    END LOOP;
    RETURN;
  END load_raw_table;
-------------------------------
  FUNCTION cashflow_smry_report(p_client_id       IN VARCHAR2,
                                p_fund_group      IN VARCHAR2,
                                p_fund            IN VARCHAR2,
                                p_date_end        IN VARCHAR2,
                                p_cash_type       IN VARCHAR2 DEFAULT NULL,
                                p_period_ind      IN VARCHAR2 DEFAULT NULL,
                                p_trade_source_nm IN VARCHAR2 DEFAULT NULL)
    RETURN TypeCashFlowSmryTable PIPELINED IS
    rec_out TypeCashFlowSmry;
  BEGIN
    FOR rec IN (SELECT  fund_id,
                        MAX(jurisdiction)               jurisdiction,
                        SUM(contracts)                  contracts,
                        SUM(gross_dividend_local)       gross_dividend_local,
                        SUM(gross_dividend_base)        gross_dividend_base,
                        SUM(dividend_income_local)      dividend_income_local,
                        SUM(dividend_income_base)       dividend_income_base,
                        SUM(dividend_expense_local)     dividend_expense_local,
                        SUM(dividend_expense_base)      dividend_expense_base,
                        currency,
                        SUM(unrealized_gain_loss)       unrealized_gain_loss,
                        MAX(client_long_nm)             client_long_nm,
                        MAX(crncy_lng_nm)               crncy_lng_nm,
                        MAX(exchange_rate_source)       exchange_rate_source,
                        MAX(fisc_ye)                    fisc_ye,
                        MAX(fisc_ye_nm)                 fisc_ye_nm,
                        MAX(custody_fund_nm)            custody_fund_nm,
                        MAX(client_id)                  client_id,
                        MAX(im_acct_id)                 im_acct_id,
                        MAX(base_currency_code)         base_currency_code,
                        MAX(custodian_acct_id)          custodian_acct_id,
                        MAX(income_expense_indicator)   income_expense_indicator,
                        SUM(prncpl_lcl)                 prncpl_lcl,
                        SUM(intrt_lcl)                  intrt_lcl,
                        SUM(lcl_amt)                    lcl_amt,
                        SUM(base_amt)                   base_amt,
                        MAX(undrg_isin)                 undrg_isin,
                        MAX(undrg_cusip)                undrg_cusip,
                        MAX(undrg_sedol)                undrg_sedol,
                        MAX(trans_type)                 trans_type,
                        MAX(long_short_ind_desc)        long_short_ind_desc,
                        MAX(cash_type)                  cash_type,
                        MAX(issue_longname)             issue_longname,
                        MAX(days_aged)                  days_aged,
                        MAX(trade_price)                trade_price,
                        SUM(commission)                 commission,
                        MAX(cashprocessing_flg)         cashprocessing_flg,
                        MAX(ex_date)                    ex_date,
                        MAX(pay_date)                   pay_date,
                        SUM(current_value_base)         current_value_base,
                        rec_pay_flg,
                        MAX(fx_rate)                    fx_rate,
                        MAX(accounting_date)            accounting_date,
                        MAX(long_short_indicator)       long_short_indicator,
                        MAX(local_crncy)                local_crncy,
                        MAX(local_crncy_descr)          local_crncy_descr,
                        trade_source_seqno              trade_source_seqno,
                        trade_source_nm                 trade_source_nm,
                        MAX(client_short_nm)            client_short_nm,
                        MAX(custodian_acct_nm)          custodian_acct_nm,
                        MAX(custodian_nm)               custodian_nm,
                        MAX(party_1_id)                 party_1_id,
                        MAX(product_family)             product_family,
                        MAX(ssc_id)                     ssc_id,
                        MAX(mch_post_acct)              mch_post_acct,
                        MAX(prcsng_cntr)                prcsng_cntr
                  FROM  TABLE(cashflow_report( p_client_id,
                                               p_fund_group,
                                               p_fund,
                                               p_date_end,
                                               p_cash_type,
                                               p_period_ind,
                                               p_trade_source_nm))
                 GROUP BY fund_id,
                          rec_pay_flg,
                          currency,
                          trade_source_seqno,
                          trade_source_nm
                 ORDER BY fund_id,
                          rec_pay_flg DESC,
                          currency
                )
  LOOP
      rec_out.jurisdiction             := rec.jurisdiction;
      rec_out.contracts                := rec.contracts;
      rec_out.gross_dividend_local     := rec.gross_dividend_local;
      rec_out.gross_dividend_base      := rec.gross_dividend_base;
      rec_out.dividend_income_local    := rec.dividend_income_local;
      rec_out.dividend_income_base     := rec.dividend_income_base;
      rec_out.dividend_expense_local   := rec.dividend_expense_local;
      rec_out.dividend_expense_base    := rec.dividend_expense_base;
      rec_out.currency                 := rec.currency;
      rec_out.unrealized_gain_loss     := rec.unrealized_gain_loss;
      rec_out.client_long_nm           := rec.client_long_nm;
      rec_out.crncy_lng_nm             := rec.crncy_lng_nm;
      rec_out.exchange_rate_source     := rec.exchange_rate_source;
      rec_out.fisc_ye                  := rec.fisc_ye;
      rec_out.fisc_ye_nm               := rec.fisc_ye_nm;
      rec_out.custody_fund_nm          := rec.custody_fund_nm;
      rec_out.client_id                := rec.client_id;
      rec_out.im_acct_id               := rec.im_acct_id;
      rec_out.base_currency_code       := rec.base_currency_code;
      rec_out.custodian_acct_id        := rec.custodian_acct_id;
      rec_out.income_expense_indicator := rec.income_expense_indicator;
      rec_out.prncpl_lcl               := rec.prncpl_lcl;
      rec_out.intrt_lcl                := rec.intrt_lcl;
      rec_out.lcl_amt                  := rec.lcl_amt;
      rec_out.base_amt                 := rec.base_amt;
      rec_out.undrg_isin               := rec.undrg_isin;
      rec_out.undrg_cusip              := rec.undrg_cusip;
      rec_out.undrg_sedol              := rec.undrg_sedol;
      rec_out.trans_type               := rec.trans_type;
      rec_out.long_short_ind_desc      := rec.long_short_ind_desc;
      rec_out.cash_type                := rec.cash_type;
      rec_out.issue_longname           := rec.issue_longname;
      rec_out.days_aged                := rec.days_aged;
      rec_out.trade_price              := rec.trade_price;
      rec_out.commission               := rec.commission;
      rec_out.cashprocessing_flg       := rec.cashprocessing_flg;
      rec_out.current_value_base       := rec.current_value_base;
      rec_out.rec_pay_flg              := rec.rec_pay_flg;
      rec_out.fx_rate                  := rec.fx_rate;
      rec_out.accounting_date          := rec.accounting_date;
      CASE rec_out.rec_pay_flg
        WHEN 'Payable' 
        THEN rec_out.local_withholding := 0;
             rec_out.base_withholding  := 0;
        ELSE rec_out.local_withholding := nvl(rec.Gross_Dividend_Local, 0) -
                                          nvl(rec.Dividend_Income_Local, 0);
             rec_out.base_withholding  := nvl(rec.Gross_Dividend_Base, 0) -
                                          nvl(rec.Dividend_Income_Base, 0);
      END CASE;
      rec_out.local_crncy              := rec.local_crncy;
      rec_out.local_crncy_descr        := rec.local_crncy_descr;
      rec_out.long_short_indicator     := rec.long_short_indicator;
      rec_out.trade_source_seqno       := rec.trade_source_seqno;
      rec_out.trade_source_nm          := rec.trade_source_nm;
      --373
      rec_out.client_short_nm          := rec.client_short_nm;
      rec_out.custodian_acct_nm        := rec.custodian_acct_nm;
      rec_out.custodian_nm             := rec.custodian_nm;
      rec_out.party_1_id               := rec.party_1_id;
      rec_out.product_family           := rec.product_family;
      rec_out.ssc_id                   := rec.ssc_id;
      rec_out.mch_post_acct            := rec.mch_post_acct;
      rec_out.prcsng_cntr              := rec.prcsng_cntr;
      
      PIPE ROW(rec_out);
    END LOOP;
    RETURN;
  END cashflow_smry_report;

END CFD_CASH_REPORTS_V5_PCK;
/
show errors;
spool off;
