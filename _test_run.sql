begin   -- 3.5 минуты
    --load_rss.load_deals( date'2021-08-01', 1);
    --load_rss.load_deals_by_period( date'2021-08-02');
    load_rss.load_demands_by_period( date'2021-08-01');
    load_rss.load_demands_by_period( date'2021-08-02');
end;

begin  -- 7 минут
    load_rss.load_demands( date'2021-08-01', 1);
end;

begin   
    load_rss.load_deals( date'2021-08-01', 1);
    load_rss.load_demands( date'2021-08-01', 1);
end;


---------------------------------------------
-- Очистка для перезагрузки сделок

delete ddl_tick_bad;
truncate table ddl_tick_dbt;
truncate table ddl_leg_dbt;
delete from dtxreplobj_dbt where t_objecttype in (80) and t_objectid>=20000000000;
update dtxdeal_dbt set t_replstate=0 where t_replstate=1 and t_dealid>=20000000000;
truncate table  dnotetext_dbt;
truncate table  dobjatcor_dbt;
truncate table DSPGRDOC_DBT;
truncate table dtxloadlog_dbt;
truncate table DSPGROUND_DBT;

---------------------------------------------
-- Очистка для перезагрузки платежей

delete from dtxreplobj_dbt where t_objecttype in (90) and t_objectid>=20000000000;
update dtxdemand_dbt set t_replstate=0 where  t_demandid>=20000000000 and t_instancedate< date'2021-08-03';
truncate table ddlrq_dbt;
truncate table dtxloadlog_dbt;




select /*+ parallel(4) */ t_replstate, count(*) from dtxdemand_dbt where t_instancedate= date'2021-08-02' group by t_replstate

select t_replstate, t_action, count(*) from dtxdemand_dbt where t_instancedate= date'2021-08-02' group by t_replstate, t_action

select * from dtxdemand_tmp 

select * from dtxreplobj_dbt where t_objectid=20000000262

update dtxdemand_dbt set t_replstate=0 where t_instancedate= date'2021-08-02' and t_action=2




truncate table ddlrq_dbt;
truncate table dtxloadlog_dbt;

--------------------------------------------

select rowid, a.* from dtx_query_dbt a order by t_objecttype, t_set, t_num



select count(*) from dtxdemand_tmp

select count(*) from dtxdemand_tmp where t_replstate=0 and t_action=1

select * from dtxdemand_dt where t_ 

select * from ddlrq_dbt

select count(*) from ddlrq_dbt

select count(*) from ddl_tick_dbt

select * from dtxloadlog_dbt



insert into dtxdemand_dbt(T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, T_BALANCERATE, T_NETTING, T_PLANDEMEND, T_NOTE, T_STATE)
select T_DEMANDID, T_INSTANCEDATE+1, 2, 0, T_DEALID, T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, T_SUM+1000000, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, T_BALANCERATE, T_NETTING, T_PLANDEMEND, T_NOTE, T_STATE 
from dtxdemand_dbt 
where t_instancedate = date'2021-08-01' and rownum<=10

insert into dtxdeal_dbt(T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, T_MARKETID, T_SECTOR, T_BROKERID, T_PARTYID, T_DEPARTMENT, T_AVOIRISSID, T_WARRANTID, T_PARTIALID, T_AMOUNT, T_CURRENCYID, T_PRICE, T_POINT, T_COST, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_VERSION, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID)
select T_DEALID, T_INSTANCEDATE+1, 2, 0, T_KIND, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, T_MARKETID, T_SECTOR, T_BROKERID, T_PARTYID, T_DEPARTMENT, T_AVOIRISSID, T_WARRANTID, T_PARTIALID, 10000+T_AMOUNT, T_CURRENCYID, T_PRICE, T_POINT, T_COST, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_VERSION, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID
from dtxdeal_dbt 
where t_instancedate = date'2021-08-01' and rownum<=10

insert into dtxdeal_dbt(T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, T_MARKETID, T_SECTOR, T_BROKERID, T_PARTYID, T_DEPARTMENT, T_AVOIRISSID, T_WARRANTID, T_PARTIALID, T_AMOUNT, T_CURRENCYID, T_PRICE, T_POINT, T_COST, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_VERSION, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID)
select T_DEALID, T_INSTANCEDATE+1, 3, 0, 0, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, 0, 0, null, null, T_DEPARTMENT, null, T_WARRANTID, T_PARTIALID, 10000+T_AMOUNT, T_CURRENCYID, null, T_POINT, null, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_VERSION, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID
from dtxdeal_dbt 
where t_instancedate = date'2021-08-01' and rownum<=10 and t_dealid >=20000000100



select * from dtxdemand_tmp where t_instancedate = date'2021-08-02' and t_action=2

select * from dtxdeal_dbt where t_instancedate = date'2021-08-02' and t_action=3

select * from dtxdeal_tmp where t_instancedate = date'2021-08-02' and t_action=3

select * from dtxdeal_dbt where t_instancedate = date'2021-08-02' and t_replstate=0

select * from dtxreplobj_dbt where t_objecttype=80 order by 2

select * from ddl_leg_dbt where t_dealid = 11299465

update dtxdeal_dbt set t_replstate=0 where t_instancedate = date'2021-08-02' and t_action=2

select * from dtxdeal_dbt where t_instancedate = date'2021-08-02' and t_replstate=0

select count(*) from dtxdeal_dbt where t_replstate=1

select * from dtxdemand_dbt where t_replstate=1

select count(*) from dtxdeal_dbt where t_replstate=1

select t_replstate, count(*) from dtxdeal_dbt where t_instancedate between date'2021-08-01' and date'2021-08-02'-interval '1' minute group by t_replstate

select count(*) from dspground_dbt

--delete from ddl_tick_dbt;

truncate table ddl_tick_dbt;

select count(*) from ddl_tick_bad 

select * from dtxdeal_dbt where t_code = '1010419770' 

select * from ddl_tick_bad where t_dealcode = '465474489'

select * from ddl_tick_dbt where t_dealid= 581838
union
select * from ddl_tick_bad

select * from dtxdeal_dbt where t_code = '465474489'

alter session enable parallel dml

update /*+ parallel(8) */ dtxdemand_dbt set T_PAYCURRENCYID=2 where T_PAYCURRENCYID=1


select * from dtxloadlog_dbt order by 1