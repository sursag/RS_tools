select * from dtxdeal_dbt where t_extcode='10000002228'


-- проверка репликации сделки
declare
    l_dealcode varchar2(100) := '10000002228';
    l_date     date    := date'2021-06-30';
begin

    delete from ddl_leg_dbt where t_dealid in (select t_dealid from ddl_tick_dbt where t_dealcode = l_dealcode);
    delete from ddl_tick_dbt where t_dealcode = l_dealcode;
    delete from dtxreplobj_dbt where t_objecttype=80 and t_objectid in (select t_dealid from dtxdeal_dbt where t_code = l_dealcode);
    update dtxdeal_dbt set t_replstate=0 where t_action = 1 and t_dealid=l_dealcode and t_instancedate between l_date and l_date+1;
    delete from dtxloadlog_dbt;
    commit;

    load_rss.replobj_rec_arr.delete;
    load_rss.replobj_rec_inx_arr.delete;
    load_rss.note_arr.delete;
    load_rss.load_deals( l_date, 1);
    commit;
end;

select * from ddl_tick_dbt
select * from ddl_leg_dbt
select * from ddlrq_dbt
select * from dtxdeal_dbt where t_dealid='10000002228'
select * from dtxreplobj_dbt where t_objecttype in (80,90)


-- проверка репликации изменения сделки
declare
    l_dealcode varchar2(100) := '10000002228';
    l_date  date    := date'2021-07-01';
begin
    update dtxdeal_dbt set t_replstate=0 where t_action = 2 and t_dealid=l_dealcode and t_instancedate between l_date and l_date+1;
    delete from dtxloadlog_dbt;
    commit;

    load_rss.replobj_rec_arr.delete;
    load_rss.replobj_rec_inx_arr.delete;
    load_rss.note_arr.delete;
    load_rss.load_deals( l_date, 2);
    commit;
end;


select * from ddl_tick_dbt
select * from ddl_leg_dbt
select * from ddlrq_dbt
select * from dtxdeal_dbt where t_dealid='10000002228'
select * from dtxreplobj_dbt where t_objecttype in (80,90)



-- проверка репликации УДАЛЕНИЯ сделки
declare
    l_dealcode varchar2(100) := '10000002228';
    l_date  date    := date'2021-07-02';
begin
    update dtxdeal_dbt set t_replstate=0 where t_action = 3 and t_dealid=l_dealcode and t_instancedate between l_date and l_date+1;
    delete from dtxloadlog_dbt;
    commit;

    load_rss.replobj_rec_arr.delete;
    load_rss.replobj_rec_inx_arr.delete;
    load_rss.note_arr.delete;
    load_rss.load_deals( l_date, 3);
    commit;
end;


select * from ddl_tick_dbt
select * from ddl_leg_dbt
select * from ddlrq_dbt
select * from dtxdeal_dbt where t_dealid='10000002228'
select * from dtxreplobj_dbt where t_objecttype in (80,90)
-- после удаления поправить replobj

--============================================================================
--============================================================================
--============================================================================
--============================================================================
-- ПЛАТЕЖИ --

select * from dtxdemand_dbt where t_dealid=10000002228

insert into dtxdemand_dbt 
select T_DEMANDID, date'2021-07-01', 2, 0, T_DEALID, T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, T_BALANCERATE, T_NETTING, T_PLANDEMEND, T_NOTE, T_STATE, T_VERSION
from dtxdemand_dbt where t_dealid=10000002228 and t_action=1

insert into dtxdemand_dbt 
select T_DEMANDID, date'2021-07-02', 3, 0, T_DEALID, T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, T_BALANCERATE, T_NETTING, T_PLANDEMEND, T_NOTE, T_STATE, T_VERSION
from dtxdemand_dbt where t_dealid=10000002228 and t_action=1


-- проверка репликации единичного платежа
declare
    l_dealcode varchar2(100) := '10000002228';
    l_date  date    := date'2021-06-30';
begin
    update dtxdemand_dbt set t_replstate=0 where t_action = 1 and t_dealid=l_dealcode and t_instancedate between l_date and l_date+1;
    delete from ddlrq_dbt where t_docid = (select t_dealid from ddl_tick_dbt where t_dealcode=l_dealcode);
    delete from dtxreplobj_dbt where t_objecttype=90 and t_objectid in (select t_demandid from dtxdemand_dbt where t_dealid=l_dealcode);
    delete from dtxloadlog_dbt;
    commit;

    load_rss.replobj_rec_arr.delete;
    load_rss.replobj_rec_inx_arr.delete;
    load_rss.note_arr.delete;
    load_rss.load_demands( l_date, 1);
    commit;
end;    


select * from ddlrq_dbt
select * from dtxdemand_dbt where t_dealid=10000002228
select * from dtxreplobj_dbt where t_objecttype in (80,90)



-- проверка репликации ИЗМЕНЕНИЯ платежа
declare
    l_dealcode varchar2(100) := '10000002228';
    l_date  date    := date'2021-07-01';
begin
    update dtxdemand_dbt set t_replstate=0 where t_action = 2 and t_dealid=l_dealcode and t_instancedate between l_date and l_date+1;
    delete from dtxloadlog_dbt;
    commit;

    load_rss.replobj_rec_arr.delete;
    load_rss.replobj_rec_inx_arr.delete;
    load_rss.note_arr.delete;
    load_rss.load_demands( l_date, 2);
    commit;
end;    
    
select * from ddlrq_dbt
select rowid, a.* from dtxdemand_dbt a where t_dealid=10000002228
select * from dtxreplobj_dbt where t_objecttype in (80,90)


-- проверка репликации УДАЛЕНИЯ платежа
declare
    l_dealcode varchar2(100) := '10000002228';
    l_date  date    := date'2021-07-02';
begin
    update dtxdemand_dbt set t_replstate=0 where t_dealid=l_dealcode and t_instancedate between l_date and l_date+1;
    delete from dtxloadlog_dbt;
    commit;

    load_rss.replobj_rec_arr.delete;
    load_rss.replobj_rec_inx_arr.delete;
    load_rss.note_arr.delete;
    load_rss.load_demands( l_date, 3);
    commit;
end;        

select * from ddlrq_dbt
select * from dtxdemand_dbt where t_dealid=10000002228
select * from dtxreplobj_dbt where t_objecttype in (80,90)


---------------------
ddlrq_dbt

select rowid, a.* from dtxdemand_dbt a where t_dealid = '10000002228'

select rowid, a.* from dtxdeal_dbt a where t_dealid = '10000002228'

select * from ddl_tick_dbt where t_dealcode = '10000002228'

select * from ddl_leg_dbt where t_dealid in (select t_dealid from ddl_tick_dbt where t_dealcode = '10000002228')

select * from ddlrq_dbt where t_docid in (select t_dealid from ddl_tick_dbt where t_dealcode = '10000002228')

delete from ddlrq_dbt where t_docid in (select t_dealid from ddl_tick_dbt where t_dealcode = '10000002228')

select * from dtxreplobj_dbt where t_objecttype=80 and t_objectid='10000002228'

select * from dtxreplobj_dbt where t_objecttype=90

---------------------

insert into dtxdeal_dbt(T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, T_MARKETID, T_SECTOR, T_BROKERID, T_PARTYID, T_DEPARTMENT, T_AVOIRISSID, T_WARRANTID, T_PARTIALID, T_AMOUNT, T_CURRENCYID, T_PRICE, T_POINT, T_COST, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_VERSION, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID, T_NETTING_DEALID_DEST)
select T_DEALID, date'2021-07-01', 2, T_REPLSTATE, T_KIND, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, T_MARKETID, T_SECTOR, T_BROKERID, T_PARTYID, T_DEPARTMENT, T_AVOIRISSID, T_WARRANTID, T_PARTIALID, T_AMOUNT, T_CURRENCYID, T_PRICE, T_POINT, T_COST, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_VERSION, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID, T_NETTING_DEALID_DEST 
from dtxdeal_dbt a where t_dealid = '10000002228'

select * from dtxreplobj_dbt where t_objecttype=30 and t_destid=4

select * from dtxreplobj_dbt where t_objecttype=40

select * from dtxreplobj_dbt where t_objecttype=30 and t_objectid=

select * from dptoffice_dbt

insert into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM)
values(40, 10000178951 ,10000000000, 110, 1)


select * from dtxmarksect_dbt

delete from dtxreplobj_dbt where t_objecttype=80

select * from dtxreplobj_dbt where t_objecttype=80

select * from dtxloadlog_dbt

delete from dtxloadlog_dbt

select * from dcountry_dbt where t_countryid=165

select rowid, a.* from dtxdeal_dbt a

select * from ddlrq_dbt

delete from ddlrq_dbt


update dtxdemand_dbt set t_replstate=0 where t_dealid='10000002228'

select * from ddlrq_dbt where t_docid in '10000002228'

update dtxdemand_dbt set t_replstate=2 

select * from dtxdemand_dbt



SET DEFINE OFF;
Insert into GEB_20210823.DTXDEAL_DBT
   (T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, 
    T_EXTCODE, T_MARKETCODE, T_CODE, T_DATE, T_TIME, 
    T_CLOSEDATE, T_TSKIND, T_MARKETID, T_SECTOR, T_BROKERID, 
    T_PARTYID, T_AVOIRISSID, T_AMOUNT, T_CURRENCYID, T_PRICE, 
    T_POINT, T_COST, T_NKD, T_TOTALCOST, T_PAYDATE, 
    T_SUPLDATE, T_CONTRNUM, T_CONTRDATE, T_PAYMCUR, T_COUNTRY, 
    T_BALANCEDATE, T_VERSION, T_PORTFOLIOID)
 Values
   (10000002228, TO_DATE('30/06/2021', 'DD/MM/YYYY'), 1, 1, 20, 
    '10000002228', '3844482538', '10000002228', TO_DATE('20/04/2021', 'DD/MM/YYYY'), TO_DATE('01/09/2021 17:01:54', 'DD/MM/YYYY HH24:MI:SS'), 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), 'P', 10000178951, 10000000000, 2000, 
    10000178952, 10000000218, 719, 2, 105.6, 
    4, 759264, 1092.88, 760356.88, TO_DATE('21/04/2021', 'DD/MM/YYYY'), 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), '131313', TO_DATE('13/09/2013', 'DD/MM/YYYY'), 2, 165, 
    TO_DATE('20/04/2021', 'DD/MM/YYYY'), 206, '2');
Insert into GEB_20210823.DTXDEAL_DBT
   (T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, 
    T_EXTCODE, T_MARKETCODE, T_CODE, T_DATE, T_TIME, 
    T_CLOSEDATE, T_TSKIND, T_MARKETID, T_SECTOR, T_BROKERID, 
    T_PARTYID, T_AVOIRISSID, T_AMOUNT, T_CURRENCYID, T_PRICE, 
    T_POINT, T_COST, T_NKD, T_TOTALCOST, T_PAYDATE, 
    T_SUPLDATE, T_CONTRNUM, T_CONTRDATE, T_PAYMCUR, T_COUNTRY, 
    T_BALANCEDATE, T_VERSION, T_PORTFOLIOID)
 Values
   (10000002228, TO_DATE('01/07/2021', 'DD/MM/YYYY'), 2, 0, 20, 
    '10000002228', '3844482538', '10000002228', TO_DATE('20/04/2021', 'DD/MM/YYYY'), TO_DATE('01/09/2021 17:01:54', 'DD/MM/YYYY HH24:MI:SS'), 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), 'P', 10000178951, 10000000000, 2000, 
    10000178952, 10000000218, 719, 2, 106.6, 
    4, 759264, 1077.77, 760777.77, TO_DATE('21/04/2021', 'DD/MM/YYYY'), 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), '131313', TO_DATE('13/09/2013', 'DD/MM/YYYY'), 2, 165, 
    TO_DATE('20/04/2021', 'DD/MM/YYYY'), 109, '2');
Insert into GEB_20210823.DTXDEAL_DBT
   (T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, 
    T_EXTCODE, T_MARKETCODE, T_CODE, T_DATE, T_TIME, 
    T_CLOSEDATE, T_TSKIND, T_MARKETID, T_SECTOR, T_BROKERID, 
    T_PARTYID, T_AVOIRISSID, T_AMOUNT, T_CURRENCYID, T_PRICE, 
    T_POINT, T_COST, T_NKD, T_TOTALCOST, T_PAYDATE, 
    T_SUPLDATE, T_CONTRNUM, T_CONTRDATE, T_PAYMCUR, T_COUNTRY, 
    T_BALANCEDATE, T_VERSION, T_PORTFOLIOID)
 Values
   (10000002228, TO_DATE('02/07/2021', 'DD/MM/YYYY'), 3, 1, 20, 
    '10000002228', '3844482538', '10000002228', TO_DATE('20/04/2021', 'DD/MM/YYYY'), TO_DATE('01/09/2021 17:01:54', 'DD/MM/YYYY HH24:MI:SS'), 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), 'P', 10000178951, 10000000000, 2000, 
    10000178952, 10000000218, 719, 2, 106.6, 
    4, 759264, 1077.77, 760777.77, TO_DATE('21/04/2021', 'DD/MM/YYYY'), 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), '131313', TO_DATE('13/09/2013', 'DD/MM/YYYY'), 2, 165, 
    TO_DATE('20/04/2021', 'DD/MM/YYYY'), 4, '2');
COMMIT;



Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, 
    T_DATE, T_SUM, T_BALANCERATE, T_PLANDEMEND, T_STATE, 
    T_VERSION)
 Values
   (10000009645, TO_DATE('30/06/2021', 'DD/MM/YYYY'), 1, 1, 10000002228, 
    1, 'X', 10, 1, 20, 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), 719, 1, 10000008152, 3, 
    80);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, 
    T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, 
    T_BALANCERATE, T_PLANDEMEND, T_STATE, T_VERSION)
 Values
   (10000009151, TO_DATE('30/06/2021', 'DD/MM/YYYY'), 1, 1, 10000002228, 
    1, 'X', 40, 2, 10, 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), 760356.88, 2, 760356.88, 1, 
    1, 10000007707, 3, 78);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, 
    T_SUM, T_STATE, T_VERSION)
 Values
   (10000008152, TO_DATE('30/06/2021', 'DD/MM/YYYY'), 1, 1, 10000002228, 
    1, 10, 1, 20, TO_DATE('21/04/2021', 'DD/MM/YYYY'), 
    719, 1, 74);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, 
    T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_STATE, T_VERSION)
 Values
   (10000007707, TO_DATE('30/06/2021', 'DD/MM/YYYY'), 1, 1, 10000002228, 
    1, 40, 2, 10, TO_DATE('21/04/2021', 'DD/MM/YYYY'), 
    760356.88, 2, 760356.88, 1, 76);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, 
    T_DATE, T_SUM, T_BALANCERATE, T_PLANDEMEND, T_STATE, 
    T_VERSION)
 Values
   (10000009645, TO_DATE('01/07/2021', 'DD/MM/YYYY'), 2, 1, 10000002228, 
    1, 'X', 10, 1, 20, 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), 719, 1, 10000008152, 3, 
    43);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, 
    T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, 
    T_BALANCERATE, T_PLANDEMEND, T_STATE, T_VERSION)
 Values
   (10000009151, TO_DATE('01/07/2021', 'DD/MM/YYYY'), 2, 1, 10000002228, 
    1, 'X', 40, 2, 10, 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), 700000.88, 2, 760356.88, 1, 
    1, 10000007707, 3, 42);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, 
    T_SUM, T_STATE, T_VERSION)
 Values
   (10000008152, TO_DATE('01/07/2021', 'DD/MM/YYYY'), 2, 1, 10000002228, 
    1, 10, 1, 20, TO_DATE('21/04/2021', 'DD/MM/YYYY'), 
    719, 1, 40);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, 
    T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_STATE, T_VERSION)
 Values
   (10000007707, TO_DATE('01/07/2021', 'DD/MM/YYYY'), 2, 1, 10000002228, 
    1, 40, 2, 10, TO_DATE('21/04/2021', 'DD/MM/YYYY'), 
    711111, 2, 760356.88, 1, 43);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, 
    T_DATE, T_SUM, T_BALANCERATE, T_PLANDEMEND, T_STATE, 
    T_VERSION)
 Values
   (10000009645, TO_DATE('02/07/2021', 'DD/MM/YYYY'), 3, 1, 10000002228, 
    1, 'X', 10, 1, 20, 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), 719, 1, 10000008152, 3, 
    27);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, 
    T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, 
    T_BALANCERATE, T_PLANDEMEND, T_STATE, T_VERSION)
 Values
   (10000009151, TO_DATE('02/07/2021', 'DD/MM/YYYY'), 3, 1, 10000002228, 
    1, 'X', 40, 2, 10, 
    TO_DATE('21/04/2021', 'DD/MM/YYYY'), 760356.88, 2, 760356.88, 1, 
    1, 10000007707, 3, 25);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, 
    T_SUM, T_STATE, T_VERSION)
 Values
   (10000008152, TO_DATE('02/07/2021', 'DD/MM/YYYY'), 3, 1, 10000002228, 
    1, 10, 1, 20, TO_DATE('21/04/2021', 'DD/MM/YYYY'), 
    719, 1, 24);
Insert into GEB_20210823.DTXDEMAND_DBT
   (T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, 
    T_PART, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, 
    T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_STATE, T_VERSION)
 Values
   (10000007707, TO_DATE('02/07/2021', 'DD/MM/YYYY'), 3, 1, 10000002228, 
    1, 40, 2, 10, TO_DATE('21/04/2021', 'DD/MM/YYYY'), 
    760356.88, 2, 760356.88, 1, 26);
COMMIT;
