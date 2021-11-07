begin   -- 3.5 минуты
    --load_rss.load_deals( date'2021-08-01', 1);
    load_rss.load_deals_by_period( date'2021-08-01');
    load_rss.load_demands_by_period( date'2021-08-01');
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
update dtxdemand_dbt set t_replstate=0 where  t_demandid>=20000000000 and t_instancedate< date'2021-08-02';
truncate table ddlrq_dbt;
truncate table dtxloadlog_dbt;

--------------------------------------------

select rowid, a.* from dtx_query_dbt a order by t_objecttype, t_set, t_num


select count(*) from ddl_tick_dbt

select * from dtxloadlog_dbt

select * from dtxdemand_dbt where t_instancedate = date'2021-08-01'

select * from dtxdemand_tmp where t_instancedate = date'2021-08-01'

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