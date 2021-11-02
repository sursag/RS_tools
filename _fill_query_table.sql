--SQL Statement which produced this data:
--
--  select rowid, a.* from DTX_QUERY_DBT a;
--
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (33, 80, 3, 40, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_MARKETID is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID)'||CHR(10)||'when matched then update set tgt.TGT_MARKETID=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_MARKETID из DTXREPLOBJ_DBT', 'Добавление TGT_MARKETID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (34, 80, 3, 45, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_BROKERID is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_BROKERID)'||CHR(10)||'when matched then update set tgt.TGT_BROKERID=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_BROKERID из DTXREPLOBJ_DBT', 'Добавление TGT_BROKERID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (35, 80, 3, 50, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_SECTOR is not null) tgt '||CHR(10)||'using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=40 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID  and  sou.T_SUBOBJNUM=tgt.T_SECTOR)'||CHR(10)||'when matched then update set tgt.TGT_SECTOR=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_SECTOR из DTXREPLOBJ_DBT', 'Добавление TGT_SECTOR', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (36, 80, 3, 55, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_NKDFIID is not null) tgt '||CHR(13)||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_NKDFIID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_NKDFIID=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_NKDFIID из DTXREPLOBJ_DBT', 'Добавление TGT_NKDFIID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (37, 80, 3, 60, 'update dtxdeal_tmp set TGT_REPOBASE = CASE t_repobase when 1 then 1  when 2 then 3  when 3 then 5  when 4 then 2   when 5 then 0   when 6 then 4   else -1  end'||CHR(10)||' where t_replstate=0 and T_REPOBASE is not null', 
    1, 'Обогащение записи - добавление TGT_REPOBASE', 'Добавление TGT_REPOBASE', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (38, 80, 3, 65, 'update dtxdeal_tmp set TGT_COUNTRY = (select t_codelat3 from dcountry_dbt where t_countryid = T_COUNTRY)'||CHR(10)||' where t_replstate=0 and T_COUNTRY <> 165', 
    1, 'Обогащение записи - добавление TGT_COUNTRY', 'Добавление TGT_COUNTRY', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (39, 80, 3, 26, 'update dtxdeal_tmp set tgt_avoirissid = 2192, t_isbasket=chr(88) where t_replstate=0 and t_avoirissid=-20', 
    1, 'Обогащение записи - добавление TGT_AVOIRISSID для корзины', 'Добавление TGT_AVOIRISSID (2192) для корзины', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (40, 80, 3, 500, 'update dtxdeal_tmp set TGT_DEALID = ddl_tick_dbt_seq.nextval where t_replstate=0', 
    1, 'Обогащение записи - добавление TGT_DEALID из последовательности для вставок', 'Добавление TGT_DEALID для вставок', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (41, 80, 1, 10, 'update dtxdeal_tmp set T_EXTCODE = trim(T_EXTCODE), T_MARKETCODE = trim(T_MARKETCODE), T_PARTYCODE = trim(T_PARTYCODE), T_CODE = trim(T_CODE)', 
    1, 'Форматирование записи - TRIM текстовых полей', 'TRIM текстовых полей', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (42, 80, 3, 70, 'update dtxdeal_tmp set TGT_EXISTBACK = chr(88) where t_replstate=0 and T_KIND in (30,40)', 
    1, 'Обогащение записи - добавление TGT_EXISTBACK', 'Добавление TGT_EXISTBACK', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (43, 80, 3, 75, 'update dtxdeal_tmp set tgt_objtype = case when T_KIND in (70,80,90) then 117 else 101 end where t_replstate=0', 
    1, 'Обогащение записи - добавление TGT_OBJTYPE', 'Добавление TGT_OBJTYPE', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (44, 80, 1, 15, 'update dtxdeal_tmp set T_AMOUNT=nvl(T_AMOUNT, 0), T_PRICE=nvl(T_PRICE, 0), T_COST=nvl(T_COST, 0), T_NKD=nvl(T_NKD, 0), T_TOTALCOST=nvl(T_TOTALCOST, 0), T_RATE=nvl(T_RATE, 0), T_REPOBASE=nvl(T_REPOBASE, 0),'||CHR(10)||' T_ISPFI_1=nvl(T_ISPFI_1, 0), T_ISPFI_2=nvl(T_ISPFI_2, 0), T_LIMIT=nvl(T_LIMIT, 0), T_CHRATE=nvl(T_CHRATE, 0), T_COUNTRY=nvl(T_COUNTRY, 165), T_PRICE2=nvl(T_PRICE2, 0), T_COST2=nvl(T_COST2, 0), T_NKD2=nvl(T_NKD2, 0),'||CHR(10)||' T_TOTALCOST2=nvl(T_TOTALCOST2, 0), T_DOPCONTROL=nvl(T_DOPCONTROL, 0), T_FISSKIND=nvl(T_FISSKIND, 0), T_PRICE_CALC=nvl(T_PRICE_CALC, 0), T_PRICE_CALC_DEF=nvl(T_PRICE_CALC_DEF, 0), T_PRICE_CALC_METHOD=nvl(T_PRICE_CALC_METHOD, 0),'||CHR(10)||' T_PRICE_CALC_DEF=nvl(T_PRICE_CALC_DEF, 0), T_PRICE_CALC_VAL=nvl(T_PRICE_CALC_VAL, -1)', 
    1, 'Форматирование записи - NVL числовых полей', 'NVL числовых полей', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (45, 80, 1, 15, 'update dtxdeal_tmp set T_AMOUNT=nvl(T_AMOUNT, 0), T_PRICE=nvl(T_PRICE, 0), T_COST=nvl(T_COST, 0), T_NKD=nvl(T_NKD, 0), T_TOTALCOST=nvl(T_TOTALCOST, 0), T_RATE=nvl(T_RATE, 0), T_REPOBASE=nvl(T_REPOBASE, 0),'||CHR(10)||' T_ISPFI_1=nvl(T_ISPFI_1, 0), T_ISPFI_2=nvl(T_ISPFI_2, 0), T_LIMIT=nvl(T_LIMIT, 0), T_CHRATE=nvl(T_CHRATE, 0), T_COUNTRY=nvl(T_COUNTRY, 165), T_PRICE2=nvl(T_PRICE2, 0), T_COST2=nvl(T_COST2, 0), T_NKD2=nvl(T_NKD2, 0),'||CHR(10)||' T_TOTALCOST2=nvl(T_TOTALCOST2, 0), T_DOPCONTROL=nvl(T_DOPCONTROL, 0), T_FISSKIND=nvl(T_FISSKIND, 0), T_PRICE_CALC=nvl(T_PRICE_CALC, 0), T_PRICE_CALC_DEF=nvl(T_PRICE_CALC_DEF, 0), T_PRICE_CALC_METHOD=nvl(T_PRICE_CALC_METHOD, 0),'||CHR(10)||' T_PRICE_CALC_VAL=nvl(T_PRICE_CALC_VAL, -1), T_ADJUSTMENT=nvl(T_ADJUSTMENT, chr(0)), T_ATANYDAY=nvl(T_ATANYDAY, chr(0)), T_DIV=nvl(T_DIV, chr(0)), T_COSTCHANGEONAMOR=nvl(T_COSTCHANGEONAMOR, chr(0)),'||CHR(10)||' T_COSTCHANGEONCOMP=nvl(T_COSTCHANGEONCOMP, chr(0)), T_COSTCHANGE=nvl(T_COSTCHANGE, chr(0)) ', 
    1, 'Форматирование записи - NVL полей', 'Обработка полей с помощью NVL', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (46, 80, 3, 80, 'update /*+ # */ dtxdeal_tmp set tgt_department = nvl(( select t_code from dtxreplobj_dbt ro join ddp_dep_dbt dp on (ro.t_destid=dp.t_partyid and ro.t_objecttype=40 and ro.t_objstate=0)),1)', 
    1, 'Обогащение записи - добавление TGT_DEPARTMENT', 'Добавление TGT_DEPARTMENT', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (47, 80, 3, 85, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_WARRANTID is not null) tgt '||CHR(10)||'using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=50 and t_objstate=0) sou on (sou.t_objectid=tgt.T_WARRANTID)'||CHR(10)||'when matched then update set tgt.TGT_WARRANTID=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_WARRANTID из DTXREPLOBJ_DBT', 'Добавление TGT_WARRANTID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (48, 80, 3, 90, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARTIALID is not null) tgt '||CHR(10)||'using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=50 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTIALID)'||CHR(10)||'when matched then update set tgt.TGT_WARRANTID=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_PARTIALID из DTXREPLOBJ_DBT', 'Добавление TGT_PARTIALID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (26, 80, 3, 10, 'merge /*+ # */ into dtxdeal_tmp tgt using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.t_dealid)'||CHR(13)||CHR(10)||'when matched then update set tgt.tgt_dealid=sou.t_destid', 
    1, 'Обогащение записи - добавление TGT_DEALID из DTXREPLOBJ_DBT', 'Добавление TGT_DEALID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (28, 80, 3, 20, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PAYMCUR is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PAYMCUR)'||CHR(10)||'when matched then update set tgt.TGT_PAYMCUR=sou.t_destid', 
    1, 'Обогащение записи - добавление TGT_PAYMCUR из DTXREPLOBJ_DBT', 'Добавление TGT_PAYMCUR', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (27, 80, 3, 15, 'update /*+ # */ dtxdeal_tmp tgt set TGT_BOFFICEKIND =  '||CHR(13)||CHR(10)||'                                case T_KIND'||CHR(13)||CHR(10)||'                                when 30 then 101'||CHR(13)||CHR(10)||'                                when 40 then 101'||CHR(13)||CHR(10)||'                                when 70 then 117'||CHR(13)||CHR(10)||'                                when 80 then 117'||CHR(13)||CHR(10)||'                                when 90 then 117'||CHR(13)||CHR(10)||'                                when 100 then 158'||CHR(13)||CHR(10)||'                                when 110 then 140'||CHR(13)||CHR(10)||'                                else 101'||CHR(13)||CHR(10)||'                                end', 
    1, 'Обогащение записи - добавление TGT_BOFFICEKIND', 'Добавление TGT_BOFFICEKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (29, 80, 3, 20, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARENTID is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARENTID)'||CHR(10)||'when matched then update set tgt.TGT_PARENTID=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_PARENTID из DTXREPLOBJ_DBT', 'Добавление TGT_PARENTID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (30, 80, 3, 25, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=20 and t_objstate=0) sou on (sou.t_objectid=tgt.T_AVOIRISSID)'||CHR(10)||'when matched then update set tgt.TGT_AVOIRISSID=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_AVOIRISSID из DTXREPLOBJ_DBT', 'Добавление TGT_AVOIRISSID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (31, 80, 3, 30, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTYID)'||CHR(10)||'when matched then update set tgt.TGT_PARTYID=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_PARTYID из DTXREPLOBJ_DBT', 'Добавление TGT_PARTYID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_IN_USE)
 Values
   (32, 80, 3, 35, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTYID)'||CHR(10)||'when matched then update set tgt.TGT_CURRENCYID=sou.T_DESTID', 
    1, 'Обогащение записи - добавление TGT_CURRENCYID из DTXREPLOBJ_DBT', 'Добавление TGT_CURRENCYID', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (1, 80, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, dtxdeal_tmp.t_dealid, '||CHR(13)||CHR(10)||'case    when ro.t_objstate=2 then 423 -- сделка уже удалена в целевой системе'||CHR(13)||CHR(10)||'    when tick.t_dealid is null then 422 -- нет тикета сделки'||CHR(13)||CHR(10)||'    when leg.t_dealid is null then 422 -- нет ценовых условий сделки'||CHR(13)||CHR(10)||'    when ro.t_destid is null  then 422 -- сделка с таким dealid не реплицировалась'||CHR(13)||CHR(10)||'    when ro.t_objstate=1 then 206 -- сделка в ручном режиме'||CHR(13)||CHR(10)||'end T_ERRORCODE '||CHR(13)||CHR(10)||'from dtxdeal_tmp left join dtxreplobj_dbt ro  on (ro.t_objecttype=80 and dtxdeal_tmp.t_dealid=ro.t_objectid)'||CHR(13)||CHR(10)||'left join ddl_tick_dbt tick on (ro.t_destid = tick.t_dealid) '||CHR(13)||CHR(10)||'left join ddl_leg_dbt leg on (ro.t_destid = leg.t_dealid and leg.t_legkind=0)'||CHR(13)||CHR(10)||'where dtxdeal_tmp.t_action in (2,3) and (ro.t_destid is null or tick.t_dealid is null or leg.t_dealid is null)'||CHR(13)||CHR(10)||'', 
    1, 'Запрос выполняется только для операций обновления/удаления. Проверяет, есть ли в целевой системе реплицированные сделки с данным ID. Обрабатывает сразу несколько событий - отсутствие сделки в тикетах, в leg, отсутствие в replobj и ручной режим обработки. Может, потом разобью на несколько запросов', 'Проверка наличия сделки в RS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_USE_BIND)
 Values
   (2, 80, 2, 15, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  d.t_dealid, 421 from dtxdeal_dbt dp, dtxdeal_tmp d where d.t_code=dp.t_code and d.t_dealid > dp.t_dealid'||CHR(13)||CHR(10)||'', 
    1, 'Запрос выполняется только для операций вставки. Проверяет дубли сделок по T_DEALCODE.', 'Проверка дублей по DEALCODE', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (3, 80, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, d.t_dealid, 206 from dtxdeal_tmp d join dtxreplobj_dbt ro on (ro.t_objecttype=80 and ro.t_objectid=d.t_dealid)'||CHR(13)||CHR(10)||'', 
    1, 'Объект находится в режиме ручного редактирования.', 'Проверка на ручное редактирование', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (4, 80, 2, 25, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, t_dealid, 539 from dtxdeal_tmp where t_code is null'||CHR(13)||CHR(10)||'', 
    1, 'Не задан параметр T_CODE - код сделки', 'Проверка заполнения T_CODE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (5, 80, 2, 25, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  t_dealid, 568 from dtxdeal_tmp where t_kind is null'||CHR(13)||CHR(10)||'', 
    1, 'Не задан параметр T_KIND - код сделки', 'Проверка заполения T_KIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (6, 80, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, t_dealid, 568 from dtxdeal_tmp where t_amount is null'||CHR(13)||CHR(10)||'', 
    1, 'Не задан параметр T_AMOUNT - количество ценных бумаг', 'Проверка заполнения T_AMOUNT', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (7, 80, 2, 35, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  t_dealid, 556 from dtxdeal_tmp where (T_PRICE + T_COST + T_TOTALCOST) = 0 and T_KIND not in (70,80,90,100,110)'||CHR(13)||CHR(10)||'', 
    1, 'Не задан параметр T_PRICE - цена за шт. ценной бумаги, не включая НКД', 'Проверка заполнения T_PRICE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (8, 80, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  t_dealid, 557 from dtxdeal_tmp where (T_PRICE + T_TOTALCOST) = 0 and T_COST > 0'||CHR(13)||CHR(10)||'', 
    1, 'Не задан параметр T_COST - стоимость ценных бумаг без НКД', 'Проверка заполнения T_COST', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_SCREENID, T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, 
    T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (9, 80, 2, 45, 'insert /*+ append */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, t_dealid, 558 from dtxdeal_tmp where (T_PRICE + T_COST) = 0 and T_TOTALCOST > 0'||CHR(13)||CHR(10)||'', 
    1, 'Не задан параметр T_TOTALCOST - общая сумма сделки вкл. НКД в валюте сделки', 'Проверка заполнения T_TOTALCOST', 'X', 'X');
COMMIT;
