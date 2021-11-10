--SQL Statement which produced this data:
--
--  -- запрос для выгрузки таблицы
--  select T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE from 
--  dtx_query_dbt order by t_objecttype, t_set, t_num;
--
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 1, 10, 'update /*+ parallel(4) */ DTXCOURSE_TMP set'||CHR(13)||CHR(10)||'T_TYPE=nvl(T_TYPE,0), T_BASEFIID=nvl(T_BASEFIID,0), T_FIID=nvl(T_FIID,0), T_MARKETID=nvl(T_MARKETID,0), T_MARKETSECTORID=nvl(T_MARKETSECTORID,0), T_POINT=nvl(T_POINT,0), T_SCALE=nvl(T_SCALE,0)    '||CHR(13)||CHR(10)||'', 1, 
    'Обработка записи NVL', 'Обработка записей NVL', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 303 from dtxcourse_tmp where t_type=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задан вид', 'Проверка вида курса', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 304 from dtxcourse_tmp where t_rate=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задано значение', 'Проверка значения курса', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 301 from dtxcourse_tmp where T_MARKETID=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задана торговая площадка', 'Проверка торговой площадки курса', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 305 from dtxcourse_tmp where T_BASEFIID=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задан базовый финансовый инструмент', 'Проверка базового финансового инструмента', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 310 from dtxcourse_tmp where T_BASEFIID=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задан котируемый финансовый инструмент', 'Проверка котируемого финансового инструмента', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 10, 'merge /*+ # */ into dtxcourse_tmp tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID)'||CHR(10)||'when matched then update set tgt.TGT_MARKETID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_MARKETID', 'Добавление TGT_MARKETID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 15, 'update /*+ # */ DTXCOURSE_TMP set TGT_ISNOMINAL=chr(88) where t_type=100     '||CHR(10)||'', 1, 
    'Обогащение записи - добавление TGT_ISNOMINAL', 'Добавление TGT_ISNOMINAL', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 20, 'merge /*+ # */ into dtxcourse_tmp tgt '||CHR(10)||'using (select t_objectid, t_subobjnum, t_destid from dtxreplobj_dbt where t_objecttype=40 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID and sou.t_subobjnum=tgt.T_MARKETSECTORID)'||CHR(10)||'when matched then update set tgt.TGT_SECTORID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_SECTORID', 'Добавление TGT_SECTORID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 30, 'merge /*+ # */ into dtxcourse_tmp tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=20 and t_objstate=0) sou on (sou.t_objectid=tgt.T_BASEFIID)'||CHR(10)||'when matched then update set tgt.TGT_BASEFIID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_BASEFIID', 'Добавление TGT_BASEFIID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 35, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_basefikind=20) tgt '||CHR(13)||CHR(10)||'using (select * from dfininstr_dbt) sou on (sou.t_fiid=tgt.TGT_BASEFIID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_FACEVALUE_FIID=sou.T_FACEVALUEFI', 1, 
    'Обогащение записи - добавление TGT_FACEVALUE_FIID (если базовый инструмент - бумага)', 'Добавление TGT_FACEVALUE_FIID (для бумаг)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 40, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_basefikind=10) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_FIID)'||CHR(10)||'when matched then update set tgt.TGT_FIID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_FIID (деньги)', 'Добавление TGT_FIID (деньги)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 50, 'update dtxcourse_tmp set TGT_FIID=TGT_FACEVALUE_FIID where t_basefikind=20', 1, 
    'Обогащение записи - добавление TGT_FIID (бумаги)', 'Добавление TGT_FIID (бумаги)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 55, 'merge into (select * from dtxcourse_tmp where t_basefikind=20) tgt '||CHR(13)||CHR(10)||'using (select t_fiid, t_root from dfininstr_dbt f join davrkinds_dbt k on (f.T_AVOIRKIND=k.T_AVOIRKIND)) sou '||CHR(13)||CHR(10)||'on (sou.t_fiid=tgt.TGT_BASEFIID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_BASEFIKIND=sou.T_ROOT'||CHR(13)||CHR(10)||'', 1, 
    'Обогащение записи - добавление TGT_BASEFIKIND', 'Добавление TGT_BASEFIKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 60, 'update /*+ # */ DTXCOURSE_TMP set TGT_ISDOMINANT=chr(88) where t_type=6 and t_basefikind=10     '||CHR(10)||'', 1, 
    'Обогащение записи - добавление TGT_ISDOMINANT', 'Добавление TGT_ISDOMINANT', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 70, 'update /*+ # */ DTXCOURSE_TMP set TGT_TYPE=load_rss.getratetype(t_type)     '||CHR(10)||'', 1, 
    'Обогащение записи - добавление TGT_TYPE', 'Добавление TGT_TYPE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 90, 'update /*+ # */ DTXCOURSE_TMP set TGT_ISRELATIVE=chr(88) where TGT_BASEFIKIND=17 and T_TYPE<>15', 1, 
    'Обогащение записи - добавление TGT_ISRELATIVE,если бумага-облигация и вид курса - не НКД на дату', 'Добавление TGT_ISRELATIVE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 100, 'merge /*+ # */ into (select * from dtxcourse_tmp where tgt_isnominal is null) tgt '||CHR(13)||CHR(10)||'using (select * from dratedef_dbt) sou on (sou.T_OTHERFI=TGT_BASEFIID and sou.t_fiid=TGT_FIID and sou.t_type=TGT_TYPE and sou.t_market_place=TGT_MARKETID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_RATEID=sou.T_RATEID, tgt.TGT_LAST_DATE_RSS=sou.T_SINCEDATE', 1, 
    'Обогащение записи - добавление TGT_RATEID / TGT_LAST_DATE_RSS', 'Добавление TGT_RATEID / TGT_LAST_DATE_RSS', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 105, 'merge into /*+ # */ DTXCOURSE_TMP tgt '||CHR(13)||CHR(10)||'using  (select t_basefiid, t_fiid, t_type, t_marketid, T_MARKETSECTORID, max(t_ratedate) mdate from DTXCOURSE_TMP group by t_basefiid, t_fiid, t_type, t_marketid, T_MARKETSECTORID) sou'||CHR(13)||CHR(10)||'on (sou.t_basefiid=tgt.t_basefiid and sou.t_fiid=tgt.t_fiid and sou.t_type=tgt.t_type and sou.t_marketid=tgt.t_marketid and sou.T_MARKETSECTORID=tgt.T_MARKETSECTORID)'||CHR(13)||CHR(10)||'when matched then update set TGT_LAST_DATE_DTX = sou.mdate ', 1, 
    'Обогащение записи - добавление TGT_LAST_DATE_DTX - максимальной даты по курсу сруди находящихся в DTXCOURSE_TMP', 'Добавление TGT_LAST_DATE_DTX (последняя дата по курсу в DTXCOURSE)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 110, 'update dtxcourse_tmp tgt  set TGT_ISLASTDATE=chr(88) '||CHR(13)||CHR(10)||'where t_ratedate = TGT_LAST_DATE_DTX '||CHR(13)||CHR(10)||'and (TGT_LAST_DATE_DTX > TGT_LAST_DATE_RSS or TGT_LAST_DATE_RSS is null)  ', 1, 
    'Обогащение записи - добавление TGT_ISLASTDATE - признак того, что это значение обновляет DRATEDEF и не надо отправлять его в DRATEHIST', 'Добавление TGT_ISLASTDATE (флаг, что это значение идёт в dratedef)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 120, 'update dtxcourse_tmp set tgt_newrateid=dratedef_dbt_seq.nextval '||CHR(13)||CHR(10)||'where TGT_RATEID is null and TGT_ISLASTDATE = chr(88) and t_action=1'||CHR(13)||CHR(10)||'', 1, 
    'Для нового курса назначаем tgt_newrateid, который потом пойдет в DRATEDEF_DBT.T_RATEID. Сначала назначаем его последнему значению курса (TGT_ISLASTDATE)', 'Для нового курса назначаем TGT_NEWRATEID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 130, 'merge into (select * from dtxcourse_tmp where TGT_RATEID is null and T_ACTION=1 and TGT_ISLASTDATE is null) tgt'||CHR(13)||CHR(10)||'using (select * from dtxcourse_tmp where TGT_NEWRATEID is not null) sou'||CHR(13)||CHR(10)||'on (sou.t_basefiid = tgt.t_basefiid and sou.t_type = tgt.t_type and sou.t_fiid = tgt.t_fiid and sou.T_MARKETID = tgt.T_MARKETID and sou.T_MARKETSECTORID = tgt.T_MARKETSECTORID) '||CHR(13)||CHR(10)||'when matched then update set tgt.tgt_newrateid = sou.tgt_newrateid'||CHR(13)||CHR(10)||'', 1, 
    'Распространяем новый TGT_NEWRATEID среди остальных значений курса', 'Распространяем новый TGT_NEWRATEID среди значений курса', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 3, 140, 'update DTXCOURSE_TMP set TGT_RATE = T_RATE * power(10, t_point)', 1, 
    'Обогащение записи - добавление TGT_RATE', 'Добавление TGT_RATE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 302 from dtxcourse_tmp where TGT_MARKETID is null', 1, 
    'Ошибка: торговая площадка не найдена в replobj_dbt', 'Проверка торговой площадки в RSS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 306 from dtxcourse_tmp where TGT_BASEFIID is null', 1, 
    'Ошибка: базовый финансоввый инструмент не найден в replobj_dbt', 'Проверка базового финиинструмента в RSS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 311 from dtxcourse_tmp where TGT_FIID is null', 1, 
    'Ошибка: котируемый финансоввый инструмент не найден в replobj_dbt', 'Проверка котируемого финиинструмента в RSS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 321 from dtxcourse_tmp where TGT_RATEID is null and t_action=2', 1, 
    'Ошибка: курс не найден в целевой системе', 'Проверка наличия курса в RSS для обновлений', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 322 from dtxcourse_tmp where TGT_RATEID is null and t_action=3', 1, 
    'Ошибка: курс не найден в целевой системе (для удалений)', 'Проверка наличия курса в RSS для удалений', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 70, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(13)||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 340 from dtxcourse_tmp sou left join dratehist_dbt h '||CHR(13)||CHR(10)||'on (sou.tgt_rateid=h.t_rateid and sou.t_ratedate=h.t_sincedate)'||CHR(13)||CHR(10)||'where (h.T_RATEID is null and nvl(sou.TGT_LAST_DATE_RSS,date''0001-01-01'')<>t_ratedate and t_action in (2,3))', 1, 
    'Для изменений/удалений проверяем, есть ли такой курс за такую дату в DRATEHIST_DBT или в DRATEDEF_DBT(откуда он уже перешел в TGT_LAST_DATE_RSS) ', 'Проверка наличия значения курса в RSS для изменений/удалений', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 80, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 70, sysdate, t_courseid, 340 from dtxcourse_tmp sou left join dratehist_dbt h on (sou.tgt_rateid=h.t_rateid and sou.t_ratedate=h.t_sincedate)'||CHR(10)||'where (h.T_RATEID > 0 or sou.TGT_LAST_DATE_RSS=t_ratedate and t_action = 1)', 1, 
    'Для вставок проверяем, есть ли уже такой курс за такую дату в DRATEHIST_DBT или в DRATEDEF_DBT(откуда он уже перешел в TGT_LAST_DATE_RSS) ', 'Проверка наличия значения курса в RSS для вставок (action=1)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 1, 10, 'update dtxdeal_tmp set T_EXTCODE = trim(T_EXTCODE), T_MARKETCODE = trim(T_MARKETCODE), '||CHR(13)||CHR(10)||'T_PARTYCODE = trim(T_PARTYCODE), T_CODE = trim(T_CODE), t_conditions=trim(t_conditions),'||CHR(13)||CHR(10)||'T_CONTRNUM=trim(T_CONTRNUM), T_DOPCONTROL_NOTE=trim(T_DOPCONTROL_NOTE), T_PRICE_CALC_MET_NOTE=trim(T_PRICE_CALC_MET_NOTE)', 1, 
    'Форматирование записи - TRIM текстовых полей', 'TRIM текстовых полей', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 1, 20, 'update dtxdeal_tmp set T_AMOUNT=nvl(T_AMOUNT, 0), T_PRICE=nvl(T_PRICE, 0), T_COST=nvl(T_COST, 0), '||CHR(13)||CHR(10)||'T_NKD=nvl(T_NKD, 0), T_TOTALCOST=nvl(T_TOTALCOST, 0), T_RATE=nvl(T_RATE, 0), '||CHR(13)||CHR(10)||'T_REPOBASE=nvl(T_REPOBASE, 0), T_ISPFI_1=nvl(T_ISPFI_1, 0), T_ISPFI_2=nvl(T_ISPFI_2, 0), '||CHR(13)||CHR(10)||'T_LIMIT=nvl(T_LIMIT, 0), T_CHRATE=nvl(T_CHRATE, 0), T_COUNTRY=nvl(T_COUNTRY, 165), '||CHR(13)||CHR(10)||'T_PRICE2=nvl(T_PRICE2, 0), T_COST2=nvl(T_COST2, 0), T_NKD2=nvl(T_NKD2, 0),'||CHR(13)||CHR(10)||'T_TOTALCOST2=nvl(T_TOTALCOST2, 0), T_DOPCONTROL=nvl(T_DOPCONTROL, 0), T_FISSKIND=nvl(T_FISSKIND, 0), '||CHR(13)||CHR(10)||'T_PRICE_CALC=nvl(T_PRICE_CALC, 0), T_PRICE_CALC_DEF=nvl(T_PRICE_CALC_DEF, 0), '||CHR(13)||CHR(10)||'T_PRICE_CALC_METHOD=nvl(T_PRICE_CALC_METHOD, 0), T_PRICE_CALC_VAL=nvl(T_PRICE_CALC_VAL, -1), '||CHR(13)||CHR(10)||'T_ADJUSTMENT=nvl(T_ADJUSTMENT, chr(0)), T_ATANYDAY=nvl(T_ATANYDAY, chr(0)), T_DIV=nvl(T_DIV, chr(0)), '||CHR(13)||CHR(10)||'T_COSTCHANGEONAMOR=nvl(T_COSTCHANGEONAMOR, chr(0)), T_COSTCHANGEONCOMP=nvl(T_COSTCHANGEONCOMP, chr(0)), '||CHR(13)||CHR(10)||'T_COSTCHANGE=nvl(T_COSTCHANGE, chr(0)), T_NEEDDEMAND=nvl(T_NEEDDEMAND, chr(0)), T_NEEDDEMAND2=nvl(T_NEEDDEMAND, chr(0))', 1, 
    'Форматирование записи - NVL полей', 'Обработка полей NVL', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, dtxdeal_tmp.t_dealid, '||CHR(13)||CHR(10)||'case    when ro.t_objstate=2 then 423 -- сделка уже удалена в целевой системе'||CHR(13)||CHR(10)||'    when tick.t_dealid is null then 422 -- нет тикета сделки'||CHR(13)||CHR(10)||'    when leg.t_dealid is null then 422 -- нет ценовых условий сделки'||CHR(13)||CHR(10)||'    when ro.t_destid is null  then 422 -- сделка с таким dealid не реплицировалась'||CHR(13)||CHR(10)||'end T_ERRORCODE '||CHR(13)||CHR(10)||'from dtxdeal_tmp left join dtxreplobj_dbt ro  on (ro.t_objecttype=80 and dtxdeal_tmp.t_dealid=ro.t_objectid)'||CHR(13)||CHR(10)||'left join ddl_tick_dbt tick on (ro.t_destid = tick.t_dealid) '||CHR(13)||CHR(10)||'left join ddl_leg_dbt leg on (ro.t_destid = leg.t_dealid and leg.t_legkind=0)'||CHR(13)||CHR(10)||'where dtxdeal_tmp.t_action in (2,3) and (ro.t_destid is null or tick.t_dealid is null or leg.t_dealid is null)'||CHR(13)||CHR(10)||'', 1, 
    'Запрос выполняется только для операций обновления/удаления. Проверяет, есть ли в целевой системе реплицированные сделки с данным ID. Обрабатывает сразу несколько событий - отсутствие сделки в тикетах, в leg, отсутствие в replobj и ручной режим обработки. Может, потом разобью на несколько запросов', 'Проверка наличия сделки в RS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 20, 'insert /*+ parallel(4) */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(13)||CHR(10)||'select :1, :2, :3, 80, sysdate,  d.t_dealid, 421 from dtxdeal_dbt dp join dtxdeal_tmp d on (d.t_code=dp.t_code and d.t_dealid > dp.t_dealid )'||CHR(13)||CHR(10)||'where d.t_replstate=0 and d.t_action=1'||CHR(13)||CHR(10)||'', 1, 
    'Запрос выполняется только для операций вставки. Проверяет дубли сделок по T_DEALCODE.', 'Проверка дублей по DEALCODE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(13)||CHR(10)||'select :1, :2, :3, 80, sysdate, d.t_dealid, 206 from dtxdeal_tmp d join dtxreplobj_dbt ro on (ro.t_objecttype=80 and ro.t_objectid=d.t_dealid and ro.t_objstate=1)'||CHR(13)||CHR(10)||'', 1, 
    'Объект находится в режиме ручного редактирования.', 'Проверка на ручное редактирование', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, t_dealid, 539 from dtxdeal_tmp where t_code is null'||CHR(13)||CHR(10)||'', 1, 
    'Не задан параметр T_CODE - код сделки', 'Проверка заполнения T_CODE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  t_dealid, 568 from dtxdeal_tmp where t_kind is null'||CHR(13)||CHR(10)||'', 1, 
    'Не задан параметр T_KIND - код сделки', 'Проверка заполнения T_KIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, t_dealid, 568 from dtxdeal_tmp where t_amount is null'||CHR(13)||CHR(10)||'', 1, 
    'Не задан параметр T_AMOUNT - количество ценных бумаг', 'Проверка заполнения T_AMOUNT', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 70, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  t_dealid, 556 from dtxdeal_tmp where (T_PRICE + T_COST + T_TOTALCOST) = 0 and T_KIND not in (70,80,90,100,110)'||CHR(13)||CHR(10)||'', 1, 
    'Не задан параметр T_PRICE - цена за шт. ценной бумаги, не включая НКД', 'Проверка заполнения T_PRICE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 80, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  t_dealid, 557 from dtxdeal_tmp where (T_PRICE + T_TOTALCOST) = 0 and T_COST > 0'||CHR(13)||CHR(10)||'', 1, 
    'Не задан параметр T_COST - стоимость ценных бумаг без НКД', 'Проверка заполнения T_COST', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 90, 'insert /*+ append */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(13)||CHR(10)||'select :1, :2, :3, 80, sysdate, t_dealid, 558 from dtxdeal_tmp where (T_PRICE + T_COST) = 0 and T_TOTALCOST > 0 and t_action in (1,2)'||CHR(13)||CHR(10)||'', 1, 
    'Не задан параметр T_TOTALCOST - общая сумма сделки вкл. НКД в валюте сделки', 'Проверка заполнения T_TOTALCOST', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 10, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_action>1) tgt using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.t_dealid)'||CHR(13)||CHR(10)||'when matched then update set tgt.tgt_dealid=sou.t_destid', 1, 
    'Обогащение записи - добавление TGT_DEALID из DTXREPLOBJ_DBT', 'Добавление TGT_DEALID (для измененй/удалений)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 20, 'update /*+ # */ dtxdeal_tmp tgt set TGT_BOFFICEKIND =  '||CHR(13)||CHR(10)||'                                case T_KIND'||CHR(13)||CHR(10)||'                                when 30 then 101'||CHR(13)||CHR(10)||'                                when 40 then 101'||CHR(13)||CHR(10)||'                                when 70 then 117'||CHR(13)||CHR(10)||'                                when 80 then 117'||CHR(13)||CHR(10)||'                                when 90 then 117'||CHR(13)||CHR(10)||'                                when 100 then 158'||CHR(13)||CHR(10)||'                                when 110 then 140'||CHR(13)||CHR(10)||'                                else 101'||CHR(13)||CHR(10)||'                                end', 1, 
    'Обогащение записи - добавление TGT_BOFFICEKIND', 'Добавление TGT_BOFFICEKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 30, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARENTID is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARENTID)'||CHR(10)||'when matched then update set tgt.TGT_PARENTID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_PARENTID из DTXREPLOBJ_DBT', 'Добавление TGT_PARENTID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 40, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PAYMCUR is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PAYMCUR)'||CHR(10)||'when matched then update set tgt.TGT_PAYMCUR=sou.t_destid', 1, 
    'Обогащение записи - добавление TGT_PAYMCUR из DTXREPLOBJ_DBT', 'Добавление TGT_PAYMCUR', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 50, '4', 1, 
    'Обогащение записи - добавление TGT_AVOIRISSID из DTXREPLOBJ_DBT', 'Добавление TGT_AVOIRISSID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 60, 'update dtxdeal_tmp set tgt_avoirissid = 2192, '||CHR(13)||CHR(10)||'TGT_isbasket=chr(88),'||CHR(13)||CHR(10)||'T_PAYDATE = T_SUPLDATE,'||CHR(13)||CHR(10)||'T_SUPLDATE = T_PAYDATE'||CHR(13)||CHR(10)||'where t_replstate=0 and t_avoirissid=-20', 1, 
    'Обогащение записи - добавление TGT_AVOIRISSID для корзины', 'Добавление TGT_AVOIRISSID (2192) для корзины', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 70, 'merge into dtxdeal_tmp tgt '||CHR(13)||CHR(10)||'using (select t_fiid, t_root from dfininstr_dbt f join davrkinds_dbt k on (f.T_AVOIRKIND=k.T_AVOIRKIND)) sou '||CHR(13)||CHR(10)||'on (sou.t_fiid=tgt.TGT_AVOIRISSID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_AVOIRKIND=sou.T_ROOT'||CHR(13)||CHR(10)||'', 1, 
    'Обогащение записи - добавление TGT_AVOIRKIND', 'Добавление TGT_AVOIRKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 80, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTYID)'||CHR(10)||'when matched then update set tgt.TGT_PARTYID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_PARTYID из DTXREPLOBJ_DBT', 'Добавление TGT_PARTYID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 90, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt '||CHR(13)||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_CURRENCYID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_CURRENCYID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_CURRENCYID из DTXREPLOBJ_DBT', 'Добавление TGT_CURRENCYID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 100, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_MARKETID is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID)'||CHR(10)||'when matched then update set tgt.TGT_MARKETID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_MARKETID из DTXREPLOBJ_DBT', 'Добавление TGT_MARKETID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 110, 'update /*+ parallel(4) */ dtxdeal_tmp d set tgt_brokerid=nvl((select t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0 and t_objectid=d.t_brokerid),-1)'||CHR(13)||CHR(10)||'where t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_BROKERID из DTXREPLOBJ_DBT', 'Добавление TGT_BROKERID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 120, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_SECTOR is not null) tgt '||CHR(10)||'using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=40 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID  and  sou.T_SUBOBJNUM=tgt.T_SECTOR)'||CHR(10)||'when matched then update set tgt.TGT_SECTOR=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_SECTOR из DTXREPLOBJ_DBT', 'Добавление TGT_SECTOR', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 130, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_NKDFIID is not null) tgt '||CHR(13)||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_NKDFIID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_NKDFIID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_NKDFIID из DTXREPLOBJ_DBT', 'Добавление TGT_NKDFIID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 140, 'update dtxdeal_tmp set TGT_REPOBASE = CASE t_repobase when 1 then 1  when 2 then 3  when 3 then 5  when 4 then 2   when 5 then 0   when 6 then 4   else -1  end'||CHR(10)||' where t_replstate=0 and T_REPOBASE is not null', 1, 
    'Обогащение записи - добавление TGT_REPOBASE', 'Добавление TGT_REPOBASE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 150, 'update dtxdeal_tmp set TGT_COUNTRY = (select t_codelat3 from dcountry_dbt where t_countryid = T_COUNTRY)'||CHR(10)||' where t_replstate=0 and T_COUNTRY <> 165', 1, 
    'Обогащение записи - добавление TGT_COUNTRY', 'Добавление TGT_COUNTRY', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 160, 'update dtxdeal_tmp set TGT_EXISTBACK = chr(88) where t_replstate=0 and T_KIND in (30,40)', 1, 
    'Обогащение записи - добавление TGT_EXISTBACK', 'Добавление TGT_EXISTBACK', NULL, NULL);
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 170, 'update dtxdeal_tmp set tgt_objtype = case when T_KIND in (70,80,90) then 117 else 101 end where t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_OBJTYPE', 'Добавление TGT_OBJTYPE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 180, 'update /*+ # */ dtxdeal_tmp set tgt_department = nvl(( select t_code from dtxreplobj_dbt ro join ddp_dep_dbt dp on (ro.t_destid=dp.t_partyid and ro.t_objecttype=40 and ro.t_objstate=0)),1)', 1, 
    'Обогащение записи - добавление TGT_DEPARTMENT', 'Добавление TGT_DEPARTMENT', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 190, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_WARRANTID is not null) tgt '||CHR(10)||'using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=50 and t_objstate=0) sou on (sou.t_objectid=tgt.T_WARRANTID)'||CHR(10)||'when matched then update set tgt.TGT_WARRANTID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_WARRANTID из DTXREPLOBJ_DBT', 'Добавление TGT_WARRANTID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 200, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARTIALID is not null) tgt '||CHR(13)||CHR(10)||'using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=60 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTIALID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_PARTIALID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_PARTIALID из DTXREPLOBJ_DBT', 'Добавление TGT_PARTIALID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 230, 'update dtxdeal_tmp set '||CHR(13)||CHR(10)||'TGT_ISLOAN = decode( t_kind, 50, CHR(88), 60, CHR(88), CHR(0)),'||CHR(13)||CHR(10)||'TGT_ISREPO = case when t_kind in (30,40,50,60) then CHR(88) else chr(0) end'||CHR(13)||CHR(10)||'where t_replstate=0 and t_action in (1,2)', 1, 
    'Обогащение записи - добавление TGT_ISREPO / TGT_ISLOAN ', 'Добавление TGT_ISREPO / TGT_ISLOAN', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 240, 'update dtxdeal_tmp set TGT_WARRANT_NUM = (select t_number from dfiwarnts_dbt where t_id = TGT_WARRANTID) where TGT_WARRANTID is not null', 1, 
    'Обогащение записи - добавление TGT_WARRANT_NUM по TGT_WARRANTID', 'Добавление TGT_WARRANT_NUM по TGT_WARRANTID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 250, 'update dtxdeal_tmp set TGT_PARTIAL_NUM = (select t_number from dfiwarnts_dbt where t_id = TGT_PARTIALID) where TGT_PARTIALID is not null', 1, 
    'Обогащение записи - добавление TGT_PARTIAL_NUM по TGT_PARTIALID', 'Добавление TGT_PARTIAL_NUM по TGT_PARTIALID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 260, 'update /*+ # */ dtxdeal_tmp tgt set TGT_CURNOM = LOAD_RSS.GetCurrentNom( tgt_avoirissid, t_instancedate )', 1, 
    'Обогащение записи - добавление TGT_CURNOM - текущего номинала бумаги', 'Добавление TGT_CURNOM', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 270, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_MATURITYISPRINCIPAL = case when (TGT_ISLOAN=CHR(88) or T_KIND in (80,90 /*для погашений и ЧП*/ ) or (T_SUPLDATE < T_PAYDATE)) then CHR(88) else CHR(0) end,'||CHR(10)||'tgt_maturity = case when TGT_ISLOAN=CHR(88) then T_SUPLDATE WHEN T_KIND in (80,90) THEN T_BALANCEDATE WHEN T_SUPLDATE < T_PAYDATE THEN T_SUPLDATE ELSE T_PAYDATE end,'||CHR(10)||'tgt_expiry = case when TGT_ISLOAN=CHR(88) then T_SUPLDATE2 WHEN T_KIND in (80,90) THEN T_BALANCEDATE WHEN T_SUPLDATE < T_PAYDATE THEN T_PAYDATE ELSE T_SUPLDATE end,'||CHR(10)||'tgt_ReceiptAmount = case when TGT_ISLOAN=CHR(88) then T_NKD2 end', 1, 
    'Обогащение записи - добавление TGT_MATURITY / TGT_EXPIRY / TGT_RECEIPTAMOUNT / TGT_MATURITYISPRINCIPAL', 'Добавление TGT_MATURITY / TGT_EXPIRY / TGT_RECEIPTAMOUNT / TGT_MATURITYISPRINCIPAL', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 280, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_MATURITYISPRINCIPAL2 = case when T_SUPLDATE < T_PAYDATE then CHR(88) else CHR(0) end,'||CHR(10)||'tgt_maturity2 = case when T_SUPLDATE < T_PAYDATE THEN T_SUPLDATE ELSE T_PAYDATE end,'||CHR(10)||'tgt_expiry2 = case when T_SUPLDATE < T_PAYDATE THEN T_PAYDATE ELSE T_SUPLDATE end'||CHR(10)||'    ', 1, 
    'Обогащение записи - добавление TGT_MATURITY2 / TGT_EXPIRY2 / TGT_MATURITYISPRINCIPAL2', 'Добавление TGT_MATURITY2 / TGT_EXPIRY2 / TGT_MATURITYISPRINCIPAL2', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 290, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(13)||CHR(10)||'TGT_FORMULA = case T_ACCOUNTTYPE'||CHR(13)||CHR(10)||'              WHEN 1 THEN 50 /*DVP*/'||CHR(13)||CHR(10)||'              WHEN 2 THEN 49 /*DFP*/'||CHR(13)||CHR(10)||'              WHEN 3 THEN 52 /*PP*/'||CHR(13)||CHR(10)||'              WHEN 4 THEN 51 /*PD*/ '||CHR(13)||CHR(10)||'              end '||CHR(13)||CHR(10)||'', 1, 
    'Обогащение записи - добавление TGT_FORMULA', 'Добавление TGT_FORMULA', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 300, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(13)||CHR(10)||'TGT_ISQUOTED = LOAD_RSS.GETISQUOTED(TGT_AVOIRISSID, T_INSTANCEDATE),'||CHR(13)||CHR(10)||'TGT_ISKSU = LOAD_RSS.GETISKSU(TGT_AVOIRISSID),'||CHR(13)||CHR(10)||'TGT_ISBOND = decode(TGT_AVOIRKIND, 17, chr(88), chr(0)) '||CHR(13)||CHR(10)||'', 1, 
    'Обогащение записи - добавление TGT_ISQUOTED / TGT_ISKSU / TGT_ISBOND', 'Добавление TGT_ISQUOTED / TGT_ISKSU / TGT_ISBOND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 310, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_RELATIVEPRICE = case when TGT_ISBOND=chr(88) and tgt_bofficekind <> 117 then chr(88) else chr(0) end '||CHR(10)||'', 1, 
    'Обогащение записи - добавление TGT_RELATIVEPRICE', 'Добавление TGT_RELATIVEPRICE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 320, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(13)||CHR(10)||'TGT_PORTFOLIOID = case when TGT_ISQUOTED=chr(88) then 1 else 5 end,'||CHR(13)||CHR(10)||'TGT_PORTFOLIOID_2 = case when TGT_ISQUOTED=chr(88) and TGT_ISREPO=chr(88) then 1 else 5 end '||CHR(13)||CHR(10)||'where TGT_ISLOAN = chr(0)'||CHR(13)||CHR(10)||'', 1, 
    'Обогащение записи - переопределение портфелей для НЕ займов (TGT_PORTFOLIOID/TGT_PORTFOLIOID_2)', 'Добавление TGT_PORTFOLIOID / TGT_PORTFOLIOID_2', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 330, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_DEALKIND = Load_RSS.GetDealKind(T_KIND, T_MARKETID, TGT_AVOIRISSID, TGT_ISBASKET, TGT_ISKSU) '||CHR(10)||'    ', 1, 
    'Обогащение записи - добавление TGT_DEALKIND - вид сделки', 'Добавление TGT_DEALKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 340, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_PRICE = case when tgt_isbond=chr(88) and tgt_bofficekind=117 and t_kind in (70,90) then tgt_curnom'||CHR(10)||'                 when tgt_isbond=chr(88) and tgt_bofficekind=117 and t_kind = 80 then 0'||CHR(10)||'                 else t_price'||CHR(10)||'            end'||CHR(10)||'    ', 1, 
    'Обогащение записи - добавление TGT_PRICE - он особенный для погашений облигаций и купонов, в остальных случаях совпадает с t_price', 'Добавление TGT_PRICE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 350, 'update dtxdeal_tmp set TGT_DEALID = ddl_tick_dbt_seq.nextval where t_replstate=0 and t_action=1', 1, 
    'Обогащение записи - добавление TGT_DEALID из последовательности для вставок', 'Добавление TGT_DEALID для вставок', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 601 from dtxdemand_tmp where t_dealid is null', 1, 
    'Не задан параметр T_DEALID - идентификатор сделки', 'Проверка заполнения T_DEALID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 602 from dtxdemand_tmp where t_part is null', 1, 
    'Не задан параметр T_PART - номер части сделки', 'Проверка заполнения T_PART', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 603 from dtxdemand_tmp where t_kind is null', 1, 
    'Не задан параметр T_KIND - вид платежа', 'Проверка заполнения T_KIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 604 from dtxdemand_tmp where t_direction is null', 1, 
    'Не задан параметр t_direction - вид платежа', 'Проверка заполнения T_DIRECTION', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 605 from dtxdemand_tmp where t_fikind is null', 1, 
    'Не задан t_fikind  платежа', 'Проверка заполнения T_FIKIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 606 from dtxdemand_tmp where t_date is null', 1, 
    'Не задано t_date - дата платежа', 'Проверка заполнения T_DATE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 70, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 607 from dtxdemand_tmp where t_sum is null', 1, 
    'Не задано t_sum - сумма платежа', 'Проверка заполнения T_SUM', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 80, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 608 from dtxdemand_tmp where t_state is null', 1, 
    'Не задано t_state - статус платежа', 'Проверка заполнения T_STATE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 90, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(13)||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 609 from dtxdemand_tmp where t_paycurrencyid is null and t_kind not in (10,60,120)', 1, 
    'Не задано t_paycurrencyid - валюта платежа', 'Проверка заполнения T_PAYCURRENCYID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 10, 'merge /*+ # */ into (select * from dtxdemand_tmp where t_replstate=0 ) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=90 and t_objstate=0) sou on (sou.t_objectid=tgt.T_DEMANDID)'||CHR(10)||'when matched then update set tgt.TGT_DEMANDID=sou.T_DESTID', 1, 
    'Обогащение записи - поиск TGT_DEMANDID для обновлений/удалений', 'Добавление TGT_DEMANDID для обновлений/удалений (action 2,3)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 12, 'update dtxdemand_tmp set TGT_DEMANDID=ddlrq_dbt_seq.nextval where t_action=1 and t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_DEMANDID для вставок (action=1)', 'Добавление TGT_DEMANDID для вставок (action=1)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 20, 'merge /*+ # */ into dtxdemand_tmp tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_DEALID)'||CHR(10)||'when matched then update set tgt.TGT_DEALID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_DEALID', 'Добавление TGT_DEALID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 30, 'merge /*+ # */ into (select * from dtxdemand_tmp where t_fikind=20) tgt '||CHR(13)||CHR(10)||'using (select t_dealid, t_pfi from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_FIID=sou.T_PFI', 1, 
    'Обогащение записи - добавление TGT_FIID для платежей бумагами', 'Добавление TGT_FIID для платежей бумагами', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 40, 'merge /*+ # */ into dtxdemand_tmp tgt '||CHR(10)||'using (select t_dealid, T_DEALTYPE from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID)'||CHR(10)||'when matched then update set tgt.TGT_DEALKIND=sou.T_DEALTYPE', 1, 
    'Обогащение записи - добавление TGT_DEALKIND', 'Добавление TGT_DEALKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 50, 'merge /*+ # */ into (select * from dtxdemand_tmp where t_fikind=10) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PAYCURRENCYID)'||CHR(10)||'when matched then update set tgt.TGT_FIID=sou.t_destid', 1, 
    'Обогащение записи - добавление TGT_FIID для платежей деньгами', 'Добавление TGT_FIID для платежей деньгами', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 60, 'merge /*+ # */ into (select * from dtxdemand_tmp) tgt '||CHR(10)||'   using (select t_dealid, T_BOFFICEKIND from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID) '||CHR(10)||'   when matched then update set tgt.TGT_BOFFICEKIND=sou.T_BOFFICEKIND', 1, 
    'Обогащение записи - добавление TGT_BOFFICEKIND', 'Добавление TGT_BOFFICEKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 70, 'merge /*+ # */ into (select * from dtxdemand_tmp) tgt '||CHR(10)||'   using (select t_dealid, T_MARKETID, T_PARTYID from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID) '||CHR(10)||'   when matched then update set tgt.TGT_PARTY= case when sou.T_MARKETID>0 then T_MARKETID else T_PARTYID end', 1, 
    'Обогащение записи - добавление TGT_PARTY', 'Добавление TGT_PARTY', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 90, 'update dtxdemand_tmp set TGT_KIND=case when t_direction=1 then 0 else 1 end', 1, 
    'Обогащение записи - добавление TGT_KIND', 'Добавление TGT_KIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 100, 'update dtxdemand_tmp set TGT_STATE=2, TGT_PLANDATE=date''0001-01-01'', TGT_FACTDATE=T_DATE, TGT_CHANGEDATE=date''0001-01-01'' where t_isfact=chr(88)', 1, 
    'Обогащение записи - добавление TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE для фактических платежей', 'Добавление TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE - 1', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 110, 'update dtxdemand_tmp set TGT_STATE=0, TGT_PLANDATE=T_DATE, TGT_FACTDATE=date''0001-01-01'', TGT_CHANGEDATE=T_DATE where t_isfact<>chr(88)', 1, 
    'Обогащение записи - добавление TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE для плановых платежей', 'Добавление TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE - 2', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 120, 'update dtxdemand_tmp set TGT_TYPE='||CHR(10)||'case t_kind'||CHR(10)||'    when 10 then 8 -- поставка'||CHR(10)||'    when 20 then 0 -- задаток'||CHR(10)||'    when 30 then 1 -- аванс'||CHR(10)||'    when 40 then 2 -- оплата'||CHR(10)||'    when 50 then 3 -- проценты'||CHR(10)||'    when 60 then 8 -- итоговая поставка'||CHR(10)||'    when 70 then 2 -- итоговая оплата'||CHR(10)||'    when 80 then 4 -- выплата купонного дохода'||CHR(10)||'    when 90 then 5 -- частичное погашение'||CHR(10)||'    when 100 then 10 -- выплата дивидендов'||CHR(10)||'    when 110 then 7  -- компенсационная оплата'||CHR(10)||'    when 120 then 9  -- компенсационная поставка'||CHR(10)||'    else -1'||CHR(10)||'end'||CHR(10)||'', 1, 
    'Обогащение записи - добавление TGT_TYPE', 'Добавление TGT_TYPE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 3, 130, 'update dtxdemand_tmp set TGT_SUBKIND=case when tgt_type in (8,9) then 1 else 0 end', 1, 
    'Обогащение записи - добавление TGT_SUBKIND', 'Добавление TGT_SUBKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) '||CHR(13)||CHR(10)||'select :1, :2, :3, 90, sysdate, t_demandid, 611 from dtxdemand_tmp where TGT_DEMANDID is null and t_action in (2,3)', 1, 
    'Ошбка: Не найдено в dtxreplobj реплицированное Т/О по сделке (для обновлений и удалений)', 'Не найдено в dtxreplobj Т/О по сделке (action = 2,3)', 'X', 'X');
COMMIT;
