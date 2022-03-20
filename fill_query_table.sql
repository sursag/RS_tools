SET DEFINE OFF;

delete DTX_QUERY_DBT;


Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 1, 10, 'update /*+ # */ DTXCOURSE_TMP set
T_TYPE=nvl(T_TYPE,0), T_BASEFIID=nvl(T_BASEFIID,0), T_FIID=nvl(T_FIID,0), T_MARKETID=nvl(T_MARKETID,0), T_MARKETSECTORID=nvl(T_MARKETSECTORID,0), T_POINT=nvl(T_POINT,0), T_SCALE=nvl(T_SCALE,0)    
', 1, 
    'Обработка записи NVL', 'Обработка записей NVL', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 303 from dtxcourse_tmp where t_type=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задан вид', 'Проверка вида курса', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 304 from dtxcourse_tmp where t_rate=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задано значение', 'Проверка значения курса', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 301 from dtxcourse_tmp where T_MARKETID=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задана торговая площадка', 'Проверка торговой площадки курса', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 305 from dtxcourse_tmp where T_BASEFIID=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задан базовый финансовый инструмент', 'Проверка базового финансового инструмента', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 310 from dtxcourse_tmp where T_BASEFIID=0 and t_action in (1,2)', 1, 
    'Ошибка: для курса не задан котируемый финансовый инструмент', 'Проверка котируемого финансового инструмента', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select * from (select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 900 from dtxcourse_tmp sou group by t_courseid, t_instancedate having count(*)>1)', 2, 
    'Проверяем записи на предмет дулирования id внутри t_instancedate, поскольку по ограничениям реализации объект может присутствовать только 1 раз в день', 'Проверка на дублирование id внутри дня', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 10, 'merge /*+ # */ into dtxcourse_tmp tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID)
when matched then update set tgt.TGT_MARKETID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_MARKETID', 'Добавление TGT_MARKETID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 15, 'update /*+ # */ DTXCOURSE_TMP set TGT_ISNOMINAL=chr(88) where t_type=100     
', 1, 
    'Обогащение записи - добавление TGT_ISNOMINAL', 'Добавление TGT_ISNOMINAL', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 20, 'merge /*+ # */ into dtxcourse_tmp tgt 
using (select t_objectid, t_subobjnum, t_destid from dtxreplobj_dbt where t_objecttype=40 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID and sou.t_subobjnum=tgt.T_MARKETSECTORID)
when matched then update set tgt.TGT_SECTORID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_SECTORID', 'Добавление TGT_SECTORID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 30, 'merge /*+ # */ into dtxcourse_tmp tgt 
using (select t_objecttype, t_objectid, t_destid from dtxreplobj_dbt where t_objstate<>2) sou on (sou.t_objectid=tgt.T_BASEFIID and sou.t_objecttype=tgt.t_basefikind)
when matched then update set tgt.TGT_BASEFIID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_BASEFIID', 'Добавление TGT_BASEFIID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 35, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_basefikind=20) tgt 
using (select * from dfininstr_dbt) sou on (sou.t_fiid=tgt.TGT_BASEFIID)
when matched then update set tgt.TGT_FACEVALUE_FIID=sou.T_FACEVALUEFI', 1, 
    'Обогащение записи - добавление TGT_FACEVALUE_FIID (если базовый инструмент - бумага)', 'Добавление TGT_FACEVALUE_FIID (для бумаг)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 40, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_basefikind=10) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_FIID)
when matched then update set tgt.TGT_FIID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_FIID (деньги)', 'Добавление TGT_FIID (деньги)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 50, 'update /*+ # */ dtxcourse_tmp set TGT_FIID=TGT_FACEVALUE_FIID where t_basefikind=20', 1, 
    'Обогащение записи - добавление TGT_FIID (бумаги)', 'Добавление TGT_FIID (бумаги)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 55, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_basefikind=20) tgt 
using (select t_fiid, t_root from dfininstr_dbt f join davrkinds_dbt k on (f.T_AVOIRKIND=k.T_AVOIRKIND and f.T_FI_KIND=k.T_FI_KIND)) sou 
on (sou.t_fiid=tgt.TGT_BASEFIID)
when matched then update set tgt.TGT_BASEFIKIND=sou.T_ROOT
', 1, 
    'Обогащение записи - добавление TGT_BASEFIKIND', 'Добавление TGT_BASEFIKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 60, 'update /*+ # */ DTXCOURSE_TMP set TGT_ISDOMINANT=chr(88) where t_type=6 and t_basefikind=10     
', 1, 
    'Обогащение записи - добавление TGT_ISDOMINANT', 'Добавление TGT_ISDOMINANT', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 70, 'update /*+ # */ DTXCOURSE_TMP set TGT_TYPE=load_rss.getratetype(t_type)     
', 1, 
    'Обогащение записи - добавление TGT_TYPE', 'Добавление TGT_TYPE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 90, 'update /*+ # */ DTXCOURSE_TMP set TGT_ISRELATIVE=chr(88) where TGT_BASEFIKIND=17 and T_TYPE<>15', 1, 
    'Обогащение записи - добавление TGT_ISRELATIVE,если бумага-облигация и вид курса - не НКД на дату', 'Добавление TGT_ISRELATIVE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 100, 'merge /*+ # */ into (select * from dtxcourse_tmp where tgt_isnominal is null and tgt_isnominal is null) tgt 
using (select * from dratedef_dbt) sou on (sou.T_OTHERFI=TGT_BASEFIID and sou.t_fiid=TGT_FIID and sou.t_type=TGT_TYPE and sou.t_market_place=TGT_MARKETID)
when matched then update set tgt.TGT_RATEID=sou.T_RATEID, tgt.TGT_LAST_DATE_RSS=sou.T_SINCEDATE', 1, 
    'Обогащение записи - добавление TGT_RATEID / TGT_LAST_DATE_RSS (кроме записей на изменение номинала)', 'Добавление TGT_RATEID / TGT_LAST_DATE_RSS (кроме изменений номинала)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 103, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_replstate=0 and tgt_isnominal = chr(88)) tgt 
using (select * from dtxreplobj_dbt where t_objecttype=70 and t_objstate<2) sou 
on (sou.t_objectid = tgt.t_courseid) 
when matched then update set tgt.TGT_RATEID=sou.t_destid', 1, 
    'Обогащение записи - добавление TGT_RATEID (для записей изменения номинала)', 'Добавление TGT_RATEID (для изменений номинала)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 105, 'merge /*+ # */ into (select * from DTXCOURSE_TMP where tgt_isnominal is null and t_replstate=0) tgt 
using  (select t_basefiid, t_fiid, t_type, t_marketid, T_MARKETSECTORID, max(t_ratedate) mdate from DTXCOURSE_TMP where tgt_isnominal is null group by t_basefiid, t_fiid, t_type, t_marketid, t_marketsectorid) sou
on (sou.t_basefiid=tgt.t_basefiid and sou.t_fiid=tgt.t_fiid and sou.t_type=tgt.t_type and sou.t_marketid=tgt.t_marketid and sou.T_MARKETSECTORID=tgt.T_MARKETSECTORID)
when matched then update set TGT_LAST_DATE_DTX = sou.mdate ', 1, 
    'Обогащение записи - добавление TGT_LAST_DATE_DTX - максимальной даты по курсу сруди находящихся в DTXCOURSE_TMP', 'Добавление TGT_LAST_DATE_DTX (последняя дата по курсу в DTXCOURSE)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 110, 'update /*+ # */ dtxcourse_tmp tgt  set TGT_ISLASTDATE=chr(88) 
where t_ratedate = TGT_LAST_DATE_DTX 
and (TGT_LAST_DATE_DTX > TGT_LAST_DATE_RSS or TGT_LAST_DATE_RSS is null)  
and tgt_isnominal is null', 1, 
    'Обогащение записи - добавление TGT_ISLASTDATE - признак того, что это значение обновляет DRATEDEF и не надо отправлять его в DRATEHIST', 'Добавление TGT_ISLASTDATE (флаг, что это значение идёт в dratedef)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 120, 'update /*+ # */ dtxcourse_tmp set tgt_newrateid=dratedef_dbt_seq.nextval 
where TGT_RATEID is null and TGT_ISLASTDATE = chr(88) and t_action=1
', 1, 
    'Для нового курса назначаем tgt_newrateid, который потом пойдет в DRATEDEF_DBT.T_RATEID. Сначала назначаем его последнему значению курса (TGT_ISLASTDATE)', 'Для нового курса назначаем TGT_NEWRATEID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 125, 'update /*+ # */ DTXCOURSE_TMP set TGT_RATEID = DFIVLHIST_DBT_seq.nextval
 where t_replstate=0 and tgt_isnominal = chr(88)', 1, 
    'Обогащение записи - добавление TGT_RATEID для вставок изменений номинала (t_action=1)', 'Добавление TGT_RATEID (для вставок изменений номинала)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 130, 'merge /*+ # */ into (select * from dtxcourse_tmp where TGT_RATEID is null and T_ACTION=1 and TGT_ISLASTDATE is null and TGT_ISNOMINAL is null) tgt
using (select * from dtxcourse_tmp where TGT_NEWRATEID is not null) sou
on (sou.t_basefiid = tgt.t_basefiid and sou.t_type = tgt.t_type and sou.t_fiid = tgt.t_fiid and sou.T_MARKETID = tgt.T_MARKETID and sou.T_MARKETSECTORID = tgt.T_MARKETSECTORID) 
when matched then update set tgt.tgt_newrateid = sou.tgt_newrateid
', 1, 
    'Распространяем новый TGT_NEWRATEID среди остальных значений курса', 'Распространяем новый TGT_NEWRATEID среди значений курса', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 140, 'update /*+ # */ DTXCOURSE_TMP set TGT_RATE = T_RATE * power(10, t_point)', 1, 
    'Обогащение записи - добавление TGT_RATE', 'Добавление TGT_RATE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 302 from dtxcourse_tmp where TGT_MARKETID is null', 1, 
    'Ошибка: торговая площадка не найдена в replobj_dbt', 'Проверка торговой площадки в RSS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 306 from dtxcourse_tmp where TGT_BASEFIID is null', 1, 
    'Ошибка: базовый финансоввый инструмент не найден в replobj_dbt', 'Проверка базового финиинструмента в RSS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 311 from dtxcourse_tmp where TGT_FIID is null', 1, 
    'Ошибка: котируемый финансоввый инструмент не найден в replobj_dbt', 'Проверка котируемого финиинструмента в RSS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 321 from dtxcourse_tmp where TGT_RATEID is null and t_action=2', 1, 
    'Ошибка: курс не найден в целевой системе', 'Проверка наличия курса в RSS для обновлений', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 322 from dtxcourse_tmp where TGT_RATEID is null and t_action=3', 1, 
    'Ошибка: курс не найден в целевой системе (для удалений)', 'Проверка наличия курса в RSS для удалений', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 70, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 340 from dtxcourse_tmp sou left join dratehist_dbt h 
on (sou.tgt_rateid=h.t_rateid and sou.t_ratedate=h.t_sincedate)
where (h.T_RATEID is null and nvl(sou.TGT_LAST_DATE_RSS,date''0001-01-01'')<>t_ratedate and t_action in (2,3))', 1, 
    'Для изменений/удалений проверяем, есть ли такой курс за такую дату в DRATEHIST_DBT или в DRATEDEF_DBT(откуда он уже перешел в TGT_LAST_DATE_RSS) ', 'Проверка наличия значения курса в RSS для изменений/удалений', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 80, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 340 from dtxcourse_tmp sou left join dratehist_dbt h on (sou.tgt_rateid=h.t_rateid and sou.t_ratedate=h.t_sincedate)
where (h.T_RATEID > 0 or sou.TGT_LAST_DATE_RSS=t_ratedate and t_action = 1)', 1, 
    'Для вставок проверяем, есть ли уже такой курс за такую дату в DRATEHIST_DBT или в DRATEDEF_DBT(откуда он уже перешел в TGT_LAST_DATE_RSS) ', 'Проверка наличия значения курса в RSS для вставок (action=1)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 1, 10, 'update /*+ # */ dtxdeal_tmp set T_EXTCODE = trim(T_EXTCODE), T_MARKETCODE = trim(T_MARKETCODE), 
T_PARTYCODE = trim(T_PARTYCODE), T_CODE = trim(T_CODE), t_conditions=trim(t_conditions),
T_CONTRNUM=trim(T_CONTRNUM), T_DOPCONTROL_NOTE=trim(T_DOPCONTROL_NOTE), T_PRICE_CALC_MET_NOTE=trim(T_PRICE_CALC_MET_NOTE)', 1, 
    'Форматирование записи - TRIM текстовых полей', 'TRIM текстовых полей', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 1, 20, 'update /*+ # */ dtxdeal_tmp set T_AMOUNT=nvl(T_AMOUNT, 0), T_PRICE=nvl(T_PRICE, 0), T_COST=nvl(T_COST, 0), 
T_NKD=nvl(T_NKD, 0), T_TOTALCOST=nvl(T_TOTALCOST, 0), T_RATE=nvl(T_RATE, 0), 
T_REPOBASE=nvl(T_REPOBASE, 0), T_ISPFI_1=nvl(T_ISPFI_1, 0), T_ISPFI_2=nvl(T_ISPFI_2, 0), 
T_LIMIT=nvl(T_LIMIT, chr(0)), T_CHRATE=nvl(T_CHRATE, chr(0)), T_COUNTRY=nvl(T_COUNTRY, 165), 
T_PRICE2=nvl(T_PRICE2, 0), T_COST2=nvl(T_COST2, 0), T_NKD2=nvl(T_NKD2, 0),
T_TOTALCOST2=nvl(T_TOTALCOST2, 0), T_DOPCONTROL=nvl(T_DOPCONTROL, 0), T_FISSKIND=nvl(T_FISSKIND, 0), 
T_PRICE_CALC=nvl(T_PRICE_CALC, 0), T_PRICE_CALC_DEF=nvl(T_PRICE_CALC_DEF, 0), 
T_PRICE_CALC_METHOD=nvl(T_PRICE_CALC_METHOD, 0), T_PRICE_CALC_VAL=nvl(T_PRICE_CALC_VAL, -1), 
T_ADJUSTMENT=nvl(T_ADJUSTMENT, chr(0)), T_ATANYDAY=nvl(T_ATANYDAY, chr(0)), T_DIV=nvl(T_DIV, chr(0)), 
T_COSTCHANGEONAMOR=nvl(T_COSTCHANGEONAMOR, chr(0)), T_COSTCHANGEONCOMP=nvl(T_COSTCHANGEONCOMP, chr(0)), 
T_COSTCHANGE=nvl(T_COSTCHANGE, chr(0)), T_NEEDDEMAND=nvl(T_NEEDDEMAND, chr(0)), T_NEEDDEMAND2=nvl(T_NEEDDEMAND, chr(0))', 1, 
    'Форматирование записи - NVL полей', 'Обработка полей NVL', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 1, 30, 'update /*+ # */ DTXDEAL_TMP set T_TIME=DATE''0001-01-01'' + (T_TIME-trunc(T_TIME))', 1, 
    'Форматирование записи - преобразование поля T_TIME', 'Форматирование записи - поле T_TIME', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate, dtxdeal_tmp.t_dealid, 
case    when ro.t_objstate=2 then 423 -- сделка уже удалена в целевой системе
    when tick.t_dealid is null then 422 -- нет тикета сделки
    when leg.t_dealid is null then 422 -- нет ценовых условий сделки
    when ro.t_destid is null  then 422 -- сделка с таким dealid не реплицировалась
end T_ERRORCODE 
from dtxdeal_tmp left join dtxreplobj_dbt ro  on (ro.t_objecttype=80 and dtxdeal_tmp.t_dealid=ro.t_objectid)
left join ddl_tick_dbt tick on (ro.t_destid = tick.t_dealid) 
left join ddl_leg_dbt leg on (ro.t_destid = leg.t_dealid and leg.t_legkind=0)
where dtxdeal_tmp.t_action in (2,3) and dtxdeal_tmp.t_parentid>0 and (ro.t_destid is null or tick.t_dealid is null or leg.t_dealid is null)
', 1, 
    'Запрос выполняется только для операций обновления/удаления. Проверяет, есть ли в целевой системе реплицированные сделки с данным ID. Обрабатывает сразу несколько событий - отсутствие сделки в тикетах, в leg, отсутствие в replobj и ручной режим обработки. Может, потом разобью на несколько запросов', 'Проверка наличия сделки в RS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, d.t_instancedate, 80, sysdate,  d.t_dealid, 421 from dtxdeal_dbt dp join dtxdeal_tmp d on (d.t_code=dp.t_code and d.t_dealid > dp.t_dealid )
where d.t_replstate=0 and d.t_action=1
', 1, 
    'Запрос выполняется только для операций вставки. Проверяет дубли сделок по T_DEALCODE.', 'Проверка дублей по DEALCODE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, d.t_dealid, 206 from dtxdeal_tmp d join dtxreplobj_dbt ro on (ro.t_objecttype=80 and ro.t_objectid=d.t_dealid and ro.t_objstate=1)
', 1, 
    'Объект находится в режиме ручного редактирования.', 'Проверка на ручное редактирование', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 539 from dtxdeal_tmp where t_code is null
', 1, 
    'Не задан параметр T_CODE - код сделки', 'Проверка заполнения T_CODE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate,  t_dealid, 568 from dtxdeal_tmp where t_kind = 0
', 1, 
    'Не задан параметр T_KIND - код сделки', 'Проверка заполнения T_KIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 568 from dtxdeal_tmp where t_amount = 0
', 1, 
    'Не задан параметр T_AMOUNT - количество ценных бумаг', 'Проверка заполнения T_AMOUNT', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 70, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate,  t_dealid, 556 from dtxdeal_tmp where T_PRICE = 0 and T_KIND not in (70,80,90,100,110)
', 1, 
    'Не задан параметр T_PRICE - цена за шт. ценной бумаги, не включая НКД', 'Проверка заполнения T_PRICE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 80, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate,  t_dealid, 557 from dtxdeal_tmp where T_COST = 0
', 1, 
    'Не задан параметр T_COST - стоимость ценных бумаг без НКД', 'Проверка заполнения T_COST', 'X', '');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 90, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 558 from dtxdeal_tmp where T_TOTALCOST = 0 and t_action in (1,2)
', 1, 
    'Не задан параметр T_TOTALCOST - общая сумма сделки вкл. НКД в валюте сделки', 'Проверка заполнения T_TOTALCOST', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 100, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select * from (select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 900 from dtxdeal_tmp sou group by t_dealid, t_instancedate having count(*)>1)', 2, 
    'Проверяем записи на предмет дублирования id внутри t_instancedate, поскольку по ограничениям реализации объект может присутствовать только 1 раз в день', 'Проверка на дублирование id внутри дня', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 110, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 597 from dtxdeal_tmp sou where t_parentid>0 and t_kind not in (10,20)', 1, 
    'Проверяем фиктивные сделки по двжению обеспечения для РЕПО с корзиной. Они могут иметь только 2 типа:  20(ввод обеспечения) или 10(вывод обеспечения)', 'Проверка на корректность типа движения по корзине', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 10, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_action>1) tgt using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.t_dealid)
when matched then update set tgt.tgt_dealid=sou.t_destid', 1, 
    'Обогащение записи - добавление TGT_DEALID из DTXREPLOBJ_DBT', 'Добавление TGT_DEALID (для измененй/удалений)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 20, 'update /*+ # */ dtxdeal_tmp tgt set TGT_BOFFICEKIND =  
                                case T_KIND
                                when 30 then 101
                                when 40 then 101
                                when 70 then 117
                                when 80 then 117
                                when 90 then 117
                                when 100 then 158
                                when 110 then 140
                                else 101
                                end', 1, 
    'Обогащение записи - добавление TGT_BOFFICEKIND', 'Добавление TGT_BOFFICEKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 30, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARENTID is not null) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARENTID)
when matched then update set tgt.TGT_PARENTID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_PARENTID из DTXREPLOBJ_DBT', 'Добавление TGT_PARENTID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 35, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt 
using ddl_tick_dbt sou 
on (sou.t_dealid=tgt.TGT_PARENTID)
when matched then update set tgt.TGT_PARTYID=sou.T_PARTYID', 1, 
    'Обогащение записи - добавление TGT_PARTYID (ID контрагента)', 'Добавление TGT_PARTYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 40, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PAYMCUR is not null) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PAYMCUR)
when matched then update set tgt.TGT_PAYMCUR=sou.t_destid', 1, 
    'Обогащение записи - добавление TGT_PAYMCUR из DTXREPLOBJ_DBT', 'Добавление TGT_PAYMCUR', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 50, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=20 and t_objstate=0) sou 
on (sou.t_objectid=tgt.T_AVOIRISSID)
when matched then update set tgt.TGT_AVOIRISSID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_AVOIRISSID из DTXREPLOBJ_DBT', 'Добавление TGT_AVOIRISSID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 60, 'update /*+ # */ dtxdeal_tmp set 
TGT_AVOIRISSID = Load_RSS.GetBasketFI,
TGT_DEALKIND = 2139, 
TGT_ISBASKET=chr(88),
T_PAYDATE = T_SUPLDATE,
T_SUPLDATE = T_PAYDATE
where t_replstate=0 and t_avoirissid=-20', 1, 
    'Обогащение записи - добавление TGT_AVOIRISSID для корзины', 'Добавление TGT_AVOIRISSID (2192) для корзины', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 70, 'merge /*+ # */ into dtxdeal_tmp tgt 
using (select t_fiid, t_root from dfininstr_dbt f join davrkinds_dbt k on (f.T_AVOIRKIND=k.T_AVOIRKIND and k.t_fi_kind=2)) sou 
on (sou.t_fiid=tgt.TGT_AVOIRISSID)
when matched then update set tgt.TGT_AVOIRKIND=sou.T_ROOT
', 1, 
    'Обогащение записи - добавление TGT_AVOIRKIND', 'Добавление TGT_AVOIRKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 80, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTYID)
when matched then update set tgt.TGT_PARTYID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_PARTYID из DTXREPLOBJ_DBT', 'Добавление TGT_PARTYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 90, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_CURRENCYID)
when matched then update set tgt.TGT_CURRENCYID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_CURRENCYID из DTXREPLOBJ_DBT', 'Добавление TGT_CURRENCYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 100, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_MARKETID is not null) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID)
when matched then update set tgt.TGT_MARKETID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_MARKETID из DTXREPLOBJ_DBT', 'Добавление TGT_MARKETID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 110, 'update /*+ # */ dtxdeal_tmp d set tgt_brokerid=nvl((select t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0 and t_objectid=d.t_brokerid),-1)
where t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_BROKERID из DTXREPLOBJ_DBT', 'Добавление TGT_BROKERID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 120, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_SECTOR is not null) tgt 
using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=40 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID  and  sou.T_SUBOBJNUM=tgt.T_SECTOR)
when matched then update set tgt.TGT_SECTOR=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_SECTOR из DTXREPLOBJ_DBT', 'Добавление TGT_SECTOR', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 130, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_NKDFIID is not null) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_NKDFIID)
when matched then update set tgt.TGT_NKDFIID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_NKDFIID из DTXREPLOBJ_DBT', 'Добавление TGT_NKDFIID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 140, 'update /*+ # */ dtxdeal_tmp set TGT_REPOBASE = CASE t_repobase when 1 then 1  when 2 then 3  when 3 then 5  when 4 then 2   when 5 then 0   when 6 then 4   else -1  end
 where t_replstate=0 and T_REPOBASE is not null', 1, 
    'Обогащение записи - добавление TGT_REPOBASE', 'Добавление TGT_REPOBASE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 150, 'update /*+ # */ dtxdeal_tmp set TGT_COUNTRY = (select t_codelat3 from dcountry_dbt where t_countryid = T_COUNTRY)
 where t_replstate=0 and T_COUNTRY <> 165', 1, 
    'Обогащение записи - добавление TGT_COUNTRY', 'Добавление TGT_COUNTRY', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 160, 'update /*+ # */ dtxdeal_tmp set TGT_EXISTBACK = chr(88) where t_replstate=0 and T_KIND in (30,40)', 1, 
    'Обогащение записи - добавление TGT_EXISTBACK', 'Добавление TGT_EXISTBACK', null);
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 170, 'update /*+ # */ dtxdeal_tmp set tgt_objtype = case when T_KIND in (70,80,90) then 117 else 101 end where t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_OBJTYPE', 'Добавление TGT_OBJTYPE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 180, 'update /*+ # */ dtxdeal_tmp set tgt_department = LOAD_RSS.GetDepartment /* --nvl(( select t_code from dtxreplobj_dbt ro join ddp_dep_dbt dp on (ro.t_destid=dp.t_partyid and ro.t_objecttype=40 and ro.t_objstate=0)),1) */', 1, 
    'Обогащение записи - добавление TGT_DEPARTMENT', 'Добавление TGT_DEPARTMENT', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 190, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_WARRANTID is not null) tgt 
using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=50 and t_objstate=0) sou on (sou.t_objectid=tgt.T_WARRANTID)
when matched then update set tgt.TGT_WARRANTID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_WARRANTID из DTXREPLOBJ_DBT', 'Добавление TGT_WARRANTID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 200, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARTIALID is not null) tgt 
using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=60 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTIALID)
when matched then update set tgt.TGT_PARTIALID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_PARTIALID из DTXREPLOBJ_DBT', 'Добавление TGT_PARTIALID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 230, 'update /*+ # */ dtxdeal_tmp set 
TGT_ISLOAN = decode( t_kind, 50, CHR(88), 60, CHR(88), CHR(0)),
TGT_ISREPO = case when t_kind in (30,40,50,60) then CHR(88) else chr(0) end
where t_replstate=0 and t_action in (1,2)', 1, 
    'Обогащение записи - добавление TGT_ISREPO / TGT_ISLOAN ', 'Добавление TGT_ISREPO / TGT_ISLOAN', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 240, 'update /*+ # */ dtxdeal_tmp set TGT_WARRANT_NUM = (select t_number from dfiwarnts_dbt where t_id = TGT_WARRANTID) where TGT_WARRANTID is not null', 1, 
    'Обогащение записи - добавление TGT_WARRANT_NUM по TGT_WARRANTID', 'Добавление TGT_WARRANT_NUM по TGT_WARRANTID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 250, 'update /*+ # */ dtxdeal_tmp set TGT_PARTIAL_NUM = (select t_number from dfiwarnts_dbt where t_id = TGT_PARTIALID) where TGT_PARTIALID is not null', 1, 
    'Обогащение записи - добавление TGT_PARTIAL_NUM по TGT_PARTIALID', 'Добавление TGT_PARTIAL_NUM по TGT_PARTIALID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 260, 'update /*+ # */ dtxdeal_tmp tgt set TGT_CURNOM = LOAD_RSS.GetCurrentNom( tgt_avoirissid, t_instancedate )
where t_kind in (70,90)  ', 1, 
    'Обогащение записи - добавление TGT_CURNOM - текущего номинала бумаги', 'Добавление TGT_CURNOM', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 270, 'update /*+ # */ DTXDEAL_TMP set
TGT_MATURITYISPRINCIPAL = case when (TGT_ISLOAN=CHR(88) or T_KIND in (80,90 /*для погашений и ЧП*/ ) or (T_SUPLDATE < T_PAYDATE)) then CHR(88) else CHR(0) end,
tgt_maturity = case when TGT_ISLOAN=CHR(88) then T_SUPLDATE WHEN T_KIND in (80,90) THEN T_BALANCEDATE WHEN T_SUPLDATE < T_PAYDATE THEN T_SUPLDATE ELSE T_PAYDATE end,
tgt_expiry = case when TGT_ISLOAN=CHR(88) then T_SUPLDATE2 WHEN T_KIND in (80,90) THEN T_BALANCEDATE WHEN T_SUPLDATE < T_PAYDATE THEN T_PAYDATE ELSE T_SUPLDATE end,
tgt_ReceiptAmount = case when TGT_ISLOAN=CHR(88) then T_NKD2 end', 1, 
    'Обогащение записи - добавление TGT_MATURITY / TGT_EXPIRY / TGT_RECEIPTAMOUNT / TGT_MATURITYISPRINCIPAL', 'Добавление TGT_MATURITY / TGT_EXPIRY / TGT_RECEIPTAMOUNT / TGT_MATURITYISPRINCIPAL', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 280, 'update /*+ # */ DTXDEAL_TMP set
TGT_MATURITYISPRINCIPAL2 = case when T_SUPLDATE2 < T_PAYDATE2 then CHR(88) else CHR(0) end,
tgt_maturity2 = case when T_SUPLDATE2 < T_PAYDATE2 THEN T_SUPLDATE2 ELSE T_PAYDATE2 end,
tgt_expiry2 = case when T_SUPLDATE2 < T_PAYDATE2 THEN T_PAYDATE2 ELSE T_SUPLDATE2 end
    ', 1, 
    'Обогащение записи - добавление TGT_MATURITY2 / TGT_EXPIRY2 / TGT_MATURITYISPRINCIPAL2', 'Добавление TGT_MATURITY2 / TGT_EXPIRY2 / TGT_MATURITYISPRINCIPAL2', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 290, 'update /*+ # */ DTXDEAL_TMP set
TGT_FORMULA = case T_ACCOUNTTYPE
              WHEN 1 THEN 50 /*DVP*/
              WHEN 2 THEN 49 /*DFP*/
              WHEN 3 THEN 52 /*PP*/
              WHEN 4 THEN 51 /*PD*/ 
              end 
', 1, 
    'Обогащение записи - добавление TGT_FORMULA', 'Добавление TGT_FORMULA', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 300, 'update /*+ # */ DTXDEAL_TMP tgt
set TGT_ISQUOTED = CHR(88) where tgt.tgt_avoirissid in 
(select t_otherfi from dratedef_dbt where t_sincedate > tgt.t_date-30)

', 1, 
    'Обогащение записи - добавление TGT_ISQUOTED', 'Добавление TGT_ISQUOTED', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 305, 'update /*+ # */ DTXDEAL_TMP set 
TGT_ISKSU = decode(TGT_AVOIRKIND, 51, chr(88), chr(0)), -- клиринговый сертификат участия
TGT_ISBOND = decode(TGT_AVOIRKIND, 17, chr(88), chr(0)) -- облигация
', 1, 
    'Обогащение записи - добавление TGT_ISKSU / TGT_ISBOND', 'Добавление TGT_ISKSU / TGT_ISBOND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 310, 'update /*+ # */ DTXDEAL_TMP set
TGT_RELATIVEPRICE = case when TGT_ISBOND=chr(88) and tgt_bofficekind <> 117 then chr(88) else chr(0) end 
', 1, 
    'Обогащение записи - добавление TGT_RELATIVEPRICE', 'Добавление TGT_RELATIVEPRICE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 320, 'update /*+ # */ DTXDEAL_TMP set
TGT_PORTFOLIOID = T_PORTFOLIOID,
TGT_PORTFOLIOID_2 = 5', 1, 
    'Обогащение записи - переопределение портфелей для НЕ займов (TGT_PORTFOLIOID/TGT_PORTFOLIOID_2)', 'Добавление TGT_PORTFOLIOID / TGT_PORTFOLIOID_2', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 330, 'update /*+ # */ DTXDEAL_TMP set
TGT_DEALKIND = Load_RSS.GetDealKind(T_KIND, TGT_MARKETID, TGT_AVOIRISSID, TGT_ISBASKET, TGT_ISKSU) 
where TGT_DEALKIND is null and t_replstate=0
    ', 1, 
    'Обогащение записи - добавление TGT_DEALKIND - вид сделки. Для базовой сделки РЕПО с корзиной он был определен раньше.', 'Добавление TGT_DEALKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 340, 'update /*+ # */ DTXDEAL_TMP set
TGT_PRICE = case when tgt_isbond=chr(88) and tgt_bofficekind=117 and t_kind in (70,90) then tgt_curnom
                 when tgt_isbond=chr(88) and tgt_bofficekind=117 and t_kind = 80 then 0
                 else t_price
            end
    ', 1, 
    'Обогащение записи - добавление TGT_PRICE - он особенный для погашений облигаций и купонов, в остальных случаях совпадает с t_price', 'Добавление TGT_PRICE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 350, 'update /*+ # */ dtxdeal_tmp set TGT_DEALID = ddl_tick_dbt_seq.nextval where t_replstate=0 and t_action=1 and t_parentid is null', 1, 
    'Обогащение записи - добавление TGT_DEALID из последовательности для вставок (не для фиктивных сделок по РЕПО с корзиной, там используется другая последовательность)', 'Добавление TGT_DEALID для вставок', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 360, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_parentid>0) tgt 
using (SELECT t_dealid, t_fiid, sum(t_principal * (CASE t_kind WHEN 0 THEN 1 WHEN 1 THEN -1 END)) cou FROM DDL_TICK_ENS_DBT group by t_dealid, t_fiid ) sou 
on (sou.t_dealid=tgt.TGT_PARENTID and sou.T_FIID = TGT.TGT_AVOIRISSID)
when matched then update set tgt.TGT_BS_AMOUNT=sou.cou', 1, 
    'Обогащение записи для РЕПО с корзиной - добавление TGT_BS_ANOUNT - объхем обеспечения на момент прихода движения по данной бумаге', 'Добавление TGT_BS_ANOUNT (для РЕПО с корзиной)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 380, 'update /*+ # */ DTXDEAL_TMP set TGT_DEALID = ddl_tick_ens_dbt_seq.nextval where t_parentid > 0 and t_action=1 and t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_DEALID для РЕПО с корзиной из последовательности. Это будущий ID в таблице ddl_tick_ens_dbt', 'Добавление TGT_DEALID (для РЕПО с корзиной)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 400, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_parentid>0) tgt 
using (select t_dealid, coalesce( nullif(t_maturity, date''0001-01-01''), nullif(t_expiry, date''0001-01-01'')) retdate from ddl_leg_dbt where t_legkind=2) sou 
on (sou.t_dealid=tgt.TGT_PARENTID)
when matched then update set tgt.TGT_BS_RETDATE=sou.retdate', 1, 
    'Обогащение записи для РЕПО с корзиной - добавление TGT_BS_RETDATE - дата возврата обеспечения', 'Добавление TGT_BS_RETDATE (для РЕПО с корзиной)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 410, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_parentid>0) tgt 
using (select t_dealid, coalesce( nullif(t_maturity, date''0001-01-01''), nullif(t_expiry, date''0001-01-01'')) begdate from ddl_leg_dbt where t_legkind=0) sou 
on (sou.t_dealid=tgt.TGT_PARENTID)
when matched then update set tgt.TGT_BS_BEGDATE=sou.begdate', 1, 
    'Обогащение записи для РЕПО с корзиной - добавление TGT_BS_BEGDATE - дата начальной поставки обеспечения', 'Добавление TGT_BS_BEGDATE (для РЕПО с корзиной)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 420, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_parentid>0) tgt 
using ddl_tick_dbt sou 
on (sou.t_dealid=tgt.TGT_PARENTID)
when matched then update set tgt.TGT_BS_DEALKIND=sou.t_dealtype', 1, 
    'Обогащение записи для РЕПО с корзиной - добавление TGT_BS_DEALKIND - вид базовой сделки (2134 для обратного РЕПО или 2139 для прямого)', 'Добавление TGT_BS_DEALKIND (для РЕПО с корзиной)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 430, 'update /*+ # */ DTXDEAL_TMP set TGT_BS_DIRECTION=
CASE T_KIND WHEN 10 THEN decode(TGT_BS_DEALKIND, 2139, 1, 2134, 0)
            WHEN 20 THEN decode(TGT_BS_DEALKIND, 2139, 0, 2134, 1)
            END
 where t_parentid > 0 and t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_BS_DIRECTION - направление движения обеспечения по сделке РЕПО с корзиной. 0 - ввод, 1 - вывод', 'Добавление TGT_BS_DIRECTION (для РЕПО с корзиной)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 450, 'update /*+ # */ DTXDEAL_TMP set TGT_BS_PAYER=
CASE T_KIND WHEN 10 THEN decode(TGT_BS_DEALKIND, 2139, t_partyid, 2134, 1)
            WHEN 20 THEN decode(TGT_BS_DEALKIND, 2139, 1, 2134, t_partyid)
            END
 where t_parentid > 0 and t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_BS_PAYER - плательщик в этом движении обеспечения. Либо наш банк(1), либо t_dealid из операции', 'Добавление TGT_BS_PAYER (для РЕПО с корзиной)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 460, 'update /*+ # */ DTXDEAL_TMP set TGT_BS_AMOUNT_AFTER = CASE TGT_BS_DIRECTION WHEN 1 THEN tgt_bs_amount-t_amount ELSE tgt_bs_amount-t_amount END  where t_parentid > 0 and t_action=1 and t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_BS_AMOUNT_AFTER - количество бумаг в обеспечении под сделкой РЕПО на корзину после выполнения этой операции', 'Добавление TGT_BS_AMOUNT_AFTER (для РЕПО с корзиной)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 470, 'update /*+ # */ DTXDEAL_TMP set TGT_BS_RQTYPE=
CASE WHEN TGT_BS_RETDATE=T_DATE THEN 8
     WHEN TGT_BS_BEGDATE=T_DATE THEN 8
     ELSE 9
END
 where t_parentid > 0 and t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_BS_RQTYPE - вид платежа по этому движению обеспечения. Либо 9-комп. поставка, либо 8-ввод или вывод обеспечения', 'Добавление TGT_BS_RQTYPE (для РЕПО с корзиной)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 598 from dtxdeal_tmp sou where t_parentid>0 and tgt_parentid is null', 1, 
    'Проверяем наличие базовой сделки по РЕПО с корзиной для этого движения обеспечения', 'Проверка заполнения T_PARENTID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 4, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 599 from dtxdeal_tmp sou where TGT_BS_AMOUNT_AFTER < 0', 1, 
    'Проверяем достаточность обеспечения для вывода (обработка фиктивной сделки для РЕПО на корзину)', 'Проверка достаточности обеспечения (РЕПО с корзиной)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 4, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 422 from dtxdeal_tmp where TGT_DEALID is null and t_action in (2,3)', 1, 
    'Ошибка: не найдена реплицированная сделка для изменения/удаления', 'Проверка наличия сделки (для изменения/удаления)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 601 from dtxdemand_tmp where t_dealid is null', 1, 
    'Не задан параметр T_DEALID - идентификатор сделки', 'Проверка заполнения T_DEALID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 602 from dtxdemand_tmp where t_part is null', 1, 
    'Не задан параметр T_PART - номер части сделки', 'Проверка заполнения T_PART', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 603 from dtxdemand_tmp where t_kind is null', 1, 
    'Не задан параметр T_KIND - вид платежа', 'Проверка заполнения T_KIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 604 from dtxdemand_tmp where t_direction is null', 1, 
    'Не задан параметр t_direction - вид платежа', 'Проверка заполнения T_DIRECTION', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 605 from dtxdemand_tmp where t_fikind is null', 1, 
    'Не задан t_fikind  платежа', 'Проверка заполнения T_FIKIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 606 from dtxdemand_tmp where t_date is null', 1, 
    'Не задано t_date - дата платежа', 'Проверка заполнения T_DATE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 70, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 607 from dtxdemand_tmp where t_sum is null', 1, 
    'Не задано t_sum - сумма платежа', 'Проверка заполнения T_SUM', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 80, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 608 from dtxdemand_tmp where t_state is null', 1, 
    'Не задано t_state - статус платежа', 'Проверка заполнения T_STATE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 90, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 609 from dtxdemand_tmp where t_paycurrencyid is null and t_kind not in (10,60,120)', 1, 
    'Не задано t_paycurrencyid - валюта платежа', 'Проверка заполнения T_PAYCURRENCYID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 100, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select * from (select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_demandid, 900 from dtxdemand_tmp sou group by t_demandid, t_instancedate having count(*)>1)', 2, 
    'Проверяем записи на предмет дублирования id внутри t_instancedate, поскольку по ограничениям реализации объект может присутствовать только 1 раз в день', 'Проверка на дублирование id внутри дня', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 10, 'merge /*+ # */ into (select * from dtxdemand_tmp where t_replstate=0 ) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=90 and t_objstate=0) sou on (sou.t_objectid=tgt.T_DEMANDID)
when matched then update set tgt.TGT_DEMANDID=sou.T_DESTID', 1, 
    'Обогащение записи - поиск TGT_DEMANDID для обновлений/удалений', 'Добавление TGT_DEMANDID для обновлений/удалений (action 2,3)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 12, 'update /*+ # */ dtxdemand_tmp set TGT_DEMANDID=ddlrq_dbt_seq.nextval where t_action=1 and t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_DEMANDID для вставок (action=1)', 'Добавление TGT_DEMANDID для вставок (action=1)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 20, 'merge /*+ # */ into dtxdemand_tmp tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_DEALID)
when matched then update set tgt.TGT_DEALID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_DEALID', 'Добавление TGT_DEALID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 30, 'merge /*+ # */ into (select * from dtxdemand_tmp where t_fikind=20 and t_replstate=0 and t_action in (1,2)) tgt 
using (select t_dealid, t_pfi from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID)
when matched then update set tgt.TGT_FIID=sou.T_PFI', 1, 
    'Обогащение записи - добавление TGT_FIID для платежей бумагами', 'Добавление TGT_FIID для платежей бумагами', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 40, 'merge /*+ # */ into dtxdemand_tmp tgt 
using (select t_dealid, T_DEALTYPE from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID)
when matched then update set tgt.TGT_DEALKIND=sou.T_DEALTYPE', 1, 
    'Обогащение записи - добавление TGT_DEALKIND', 'Добавление TGT_DEALKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 50, 'merge /*+ # */ into (select * from dtxdemand_tmp where t_fikind=10 and t_replstate=0 and t_action in (1,2)) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PAYCURRENCYID)
when matched then update set tgt.TGT_FIID=sou.t_destid', 1, 
    'Обогащение записи - добавление TGT_FIID для платежей деньгами', 'Добавление TGT_FIID для платежей деньгами', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 60, 'merge /*+ # */ into (select * from dtxdemand_tmp) tgt 
   using (select t_dealid, T_BOFFICEKIND from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID) 
   when matched then update set tgt.TGT_BOFFICEKIND=sou.T_BOFFICEKIND', 1, 
    'Обогащение записи - добавление TGT_BOFFICEKIND', 'Добавление TGT_BOFFICEKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 70, 'merge /*+ # */ into (select * from dtxdemand_tmp) tgt 
   using (select t_dealid, T_MARKETID, T_PARTYID from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID) 
   when matched then update set tgt.TGT_PARTY= case when sou.T_MARKETID>0 then T_MARKETID else T_PARTYID end', 1, 
    'Обогащение записи - добавление TGT_PARTY', 'Добавление TGT_PARTY', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 90, 'update /*+ # */ dtxdemand_tmp set TGT_KIND=case when t_direction=1 then 0 else 1 end', 1, 
    'Обогащение записи - добавление TGT_KIND', 'Добавление TGT_KIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 100, 'update /*+ # */ dtxdemand_tmp set TGT_STATE=2, TGT_PLANDATE=date''0001-01-01'', TGT_FACTDATE=T_DATE, TGT_CHANGEDATE=date''0001-01-01'' where t_isfact=chr(88)', 1, 
    'Обогащение записи - добавление TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE для фактических платежей', 'Добавление TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE - 1', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 110, 'update /*+ # */ dtxdemand_tmp set TGT_STATE=0, TGT_PLANDATE=T_DATE, TGT_FACTDATE=date''0001-01-01'', TGT_CHANGEDATE=T_DATE where t_isfact is null', 1, 
    'Обогащение записи - добавление TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE для плановых платежей', 'Добавление TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE - 2', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 120, 'update /*+ # */ dtxdemand_tmp set TGT_TYPE=
case t_kind
    when 10 then 8 -- поставка
    when 20 then 0 -- задаток
    when 30 then 1 -- аванс
    when 40 then 2 -- оплата
    when 50 then 3 -- проценты
    when 60 then 8 -- итоговая поставка
    when 70 then 2 -- итоговая оплата
    when 80 then 4 -- выплата купонного дохода
    when 90 then 5 -- частичное погашение
    when 100 then 10 -- выплата дивидендов
    when 110 then 7  -- компенсационная оплата
    when 120 then 9  -- компенсационная поставка
    else -1
end
', 1, 
    'Обогащение записи - добавление TGT_TYPE', 'Добавление TGT_TYPE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 130, 'update /*+ # */ dtxdemand_tmp set TGT_SUBKIND=case when tgt_type in (8,9) then 1 else 0 end', 1, 
    'Обогащение записи - добавление TGT_SUBKIND', 'Добавление TGT_SUBKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 145, 'update /*+ # */ (select * from dtxdemand_tmp a where t_replstate=0) tgt
set tgt_num = nvl((select max(t_num)+1 from ddlrq_dbt sou where 
tgt.tgt_bofficekind = sou.T_DOCKIND
and tgt.tgt_dealid = sou.T_DOCID 
and tgt.t_part = sou.T_DEALPART 
and tgt.tgt_type = sou.T_TYPE
and tgt.tgt_fiid = sou.T_FIID), 0)', 1, 
    'Обогащение записи - добавление TGT_NUM из ddlrq_dbt', 'Добавление TGT_NUM из ddlrq_dbt', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 611 from dtxdemand_tmp where TGT_DEMANDID is null and t_action in (2,3)', 1, 
    'Ошбка: Не найдено в dtxreplobj реплицированное Т/О по сделке (для обновлений и удалений)', 'Не найдено в dtxreplobj Т/О по сделке (action = 2,3)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 4, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 612 from dtxdemand_tmp where tgt_dealid is null and t_action in (1,2)', 1, 
    'Проверяем наличие реплицированной сделки по платежу в DTXREPLOBJ', 'Проверка наличие сделки в dtxreplobj', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 5, 10, 'merge /*+ # */ into (select rowid ri, a.* from dtxdemand_tmp a where t_replstate=0 and t_action in (1,2)) tgt 
using (select rowid ri, nvl(tgt_num,0) old_num, sum(1) over(partition by tgt_bofficekind,tgt_dealid, t_part, tgt_type,tgt_fiid order by t_demandid) num from dtxdemand_tmp) sou 
on (sou.ri=tgt.ri)
when matched then update set tgt.TGT_NUM=sou.old_num + sou.NUM-1', 1, 
    'Обогащение записи - пересчет TGT_NUM', 'Пересчет TGT_NUM', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 1, 10, 'update /*+ # */ DTXCOMISS_TMP set T_DEALID=nvl(T_DEALID,0), T_TYPE=nvl(T_TYPE,0), T_SUM=nvl(T_SUM,0), T_NDS=nvl(T_NDS,0), T_CURRENCYID=nvl(T_CURRENCYID,-1)', 1, 
    'Форматирование записи - преобразование NVL', 'Форматирование записи - NVL по полям', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 701 from dtxcomiss_tmp where t_dealid = 0', 1, 
    'Ошибка: сделка под комиссией не задана', 'Проверка T_DEALID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 703 from dtxcomiss_tmp where t_currencyid = -1', 1, 
    'Ошибка: валюта комиссиии не задана', 'Проверка T_CURRENCYID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 709 from dtxcomiss_tmp where t_sum = 0', 1, 
    'Ошибка: не задана сумма комиссии', 'Проверка T_SUM - суммы комиссии', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 20, 'merge /*+ # */ into (select * from dtxcomiss_tmp) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=100 and t_objstate=0) sou on (sou.t_objectid=tgt.T_COMISSID)
when matched then update set tgt.TGT_COMISSID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_COMISSID (ID комиссии). Для изменений/удалений, из DTXREPLOBJ_DBT', 'Добавление TGT_COMISSID (для изменений/удалений)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 30, 'merge /*+ # */ into (select * from dtxcomiss_tmp) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_DEALID)
when matched then update set tgt.TGT_DEALID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_DEALID (ID сделки)', 'Добавление TGT_DEALID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 40, 'merge /*+ # */ into (select * from dtxcomiss_tmp) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_CURRENCYID)
when matched then update set tgt.TGT_CURRENCYID=sou.T_DESTID', 1, 
    'Обогащение записи - добавление TGT_CURRENCYID (ID валюты комиссии)', 'Добавление TGT_CURRENCYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 50, 'update /*+ # */ DTXCOMISS_TMP set TGT_COMMCODE = (select t_fi_code || lpad(T_TYPE,2,''0'') from dfininstr_dbt where t_fiid=TGT_CURRENCYID)', 1, 
    'Обогащение записи - добавление TGT_COMMCODE (код комиссии)', 'Добавление TGT_COMMCODE (код комиссии)', 'X');

Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 55, 'update /*+ # */ DTXCOMISS_TMP set TGT_COMMNUMBER = (select t_number from dsfcomiss_dbt where t_code=TGT_COMMCODE)', 1, 
    'Обогащение записи - добавление TGT_COMMNUMBER (номер комиссии)', 'Добавление TGT_COMMNUMBER (номер комиссии)', 'X');


Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 60, 'update /*+ # */ DTXCOMISS_TMP set TGT_NDS = 0
--CASE WHEN t_type in (3,9) THEN  T_SUM-round(T_SUM/118*100, 2) ELSE 0 END', 1, 
    'Обогащение записи - добавление TGT_NDS ', 'Добавление TGT_NDS (отключено)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 65, 'merge /*+ # */ into (select * from dtxcomiss_tmp where t_replstate=0) tgt 
using ddl_tick_dbt sou 
on (sou.t_dealid=tgt.tgt_dealid)
when matched then update set tgt.TGT_PARTYID=sou.t_partyid', 1, 
    'Обогащение записи - добавление TGT_PARTYID (ID контрагента)', 'Добавление TGT_PARTYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 70, 'merge /*+ # */ into (select * from DTXCOMISS_TMP where t_replstate=0) tgt
using dsfcontr_dbt sou on (tgt.tgt_partyid=sou.t_partyid)
when matched then update set tgt.TGT_CONTRACTID = sou.t_id
', 1, 
    'Обогащение записи - добавление TGT_CONTRACTID', 'Добавление TGT_CONTRACTID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 702 from dtxcomiss_tmp where TGT_DEALID is null', 1, 
    'Ошибка: не найдена реплицированная сделка под комиссией', 'Проверка TGT_DEALID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 704 from dtxcomiss_tmp where TGT_CURRENCYID is null', 1, 
    'Ошибка: не найдена реплицированная валюта комиссии', 'Проверка TGT_CURRENCYID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 706 from dtxcomiss_tmp where TGT_COMISSID is null and t_action in (2,3)', 1, 
    'Ошибка: изменяемая/удаляемая комиссия по сделке не существует. Проверка по DTXREPLOBJ_DBT', 'Проверка TGT_COMISSID (изменяемой комиссии нет в системе) - 1', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 706 from (select * from dtxcomiss_tmp where t_replstate=0 and t_action in (2,3)) s 
LEFT JOIN ddlcomis_dbt t on (s.tgt_comissid=t.t_id)
where t.t_id is null
', 1, 
    'Ошибка: изменяемая/удаляемая комиссия по сделке не существует. Проверка по DDLCOMIS_DBT', 'Проверка TGT_COMISSID (изменяемой комиссии нет в системе) - 2', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 707 from dtxcomiss_tmp where TGT_COMISSID is not null and t_action = 1', 1, 
    'Ошибка: добавляемая комиссия уже есть в системе', 'Проверка TGT_COMISSID (добавляемая комиссия уже в системе)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 5, 10, 'update /*+ # */ DTXCOMISS_TMP set TGT_COMISSID=ddlcomis_dbt_seq.nextval where t_action=1 and t_replstate=0', 1, 
    'Обогащение записи - добавление TGT_COMISSID (ID комиссии). Для вставок, из последовательности ddlcomis_dbt_seq', 'Добавление TGT_COMISSID (для изменений/удалений)', 'X');
COMMIT;
