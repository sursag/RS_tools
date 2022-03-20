-- Скрипт создает все вспомогательные структуры
-- Заполнение таблицы ошибок надо перенести в скрипт _fill_query_table.sql, когда тот перестанет так часто меняться


-- удаление старых таблиц
BEGIN
   FOR i in (select * from user_tables where table_name in ('DTX_SESSION_DBT', 'DTX_SESS_DETAIL_DBT', 'DTX_QUERY_DBT', 'DTX_ERROR_DBT', 'DTX_ERRORKINDS_DBT', 'DTX_QUERYLOG_DBT'))
   LOOP
	execute immediate 'DROP TABLE ' || i.table_name;
        dbms_output.put_line('DROP TABLE ' || i.table_name);
   END LOOP;
END;


-- таблица сеансов
CREATE TABLE DTX_SESSION_DBT
(
  T_SESSID     NUMBER(10) GENERATED ALWAYS AS IDENTITY ( START WITH 21 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NOCYCLE CACHE 20 NOORDER NOKEEP NOSCALE) NOT NULL,
  T_STARTDATE  DATE,
  T_ENDDATE    DATE,
  T_USER       VARCHAR2(100 CHAR),
  T_STATUS     CHAR(1 CHAR)
);

-- таблица деталей сессий
CREATE TABLE DTX_SESS_DETAIL_DBT
(
  T_DETAILID      NUMBER(10) GENERATED ALWAYS AS IDENTITY ( START WITH 41 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NOCYCLE CACHE 20 NOORDER NOKEEP NOSCALE) NOT NULL,
  T_SESSID        NUMBER(5),
  T_PROCEDURE     VARCHAR2(128 CHAR),
  T_STARTDATE     DATE,
  T_ENDDATE       DATE,
  T_DURATION      NUMBER(5),
  T_RESULT        CHAR(1 CHAR),
  T_TOTALROWS     NUMBER(10),
  T_ERRORS        NUMBER(10)
);


-- таблица запросов
CREATE TABLE DTX_QUERY_DBT
(
  T_QUERYID    NUMBER(5) GENERATED ALWAYS AS IDENTITY ( START WITH 61 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NOCYCLE CACHE 20 NOORDER NOKEEP NOSCALE) NOT NULL,
  T_OBJECTTYPE  NUMBER(5),
  T_SET         NUMBER(1),
  T_NUM         NUMBER(5),
  T_TEXT        VARCHAR2(2048 CHAR),
  T_SEVERITY    NUMBER(2),
  T_DESC        VARCHAR2(2048 BYTE),
  T_NAME        VARCHAR2(200 BYTE),
  T_USE_BIND    CHAR(1 CHAR),
  T_IN_USE      CHAR(1 CHAR)                    DEFAULT chr(88)
);

CREATE UNIQUE INDEX DTX_QUERY_DBT_ID1 ON DTX_QUERY_DBT (T_OBJECTTYPE,T_SET, T_NUM);


-- таблица инстансов ошибок
CREATE TABLE DTX_ERROR_DBT
(
  T_ID          NUMBER GENERATED ALWAYS AS IDENTITY NOT NULL,
  T_SESSID      NUMBER(10),
  T_INSTANCEDATE DATE,
  T_DETAILID    NUMBER(10),
  T_OBJECTTYPE  NUMBER(5),
  T_OBJECTID    NUMBER(15),
  T_QUERYID     NUMBER(5),
  T_ERRORCODE   NUMBER(4),
  T_TIMESTAMP   DATE,
  IS_LOGGED     CHAR(1 CHAR),
  T_SEVERITY    NUMBER(1)
)
LOGGING 
MONITORING;



CREATE TABLE dtx_errorkinds_dbt (t_code number(4) primary key, t_desc varchar2(1024 char));



CREATE TABLE DTX_QUERYLOG_DBT
(
  T_SESSION     NUMBER(10),
  T_SESSDETAIL  NUMBER(10),
  T_STARTTIME   DATE,
  T_DURATION    NUMBER(5),
  T_TEXT        VARCHAR2(2000 CHAR),  
  T_OBJECTTYPE   NUMBER(3),
  T_SET         NUMBER(5),
  T_NUM         NUMBER(5),
  T_RESULT      CHAR(1 CHAR),
  T_TOTALROWS   NUMBER(10),
  T_EXECROWS    NUMBER(10),
  T_ID          NUMBER GENERATED ALWAYS AS IDENTITY NOT NULL
);


alter sequence ddl_tick_dbt_seq cache 200;
alter sequence ddl_leg_dbt_seq cache 200;
alter sequence ddlrq_dbt_seq cache 200;
alter sequence ddlcomis_dbt_seq  cache 200;
alter sequence dnotetext_dbt_seq cache 400;
alter sequence dobjatcor_dbt_seq cache 400;
alter sequence dsfcontr_dbt_seq  cache 200;
alter sequence dspground_dbt_seq cache 200;
alter sequence DTXLOADLOG_DBT_SEQ cache 200;



create or replace view v_log as
select t_starttime start_time, trunc(t_duration/60)||':'||lpad(mod(t_duration,60),2,'0') Exec_Time, t_text,  t_objecttype, t_set, t_num, T_EXECROWS, 
trunc(by_procedure/60)||':'||lpad(mod(by_procedure,60),2,'0') Time_by_procedure,
trunc(total/60)||':'||lpad(mod(total,60),2,'0') Total_Time from (
select a.*, 
sum(t_duration) over(partition by t_sessdetail) by_procedure,
sum(t_duration) over() total from DTX_QUERYLOG_DBT a where t_session=(select max(t_session) from DTX_QUERYLOG_DBT )
)  
order by t_id desc;



create or replace view v_err as
select T_TIMESTAMP, T_SEVERITY, T_OBJECTTYPE, T_OBJECTID, T_QUERYID, IS_LOGGED, T_ERRORCODE, T_DESC 
from dtx_error_dbt left join dtx_errorkinds_dbt on (T_ERRORCODE=t_code)
where t_sessid=(select max(t_sessid) from dtx_error_dbt) order by t_id;




Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (997, 'Ошибка: для операции погашения не найдено ЧП (T_PARTIALID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (900, 'Предупреждение: объекты дублировались в пределах t_instancedate');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (710, 'Ошибка: некорректный тип комиссии');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (709, 'Ошибка: не задана сумма комиссии');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (708, 'Ошибка: комиссия находится в режиме ручного редактирования');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (707, 'Ошибка: добавляемая комиссия уже есть в системе');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (706, 'Ошибка: изменяемая/удаляемая комиссия по сделке не существует');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (705, 'Ошибка: не найден получатель комисии');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (704, 'Ошибка: не найдена реплицированная валюта комиссии');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (703, 'Ошибка: не указана валюта комиссии');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (702, 'Ошибка: не найдена реплицированная сделка под комиссией');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (701, 'Ошибка: не указана сделка для комиссии');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (700, 'Ошибка: не найден договор обслуживания');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (651, 'Ошибка: не найдена запись с условиями второй части сделки (ddl_leg_dbt)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (650, 'Ошибка: не найдена запись с условиями сделки (ddl_leg_dbt)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (612, 'Ошибка: Нет реплицированной сделки для Т/О');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (611, 'Ошибка: Не найдено в dtx_replobj реплицированное Т/О по сделке');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (610, 'Ошибка: Т/О по сделке находится в режиме ручного редактирования');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (609, 'Ошибка: Не задано t_paycurrencyid - валюта платежа');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (608, 'Ошибка: Не задано t_state - статус платежа');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (607, 'Ошибка: Не задано t_sum - сумма платежа');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (606, 'Ошибка: Не задано t_date - дата платежа');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (605, 'Ошибка: Не задан t_fikind  платежа');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (604, 'Ошибка: Не задан t_direction - направление платежа');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (603, 'Ошибка: Не задан t_kind платежа');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (602, 'Ошибка: Не задан номер части сделки для платежа');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (601, 'Ошибка: Не задан номер сделки для платежа');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (599, 'Ошибка: недостаточно обеспечения для вывода');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (598, 'Ошибка: не найдена базовая сделка РЕПО с корзиной для операции движения обеспечения');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (597, 'Ошибка: некорректный тип операции для обеспечения по сделке с корзиной');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (596, 'Ошибка: не найдена сделка РЕПО на корзину (T_PARENTID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (568, 'Ошибка: не задан параметр T_KIND - вид сделки');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (567, 'Ошибка: не задан параметр T_PAYDATE2 - плановая дата оплаты в сделках');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (566, 'Ошибка: не задан параметр T_SUPLDATE2 - плановая дата поставки в сделках');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (565, 'Ошибка: не задан параметр T_TOTALCOST2 - общая сумма сделки вкл. НКД в валюте сделки ао 2-ой части РЕПО');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (564, 'Ошибка: не задан параметр T_COST2 - стоимость ценных бумаг без НКД по 2-ой части РЕПО');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (563, 'Ошибка: не задан параметр T_PRICE2 - цена за шт. ценной бумаги, не включая НКД по 2-ой части РЕПО');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (562, 'Ошибка: не задан параметр T_PAYDATE - плановая дата оплаты в сделках');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (561, 'Ошибка: не задан параметр T_SUPLDATE - плановая дата поставки в сделках');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (560, 'Ошибка: не задан параметр T_SUPLDATE2 - плановая дата передачи/возврата в займе');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (559, 'Ошибка: не задан параметр T_SUPLDATE - плановая дата передачи/возврата в займе');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (558, 'Ошибка: не задан параметр T_TOTALCOST - общая сумма сделки вкл. НКД в валюте сделки');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (557, 'Ошибка: не задан параметр T_COST - стоимость ценных бумаг без НКД');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (556, 'Ошибка: не задан параметр T_PRICE - цена за шт. ценной бумаги, не включая НКД');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (555, 'Ошибка: не найдена валюта НКД (T_NKDFIID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (554, 'Ошибка: не найдена валюта сделки (T_CURRENCYID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (553, 'Ошибка: не задан параметр T_AMOUNT - количество ценных бумаг');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (552, 'Ошибка: не найдена ценная бумага (T_AVOIRISSID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (551, 'Ошибка: для операции погашения не найдено загруженное ЧП (T_PARTIALID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (550, 'Ошибка: для операции погашения не задано ЧП (T_PARTIALID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (549, 'Ошибка: для операции погашения не найден загруженный купон (T_WARRANTID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (548, 'Ошибка: для операции погашения не задан купон (T_WARRANTID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (547, 'Ошибка: для внебиржевой сделки не найден контрагент, параметр T_PARTYID');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (545, 'Ошибка: неверно задан параметр T_BROKERID, наш банк не может быть брокером');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (544, 'Ошибка: не найден субъект T_BROKERID');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (543, 'Ошибка: не найден сектор биржи');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (542, 'Ошибка: неверно задан параметр T_MARKETID, наш банк не может быть биржой');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (539, 'Ошибка: не задан параметр T_CODE - код сделки');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (534, 'Ошибка: не найден субъект T_MARKETID');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (527, 'Предупреждение: техническая сделка в систему не реплицируется');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (525, 'Не найден контрагент по внебиржевой сделке');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (502, 'Предупреждение: стоимость по второй части сделки (T_COST2) не равна произведению цены (T_PRICE2) на количество бумаг (T_AMOUNT)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (501, 'Предупреждение: стоимость (T_COST) не равна произведению цены (T_PRICE) на количество бумаг (T_AMOUNT)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (500, 'Предупреждение: стоимость (T_COST) не равна произведению цены (T_PRICE) в валюте на количество бумаг (T_AMOUNT)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (423, 'Ошибка: сделка уже удалена в целевой системе');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (422, 'Ошибка: сделка не найдена в целевой системе');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (421, 'Ошибка: сделка уже существует в целевой системе');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (341, 'Ошибка: уже существует значение курса за такую дату');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (340, 'Ошибка: Не существует значения такого курса за дату');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (333, 'Ошибка: уже существует номинал за такую дату');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (332, 'Ошибка: изменяемый/удаляемый номинал отсутствует в системе');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (331, 'Ошибка: изменяемый/удаляемый номинал не реплицирован в целевую систему');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (330, 'Ошибка: значение номинала указано не в валюте номинала');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (322, 'Ошибка: невозможно удалить несуществующий курс');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (321, 'Ошибка: невозможно обновить несуществующий курс');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (320, 'Ошибка: уже существует данный курс за такую дату');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (311, 'Ошибка: котируемый финансовый инструмент не найден в replobj_dbt');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (310, 'Ошибка: для курса не задан котируемый финансовый инструмент');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (306, 'Ошибка: базовый финансовый инструмент не найден в replobj_dbt');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (305, 'Ошибка: для курса не задан базовый финансовый инструмент');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (304, 'Ошибка: для курса не задано значение');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (303, 'Ошибка: для курса не задан тип');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (302, 'Ошибка: торговая площадка не найдена в replobj_dbt');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (301, 'Ошибка: для курса не задана торговая площадка ');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (300, 'Ошибка: в буферной схеме отсутствует удаляемый курс');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (206, 'Ошибка: объект находится в режиме ручного редактирования');
COMMIT;
