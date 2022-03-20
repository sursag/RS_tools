-- ������ ������� ��� ��������������� ���������
-- ���������� ������� ������ ���� ��������� � ������ _fill_query_table.sql, ����� ��� ���������� ��� ����� ��������


-- �������� ������ ������
BEGIN
   FOR i in (select * from user_tables where table_name in ('DTX_SESSION_DBT', 'DTX_SESS_DETAIL_DBT', 'DTX_QUERY_DBT', 'DTX_ERROR_DBT', 'DTX_ERRORKINDS_DBT', 'DTX_QUERYLOG_DBT'))
   LOOP
	execute immediate 'DROP TABLE ' || i.table_name;
        dbms_output.put_line('DROP TABLE ' || i.table_name);
   END LOOP;
END;


-- ������� �������
CREATE TABLE DTX_SESSION_DBT
(
  T_SESSID     NUMBER(10) GENERATED ALWAYS AS IDENTITY ( START WITH 21 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NOCYCLE CACHE 20 NOORDER NOKEEP NOSCALE) NOT NULL,
  T_STARTDATE  DATE,
  T_ENDDATE    DATE,
  T_USER       VARCHAR2(100 CHAR),
  T_STATUS     CHAR(1 CHAR)
);

-- ������� ������� ������
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


-- ������� ��������
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


-- ������� ��������� ������
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
   (997, '������: ��� �������� ��������� �� ������� �� (T_PARTIALID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (900, '��������������: ������� ������������� � �������� t_instancedate');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (710, '������: ������������ ��� ��������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (709, '������: �� ������ ����� ��������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (708, '������: �������� ��������� � ������ ������� ��������������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (707, '������: ����������� �������� ��� ���� � �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (706, '������: ����������/��������� �������� �� ������ �� ����������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (705, '������: �� ������ ���������� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (704, '������: �� ������� ��������������� ������ ��������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (703, '������: �� ������� ������ ��������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (702, '������: �� ������� ��������������� ������ ��� ���������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (701, '������: �� ������� ������ ��� ��������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (700, '������: �� ������ ������� ������������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (651, '������: �� ������� ������ � ��������� ������ ����� ������ (ddl_leg_dbt)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (650, '������: �� ������� ������ � ��������� ������ (ddl_leg_dbt)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (612, '������: ��� ��������������� ������ ��� �/�');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (611, '������: �� ������� � dtx_replobj ��������������� �/� �� ������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (610, '������: �/� �� ������ ��������� � ������ ������� ��������������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (609, '������: �� ������ t_paycurrencyid - ������ �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (608, '������: �� ������ t_state - ������ �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (607, '������: �� ������ t_sum - ����� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (606, '������: �� ������ t_date - ���� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (605, '������: �� ����� t_fikind  �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (604, '������: �� ����� t_direction - ����������� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (603, '������: �� ����� t_kind �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (602, '������: �� ����� ����� ����� ������ ��� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (601, '������: �� ����� ����� ������ ��� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (599, '������: ������������ ����������� ��� ������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (598, '������: �� ������� ������� ������ ���� � �������� ��� �������� �������� �����������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (597, '������: ������������ ��� �������� ��� ����������� �� ������ � ��������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (596, '������: �� ������� ������ ���� �� ������� (T_PARENTID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (568, '������: �� ����� �������� T_KIND - ��� ������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (567, '������: �� ����� �������� T_PAYDATE2 - �������� ���� ������ � �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (566, '������: �� ����� �������� T_SUPLDATE2 - �������� ���� �������� � �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (565, '������: �� ����� �������� T_TOTALCOST2 - ����� ����� ������ ���. ��� � ������ ������ �� 2-�� ����� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (564, '������: �� ����� �������� T_COST2 - ��������� ������ ����� ��� ��� �� 2-�� ����� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (563, '������: �� ����� �������� T_PRICE2 - ���� �� ��. ������ ������, �� ������� ��� �� 2-�� ����� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (562, '������: �� ����� �������� T_PAYDATE - �������� ���� ������ � �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (561, '������: �� ����� �������� T_SUPLDATE - �������� ���� �������� � �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (560, '������: �� ����� �������� T_SUPLDATE2 - �������� ���� ��������/�������� � �����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (559, '������: �� ����� �������� T_SUPLDATE - �������� ���� ��������/�������� � �����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (558, '������: �� ����� �������� T_TOTALCOST - ����� ����� ������ ���. ��� � ������ ������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (557, '������: �� ����� �������� T_COST - ��������� ������ ����� ��� ���');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (556, '������: �� ����� �������� T_PRICE - ���� �� ��. ������ ������, �� ������� ���');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (555, '������: �� ������� ������ ��� (T_NKDFIID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (554, '������: �� ������� ������ ������ (T_CURRENCYID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (553, '������: �� ����� �������� T_AMOUNT - ���������� ������ �����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (552, '������: �� ������� ������ ������ (T_AVOIRISSID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (551, '������: ��� �������� ��������� �� ������� ����������� �� (T_PARTIALID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (550, '������: ��� �������� ��������� �� ������ �� (T_PARTIALID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (549, '������: ��� �������� ��������� �� ������ ����������� ����� (T_WARRANTID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (548, '������: ��� �������� ��������� �� ����� ����� (T_WARRANTID)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (547, '������: ��� ����������� ������ �� ������ ����������, �������� T_PARTYID');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (545, '������: ������� ����� �������� T_BROKERID, ��� ���� �� ����� ���� ��������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (544, '������: �� ������ ������� T_BROKERID');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (543, '������: �� ������ ������ �����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (542, '������: ������� ����� �������� T_MARKETID, ��� ���� �� ����� ���� ������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (539, '������: �� ����� �������� T_CODE - ��� ������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (534, '������: �� ������ ������� T_MARKETID');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (527, '��������������: ����������� ������ � ������� �� �������������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (525, '�� ������ ���������� �� ����������� ������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (502, '��������������: ��������� �� ������ ����� ������ (T_COST2) �� ����� ������������ ���� (T_PRICE2) �� ���������� ����� (T_AMOUNT)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (501, '��������������: ��������� (T_COST) �� ����� ������������ ���� (T_PRICE) �� ���������� ����� (T_AMOUNT)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (500, '��������������: ��������� (T_COST) �� ����� ������������ ���� (T_PRICE) � ������ �� ���������� ����� (T_AMOUNT)');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (423, '������: ������ ��� ������� � ������� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (422, '������: ������ �� ������� � ������� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (421, '������: ������ ��� ���������� � ������� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (341, '������: ��� ���������� �������� ����� �� ����� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (340, '������: �� ���������� �������� ������ ����� �� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (333, '������: ��� ���������� ������� �� ����� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (332, '������: ����������/��������� ������� ����������� � �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (331, '������: ����������/��������� ������� �� ������������ � ������� �������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (330, '������: �������� �������� ������� �� � ������ ��������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (322, '������: ���������� ������� �������������� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (321, '������: ���������� �������� �������������� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (320, '������: ��� ���������� ������ ���� �� ����� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (311, '������: ���������� ���������� ���������� �� ������ � replobj_dbt');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (310, '������: ��� ����� �� ����� ���������� ���������� ����������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (306, '������: ������� ���������� ���������� �� ������ � replobj_dbt');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (305, '������: ��� ����� �� ����� ������� ���������� ����������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (304, '������: ��� ����� �� ������ ��������');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (303, '������: ��� ����� �� ����� ���');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (302, '������: �������� �������� �� ������� � replobj_dbt');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (301, '������: ��� ����� �� ������ �������� �������� ');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (300, '������: � �������� ����� ����������� ��������� ����');
Insert into DTX_ERRORKINDS_DBT
   (T_CODE, T_DESC)
 Values
   (206, '������: ������ ��������� � ������ ������� ��������������');
COMMIT;
