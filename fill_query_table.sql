SET DEFINE OFF;

delete DTX_QUERY_DBT;


Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 1, 10, 'update /*+ # */ DTXCOURSE_TMP set
T_TYPE=nvl(T_TYPE,0), T_BASEFIID=nvl(T_BASEFIID,0), T_FIID=nvl(T_FIID,0), T_MARKETID=nvl(T_MARKETID,0), T_MARKETSECTORID=nvl(T_MARKETSECTORID,0), T_POINT=nvl(T_POINT,0), T_SCALE=nvl(T_SCALE,0)    
', 1, 
    '��������� ������ NVL', '��������� ������� NVL', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 303 from dtxcourse_tmp where t_type=0 and t_action in (1,2)', 1, 
    '������: ��� ����� �� ����� ���', '�������� ���� �����', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 304 from dtxcourse_tmp where t_rate=0 and t_action in (1,2)', 1, 
    '������: ��� ����� �� ������ ��������', '�������� �������� �����', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 301 from dtxcourse_tmp where T_MARKETID=0 and t_action in (1,2)', 1, 
    '������: ��� ����� �� ������ �������� ��������', '�������� �������� �������� �����', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 305 from dtxcourse_tmp where T_BASEFIID=0 and t_action in (1,2)', 1, 
    '������: ��� ����� �� ����� ������� ���������� ����������', '�������� �������� ����������� �����������', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 310 from dtxcourse_tmp where T_BASEFIID=0 and t_action in (1,2)', 1, 
    '������: ��� ����� �� ����� ���������� ���������� ����������', '�������� ����������� ����������� �����������', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 2, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select * from (select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 900 from dtxcourse_tmp sou group by t_courseid, t_instancedate having count(*)>1)', 2, 
    '��������� ������ �� ������� ����������� id ������ t_instancedate, ��������� �� ������������ ���������� ������ ����� �������������� ������ 1 ��� � ����', '�������� �� ������������ id ������ ���', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 10, 'merge /*+ # */ into dtxcourse_tmp tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID)
when matched then update set tgt.TGT_MARKETID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_MARKETID', '���������� TGT_MARKETID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 15, 'update /*+ # */ DTXCOURSE_TMP set TGT_ISNOMINAL=chr(88) where t_type=100     
', 1, 
    '���������� ������ - ���������� TGT_ISNOMINAL', '���������� TGT_ISNOMINAL', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 20, 'merge /*+ # */ into dtxcourse_tmp tgt 
using (select t_objectid, t_subobjnum, t_destid from dtxreplobj_dbt where t_objecttype=40 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID and sou.t_subobjnum=tgt.T_MARKETSECTORID)
when matched then update set tgt.TGT_SECTORID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_SECTORID', '���������� TGT_SECTORID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 30, 'merge /*+ # */ into dtxcourse_tmp tgt 
using (select t_objecttype, t_objectid, t_destid from dtxreplobj_dbt where t_objstate<>2) sou on (sou.t_objectid=tgt.T_BASEFIID and sou.t_objecttype=tgt.t_basefikind)
when matched then update set tgt.TGT_BASEFIID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_BASEFIID', '���������� TGT_BASEFIID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 35, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_basefikind=20) tgt 
using (select * from dfininstr_dbt) sou on (sou.t_fiid=tgt.TGT_BASEFIID)
when matched then update set tgt.TGT_FACEVALUE_FIID=sou.T_FACEVALUEFI', 1, 
    '���������� ������ - ���������� TGT_FACEVALUE_FIID (���� ������� ���������� - ������)', '���������� TGT_FACEVALUE_FIID (��� �����)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 40, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_basefikind=10) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_FIID)
when matched then update set tgt.TGT_FIID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_FIID (������)', '���������� TGT_FIID (������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 50, 'update /*+ # */ dtxcourse_tmp set TGT_FIID=TGT_FACEVALUE_FIID where t_basefikind=20', 1, 
    '���������� ������ - ���������� TGT_FIID (������)', '���������� TGT_FIID (������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 55, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_basefikind=20) tgt 
using (select t_fiid, t_root from dfininstr_dbt f join davrkinds_dbt k on (f.T_AVOIRKIND=k.T_AVOIRKIND and f.T_FI_KIND=k.T_FI_KIND)) sou 
on (sou.t_fiid=tgt.TGT_BASEFIID)
when matched then update set tgt.TGT_BASEFIKIND=sou.T_ROOT
', 1, 
    '���������� ������ - ���������� TGT_BASEFIKIND', '���������� TGT_BASEFIKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 60, 'update /*+ # */ DTXCOURSE_TMP set TGT_ISDOMINANT=chr(88) where t_type=6 and t_basefikind=10     
', 1, 
    '���������� ������ - ���������� TGT_ISDOMINANT', '���������� TGT_ISDOMINANT', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 70, 'update /*+ # */ DTXCOURSE_TMP set TGT_TYPE=load_rss.getratetype(t_type)     
', 1, 
    '���������� ������ - ���������� TGT_TYPE', '���������� TGT_TYPE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 90, 'update /*+ # */ DTXCOURSE_TMP set TGT_ISRELATIVE=chr(88) where TGT_BASEFIKIND=17 and T_TYPE<>15', 1, 
    '���������� ������ - ���������� TGT_ISRELATIVE,���� ������-��������� � ��� ����� - �� ��� �� ����', '���������� TGT_ISRELATIVE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 100, 'merge /*+ # */ into (select * from dtxcourse_tmp where tgt_isnominal is null and tgt_isnominal is null) tgt 
using (select * from dratedef_dbt) sou on (sou.T_OTHERFI=TGT_BASEFIID and sou.t_fiid=TGT_FIID and sou.t_type=TGT_TYPE and sou.t_market_place=TGT_MARKETID)
when matched then update set tgt.TGT_RATEID=sou.T_RATEID, tgt.TGT_LAST_DATE_RSS=sou.T_SINCEDATE', 1, 
    '���������� ������ - ���������� TGT_RATEID / TGT_LAST_DATE_RSS (����� ������� �� ��������� ��������)', '���������� TGT_RATEID / TGT_LAST_DATE_RSS (����� ��������� ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 103, 'merge /*+ # */ into (select * from dtxcourse_tmp where t_replstate=0 and tgt_isnominal = chr(88)) tgt 
using (select * from dtxreplobj_dbt where t_objecttype=70 and t_objstate<2) sou 
on (sou.t_objectid = tgt.t_courseid) 
when matched then update set tgt.TGT_RATEID=sou.t_destid', 1, 
    '���������� ������ - ���������� TGT_RATEID (��� ������� ��������� ��������)', '���������� TGT_RATEID (��� ��������� ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 105, 'merge /*+ # */ into (select * from DTXCOURSE_TMP where tgt_isnominal is null and t_replstate=0) tgt 
using  (select t_basefiid, t_fiid, t_type, t_marketid, T_MARKETSECTORID, max(t_ratedate) mdate from DTXCOURSE_TMP where tgt_isnominal is null group by t_basefiid, t_fiid, t_type, t_marketid, t_marketsectorid) sou
on (sou.t_basefiid=tgt.t_basefiid and sou.t_fiid=tgt.t_fiid and sou.t_type=tgt.t_type and sou.t_marketid=tgt.t_marketid and sou.T_MARKETSECTORID=tgt.T_MARKETSECTORID)
when matched then update set TGT_LAST_DATE_DTX = sou.mdate ', 1, 
    '���������� ������ - ���������� TGT_LAST_DATE_DTX - ������������ ���� �� ����� ����� ����������� � DTXCOURSE_TMP', '���������� TGT_LAST_DATE_DTX (��������� ���� �� ����� � DTXCOURSE)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 110, 'update /*+ # */ dtxcourse_tmp tgt  set TGT_ISLASTDATE=chr(88) 
where t_ratedate = TGT_LAST_DATE_DTX 
and (TGT_LAST_DATE_DTX > TGT_LAST_DATE_RSS or TGT_LAST_DATE_RSS is null)  
and tgt_isnominal is null', 1, 
    '���������� ������ - ���������� TGT_ISLASTDATE - ������� ����, ��� ��� �������� ��������� DRATEDEF � �� ���� ���������� ��� � DRATEHIST', '���������� TGT_ISLASTDATE (����, ��� ��� �������� ��� � dratedef)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 120, 'update /*+ # */ dtxcourse_tmp set tgt_newrateid=dratedef_dbt_seq.nextval 
where TGT_RATEID is null and TGT_ISLASTDATE = chr(88) and t_action=1
', 1, 
    '��� ������ ����� ��������� tgt_newrateid, ������� ����� ������ � DRATEDEF_DBT.T_RATEID. ������� ��������� ��� ���������� �������� ����� (TGT_ISLASTDATE)', '��� ������ ����� ��������� TGT_NEWRATEID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 125, 'update /*+ # */ DTXCOURSE_TMP set TGT_RATEID = DFIVLHIST_DBT_seq.nextval
 where t_replstate=0 and tgt_isnominal = chr(88)', 1, 
    '���������� ������ - ���������� TGT_RATEID ��� ������� ��������� �������� (t_action=1)', '���������� TGT_RATEID (��� ������� ��������� ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 130, 'merge /*+ # */ into (select * from dtxcourse_tmp where TGT_RATEID is null and T_ACTION=1 and TGT_ISLASTDATE is null and TGT_ISNOMINAL is null) tgt
using (select * from dtxcourse_tmp where TGT_NEWRATEID is not null) sou
on (sou.t_basefiid = tgt.t_basefiid and sou.t_type = tgt.t_type and sou.t_fiid = tgt.t_fiid and sou.T_MARKETID = tgt.T_MARKETID and sou.T_MARKETSECTORID = tgt.T_MARKETSECTORID) 
when matched then update set tgt.tgt_newrateid = sou.tgt_newrateid
', 1, 
    '�������������� ����� TGT_NEWRATEID ����� ��������� �������� �����', '�������������� ����� TGT_NEWRATEID ����� �������� �����', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (70, 3, 140, 'update /*+ # */ DTXCOURSE_TMP set TGT_RATE = T_RATE * power(10, t_point)', 1, 
    '���������� ������ - ���������� TGT_RATE', '���������� TGT_RATE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 302 from dtxcourse_tmp where TGT_MARKETID is null', 1, 
    '������: �������� �������� �� ������� � replobj_dbt', '�������� �������� �������� � RSS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 306 from dtxcourse_tmp where TGT_BASEFIID is null', 1, 
    '������: ������� ����������� ���������� �� ������ � replobj_dbt', '�������� �������� ��������������� � RSS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 311 from dtxcourse_tmp where TGT_FIID is null', 1, 
    '������: ���������� ����������� ���������� �� ������ � replobj_dbt', '�������� ����������� ��������������� � RSS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 321 from dtxcourse_tmp where TGT_RATEID is null and t_action=2', 1, 
    '������: ���� �� ������ � ������� �������', '�������� ������� ����� � RSS ��� ����������', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 322 from dtxcourse_tmp where TGT_RATEID is null and t_action=3', 1, 
    '������: ���� �� ������ � ������� ������� (��� ��������)', '�������� ������� ����� � RSS ��� ��������', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 70, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 340 from dtxcourse_tmp sou left join dratehist_dbt h 
on (sou.tgt_rateid=h.t_rateid and sou.t_ratedate=h.t_sincedate)
where (h.T_RATEID is null and nvl(sou.TGT_LAST_DATE_RSS,date''0001-01-01'')<>t_ratedate and t_action in (2,3))', 1, 
    '��� ���������/�������� ���������, ���� �� ����� ���� �� ����� ���� � DRATEHIST_DBT ��� � DRATEDEF_DBT(������ �� ��� ������� � TGT_LAST_DATE_RSS) ', '�������� ������� �������� ����� � RSS ��� ���������/��������', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (70, 4, 80, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_courseid, 340 from dtxcourse_tmp sou left join dratehist_dbt h on (sou.tgt_rateid=h.t_rateid and sou.t_ratedate=h.t_sincedate)
where (h.T_RATEID > 0 or sou.TGT_LAST_DATE_RSS=t_ratedate and t_action = 1)', 1, 
    '��� ������� ���������, ���� �� ��� ����� ���� �� ����� ���� � DRATEHIST_DBT ��� � DRATEDEF_DBT(������ �� ��� ������� � TGT_LAST_DATE_RSS) ', '�������� ������� �������� ����� � RSS ��� ������� (action=1)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 1, 10, 'update /*+ # */ dtxdeal_tmp set T_EXTCODE = trim(T_EXTCODE), T_MARKETCODE = trim(T_MARKETCODE), 
T_PARTYCODE = trim(T_PARTYCODE), T_CODE = trim(T_CODE), t_conditions=trim(t_conditions),
T_CONTRNUM=trim(T_CONTRNUM), T_DOPCONTROL_NOTE=trim(T_DOPCONTROL_NOTE), T_PRICE_CALC_MET_NOTE=trim(T_PRICE_CALC_MET_NOTE)', 1, 
    '�������������� ������ - TRIM ��������� �����', 'TRIM ��������� �����', 'X');
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
    '�������������� ������ - NVL �����', '��������� ����� NVL', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 1, 30, 'update /*+ # */ DTXDEAL_TMP set T_TIME=DATE''0001-01-01'' + (T_TIME-trunc(T_TIME))', 1, 
    '�������������� ������ - �������������� ���� T_TIME', '�������������� ������ - ���� T_TIME', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate, dtxdeal_tmp.t_dealid, 
case    when ro.t_objstate=2 then 423 -- ������ ��� ������� � ������� �������
    when tick.t_dealid is null then 422 -- ��� ������ ������
    when leg.t_dealid is null then 422 -- ��� ������� ������� ������
    when ro.t_destid is null  then 422 -- ������ � ����� dealid �� ���������������
end T_ERRORCODE 
from dtxdeal_tmp left join dtxreplobj_dbt ro  on (ro.t_objecttype=80 and dtxdeal_tmp.t_dealid=ro.t_objectid)
left join ddl_tick_dbt tick on (ro.t_destid = tick.t_dealid) 
left join ddl_leg_dbt leg on (ro.t_destid = leg.t_dealid and leg.t_legkind=0)
where dtxdeal_tmp.t_action in (2,3) and dtxdeal_tmp.t_parentid>0 and (ro.t_destid is null or tick.t_dealid is null or leg.t_dealid is null)
', 1, 
    '������ ����������� ������ ��� �������� ����������/��������. ���������, ���� �� � ������� ������� ��������������� ������ � ������ ID. ������������ ����� ��������� ������� - ���������� ������ � �������, � leg, ���������� � replobj � ������ ����� ���������. �����, ����� ������� �� ��������� ��������', '�������� ������� ������ � RS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, d.t_instancedate, 80, sysdate,  d.t_dealid, 421 from dtxdeal_dbt dp join dtxdeal_tmp d on (d.t_code=dp.t_code and d.t_dealid > dp.t_dealid )
where d.t_replstate=0 and d.t_action=1
', 1, 
    '������ ����������� ������ ��� �������� �������. ��������� ����� ������ �� T_DEALCODE.', '�������� ������ �� DEALCODE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, d.t_dealid, 206 from dtxdeal_tmp d join dtxreplobj_dbt ro on (ro.t_objecttype=80 and ro.t_objectid=d.t_dealid and ro.t_objstate=1)
', 1, 
    '������ ��������� � ������ ������� ��������������.', '�������� �� ������ ��������������', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 539 from dtxdeal_tmp where t_code is null
', 1, 
    '�� ����� �������� T_CODE - ��� ������', '�������� ���������� T_CODE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate,  t_dealid, 568 from dtxdeal_tmp where t_kind = 0
', 1, 
    '�� ����� �������� T_KIND - ��� ������', '�������� ���������� T_KIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 568 from dtxdeal_tmp where t_amount = 0
', 1, 
    '�� ����� �������� T_AMOUNT - ���������� ������ �����', '�������� ���������� T_AMOUNT', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 70, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate,  t_dealid, 556 from dtxdeal_tmp where T_PRICE = 0 and T_KIND not in (70,80,90,100,110)
', 1, 
    '�� ����� �������� T_PRICE - ���� �� ��. ������ ������, �� ������� ���', '�������� ���������� T_PRICE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 80, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, :4, t_instancedate, 80, sysdate,  t_dealid, 557 from dtxdeal_tmp where T_COST = 0
', 1, 
    '�� ����� �������� T_COST - ��������� ������ ����� ��� ���', '�������� ���������� T_COST', 'X', '');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 90, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 558 from dtxdeal_tmp where T_TOTALCOST = 0 and t_action in (1,2)
', 1, 
    '�� ����� �������� T_TOTALCOST - ����� ����� ������ ���. ��� � ������ ������', '�������� ���������� T_TOTALCOST', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 100, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select * from (select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 900 from dtxdeal_tmp sou group by t_dealid, t_instancedate having count(*)>1)', 2, 
    '��������� ������ �� ������� ������������ id ������ t_instancedate, ��������� �� ������������ ���������� ������ ����� �������������� ������ 1 ��� � ����', '�������� �� ������������ id ������ ���', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 110, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 597 from dtxdeal_tmp sou where t_parentid>0 and t_kind not in (10,20)', 1, 
    '��������� ��������� ������ �� ������� ����������� ��� ���� � ��������. ��� ����� ����� ������ 2 ����:  20(���� �����������) ��� 10(����� �����������)', '�������� �� ������������ ���� �������� �� �������', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 10, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_action>1) tgt using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.t_dealid)
when matched then update set tgt.tgt_dealid=sou.t_destid', 1, 
    '���������� ������ - ���������� TGT_DEALID �� DTXREPLOBJ_DBT', '���������� TGT_DEALID (��� ��������/��������)', 'X');
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
    '���������� ������ - ���������� TGT_BOFFICEKIND', '���������� TGT_BOFFICEKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 30, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARENTID is not null) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARENTID)
when matched then update set tgt.TGT_PARENTID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_PARENTID �� DTXREPLOBJ_DBT', '���������� TGT_PARENTID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 35, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt 
using ddl_tick_dbt sou 
on (sou.t_dealid=tgt.TGT_PARENTID)
when matched then update set tgt.TGT_PARTYID=sou.T_PARTYID', 1, 
    '���������� ������ - ���������� TGT_PARTYID (ID �����������)', '���������� TGT_PARTYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 40, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PAYMCUR is not null) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PAYMCUR)
when matched then update set tgt.TGT_PAYMCUR=sou.t_destid', 1, 
    '���������� ������ - ���������� TGT_PAYMCUR �� DTXREPLOBJ_DBT', '���������� TGT_PAYMCUR', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 50, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=20 and t_objstate=0) sou 
on (sou.t_objectid=tgt.T_AVOIRISSID)
when matched then update set tgt.TGT_AVOIRISSID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_AVOIRISSID �� DTXREPLOBJ_DBT', '���������� TGT_AVOIRISSID', 'X');
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
    '���������� ������ - ���������� TGT_AVOIRISSID ��� �������', '���������� TGT_AVOIRISSID (2192) ��� �������', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 70, 'merge /*+ # */ into dtxdeal_tmp tgt 
using (select t_fiid, t_root from dfininstr_dbt f join davrkinds_dbt k on (f.T_AVOIRKIND=k.T_AVOIRKIND and k.t_fi_kind=2)) sou 
on (sou.t_fiid=tgt.TGT_AVOIRISSID)
when matched then update set tgt.TGT_AVOIRKIND=sou.T_ROOT
', 1, 
    '���������� ������ - ���������� TGT_AVOIRKIND', '���������� TGT_AVOIRKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 80, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTYID)
when matched then update set tgt.TGT_PARTYID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_PARTYID �� DTXREPLOBJ_DBT', '���������� TGT_PARTYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 90, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_CURRENCYID)
when matched then update set tgt.TGT_CURRENCYID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_CURRENCYID �� DTXREPLOBJ_DBT', '���������� TGT_CURRENCYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 100, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_MARKETID is not null) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID)
when matched then update set tgt.TGT_MARKETID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_MARKETID �� DTXREPLOBJ_DBT', '���������� TGT_MARKETID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 110, 'update /*+ # */ dtxdeal_tmp d set tgt_brokerid=nvl((select t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0 and t_objectid=d.t_brokerid),-1)
where t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_BROKERID �� DTXREPLOBJ_DBT', '���������� TGT_BROKERID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 120, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_SECTOR is not null) tgt 
using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=40 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID  and  sou.T_SUBOBJNUM=tgt.T_SECTOR)
when matched then update set tgt.TGT_SECTOR=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_SECTOR �� DTXREPLOBJ_DBT', '���������� TGT_SECTOR', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 130, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_NKDFIID is not null) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_NKDFIID)
when matched then update set tgt.TGT_NKDFIID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_NKDFIID �� DTXREPLOBJ_DBT', '���������� TGT_NKDFIID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 140, 'update /*+ # */ dtxdeal_tmp set TGT_REPOBASE = CASE t_repobase when 1 then 1  when 2 then 3  when 3 then 5  when 4 then 2   when 5 then 0   when 6 then 4   else -1  end
 where t_replstate=0 and T_REPOBASE is not null', 1, 
    '���������� ������ - ���������� TGT_REPOBASE', '���������� TGT_REPOBASE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 150, 'update /*+ # */ dtxdeal_tmp set TGT_COUNTRY = (select t_codelat3 from dcountry_dbt where t_countryid = T_COUNTRY)
 where t_replstate=0 and T_COUNTRY <> 165', 1, 
    '���������� ������ - ���������� TGT_COUNTRY', '���������� TGT_COUNTRY', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 160, 'update /*+ # */ dtxdeal_tmp set TGT_EXISTBACK = chr(88) where t_replstate=0 and T_KIND in (30,40)', 1, 
    '���������� ������ - ���������� TGT_EXISTBACK', '���������� TGT_EXISTBACK', null);
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 170, 'update /*+ # */ dtxdeal_tmp set tgt_objtype = case when T_KIND in (70,80,90) then 117 else 101 end where t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_OBJTYPE', '���������� TGT_OBJTYPE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 180, 'update /*+ # */ dtxdeal_tmp set tgt_department = LOAD_RSS.GetDepartment /* --nvl(( select t_code from dtxreplobj_dbt ro join ddp_dep_dbt dp on (ro.t_destid=dp.t_partyid and ro.t_objecttype=40 and ro.t_objstate=0)),1) */', 1, 
    '���������� ������ - ���������� TGT_DEPARTMENT', '���������� TGT_DEPARTMENT', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 190, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_WARRANTID is not null) tgt 
using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=50 and t_objstate=0) sou on (sou.t_objectid=tgt.T_WARRANTID)
when matched then update set tgt.TGT_WARRANTID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_WARRANTID �� DTXREPLOBJ_DBT', '���������� TGT_WARRANTID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 200, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARTIALID is not null) tgt 
using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=60 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTIALID)
when matched then update set tgt.TGT_PARTIALID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_PARTIALID �� DTXREPLOBJ_DBT', '���������� TGT_PARTIALID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 230, 'update /*+ # */ dtxdeal_tmp set 
TGT_ISLOAN = decode( t_kind, 50, CHR(88), 60, CHR(88), CHR(0)),
TGT_ISREPO = case when t_kind in (30,40,50,60) then CHR(88) else chr(0) end
where t_replstate=0 and t_action in (1,2)', 1, 
    '���������� ������ - ���������� TGT_ISREPO / TGT_ISLOAN ', '���������� TGT_ISREPO / TGT_ISLOAN', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 240, 'update /*+ # */ dtxdeal_tmp set TGT_WARRANT_NUM = (select t_number from dfiwarnts_dbt where t_id = TGT_WARRANTID) where TGT_WARRANTID is not null', 1, 
    '���������� ������ - ���������� TGT_WARRANT_NUM �� TGT_WARRANTID', '���������� TGT_WARRANT_NUM �� TGT_WARRANTID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 250, 'update /*+ # */ dtxdeal_tmp set TGT_PARTIAL_NUM = (select t_number from dfiwarnts_dbt where t_id = TGT_PARTIALID) where TGT_PARTIALID is not null', 1, 
    '���������� ������ - ���������� TGT_PARTIAL_NUM �� TGT_PARTIALID', '���������� TGT_PARTIAL_NUM �� TGT_PARTIALID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 260, 'update /*+ # */ dtxdeal_tmp tgt set TGT_CURNOM = LOAD_RSS.GetCurrentNom( tgt_avoirissid, t_instancedate )
where t_kind in (70,90)  ', 1, 
    '���������� ������ - ���������� TGT_CURNOM - �������� �������� ������', '���������� TGT_CURNOM', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 270, 'update /*+ # */ DTXDEAL_TMP set
TGT_MATURITYISPRINCIPAL = case when (TGT_ISLOAN=CHR(88) or T_KIND in (80,90 /*��� ��������� � ��*/ ) or (T_SUPLDATE < T_PAYDATE)) then CHR(88) else CHR(0) end,
tgt_maturity = case when TGT_ISLOAN=CHR(88) then T_SUPLDATE WHEN T_KIND in (80,90) THEN T_BALANCEDATE WHEN T_SUPLDATE < T_PAYDATE THEN T_SUPLDATE ELSE T_PAYDATE end,
tgt_expiry = case when TGT_ISLOAN=CHR(88) then T_SUPLDATE2 WHEN T_KIND in (80,90) THEN T_BALANCEDATE WHEN T_SUPLDATE < T_PAYDATE THEN T_PAYDATE ELSE T_SUPLDATE end,
tgt_ReceiptAmount = case when TGT_ISLOAN=CHR(88) then T_NKD2 end', 1, 
    '���������� ������ - ���������� TGT_MATURITY / TGT_EXPIRY / TGT_RECEIPTAMOUNT / TGT_MATURITYISPRINCIPAL', '���������� TGT_MATURITY / TGT_EXPIRY / TGT_RECEIPTAMOUNT / TGT_MATURITYISPRINCIPAL', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 280, 'update /*+ # */ DTXDEAL_TMP set
TGT_MATURITYISPRINCIPAL2 = case when T_SUPLDATE2 < T_PAYDATE2 then CHR(88) else CHR(0) end,
tgt_maturity2 = case when T_SUPLDATE2 < T_PAYDATE2 THEN T_SUPLDATE2 ELSE T_PAYDATE2 end,
tgt_expiry2 = case when T_SUPLDATE2 < T_PAYDATE2 THEN T_PAYDATE2 ELSE T_SUPLDATE2 end
    ', 1, 
    '���������� ������ - ���������� TGT_MATURITY2 / TGT_EXPIRY2 / TGT_MATURITYISPRINCIPAL2', '���������� TGT_MATURITY2 / TGT_EXPIRY2 / TGT_MATURITYISPRINCIPAL2', 'X');
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
    '���������� ������ - ���������� TGT_FORMULA', '���������� TGT_FORMULA', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 300, 'update /*+ # */ DTXDEAL_TMP tgt
set TGT_ISQUOTED = CHR(88) where tgt.tgt_avoirissid in 
(select t_otherfi from dratedef_dbt where t_sincedate > tgt.t_date-30)

', 1, 
    '���������� ������ - ���������� TGT_ISQUOTED', '���������� TGT_ISQUOTED', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 305, 'update /*+ # */ DTXDEAL_TMP set 
TGT_ISKSU = decode(TGT_AVOIRKIND, 51, chr(88), chr(0)), -- ����������� ���������� �������
TGT_ISBOND = decode(TGT_AVOIRKIND, 17, chr(88), chr(0)) -- ���������
', 1, 
    '���������� ������ - ���������� TGT_ISKSU / TGT_ISBOND', '���������� TGT_ISKSU / TGT_ISBOND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 310, 'update /*+ # */ DTXDEAL_TMP set
TGT_RELATIVEPRICE = case when TGT_ISBOND=chr(88) and tgt_bofficekind <> 117 then chr(88) else chr(0) end 
', 1, 
    '���������� ������ - ���������� TGT_RELATIVEPRICE', '���������� TGT_RELATIVEPRICE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 320, 'update /*+ # */ DTXDEAL_TMP set
TGT_PORTFOLIOID = T_PORTFOLIOID,
TGT_PORTFOLIOID_2 = 5', 1, 
    '���������� ������ - ��������������� ��������� ��� �� ������ (TGT_PORTFOLIOID/TGT_PORTFOLIOID_2)', '���������� TGT_PORTFOLIOID / TGT_PORTFOLIOID_2', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 330, 'update /*+ # */ DTXDEAL_TMP set
TGT_DEALKIND = Load_RSS.GetDealKind(T_KIND, TGT_MARKETID, TGT_AVOIRISSID, TGT_ISBASKET, TGT_ISKSU) 
where TGT_DEALKIND is null and t_replstate=0
    ', 1, 
    '���������� ������ - ���������� TGT_DEALKIND - ��� ������. ��� ������� ������ ���� � �������� �� ��� ��������� ������.', '���������� TGT_DEALKIND', 'X');
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
    '���������� ������ - ���������� TGT_PRICE - �� ��������� ��� ��������� ��������� � �������, � ��������� ������� ��������� � t_price', '���������� TGT_PRICE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 350, 'update /*+ # */ dtxdeal_tmp set TGT_DEALID = ddl_tick_dbt_seq.nextval where t_replstate=0 and t_action=1 and t_parentid is null', 1, 
    '���������� ������ - ���������� TGT_DEALID �� ������������������ ��� ������� (�� ��� ��������� ������ �� ���� � ��������, ��� ������������ ������ ������������������)', '���������� TGT_DEALID ��� �������', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 360, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_parentid>0) tgt 
using (SELECT t_dealid, t_fiid, sum(t_principal * (CASE t_kind WHEN 0 THEN 1 WHEN 1 THEN -1 END)) cou FROM DDL_TICK_ENS_DBT group by t_dealid, t_fiid ) sou 
on (sou.t_dealid=tgt.TGT_PARENTID and sou.T_FIID = TGT.TGT_AVOIRISSID)
when matched then update set tgt.TGT_BS_AMOUNT=sou.cou', 1, 
    '���������� ������ ��� ���� � �������� - ���������� TGT_BS_ANOUNT - ������ ����������� �� ������ ������� �������� �� ������ ������', '���������� TGT_BS_ANOUNT (��� ���� � ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 380, 'update /*+ # */ DTXDEAL_TMP set TGT_DEALID = ddl_tick_ens_dbt_seq.nextval where t_parentid > 0 and t_action=1 and t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_DEALID ��� ���� � �������� �� ������������������. ��� ������� ID � ������� ddl_tick_ens_dbt', '���������� TGT_DEALID (��� ���� � ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 400, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_parentid>0) tgt 
using (select t_dealid, coalesce( nullif(t_maturity, date''0001-01-01''), nullif(t_expiry, date''0001-01-01'')) retdate from ddl_leg_dbt where t_legkind=2) sou 
on (sou.t_dealid=tgt.TGT_PARENTID)
when matched then update set tgt.TGT_BS_RETDATE=sou.retdate', 1, 
    '���������� ������ ��� ���� � �������� - ���������� TGT_BS_RETDATE - ���� �������� �����������', '���������� TGT_BS_RETDATE (��� ���� � ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 410, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_parentid>0) tgt 
using (select t_dealid, coalesce( nullif(t_maturity, date''0001-01-01''), nullif(t_expiry, date''0001-01-01'')) begdate from ddl_leg_dbt where t_legkind=0) sou 
on (sou.t_dealid=tgt.TGT_PARENTID)
when matched then update set tgt.TGT_BS_BEGDATE=sou.begdate', 1, 
    '���������� ������ ��� ���� � �������� - ���������� TGT_BS_BEGDATE - ���� ��������� �������� �����������', '���������� TGT_BS_BEGDATE (��� ���� � ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 420, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_parentid>0) tgt 
using ddl_tick_dbt sou 
on (sou.t_dealid=tgt.TGT_PARENTID)
when matched then update set tgt.TGT_BS_DEALKIND=sou.t_dealtype', 1, 
    '���������� ������ ��� ���� � �������� - ���������� TGT_BS_DEALKIND - ��� ������� ������ (2134 ��� ��������� ���� ��� 2139 ��� �������)', '���������� TGT_BS_DEALKIND (��� ���� � ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 430, 'update /*+ # */ DTXDEAL_TMP set TGT_BS_DIRECTION=
CASE T_KIND WHEN 10 THEN decode(TGT_BS_DEALKIND, 2139, 1, 2134, 0)
            WHEN 20 THEN decode(TGT_BS_DEALKIND, 2139, 0, 2134, 1)
            END
 where t_parentid > 0 and t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_BS_DIRECTION - ����������� �������� ����������� �� ������ ���� � ��������. 0 - ����, 1 - �����', '���������� TGT_BS_DIRECTION (��� ���� � ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 450, 'update /*+ # */ DTXDEAL_TMP set TGT_BS_PAYER=
CASE T_KIND WHEN 10 THEN decode(TGT_BS_DEALKIND, 2139, t_partyid, 2134, 1)
            WHEN 20 THEN decode(TGT_BS_DEALKIND, 2139, 1, 2134, t_partyid)
            END
 where t_parentid > 0 and t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_BS_PAYER - ���������� � ���� �������� �����������. ���� ��� ����(1), ���� t_dealid �� ��������', '���������� TGT_BS_PAYER (��� ���� � ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (80, 3, 460, 'update /*+ # */ DTXDEAL_TMP set TGT_BS_AMOUNT_AFTER = CASE TGT_BS_DIRECTION WHEN 1 THEN tgt_bs_amount-t_amount ELSE tgt_bs_amount-t_amount END  where t_parentid > 0 and t_action=1 and t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_BS_AMOUNT_AFTER - ���������� ����� � ����������� ��� ������� ���� �� ������� ����� ���������� ���� ��������', '���������� TGT_BS_AMOUNT_AFTER (��� ���� � ��������)', 'X');
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
    '���������� ������ - ���������� TGT_BS_RQTYPE - ��� ������� �� ����� �������� �����������. ���� 9-����. ��������, ���� 8-���� ��� ����� �����������', '���������� TGT_BS_RQTYPE (��� ���� � ��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 598 from dtxdeal_tmp sou where t_parentid>0 and tgt_parentid is null', 1, 
    '��������� ������� ������� ������ �� ���� � �������� ��� ����� �������� �����������', '�������� ���������� T_PARENTID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 4, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 599 from dtxdeal_tmp sou where TGT_BS_AMOUNT_AFTER < 0', 1, 
    '��������� ������������� ����������� ��� ������ (��������� ��������� ������ ��� ���� �� �������)', '�������� ������������� ����������� (���� � ��������)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 4, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 80, sysdate, t_dealid, 422 from dtxdeal_tmp where TGT_DEALID is null and t_action in (2,3)', 1, 
    '������: �� ������� ��������������� ������ ��� ���������/��������', '�������� ������� ������ (��� ���������/��������)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 601 from dtxdemand_tmp where t_dealid is null', 1, 
    '�� ����� �������� T_DEALID - ������������� ������', '�������� ���������� T_DEALID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 602 from dtxdemand_tmp where t_part is null', 1, 
    '�� ����� �������� T_PART - ����� ����� ������', '�������� ���������� T_PART', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 603 from dtxdemand_tmp where t_kind is null', 1, 
    '�� ����� �������� T_KIND - ��� �������', '�������� ���������� T_KIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 604 from dtxdemand_tmp where t_direction is null', 1, 
    '�� ����� �������� t_direction - ��� �������', '�������� ���������� T_DIRECTION', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 605 from dtxdemand_tmp where t_fikind is null', 1, 
    '�� ����� t_fikind  �������', '�������� ���������� T_FIKIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 60, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 606 from dtxdemand_tmp where t_date is null', 1, 
    '�� ������ t_date - ���� �������', '�������� ���������� T_DATE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 70, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 607 from dtxdemand_tmp where t_sum is null', 1, 
    '�� ������ t_sum - ����� �������', '�������� ���������� T_SUM', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 80, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 608 from dtxdemand_tmp where t_state is null', 1, 
    '�� ������ t_state - ������ �������', '�������� ���������� T_STATE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 90, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 609 from dtxdemand_tmp where t_paycurrencyid is null and t_kind not in (10,60,120)', 1, 
    '�� ������ t_paycurrencyid - ������ �������', '�������� ���������� T_PAYCURRENCYID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 2, 100, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select * from (select :1, :2, :3, :4, t_instancedate, 70, sysdate, t_demandid, 900 from dtxdemand_tmp sou group by t_demandid, t_instancedate having count(*)>1)', 2, 
    '��������� ������ �� ������� ������������ id ������ t_instancedate, ��������� �� ������������ ���������� ������ ����� �������������� ������ 1 ��� � ����', '�������� �� ������������ id ������ ���', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 10, 'merge /*+ # */ into (select * from dtxdemand_tmp where t_replstate=0 ) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=90 and t_objstate=0) sou on (sou.t_objectid=tgt.T_DEMANDID)
when matched then update set tgt.TGT_DEMANDID=sou.T_DESTID', 1, 
    '���������� ������ - ����� TGT_DEMANDID ��� ����������/��������', '���������� TGT_DEMANDID ��� ����������/�������� (action 2,3)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 12, 'update /*+ # */ dtxdemand_tmp set TGT_DEMANDID=ddlrq_dbt_seq.nextval where t_action=1 and t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_DEMANDID ��� ������� (action=1)', '���������� TGT_DEMANDID ��� ������� (action=1)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 20, 'merge /*+ # */ into dtxdemand_tmp tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_DEALID)
when matched then update set tgt.TGT_DEALID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_DEALID', '���������� TGT_DEALID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 30, 'merge /*+ # */ into (select * from dtxdemand_tmp where t_fikind=20 and t_replstate=0 and t_action in (1,2)) tgt 
using (select t_dealid, t_pfi from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID)
when matched then update set tgt.TGT_FIID=sou.T_PFI', 1, 
    '���������� ������ - ���������� TGT_FIID ��� �������� ��������', '���������� TGT_FIID ��� �������� ��������', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 40, 'merge /*+ # */ into dtxdemand_tmp tgt 
using (select t_dealid, T_DEALTYPE from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID)
when matched then update set tgt.TGT_DEALKIND=sou.T_DEALTYPE', 1, 
    '���������� ������ - ���������� TGT_DEALKIND', '���������� TGT_DEALKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 50, 'merge /*+ # */ into (select * from dtxdemand_tmp where t_fikind=10 and t_replstate=0 and t_action in (1,2)) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PAYCURRENCYID)
when matched then update set tgt.TGT_FIID=sou.t_destid', 1, 
    '���������� ������ - ���������� TGT_FIID ��� �������� ��������', '���������� TGT_FIID ��� �������� ��������', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 60, 'merge /*+ # */ into (select * from dtxdemand_tmp) tgt 
   using (select t_dealid, T_BOFFICEKIND from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID) 
   when matched then update set tgt.TGT_BOFFICEKIND=sou.T_BOFFICEKIND', 1, 
    '���������� ������ - ���������� TGT_BOFFICEKIND', '���������� TGT_BOFFICEKIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 70, 'merge /*+ # */ into (select * from dtxdemand_tmp) tgt 
   using (select t_dealid, T_MARKETID, T_PARTYID from ddl_tick_dbt) sou on (sou.t_dealid=tgt.TGT_DEALID) 
   when matched then update set tgt.TGT_PARTY= case when sou.T_MARKETID>0 then T_MARKETID else T_PARTYID end', 1, 
    '���������� ������ - ���������� TGT_PARTY', '���������� TGT_PARTY', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 90, 'update /*+ # */ dtxdemand_tmp set TGT_KIND=case when t_direction=1 then 0 else 1 end', 1, 
    '���������� ������ - ���������� TGT_KIND', '���������� TGT_KIND', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 100, 'update /*+ # */ dtxdemand_tmp set TGT_STATE=2, TGT_PLANDATE=date''0001-01-01'', TGT_FACTDATE=T_DATE, TGT_CHANGEDATE=date''0001-01-01'' where t_isfact=chr(88)', 1, 
    '���������� ������ - ���������� TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE ��� ����������� ��������', '���������� TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE - 1', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 110, 'update /*+ # */ dtxdemand_tmp set TGT_STATE=0, TGT_PLANDATE=T_DATE, TGT_FACTDATE=date''0001-01-01'', TGT_CHANGEDATE=T_DATE where t_isfact is null', 1, 
    '���������� ������ - ���������� TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE ��� �������� ��������', '���������� TGT_STATE / TGT_PLANDATE / TGT_FACTDATE / TGT_CHANGEDATE - 2', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 120, 'update /*+ # */ dtxdemand_tmp set TGT_TYPE=
case t_kind
    when 10 then 8 -- ��������
    when 20 then 0 -- �������
    when 30 then 1 -- �����
    when 40 then 2 -- ������
    when 50 then 3 -- ��������
    when 60 then 8 -- �������� ��������
    when 70 then 2 -- �������� ������
    when 80 then 4 -- ������� ��������� ������
    when 90 then 5 -- ��������� ���������
    when 100 then 10 -- ������� ����������
    when 110 then 7  -- ��������������� ������
    when 120 then 9  -- ��������������� ��������
    else -1
end
', 1, 
    '���������� ������ - ���������� TGT_TYPE', '���������� TGT_TYPE', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 3, 130, 'update /*+ # */ dtxdemand_tmp set TGT_SUBKIND=case when tgt_type in (8,9) then 1 else 0 end', 1, 
    '���������� ������ - ���������� TGT_SUBKIND', '���������� TGT_SUBKIND', 'X');
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
    '���������� ������ - ���������� TGT_NUM �� ddlrq_dbt', '���������� TGT_NUM �� ddlrq_dbt', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 611 from dtxdemand_tmp where TGT_DEMANDID is null and t_action in (2,3)', 1, 
    '�����: �� ������� � dtxreplobj ��������������� �/� �� ������ (��� ���������� � ��������)', '�� ������� � dtxreplobj �/� �� ������ (action = 2,3)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (90, 4, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 90, sysdate, t_demandid, 612 from dtxdemand_tmp where tgt_dealid is null and t_action in (1,2)', 1, 
    '��������� ������� ��������������� ������ �� ������� � DTXREPLOBJ', '�������� ������� ������ � dtxreplobj', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (90, 5, 10, 'merge /*+ # */ into (select rowid ri, a.* from dtxdemand_tmp a where t_replstate=0 and t_action in (1,2)) tgt 
using (select rowid ri, nvl(tgt_num,0) old_num, sum(1) over(partition by tgt_bofficekind,tgt_dealid, t_part, tgt_type,tgt_fiid order by t_demandid) num from dtxdemand_tmp) sou 
on (sou.ri=tgt.ri)
when matched then update set tgt.TGT_NUM=sou.old_num + sou.NUM-1', 1, 
    '���������� ������ - �������� TGT_NUM', '�������� TGT_NUM', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 1, 10, 'update /*+ # */ DTXCOMISS_TMP set T_DEALID=nvl(T_DEALID,0), T_TYPE=nvl(T_TYPE,0), T_SUM=nvl(T_SUM,0), T_NDS=nvl(T_NDS,0), T_CURRENCYID=nvl(T_CURRENCYID,-1)', 1, 
    '�������������� ������ - �������������� NVL', '�������������� ������ - NVL �� �����', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 701 from dtxcomiss_tmp where t_dealid = 0', 1, 
    '������: ������ ��� ��������� �� ������', '�������� T_DEALID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 703 from dtxcomiss_tmp where t_currencyid = -1', 1, 
    '������: ������ ��������� �� ������', '�������� T_CURRENCYID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 709 from dtxcomiss_tmp where t_sum = 0', 1, 
    '������: �� ������ ����� ��������', '�������� T_SUM - ����� ��������', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 20, 'merge /*+ # */ into (select * from dtxcomiss_tmp) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=100 and t_objstate=0) sou on (sou.t_objectid=tgt.T_COMISSID)
when matched then update set tgt.TGT_COMISSID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_COMISSID (ID ��������). ��� ���������/��������, �� DTXREPLOBJ_DBT', '���������� TGT_COMISSID (��� ���������/��������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 30, 'merge /*+ # */ into (select * from dtxcomiss_tmp) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_DEALID)
when matched then update set tgt.TGT_DEALID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_DEALID (ID ������)', '���������� TGT_DEALID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 40, 'merge /*+ # */ into (select * from dtxcomiss_tmp) tgt 
using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_CURRENCYID)
when matched then update set tgt.TGT_CURRENCYID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_CURRENCYID (ID ������ ��������)', '���������� TGT_CURRENCYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 50, 'update /*+ # */ DTXCOMISS_TMP set TGT_COMMCODE = (select t_fi_code || lpad(T_TYPE,2,''0'') from dfininstr_dbt where t_fiid=TGT_CURRENCYID)', 1, 
    '���������� ������ - ���������� TGT_COMMCODE (��� ��������)', '���������� TGT_COMMCODE (��� ��������)', 'X');

Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 55, 'update /*+ # */ DTXCOMISS_TMP set TGT_COMMNUMBER = (select t_number from dsfcomiss_dbt where t_code=TGT_COMMCODE)', 1, 
    '���������� ������ - ���������� TGT_COMMNUMBER (����� ��������)', '���������� TGT_COMMNUMBER (����� ��������)', 'X');


Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 60, 'update /*+ # */ DTXCOMISS_TMP set TGT_NDS = 0
--CASE WHEN t_type in (3,9) THEN  T_SUM-round(T_SUM/118*100, 2) ELSE 0 END', 1, 
    '���������� ������ - ���������� TGT_NDS ', '���������� TGT_NDS (���������)', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 65, 'merge /*+ # */ into (select * from dtxcomiss_tmp where t_replstate=0) tgt 
using ddl_tick_dbt sou 
on (sou.t_dealid=tgt.tgt_dealid)
when matched then update set tgt.TGT_PARTYID=sou.t_partyid', 1, 
    '���������� ������ - ���������� TGT_PARTYID (ID �����������)', '���������� TGT_PARTYID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 3, 70, 'merge /*+ # */ into (select * from DTXCOMISS_TMP where t_replstate=0) tgt
using dsfcontr_dbt sou on (tgt.tgt_partyid=sou.t_partyid)
when matched then update set tgt.TGT_CONTRACTID = sou.t_id
', 1, 
    '���������� ������ - ���������� TGT_CONTRACTID', '���������� TGT_CONTRACTID', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 702 from dtxcomiss_tmp where TGT_DEALID is null', 1, 
    '������: �� ������� ��������������� ������ ��� ���������', '�������� TGT_DEALID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 704 from dtxcomiss_tmp where TGT_CURRENCYID is null', 1, 
    '������: �� ������� ��������������� ������ ��������', '�������� TGT_CURRENCYID', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 706 from dtxcomiss_tmp where TGT_COMISSID is null and t_action in (2,3)', 1, 
    '������: ����������/��������� �������� �� ������ �� ����������. �������� �� DTXREPLOBJ_DBT', '�������� TGT_COMISSID (���������� �������� ��� � �������) - 1', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 706 from (select * from dtxcomiss_tmp where t_replstate=0 and t_action in (2,3)) s 
LEFT JOIN ddlcomis_dbt t on (s.tgt_comissid=t.t_id)
where t.t_id is null
', 1, 
    '������: ����������/��������� �������� �� ������ �� ����������. �������� �� DDLCOMIS_DBT', '�������� TGT_COMISSID (���������� �������� ��� � �������) - 2', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (100, 4, 50, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_severity, t_instancedate, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) 
select :1, :2, :3, :4, t_instancedate, 100, sysdate, t_comissid, 707 from dtxcomiss_tmp where TGT_COMISSID is not null and t_action = 1', 1, 
    '������: ����������� �������� ��� ���� � �������', '�������� TGT_COMISSID (����������� �������� ��� � �������)', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_IN_USE)
 Values
   (100, 5, 10, 'update /*+ # */ DTXCOMISS_TMP set TGT_COMISSID=ddlcomis_dbt_seq.nextval where t_action=1 and t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_COMISSID (ID ��������). ��� �������, �� ������������������ ddlcomis_dbt_seq', '���������� TGT_COMISSID (��� ���������/��������)', 'X');
COMMIT;
