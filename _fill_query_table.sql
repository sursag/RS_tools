--SQL Statement which produced this data:
--
--  select T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, T_DESC, T_NAME, T_USE_BIND, T_IN_USE from DTX_QUERY_DBT order by t_set, t_num desc;
--
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 1, 15, 'update dtxdeal_tmp set T_AMOUNT=nvl(T_AMOUNT, 0), T_PRICE=nvl(T_PRICE, 0), T_COST=nvl(T_COST, 0), '||CHR(13)||CHR(10)||'T_NKD=nvl(T_NKD, 0), T_TOTALCOST=nvl(T_TOTALCOST, 0), T_RATE=nvl(T_RATE, 0), '||CHR(13)||CHR(10)||'T_REPOBASE=nvl(T_REPOBASE, 0), T_ISPFI_1=nvl(T_ISPFI_1, 0), T_ISPFI_2=nvl(T_ISPFI_2, 0), '||CHR(13)||CHR(10)||'T_LIMIT=nvl(T_LIMIT, 0), T_CHRATE=nvl(T_CHRATE, 0), T_COUNTRY=nvl(T_COUNTRY, 165), '||CHR(13)||CHR(10)||'T_PRICE2=nvl(T_PRICE2, 0), T_COST2=nvl(T_COST2, 0), T_NKD2=nvl(T_NKD2, 0),'||CHR(13)||CHR(10)||'T_TOTALCOST2=nvl(T_TOTALCOST2, 0), T_DOPCONTROL=nvl(T_DOPCONTROL, 0), T_FISSKIND=nvl(T_FISSKIND, 0), '||CHR(13)||CHR(10)||'T_PRICE_CALC=nvl(T_PRICE_CALC, 0), T_PRICE_CALC_DEF=nvl(T_PRICE_CALC_DEF, 0), '||CHR(13)||CHR(10)||'T_PRICE_CALC_METHOD=nvl(T_PRICE_CALC_METHOD, 0), T_PRICE_CALC_VAL=nvl(T_PRICE_CALC_VAL, -1), '||CHR(13)||CHR(10)||'T_ADJUSTMENT=nvl(T_ADJUSTMENT, chr(0)), T_ATANYDAY=nvl(T_ATANYDAY, chr(0)), T_DIV=nvl(T_DIV, chr(0)), '||CHR(13)||CHR(10)||'T_COSTCHANGEONAMOR=nvl(T_COSTCHANGEONAMOR, chr(0)), T_COSTCHANGEONCOMP=nvl(T_COSTCHANGEONCOMP, chr(0)), '||CHR(13)||CHR(10)||'T_COSTCHANGE=nvl(T_COSTCHANGE, chr(0)), T_NEEDDEMAND=nvl(T_NEEDDEMAND, chr(0)), T_NEEDDEMAND2=nvl(T_NEEDDEMAND, chr(0))', 1, 
    '�������������� ������ - NVL �����', '��������� ����� NVL', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 1, 10, 'update dtxdeal_tmp set T_EXTCODE = trim(T_EXTCODE), T_MARKETCODE = trim(T_MARKETCODE), '||CHR(13)||CHR(10)||'T_PARTYCODE = trim(T_PARTYCODE), T_CODE = trim(T_CODE), t_conditions=trim(t_conditions),'||CHR(13)||CHR(10)||'T_CONTRNUM=trim(T_CONTRNUM), T_DOPCONTROL_NOTE=trim(T_DOPCONTROL_NOTE), T_PRICE_CALC_MET_NOTE=trim(T_PRICE_CALC_MET_NOTE)', 1, 
    '�������������� ������ - TRIM ��������� �����', 'TRIM ��������� �����', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 50, 'insert /*+ append */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, t_dealid, 558 from dtxdeal_tmp where (T_PRICE + T_COST) = 0 and T_TOTALCOST > 0'||CHR(13)||CHR(10)||'', 1, 
    '�� ����� �������� T_TOTALCOST - ����� ����� ������ ���. ��� � ������ ������', '�������� ���������� T_TOTALCOST', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 45, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  t_dealid, 557 from dtxdeal_tmp where (T_PRICE + T_TOTALCOST) = 0 and T_COST > 0'||CHR(13)||CHR(10)||'', 1, 
    '�� ����� �������� T_COST - ��������� ������ ����� ��� ���', '�������� ���������� T_COST', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 40, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  t_dealid, 556 from dtxdeal_tmp where (T_PRICE + T_COST + T_TOTALCOST) = 0 and T_KIND not in (70,80,90,100,110)'||CHR(13)||CHR(10)||'', 1, 
    '�� ����� �������� T_PRICE - ���� �� ��. ������ ������, �� ������� ���', '�������� ���������� T_PRICE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 35, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, t_dealid, 568 from dtxdeal_tmp where t_amount is null'||CHR(13)||CHR(10)||'', 1, 
    '�� ����� �������� T_AMOUNT - ���������� ������ �����', '�������� ���������� T_AMOUNT', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 30, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  t_dealid, 568 from dtxdeal_tmp where t_kind is null'||CHR(13)||CHR(10)||'', 1, 
    '�� ����� �������� T_KIND - ��� ������', '�������� ���������� T_KIND', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 25, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, t_dealid, 539 from dtxdeal_tmp where t_code is null'||CHR(13)||CHR(10)||'', 1, 
    '�� ����� �������� T_CODE - ��� ������', '�������� ���������� T_CODE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 20, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, d.t_dealid, 206 from dtxdeal_tmp d join dtxreplobj_dbt ro on (ro.t_objecttype=80 and ro.t_objectid=d.t_dealid)'||CHR(13)||CHR(10)||'', 1, 
    '������ ��������� � ������ ������� ��������������.', '�������� �� ������ ��������������', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 15, 'insert /*+ parallel(4) */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate,  d.t_dealid, 421 from dtxdeal_dbt dp, dtxdeal_tmp d where d.t_code=dp.t_code and d.t_dealid > dp.t_dealid'||CHR(13)||CHR(10)||'', 1, 
    '������ ����������� ������ ��� �������� �������. ��������� ����� ������ �� T_DEALCODE.', '�������� ������ �� DEALCODE', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 2, 10, 'insert /*+ # */ into dtx_error_dbt(t_sessid, t_detailid, t_queryid, t_objecttype, t_timestamp, t_objectid, T_ERRORCODE) select :1, :2, :3, 80, sysdate, dtxdeal_tmp.t_dealid, '||CHR(13)||CHR(10)||'case    when ro.t_objstate=2 then 423 -- ������ ��� ������� � ������� �������'||CHR(13)||CHR(10)||'    when tick.t_dealid is null then 422 -- ��� ������ ������'||CHR(13)||CHR(10)||'    when leg.t_dealid is null then 422 -- ��� ������� ������� ������'||CHR(13)||CHR(10)||'    when ro.t_destid is null  then 422 -- ������ � ����� dealid �� ���������������'||CHR(13)||CHR(10)||'    when ro.t_objstate=1 then 206 -- ������ � ������ ������'||CHR(13)||CHR(10)||'end T_ERRORCODE '||CHR(13)||CHR(10)||'from dtxdeal_tmp left join dtxreplobj_dbt ro  on (ro.t_objecttype=80 and dtxdeal_tmp.t_dealid=ro.t_objectid)'||CHR(13)||CHR(10)||'left join ddl_tick_dbt tick on (ro.t_destid = tick.t_dealid) '||CHR(13)||CHR(10)||'left join ddl_leg_dbt leg on (ro.t_destid = leg.t_dealid and leg.t_legkind=0)'||CHR(13)||CHR(10)||'where dtxdeal_tmp.t_action in (2,3) and (ro.t_destid is null or tick.t_dealid is null or leg.t_dealid is null)'||CHR(13)||CHR(10)||'', 1, 
    '������ ����������� ������ ��� �������� ����������/��������. ���������, ���� �� � ������� ������� ��������������� ������ � ������ ID. ������������ ����� ��������� ������� - ���������� ������ � �������, � leg, ���������� � replobj � ������ ����� ���������. �����, ����� ������� �� ��������� ��������', '�������� ������� ������ � RS', 'X', 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 500, 'update dtxdeal_tmp set TGT_DEALID = ddl_tick_dbt_seq.nextval where t_replstate=0 and t_action=1', 1, 
    '���������� ������ - ���������� TGT_DEALID �� ������������������ ��� �������', '���������� TGT_DEALID ��� �������', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 175, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_PRICE = case when tgt_isbond=chr(88) and tgt_bofficekind=117 and t_kind in (70,90) then tgt_curnom'||CHR(10)||'                 when tgt_isbond=chr(88) and tgt_bofficekind=117 and t_kind = 80 then 0'||CHR(10)||'                 else t_price'||CHR(10)||'            end'||CHR(10)||'    ', 1, 
    '���������� ������ - ���������� TGT_PRICE - �� ��������� ��� ��������� ��������� � �������, � ��������� ������� ��������� � t_price', '���������� TGT_PRICE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 170, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_DEALKIND = Load_RSS.GetDealKind(T_KIND, T_MARKETID, TGT_AVOIRISSID, TGT_ISBASKET, TGT_ISKSU) '||CHR(10)||'    ', 1, 
    '���������� ������ - ���������� TGT_DEALKIND - ��� ������', '���������� TGT_DEALKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 165, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(13)||CHR(10)||'TGT_PORTFOLIOID = case when TGT_ISQUOTED=chr(88) then 1 else 5 end,'||CHR(13)||CHR(10)||'TGT_PORTFOLIOID_2 = case when TGT_ISQUOTED=chr(88) and TGT_ISREPO=chr(88) then 1 else 5 end '||CHR(13)||CHR(10)||'where TGT_ISLOAN = chr(0)'||CHR(13)||CHR(10)||'', 1, 
    '���������� ������ - ��������������� ��������� ��� �� ������ (TGT_PORTFOLIOID/TGT_PORTFOLIOID_2)', '���������� TGT_PORTFOLIOID / TGT_PORTFOLIOID_2', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 160, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_RELATIVEPRICE = case when TGT_ISBOND=chr(88) and tgt_bofficekind <> 117 then chr(88) else chr(0) end '||CHR(10)||'', 1, 
    '���������� ������ - ���������� TGT_RELATIVEPRICE', '���������� TGT_RELATIVEPRICE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 155, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(13)||CHR(10)||'TGT_ISQUOTED = LOAD_RSS.GETISQUOTED(TGT_AVOIRISSID, T_INSTANCEDATE),'||CHR(13)||CHR(10)||'TGT_ISKSU = LOAD_RSS.GETISKSU(TGT_AVOIRISSID),'||CHR(13)||CHR(10)||'TGT_ISBOND = decode(TGT_AVOIRKIND, 17, chr(88), chr(0)) '||CHR(13)||CHR(10)||'', 1, 
    '���������� ������ - ���������� TGT_ISQUOTED / TGT_ISKSU / TGT_ISBOND', '���������� TGT_ISQUOTED / TGT_ISKSU/TGT_ISBOND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 150, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(13)||CHR(10)||'TGT_FORMULA = case T_ACCOUNTTYPE'||CHR(13)||CHR(10)||'              WHEN 1 THEN 50 /*DVP*/'||CHR(13)||CHR(10)||'              WHEN 2 THEN 49 /*DFP*/'||CHR(13)||CHR(10)||'              WHEN 3 THEN 52 /*PP*/'||CHR(13)||CHR(10)||'              WHEN 4 THEN 51 /*PD*/ '||CHR(13)||CHR(10)||'              end '||CHR(13)||CHR(10)||'', 1, 
    '���������� ������ - ���������� TGT_FORMULA', '���������� TGT_FORMULA', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 145, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_MATURITYISPRINCIPAL2 = case when T_SUPLDATE < T_PAYDATE then CHR(88) else CHR(0) end,'||CHR(10)||'tgt_maturity2 = case when T_SUPLDATE < T_PAYDATE THEN T_SUPLDATE ELSE T_PAYDATE end,'||CHR(10)||'tgt_expiry2 = case when T_SUPLDATE < T_PAYDATE THEN T_PAYDATE ELSE T_SUPLDATE end'||CHR(10)||'    ', 1, 
    '���������� ������ - ���������� TGT_MATURITY2 / TGT_EXPIRY2 / TGT_MATURITYISPRINCIPAL2', '���������� TGT_MATURITY2 / TGT_EXPIRY2 / TGT_MATURITYISPRINCIPAL2', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 140, 'update /*+ parallel(4) */ DTXDEAL_TMP set'||CHR(10)||'TGT_MATURITYISPRINCIPAL = case when (TGT_ISLOAN=CHR(88) or T_KIND in (80,90 /*��� ��������� � ��*/ ) or (T_SUPLDATE < T_PAYDATE)) then CHR(88) else CHR(0) end,'||CHR(10)||'tgt_maturity = case when TGT_ISLOAN=CHR(88) then T_SUPLDATE WHEN T_KIND in (80,90) THEN T_BALANCEDATE WHEN T_SUPLDATE < T_PAYDATE THEN T_SUPLDATE ELSE T_PAYDATE end,'||CHR(10)||'tgt_expiry = case when TGT_ISLOAN=CHR(88) then T_SUPLDATE2 WHEN T_KIND in (80,90) THEN T_BALANCEDATE WHEN T_SUPLDATE < T_PAYDATE THEN T_PAYDATE ELSE T_SUPLDATE end,'||CHR(10)||'tgt_ReceiptAmount = case when TGT_ISLOAN=CHR(88) then T_NKD2 end', 1, 
    '���������� ������ - ���������� TGT_MATURITY / TGT_EXPIRY / TGT_RECEIPTAMOUNT / TGT_MATURITYISPRINCIPAL', '���������� TGT_MATURITY / TGT_EXPIRY / TGT_RECEIPTAMOUNT / TGT_MATURITYISPRINCIPAL', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 135, 'update /*+ # */ dtxdeal_tmp tgt set TGT_CURNOM = LOAD_RSS.GetCurrentNom( tgt_avoirissid, t_instancedate )', 1, 
    '���������� ������ - ���������� TGT_CURNOM - �������� �������� ������', '���������� TGT_CURNOM', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 130, 'update dtxdeal_tmp set TGT_PARTIAL_NUM = (select t_number from dfiwarnts_dbt where t_id = TGT_PARTIALID) where TGT_PARTIALID is not null', 1, 
    '���������� ������ - ���������� TGT_PARTIAL_NUM �� TGT_PARTIALID', '���������� TGT_PARTIAL_NUM �� TGT_PARTIALID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 125, 'update dtxdeal_tmp set TGT_WARRANT_NUM = (select t_number from dfiwarnts_dbt where t_id = TGT_WARRANTID) where TGT_WARRANTID is not null', 1, 
    '���������� ������ - ���������� TGT_WARRANT_NUM �� TGT_WARRANTID', '���������� TGT_WARRANT_NUM �� TGT_WARRANTID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 120, 'update dtxdeal_tmp set '||CHR(13)||CHR(10)||'TGT_ISLOAN = decode( t_kind, 50, CHR(88), 60, CHR(88), CHR(0)),'||CHR(13)||CHR(10)||'TGT_ISREPO = case when t_kind in (30,40,50,60) then CHR(88) else chr(0) end'||CHR(13)||CHR(10)||'where t_replstate=0 and t_action=1', 1, 
    '���������� ������ - ���������� TGT_ISREPO / TGT_ISLOAN ��� ������� (action=1)', '���������� TGT_ISREPO / TGT_ISLOAN ��� ������� (action=1)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 115, 'update dtxdeal_tmp set  TGT_ISBUY   = decode(RSB_SECUR.IsBuy(TGT_GROUP), 1, chr(88), chr(0)),'||CHR(13)||CHR(10)||'                        TGT_ISSALE = decode(RSB_SECUR.IsSale(TGT_GROUP), 1, chr(88), chr(0)),'||CHR(13)||CHR(10)||'                        TGT_ISREPO = decode(RSB_SECUR.IsRepo(TGT_GROUP), 1, chr(88), chr(0)),'||CHR(13)||CHR(10)||'                        TGT_ISLOAN = decode(RSB_SECUR.IsLoan(TGT_GROUP), 1, chr(88), chr(0))'||CHR(13)||CHR(10)||' where t_replstate=0 and t_action=2', 1, 
    '���������� ������ - ���������� TGT_ISREPO / TGT_ISLOAN ��� ��������� (action=2)', '���������� TGT_ISREPO / TGT_ISLOAN ��� ��������� (action=2)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 110, 'update dtxdeal_tmp set TGT_GROUP = (select RSB_SECUR.get_OperationGroup( op.t_systypes ) from ddl_tick_dbt tk, doprkoper_dbt op where op.T_KIND_OPERATION = tk.T_DEALTYPE and op.T_DOCKIND = tk.T_BOFFICEKIND and tk.t_dealid=TGT_DEALID) where t_replstate=0 and t_action=2', 1, 
    '���������� ������ - ���������� TGT_GROUP ��� ���������', '���������� TGT_GROUP ��� ��������� (action=2)', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 105, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARTIALID is not null) tgt '||CHR(13)||CHR(10)||'using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=60 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTIALID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_PARTIALID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_PARTIALID �� DTXREPLOBJ_DBT', '���������� TGT_PARTIALID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 100, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_WARRANTID is not null) tgt '||CHR(10)||'using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=50 and t_objstate=0) sou on (sou.t_objectid=tgt.T_WARRANTID)'||CHR(10)||'when matched then update set tgt.TGT_WARRANTID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_WARRANTID �� DTXREPLOBJ_DBT', '���������� TGT_WARRANTID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 95, 'update /*+ # */ dtxdeal_tmp set tgt_department = nvl(( select t_code from dtxreplobj_dbt ro join ddp_dep_dbt dp on (ro.t_destid=dp.t_partyid and ro.t_objecttype=40 and ro.t_objstate=0)),1)', 1, 
    '���������� ������ - ���������� TGT_DEPARTMENT', '���������� TGT_DEPARTMENT', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 90, 'update dtxdeal_tmp set tgt_objtype = case when T_KIND in (70,80,90) then 117 else 101 end where t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_OBJTYPE', '���������� TGT_OBJTYPE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 85, 'update dtxdeal_tmp set TGT_EXISTBACK = chr(88) where t_replstate=0 and T_KIND in (30,40)', 1, 
    '���������� ������ - ���������� TGT_EXISTBACK', '���������� TGT_EXISTBACK', NULL, NULL);
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 80, 'update dtxdeal_tmp set TGT_COUNTRY = (select t_codelat3 from dcountry_dbt where t_countryid = T_COUNTRY)'||CHR(10)||' where t_replstate=0 and T_COUNTRY <> 165', 1, 
    '���������� ������ - ���������� TGT_COUNTRY', '���������� TGT_COUNTRY', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 75, 'update dtxdeal_tmp set TGT_REPOBASE = CASE t_repobase when 1 then 1  when 2 then 3  when 3 then 5  when 4 then 2   when 5 then 0   when 6 then 4   else -1  end'||CHR(10)||' where t_replstate=0 and T_REPOBASE is not null', 1, 
    '���������� ������ - ���������� TGT_REPOBASE', '���������� TGT_REPOBASE', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 70, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_NKDFIID is not null) tgt '||CHR(13)||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_NKDFIID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_NKDFIID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_NKDFIID �� DTXREPLOBJ_DBT', '���������� TGT_NKDFIID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 65, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_SECTOR is not null) tgt '||CHR(10)||'using (select t_objectid, T_SUBOBJNUM, t_destid from dtxreplobj_dbt where t_objecttype=40 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID  and  sou.T_SUBOBJNUM=tgt.T_SECTOR)'||CHR(10)||'when matched then update set tgt.TGT_SECTOR=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_SECTOR �� DTXREPLOBJ_DBT', '���������� TGT_SECTOR', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 60, 'update /*+ parallel(4) */ dtxdeal_tmp d set tgt_brokerid=nvl((select t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0 and t_objectid=d.t_brokerid),-1)'||CHR(13)||CHR(10)||'where t_replstate=0', 1, 
    '���������� ������ - ���������� TGT_BROKERID �� DTXREPLOBJ_DBT', '���������� TGT_BROKERID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 55, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and t_MARKETID is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_MARKETID)'||CHR(10)||'when matched then update set tgt.TGT_MARKETID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_MARKETID �� DTXREPLOBJ_DBT', '���������� TGT_MARKETID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 50, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt '||CHR(13)||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_CURRENCYID)'||CHR(13)||CHR(10)||'when matched then update set tgt.TGT_CURRENCYID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_CURRENCYID �� DTXREPLOBJ_DBT', '���������� TGT_CURRENCYID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 45, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=30 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARTYID)'||CHR(10)||'when matched then update set tgt.TGT_PARTYID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_PARTYID �� DTXREPLOBJ_DBT', '���������� TGT_PARTYID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 40, 'merge /*+ parallel(4) */ into dtxdeal_tmp tgt '||CHR(10)||'using dfininstr_dbt sou on (sou.t_fiid=tgt.TGT_AVOIRISSID)'||CHR(10)||'when matched then update set tgt.TGT_AVOIRKIND=sou.t_avoirkind'||CHR(10)||'', 1, 
    '���������� ������ - ���������� TGT_AVOIRKIND', '���������� TGT_AVOIRKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 35, 'update dtxdeal_tmp set tgt_avoirissid = 2192, '||CHR(13)||CHR(10)||'TGT_isbasket=chr(88),'||CHR(13)||CHR(10)||'T_PAYDATE = T_SUPLDATE,'||CHR(13)||CHR(10)||'T_SUPLDATE = T_PAYDATE'||CHR(13)||CHR(10)||'where t_replstate=0 and t_avoirissid=-20', 1, 
    '���������� ������ - ���������� TGT_AVOIRISSID ��� �������', '���������� TGT_AVOIRISSID (2192) ��� �������', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 30, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=20 and t_objstate=0) sou on (sou.t_objectid=tgt.T_AVOIRISSID)'||CHR(10)||'when matched then update set tgt.TGT_AVOIRISSID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_AVOIRISSID �� DTXREPLOBJ_DBT', '���������� TGT_AVOIRISSID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 25, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PAYMCUR is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=10 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PAYMCUR)'||CHR(10)||'when matched then update set tgt.TGT_PAYMCUR=sou.t_destid', 1, 
    '���������� ������ - ���������� TGT_PAYMCUR �� DTXREPLOBJ_DBT', '���������� TGT_PAYMCUR', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 20, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_replstate=0 and T_PARENTID is not null) tgt '||CHR(10)||'using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.T_PARENTID)'||CHR(10)||'when matched then update set tgt.TGT_PARENTID=sou.T_DESTID', 1, 
    '���������� ������ - ���������� TGT_PARENTID �� DTXREPLOBJ_DBT', '���������� TGT_PARENTID', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 15, 'update /*+ # */ dtxdeal_tmp tgt set TGT_BOFFICEKIND =  '||CHR(13)||CHR(10)||'                                case T_KIND'||CHR(13)||CHR(10)||'                                when 30 then 101'||CHR(13)||CHR(10)||'                                when 40 then 101'||CHR(13)||CHR(10)||'                                when 70 then 117'||CHR(13)||CHR(10)||'                                when 80 then 117'||CHR(13)||CHR(10)||'                                when 90 then 117'||CHR(13)||CHR(10)||'                                when 100 then 158'||CHR(13)||CHR(10)||'                                when 110 then 140'||CHR(13)||CHR(10)||'                                else 101'||CHR(13)||CHR(10)||'                                end', 1, 
    '���������� ������ - ���������� TGT_BOFFICEKIND', '���������� TGT_BOFFICEKIND', NULL, 'X');
Insert into DTX_QUERY_DBT
   (T_OBJECTTYPE, T_SET, T_NUM, T_TEXT, T_SEVERITY, 
    T_DESC, T_NAME, T_USE_BIND, T_IN_USE)
 Values
   (80, 3, 10, 'merge /*+ # */ into (select * from dtxdeal_tmp where t_action>1) tgt using (select t_objectid, t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objstate=0) sou on (sou.t_objectid=tgt.t_dealid)'||CHR(13)||CHR(10)||'when matched then update set tgt.tgt_dealid=sou.t_destid', 1, 
    '���������� ������ - ���������� TGT_DEALID �� DTXREPLOBJ_DBT', '���������� TGT_DEALID (��� ��������/��������)', NULL, 'X');
COMMIT;
