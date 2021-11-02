CREATE OR REPLACE PACKAGE GEB_20210823_TST.load_rss
is
    -- ���������
    g_limit constant number := 200000;             -- ���������� ������������ ����������� ������� ���������
    g_limit_demand constant number := 400000;      -- ���������� ������������ ����������� ������� ���������, �������� ��� ��������
    g_debug_output boolean := true;     -- ���������� ���������� ���������� � ����� DBMS_OUTPUT
    g_debug_table  boolean := false;    -- ���������� ���������� ���������� � �������
    g_debug_level_limit constant pls_integer := 3;    -- ������������ ������� �������� ���������, ������� ����� ������������. �������� ����������� �� 0 �� 10.
                                             -- �� ������ �� 5 ��������� ���������� � ���� �� ��������� �������, ������ ����������.
                                             -- �������, ��� �� 10 ����� � ��������� - ���������� �������.
                                             -- ��� ���������� ������� � ��������� < 10 ���� �������� �������� ������������� ������������� � 10, ����� ��������� �����������������
    g_debug_level_current pls_integer;
    g_oper constant number := 1;  -- ������������, ������� ����� ������������� �� ��� �������
    g_ourbank constant number := 12;     -- ��� ������ �����. ���������
    g_department  constant number := 1;  -- ����������� �� ���������. ��. ddp_dep_dbt
    
    c_DEFAULT_FICTFI  constant number := 2192;  -- ��������� FIID ��� ������ � ��������, �� ���������.
        
    -- ��������� �����
    c_OBJTYPE_MONEY     constant number  := 10;
    c_OBJTYPE_AVOIRISS  constant number  := 20;
    c_OBJTYPE_PARTY     constant number  := 30;
    c_OBJTYPE_MARKET    constant number  := 30;
    c_OBJTYPE_SECTION   constant number  := 40;
    c_OBJTYPE_WARRANT   constant number  := 50;
    c_OBJTYPE_PARTIAL   constant number  := 60;
    c_OBJTYPE_RATE      constant number  := 70;
    c_OBJTYPE_DEAL      constant number  := 80;
    c_OBJTYPE_PAYMENT   constant number  := 90;

    -- ����������� ���� ������
    c_RATE_TYPE_NKDONDATE       constant number := 15;        -- ��� ����� "��� �� ����"
    c_RATE_TYPE_NOMINALONDATE   constant number :=  100;  -- ���������� ��������� �/� �� ���� (������������� �� ������� ������)

    -- ���� ���������
    c_KINDPORT_TRADE    constant number := 1;
    c_KINDPORT_LOAN     constant number := 0; -- ���� ������
    c_KINDPORT_INVEST   constant number := 5;

    -- ���� ��������� � dpartyown_dbt
    c_PTK_CONTR         constant number := 7; 
    c_PTK_BROKER        constant number := 22;
    c_PTK_MARKETPLACE   constant number := 3;
    
    -- ���� ���������
    c_DL_NTGDOC         constant number := 140; 
    c_DL_RETIREMENT     constant number := 117;
    c_DL_SECURITYDOC    constant number := 101;
    c_DL_GET_DIVIDEND   constant number := 158;

    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- ��������� � ������������ ������ �� ����, ������� ������������ � ����������� TABLE()
    -- ��� ������� ��������� �� ���������
    type rate_sou_arr_type is table of DTXCOURSE_DBT%ROWTYPE;
    type rate_sou_add_type is record(   -- �������������� ����, ������������� � ������� �������� rate_sou_arr_type �� ��������
                                    tgt_rate_id number,  -- id ����� � ������� �������, 0 ���� ���
                                    tgt_rate_val number, -- �������� ����� �� ���� � ������� �������
                                    tgt_state   number,  -- ������ ����� �� replobj: 1-������ ���������, 2-������
                                    rate_date date,      -- ���� �������� �������� �����
                                    type_id  number,     -- ���������������� ���
                                    market_id number,    -- ���������������� ID �����
                                    section_id number,   -- ���������������� ID ������� �����
                                    base_fi number,      -- ���������������� ������� FI
                                    fi number,           -- ���������������� ���������� id
                                    isdominant char,     -- ������� ��������� ����� ������, ���� ��
                                    isrelative char,     -- ������� �������������� �����
                                    result number(1)     -- ������� t_replstate. 1 - ��������� �������, 2 - ������
                                  );
    type rate_sou_add_arr_type is table of rate_sou_add_type index by pls_integer;

    rate_sou_arr        rate_sou_arr_type;
    rate_sou_add_arr    rate_sou_add_arr_type;
    
    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- ��� ������� ������ 
    
    type deal_sou_arr_type is table of DTXDEAL_DBT%ROWTYPE;
    type deal_sou_add_type is record (
                                    tgt_dealid  number,
                                    tgt_avoirissid  number,
                                    tgt_currencyid  number,
                                    tgt_ogroup  number,
                                    tgt_paymcur number,
                                    tgt_nkdfiid number,
                                    tgt_type    number,
                                    tgt_market  number,
                                    tgt_sector  number,
                                    tgt_broker  number,
                                    tgt_party   number,
                                    tgt_parentid number,
                                    tgt_portfolioid number,
                                    tgt_repobase    number,
                                    tgt_warrant_num   number,
                                    tgt_partial_num   number,
                                    tgt_state   number,
                                    tgt_department number,
                                    tgt_bofficekind number,
                                    tgt_ismarketoper boolean,
                                    tgt_existback boolean,
                                    tgt_objtype number,
                                    tgt_portfolio number,
                                    tgt_kind    number,
                                    tgt_country varchar2(3),
                                    is_judicialoper boolean := false, -- ������ �� �������� ����, ��������� ����� ���� �������
                                    is_basket   boolean := false,
                                    is_maindeal char(1) := chr(0),  -- ������� ������ � �������� ���� � ��������. TODO �������. ����� ��
                                    is_matched_by_code boolean,
                                    is_buy  boolean,
                                    is_sale boolean,
                                    is_loan boolean,
                                    is_repo boolean,
                                    is_loan_to_repo boolean,
                                    begindate   date,
                                    enddate     date,
                                    DDL_TICK_BUF    ddl_tick_dbt%rowtype,  -- ��������� � ������� ������� ������ � �� ������� ���������
                                    DDL_LEG1_BUF    ddl_LEG_dbt%rowtype,
                                    DDL_LEG2_BUF    ddl_LEG_dbt%rowtype,
                                    result      number(1) := 0 -- 2 - ������
                                    );
    type deal_sou_add_arr_type  is table of deal_sou_add_type index by pls_integer;
    type deal_sou_back_arr_type is table of number index by pls_integer; -- ��������� ���������. ������������� DDL_TICK_DBT.DEALID, �������� ������ ������ ������
    
    type tmp_dealid_type        is record (T_DEALID number, T_BOFFICEKIND number );
    type tmp_dealid_arr_type    is table of tmp_dealid_type  index by pls_integer;
    type tmp_dealcode_type      is record (INDEX_NUM number, T_DEALCODE varchar2(30)); 
    type tmp_dealcode_arr_type  is table of tmp_dealcode_type  index by pls_integer;    
    
    deal_sou_arr        deal_sou_arr_type;      -- �������� ��������� ������
    deal_sou_add_arr    deal_sou_add_arr_type;  -- �������������� ��������� ������
    deal_sou_back       deal_sou_back_arr_type; -- ��������� ���������.
    ---------------------------------------------------------------------------------------------------------------------------------------------
    
    
    -------------------------------------------------------------------------------------------------------------------------------------------
    -- ��� ������� �������� 
    
    type demand_sou_arr_type is table of DTXDEMAND_DBT%ROWTYPE;
    demand_sou_arr      demand_sou_arr_type;
    -- ������ ��������������� ��������� ����� ��������� ������, ��������� ��� ����������� ���������,
    -- ��� ����������������. �����, ��������� ������� ����� ������������� �� ������ �� DTXDEMAND_DBT,
    -- �� � �� ������. ����� ����������� ��������� ��� �������� ����������.
    
    -- �� ����, ��� ��������� ����� �������� ������������ �������� ��������� �� ��������� (������� �����), �������������� ���������,
    -- ��������������� ��� ��, � ���������, ���������� ������ � ������� ������� ������� ( ������� �� �������� ����� )
    -- ����� �� �������������� ��������� ������������ �� ������� �����, � ��������. � ������ ��������� load_demand - ������ ��������
    -- ������ � add_demand, ����������� �� ��������� �� �� ���.
    type demand_type is record(
                            tgt_demandid    number,
                            tgt_docid   number,
                            tgt_party   number,
                            tgt_fiid    number,
                            tgt_dockind number,
                            r_action    number(1),
                            r_isauto    boolean,  
                            r_isfact    boolean,
                            r_isnetting boolean,
                            r_part      number(1),
                            r_kind      number(2), -- � ��������� ������� dtxdemand: 10-�������� �/�, 40-������, 
                            r_direction number(1), -- 1-����������, 2-�������������
                            r_fikind    number(2), -- 10-������, 20-������
                            r_state     number(2),
                            r_sum       number,
                            r_note      varchar2(500),
                            r_balancerate number,
                            r_date      date,
                            r_oldobjectid number,  -- ����� �� dtxdemand ��� ����������� replstate
                            r_subobjnum number,    -- ��� ������ ������������ � dtxreplobj.T_SUBOBJNUM
                            r_destsubobjnum number,-- ��� ������ ������������ � dtxreplobj.T_DESTSUBOBJNUM
                            r_result    number(1)  -- ������. 0 - ���������, 2 - ������ � ���������� �������
                            );
                                    

    -- �������� ���������. �����������, ����� ����� ������������    
    type demand_rq_arr_type is table of ddlrq_dbt%rowtype index by pls_integer;
    demand_rq_arr   demand_rq_arr_type;
    
    -- �������������� ��������� �������, ������� ������������ ��������� RQ ��� ������ � DTXDEMAND_DBT � DTXREPLOBJ_DBT. ���-�� ���� ������� DTXDEMAND_DBT.t_demandid 
    type demand_add_type is record(
                            r_oldobjectid number,
                            r_subobjnum number,
                            r_destsubobjnum number,
                            r_action    number,
                            r_result    number(1) -- ������. 0 - ���������, 2 - ����������� ������ ��� ������� �������
                            );
    type demand_add_arr_type is table of demand_add_type index by pls_integer;
    demand_add_arr      demand_add_arr_type; 
                       
                                      
    function GetDealKind( p_kind number, p_avoirissid number, p_market number, p_isbasket char, p_isksu char)    return number DETERMINISTIC;
                    
    -- ��������� ���������� ������� � ��� DEMAND_RQ_ARR
    procedure   add_demand (p_demand   demand_type);
    -- ��������� ������ �������� �� ���� � ��. ������������ � load_demands � load_deals
    procedure   write_demands( p_action number );                             


    -- �������� ��������� --
    procedure load_rates(p_date date, p_action number);
    procedure load_deals(p_date date, p_action number);
    procedure load_demands(p_date date, p_action number);
    

    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- ������ � ����� replobj
    -- �������� ������� ������ ��������������� ������, ����� ���������� ����� ������������ �� � ��������� TABLE
    -- �� ��� ��������� ������ ��� ����� ����������� ��������� ��������� ��������� replobj_rec_inx_arr
    type replobj_rec_type            is record(obj_type number, obj_id number, obj_sub_id number, dest_id number, state number, comment varchar2(100));
    type replobj_rec_arr_type        is table of replobj_rec_type index by pls_integer;  -- �������� ���������
    type replobj_rec_inx_arr_type    is table of pls_integer index by varchar2(200);     -- ��������� ���������, ��������� �� ������ ��������, ������������� ������������� ���������� ����� (objtype,obj_id,obj_sub_id)
    type replobj_tmp_arr_type        is table of dtxreplobj_dbt%rowtype;                 -- ��������� ����� ��� BULK COLLECT, ���� �� ���������� �� ���������� ����

    replobj_rec_arr             replobj_rec_arr_type;
    replobj_rec_inx_arr         replobj_rec_inx_arr_type; -- ��������� ���������
    replobj_tmp_arr             replobj_tmp_arr_type;

    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number := 0, p_comment varchar2 := null, p_destid number := -1 ); -- ���������� �������� ID � ���������
    procedure replobj_load; -- �������� ���������
    function  replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number := 0) return replobj_rec_type; -- ������� �������� �� ���� REPLOBJ
    -- procedure savestat;   -- �������� � ��� ���������� �� ������ ���� � ������� �����
    -- procedure replobj_clear; -- ������� ���� ���������
    ----------------------------------------------------------------------------------------------------------------------------------------------

    -- ������ � ����������� �������� � ������������.
    -- ���������� � ���������, ����� ������� ���������
    type note_type is record(
                        r_objtype   number(5),
                        r_struniid  varchar2(50),
                        r_kind      number,
                        r_value     varchar2(500),
                        r_date      date
                        );
    type note_arr_type  is table of note_type index by pls_integer;
    note_arr    note_arr_type;
    
    type categ_type is record(
                        r_objtype   number(5),
                        r_struniid  varchar2(50),
                        r_kind      number,
                        r_value     varchar2(500),
                        r_date      date
                        );
    type categ_arr_type  is table of categ_type index by pls_integer;
    categ_arr    categ_arr_type;
    
    -- � ��������� � ���
    procedure add_note( p_objtype number, p_objectid number, p_kind number, p_value varchar2, p_date  date);
    procedure write_notes;
    procedure add_categ( p_objtype number, p_objectid number, p_kind number, p_value varchar2, p_date  date);
    procedure write_categs;
    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- ������ � �����
    type log_rows_type is table of dtxloadlog_dbt%rowtype;  -- ��� ���������� ������ � ���.
    log_rows  log_rows_type;
    -- ��������� ���������������� ������ � ���
    procedure add_log( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2, p_date date); -- ������ � ���
    -- ��������� ���������� ������. ����� ������� ������� ����� ����������� � ����� ����������� ��������, ����� �������� ������ � ���������, ����� ����������� � ��� ����� ������
    -- procedure add_log_deferred( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- ������ � ���
    -- procedure add_log_exec; -- ��������� � ������� ����. ������� �������� ���������.
    ---------------------------------------------------------------------------------------------------------------------------------------------

    procedure deb(p_text varchar2, num1 number default null, num2 number default null, num3 number default null, p_level pls_integer := 1);
    
    procedure deb_initialize(p_output boolean, p_table boolean);

    type tmp_arr_type is table of number index by pls_integer;
    type tmp_reverse_arr_type is table of number index by varchar2(100);    
    type dpartyown_arr_type is table of DPARTYOWN_DBT%ROWTYPE index by pls_integer; -- ��� ����� ������� � �������  DPARTYOWN_DBT
    
    type tmp_varchar_arr_type is table of varchar2(100) index by pls_integer;  -- ����� ���������������
    type tmp_varchar_back_arr_type is table of pls_integer index by varchar2(100);  -- ����� ���������������

    my_SID  number; -- ��� ����� ���������� �� ���������� ������ � �.�.
    g_SESSION_ID number(10);
    g_SESS_DETAIL_ID number(10);
    
end load_rss;
/
