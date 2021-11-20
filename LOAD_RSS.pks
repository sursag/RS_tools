CREATE OR REPLACE PACKAGE GEB_20210823_TST.load_rss
is
    -- ���������
    g_debug_output boolean := true;         -- ���������� ���������� ���������� � ����� DBMS_OUTPUT
    g_debug_table  boolean := false;        -- ���������� ���������� ���������� � �������
    g_oper constant number := 1;            -- ������������, ������� ����� ������������� �� ��� �������
    g_ourbank constant number := 12;        -- ��� ������ �����. ���������
    g_department  constant number := 1;     -- ����������� �� ���������. ��. ddp_dep_dbt
    g_is_initialized  boolean := false;
    g_fictive_comiss_contract number := null;   -- ��������� �������� �������� ( dsfcontr_dbt.t_id )
    
    g_use_needdemand constant boolean := true;  -- ������������ NEEDDEMAND � �������. ��������� ������ ����� �� ���������� ��� � ��������, ��� ������ ��������� ������ ��� �������
    
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
    c_RATE_TYPE_NOMINALONDATE   constant number := 100;  -- ���������� ��������� �/� �� ���� (������������� �� ������� ������)

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


                                      
    -- ������� ��� ������ �� SQL
    function GetDealKind( p_kind number, p_avoirissid number, p_market number, p_isbasket char, p_isksu char)    return number PARALLEL_ENABLE ;
    function GetIsQuoted(p_fi number, p_date date) return char DETERMINISTIC PARALLEL_ENABLE;
    function GetCurrentNom(p_fi number, p_date date) return number DETERMINISTIC PARALLEL_ENABLE;
    function GetRateType( p_tp number ) return number DETERMINISTIC PARALLEL_ENABLE;    
    function GetFictContract return number DETERMINISTIC PARALLEL_ENABLE;

    -- ��������� �������
    -- p_startdate ���������� ��������� ���� ����������
    -- p_enddate ���������� �������� ���� ����������. ������ ����� ������������� �� �� ���� �������������. ���� �� ������, ��� ����������� ������ (p_startdate + 1 ���� - 1 �������)
    -- p_stage ������� ����������� �������, ��� �������. ���� = 1, ����� ��������� ������ ���������� ������� TMP. ���� = 2, �� ������ ������� ������ � ������� �������
    -- ���� null - ����� ��������� ��� ����. ����� ������ �������� - ������.
    procedure load_deals_by_period(p_startdate date, p_enddate date default null, p_stage number default null);
    procedure load_demands_by_period(p_startdate date, p_enddate date default null, p_stage number default null);
    procedure load_courses_by_period(p_startdate date, p_enddate date default null, p_stage number default null);
    procedure load_comiss_by_period(p_startdate date, p_enddate date default null, p_stage number default null);


    g_my_SID  number; -- ��� ����� ���������� �� ���������� ������ � �.�.
    g_SESSION_ID number(10); -- ����� ������ ��� �����
    g_SESS_DETAIL_ID number(10); -- ����� �������, ��������� ��� ���� � ������ ��������� ���������. ��� �����
    
end load_rss;
/
