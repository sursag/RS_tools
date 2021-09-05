DROP PACKAGE GEB_20210823.LOAD_RSS;

CREATE OR REPLACE PACKAGE GEB_20210823.load_rss
is
    -- ���������
    g_limit number := 1000;             -- ���������� ������������ ����������� ������� ���������
    g_debug_output boolean := true;     -- ���������� ���������� ���������� � ����� DBMS_OUTPUT
    g_debug_table  boolean := false;    -- ���������� ���������� ���������� � �������
    g_debug_level_limit pls_integer := 5;    -- ������������ ������� �������� ���������, ������� ����� ������������. �������� ����������� �� 0 �� 10.
    g_oper number := 1;  -- ������������, ������� ����� ������������� �� ��� �������
        
    -- ��������� �����
    c_OBJTYPE_RATE constant number := 70;
    c_OBJTYPE_MONEY constant number  := 10;
    c_OBJTYPE_SEC  constant number  := 20;
    c_OBJTYPE_MARKET  constant number  := 30;
    c_OBJTYPE_MARKET_SECTION  constant number  := 40;

    c_RATE_TYPE_NKDONDATE constant number := 15;        -- ��� ����� "��� �� ����"
    c_RATE_TYPE_NOMINALONDATE constant number :=  100;  -- ���������� ��������� �/� �� ���� (������������� �� ������� ������)



    ---------------------------------------------------------------------------------------------------------------------------------------------
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



    -- �������� ��������� --
    procedure load_rate(p_date date, p_action number);

    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- ������ � ����� replobj
    -- �������� ������� ������ ��������������� ������, ����� ���������� ����� ������������ �� � ��������� TABLE
    -- �� ��� ��������� ������ ��� ����� ����������� ��������� ��������� ��������� replobj_rec_inx_arr
    type replobj_rec_type            is record(obj_type number, obj_id number, obj_sub_id number, dest_id number, state number, comment varchar2(100));
    type replobj_rec_arr_type        is table of replobj_rec_type index by pls_integer;  -- �������� ���������
    type replobj_rec_inx_arr_type    is table of pls_integer index by varchar2(200);     -- ��������� ���������, ��������� �� ������ ��������, ������������� ������������� ���������� ����� (objtype,obj_id,obj_sub_id)
    type replobj_tmp_arr_type        is table of dtxreplobj_dbt%rowtype;                 -- ��������� ����� ��� BULK COLLECT, ���� �� ���������� �� ���������� ����

    replobj_rec_arr             load_rss.replobj_rec_arr_type;
    replobj_rec_inx_arr         replobj_rec_inx_arr_type; -- ��������� ���������
    replobj_tmp_arr             replobj_tmp_arr_type;

    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number := 0, p_comment varchar2 := null, p_destid number := -1 ); -- ���������� �������� ID � ���������
    procedure replobj_load; -- �������� ���������
    function  replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number := 0) return replobj_rec_type; -- ������� �������� �� ���� REPLOBJ
    -- procedure savestat;   -- �������� � ��� ���������� �� ������ ���� � ������� �����
    -- procedure replobj_clear; -- ������� ���� ���������
    ----------------------------------------------------------------------------------------------------------------------------------------------

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
    
end load_rss;
/
DROP PACKAGE BODY GEB_20210823.LOAD_RSS;

CREATE OR REPLACE PACKAGE BODY GEB_20210823.load_rss
is

    procedure deb_empty(p_line char := null) -- ������ ������ � �����
    is
    begin 
        -- ��� ������������������
        -- ����� �������� � ������������ ������. ���� ���������� �������� ����, ��������� �� ������������.
        if not (g_debug_output or g_debug_table) 
            then return; 
        end if;
        if g_debug_output 
        then
            if p_line is not null 
            then
                dbms_output.put_line(rpad(p_line,80,p_line));
            else
                dbms_output.put_line(' ');
            end if;
        end if;        
    end deb_empty;


    -- ��������� ������� ����������� ������� � ���������. ���� ��� ���, ����������. ���� �����������, ���������. 
    -- ���� ���������� p_destid, ��������� ������ ������������ ��� �������� � ������, ���� ��� ����. ��� ������ ������� ����������
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number default 0, p_comment varchar2 := null, p_destid number := -1 )
    is
        v_searchstr varchar2(200);
        mas_idx pls_integer;
    begin
        -- ��������� ��������� ������, ���� � � ��������� �������. ���� ������� �� ������, ������� ��� � �������� ������
        -- ����� ����������� � ���������. ���� ��������� � ������ ���������� DEST_ID - � ������, ���� ��� ������� ����� ���� � DRATEDEF_DBT
        -- � �� �� �����, ����� ��������� ������ ���������� �� ��������� ������ ����� ������ ������ DEST_ID ������ - ������� �� ������ IF
        -- ������ �� ���� �������� �� ��������� �� ����� ������� �� ���������, ������ ����������� �����. ��������, ����� ��� ������.
        v_searchstr := to_char(p_objtype) || '#' || to_char(p_obj_id) || '#' || to_char(p_obj_sub_id);
        if not replobj_rec_inx_arr.exists(v_searchstr) then
            mas_idx := nvl(replobj_rec_arr.last,-1)+1;  -- count ������, '-1'� ������� ����� ���������
            replobj_rec_arr(mas_idx).obj_type := p_objtype;
            replobj_rec_arr(mas_idx).obj_id := p_obj_id;
            replobj_rec_arr(mas_idx).obj_sub_id := p_obj_sub_id;
            replobj_rec_arr(mas_idx).comment := p_comment;
            replobj_rec_arr(mas_idx).dest_id := -1;  
            -- ������ � ��������� ���������
            replobj_rec_inx_arr( v_searchstr ) := mas_idx;
        end if;
        
        if  p_destid <> -1
        then 
            deb( '��������� � ������ replobj: ��� �������� ' || v_searchstr || ' �������� dest_id ����������� �� #1 � #2',
                replobj_rec_arr(  replobj_rec_inx_arr( v_searchstr )  ).dest_id, p_destid, p_level => 4 );
            replobj_rec_arr(  replobj_rec_inx_arr( v_searchstr )  ).dest_id := p_destid;
        end if;
        
        return;
    end replobj_add;

    -- ���������� �� ���� ������ ���������� �������. ���� ����������� � ����, ���������� -1
    -- ��������� ������� �������� � ��������� ���������. ���� ����, ����� �� ��������� �������� �������,
    -- �� ���� �������� ������� � ��������.
    function replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number default 0) return replobj_rec_type
    is
        v_searchstr varchar2(200);
    begin
        v_searchstr := to_char(p_objtype) || '#' || to_char(p_obj_id) || '#' || to_char(p_obj_sub_id);
        if replobj_rec_inx_arr.exists(v_searchstr) then
            return replobj_rec_arr( replobj_rec_inx_arr(v_searchstr));
        end if;
        return replobj_rec_arr( -1 );  -- ��� ���� ���� ������ �������
    end replobj_get;


    -- ��������� ������ �� ������, � ������� ��� DEST_ID � ����
    procedure replobj_load
    is
        emerg_limit number := 40000;
        v_search_str varchar2(200);
        v_search_idx pls_integer;
        err_count pls_integer := 0;
        v_counter pls_integer;
        v_counter_str varchar2(100);
    begin
        deb_empty;
        deb( '�������� ���������  replobj_load');
        deb( '���������� ������� � ������ replobj: ' ||load_rss.replobj_rec_arr.count() );
        
        if g_debug_level_limit > 3 then
            deb( '>  ���������� ������� � ������ replobj ����� ���������: ' ||load_rss.replobj_rec_arr.count() );
            v_counter := load_rss.replobj_rec_arr.first; 
            while (v_counter is not null)
            loop
                deb( '> ����� - ��� - object_id - destid:      ' || v_counter || '\t\t' || load_rss.replobj_rec_arr(v_counter).obj_type || '\t\t' || load_rss.replobj_rec_arr(v_counter).obj_id || '\t\t' || load_rss.replobj_rec_arr(v_counter).dest_id);
                v_counter := replobj_rec_arr.next(v_counter);
            end loop;
            deb( '>  ���������� ������� � ��������� ������� replobj ����� ���������: ' ||load_rss.replobj_rec_inx_arr.count() );
            v_counter_str := load_rss.replobj_rec_inx_arr.first; 
            while (v_counter_str is not null)
            loop
                deb( '> ������ - ��������:      ' || v_counter_str || '\t\t' || load_rss.replobj_rec_inx_arr(v_counter_str));
                v_counter_str := replobj_rec_inx_arr.next(v_counter_str);
            end loop;
        end if;        
        
        
        --deb( '���������� ������� � ������ replobj_tmp_arr: ' ||load_rss.replobj_tmp_arr.count() );
        SELECT ro.* BULK COLLECT INTO replobj_tmp_arr
        FROM dtxreplobj_dbt ro, (select * from table(replobj_rec_arr)) inp where t_objecttype = inp.obj_type and t_objectid = inp.obj_id and (t_subobjnum = obj_sub_id or obj_sub_id = 0) and t_objstate != 2 and inp.dest_id=-1 and inp.obj_id>0;

        deb( '��������� #1 �������', SQL%ROWCOUNT);
        IF SQL%ROWCOUNT = emerg_limit THEN
            null;
            -- ������� ����� �������, ������������ ����������
        END IF;

        -- ��������� �������� � �������� ��� ��������� � replid
        FOR i in 1..replobj_tmp_arr.count
        LOOP
            -- ���������� ������ �������� � �������� �������
            v_search_str := to_char(replobj_tmp_arr(i).t_objecttype) || '#' || to_char(replobj_tmp_arr(i).t_objectid) || '#' || nvl(to_char(replobj_tmp_arr(i).t_subobjnum),'0');
            v_search_idx := replobj_rec_inx_arr(v_search_str);  -- ����� ������ ������ � �������� �������
            deb( '>  ������_�_������� - ���������_������ - ��������_t_destid: \t\t' ||v_search_idx || '\t\t' ||v_search_str || '\t\t' || replobj_tmp_arr(i).T_DESTID);
            -- �������� �������
            replobj_rec_arr(v_search_idx).dest_id := replobj_tmp_arr(i).T_DESTID;
            replobj_rec_arr(v_search_idx).state := replobj_tmp_arr(i).t_objstate;
        END LOOP;

        -- ����� ��� ������ �� �������������, ���������� ������ �� � DREPLOBJ_DBT. ��������� �������.
        IF  replobj_rec_inx_arr.exists('10#0#0') 
        THEN
            replobj_rec_arr( replobj_rec_inx_arr('10#0#0') ).dest_id := 0;
            replobj_rec_arr( replobj_rec_inx_arr('10#0#0') ).state   := 0;
        END IF;
            

        -- ������� �������, ������� ����� ������������, ��� NULL
        replobj_rec_arr(-1).obj_type     := 0;
        replobj_rec_arr(-1).obj_id      := -1;
        replobj_rec_arr(-1).obj_sub_id  := 0;
        replobj_rec_arr(-1).dest_id     := -1;
        replobj_rec_arr(-1).state       := 0;
        
        if g_debug_level_limit > 3 then
            deb( '>  ���������� ������� � ������ replobj ����� ��������: ' ||load_rss.replobj_rec_arr.count() );
            v_counter := load_rss.replobj_rec_arr.first; 
            while (v_counter is not null)
            loop
                deb( '> ����� - ��� - object_id - destid:      ' || v_counter || '\t\t' || load_rss.replobj_rec_arr(v_counter).obj_type || '\t\t' || load_rss.replobj_rec_arr(v_counter).obj_id || '\t\t' || load_rss.replobj_rec_arr(v_counter).dest_id);
                v_counter := replobj_rec_arr.next(v_counter);
            end loop;
            deb( '>  ���������� ������� � ��������� ������� replobj ����� ��������: ' ||load_rss.replobj_rec_inx_arr.count() );
            v_counter_str := load_rss.replobj_rec_inx_arr.first; 
            while (v_counter_str is not null)
            loop
                deb( '> ������ - ��������:      ' || v_counter_str || '\t\t' || load_rss.replobj_rec_inx_arr(v_counter_str));
                v_counter_str := replobj_rec_inx_arr.next(v_counter_str);
            end loop;
        end if; 
        
        deb('��������� ���������  replobj_load');
    end replobj_load;



    -- ����� ���������� ����������
    -- ������� 0 ���������� ������ ������ �������� �������� � ���������� ����������
    -- ������� 1 ���������� ���������� �� ����������
    -- ������� 2 ���������� ���������� ������ REPLOBJ
    -- ������� 3 ���������� ����� ��������� �� ������ ������
    -- ������� 5 ���������� ����������� �� ������ ������
    procedure deb( p_text varchar2, num1 number default null, num2 number default null, num3 number default null, p_level pls_integer := 1)
    is
        l_id number;
        l_text varchar2(1000) := p_text;
        l_delim varchar2(50) := ''; --trim(substr(dbms_utility.format_call_stack, 99,10));
    begin
        -- ��� ������������������
        -- ����� �������� � ������������ ������. ���� ���������� �������� ����, ��������� �� ������������.
        if not (g_debug_output or g_debug_table) or (p_level > g_debug_level_limit)
        then return; 
        end if;
        
        l_text := replace(l_text, '\t', chr(9));
        if num1 is not null
        then 
            l_text := replace(l_text, '#1', num1);
        end if;
        if num2 is not null
        then 
            l_text := replace(l_text, '#2', num2);
        end if;
        if num3 is not null
        then 
            l_text := replace(l_text, '#3', num3);
        end if;
    
        if p_level > 1 then
            l_delim := l_delim || ' >>>>>';
        else
            l_delim := l_delim || ' >>';
        end if;
    
        if g_debug_output 
        then
            dbms_output.put_line(to_char(sysdate,'hh24:mi:ss') || '   ' || l_delim || '   ' || l_text);
        end if;
        if g_debug_table 
        then
            null;
        end if;
    end deb;
    
    -- ��������� ����������� ������
    procedure deb_initialize(p_output boolean, p_table boolean)
    is 
    begin
        g_debug_output := p_output;
        g_debug_table  := p_table;
    end deb_initialize;




    -------------------------------------------------
    procedure load_rate(p_date date, p_action number)
    is
        -- �������� ��������� - ��������� ������ �� n �������, �������� id ���� ��������� ��������� � ��������� �� �������.
        -- ����� �� ������� ������� ��������� �������� � ������� ������, ��������������� �������������� �������.
        -- ������ � ������������ � �������� action � ��������/����������� ����� � ������� ������� ��������� ����� �������.
        -- ��� dratedef_dbt ������ ������� ����� ������ �����������. ���� ���-�� �� ��������� ��� �������� �������, ����� ��������������� ��������, ��� ������ ������.
        -- ��� dratehist_dbt ��������� ��� ������ - �� ������� � ��������. ������ ������������ ���������� �������/��������, ��������� ������.
        -- � ����� ������� ������ ����������� ��������� �������� �����. ���������� ������� � ratehist � ������ dratedef_dbt
        cursor m_cur(pp_date date, pp_action number) is select * from DTXCOURSE_DBT where t_instancedate between pp_date and pp_date+1 and t_replstate=0 and t_action = pp_action order by t_instancedate, t_action, t_basefiid, t_marketsectorid, t_type;

        type index_collection_type is table of number index by varchar2(100);  -- ��� ��������� ���������, ������������ ��� ������ ������ ���������.



        ---------------------------------------------------------------------------------------------------------------------------------------------
        -- ��� ������
        type dratedef_arr_type is table of dratedef_dbt%rowtype index by pls_integer;
        -- ��� ������� �����
        type add_dratehist_arr_type is table of dratehist_dbt%ROWTYPE index by pls_integer;
        type del_dratehist_type is record( t_rateid number, t_sincedate date );
        type del_dratehist_arr_type is table of del_dratehist_type index by pls_integer;
        ---
        dratedef_arr_tmp  dratedef_arr_type;       --  IN, ��� bulk collect
        dratedef_arr      dratedef_arr_type;       --  IN, ��������� ������������� rate_id �� ������� �������
        dratehist_arr     add_dratehist_arr_type;  --  IN, ������ �� ������� �������.
        dratehist_ind_arr  index_collection_type;  --  IN, ��������� ��������� ��� ������ � �������. ������ ������� ��������� dratehist_arr
        new_dratehist_arr  add_dratehist_arr_type; --  OUT, ����� ��� ������� ������ � �������.
        del_dratehist_arr  del_dratehist_arr_type; --  OUT, ����� ��� �������� ������ �� �������.
        new_dratedef_arr   dratedef_arr_type;      --  OUT, ����� ��� ��������� ������ �����. ������ � ������� ������� ������ ��������. ���� ���� ����� - ������ �������� � ������ ������ � update. ���� delete - ������� �����
        ---------------------------------------------------------------------------------------------------------------------------------------------
        -- ��� ��������������� ���������
        type ind_nominal_type is record (T_FIID number, T_BEGDATE date, T_FACEVALUE number);
        type ind_nominal_arr_type is table of ind_nominal_type index by pls_integer;
        type ind_nominal_tmp_type is table of dv_fi_facevalue_hist%ROWTYPE;  
        ---
        ind_nominal_arr ind_nominal_arr_type;
        ind_nominal_tmp ind_nominal_tmp_type;
        ind_nominal_flag  boolean;
        ---------------------------------------------------------------------------------------------------------------------------------------------

        add_tmp  rate_sou_add_type;
        main_tmp DTXCOURSE_DBT%ROWTYPE;
        stat_tmp number;
        rateind_tmp number;
        rateindstr_tmp varchar2(100);
        rate_tmp number; -- ���� �� ��� � ������� ���� �� �������� ����
        is_last_date boolean;  -- ���� �� ��� ���� ����� � dratedef_dbt, �� ���� ���� ����� ������
        dratedef_tmp dratedef_dbt%rowtype;
        dratehist_tmp dratehist_dbt%rowtype;
        ---------------------------------------------------------------------------------------------------------------------------------------------




        -- �������� �������� � �������. �������� ������ � ��������� ������ �� ���������.
        procedure pr_exclude(p_code number, p_objtype number, p_id number, p_subnum number := 0, p_text varchar2, p_counter number, p_action number, p_silent boolean := false)
        is
            text_corr varchar2(1000);
            v_row DTXCOURSE_DBT%ROWTYPE;
        begin
            deb('�������� ���������  pr_exclude');
            v_row := rate_sou_arr(p_counter);
            text_corr := replace(p_text, '%act%',  (case p_action when 1 then '�������' when 2 then '���������' when 3 then '��������' end) );
            text_corr := replace(text_corr, '%fiid%', v_row.t_fiid);
            text_corr := replace(text_corr, '%basefiid%', v_row.t_basefiid);
            text_corr := replace(text_corr, '%type%', v_row.t_type);
            text_corr := replace(text_corr, '%date%', to_char(v_row.t_ratedate,'dd.mm.yyyy'));
            -- ����� �������� �� add_log_deferred
            if not p_silent
            then
                add_log( p_code, p_objtype, p_id, p_subnum, text_corr, p_date);
            end if;

            -- ��������� �������
            rate_sou_add_arr(p_counter).result := 2;

        end pr_exclude;
        
        -- ������ ���������� �������.
        procedure pr_include( p_counter number)
        is
        begin
            deb('������ ���������� �������! ��������� pr_include ��� ������ ����� #1', p_counter, p_level => 3);
            
            rate_sou_add_arr(p_counter).result := 1;

        end pr_include;
        


        -- ������������� ����� ������
        function RTYPE( p_tp number ) return number
        is
            v_rez number;
        begin
            deb('�������� ������� RTYPE');
            case
                when p_tp = 26 then v_rez := 23;
                when p_tp = 10 then v_rez := 23;
                when p_tp = 27 then v_rez := 1001;
                when p_tp = 28 then v_rez := 1002;
                when p_tp = 29 then v_rez := 1003;
                -- ������ �������
                when p_tp = 1 then v_rez := 2;
                when p_tp = 2 then v_rez := 3;
                when p_tp = 3 then v_rez := 4;
                when p_tp = 4 then v_rez := 15;
                when p_tp = 5 then v_rez := 9;
                when p_tp = 6 then v_rez := 7;
                when p_tp = 7 then v_rez := 16; --KD 16 - ����� ����� "����� ������"
                -------------------------------------------------------------------------
                else v_rez := -p_tp;
            end case;
            return v_rez;
        end;


        -- ���������� ������ � ����� dratedef
        procedure add_dratedef_buf( buf dratedef_dbt%rowtype := null, T_RATEID number := null, T_FIID number := null, T_OTHERFI number := null, T_NAME varchar2 := null, T_DEFINITION varchar2 := null, T_TYPE number := null, T_ISDOMINANT char := null, T_ISRELATIVE char := null, T_INFORMATOR number := null, T_MARKET_PLACE number := null, T_ISINVERSE char := null, T_RATE number := null, T_SCALE number := null, T_POINT number := null, T_INPUTDATE date := null, T_INPUTTIME date := null, T_OPER number := null, T_SINCEDATE date := null, T_SECTION number := null, T_ISMANUALINPUT char := null)
        is
            tmp dratedef_dbt%rowtype;
        begin
            if buf.t_rateid is not null then
                    tmp := buf;
            end if;

            if  T_FIID is not null          then    tmp.t_fiid := T_FIID;               end if;
            if  T_OTHERFI is not null       then    tmp.T_OTHERFI := T_OTHERFI;         end if;
            if  T_DEFINITION is not null    then    tmp.T_DEFINITION := T_DEFINITION;   end if;
            if  T_NAME is not null          then    tmp.T_NAME := T_NAME;               end if;
            if  T_TYPE is not null          then    tmp.T_TYPE := T_TYPE;               end if;
            if  T_ISDOMINANT is not null    then    tmp.T_ISDOMINANT := T_ISDOMINANT;   end if;
            if  T_ISRELATIVE is not null    then    tmp.T_ISRELATIVE := T_ISRELATIVE;   end if;
            if  T_INFORMATOR is not null    then    tmp.T_INFORMATOR := T_INFORMATOR;   end if;
            if  T_MARKET_PLACE is not null  then    tmp.T_MARKET_PLACE := T_MARKET_PLACE;   end if;
            if  T_ISINVERSE is not null     then    tmp.T_ISINVERSE := T_ISINVERSE;     end if;
            if  T_RATE is not null          then    tmp.T_RATE := T_RATE;               end if;
            if  T_SCALE is not null         then    tmp.T_SCALE := T_SCALE;             end if;
            if  T_POINT is not null         then    tmp.T_POINT := T_POINT;             end if;
            if  T_INPUTDATE is not null     then    tmp.T_INPUTDATE := T_INPUTDATE;     end if;
            if  T_INPUTTIME is not null     then    tmp.T_INPUTTIME := T_INPUTTIME;     end if;
            if  T_OPER is not null          then    tmp.T_OPER := T_OPER;               end if;
            if  T_SINCEDATE is not null     then    tmp.T_SINCEDATE := T_SINCEDATE;     end if;
            if  T_SECTION is not null       then    tmp.T_SECTION := T_SECTION;         end if;
            if  T_ISMANUALINPUT is not null then    tmp.T_ISMANUALINPUT := T_ISMANUALINPUT; end if;

            if T_RATEID = 0 and nvl(tmp.t_rateid,0) = 0
            then
                tmp.t_rateid := dratedef_dbt_seq.nextval;
                dratedef_arr(  tmp.t_rateid ) := tmp;
            end if;

            --TODO  new_dratedef_arr( new_dratedef_arr.count() ) := tmp;
            insert into dratedef_dbt values tmp;

        end add_dratedef_buf;
        
        
        
        --==================================================================================================
        procedure execute_rate( i pls_integer)
        is begin
                    deb('! ������������� ������, ���������/����. BASEFIID=#1, �������=#2, ����=' || to_char(add_tmp.rate_date,'DD.MM.YYYY'), add_tmp.base_fi, main_tmp.t_rate, p_level => 5);
                               
                    -- ��������� ����
                    
                    is_last_date := false;
                    rateindstr_tmp := add_tmp.tgt_rate_id || '#' || to_char(add_tmp.rate_date, 'ddmmyyyy');
                    if dratehist_ind_arr.exists( rateindstr_tmp)
                    then
                        rate_tmp := dratehist_arr( dratehist_ind_arr ( rateindstr_tmp)).t_rate;  -- �������� ����� �� ���� ������� � ������� �������
                    else
                        -- ��������, �� ��������� �� ��� ����
                        if  dratedef_arr.exists( add_tmp.tgt_rate_id ) and ( dratedef_arr( add_tmp.tgt_rate_id ).t_sincedate = add_tmp.rate_date )
                        then
                            rate_tmp := dratedef_arr( add_tmp.tgt_rate_id ).t_rate;
                            is_last_date := true;
                        else
                            rate_tmp := 0;  -- �������� ����� �� ���� ����������� � ������� �������.
                        end if;
                    end if;

                    case p_action
                    when 1 then -- ����������
                        deb('���� 3 - ����� ���������� ������', p_level => 5);
                        if rate_tmp > 0 then  -- ������: ��� ���������� ������ ���� �� ����� ����
                                deb('���� 3 - ��� ���������� ���� �� ����', p_level => 5);
                                -- ��������, ����������� �� �� � ������������
                                if rate_tmp <> rate_sou_arr(i).t_rate
                                then
                                   pr_exclude(418, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ��� ���������� ������ ���� �� ���� %date% �� �������������� %basefiid%, ��� ����� - %type%', i, p_action);
                                   --continue;
                                end if;
                                -- ���� ���� ���������� �������, �� ������ ������
                        else    -- �������� ����� �� ���� ��� ��� (��� ����� ��� ������), ������� �������
                                deb('���� 3 - ����� �� ���� ��� ���', p_level => 5);
                                if  not dratedef_arr.exists(add_tmp.tgt_rate_id) or add_tmp.rate_date > dratedef_arr( add_tmp.tgt_rate_id ).t_sincedate
                                then
                                        -- ���� ������ ����� ������ ��������, ���� �������������� dratedef
                                        deb('���� 3 - ������ ����� ������ �������� �����', p_level => 5);
                                        dratedef_tmp.T_RATE := main_tmp.t_rate * power(10, main_tmp.t_point);
                                        dratedef_tmp.T_SINCEDATE := main_tmp.t_ratedate;
                                        dratedef_tmp.T_SCALE := main_tmp.t_scale;
                                        dratedef_tmp.T_POINT := main_tmp.t_point;

                                        if (add_tmp.tgt_rate_id = 0 ) or not dratedef_arr.exists( add_tmp.tgt_rate_id ) -- ������ ����� ������ �� ����
                                        then
                                            deb('���� 3 - ����� ��� ���, ������� ������ � dratedef_dbt', p_level => 5);
                                            dratedef_tmp.t_rateid := dratedef_dbt_seq.nextval;
                                            
                                            -- ���������� ��������� ���� ������
                                            dratedef_tmp.t_fiid := add_tmp.fi;
                                            dratedef_tmp.t_otherfi := add_tmp.base_fi;
                                            dratedef_tmp.t_name := chr(1);
                                            dratedef_tmp.T_DEFINITION := chr(1);
                                            dratedef_tmp.T_TYPE := add_tmp.type_id;
                                            dratedef_tmp.T_ISDOMINANT := add_tmp.isdominant;
                                            dratedef_tmp.T_ISRELATIVE := add_tmp.isrelative;
                                            dratedef_tmp.T_INFORMATOR := add_tmp.market_id;
                                            dratedef_tmp.T_MARKET_PLACE := add_tmp.market_id;
                                            dratedef_tmp.T_ISINVERSE := chr(0); 
                                            dratedef_tmp.T_SECTION := case when add_tmp.section_id = -1 then 0 else add_tmp.section_id end;  
                                            dratedef_tmp.T_ISMANUALINPUT :=  chr(0);
                                            dratedef_tmp.T_INPUTDATE := trunc(sysdate);
                                            dratedef_tmp.T_INPUTTIME := date'0001-01-01' + (sysdate - trunc(sysdate));
                                            dratedef_tmp.T_OPER :=  g_oper;

                                            insert into dratedef_dbt values dratedef_tmp;
                                            --TODO  add_dratedef_buf(dratedef_tmp);  -- ������ � ���� � � �����
                                            --commit;
                                            
                                            delete from DTXREPLOBJ_DBT where T_OBJECTTYPE=70 and t_objectid = main_tmp.t_courseid; 
                                            insert into DTXREPLOBJ_DBT (T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE) values(70, main_tmp.t_courseid, main_tmp.t_type, dratedef_tmp.t_rateid, 1, 0 );
                                            pr_include(i);
                                            
                                            -- ������ �������� 3 ������ - replobj, rate_sou_add_arr � dratedef_arr
                                            -- ������ ������� ������
                                            deb('���� 3 - ��������� ������ � ����� ����� � ������', p_level => 5);
                                            dratedef_arr( dratedef_tmp.t_rateid ) := dratedef_tmp;
                                            -- ����� ��������� ������� ����� ���� ��, ��� ��������� � ����� �� �����
                                            for j in i..rate_sou_arr.count  -- �� ������� ������ �� �����
                                            loop
                                                if rate_sou_arr(j).t_courseid=main_tmp.t_courseid and rate_sou_arr(j).t_type=main_tmp.t_type
                                                then
                                                    rate_sou_add_arr(j).tgt_rate_id := dratedef_tmp.t_rateid;               
                                                end if;
                                            end loop;
                                            replobj_add( c_OBJTYPE_RATE, rate_sou_arr(i).T_COURSEID, rate_sou_arr(i).T_TYPE, p_destid => dratedef_tmp.t_rateid);       
                                            add_tmp.tgt_rate_id := dratedef_tmp.t_rateid;             
                                            
                                        else
                                            -- ��������� �� dratedef_dbt �������� � dratehist_dbt
                                            deb('���� 3 - ��������� ������ �� dratedef_dbt � dratehist_dbt', p_level => 5);
                                            begin
                                                INSERT INTO dratehist_dbt(t_rateid, t_isinverse, t_rate, t_scale, t_point, t_inputdate, t_inputtime, t_oper, t_sincedate, t_ismanualinput)
                                                SELECT t_rateid, chr(0),  t_rate , t_scale, t_point, t_inputdate, t_inputtime, t_oper, t_sincedate, chr(0)
                                                FROM dratedef_dbt where t_rateid = add_tmp.tgt_rate_id;
                                                deb('���� 3 - ���������� #1 �����', SQL%ROWCOUNT, p_level => 5);
                                                
                                            exception
                                                when dup_val_on_index then null;  -- ��� ��������� ������ � ������������ �������
                                            end;

                                            dratedef_tmp.t_rateid := add_tmp.tgt_rate_id;
                                            update dratedef_dbt set T_RATE = dratedef_tmp.T_RATE, T_SINCEDATE = dratedef_tmp.T_SINCEDATE, T_SCALE = dratedef_tmp.T_SCALE, T_POINT = dratedef_tmp.T_POINT where t_rateid = add_tmp.tgt_rate_id ;
                                            pr_include(i);
                                        end if;
                                        -- ��������� ��� ��������� �����
                                        dratedef_arr(  add_tmp.tgt_rate_id ) := dratedef_tmp;
                                else  -- �� ����� ������ ����
                                    deb('���� 3 - �������������� ������������ ����', p_level => 5);
                                      -- ������������ ����� ������ � �������
                                    dratehist_tmp.t_rateid := add_tmp.tgt_rate_id;
                                    dratehist_tmp.t_isinverse := chr(0);
                                    dratehist_tmp.t_rate := main_tmp.t_rate * power(10, main_tmp.t_point);
                                    dratehist_tmp.t_scale := main_tmp.t_scale;
                                    dratehist_tmp.t_point := main_tmp.t_point;
                                    dratehist_tmp.t_inputdate := trunc(sysdate);
                                    dratehist_tmp.t_inputtime := date'0001-01-01' + (sysdate - trunc(sysdate));
                                    dratehist_tmp.t_oper := g_oper;
                                    dratehist_tmp.t_sincedate := main_tmp.t_ratedate;
                                    dratehist_tmp.t_ismanualinput := chr(0);

                                    insert into dratehist_dbt values dratehist_tmp;
                                    -- ������� � ����� �� ������ �����.
                                    dratehist_ind_arr( rateindstr_tmp ) := dratehist_arr.count;
                                    dratehist_arr(dratehist_arr.count)  := dratehist_tmp;
                                    pr_include(i);
                                end if;

                        end if;


                    when 2 then
                        deb('���� 3 - ����� ���������� ������', p_level => 5);
                        if rate_tmp = 0 then
                            -- ������, �������� ����������� ����� �� ������� � ������� �������
                            deb('���� 3 - ������ - �������� ����������� ����� �� ������� � ������� �������', p_level => 5);
                            pr_exclude(419, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ���������� �������� �������������� ���� �� ���� %date% �� �������������� %fiid%, ��� ����� - %type%', i, p_action);

                        elsif  add_tmp.tgt_state = 1 then
                            -- ������ � ������ ������� ���������
                            deb('���� 3 - ������ - ������ � ������ ������� ���������', p_level => 5);
                            pr_exclude(205, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ������ ��������� � ������ ������� ��������������, ���� �� ���� %date% �� �������������� %fiid%, ��� ����� - %type%', i, p_action);

                        -- ���� ����������� ��������� �������� �����
                        elsif is_last_date
                        then
                            deb('���� 3 - ����������� ��������� �������� �����', p_level => 5);
                            --dratedef_tmp := dratedef_arr( add_tmp.tgt_rate_id );
                            dratedef_tmp.T_RATEID := add_tmp.tgt_rate_id;
                            dratedef_tmp.T_RATE := main_tmp.t_rate * power(10, main_tmp.t_point);
                            dratedef_tmp.T_SINCEDATE := main_tmp.t_ratedate;
                            dratedef_tmp.T_SCALE := main_tmp.t_scale;
                            dratedef_tmp.T_POINT := main_tmp.t_point;
                            dratedef_tmp.t_fiid := add_tmp.fi;
                            dratedef_tmp.t_otherfi := add_tmp.base_fi;
                            dratedef_tmp.t_name := chr(1);
                            dratedef_tmp.T_DEFINITION := chr(1);
                            dratedef_tmp.T_TYPE := add_tmp.type_id;
                            dratedef_tmp.T_ISDOMINANT := add_tmp.isdominant;
                            dratedef_tmp.T_ISRELATIVE := add_tmp.isrelative;
                            dratedef_tmp.T_INFORMATOR := add_tmp.market_id;
                            dratedef_tmp.T_MARKET_PLACE := add_tmp.market_id;
                            dratedef_tmp.T_ISINVERSE := chr(0);
                            dratedef_tmp.T_SECTION := case when add_tmp.section_id = -1 then 0 else add_tmp.section_id end;
                            dratedef_tmp.T_ISMANUALINPUT :=  chr(0);
                            dratedef_tmp.T_INPUTDATE := trunc(sysdate);
                            dratedef_tmp.T_INPUTTIME := date'0001-01-01' + (sysdate - trunc(sysdate));
                            dratedef_tmp.T_OPER :=  g_oper;

                            update dratedef_dbt set row = dratedef_tmp where t_rateid =  add_tmp.tgt_rate_id;
                            pr_include(i);
                        else
                            deb('���� 3 - ����������� ������������ �������� �����', p_level => 5);
                            dratehist_tmp.t_rateid := add_tmp.tgt_rate_id;
                            dratehist_tmp.t_isinverse := chr(0);
                            dratehist_tmp.t_rate := main_tmp.t_rate * power(10, main_tmp.t_point);
                            dratehist_tmp.t_scale := main_tmp.t_scale;
                            dratehist_tmp.t_point := main_tmp.t_point;
                            dratehist_tmp.t_inputdate := trunc(sysdate);
                            dratehist_tmp.t_inputtime := date'0001-01-01' + (sysdate - trunc(sysdate));
                            dratehist_tmp.t_oper := g_oper;
                            dratehist_tmp.t_sincedate := main_tmp.t_ratedate;
                            dratehist_tmp.t_ismanualinput := chr(0);
                            update dratehist_dbt set row = dratehist_tmp where t_rateid =  add_tmp.tgt_rate_id and t_sincedate = main_tmp.t_ratedate;
                            pr_include(i);
                        end if;
                        
                    when 3 then
                        deb('���� 3 - ����� �������� ������', p_level => 5);
                        if rate_tmp = 0 then
                            deb('���� 3 - ������ - �������� ���������� ����� �� ������� � ������� �������', p_level => 5);
                            -- ������, �������� ����������� ����� �� ������� � ������� �������
                            pr_exclude(420, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ���������� ������� �������������� ���� �� ���� %date% �� �������������� %fiid%, ��� ����� - %type%', i, p_action);
                        else    

                            delete from dratehist_dbt where t_rateid = add_tmp.tgt_rate_id and t_sincedate = main_tmp.t_ratedate;
                            pr_include(i);
                        end if;
                    end case;        
        
        end execute_rate;



--===========================================================================================================================
--===========================================================================================================================
        procedure execute_nominal( i pls_integer)
        is 
            l_initnom number := 0;
            l_destid  number;
            l_flag    number;
            l_nom_for_date number;
            l_id      number;
        begin
            deb('! ������������� ������, ������������� �������. BASEFIID=#1, �������=#2, ����=' || to_char(add_tmp.rate_date,'DD.MM.YYYY'), add_tmp.base_fi, main_tmp.t_rate, p_level => 5);
            if  ind_nominal_arr.exists( add_tmp.base_fi )  and  ind_nominal_arr( add_tmp.base_fi ).T_BEGDATE = add_tmp.rate_date
            then
                l_initnom := ind_nominal_arr( add_tmp.base_fi ).T_FACEVALUE;
                deb('�� ������ �������� �������� ���������� �������� �� ������� ���� - #1', l_initnom, p_level => 5);
            end if;
                
            if p_action = 2 or p_action = 3 
            then
                ----------------------------------------------------------------------
                if l_initnom > 0 
                then 
                    deb('������: ��������� ���������� �������� ������ ���������� ��������� ��� ���������� �� DTXAVOIRISS_DBT', p_level => 5);
                    pr_exclude(525, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ��������� ���������� �������� ������ ���������� ��������� ��� ���������� �� DTXAVOIRISS_DBT', i, p_action);
                else
                    ----------------------------------------------------------------------
                    l_destid := replobj_get( c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type).DEST_ID;  
                    if  l_destid = -1  -- �� ����� ������� � ����������
                    then
                        deb('������: ����������/��������� ������� �� ������������ � ������� �������', p_level => 5);
                        pr_exclude(525, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ����������/��������� ������� �� ������������ � ������� �������', i, p_action);
                    else
                        ----------------------------------------------------------------------
                        select count(*) into l_flag from DFIVLHIST_DBT where t_id = l_destid;
                        if l_flag = 0   -- �� ����� ������� � ������� �������
                        then
                            deb('������: ����������/��������� ������� ����������� � �������', p_level => 5);
                            pr_exclude(525, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ����������/��������� ������� ����������� � �������', i, p_action);
                        else
                            --=======================================================================================
                            if p_action = 2
                            then
                                    if  add_tmp.tgt_state = 1 
                                    then
                                        -- ������ � ������ ������� ���������
                                        deb('���� 3 - ������ - ������ � ������ ������� ���������', p_level => 5);
                                        pr_exclude(205, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ������ ��������� � ������ ������� ��������������, ���� �� ���� %date% �� �������������� %fiid%, ��� ����� - %type%', i, p_action);
                                    else
                                        update DFIVLHIST_DBT set T_FIID = add_tmp.fi, T_VALKIND = 1/*��������� ��������*/, T_ENDDATE = main_tmp.t_ratedate, T_VALUE = main_tmp.t_rate, T_INTVALUE = 0 where t_ID = l_destid;
                                        pr_include(i);
                                        deb('���� 3 - ������ ������� �������', p_level => 5);
                                    end if;
                            elsif p_action = 3
                            then
                                    deb('���� 3 - �������� ������� �� DFIVLHIST_DBT (#1)', l_destid, p_level => 5);
                                    delete from DFIVLHIST_DBT where t_ID = l_destid;
                                    pr_include(i);
                                    deb('���� 3 - ������ ������� ������', p_level => 5);                                                                                                               
                            end if;                                
                        end if;
                    end if;
                
                end if;
            else  -- p_action == 1 

                if l_initnom = 0 -- � ������ �� ���� ���������� �������� �� ����
                then  
                    begin
                        select t_value into l_nom_for_date from DFIVLHIST_DBT where t_ValKind = 1/*��������� ��������*/ and T_FIID = add_tmp.fi and t_EndDate = main_tmp.t_ratedate;
                        deb('���� 3 - ������� ������ � DFIVLHIST_DBT: �������� #1, fiid=#2', l_nom_for_date, add_tmp.fi, p_level => 5);
                    exception 
                        when no_data_found 
                        then l_nom_for_date := 0;
                    end;
                end if;
                
                if l_initnom + l_nom_for_date > 0
                then
                    deb('���� 3 - ����������� ������� (�������� #1) ��� ���� � ������� (�������� #2)', main_tmp.t_rate, (l_initnom + l_nom_for_date), p_level => 5);
                    if (l_initnom + l_nom_for_date) = main_tmp.t_rate
                    then -- ���� ������� ��� ��, ��� � �������, � ��� �� �����
                        pr_exclude(418, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ��� ���������� ������� �� ���� %date% �� �������������� %fiid%, ��� ����� - %type%', i, p_action, p_silent => true);
                    else    
                        pr_exclude(418, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ��� ���������� ������� �� ���� %date% �� �������������� %fiid%, ��� ����� - %type%', i, p_action, p_silent => false);
                    end if;
                else
                    
                    select DFIVLHIST_DBT_seq.nextval into l_id FROM dual;
                    deb('���� 3 - �������� ID ����� ������ � DFIVLHIST_DBT (#1)', l_id, p_level => 5);
                    
                    insert into DFIVLHIST_DBT (T_ID, T_FIID, T_VALKIND, T_ENDDATE, T_VALUE, T_INTVALUE) 
                    values( l_id, add_tmp.base_fi, 1, main_tmp.t_ratedate, main_tmp.t_rate, 0);
                    
                    
                    delete from DTXREPLOBJ_DBT where T_OBJECTTYPE=70 and t_objectid = main_tmp.t_courseid; 
                    insert into DTXREPLOBJ_DBT (T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE) values(70, main_tmp.t_courseid, main_tmp.t_type, l_id, 1, 0 );
                    pr_include(i);
                                            
                    -- ������ �������� 2 ������ - replobj, rate_sou_add_arr
                    -- ������ ������� ������
                    deb('���� 3 - ��������� ������ � ����� �������� � ������', p_level => 5);
                    dratedef_arr( dratedef_tmp.t_rateid ) := dratedef_tmp;
                    -- ����� ��������� ������� ����� ���� ��, ��� ��������� � ����� �� �����
                    for j in i..rate_sou_arr.count  -- �� ������� ������ �� �����
                    loop
                        if rate_sou_arr(j).t_courseid=main_tmp.t_courseid and rate_sou_arr(j).t_type=main_tmp.t_type
                        then
                            rate_sou_add_arr(j).tgt_rate_id := l_id;               
                        end if;
                    end loop;
                    replobj_add( c_OBJTYPE_RATE, rate_sou_arr(i).T_COURSEID, rate_sou_arr(i).T_TYPE, p_destid => l_id);       
                    add_tmp.tgt_rate_id := l_id; 
                    
                
                end if;
            end if;

        end execute_nominal;


--================================================================================================================
--================================================================================================================
--================================================================================================================
--================================================================================================================
    begin
        deb_empty('=');

        deb('�������� ���������  LOAD_RATE �� ' || to_char(p_date, 'dd.mm.yyyy') || ', ��� �������� ' || p_action);
        
        open m_cur(p_date, p_action);
        loop

            -- �������� ������ ������
            fetch m_cur bulk collect into rate_sou_arr limit g_limit;
            exit when rate_sou_arr.count=0;
            deb('��������� ������ �� DTXCOURSE_DBT, #1 �����', m_cur%rowcount);

            -- ������������ ��� �������� ��� �������� �� REPLOBJ
            deb_empty('=');
            deb('���� 1 - ����������� ����� � ������ REPLOBJ');
            for i in 1..rate_sou_arr.count
            loop
                -- �������� ���������� fiid
                replobj_add( c_OBJTYPE_MONEY, rate_sou_arr(i).t_fiid, p_comment => '���������� �������������');
                replobj_add( rate_sou_arr(i).T_BASEFIKIND, rate_sou_arr(i).t_basefiid, p_comment => '������� �������������');

                -- �������� ���������� MARKETID
                replobj_add( c_OBJTYPE_MARKET, rate_sou_arr(i).T_MARKETID, p_comment => '�������� ��������');
                replobj_add( c_OBJTYPE_MARKET_SECTION, rate_sou_arr(i).T_MARKETID, rate_sou_arr(i).T_MARKETSECTORID);

                -- ����������, ����
                replobj_add( c_OBJTYPE_RATE, rate_sou_arr(i).T_COURSEID, rate_sou_arr(i).T_TYPE);
            end loop;
            deb('������� ������ � ����� REPLOBJ, #1 �������', replobj_rec_arr.count);

            -- ��������� ��� �� REPLOBJ
            replobj_load;

            

            -- ���������� ������, ��������� ����������������� ������ �������������� ���������. �������� ���������� �������.
            deb_empty;
            deb_empty('=');
            ind_nominal_flag := false;
            deb('���� 2 - ��������������� � �������� ����������');
            for i in 1..rate_sou_arr.count
            loop
                main_tmp := rate_sou_arr(i);
                rate_sou_add_arr(i) := add_tmp; -- ��� ��������� pr_exclude,��� �������� � ���� �������� ��� ������

                add_tmp.isdominant := case when ( main_tmp.T_BASEFIKIND = 10 and main_tmp.t_type = 6 ) THEN chr(88) else chr(0) end;

                add_tmp.type_id := rtype( main_tmp.t_type );       -- ������������ ��� �����
                
                if add_tmp.type_id = c_RATE_TYPE_NOMINALONDATE
                then 
                    -- ����� ������ ����������� ������ �� ���������� ���������������� ��������, ������� ���� ������������ ��������
                    ind_nominal_flag := true;
                end if;
                
                add_tmp.rate_date := main_tmp.t_ratedate;       -- ��� ������������� �� SQL, ��� #R

                add_tmp.tgt_rate_id :=  replobj_get(c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type).dest_id;
                add_tmp.tgt_state :=  replobj_get(c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type).state;

                stat_tmp :=  replobj_get( c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type).state;
                        if  ( add_tmp.tgt_rate_id = 0) and (p_action > 1 ) then
                            pr_exclude(419, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ���������� %act% �������������� ����, ���������� ���������� - %basefiid%', i, p_action );
                        elsif ( stat_tmp = 1) and (p_action > 1 ) then
                            pr_exclude(205, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ������ ��������� � ������ ������� ��������������, ���������� ���������� - %basefiid%', i, p_action);
                        end if;

                add_tmp.fi  :=  replobj_get( c_OBJTYPE_MONEY, main_tmp.t_fiid).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action < 3 ) then 
                            pr_exclude(527, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ���������� ������ ������� � ������ ��� ��������������� ����������� ����������� �����������, ������� ���������� - %basefiid%, ��� ����� - %type%', i, p_action);
                        end if;

                add_tmp.market_id   :=  replobj_get( c_OBJTYPE_MARKET, main_tmp.T_MARKETID).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action < 3 ) then
                            pr_exclude(525, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ���������� ������ ������� � ������ ��� �������������� �������� ��������, ���������� ���������� - %basefiid%', i, p_action);
                        end if;
                add_tmp.section_id  :=  replobj_get( c_OBJTYPE_MARKET_SECTION, main_tmp.T_MARKETID, main_tmp.T_MARKETSECTORID).dest_id;
                        if  ( add_tmp.section_id = 0) and (p_action > 1 ) then
                            --pr_exclude(525, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ���������� ������ ������� � ������ ��� �������������� ������ �������� ��������, ���������� ���������� - %basefiid%', i, p_action);
                            null;
                        end if;
                add_tmp.base_fi     :=  replobj_get( main_tmp.T_BASEFIKIND, main_tmp.t_basefiid).dest_id;
                        if  ( add_tmp.base_fi <= 0) and (p_action > 1 ) then
                            pr_exclude(528, 70, main_tmp.t_courseid, main_tmp.t_type, '������: ���������� ������ ������� � ������ ��� ��������������� �������� ����������� �����������, ���������� ���������� - %basefiid%, ��� ����� - %type%', i, p_action);
                        end if;
                -- ���� ����� ������, �� � ���� ���������
                if  rate_sou_add_arr(i).result = 2
                    then continue;
                end if;
                
                add_tmp.isrelative := case when ( main_tmp.T_BASEFIKIND = 20 and add_tmp.type_id <> c_RATE_TYPE_NKDONDATE and RSI_RSB_FIInstr.FI_IsAvrKindBond( RSI_RSB_FIInstr.FI_AvrKindsGetRootByFIID( add_tmp.base_fi))) THEN chr(88) else chr(0) end;
                rate_sou_add_arr(i) := add_tmp;

            end loop;

            -- ������� ������ ����� - dratedef|dratehist. �������� ������� � dratedef �������������� ����������� �� rate_id, � dratehist - ��������� ����������.
            -- ������������� ��������� � varchar ������ ����� ������.
            -- ��������� �� ������� ������� �������� ����� (�� �������� ������) �� ���� action
            deb('�������� ������ dratedef_dbt');
            select * bulk collect into dratedef_arr_tmp from dratedef_dbt where t_rateid in ( select tgt_rate_id from table(rate_sou_add_arr) );
            for i in 1..dratedef_arr_tmp.count
            loop
                -- ��������������� ��������� �� rate_id
                dratedef_arr( dratedef_arr_tmp(i).t_rateid ) := dratedef_arr_tmp(i);
            end loop;
            -- ��������� ������ �� �����
            dratedef_arr_tmp.delete;

            -- ��������� ����� �� �������. #R
            -- ������ �������� �� ������ ������ �� ������ ����
            -- ����� ����������� ����. ����� ����������������� � ��������� � varchar ��������, �� ��� ����� ������� �����.
            -- ��������� ��� ���� ����� ������������ ��� ���� ������� ��������, � �� ��� ��������.
            deb('�������� ������ dratehist_dbt');
            select * bulk collect into dratehist_arr from dratehist_dbt where (t_rateid, t_sincedate) in ( select tgt_rate_id, rate_date from table(rate_sou_add_arr) );
            -- TODO �������� emerg_limit
            for i in 1..dratehist_arr.count
            loop
                -- ������� ��������� ���������
                -- ���� ���� "12345#12082020", {rate_id}#{����_������_��������_�����}
                dratehist_ind_arr( to_char(dratehist_arr(i).t_rateid) || '#' || to_char(dratehist_arr(i).t_sincedate, 'ddmmyyyy') ) := i;
            end loop;
            
            ---------
            -- ���� ���� ������������� ��������, �������� ����� ��������� ��������� �� ���� ������������ �������
            deb('�������� ������ ��������������� ���������');
            if ind_nominal_flag
            then
                select * bulk collect into ind_nominal_tmp from dv_fi_facevalue_hist  where t_id=0  
                and t_fiid in (select base_fi from table( rate_sou_add_arr ) where type_id = c_RATE_TYPE_NOMINALONDATE );

                for j in 1..ind_nominal_tmp.count
                loop
                    ind_nominal_arr( ind_nominal_tmp(j).t_fiid ).t_fiid := ind_nominal_tmp(j).t_fiid;
                    ind_nominal_arr( ind_nominal_tmp(j).t_fiid ).t_begdate := ind_nominal_tmp(j).t_begdate;
                    ind_nominal_arr( ind_nominal_tmp(j).t_fiid ).t_facevalue := ind_nominal_tmp(j).t_facevalue;
                end loop;
                ind_nominal_tmp.delete;
            end if;
            ---------------------------------------------------------------------------------------------------------------------------------------------
            --- ��� ������ ���������



            -- ��� ��� �������� �� �������� ���������. ���������, ���� �� ���� � �������, � ���������
            deb_empty;
            deb_empty('=');
            deb('���� 3 - ������ ������/��������� � �������-���������');
            for i in 1..rate_sou_arr.count
            loop
                add_tmp := rate_sou_add_arr(i);
                main_tmp := rate_sou_arr(i);

                -- ����� ��������� �������� ��� ���������� ���������, �������� � ������� ���������
                if add_tmp.type_id <> c_RATE_TYPE_NKDONDATE
                then
                    -- ������ ��������� �����
                    execute_rate(i);
                else
                    -- ������ ��������� ��������� ��������
                    execute_nominal(i);
                end if;   
                 
                deb('��������� ������ ���������� ������ �� #1', rate_sou_add_arr(i).result, p_level => 4);
                update DTXCOURSE_DBT set t_replstate = rate_sou_add_arr(i).result  where T_COURSEID = main_tmp.t_courseid and t_action = p_action and t_replstate = 0 and t_type = main_tmp.t_type and t_instancedate = main_tmp.t_instancedate;

            end loop;

            commit;
            deb('��������� ����� ������ ���������. COMMIT.');
        end loop;

        deb('��������� ��������� load_rate');
    end load_rate;







    -- ������ � ���
    procedure add_log( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2, p_date date)
    is
        pragma autonomous_transaction;
        T_SEVERITY number;
    begin
        case
            when p_code > 600 THEN T_SEVERITY := 4;
            when p_code > 500 THEN T_SEVERITY := 5;
            when p_code > 400 THEN T_SEVERITY := 6;
            when p_code > 300 THEN T_SEVERITY := 7;
            when p_code > 200 THEN T_SEVERITY := 8;
            when p_code > 100 THEN T_SEVERITY := 9;
            when p_code > 0 THEN T_SEVERITY   := 10;
        end case;

        insert into dtxloadlog_dbt( T_MSGTIME, T_MSGCODE, T_SEVERITY, T_OBJTYPE, T_OBJECTID, T_SUBOBJNUM, T_FIELD, T_MESSAGE, T_CORRECTION, T_CORRUSER, T_CORRTIME, T_INSTANCEDATE)
        values( sysdate, p_code, t_severity, p_objtype, p_id, p_subnum, ' ', p_text, chr(0), chr(1), null, p_date);
        commit;
    end add_log;

        -- ������ � ���
    procedure add_log_deferred( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2, p_date date)
    is
        pragma autonomous_transaction;
        T_SEVERITY number;
    begin
        case
            when p_code > 600 THEN T_SEVERITY := 4;
            when p_code > 500 THEN T_SEVERITY := 5;
            when p_code > 400 THEN T_SEVERITY := 6;
            when p_code > 300 THEN T_SEVERITY := 7;
            when p_code > 200 THEN T_SEVERITY := 8;
            when p_code > 100 THEN T_SEVERITY := 9;
            when p_code > 0 THEN T_SEVERITY   := 10;
        end case;

        null;
    end add_log_deferred;


/*  ����������
        TODO  � ��������� LOAD_RATE

        -- ���������� ������ � ����� dratehist �� �������
        procedure add_dratehist_buf( T_RATEID number, T_ISINVERSE number, T_RATE number, T_SCALE number, T_POINT number, T_OPER number, T_SINCEDATE date, T_ISMANUALINPUT char)
        is
            T_INPUTDATE date := trim(sysdate);
            T_INPUTTIME date := to_date('0001-01-01') + (sysdate - trim(sysdate));
            tmp dratehist_dbt%rowtype;
        begin
            tmp.t_rateid := t_rateid;
            tmp.T_ISINVERSE := T_ISINVERSE;
            tmp.T_RATE := T_RATE;
            tmp.T_SCALE := T_SCALE;
            tmp.T_POINT := T_POINT;
            tmp.T_OPER := T_OPER;
            tmp.T_INPUTDATE := T_INPUTDATE;
            tmp.T_INPUTTIME := T_INPUTTIME;
            tmp.T_SINCEDATE := T_SINCEDATE;
            tmp.T_ISMANUALINPUT := T_ISMANUALINPUT;

            new_dratehist_arr( new_dratehist_arr.count ) := tmp;

        end add_dratehist_buf;

        -- ���������� ������ � ����� dratehist �� ��������
        procedure del_dratehist_buf( T_RATEID number, T_SINCEDATE date)
        is
            T_INPUTDATE date := trim(sysdate);
            T_INPUTTIME date := to_date('0001-01-01') + (sysdate - trim(sysdate));
            tmp dratehist_dbt%rowtype;
        begin
            tmp.t_rateid := t_rateid;
            tmp.T_SINCEDATE := T_SINCEDATE;

            del_dratehist_arr( del_dratehist_arr.count ) := tmp;

        end del_dratehist_buf;
*/



end load_rss;
/
