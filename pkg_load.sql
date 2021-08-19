create or replace package load_rss
is
    -- ���������
    g_limit number := 1000; -- ���������� ������������ ����������� ������� ���������
    
    -- ��� ������� ��������� �� ���������    
    type rate_sou_type is DTXCOURSE_DBT%ROWTYPE;
    type rate_sou_arr_type is table of rate_sou_type;
    type rate_sou_add_type is record(
                                    tgt_rate_id number,  -- id ����� � ������� �������, 0 ���� ���
                                    tgt_rate_val number, -- �������� ����� �� ���� � ������� �������
                                    tgt_rate_date date,  -- ���� �������� �������� ����� � ������� �������
                                    market_id number,    -- ���������������� ID �����
                                    section_id number,   -- ���������������� ID ������� �����
                                    base_fi number,      -- ���������������� ������� FI
                                    base_fi_name varchar2(500), -- ��� �������� FI?
                                    isrelative boolean)  -- ������� �������������� �����
    type rate_sou_add_arr_type is table of rate_sou_add_type index by pls_integer;
    
    rate_sou_arr rate_sou_arr_type;                                     
    -- ��� ������� ����� 
    type rhist_tgt_type is dratehist_dbt%ROWTYPE;
    type rhist_tgt_type_arr is table of rhist_tgt_type;  
    
    -- ���� ��������� ���������
    type fi_type is record( tgt_id number, tgt_fi_name varchar2(100));    
    type fi_arr_type is table of fi_type index by pls_integer;
    type fi_id_type is table of number;
    fi_arr fi_arr_type;
    procedure add_fi_id( p_id pls_integer); -- ��������� ID � ������, ���� ��� ��� ���
    procedure add_fi_cache; -- ��������� ��� �� ���� fiid �� �������
    procedure clear_fi_cache; -- ������� ��� fi
    
    procedure load_rate(p_date date, p_action number);
    
    procedure add_log( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- ������ � ���
    
    -- ������ � ����� replobj
    -- �������� ������� ������ ��������������� ������, ����� ���������� ����� ������������ �� � ��������� TABLE
    -- �� ��� ��������� ������ ��� ����� �������� ��������� ��������� ��������� replobj_rec_inx_arr
    type replobj_rec_type is record(objtype pls_integer, obj_id number, obj_sub_id number, dest_id number, state pls_integer);
    replobj_rec_arr_type is table of replobj_rec_type index by pls_integer;   -- �������� ���������
    replobj_rec_inx_arr_type is table of pls_integer index by varchar2(200);  -- ��������� ���������, ���������� �� ������ ��������.
    replobj_tmp_arr_type is table of dtxreplobj_dbt%rowtype; -- ��������� �����
    replobj_rec_arr         replobj_rec_arr_type;  
    replobj_rec_inx_arr     replobj_rec_inx_arr_type; -- ��������� ���������
    replobj_tmp_arr         replobj_tmp_arr_type;
     
    
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number, destid number); -- ���������� �������� ID � ���������
    procedure replobj_load; -- �������� ���������
    function replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number) return replobj_rec_type; 
    -----------------------------------
     
end load_rss;


create or replace package body load_rss
is
    -- ��������� ������� ����������� ������� � ���������. ���� ��� ���, ����������. ���� �����������, ���������.
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number default 0)
    is
        v_searchstr varchar2(200);
        mas_idx pls_integer;
    begin
        v_searchstr := to_char(p_objtype) || '#' || to_char(p_obj_id) || '#' || to_char(p_obj_sub_id);
        if not replobj_rec_inx_arr.exists(v_searchstr) then
            mas_idx := replobj_rec_arr.count;
            replobj_rec_arr(mas_idx).objtype := p_objtype;
            replobj_rec_arr(mas_idx).obj_id := p_obj_id;
            replobj_rec_arr(mas_idx).obj_sub_id := p_obj_sub_id;
            -- ������ � ��������� ���������            
            replobj_rec_inx_arr( v_searchstr ) := mas_idx;
        end if;
        return;
    end replobj_add;
    
    -- ���������� �� ���� ������ ���������� �������. ���� ����������� � ����, ���������� -1
    -- ��������� ������� �������� � ��������� ���������. ���� ����, ����� �� ��������� �������� �������,
    -- �� ���� �������� ������� � ��������.
    function replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number default 0)
    is
        v_searchstr varchar2(200);
    begin
        v_searchstr := to_char(p_objtype) || '#' || to_char(p_obj_id) || '#' || to_char(p_obj_sub_id);
        if replobj_rec_inx_arr.exists(v_searchstr) then  
            return replobj_rec_arr( replobj_rec_inx_arr(v_searchstr));
        end if;
        return replobj_rec_arr(-1);  -- ��� ���� ���� ������ �������       
    end replobj_get;      
    
    procedure replobj_load 
    is
        emerg_limit number := 10000;        
        v_search_str varchar2(200);
        v_search_idx pls_integer;
        err_count pls_integer := 0;
    begin
        SELECT * BULK COLLECT INTO replobj_tmp_arr
        FROM dtxreplobj_dbt ro, table(replobj_rec_arr) inp where t_objecttype = inp.obj_type and t_objectid = inp.obj_id and t_objstate != 2; 
        IF SQL%ROWCOUNT = emerg_limit THEN
            null;
            -- ������� ����� �������, ����������
        END IF;
        -- ��������� �������� � �������� ��� ��������� � replid
        FOR i in 1..replobj_tmp_arr.count
        LOOP
            v_search_str := to_char(replobj_tmp_arr(i).t_objecttype) || '#' || to_char(t_objectid) || '#' || nvl(to_char(t_subobjnum),'0');
            v_search_idx := replobj_rec_inx_arr(v_search_str);  -- ����� ������ ������ � �������� �������
            replobj_rec_arr(v_search_idx).dest_id := replobj_tmp_arr(i).T_DESTID;
            replobj_rec_arr(v_search_idx).state := replobj_tmp_arr(i).t_objstate;
        END LOOP;             
        
        -- ������� �������, ������� ����� ������������, ��� NULL
        replobj_rec_arr(-1).objtype     := 0;
        replobj_rec_arr(-1).obj_id      := 0;
        replobj_rec_arr(-1).obj_sub_id  := 0;        
        replobj_rec_arr(-1).dest_id     := 0;
        replobj_rec_arr(-1).state       := 0;           
    
    end replobj_load;


    procedure load_rate(p_date date, p_action number)
    is 
        cursor m_cur(pp_date, pp_action) is select * from DTXCOURSE_DBT where t_instancedate between pp_date and pp_date+1 and t_action = pp_action order by t_instancedate, t_action, t_basefiid, t_marketsectorid, t_type; 
    begin
        
        open m_cur(p_date, p_action);
        
        loop
            -- �������� ������ ������
            fetch m_cur bulk collect into rate_sou_arr limit g_limit;
            exit when m_cur%notfound;
            -- ���������� 
            

        end loop; 
                
        
    end load_rate;

    -- ������ � ���
    procedure add_log( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date)
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

end load_rss;