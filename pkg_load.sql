create or replace package load_rss
is
    -- ���������
    g_limit number := 1000; -- ���������� ������������ ����������� ������� ���������
    
    -- ��������� �����
    OBJTYPE_RATE constant number := 70;
    OBJTYPE_MONEY constant number  := 10;
    OBJTYPE_SEC  constant number  := 20;
    OBJTYPE_MARKET  constant number  := 30;
    OBJTYPE_MARKET_SECTION  constant number  := 40;    
    
    
    -- ��� ������� ��������� �� ���������    
    type rate_sou_arr_type is table of DTXCOURSE_DBT%ROWTYPE; 
    type rate_sou_add_type is record(   -- �������������� ����, ������������� � ������� �������� rate_sou_arr_type �� ��������   
                                    tgt_rate_id number,  -- id ����� � ������� �������, 0 ���� ���
                                    tgt_rate_val number, -- �������� ����� �� ���� � ������� �������
                                    tgt_rate_date date,  -- ���� �������� �������� ����� � ������� �������
                                    market_id number,    -- ���������������� ID �����
                                    section_id number,   -- ���������������� ID ������� �����
                                    base_fi number,      -- ���������������� ������� FI
                                    base_fi_name varchar2(500), -- ��� �������� FI?
                                    fi number,           -- ���������������� ���������� id
                                    isrelative boolean  -- ������� �������������� �����,
                                    excluded boolean := false; -- �� ����� �� ������� ������ ����������, �� �������� �� ���������
                                  );
    type rate_sou_add_arr_type is table of rate_sou_add_type index by pls_integer;
    
    rate_sou_arr        rate_sou_arr_type;
    rate_sou_add_arr    rate_sou_add_arr_type;                                     
    
    -- ��� ������� ����� 
    type rhist_tgt_arr_type is table of dratehist_dbt%ROWTYPE;  
    
    -- ���� ��������� ���������
    type fi_type is record( tgt_id number, tgt_fi_name varchar2(100));    
    type fi_arr_type is table of fi_type index by pls_integer;
    type fi_id_type is table of number;
    fi_arr fi_arr_type;
    procedure add_fi_id( p_id pls_integer); -- ��������� ID � ������, ���� ��� ��� ���
    procedure add_fi_cache; -- ��������� ��� �� ���� fiid �� �������
    procedure clear_fi_cache; -- ������� ��� fi
    
    -- �������� ��������� --
    procedure load_rate(p_date date, p_action number);
    
    ---------------------------------------------------------------------------------------------------------------------------------------------   
    -- ������ � ����� replobj
    -- �������� ������� ������ ��������������� ������, ����� ���������� ����� ������������ �� � ��������� TABLE
    -- �� ��� ��������� ������ ��� ����� ����������� ��������� ��������� ��������� replobj_rec_inx_arr
    type replobj_rec_type            is record(objtype pls_integer, obj_id number, obj_sub_id number, dest_id number, state pls_integer, comment varchar2(100));
    type replobj_rec_arr_type        is table of replobj_rec_type index by pls_integer;  -- �������� ���������
    type replobj_rec_inx_arr_type    is table of pls_integer index by varchar2(200);     -- ��������� ���������, ��������� �� ������ ��������, ������������� ������������� ���������� ����� (objtype,obj_id,obj_sub_id)
    type replobj_tmp_arr_type        is table of dtxreplobj_dbt%rowtype;                 -- ��������� ����� ��� BULK COLLECT, ���� �� ���������� �� ���������� ����

    replobj_rec_arr             replobj_rec_arr_type;  
    replobj_rec_inx_arr         replobj_rec_inx_arr_type; -- ��������� ���������
    replobj_tmp_arr             replobj_tmp_arr_type;
    
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number, p_comment varchar2 default ''); -- ���������� �������� ID � ���������
    procedure replobj_load; -- �������� ���������
    function replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number) return replobj_rec_type; -- ������� �������� �� ���� REPLOBJ
    -- procedure savestat;   -- �������� � ��� ���������� �� ������ ���� � ������� �����
    -- procedure replobj_clear; -- ������� ���� ���������  
    ----------------------------------------------------------------------------------------------------------------------------------------------
    
    
    
    -- ������ � �����
    type log_rows_type is table of dtxloadlog%rowtype;  -- ��� ���������� ������ � ���.
    log_rows  log_rows_type; 
    -- ��������� ���������������� ������ � ���
    procedure add_log( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- ������ � ���
    -- ��������� ���������� ������. ����� ������� ������� ����� ����������� � ����� ����������� ��������, ����� �������� ������ � ���������, ����� ����������� � ��� ����� ������
    -- procedure add_log_deferred( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- ������ � ���
    -- procedure add_log_exec; -- ��������� � ������� ����. ������� �������� ���������.
     
    
end load_rss;


create or replace package body load_rss
is
    -- ������������� ����� ������
    function RTYPE( p_tp number ) return number
    is
        v_rez number;
    begin
    if    p_tp = 26 then v_rez := 23;
    elsif p_tp = 10 then v_rez := 23;
    elsif p_tp = 27 then v_rez := 1001;
    elsif p_tp = 28 then v_rez := 1002;
    elsif p_tp = 29 then v_rez := 1003;
   -- ������ �������
    elsif p_tp = 1 then v_rez := 2;
    elsif p_tp = 2 then v_rez := 3;
    elsif p_tp = 3 then v_rez := 4;
    elsif p_tp = 4 then v_rez := 15;
    elsif p_tp = 5 then v_rez := 9;
    elsif p_tp = 6 then v_rez := 7;
    elsif p_tp = 7 then v_rez := 16; --KD 16 - ����� ����� "����� ������"
    -------------------------------------------------------------------------
    else return -p_tp;
    end if;
end;


    -- ��������� ������� ����������� ������� � ���������. ���� ��� ���, ����������. ���� �����������, ���������.
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number default 0, p_comment varchar2 default '')
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
            replobj_rec_arr(mas_idx).comment := p_comment;
            replobj_rec_arr(mas_idx).dest_id := 0;
            -- ������ � ��������� ���������            
            replobj_rec_inx_arr( v_searchstr ) := mas_idx;
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
    begin
        SELECT * BULK COLLECT INTO replobj_tmp_arr
        FROM dtxreplobj_dbt ro, table(replobj_rec_arr) inp where t_objecttype = inp.obj_type and t_objectid = inp.obj_id and (t_subobjnum = obj_sub_id or obj_sub_id = 0) and t_objstate != 2 and inp.dest_id=0;
                 
        IF SQL%ROWCOUNT = emerg_limit THEN
            null;
            -- ������� ����� �������, ������������ ����������
        END IF;
        -- ��������� �������� � �������� ��� ��������� � replid
        FOR i in 1..replobj_tmp_arr.count
        LOOP
            -- ���������� ������ �������� � �������� �������
            v_search_str := to_char(replobj_tmp_arr(i).t_objecttype) || '#' || to_char(t_objectid) || '#' || nvl(to_char(t_subobjnum),'0');
            v_search_idx := replobj_rec_inx_arr(v_search_str);  -- ����� ������ ������ � �������� �������
            -- �������� �������
            replobj_rec_arr(v_search_idx).dest_id := replobj_tmp_arr(i).T_DESTID;
            replobj_rec_arr(v_search_idx).state := replobj_tmp_arr(i).t_objstate;
        END LOOP;             
        
        -- ������� �������, ������� ����� ������������, ��� NULL
        replobj_rec_arr(-1).objtype     := 0;
        replobj_rec_arr(-1).obj_id      := 0;
        replobj_rec_arr(-1).obj_sub_id  := 0;        
        replobj_rec_arr(-1).dest_id     := -1;
        replobj_rec_arr(-1).state       := 0;           
    
    end replobj_load;





    -------------------------------------------------
    procedure load_rate(p_date date, p_action number)
    is 
        cursor m_cur(pp_date date, pp_action number) is select * from DTXCOURSE_DBT where t_instancedate between pp_date and pp_date+1 and t_action = pp_action order by t_instancedate, t_action, t_basefiid, t_marketsectorid, t_type;
        v_search_str varchar2(200);
        v_search_idx pls_integer; 
        type tmp_num_arr_type is table of number;  -- nested table, ���� �������� ��� �����������
        --fiid_arr tmp_num_array = tmp_num_array();
        --market_arr tmp_num_array = tmp_num_array();
        --marksect_arr tmp_num_array = tmp_num_array();
        
        -- ���������� �� ��������� ������� ���������
        procedure add_to_array( p_arr IN OUT NOCOPY tmp_num_array, p_id number ) 
        is begin
            p_arr.extend();
            p_arr(p_arr.count) := p_id;
        end add_to_array;
         
        -- �������� � �������. �������� ������ � ��������� ������ �� ���������.
        procedure pr_exclude(p_code number, p_objtype number, p_text varchar2(1000), p_counter number, p_action number)
        is 
            text_corr varchar2(1000);
            v_row DTXCOURSE_DBT%ROWTYPE;
        begin
            v_row := rate_sou_arr(p_counter);
            text_corr := replace(p_text, '%act%',  (case p_action when 1 then '�������' when 2 then '���������' when 3 then '��������' end) );
            text_corr := replace(text_corr, '%fiid%', v_row.t_fiid);
            text_corr := replace(text_corr, '%base_fiid%', v_row.t_basefiid);
            text_corr := replace(text_corr, '%type%', v_row.t_type);
            -- ����� �������� �� add_log_deferred 
            add_log( p_code, p_objtype, p_id, p_subnum, text_corr, p_date);
            
            -- ��������� �������
            rate_sou_add_arr(p_counter).exclude := true;

        end pr_exclude;
        
        
        add_tmp  rate_sou_add_type;
        stat_tmp number;
                
    begin
        fiid_arr := tmp_num_array();
        
        open m_cur(p_date, p_action);
        loop
            -- ��������� ��� MARKETID � MARKETSECTORID
            
            -- �������� ������ ������
            fetch m_cur bulk collect into rate_sou_arr limit g_limit;
            exit when rate_sou_arr.count=0;
            -- ����������
             
            for i in 1..rate_sou_arr.count
            loop
                -- �������� ���������� fiid
                replobj_add( OBJTYPE_MONEY, rate_sou_arr(i).t_fiid, '���������� �������������');  -- ����� ������������ �� ��� ��������
                replobj_add( rate_sou_arr(i).T_BASEFIKIND, rate_sou_arr(i).t_basefiid, '������� �������������');  -- ����� ������������ �� ��� ��������
                
                -- �������� ���������� MARKETID
                replobj_add( OBJTYPE_MARKET, rate_sou_arr(i).T_MARKETID, '�������� ��������');  
                replobj_add( OBJTYPE_MARKET_SECTION, rate_sou_arr(i).T_MARKETID, rate_sou_arr(i).T_MARKETSECTORID);  
                
                -- ����������, ����
                replobj_add( OBJTYPE_RATE, rate_sou_arr(i).T_COURSEID);
            end loop;
            
            -- ��������� ���
            replobj_load;
            
            -- ���������� ������, ��������� ����������������� ������ �������������� ���������. �������� ���������� �������.
            for i in 1..rate_sou_arr.count
            loop
                add_tmp.fi          :=  replobj_get( OBJTYPE_MONEY, rate_sou_arr(i).t_fiid).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(527, 70, '������: ���������� ������ ������� � ������ ��� ��������������� ����������� ����������� �����������, ������� ���������� - %base_fiid%, ��� ����� - %type%', i, p_action);
                        end if;
                add_tmp.tgt_rate_id :=  replobj_get(OBJTYPE_RATE, rate_sou_arr(i).t_rateid).dest_id;
                stat_tmp :=  replobj_get( OBJTYPE_RATE, rate_sou_arr(i).t_rateid).state;
                        if  ( add_tmp.tgt_rate_id = 0) and (p_action > 1 ) then 
                            pr_exclude(419, 70, '������: ���������� %act% �������������� ����, ���������� ���������� - %fiid%', i, p_action );
                        elsif ( stat_tmp = 1) and (p_action > 1 ) then
                            pr_exclude(205, 70, '������: ������ ��������� � ������ ������� ��������������, ���������� ���������� - %fiid%', i, p_action); 
                        end if;                           
                add_tmp.market_id   :=  replobj_get( OBJTYPE_MARKET, rate_sou_arr(i).T_MARKETID).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(525, 70, '������: ���������� ������ ������� � ������ ��� �������������� �������� ��������, ���������� ���������� - %fiid%', i, p_action);
                        end if;
                add_tmp.section_id  :=  replobj_get( OBJTYPE_MARKET_SECTION, rate_sou_arr(i).T_MARKETID, rate_sou_arr(i).T_MARKETSECTORID).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(525, 70, '������: ���������� ������ ������� � ������ ��� �������������� ������ �������� ��������, ���������� ���������� - %fiid%', i, p_action);
                        end if;
                add_tmp.base_fi     :=  replobj_get( rate_sou_arr(i).T_BASEFIKIND, rate_sou_arr(i).t_basefiid).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(528, 70, '������: ���������� ������ ������� � ������ ��� ��������������� �������� ����������� �����������, ���������� ���������� - %fiid%, ��� ����� - %type%', i, p_action);
                        end if;
                rate_sou_add_arr(i) := add_tmp;
            
            end loop; 
            
            
            -- ��������� �� ������� ������� �������� ����� (�� �������� ������, �� �� �������) �� ���� action
            -- ��������� �� ������� ����� � ���������.
                 
            

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
    
        -- ������ � ���
    procedure add_log_deferred( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date)
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