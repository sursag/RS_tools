create or replace package load_rss
is
    -- ���������
    g_limit number := 1000; -- ���������� ������������ ����������� ������� ���������
    
    -- ��������� �����
    c_OBJTYPE_RATE constant number := 70;
    c_OBJTYPE_MONEY constant number  := 10;
    c_OBJTYPE_SEC  constant number  := 20;
    c_OBJTYPE_MARKET  constant number  := 30;
    c_OBJTYPE_MARKET_SECTION  constant number  := 40;    

    c_RATE_TYPE_NKDONDATE constant number := 15;   -- ��� ����� "��� �� ����"
    c_RATE_TYPE_NOMINALONDATE constant number :=  100; -- ���������� ��������� �/� �� ���� (������������� �� ������� ������)
    
    ---------------------------------------------------------------------------------------------------------------------------------------------    
    -- ��� ������� ��������� �� ���������    
    type rate_sou_arr_type is table of DTXCOURSE_DBT%ROWTYPE; 
    type rate_sou_add_type is record(   -- �������������� ����, ������������� � ������� �������� rate_sou_arr_type �� ��������   
                                    tgt_rate_id number,  -- id ����� � ������� �������, 0 ���� ���
                                    tgt_rate_val number, -- �������� ����� �� ���� � ������� �������
                                    rate_date date,      -- ���� �������� �������� ����� 
                                    type_id  number,     -- ���������������� ���
                                    market_id number,    -- ���������������� ID �����
                                    section_id number,   -- ���������������� ID ������� �����
                                    base_fi number,      -- ���������������� ������� FI
                                    fi number,           -- ���������������� ���������� id
                                    isdominant char,     -- ������� ��������� ����� ������, ���� ��
                                    isrelative char,  -- ������� �������������� �����
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
    type replobj_rec_type            is record(objtype pls_integer, obj_id number, obj_sub_id number, dest_id number, state pls_integer, comment varchar2(100));
    type replobj_rec_arr_type        is table of replobj_rec_type index by pls_integer;  -- �������� ���������
    type replobj_rec_inx_arr_type    is table of pls_integer index by varchar2(200);     -- ��������� ���������, ��������� �� ������ ��������, ������������� ������������� ���������� ����� (objtype,obj_id,obj_sub_id)
    type replobj_tmp_arr_type        is table of dtxreplobj_dbt%rowtype;                 -- ��������� ����� ��� BULK COLLECT, ���� �� ���������� �� ���������� ����

    replobj_rec_arr             replobj_rec_arr_type;  
    replobj_rec_inx_arr         replobj_rec_inx_arr_type; -- ��������� ���������
    replobj_tmp_arr             replobj_tmp_arr_type;
    
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number, p_comment varchar2 ); -- ���������� �������� ID � ���������
    procedure replobj_load; -- �������� ���������
    function replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number) return replobj_rec_type; -- ������� �������� �� ���� REPLOBJ
    -- procedure savestat;   -- �������� � ��� ���������� �� ������ ���� � ������� �����
    -- procedure replobj_clear; -- ������� ���� ���������  
    ----------------------------------------------------------------------------------------------------------------------------------------------
    
    
    ---------------------------------------------------------------------------------------------------------------------------------------------    
    -- ������ � �����
    type log_rows_type is table of dtxloadlog%rowtype;  -- ��� ���������� ������ � ���.
    log_rows  log_rows_type; 
    -- ��������� ���������������� ������ � ���
    procedure add_log( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- ������ � ���
    -- ��������� ���������� ������. ����� ������� ������� ����� ����������� � ����� ����������� ��������, ����� �������� ������ � ���������, ����� ����������� � ��� ����� ������
    -- procedure add_log_deferred( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- ������ � ���
    -- procedure add_log_exec; -- ��������� � ������� ����. ������� �������� ���������.
    ---------------------------------------------------------------------------------------------------------------------------------------------     
    
end load_rss;








create or replace package body load_rss
is

    -- ��������� ������� ����������� ������� � ���������. ���� ��� ���, ����������. ���� �����������, ���������. TODO � ����������� �������� �� HASH � ���������
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number default 0, p_comment varchar2 )
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
        -- �������� ��������� - ��������� ������ �� n �������, �������� id ���� ��������� ��������� � ��������� �� �������.
        -- ����� �� ������� ������� ��������� �������� � ������� ������, ��������������� �������������� �������.
        -- ������ � ������������ � �������� action � ��������/����������� ����� � ������� ������� ��������� ����� �������.
        -- ��� dratedef_dbt ������ ������� ����� ������ �����������. ���� ���-�� �� ��������� ��� �������� �������, ����� ��������������� ��������, ��� ������ ������.
        -- ��� dratehist_dbt ��������� ��� ������ - �� ������� � ��������. ������ ������������ ���������� �������/��������, ��������� ������.
        -- � ����� ������� ������ ����������� ��������� �������� �����. ���������� ������� � ratehist � ������ dratedef_dbt   
        cursor m_cur(pp_date date, pp_action number) is select * from DTXCOURSE_DBT where t_instancedate between pp_date and pp_date+1 and t_action = pp_action order by t_instancedate, t_action, t_basefiid, t_marketsectorid, t_type;
        
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

        add_tmp  rate_sou_add_type;
        main_tmp rate_sou_type;
        stat_tmp number;
        rateind_tmp number;
        rateindstr_tmp varchar2(100);
        rate_tmp number; -- ���� �� ��� � ������� ���� �� �������� ����
        is_last_date boolean;  -- ���� �� ��� ���� ����� � dratedef_dbt, �� ���� ���� ����� ������
        dratedef_tmp dratedef_dbt%rowtype;
        ---------------------------------------------------------------------------------------------------------------------------------------------        
        

    
         
        -- �������� �������� � �������. �������� ������ � ��������� ������ �� ���������.
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
        

        -- ������������� ����� ������
        function RTYPE( p_tp number ) return number
        is
            v_rez number;
        begin
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
            else return -p_tp;
        end case;
        end;


        -- ���������� ������ � ����� dratedef
        procedure add_dratedef_buf( buf dratedef_dbt%rowtype := null, T_RATEID number := null, T_FIID number := null, T_OTHERFI number := null, T_NAME varchar2 := null, T_DEFINITION varchar2 := null, T_TYPE number := null, T_ISDOMINANT char := null, T_ISRELATIVE char := null, T_INFORMATOR number := null, T_MARKET_PLACE number := null, T_ISINVERSE char := null, T_RATE number := null, T_SCALE number := null, T_POINT number := null, T_INPUTDATE date := null, T_INPUTTIME date := null, T_OPER number := null, T_SINCEDATE date := null, T_SECTION number := null, T_ISMANUALINPUT char := null)
        is
            tmp dratedef_dbt%rowtype;
        begin
            if buf is not null then 
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
            
            if T_RATEID = 0 and nvl(tmp.rate_id) = 0
            then
                tmp.rate_id := dratedef_dbt_seq.nextval;
                dratedef_arr(  tmp.rate_id ) := tmp; 
            end if;
            
            --TODO  new_dratedef_arr( new_dratedef_arr.count() ) := tmp;
            insert into dratedef_dbt values tmp; 
            
        end add_dratedef_buf;
        
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
        procedure add_dratehist_buf( T_RATEID number, T_SINCEDATE date)        
        is
            T_INPUTDATE date := trim(sysdate);
            T_INPUTTIME date := to_date('0001-01-01') + (sysdate - trim(sysdate));
            tmp dratehist_dbt%rowtype; 
        begin
            tmp.t_rateid := t_rateid;
            tmp.T_SINCEDATE := T_SINCEDATE;
            
            del_dratehist_arr( del_dratehist_arr.count ) := tmp;

        end add_dratehist_buf;


        
    begin
        
        open m_cur(p_date, p_action);
        loop
            
            -- �������� ������ ������
            fetch m_cur bulk collect into rate_sou_arr limit g_limit;
            exit when rate_sou_arr.count=0;
            
            -- ������������ ��� �������� ��� �������� �� REPLOBJ
            for i in 1..rate_sou_arr.count
            loop
                -- �������� ���������� fiid
                replobj_add( c_OBJTYPE_MONEY, rate_sou_arr(i).t_fiid, '���������� �������������');  
                replobj_add( rate_sou_arr(i).T_BASEFIKIND, rate_sou_arr(i).t_basefiid, '������� �������������');  
                
                -- �������� ���������� MARKETID
                replobj_add( c_OBJTYPE_MARKET, rate_sou_arr(i).T_MARKETID, '�������� ��������');  
                replobj_add( c_OBJTYPE_MARKET_SECTION, rate_sou_arr(i).T_MARKETID, rate_sou_arr(i).T_MARKETSECTORID);  
                
                -- ����������, ����
                replobj_add( c_OBJTYPE_RATE, rate_sou_arr(i).T_COURSEID);
            end loop;
            
            -- ��������� ��� �� REPLOBJ
            replobj_load;
            
            -- ���������� ������, ��������� ����������������� ������ �������������� ���������. �������� ���������� �������.
            for i in 1..rate_sou_arr.count
            loop
                main_tmp := rate_sou_arr(i); 
                
                add_tmp.isdominant := case when ( main_tmp.T_BASEFIKIND = 10 and main_tmp.t_type = 6 ) THEN chr(88) else chr(0) end;
                
                add_tmp.type := rtype( main_tmp.t_type );       -- ������������ ��� �����   
                add_tmp.rate_date := main_tmp.t_ratedate;       -- ��� ������������� �� SQL, ��� #R 
                
                add_tmp.tgt_rate_id :=  replobj_get(c_OBJTYPE_RATE, main_tmp.t_rateid, main_tmp.t_type).dest_id;
                                
                stat_tmp :=  replobj_get( c_OBJTYPE_RATE, main_tmp.t_rateid, main_tmp.t_type).state;
                        if  ( add_tmp.tgt_rate_id = 0) and (p_action > 1 ) then 
                            pr_exclude(419, 70, '������: ���������� %act% �������������� ����, ���������� ���������� - %fiid%', i, p_action );
                        elsif ( stat_tmp = 1) and (p_action > 1 ) then
                            pr_exclude(205, 70, '������: ������ ��������� � ������ ������� ��������������, ���������� ���������� - %fiid%', i, p_action); 
                        end if;     

                add_tmp.fi  :=  replobj_get( c_OBJTYPE_MONEY, main_tmp.t_fiid).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(527, 70, '������: ���������� ������ ������� � ������ ��� ��������������� ����������� ����������� �����������, ������� ���������� - %base_fiid%, ��� ����� - %type%', i, p_action);
                        end if;
                      
                add_tmp.market_id   :=  replobj_get( c_OBJTYPE_MARKET, main_tmp.T_MARKETID).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(525, 70, '������: ���������� ������ ������� � ������ ��� �������������� �������� ��������, ���������� ���������� - %fiid%', i, p_action);
                        end if;
                add_tmp.section_id  :=  replobj_get( c_OBJTYPE_MARKET_SECTION, main_tmp.T_MARKETID, main_tmp.T_MARKETSECTORID).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(525, 70, '������: ���������� ������ ������� � ������ ��� �������������� ������ �������� ��������, ���������� ���������� - %fiid%', i, p_action);
                        end if;
                add_tmp.base_fi     :=  replobj_get( main_tmp.T_BASEFIKIND, main_tmp.t_basefiid).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(528, 70, '������: ���������� ������ ������� � ������ ��� ��������������� �������� ����������� �����������, ���������� ���������� - %fiid%, ��� ����� - %type%', i, p_action);
                        end if;
                add_tmp.isrelevant := case when ( main_tmp.T_BASEFIKIND = 20 and FI_IsBond(add_tmp.base_fi) THEN chr(88) else chr(0) end;                        
                        
                rate_sou_add_arr(i) := add_tmp;
            
            end loop; 
                        
            -- ��������� �� ������� ������� �������� ����� (�� �������� ������) �� ���� action
            select * bulk collect into dratedef_arr_tmp from dratedef_dbt where t_rateid in ( select tgt_rate_id from table(rate_sou_add_arr) );
            for i in 1..dratedef_arr_tmp.count
            loop
                -- ��������������� ��������� �� rate_id
                dratedef_arr( dratedef_arr_tmp(i).rate_id ) := dratedef_arr_tmp(i);
            end loop;
            -- ��������� ������ �� �����
            dratedef_arr_tmp.delete;
                
            -- ��������� ����� �� �������. #R
            -- ������ �������� �� ������ ������ �� ������ ����
            -- ����� ����������� ����. ����� ����������������� � ��������� � varchar ��������, �� ��� ����� ������� �����.
            -- ��������� ��� ���� ����� ������������ ��� ���� ������� ��������, � �� ��� ��������.
            select * bulk collect into dratehist_arr from dratedef_dbt where (t_rateid, t_sincedate) in ( select tgt_rate_id, rate_date from table(rate_sou_add_arr) );
            -- TODO �������� emerg_limit 
            for i in 1..dratehist_arr.count
            loop
                -- ������� ��������� ��������� 
                -- ���� ���� "12345#12082020", {rate_id}#{����_������_��������_�����}
                dratehist_ind_arr( to_char(dratehist_arr(i).rate_id) || '#' || to_char(dratehist_arr(i).t_sincedate, 'ddmmyyyy') ) := i;
            end loop;
            ---------------------------------------------------------------------------------------------------------------------------------------------            
            --- ��� ������ ���������

            -- ��� ��� �������� �� �������� ���������. ���������, ���� �� ���� � �������, � ��������� 
            for i in 1..rate_sou_arr.count
            loop
                    add_tmp := rate_sou_add_arr(i);
                    main_tmp := rate_sou_arr(i);
                    -- ��������� ����
                    is_last_date := false;
                    rateindstr_tmp := add_tmp.tgt_rate_id || '#' || to_char(add_tmp.rate_date, 'ddmmyyyy');
                    if dratehist_ind.exists( rateindstr_tmp)
                    then
                        rate_tmp := dratehist_arr( dratehist_ind_arr ( rateindstr_tmp)).t_rate;  -- �������� ����� �� ���� ������� � ������� �������
                    else 
                        -- ��������, �� ��������� �� ��� ����
                        if exists dratedef_arr( add_tmp.rate_id ) and ( dratedef_arr( add_tmp.rate_id ).t_sincedate = add_tmp.rate_date )
                        then
                            rate_tmp := dratedef_arr( add_tmp.rate_id ).t_rate;
                            is_last_date := true;
                        else
                            rate_tmp := 0;  -- �������� ����� �� ���� ����������� � ������� �������.
                        end if;
                    end if;

                    case p_action
                    when 1 then -- ����������
                        if rate_tmp > 0 then  -- ������: ��� ���������� ������ ���� �� ����� ����
                                -- ��������, ����������� �� �� � ������������
                                if rate_tmp <> rate_sou_arr(i).t_rate
                                then
                                   pr_exclude(418, 70, '������: ��� ���������� ������ ���� �� ���� %date% �� �������������� %fiid%, ��� ����� - %type%', i, p_action);
                                   continue;
                                end if;
                                -- ���� ���� ���������� �������, �� ������ ������
                        else    -- ��������� � �����
        
                                if  add_tmp.rate_date > dratedef_arr( add_tmp.rate_id ).t_sincedate
                                then 
                                        -- ���� ������ ����� ������ ��������, ��������� ������� �� ratedef � �������
                                        begin
                                            INSERT INTO dratehist_dbt(t_rateid, t_isinverse, t_rate, t_scale, t_point, t_inputdate, t_inputtime, t_oper, t_sincedate, t_ismanualinput) 
                                            SELECT t_rateid, t_isinverse, t_rate, t_scale, t_point, t_inputdate, t_inputtime, t_oper, t_sincedate, t_ismanualinput 
                                            FROM dratedef_dbt where t_rateid = add_tmp.rate_id;
                                        exception 
                                            when dup_val_on_index then null;  -- ��� ��������� ������ � ������������ �������
                                        end;
                                    
                                            dratedef_tmp.t_fiid := add_tmp.fi;
                                            dratedef_tmp.t_otherfi := add_tmp.base_fi; 
                                            dratedef_tmp.t_name := chr(1);
                                            dratedef_tmp.T_DEFINITION := chr(1); 
                                            dratedef_tmp.T_TYPE := add_tmp.type;
                                            dratedef_tmp.T_ISDOMINANT := add_tmp.isdominant;
                                            dratedef_tmp.T_ISRELATIVE := add_tmp.isrelative;
                                            dratedef_tmp.T_INFORMATOR := add_tmp.market_place;
                                            dratedef_tmp.T_MARKET_PLACE := add_tmp.market_place;
                                            dratedef_tmp.T_ISINVERSE := add_tmp.isinverse;
                                            dratedef_tmp.T_RATE := main_tmp.rate * power(10, main_tmp.t_point);
                                            dratedef_tmp.T_SCALE := main_tmp.t_scale;
                                            dratedef_tmp.T_POINT := main_tmp.t_point;
                                            dratedef_tmp.T_INPUTDATE := trunc(sysdate);
                                            dratedef_tmp.T_INPUTTIME := date'0001-01-01' + (sysdate - trunc(sysdate));
                                            dratedef_tmp.T_OPER :=  1;
                                            dratedef_tmp.T_SINCEDATE := main_tmp.t_ratedate;
                                            dratedef_tmp.T_SECTION := add_tmp.section;
                                            dratedef_tmp.T_ISMANUALINPUT :=  chr(0);

                                        if not dratedef_arr.exists( add_tmp.rate_id ) -- ������ ����� ������ �� ����
                                        then 
                                            dratedef_arr( add_tmp.rate_id ) := dratedef_dbt_seq.nextval;
                                            insert into dratedef_dbt values dratedef_tmp;
                                            --TODO  add_dratedef_buf(dratedef_tmp);  -- ������ � ���� � � �����
                                        else
                                            dratedef_tmp.t_rateid := dratedef_arr( add_tmp.rate_id );
                                            update dratedef_dbt set row = dratedef_tmp where t_rateid = dratedef_tmp.t_rateid;
                                        end if;
                                        -- ��������� ��� ��������� �����
                                        dratedef_arr(  tmp.rate_id ) := dratedef_tmp;
                                else
                                -- ������������ ����� ������ � �������
                                
                                end if;
--------------------------------------------------------
--------------------------------------------------------                                        
                        end if;
                        
                    when 2 then
                        if rateind_tmp = 0 then                    
                            -- ������, �������� ����������� ����� �� ������� � ������� �������
                        end if;                            
                    when 3 then 
                        if rateind_tmp = 0 then                    
                            -- ������, �������� ���������� ����� �� ������� � ������� �������
                        end if;                            
                    end case;
                    
            end loop;


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