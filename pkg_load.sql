create or replace package load_rss
is
    -- параметры
    g_limit number := 1000; -- количество одновременно загружаемых записей источника
    
    -- константы типов
    OBJTYPE_RATE constant number := 70;
    OBJTYPE_MONEY constant number  := 10;
    OBJTYPE_SEC  constant number  := 20;
    OBJTYPE_MARKET  constant number  := 30;
    OBJTYPE_MARKET_SECTION  constant number  := 40;    
    
    
    -- кэш таблицы котировок из источника    
    type rate_sou_arr_type is table of DTXCOURSE_DBT%ROWTYPE; 
    type rate_sou_add_type is record(   -- дополнительные поля, привязываются к строкам коллекци rate_sou_arr_type по индексам   
                                    tgt_rate_id number,  -- id курса в целевой системе, 0 если нет
                                    tgt_rate_val number, -- значение курса за дату в целевой системе
                                    tgt_rate_date date,  -- дата текущего значения курса в целевой системе
                                    market_id number,    -- перекодированный ID биржи
                                    section_id number,   -- перекодированный ID сектора биржи
                                    base_fi number,      -- перекодированный базовый FI
                                    base_fi_name varchar2(500), -- имя базового FI?
                                    fi number,           -- перекодированный котируемый id
                                    isrelative boolean  -- признак относительного курса,
                                    excluded boolean := false; -- по курсу не найдена важная информация, он исключен из обработки
                                  );
    type rate_sou_add_arr_type is table of rate_sou_add_type index by pls_integer;
    
    rate_sou_arr        rate_sou_arr_type;
    rate_sou_add_arr    rate_sou_add_arr_type;                                     
    
    -- кэш истории курса 
    type rhist_tgt_arr_type is table of dratehist_dbt%ROWTYPE;  
    
    -- кэши связанных сущностей
    type fi_type is record( tgt_id number, tgt_fi_name varchar2(100));    
    type fi_arr_type is table of fi_type index by pls_integer;
    type fi_id_type is table of number;
    fi_arr fi_arr_type;
    procedure add_fi_id( p_id pls_integer); -- добавляет ID в массив, если его там нет
    procedure add_fi_cache; -- загружает кэш по всем fiid из массива
    procedure clear_fi_cache; -- очищает кэш fi
    
    -- основная процедура --
    procedure load_rate(p_date date, p_action number);
    
    ---------------------------------------------------------------------------------------------------------------------------------------------   
    -- работа с кэшем replobj
    -- основная таблица должна индексироваться числом, иначе невозможно будет использовать ее в выражении TABLE
    -- но для ускорения поиска при вводе добавляется отдельнаю индекснаю коллекция replobj_rec_inx_arr
    type replobj_rec_type            is record(objtype pls_integer, obj_id number, obj_sub_id number, dest_id number, state pls_integer, comment varchar2(100));
    type replobj_rec_arr_type        is table of replobj_rec_type index by pls_integer;  -- основная коллекция
    type replobj_rec_inx_arr_type    is table of pls_integer index by varchar2(200);     -- индексная коллекция, указывает на индекс основной, индексируется конкатенацией составного ключа (objtype,obj_id,obj_sub_id)
    type replobj_tmp_arr_type        is table of dtxreplobj_dbt%rowtype;                 -- временный буфер для BULK COLLECT, пока не разбросаем по коллекциям выше

    replobj_rec_arr             replobj_rec_arr_type;  
    replobj_rec_inx_arr         replobj_rec_inx_arr_type; -- индексная коллекция
    replobj_tmp_arr             replobj_tmp_arr_type;
    
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number, p_comment varchar2 default ''); -- добавление входного ID в коллекцию
    procedure replobj_load; -- загрузка коллекции
    function replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number) return replobj_rec_type; -- возврат значения из кэша REPLOBJ
    -- procedure savestat;   -- записать в лог статистику по объему кэша в разрезе типов
    -- procedure replobj_clear; -- очистка всех коллекций  
    ----------------------------------------------------------------------------------------------------------------------------------------------
    
    
    
    -- работа с логом
    type log_rows_type is table of dtxloadlog%rowtype;  -- для отложенной записи в лог.
    log_rows  log_rows_type; 
    -- процедура непосредственной записи в лог
    procedure add_log( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- запись в лог
    -- процедура отложенной записи. Когда ожидаем большой объем логирования с чисто логическими ошибками, можно накопить записи в коллекцию, потом протолкнуть в лог одним блоком
    -- procedure add_log_deferred( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- запись в лог
    -- procedure add_log_exec; -- сохраняем в таблицу лога. Очищаем буферную коллекцию.
     
    
end load_rss;


create or replace package body load_rss
is
    -- перекодировка типов курсов
    function RTYPE( p_tp number ) return number
    is
        v_rez number;
    begin
    if    p_tp = 26 then v_rez := 23;
    elsif p_tp = 10 then v_rez := 23;
    elsif p_tp = 27 then v_rez := 1001;
    elsif p_tp = 28 then v_rez := 1002;
    elsif p_tp = 29 then v_rez := 1003;
   -- дальше странно
    elsif p_tp = 1 then v_rez := 2;
    elsif p_tp = 2 then v_rez := 3;
    elsif p_tp = 3 then v_rez := 4;
    elsif p_tp = 4 then v_rez := 15;
    elsif p_tp = 5 then v_rez := 9;
    elsif p_tp = 6 then v_rez := 7;
    elsif p_tp = 7 then v_rez := 16; --KD 16 - номер курса "Объем торгов"
    -------------------------------------------------------------------------
    else return -p_tp;
    end if;
end;


    -- проверяем наличие переданного объекта в коллекции. Если его нет, дописываем. Ключ комплексный, поисковый.
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
            -- запись в индексную коллекцию            
            replobj_rec_inx_arr( v_searchstr ) := mas_idx;
        end if;
        return;
    end replobj_add;
    
    -- возвращаем из кэша запись указанного объекта. Если отсутствует в кэше, возвращаем -1
    -- проверяем наличие элемента в индексной коллекции. Если есть, берем из индексной значение индекса,
    -- по нему отбираем элемент в основной.
    function replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number default 0) return replobj_rec_type
    is
        v_searchstr varchar2(200);
    begin
        v_searchstr := to_char(p_objtype) || '#' || to_char(p_obj_id) || '#' || to_char(p_obj_sub_id);
        if replobj_rec_inx_arr.exists(v_searchstr) then  
            return replobj_rec_arr( replobj_rec_inx_arr(v_searchstr));
        end if;
        return replobj_rec_arr( -1 );  -- все поля этой записи нулевые       
    end replobj_get;      
    
    
    -- Догружаем только те записи, у которых нет DEST_ID в кэше
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
            -- слишком много записей, сформировать исключение
        END IF;
        -- переберем элементы и обогатим кэш статусами и replid
        FOR i in 1..replobj_tmp_arr.count
        LOOP
            -- определяем индекс элемента в основном массиве
            v_search_str := to_char(replobj_tmp_arr(i).t_objecttype) || '#' || to_char(t_objectid) || '#' || nvl(to_char(t_subobjnum),'0');
            v_search_idx := replobj_rec_inx_arr(v_search_str);  -- нашли индекс записи в основном массиве
            -- адресуем элемент
            replobj_rec_arr(v_search_idx).dest_id := replobj_tmp_arr(i).T_DESTID;
            replobj_rec_arr(v_search_idx).state := replobj_tmp_arr(i).t_objstate;
        END LOOP;             
        
        -- добавим элемент, который будем использовать, как NULL
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
        type tmp_num_arr_type is table of number;  -- nested table, ради операций над множествами
        --fiid_arr tmp_num_array = tmp_num_array();
        --market_arr tmp_num_array = tmp_num_array();
        --marksect_arr tmp_num_array = tmp_num_array();
        
        -- добавление во временную таблицу элементов
        procedure add_to_array( p_arr IN OUT NOCOPY tmp_num_array, p_id number ) 
        is begin
            p_arr.extend();
            p_arr(p_arr.count) := p_id;
        end add_to_array;
         
        -- проблемы с записью. ЛОгируем ошибку и исключаем запись из обработки.
        procedure pr_exclude(p_code number, p_objtype number, p_text varchar2(1000), p_counter number, p_action number)
        is 
            text_corr varchar2(1000);
            v_row DTXCOURSE_DBT%ROWTYPE;
        begin
            v_row := rate_sou_arr(p_counter);
            text_corr := replace(p_text, '%act%',  (case p_action when 1 then 'Вставка' when 2 then 'изменение' when 3 then 'удаление' end) );
            text_corr := replace(text_corr, '%fiid%', v_row.t_fiid);
            text_corr := replace(text_corr, '%base_fiid%', v_row.t_basefiid);
            text_corr := replace(text_corr, '%type%', v_row.t_type);
            -- потом заменить на add_log_deferred 
            add_log( p_code, p_objtype, p_id, p_subnum, text_corr, p_date);
            
            -- исключаем элемент
            rate_sou_add_arr(p_counter).exclude := true;

        end pr_exclude;
        
        
        add_tmp  rate_sou_add_type;
        stat_tmp number;
                
    begin
        fiid_arr := tmp_num_array();
        
        open m_cur(p_date, p_action);
        loop
            -- загружаем все MARKETID и MARKETSECTORID
            
            -- загрузка порции данных
            fetch m_cur bulk collect into rate_sou_arr limit g_limit;
            exit when rate_sou_arr.count=0;
            -- заполнение
             
            for i in 1..rate_sou_arr.count
            loop
                -- собираем уникальные fiid
                replobj_add( OBJTYPE_MONEY, rate_sou_arr(i).t_fiid, 'котируемый фининструмент');  -- сразу регистрируем их для загрузки
                replobj_add( rate_sou_arr(i).T_BASEFIKIND, rate_sou_arr(i).t_basefiid, 'базовый фининструмент');  -- сразу регистрируем их для загрузки
                
                -- собираем уникальные MARKETID
                replobj_add( OBJTYPE_MARKET, rate_sou_arr(i).T_MARKETID, 'торговая площадка');  
                replobj_add( OBJTYPE_MARKET_SECTION, rate_sou_arr(i).T_MARKETID, rate_sou_arr(i).T_MARKETSECTORID);  
                
                -- собственно, курс
                replobj_add( OBJTYPE_RATE, rate_sou_arr(i).T_COURSEID);
            end loop;
            
            -- заполняем кэш
            replobj_load;
            
            -- перебираем заново, заполняем перекодированными полями подолнительную коллекцию. Логируем отсутствие записей.
            for i in 1..rate_sou_arr.count
            loop
                add_tmp.fi          :=  replobj_get( OBJTYPE_MONEY, rate_sou_arr(i).t_fiid).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(527, 70, 'Ошибка: невозможно ничего сделать с курсом для несуществующего котируемого финансового инструмента, базовый инструмент - %base_fiid%, тип курса - %type%', i, p_action);
                        end if;
                add_tmp.tgt_rate_id :=  replobj_get(OBJTYPE_RATE, rate_sou_arr(i).t_rateid).dest_id;
                stat_tmp :=  replobj_get( OBJTYPE_RATE, rate_sou_arr(i).t_rateid).state;
                        if  ( add_tmp.tgt_rate_id = 0) and (p_action > 1 ) then 
                            pr_exclude(419, 70, 'Ошибка: невозможно %act% несуществующий курс, финансовый инструмент - %fiid%', i, p_action );
                        elsif ( stat_tmp = 1) and (p_action > 1 ) then
                            pr_exclude(205, 70, 'Ошибка: объект находится в режиме ручного редактирования, финансовый инструмент - %fiid%', i, p_action); 
                        end if;                           
                add_tmp.market_id   :=  replobj_get( OBJTYPE_MARKET, rate_sou_arr(i).T_MARKETID).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(525, 70, 'Ошибка: невозможно ничего сделать с курсом для несуществующей торговой площадки, финансовый инструмент - %fiid%', i, p_action);
                        end if;
                add_tmp.section_id  :=  replobj_get( OBJTYPE_MARKET_SECTION, rate_sou_arr(i).T_MARKETID, rate_sou_arr(i).T_MARKETSECTORID).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(525, 70, 'Ошибка: невозможно ничего сделать с курсом для несуществующей секции торговой площадки, финансовый инструмент - %fiid%', i, p_action);
                        end if;
                add_tmp.base_fi     :=  replobj_get( rate_sou_arr(i).T_BASEFIKIND, rate_sou_arr(i).t_basefiid).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(528, 70, 'Ошибка: невозможно ничего сделать с курсом для несуществующего базового финансового инструмента, котируемый инструмент - %fiid%, тип курса - %type%', i, p_action);
                        end if;
                rate_sou_add_arr(i) := add_tmp;
            
            end loop; 
            
            
            -- загружаем из целевой системы значение курса (из основной записи, не из истории) по всем action
            -- загружаем из истории буфер в отдельный.
                 
            

        end loop; 
                
        
    end load_rate;







    -- запись в лог
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
    
        -- запись в лог
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