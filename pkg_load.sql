create or replace package load_rss
is
    -- параметры
    g_limit number := 1000; -- количество одновременно загружаемых записей источника
    
    -- константы типов
    c_OBJTYPE_RATE constant number := 70;
    c_OBJTYPE_MONEY constant number  := 10;
    c_OBJTYPE_SEC  constant number  := 20;
    c_OBJTYPE_MARKET  constant number  := 30;
    c_OBJTYPE_MARKET_SECTION  constant number  := 40;    

    c_RATE_TYPE_NKDONDATE constant number := 15;   -- тип курса "НКД на дату"
    c_RATE_TYPE_NOMINALONDATE constant number :=  100; -- репликация номиналов ц/б на дату (реплицируются из таблицы курсов)
    
    ---------------------------------------------------------------------------------------------------------------------------------------------    
    -- кэш таблицы котировок из источника    
    type rate_sou_arr_type is table of DTXCOURSE_DBT%ROWTYPE; 
    type rate_sou_add_type is record(   -- дополнительные поля, привязываются к строкам коллекци rate_sou_arr_type по индексам   
                                    tgt_rate_id number,  -- id курса в целевой системе, 0 если нет
                                    tgt_rate_val number, -- значение курса за дату в целевой системе
                                    rate_date date,      -- дата текущего значения курса 
                                    type_id  number,     -- перекодированный тип
                                    market_id number,    -- перекодированный ID биржи
                                    section_id number,   -- перекодированный ID сектора биржи
                                    base_fi number,      -- перекодированный базовый FI
                                    fi number,           -- перекодированный котируемый id
                                    isdominant char,     -- признак основного курса валюты, курс ЦБ
                                    isrelative char,  -- признак относительного курса
                                    result number(1)     -- будущий t_replstate. 1 - обработан успешно, 2 - ошибка
                                  );
    type rate_sou_add_arr_type is table of rate_sou_add_type index by pls_integer;
    
    rate_sou_arr        rate_sou_arr_type;
    rate_sou_add_arr    rate_sou_add_arr_type;                                     
    ---------------------------------------------------------------------------------------------------------------------------------------------    
    
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
    
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number, p_comment varchar2 ); -- добавление входного ID в коллекцию
    procedure replobj_load; -- загрузка коллекции
    function replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number) return replobj_rec_type; -- возврат значения из кэша REPLOBJ
    -- procedure savestat;   -- записать в лог статистику по объему кэша в разрезе типов
    -- procedure replobj_clear; -- очистка всех коллекций  
    ----------------------------------------------------------------------------------------------------------------------------------------------
    
    
    ---------------------------------------------------------------------------------------------------------------------------------------------    
    -- работа с логом
    type log_rows_type is table of dtxloadlog%rowtype;  -- для отложенной записи в лог.
    log_rows  log_rows_type; 
    -- процедура непосредственной записи в лог
    procedure add_log( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- запись в лог
    -- процедура отложенной записи. Когда ожидаем большой объем логирования с чисто логическими ошибками, можно накопить записи в коллекцию, потом протолкнуть в лог одним блоком
    -- procedure add_log_deferred( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- запись в лог
    -- procedure add_log_exec; -- сохраняем в таблицу лога. Очищаем буферную коллекцию.
    ---------------------------------------------------------------------------------------------------------------------------------------------     
    
end load_rss;








create or replace package body load_rss
is

    -- проверяем наличие переданного объекта в коллекции. Если его нет, дописываем. Ключ комплексный, поисковый. TODO В перспективе заменить на HASH с проверкой
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
        -- Алгоритм следующий - считываем данные по n записей, собираем id всех связанных сущностей и загружаем их массово.
        -- затем из целевой системы загружаем описания и историю курсов, соответствующие обрабатываемым записям.
        -- Дальше в соответствии с заданным action и наличием/отсутствием курса в целевой системе формируем буфер записей.
        -- Для dratedef_dbt записи массово будут только апдейтиться. Если что-то не подпадает под операцию апдейта, будет рассматриваться отдельно, это редкий случай.
        -- Для dratehist_dbt создается два буфера - на уставку и удаление. Апдейт моделируется операциями вставки/удаления, поскольку редкий.
        -- В самом обычном режиме обновляется последнее значение курса. Проводится вставка в ratehist и апдейт dratedef_dbt   
        cursor m_cur(pp_date date, pp_action number) is select * from DTXCOURSE_DBT where t_instancedate between pp_date and pp_date+1 and t_action = pp_action order by t_instancedate, t_action, t_basefiid, t_marketsectorid, t_type;
        
        type index_collection_type is table of number index by varchar2(100);  -- тип индексной коллекции, используется для поиска связей сущностей.
          
        ---------------------------------------------------------------------------------------------------------------------------------------------
        -- кэш курсов
        type dratedef_arr_type is table of dratedef_dbt%rowtype index by pls_integer;
        -- кэш истории курса
        type add_dratehist_arr_type is table of dratehist_dbt%ROWTYPE index by pls_integer;
        type del_dratehist_type is record( t_rateid number, t_sincedate date );
        type del_dratehist_arr_type is table of del_dratehist_type index by pls_integer;
        ---
        dratedef_arr_tmp  dratedef_arr_type;       --  IN, для bulk collect
        dratedef_arr      dratedef_arr_type;       --  IN, коллекция индексирована rate_id из целевой системы
        dratehist_arr     add_dratehist_arr_type;  --  IN, данные из целевой системы.  
        dratehist_ind_arr  index_collection_type;  --  IN, индексная коллекция для поиска в истории. Хранит индексы коллекции dratehist_arr
        new_dratehist_arr  add_dratehist_arr_type; --  OUT, буфер для вставки данных в историю.
        del_dratehist_arr  del_dratehist_arr_type; --  OUT, буфер для удаления данных из истории.
        new_dratedef_arr   dratedef_arr_type;      --  OUT, буфер для изменения самого курса. Данные в таблице массово только меняются. Если курс новый - вводим пустышку и сводим задачу к update. Если delete - удаляем сразу
        ---------------------------------------------------------------------------------------------------------------------------------------------

        add_tmp  rate_sou_add_type;
        main_tmp rate_sou_type;
        stat_tmp number;
        rateind_tmp number;
        rateindstr_tmp varchar2(100);
        rate_tmp number; -- есть ли уже в системе курс на заданную дату
        is_last_date boolean;  -- курс за эту дату лежит в dratedef_dbt, то есть дата самая свежая
        dratedef_tmp dratedef_dbt%rowtype;
        ---------------------------------------------------------------------------------------------------------------------------------------------        
        

    
         
        -- выявлены проблемы с записью. Логируем ошибку и исключаем запись из обработки.
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
        

        -- перекодировка типов курсов
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
            -- дальше странно
            when p_tp = 1 then v_rez := 2;
            when p_tp = 2 then v_rez := 3;
            when p_tp = 3 then v_rez := 4;
            when p_tp = 4 then v_rez := 15;
            when p_tp = 5 then v_rez := 9;
            when p_tp = 6 then v_rez := 7;
            when p_tp = 7 then v_rez := 16; --KD 16 - номер курса "Объем торгов"
            -------------------------------------------------------------------------
            else return -p_tp;
        end case;
        end;


        -- добавление записи в буфер dratedef
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
        
        -- добавление записи в буфер dratehist на вставку        
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

        -- добавление записи в буфер dratehist на удаление        
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
            
            -- загрузка порции данных
            fetch m_cur bulk collect into rate_sou_arr limit g_limit;
            exit when rate_sou_arr.count=0;
            
            -- регистрируем все сущности для загрузки из REPLOBJ
            for i in 1..rate_sou_arr.count
            loop
                -- собираем уникальные fiid
                replobj_add( c_OBJTYPE_MONEY, rate_sou_arr(i).t_fiid, 'котируемый фининструмент');  
                replobj_add( rate_sou_arr(i).T_BASEFIKIND, rate_sou_arr(i).t_basefiid, 'базовый фининструмент');  
                
                -- собираем уникальные MARKETID
                replobj_add( c_OBJTYPE_MARKET, rate_sou_arr(i).T_MARKETID, 'торговая площадка');  
                replobj_add( c_OBJTYPE_MARKET_SECTION, rate_sou_arr(i).T_MARKETID, rate_sou_arr(i).T_MARKETSECTORID);  
                
                -- собственно, курс
                replobj_add( c_OBJTYPE_RATE, rate_sou_arr(i).T_COURSEID);
            end loop;
            
            -- заполняем кэш из REPLOBJ
            replobj_load;
            
            -- перебираем заново, заполняем перекодированными полями дополнительную коллекцию. Логируем отсутствие записей.
            for i in 1..rate_sou_arr.count
            loop
                main_tmp := rate_sou_arr(i); 
                
                add_tmp.isdominant := case when ( main_tmp.T_BASEFIKIND = 10 and main_tmp.t_type = 6 ) THEN chr(88) else chr(0) end;
                
                add_tmp.type := rtype( main_tmp.t_type );       -- перекодируем тип курса   
                add_tmp.rate_date := main_tmp.t_ratedate;       -- для использования из SQL, тег #R 
                
                add_tmp.tgt_rate_id :=  replobj_get(c_OBJTYPE_RATE, main_tmp.t_rateid, main_tmp.t_type).dest_id;
                                
                stat_tmp :=  replobj_get( c_OBJTYPE_RATE, main_tmp.t_rateid, main_tmp.t_type).state;
                        if  ( add_tmp.tgt_rate_id = 0) and (p_action > 1 ) then 
                            pr_exclude(419, 70, 'Ошибка: невозможно %act% несуществующий курс, финансовый инструмент - %fiid%', i, p_action );
                        elsif ( stat_tmp = 1) and (p_action > 1 ) then
                            pr_exclude(205, 70, 'Ошибка: объект находится в режиме ручного редактирования, финансовый инструмент - %fiid%', i, p_action); 
                        end if;     

                add_tmp.fi  :=  replobj_get( c_OBJTYPE_MONEY, main_tmp.t_fiid).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(527, 70, 'Ошибка: невозможно ничего сделать с курсом для несуществующего котируемого финансового инструмента, базовый инструмент - %base_fiid%, тип курса - %type%', i, p_action);
                        end if;
                      
                add_tmp.market_id   :=  replobj_get( c_OBJTYPE_MARKET, main_tmp.T_MARKETID).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(525, 70, 'Ошибка: невозможно ничего сделать с курсом для несуществующей торговой площадки, финансовый инструмент - %fiid%', i, p_action);
                        end if;
                add_tmp.section_id  :=  replobj_get( c_OBJTYPE_MARKET_SECTION, main_tmp.T_MARKETID, main_tmp.T_MARKETSECTORID).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(525, 70, 'Ошибка: невозможно ничего сделать с курсом для несуществующей секции торговой площадки, финансовый инструмент - %fiid%', i, p_action);
                        end if;
                add_tmp.base_fi     :=  replobj_get( main_tmp.T_BASEFIKIND, main_tmp.t_basefiid).dest_id;
                        if  ( add_tmp.market_id = 0) and (p_action > 1 ) then 
                            pr_exclude(528, 70, 'Ошибка: невозможно ничего сделать с курсом для несуществующего базового финансового инструмента, котируемый инструмент - %fiid%, тип курса - %type%', i, p_action);
                        end if;
                add_tmp.isrelevant := case when ( main_tmp.T_BASEFIKIND = 20 and FI_IsBond(add_tmp.base_fi) THEN chr(88) else chr(0) end;                        
                        
                rate_sou_add_arr(i) := add_tmp;
            
            end loop; 
                        
            -- загружаем из целевой системы значение курса (из основной записи) по всем action
            select * bulk collect into dratedef_arr_tmp from dratedef_dbt where t_rateid in ( select tgt_rate_id from table(rate_sou_add_arr) );
            for i in 1..dratedef_arr_tmp.count
            loop
                -- переиндексируем коллекцию по rate_id
                dratedef_arr( dratedef_arr_tmp(i).rate_id ) := dratedef_arr_tmp(i);
            end loop;
            -- временная больше не нужна
            dratedef_arr_tmp.delete;
                
            -- загружаем буфер из истории. #R
            -- только значения по нужным курсам за нужные даты
            -- здесь комплексный ключ. Можно переиндексировать в коллекцию с varchar индексом, но тут проще сделать новую.
            -- поскольку нас чаще будет интересовать сам факт наличия элемента, а не его атрибуты.
            select * bulk collect into dratehist_arr from dratedef_dbt where (t_rateid, t_sincedate) in ( select tgt_rate_id, rate_date from table(rate_sou_add_arr) );
            -- TODO добавить emerg_limit 
            for i in 1..dratehist_arr.count
            loop
                -- создаем поисковую коллекцию 
                -- ключ вида "12345#12082020", {rate_id}#{дата_начала_действия_курса}
                dratehist_ind_arr( to_char(dratehist_arr(i).rate_id) || '#' || to_char(dratehist_arr(i).t_sincedate, 'ddmmyyyy') ) := i;
            end loop;
            ---------------------------------------------------------------------------------------------------------------------------------------------            
            --- все данные загружены

            -- еще раз проходим по датасету источника. Проверяем, есть ли курс в таргете, и реагируем 
            for i in 1..rate_sou_arr.count
            loop
                    add_tmp := rate_sou_add_arr(i);
                    main_tmp := rate_sou_arr(i);
                    -- поисковый ключ
                    is_last_date := false;
                    rateindstr_tmp := add_tmp.tgt_rate_id || '#' || to_char(add_tmp.rate_date, 'ddmmyyyy');
                    if dratehist_ind.exists( rateindstr_tmp)
                    then
                        rate_tmp := dratehist_arr( dratehist_ind_arr ( rateindstr_tmp)).t_rate;  -- значение курса за дату найдено в целевой системе
                    else 
                        -- проверим, не последняя ли это дата
                        if exists dratedef_arr( add_tmp.rate_id ) and ( dratedef_arr( add_tmp.rate_id ).t_sincedate = add_tmp.rate_date )
                        then
                            rate_tmp := dratedef_arr( add_tmp.rate_id ).t_rate;
                            is_last_date := true;
                        else
                            rate_tmp := 0;  -- значение курса за дату отсутствует в целевой системе.
                        end if;
                    end if;

                    case p_action
                    when 1 then -- добавление
                        if rate_tmp > 0 then  -- Ошибка: уже существует данный курс за такую дату
                                -- проверим, конфликтует ли он с существующим
                                if rate_tmp <> rate_sou_arr(i).t_rate
                                then
                                   pr_exclude(418, 70, 'Ошибка: уже существует данный курс за дату %date% по фининструменту %fiid%, тип курса - %type%', i, p_action);
                                   continue;
                                end if;
                                -- Если курс аналогичен старому, не делаем ничего
                        else    -- добавляем в буфер
        
                                if  add_tmp.rate_date > dratedef_arr( add_tmp.rate_id ).t_sincedate
                                then 
                                        -- если вносим самое свежее значение, перекинем текущее из ratedef в историю
                                        begin
                                            INSERT INTO dratehist_dbt(t_rateid, t_isinverse, t_rate, t_scale, t_point, t_inputdate, t_inputtime, t_oper, t_sincedate, t_ismanualinput) 
                                            SELECT t_rateid, t_isinverse, t_rate, t_scale, t_point, t_inputdate, t_inputtime, t_oper, t_sincedate, t_ismanualinput 
                                            FROM dratedef_dbt where t_rateid = add_tmp.rate_id;
                                        exception 
                                            when dup_val_on_index then null;  -- был перенесен раньше с некорректным выходом
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

                                        if not dratedef_arr.exists( add_tmp.rate_id ) -- такого курса вообще не было
                                        then 
                                            dratedef_arr( add_tmp.rate_id ) := dratedef_dbt_seq.nextval;
                                            insert into dratedef_dbt values dratedef_tmp;
                                            --TODO  add_dratedef_buf(dratedef_tmp);  -- запись в базу и в буфер
                                        else
                                            dratedef_tmp.t_rateid := dratedef_arr( add_tmp.rate_id );
                                            update dratedef_dbt set row = dratedef_tmp where t_rateid = dratedef_tmp.t_rateid;
                                        end if;
                                        -- обновляем или заполняем буфер
                                        dratedef_arr(  tmp.rate_id ) := dratedef_tmp;
                                else
                                -- обрабатывать будем только в истории
                                
                                end if;
--------------------------------------------------------
--------------------------------------------------------                                        
                        end if;
                        
                    when 2 then
                        if rateind_tmp = 0 then                    
                            -- ошибка, значение изменяемого курса не найдено в целевой системе
                        end if;                            
                    when 3 then 
                        if rateind_tmp = 0 then                    
                            -- ошибка, значение удаляемого курса не найдено в целевой системе
                        end if;                            
                    end case;
                    
            end loop;


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