CREATE OR REPLACE PACKAGE BODY GEB_20210823_TST.load_rss
is


    tmp_arr             tmp_arr_type;
    tmp_arr1            tmp_arr_type;
    ddp_dep_dbt_cache   tmp_arr_type;
    --tmp_reverse_arr tmp_reverse_arr_type;
    list_of_stocks_arr   tmp_arr_type; -- список бирж.
    list_of_brokers_arr  tmp_arr_type;
    list_of_contrs_arr   tmp_arr_type;
    dpartyown_arr        dpartyown_arr_type;
    p_emergency_limit    number := 40000; -- ограничение по количеству записей, отбираемых BULK COLLECT
    g_fictfi             number; -- код fiid, соответствующий в целевой системе корзине бумаг
    deb_flag             number := 0; -- для процедуры логирования. Уровень вложенности записей логов, для отсутпов


    write_query_log_start date; -- переменная для процедуры write_query_log

    -- коды стран из таблицы dcountry_dbt. Индекс - t_codenum3, значение - t_codelat3
    type country_arr_type is table of varchar2(3) index by varchar2(3);
    country_arr  country_arr_type;

    -- коды iso валют
    type currency_iso_arr_type is table of varchar2(5) index by pls_integer;
    currency_iso_arr    currency_iso_arr_type;


    ---- для оценки производительности -----------------------------------------
    procedure_start_date timestamp;
    procedure_exec_interval interval day to second;
    tmp_date_perf date;
    tmp_num_perf number;
    deb_start_timestamp timestamp;


    -- сбрасывает счетчик времени. Отсчет начинается с момента запуска этой процедуры 
    -- Полезна, если надо пропустить какие-то запросы, пото замерить отдельный запрос.
    procedure WRITE_LOG_START
    is 
    begin
        write_query_log_start := sysdate;
    end WRITE_LOG_START;

    -- Сохраняет текст в лог производительности, потом сбрасывает счетчик времени. 
    -- Если интересующие нас запросы идут один за другим, можно не использовать отдельную процедуру WRITE_LOG_START 
    procedure WRITE_LOG_FINISH(L_TEXT varchar2, L_OBJECTYPE number, L_SET number := 0, L_NUM number := 0)
    is
    pragma autonomous_transaction;
    l_cou number;
    begin
        l_cou := sql%rowcount;
        insert into dtx_querylog_dbt(T_STARTTIME, T_DURATION, T_TEXT, T_OBJECTYPE, T_SET, T_NUM, T_SESSION, T_SESSDETAIL, T_EXECROWS)
        values (write_query_log_start, round((sysdate-write_query_log_start)*24*60*60), L_TEXT, L_OBJECTYPE, L_SET, L_NUM, g_SESSION_ID, g_SESS_DETAIL_ID, l_cou);
        commit;
        write_query_log_start := sysdate;
    end WRITE_LOG_FINISH;    

    


    -- получение кода подразделения по ID. Список загружается при инициализации пакета. В рамках задачи список статичен.
    function get_dep_code( p_partyid number) return number
    is
    begin
        if ddp_dep_dbt_cache.exists( p_partyid )
        then
            return ddp_dep_dbt_cache( p_partyid );
        else
            return -1;
        end if;
    end get_dep_code;

    -- Проверяет наличие типа у субъекта. Если нет - добавляет. Субъект будет добавлен в соответствующий типу список
    -- и в коллекцию dpartyown_arr. Коллекция будет записана в базу по окончании обработки процедурой upload_subject_types.
    function add_type_to_subject( p_partyid number, p_type varchar2 ) return boolean
    is
    begin
        case upper(trim(p_type))
        when 'БИРЖА'  then
                                        if not list_of_stocks_arr.exists( p_partyid ) then
                                            list_of_stocks_arr( p_partyid ) := 1;
                                        else
                                            return true;
                                        end if;
        when 'БРОКЕР'       then
                                        if not list_of_brokers_arr.exists( p_partyid ) then
                                            list_of_brokers_arr( p_partyid ) := 1;
                                        else
                                            return true;
                                        end if;
        when 'КОНТРАГЕНТ'        then
                                        if not list_of_contrs_arr.exists( p_partyid ) then
                                            list_of_contrs_arr( p_partyid ) := 1;
                                        else
                                            return true;
                                        end if;
        else null;
        end case;

        dpartyown_arr( dpartyown_arr.count ).T_PARTYID := p_partyid;
        dpartyown_arr( dpartyown_arr.count ).t_partykind := p_type;

    end add_type_to_subject;

    -- аналогично функции add_type_to_subject, не возвращает значений
    procedure add_type_to_subject( p_partyid number, p_type varchar2 )
    is
        nop boolean;
    begin
        nop := add_type_to_subject( p_partyid, p_type );
    end add_type_to_subject;

    -- загружаем в конце сеанса новые типы субъектов
    -- они должны быть заданы в коллекции dpartyown_arr со структурой таблицы dpartyown_dbt
    procedure   upload_subject_types
    is
    begin
        deb('Запущена процедура UPLOAD_SUBJECT_TYPES');
        begin
            forall i in indices of dpartyown_arr SAVE EXCEPTIONS
                    insert into dpartyown_dbt(T_PARTYID, T_PARTYKIND, T_SUPERIOR, T_SUBKIND)
                    values( dpartyown_arr(i).T_PARTYID, dpartyown_arr(i).T_PARTYKIND, dpartyown_arr(i).T_SUPERIOR, dpartyown_arr(i).T_SUBKIND);
            commit;
        exception when others
        then 
            for i in 1..SQL%BULK_EXCEPTIONS.COUNT
            loop
                deb('Ошибка #3 добавления субъекту #1 типа #2', dpartyown_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).t_partyid, dpartyown_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).T_PARTYKIND, SQL%BULK_EXCEPTIONS(i).ERROR_CODE, p_level => 5);
            end loop;
        end;
        deb('Выполнена вставка в DPARTYOWN_DBT, количество ошибок - #1', SQL%BULK_EXCEPTIONS.COUNT);
        deb('Завершена процедура UPLOAD_SUBJECT_TYPES');
    end  upload_subject_types;




    procedure deb_empty(p_line char := null) -- пустая строка в поток отладочных комментариев
    is
    begin
        -- для производительности
        -- лимит важности в спецификации пакета. Если переданное значение выше, сообщение не записывается.
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


    -- проверяем наличие переданного объекта в коллекции. Если его нет, дописываем. Ключ комплексный, поисковый.
    -- если передается p_destid, процедура просто корректирует это значение в буфере, если оно есть. Это редкий вариант примерения
    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number default 0, p_comment varchar2 := null, p_destid number := -1 )
    is
        v_searchstr varchar2(200);
        mas_idx pls_integer;
    begin
        -- формируем поисковую строку, ищем в в индексном массиве. Если элемент не найден, заводим его в основной массив
        -- потом прописываем в индексном. Если процедура в режиме обновления DEST_ID - в случае, если был заведен новый курс в DRATEDEF_DBT
        -- и мы не хотим, чтобы следующая порция начитанных из источника данный стала тащить старый DEST_ID отсюда - обновим во втором IF
        -- Данные из этих массивов не стираются на новых порциях из источника, только добавляются новые. Возможно, потом это изменю.
        v_searchstr := to_char(p_objtype) || '#' || to_char(p_obj_id) || '#' || to_char(p_obj_sub_id);
        deb( '>Процедура replobj_add, добавлен объект: objtype = #1, id = #2, sub_id = #3', p_objtype, p_obj_id, p_obj_sub_id, p_level=>4);
        if not replobj_rec_inx_arr.exists(v_searchstr) then
            mas_idx := nvl(replobj_rec_arr.last,-1)+1;  -- count нельзя, '-1'й элемент потом добавится
            replobj_rec_arr(mas_idx).obj_type   := p_objtype;
            replobj_rec_arr(mas_idx).obj_id     := p_obj_id;
            if p_objtype in (c_OBJTYPE_SECTION, c_OBJTYPE_RATE)
            then
                replobj_rec_arr(mas_idx).obj_sub_id := p_obj_sub_id;
            else
                replobj_rec_arr(mas_idx).obj_sub_id := 0;
            end if;
            replobj_rec_arr(mas_idx).comment    := p_comment;
            replobj_rec_arr(mas_idx).dest_id    := -1;
            -- запись в индексную коллекцию
            replobj_rec_inx_arr( v_searchstr )  := mas_idx;
        end if;

        if  p_destid <> -1
        then
            deb( 'Коррекция в буфере replobj: для элемента ' || v_searchstr || ' значение dest_id установлено из #1 в #2',
                replobj_rec_arr(  replobj_rec_inx_arr( v_searchstr )  ).dest_id, p_destid, p_level => 4 );
            replobj_rec_arr(  replobj_rec_inx_arr( v_searchstr )  ).dest_id := p_destid;
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
        v_counter pls_integer;
        v_counter_str varchar2(100);
    begin
        deb_empty;
        deb( 'Запущена процедура  replobj_load');
        deb( 'Количество записей в буфере replobj: ' ||load_rss.replobj_rec_arr.count() );

        if g_debug_level_current > 3 then
            deb( '>  Количество записей в буфере replobj ПЕРЕД загрузкой: ' ||load_rss.replobj_rec_arr.count() );
            v_counter := load_rss.replobj_rec_arr.first;
            while (v_counter is not null)
            loop
                deb( '> Номер - тип - object_id - sub_obj_id - destid:      ' || v_counter || '\t\t' || load_rss.replobj_rec_arr(v_counter).obj_type || '\t\t' || rpad(load_rss.replobj_rec_arr(v_counter).obj_id, 20, ' ') || '\t\t' || rpad(load_rss.replobj_rec_arr(v_counter).obj_sub_id, 20, ' ') || '\t\t' || load_rss.replobj_rec_arr(v_counter).dest_id, p_level=>4);
                v_counter := replobj_rec_arr.next(v_counter);
            end loop;
            deb( '>  Количество записей в поисковом индексе replobj ПЕРЕД загрузкой: ' ||load_rss.replobj_rec_inx_arr.count() );
            v_counter_str := load_rss.replobj_rec_inx_arr.first;
            while (v_counter_str is not null)
            loop
                deb( '> Индекс - значение:      ' || v_counter_str || '\t\t' || load_rss.replobj_rec_inx_arr(v_counter_str));
                v_counter_str := replobj_rec_inx_arr.next(v_counter_str);
            end loop;
        end if;


        --deb( 'Количество записей в буфере replobj_tmp_arr: ' ||load_rss.replobj_tmp_arr.count() );
        --#query
        SELECT /*+ use_hash(ro inp) */ ro.* BULK COLLECT INTO replobj_tmp_arr
        FROM dtxreplobj_dbt ro, (select * from table(replobj_rec_arr)) inp where t_objecttype = inp.obj_type and t_objectid = inp.obj_id and (ro.t_subobjnum = obj_sub_id or obj_sub_id = 0) and t_objstate != 2 and inp.dest_id=-1 and inp.obj_id>0;

        deb( 'Загружено #1 записей', SQL%ROWCOUNT);
        IF SQL%ROWCOUNT = emerg_limit THEN
            null;
            -- слишком много записей, сформировать исключение
        END IF;
        -- переберем элементы и обогатим кэш статусами и replid
        FOR i in 1..replobj_tmp_arr.count
        LOOP
            -- определяем индекс элемента в основном массиве
            if replobj_tmp_arr(i).t_objecttype in (c_OBJTYPE_SECTION, c_OBJTYPE_RATE)
            then
                v_search_str := to_char(replobj_tmp_arr(i).t_objecttype) || '#' || to_char(replobj_tmp_arr(i).t_objectid) || '#' || nvl(to_char(replobj_tmp_arr(i).t_subobjnum),'0');
            else
                v_search_str := to_char(replobj_tmp_arr(i).t_objecttype) || '#' || to_char(replobj_tmp_arr(i).t_objectid) || '#' || '0';
            end if;
            v_search_idx := replobj_rec_inx_arr(v_search_str);  -- нашли индекс записи в основном массиве
            deb( '>  Индекс_в_массиве - Поисковая_строка - Значение_t_destid: \t\t' ||v_search_idx || '\t\t' || rpad(v_search_str, 20, ' ') || '\t\t' || replobj_tmp_arr(i).T_DESTID, p_level=>4);
            -- адресуем элемент
            replobj_rec_arr(v_search_idx).dest_id := replobj_tmp_arr(i).T_DESTID;
            replobj_rec_arr(v_search_idx).state := replobj_tmp_arr(i).t_objstate;
        END LOOP;

        -- рубли как валюта не реплиуируются, бесполезно искать их в DREPLOBJ_DBT. Добавляем вручную.
        IF  replobj_rec_inx_arr.exists('10#0#0')
        THEN
            replobj_rec_arr( replobj_rec_inx_arr('10#0#0') ).dest_id := 0;
            replobj_rec_arr( replobj_rec_inx_arr('10#0#0') ).state   := 0;
        END IF;


        -- добавим элемент, который будем использовать, как NULL
        replobj_rec_arr(-1).obj_type     := 0;
        replobj_rec_arr(-1).obj_id      := -1;
        replobj_rec_arr(-1).obj_sub_id  := 0;
        replobj_rec_arr(-1).dest_id     := -1;
        replobj_rec_arr(-1).state       := 0;

        if g_debug_level_current > 3 then
            deb( '>  Количество записей в буфере replobj ПОСЛЕ загрузки: ' ||load_rss.replobj_rec_arr.count() );
            v_counter := load_rss.replobj_rec_arr.first;
            while (v_counter is not null)
            loop
                deb( '> Номер - тип - object_id - destid:      ' || v_counter || '\t\t' || load_rss.replobj_rec_arr(v_counter).obj_type || '\t\t' || rpad(load_rss.replobj_rec_arr(v_counter).obj_id, 20, ' ') || '\t\t' || load_rss.replobj_rec_arr(v_counter).dest_id, p_level=>4);
                v_counter := replobj_rec_arr.next(v_counter);
            end loop;
            deb( '>  Количество записей в поисковом индексе replobj ПОСЛЕ загрузки: ' ||load_rss.replobj_rec_inx_arr.count() );
            v_counter_str := load_rss.replobj_rec_inx_arr.first;
            while (v_counter_str is not null)
            loop
                deb( '> Индекс - значение:      ' || v_counter_str || '\t\t' || load_rss.replobj_rec_inx_arr(v_counter_str), p_level=>4);
                v_counter_str := replobj_rec_inx_arr.next(v_counter_str);
            end loop;
        end if;

        deb('Завершена процедура REPLOBJ_LOAD');
    end replobj_load;



    -- вывод отладочной информации
    -- Уровень 0 показывает только запуск основных процедур и обобщенную статистику
    -- Уровень 1 показывает статистику по процедурам
    -- Уровень 2 показывает содержимое буфера REPLOBJ
    -- Уровень 3 показывает общий результат по каждой строке
    -- Уровень 5 показывает детализацию по каждой строке
    procedure deb( p_text varchar2, num1 number default null, num2 number default null, num3 number default null, p_level pls_integer := 1)
    is
        l_id number;
        l_text varchar2(1000) := p_text;
        l_delim varchar2(50) := ''; --trim(substr(dbms_utility.format_call_stack, 99,10));
        dur_min number;
        dur_sec number;
        dur_interval interval day to second;
    begin
        -- для производительности
        -- лимит важности в спецификации пакета. Если переданное значение выше, сообщение не записывается.
        if not (g_debug_output or g_debug_table) or (p_level > g_debug_level_current)
        then return;
        end if;

        if upper(p_text) like 'ЗАВЕРШЕНА%ПРОЦЕДУРА%'
        then
            deb_flag := deb_flag - 1;
            if deb_flag < 1
            then
                deb_flag := 0;
            end if;
        end if;

        l_text := lpad(' ', 2 * deb_flag + 1, '\t') || l_text; -- добавим отступы согласно уровню

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
            l_delim := l_delim || ' >>'; --' >>>>>';
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

        if upper(p_text) like 'ЗАПУЩЕНА%ПРОЦЕДУРА%'
        then
            deb_flag := deb_flag + 1;
            deb_start_timestamp := current_timestamp;
        end if;
    end deb;

    -- параметры отладочного вывода
    procedure deb_initialize(p_output boolean, p_table boolean)
    is
    begin
        g_debug_output := p_output;
        g_debug_table  := p_table;
    end deb_initialize;




    -------------------------------------------------
    procedure load_rates(p_date date, p_action number)
    is
        -- Алгоритм следующий - считываем данные по n записей, собираем id всех связанных сущностей и загружаем их массово.
        -- затем из целевой системы загружаем описания и историю курсов, соответствующие обрабатываемым записям.
        -- Дальше в соответствии с заданным action и наличием/отсутствием курса в целевой системе формируем буфер записей.
        -- Для dratedef_dbt записи массово будут только апдейтиться. Если что-то не подпадает под операцию апдейта, будет рассматриваться отдельно, это редкий случай.
        -- Для dratehist_dbt создается два буфера - на уставку и удаление. Апдейт моделируется операциями вставки/удаления, поскольку редкий.
        -- В самом обычном режиме обновляется последнее значение курса. Проводится вставка в ratehist и апдейт dratedef_dbt
        cursor m_cur(pp_date date, pp_action number) is select * from DTXCOURSE_DBT where t_instancedate >= pp_date and t_instancedate < (pp_date+1) and t_replstate=0 and t_action = pp_action order by t_instancedate, t_action, t_basefiid, t_marketsectorid, t_type;

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
        -- кэш индексированных номиналов
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
        rate_tmp number; -- есть ли уже в системе курс на заданную дату
        is_last_date boolean;  -- курс за эту дату лежит в dratedef_dbt, то есть дата самая свежая
        dratedef_tmp dratedef_dbt%rowtype;
        dratehist_tmp dratehist_dbt%rowtype;
        ---------------------------------------------------------------------------------------------------------------------------------------------

        l_counter   number := 0;
        l_err_counter   number := 0;
        
        --replobj_rec_arr             replobj_rec_arr_type;
        --replobj_rec_inx_arr         replobj_rec_inx_arr_type; -- индексная коллекция
        --replobj_tmp_arr             replobj_tmp_arr_type;


        -- выявлены проблемы с записью. Логируем ошибку и исключаем запись из обработки.
        procedure pr_exclude(p_code number, p_objtype number, p_id number, p_subnum number := 0, p_text varchar2, p_counter number, p_action number, p_silent boolean := false)
        is
            text_corr varchar2(1000);
            v_row DTXCOURSE_DBT%ROWTYPE;
        begin
            deb('Запущена процедура  pr_exclude');
            v_row := rate_sou_arr(p_counter);            
            text_corr := replace(p_text, '%act%',  (case p_action when 1 then 'Вставка' when 2 then 'изменение' when 3 then 'удаление' end) );
            text_corr := replace(text_corr, '%fiid%', v_row.t_fiid);
            text_corr := replace(text_corr, '%basefiid%', v_row.t_basefiid);
            text_corr := replace(text_corr, '%type%', v_row.t_type);
            text_corr := replace(text_corr, '%date%', to_char(v_row.t_ratedate,'dd.mm.yyyy'));

            deb(text_corr, p_level=>5);
            -- потом заменить на add_log_deferred
            if not p_silent
            then
                add_log( p_code, p_objtype, p_id, p_subnum, text_corr, p_date);
            end if;

            -- исключаем элемент
            rate_sou_add_arr(p_counter).result := 2;
            l_err_counter := l_err_counter + 1;
            deb('Завершена процедура  pr_exclude');
        end pr_exclude;

        -- запись обработана успешно.
        procedure pr_include( p_counter number)
        is
        begin
            deb('Запись обработана успешно! Процедура pr_include для записи номер #1', p_counter, p_level => 5);

            rate_sou_add_arr(p_counter).result := 1;
            l_counter := l_counter + 1;

        end pr_include;



        -- перекодировка типов курсов
        function RTYPE( p_tp number ) return number
        is
            v_rez number;
        begin
            deb('Запущена функция RTYPE');
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
                else v_rez := -p_tp;
            end case;
            return v_rez;
        end;


        -- добавление записи в буфер dratedef
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
        --==================================================================================================
        procedure execute_rate( i pls_integer)
        is
        begin
                    deb_empty('=');                    
                    deb('Запущена процедура  EXECUTE_RATE');
                    deb('! Реплицируется запись, котировка/курс. BASEFIID=#1, НОМИНАЛ=#2, ДАТА=' || to_char(add_tmp.rate_date,'DD.MM.YYYY'), add_tmp.base_fi, main_tmp.t_rate, p_level => 5);

                    -- поисковый ключ

                    is_last_date := false;
                    rateindstr_tmp := add_tmp.tgt_rate_id || '#' || to_char(add_tmp.rate_date, 'ddmmyyyy');
                    if dratehist_ind_arr.exists( rateindstr_tmp)
                    then
                        rate_tmp := dratehist_arr( dratehist_ind_arr ( rateindstr_tmp)).t_rate;  -- значение курса за дату найдено в целевой системе
                    else
                        -- проверим, не последняя ли это дата
                        if  dratedef_arr.exists( add_tmp.tgt_rate_id ) and ( dratedef_arr( add_tmp.tgt_rate_id ).t_sincedate = add_tmp.rate_date )
                        then
                            rate_tmp := dratedef_arr( add_tmp.tgt_rate_id ).t_rate;
                            is_last_date := true;
                        else
                            rate_tmp := 0;  -- значение курса за дату отсутствует в целевой системе.
                        end if;
                    end if;

                    case p_action
                    when 1 then -- добавление
                        deb('Цикл 3 - ветка добавления записи', p_level => 5);
                        if rate_tmp > 0 then  -- Ошибка: уже существует данный курс за такую дату
                                deb('Цикл 3 - уже существует курс за дату', p_level => 5);
                                -- проверим, конфликтует ли он с существующим
                                if rate_tmp <> rate_sou_arr(i).t_rate
                                then
                                   pr_exclude(418, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: уже существует данный курс за дату %date% по фининструменту %basefiid%, тип курса - %type%', i, p_action);
                                   --continue;
                                end if;
                                -- Если курс аналогичен старому, не делаем ничего
                        else    -- значение курса за дату еще нет (или курса нет вообще), обычная вставка
                                deb('Цикл 3 - курса за дату еще нет', p_level => 5);
                                if  not dratedef_arr.exists(add_tmp.tgt_rate_id) or add_tmp.rate_date > dratedef_arr( add_tmp.tgt_rate_id ).t_sincedate
                                then
                                        -- если вносим самое свежее значение, надо корректировать dratedef
                                        deb('Цикл 3 - вносим самое свежее значение курса', p_level => 5);
                                        dratedef_tmp.T_RATE := main_tmp.t_rate * power(10, main_tmp.t_point);
                                        dratedef_tmp.T_SINCEDATE := main_tmp.t_ratedate;
                                        dratedef_tmp.T_SCALE := main_tmp.t_scale;
                                        dratedef_tmp.T_POINT := main_tmp.t_point;

                                        if (add_tmp.tgt_rate_id = 0 ) or not dratedef_arr.exists( add_tmp.tgt_rate_id ) -- такого курса вообще не было
                                        then
                                            deb('Цикл 3 - курса еще нет, создает запись в dratedef_dbt', p_level => 5);
                                            dratedef_tmp.t_rateid := dratedef_dbt_seq.nextval;

                                            -- дозаполним остальные поля записи
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
                                            --TODO  add_dratedef_buf(dratedef_tmp);  -- запись в базу и в буфер
                                            --commit;

                                            delete from DTXREPLOBJ_DBT where T_OBJECTTYPE=c_OBJTYPE_RATE and t_objectid = main_tmp.t_courseid;
                                            insert into DTXREPLOBJ_DBT (T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE) values(c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, dratedef_tmp.t_rateid, 1, 0 );
                                            pr_include(i);

                                            -- теперь поправим 3 буфера - replobj, rate_sou_add_arr и dratedef_arr
                                            -- вносим текущую запись
                                            deb('Цикл 3 - добавляем запись о новом курсе в буферы', p_level => 5);
                                            dratedef_arr( dratedef_tmp.t_rateid ) := dratedef_tmp;
                                            -- Среди следующих записей могут быть те, что относятся к этому же курсу
                                            for j in i..rate_sou_arr.count  -- от текущей записи до конца
                                            loop
                                                if rate_sou_arr(j).t_courseid=main_tmp.t_courseid and rate_sou_arr(j).t_type=main_tmp.t_type
                                                then
                                                    rate_sou_add_arr(j).tgt_rate_id := dratedef_tmp.t_rateid;
                                                end if;
                                            end loop;
                                            replobj_add( c_OBJTYPE_RATE, rate_sou_arr(i).T_COURSEID, rate_sou_arr(i).T_TYPE, p_destid => dratedef_tmp.t_rateid);
                                            add_tmp.tgt_rate_id := dratedef_tmp.t_rateid;

                                        else
                                            -- переносим из dratedef_dbt значение в dratehist_dbt
                                            deb('Цикл 3 - переносим запись из dratedef_dbt в dratehist_dbt', p_level => 5);
                                            begin
                                                INSERT INTO dratehist_dbt(t_rateid, t_isinverse, t_rate, t_scale, t_point, t_inputdate, t_inputtime, t_oper, t_sincedate, t_ismanualinput)
                                                SELECT t_rateid, chr(0),  t_rate , t_scale, t_point, t_inputdate, t_inputtime, t_oper, t_sincedate, chr(0)
                                                FROM dratedef_dbt where t_rateid = add_tmp.tgt_rate_id;
                                                deb('Цикл 3 - Перенесено #1 строк', SQL%ROWCOUNT, p_level => 5);

                                            exception
                                                when dup_val_on_index then null;  -- был перенесен раньше с некорректным выходом
                                            end;

                                            dratedef_tmp.t_rateid := add_tmp.tgt_rate_id;
                                            update dratedef_dbt set T_RATE = dratedef_tmp.T_RATE, T_SINCEDATE = dratedef_tmp.T_SINCEDATE, T_SCALE = dratedef_tmp.T_SCALE, T_POINT = dratedef_tmp.T_POINT where t_rateid = add_tmp.tgt_rate_id ;
                                            pr_include(i);
                                        end if;
                                        -- обновляем или заполняем буфер
                                        dratedef_arr(  add_tmp.tgt_rate_id ) := dratedef_tmp;
                                else  -- не самый свежий курс
                                    deb('Цикл 3 - обрабатывается исторический курс', p_level => 5);
                                      -- обрабатывать будем только в истории
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
                                    -- добавим в буфер на случай дубля.
                                    dratehist_ind_arr( rateindstr_tmp ) := dratehist_arr.count;
                                    dratehist_arr(dratehist_arr.count)  := dratehist_tmp;
                                    pr_include(i);
                                end if;

                        end if;


                    when 2 then
                        deb('Цикл 3 - ветка обновления записи', p_level => 5);
                        if rate_tmp = 0 then
                            -- ошибка, значение изменяемого курса не найдено в целевой системе
                            deb('Цикл 3 - ошибка - значение изменяемого курса не найдено в целевой системе', p_level => 5);
                            pr_exclude(419, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: невозможно обновить несуществующий курс за дату %date% по фининструменту %fiid%, тип курса - %type%', i, p_action);

                        elsif  add_tmp.tgt_state = 1 then
                            -- объект в режиме ручного изменения
                            deb('Цикл 3 - ошибка - объект в режиме ручного изменения', p_level => 5);
                            pr_exclude(205, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: объект находится в режиме ручного редактирования, курс за дату %date% по фининструменту %fiid%, тип курса - %type%', i, p_action);

                        -- если обновляется последнее значение курса
                        elsif is_last_date
                        then
                            deb('Цикл 3 - обновляется последнее значение курса', p_level => 5);
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
                            deb('Цикл 3 - обновляется историческое значение курса', p_level => 5);
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
                        deb('Цикл 3 - ветка удаления записи', p_level => 5);
                        if rate_tmp = 0 then
                            deb('Цикл 3 - ошибка - значение удаляемого курса не найдено в целевой системе', p_level => 5);
                            -- ошибка, значение изменяемого курса не найдено в целевой системе
                            pr_exclude(420, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: невозможно удалить несуществующий курс за дату %date% по фининструменту %fiid%, тип курса - %type%', i, p_action);
                        else

                            delete from dratehist_dbt where t_rateid = add_tmp.tgt_rate_id and t_sincedate = main_tmp.t_ratedate;
                            pr_include(i);
                        end if;
                    end case;
            deb('Завершена процедура  EXECUTE_RATE');
            procedure_exec_interval := systimestamp - procedure_start_date;
            deb('Время выполнения процедуры: #1:#2', extract(minute from procedure_exec_interval), extract(second from procedure_exec_interval));
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
            deb('Запущена процедура  EXECUTE_NOMINAL');
            deb('! Реплицируется запись, индексируемый номинал. BASEFIID=#1, НОМИНАЛ=#2, ДАТА=' || to_char(add_tmp.rate_date,'DD.MM.YYYY'), add_tmp.base_fi, main_tmp.t_rate, p_level => 5);
            if  ind_nominal_arr.exists( add_tmp.base_fi )  and  ind_nominal_arr( add_tmp.base_fi ).T_BEGDATE = add_tmp.rate_date
            then
                l_initnom := ind_nominal_arr( add_tmp.base_fi ).T_FACEVALUE;
                deb('Из буфера получено значение начального номинала за целевую дату - #1', l_initnom, p_level => 5);
            end if;

            if p_action = 2 or p_action = 3
            then
                ----------------------------------------------------------------------
                if l_initnom > 0
                then
                    deb('Ошибка: изменение начального номинала бумаги необходимо выполнять при репликации из DTXAVOIRISS_DBT', p_level => 5);
                    pr_exclude(525, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: изменение начального номинала бумаги необходимо выполнять при репликации из DTXAVOIRISS_DBT', i, p_action);
                else
                    ----------------------------------------------------------------------
                    l_destid := replobj_get( c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type).DEST_ID;
                    if  l_destid = -1  -- не нашли номинал в репликации
                    then
                        deb('Ошибка: изменяемый/удаляемый номинал не реплицирован в целевую систему', p_level => 5);
                        pr_exclude(525, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: изменяемый/удаляемый номинал не реплицирован в целевую систему', i, p_action);
                    else
                        ----------------------------------------------------------------------
                        select count(*) into l_flag from DFIVLHIST_DBT where t_id = l_destid;
                        if l_flag = 0   -- не нашли номинал в целевой таблице
                        then
                            deb('Ошибка: изменяемый/удаляемый номинал отсутствует в системе', p_level => 5);
                            pr_exclude(525, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: изменяемый/удаляемый номинал отсутствует в системе', i, p_action);
                        else
                            --=======================================================================================
                            if p_action = 2
                            then
                                    if  add_tmp.tgt_state = 1
                                    then
                                        -- объект в режиме ручного изменения
                                        deb('Цикл 3 - ошибка - объект в режиме ручного изменения', p_level => 5);
                                        pr_exclude(205, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: объект находится в режиме ручного редактирования, курс за дату %date% по фининструменту %fiid%, тип курса - %type%', i, p_action);
                                    else
                                        update DFIVLHIST_DBT set T_FIID = add_tmp.fi, T_VALKIND = 1/*изменение номинала*/, T_ENDDATE = main_tmp.t_ratedate, T_VALUE = main_tmp.t_rate, T_INTVALUE = 0 where t_ID = l_destid;
                                        pr_include(i);
                                        deb('Цикл 3 - Объект успешно изменен', p_level => 5);
                                    end if;
                            elsif p_action = 3
                            then
                                    deb('Цикл 3 - удаление объекта из DFIVLHIST_DBT (#1)', l_destid, p_level => 5);
                                    delete from DFIVLHIST_DBT where t_ID = l_destid;
                                    pr_include(i);
                                    deb('Цикл 3 - Объект успешно удален', p_level => 5);
                            end if;
                        end if;
                    end if;

                end if;
            else  -- p_action == 1

                if l_initnom = 0 -- в буфере не было начального номинала за дату
                then
                    begin
                        select t_value into l_nom_for_date from DFIVLHIST_DBT where t_ValKind = 1/*изменение номинала*/ and T_FIID = add_tmp.fi and t_EndDate = main_tmp.t_ratedate;
                        deb('Цикл 3 - номинал найден в DFIVLHIST_DBT: значение #1, fiid=#2', l_nom_for_date, add_tmp.fi, p_level => 5);
                    exception
                        when no_data_found
                        then l_nom_for_date := 0;
                    end;
                end if;

                if l_initnom + l_nom_for_date > 0
                then
                    deb('Цикл 3 - добавляемый номинал (значение #1) уже есть в системе (значение #2)', main_tmp.t_rate, (l_initnom + l_nom_for_date), p_level => 5);
                    if (l_initnom + l_nom_for_date) = main_tmp.t_rate
                    then -- Если номинал тот же, что в системе, в лог не пишем
                        pr_exclude(418, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: уже существует номинал за дату %date% по фининструменту %fiid%, тип курса - %type%', i, p_action, p_silent => true);
                    else
                        pr_exclude(418, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: уже существует номинал за дату %date% по фининструменту %fiid%, тип курса - %type%', i, p_action, p_silent => false);
                    end if;
                else

                    select DFIVLHIST_DBT_seq.nextval into l_id FROM dual;
                    deb('Цикл 3 - получили ID новой записи в DFIVLHIST_DBT (#1)', l_id, p_level => 5);

                    insert into DFIVLHIST_DBT (T_ID, T_FIID, T_VALKIND, T_ENDDATE, T_VALUE, T_INTVALUE)
                    values( l_id, add_tmp.base_fi, 1, main_tmp.t_ratedate, main_tmp.t_rate, 0);


                    delete from DTXREPLOBJ_DBT where T_OBJECTTYPE=c_OBJTYPE_RATE and t_objectid = main_tmp.t_courseid;
                    insert into DTXREPLOBJ_DBT (T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE) values(c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, l_id, 1, 0 );
                    pr_include(i);

                    -- теперь поправим 2 буфера - replobj, rate_sou_add_arr
                    -- вносим текущую запись
                    deb('Цикл 3 - добавляем запись о новом номинале в буферы', p_level => 5);
                    dratedef_arr( dratedef_tmp.t_rateid ) := dratedef_tmp;
                    -- Среди следующих записей могут быть те, что относятся к этому же курсу
                    for j in i..rate_sou_arr.count  -- от текущей записи до конца
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
            deb('Завершена процедура  EXECUTE_NOMINAL');
        end execute_nominal;


--================================================================================================================
--================================================================================================================
--================================================================================================================
--================================================================================================================
    begin
        deb_empty('=');
        l_err_counter := 0;
        l_counter := 0;
        procedure_start_date := systimestamp;
        deb('Запущена процедура  LOAD_RATE за ' || to_char(p_date, 'dd.mm.yyyy') || ', тип действия ' || p_action);

        open m_cur(p_date, p_action);
        loop

            -- загрузка порции данных
            fetch m_cur bulk collect into rate_sou_arr limit g_limit;
            exit when rate_sou_arr.count=0;
            deb('Загружены данные из DTXCOURSE_DBT, #1 строк', m_cur%rowcount);

            -- регистрируем все сущности для загрузки из REPLOBJ
            deb_empty('=');
            deb('Цикл 1 - регистрация кодов в буфере REPLOBJ');
            for i in 1..rate_sou_arr.count
            loop
                -- собираем уникальные fiid
                replobj_add( c_OBJTYPE_MONEY, rate_sou_arr(i).t_fiid, p_comment => 'котируемый фининструмент');
                replobj_add( rate_sou_arr(i).T_BASEFIKIND, rate_sou_arr(i).t_basefiid, p_comment => 'базовый фининструмент');

                -- собираем уникальные MARKETID
                replobj_add( c_OBJTYPE_MARKET, rate_sou_arr(i).T_MARKETID, p_comment => 'торговая площадка');
                replobj_add( c_OBJTYPE_SECTION, rate_sou_arr(i).T_MARKETID, rate_sou_arr(i).T_MARKETSECTORID);

                -- собственно, курс
                replobj_add( c_OBJTYPE_RATE, rate_sou_arr(i).T_COURSEID, rate_sou_arr(i).T_TYPE);
            end loop;
            deb('Собрали данные в буфер REPLOBJ, #1 записей', replobj_rec_arr.count);

            -- заполняем кэш из REPLOBJ
            replobj_load;



            -- перебираем заново, заполняем перекодированными полями дополнительную коллекцию. Логируем отсутствие записей.
            deb_empty;
            deb_empty('=');
            ind_nominal_flag := false;
            deb('Цикл 2 - перекодирование и проверка параметров');
            for i in 1..rate_sou_arr.count
            loop
                main_tmp := rate_sou_arr(i);
                rate_sou_add_arr(i) := add_tmp; -- Для процедуры pr_exclude,она пытается в поле записать код ошибки

                add_tmp.isdominant := case when ( main_tmp.T_BASEFIKIND = 10 and main_tmp.t_type = 6 ) THEN chr(88) else chr(0) end;

                add_tmp.type_id := rtype( main_tmp.t_type );       -- перекодируем тип курса

                if add_tmp.type_id = c_RATE_TYPE_NOMINALONDATE
                then
                    -- Среди курсов встречаются записи об изменениии индексированного номинала, которые надо обрабатывать отдельно
                    ind_nominal_flag := true;
                end if;

                add_tmp.rate_date := main_tmp.t_ratedate;       -- для использования из SQL, тег #R

                add_tmp.tgt_rate_id :=  replobj_get(c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type).dest_id;
                add_tmp.tgt_state :=  replobj_get(c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type).state;

                stat_tmp :=  replobj_get( c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type).state;
                        if  ( add_tmp.tgt_rate_id = -1) and (p_action > 1 ) then
                            pr_exclude(419, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: невозможно %act% несуществующий курс, финансовый инструмент - %basefiid%', i, p_action );
                        elsif ( stat_tmp = 1) and (p_action > 1 ) then
                            pr_exclude(205, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: объект находится в режиме ручного редактирования, финансовый инструмент - %basefiid%', i, p_action);
                        end if;

                add_tmp.fi  :=  replobj_get( c_OBJTYPE_MONEY, main_tmp.t_fiid).dest_id;
                        if  ( add_tmp.market_id = -1) and (p_action < 3 ) then
                            pr_exclude(527, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: невозможно ничего сделать с курсом для несуществующего котируемого финансового инструмента, базовый инструмент - %basefiid%, тип курса - %type%', i, p_action);
                        end if;

                add_tmp.market_id   :=  replobj_get( c_OBJTYPE_MARKET, main_tmp.T_MARKETID).dest_id;
                        if  ( add_tmp.market_id = -1) and (p_action < 3 ) then
                            pr_exclude(525, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: невозможно ничего сделать с курсом для несуществующей торговой площадки, финансовый инструмент - %basefiid%', i, p_action);
                        end if;
                add_tmp.section_id  :=  replobj_get( c_OBJTYPE_SECTION, main_tmp.T_MARKETID, main_tmp.T_MARKETSECTORID).dest_id;
                        if  ( add_tmp.section_id = -1) and (p_action > 1 ) then
                            --pr_exclude(525, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: невозможно ничего сделать с курсом для несуществующей секции торговой площадки, финансовый инструмент - %basefiid%', i, p_action);
                            null;
                        end if;
                add_tmp.base_fi     :=  replobj_get( main_tmp.T_BASEFIKIND, main_tmp.t_basefiid).dest_id;
                        if  ( add_tmp.base_fi = -1) and (p_action > 1 ) then
                            pr_exclude(528, c_OBJTYPE_RATE, main_tmp.t_courseid, main_tmp.t_type, 'Ошибка: невозможно ничего сделать с курсом для несуществующего базового финансового инструмента, котируемый инструмент - %basefiid%, тип курса - %type%', i, p_action);
                        end if;
                -- если нашли ошибку, ни к чему сохранять
                if  rate_sou_add_arr(i).result = 2
                    then continue;
                end if;

                add_tmp.isrelative := case when ( main_tmp.T_BASEFIKIND = 20 and add_tmp.type_id <> c_RATE_TYPE_NKDONDATE and RSI_RSB_FIInstr.FI_IsAvrKindBond( RSI_RSB_FIInstr.FI_AvrKindsGetRootByFIID( add_tmp.base_fi))) THEN chr(88) else chr(0) end;
                rate_sou_add_arr(i) := add_tmp;

            end loop;

            -- создаем второй буфер - dratedef|dratehist. Скорость доступа к dratedef обеспечивается индексацией по rate_id, к dratehist - поисковой коллекцией.
            -- Пересоздавать коллекцию с varchar ключом будет дольше.
            -- загружаем из целевой системы значение курса (из основной записи) по всем action
            --#query
            deb('Загрузка буфера dratedef_dbt');
            select * bulk collect into dratedef_arr_tmp from dratedef_dbt where t_rateid in ( select tgt_rate_id from table(rate_sou_add_arr) );
            for i in 1..dratedef_arr_tmp.count
            loop
                -- переиндексируем коллекцию по rate_id
                dratedef_arr( dratedef_arr_tmp(i).t_rateid ) := dratedef_arr_tmp(i);
            end loop;
            -- временная больше не нужна
            dratedef_arr_tmp.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;

            -- загружаем буфер из истории. #R
            -- только значения по нужным курсам за нужные даты
            -- здесь комплексный ключ. Можно переиндексировать в коллекцию с varchar индексом, но тут проще сделать новую.
            -- поскольку нас чаще будет интересовать сам факт наличия элемента, а не его атрибуты.
            deb('Загрузка буфера dratehist_dbt');
            select * bulk collect into dratehist_arr from dratehist_dbt where (t_rateid, t_sincedate) in ( select tgt_rate_id, rate_date from table(rate_sou_add_arr) );
            -- TODO добавить emerg_limit
            for i in 1..dratehist_arr.count
            loop
                -- создаем поисковую коллекцию
                -- ключ вида "12345#12082020", {rate_id}#{дата_начала_действия_курса}
                dratehist_ind_arr( to_char(dratehist_arr(i).t_rateid) || '#' || to_char(dratehist_arr(i).t_sincedate, 'ddmmyyyy') ) := i;
            end loop;

            ---------
            -- если были индексируемые номиналы, загрузим буфер начальных номиналов по всем интересующим бумагам
            deb('Загрузка буфера индексированных номиналов');
            if ind_nominal_flag
            then
                --#query
                select * bulk collect into ind_nominal_tmp from dv_fi_facevalue_hist  where t_id=0
                and t_fiid in (select base_fi from table( rate_sou_add_arr ) where type_id = c_RATE_TYPE_NOMINALONDATE );
                for j in 1..ind_nominal_tmp.count
                loop
                    ind_nominal_arr( ind_nominal_tmp(j).t_fiid ).t_fiid := ind_nominal_tmp(j).t_fiid;
                    ind_nominal_arr( ind_nominal_tmp(j).t_fiid ).t_begdate := ind_nominal_tmp(j).t_begdate;
                    ind_nominal_arr( ind_nominal_tmp(j).t_fiid ).t_facevalue := ind_nominal_tmp(j).t_facevalue;
                end loop;
                ind_nominal_tmp.delete;
                DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
            end if;
            ---------------------------------------------------------------------------------------------------------------------------------------------
            --- все данные загружены



            -- еще раз проходим по датасету источника. Проверяем, есть ли курс в таргете, и реагируем
            deb_empty;
            deb_empty('=');
            deb('Цикл 3 - запись курсов/номиналов в таблицы-приемники');
            for i in 1..rate_sou_arr.count
            loop
                add_tmp := rate_sou_add_arr(i);
                main_tmp := rate_sou_arr(i);

                -- здесь отдельный алгоритм для изменяемых номиналов, работает с другими таблицами
                if add_tmp.type_id <> c_RATE_TYPE_NKDONDATE
                then
                    -- полная обработка курса
                    execute_rate(i);
                else
                    -- полная обработка изменяего номинала
                    execute_nominal(i);
                end if;

                deb('Обновляем статус репликации записи на #1', rate_sou_add_arr(i).result, p_level => 4);
                update DTXCOURSE_DBT set t_replstate = rate_sou_add_arr(i).result  where T_COURSEID = main_tmp.t_courseid and t_action = p_action and t_replstate = 0 and t_type = main_tmp.t_type and t_instancedate = main_tmp.t_instancedate;

            end loop;

            commit;
            deb('Обработка блока данных завершена. COMMIT.');
        end loop;
        deb('Завершена процедура load_rate');
    end load_rates;




    -- процедура создает снимок по сделкам из DTXDEAL_DBT в таблицу DTXDEAL_TMP
    procedure deals_create_snapshot( p_startdate date, p_enddate date)
    is 
    begin
        deb('Запущена процедура DEALS_CREATE_SNAPSHOT c ' || to_char(p_startdate, 'dd.mm.yyyy hh24:mi') || ' по '  || to_char(p_enddate, 'dd.mm.yyyy hh24:mi'));
        -- получаем снимок из dtxdeal_dbt в dtxdeal_tmp - только тот набор записей, который необходимо реплицировать. Записываем его в таблицу, имеющею пустые поля для индетификаторов целевой системы.
        deb('Очистка таблицы DTXDEAL_TMP');
        -- таблица очищается в начале, а не в конце, чтобы данные между вызовами процедуры были доступны для анализа

        deb('Очистка таблицы DTXDEAL_TMP и удаление индексов');
        WRITE_LOG_START;
        execute immediate 'truncate table dtxdeal_tmp';
        
            for j in (select index_name from user_indexes where table_name='DTXDEAL_TMP')
            loop
                begin        
                    execute immediate 'DROP INDEX ' || j.index_name;
                exception when others 
                then
                    deb('Ошибка при удалении индекса ' || j.index_name);
                end;
            end loop;
        
        deb('Создаем снимок с ' || to_char(p_startdate,'dd.mm.yyyy') || ' по ' || to_char(p_enddate,'dd.mm.yyyy'));
        -- просто переливаем данные с replstate=0 за нужный instancedate
        execute immediate
            'insert /*+ append */ into dtxdeal_tmp(T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, T_MARKETID, T_SECTOR, T_BROKERID, T_PARTYID, T_DEPARTMENT, T_AVOIRISSID, T_WARRANTID, T_PARTIALID, T_AMOUNT, T_CURRENCYID, T_PRICE, T_POINT, T_COST, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID, T_NETTING_DEALID_DEST)
            select T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, T_MARKETID, T_SECTOR, T_BROKERID, T_PARTYID, T_DEPARTMENT, T_AVOIRISSID, T_WARRANTID, T_PARTIALID, T_AMOUNT, T_CURRENCYID, T_PRICE, T_POINT, T_COST, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID, T_NETTING_DEALID_DEST
            from dtxdeal_dbt where t_instancedate between :1 and :2 and t_replstate=0' using p_startdate, p_enddate;
        deb('Загружено в снимок #1 строк', sql%rowcount);
        
        WRITE_LOG_FINISH('Создаем снимок по сделкам',80);
        commit;
        
        deb('Завершена процедура DEALS_TAKE_SNAPSHOT');
    end deals_create_snapshot;
    
    
    
    -- процедура создает снимок по сделкам из DTXDEMAND_DBT в таблицу DTXDEMAND_TMP
    procedure demands_create_snapshot( p_startdate date, p_enddate date)
    is 
    begin
        deb('Запущена процедура DEMANDS_CREATE_SNAPSHOT c ' || to_char(p_startdate, 'dd.mm.yyyy hh24:mi') || ' по '  || to_char(p_enddate, 'dd.mm.yyyy hh24:mi'));
        -- получаем снимок из dtxdemand_dbt в dtxdemand_tmp - только тот набор записей, который необходимо реплицировать. Записываем его в таблицу, имеющею пустые поля для индетификаторов целевой системы.

        deb('Очистка таблицы DTXDEMAND_TMP и удаление индексов');
        WRITE_LOG_START;
        execute immediate 'truncate table dtxdemand_tmp';
        
            for j in (select index_name from user_indexes where table_name='DTXDEMAND_TMP')
            loop
                begin        
                    execute immediate 'DROP INDEX ' || j.index_name;
                exception when others 
                then
                    deb('Ошибка при удалении индекса ' || j.index_name);
                end;
            end loop;
        
        deb('Создаем снимок с ' || to_char(p_startdate,'dd.mm.yyyy') || ' по ' || to_char(p_enddate,'dd.mm.yyyy'));
        -- просто переливаем данные с replstate=0 за нужный instancedate
        execute immediate
            'insert /*+ append */ into dtxdemand_tmp(T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, T_BALANCERATE, T_NETTING, T_PLANDEMEND, T_NOTE, T_STATE)
            select T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, T_BALANCERATE, T_NETTING, T_PLANDEMEND, T_NOTE, T_STATE
            from dtxdemand_dbt where t_instancedate between :1 and :2 and t_replstate=0' using p_startdate, p_enddate;
        deb('Загружено в снимок #1 строк', sql%rowcount);
        
        WRITE_LOG_FINISH('Создаем снимок по платежам',80);
        commit;
        
        deb('Завершена процедура DEMANDS_CREATE_SNAPSHOT');
    end demands_create_snapshot;    
    
    
    
    
    
    -- процедура записывает ошибки в лог из таблцы ошибок dtx_error_dbt
    -- только те, которые еще не были запсаны
    procedure write_errors_into_log
    is
    begin
        deb('Запущена процедура WRITE_ERRORS_INTO_LOG');
        WRITE_LOG_START;
        insert /*+ append */ into dtxloadlog_dbt(T_MSGTIME, T_MSGCODE, T_SEVERITY, T_OBJTYPE, T_OBJECTID, T_SUBOBJNUM, T_FIELD, T_MESSAGE, T_INSTANCEDATE)
        select er.T_TIMESTAMP, er.T_ERRORCODE, 1, er.T_OBJECTTYPE, er.T_OBJECTID, 0, '', erk.T_DESC,  ss.T_INSTANCEDATE
        from dtx_error_dbt er 
        join DTX_SESS_DETAIL_DBT ss on (er.T_SESSID=ss.T_SESSID and er.T_DETAILID=ss.T_DETAILID and er.T_SESSID=g_session_id and er.T_DETAILID=g_SESS_DETAIL_ID) 
        left join dtx_errorkinds_dbt erk on (er.t_errorcode=erk.t_code)
        where er.is_logged is null; 
        
        -- проставим признак записи на записи в таблице ошибок. Теперь можно повторно вызывать процедуру записи в течение сессии, ошибкии в логе не будут дублироваться
        update dtx_error_dbt set is_logged=chr(88) where rowid in (select rowid from dtx_error_dbt where is_logged is null and t_sessid=G_SESSION_ID and T_DETAILID=g_SESS_DETAIL_ID);
        WRITE_LOG_FINISH('Переносим ошибки в общий лог', 0);
        commit;         
        deb('Завершена процедура WRITE_ERRORS_INTO_LOG');
    end write_errors_into_log;

    -- процедура создает индексы на снимке и считает статистику
    procedure deals_create_indexes
    is 
    begin
        deb('Запущена процедура DEALS_CREATE_INDEXES');
        WRITE_LOG_START;
        deb('Создаем индексы на снимке');
        begin
            execute immediate 'CREATE INDEX I_DEALID ON DTXDEAL_TMP(T_DEALID)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_ACTION ON DTXDEAL_TMP(T_ACTION)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_KIND ON DTXDEAL_TMP(T_KIND)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_REPLSTATE ON DTXDEAL_TMP(T_REPLSTATE)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_ISREPO ON DTXDEAL_TMP(TGT_ISREPO)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_COSTCHANGEONCOMP ON DTXDEAL_TMP(T_COSTCHANGEONCOMP)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_COSTCHANGE ON DTXDEAL_TMP(T_COSTCHANGE)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_COSTCHANGEONAMOR ON DTXDEAL_TMP(T_COSTCHANGEONAMOR)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_ADJUSTMENT ON DTXDEAL_TMP(T_ADJUSTMENT)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_NEEDDEMAND ON DTXDEAL_TMP(T_NEEDDEMAND)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_NEEDDEMAND2 ON DTXDEAL_TMP(T_NEEDDEMAND2)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_ATANYDAY ON DTXDEAL_TMP(T_ATANYDAY)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_LIMIT ON DTXDEAL_TMP(T_LIMIT)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_CHRATE ON DTXDEAL_TMP(T_CHRATE)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_DIV ON DTXDEAL_TMP(T_DIV)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEAL_ISBASKET ON DTXDEAL_TMP(T_ISBASKET)';
        exception when others then
            deb('Ошибка при создании индексов на снимке');
        end;
        
        deb('Считаем статистику на снимке');
        dbms_stats.gather_table_stats(user, 'DTXDEAL_TMP', cascade=>true);
        
        WRITE_LOG_FINISH('Создаем индексы на снимке', 80);    
        
        deb('Завершена процедура DEALS_CREATE_INDEXES');
    end deals_create_indexes;



    -- процедура создает индексы на снимке и считает статистику
    procedure demands_create_indexes
    is 
    begin
        deb('Запущена процедура DEMANDS_CREATE_INDEXES');
        WRITE_LOG_START;
        deb('Создаем индексы на снимке');
        begin
            execute immediate 'CREATE INDEX BTREEI_DTXDEAL_DEMANDID ON DTXDEMAND_TMP(T_DEMANDID)';
            execute immediate 'CREATE INDEX BTREEI_DTXDEAL_DEALID ON DTXDEMAND_TMP(T_DEALID)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEMAND_ACTION ON DTXDEMAND_TMP(T_ACTION)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEMAND_KIND ON DTXDEMAND_TMP(T_KIND)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXDEMAND_REPLSTATE ON DTXDEMAND_TMP(T_REPLSTATE)';
  
        exception when others then
            deb('Ошибка при создании индексов на снимке');
        end;
        
        deb('Считаем статистику на снимке');
        dbms_stats.gather_table_stats(user, 'DTXDEMAND_TMP', cascade=>true);
        
        WRITE_LOG_FINISH('Создаем индексы на снимке', 90);    
        
        deb('Завершена процедура DEMANDS_CREATE_INDEXES');
    end demands_create_indexes;    
      



    procedure run_query_set( p_objecttype number, p_set number )
    is 
    begin
        deb('Запущена процедура RUN_QUERY_SET для OBJECTTYPE=#1 и сета #2', p_objecttype, p_set);
        -- выполняем заданный сет запросов
        for q_rec in (select * from dtx_query_dbt where t_objecttype=p_objecttype and t_set=p_set and t_in_use='X' order by t_num)
        loop
                begin
                    WRITE_LOG_START;
                    if q_rec.t_use_bind = 'X' then
                        execute immediate q_rec.t_text using g_SESSION_ID, g_SESS_DETAIL_ID, q_rec.t_screenid;
                    else
                        execute immediate q_rec.t_text;
                    end if;
                    deb('    ' || q_rec.t_name || '.  #1 записей', sql%rowcount);
                    WRITE_LOG_FINISH( q_rec.T_NAME, q_rec.T_OBJECTTYPE, q_rec.T_SET, q_rec.T_NUM);
                    commit; 
                exception
                when others then
                    rollback;
                    deb('Ошибка в запросе номер #2 в сете #3', q_rec.T_NUM, q_rec.T_SET);
                    WRITE_LOG_FINISH( 'Ошибка: '  || q_rec.T_NAME, q_rec.T_OBJECTTYPE, q_rec.T_SET, q_rec.T_NUM);
                    raise;
                end; 
        end loop;
        commit;
        deb('Завершена процедура RUN_QUERY_SET');
    end run_query_set;



    -- Процедура выполняет общие проверки над всем датасетом
    -- для операции вставки выполняется проверка на уникальность t_dealcode,
    -- для операции изменения/удаления - на наличие t_dealid в системе
    -- поскольку проверка выполняется на урочне sql, имеет смысл вынести    
    procedure run_all_queries( p_objecttype number )
    is 
        l_counter number;
        l_startdate date;
        l_enddate date;
        l_dur_interval interval day to second;
        l_dur_min pls_integer;
        l_dur_sec pls_integer;
        l_perf_start timestamp;
        
    begin
        deb('Запущена процедура RUN_ALL_QUERIES для OBJECTTYPE=#1', p_objecttype);
        l_perf_start := current_timestamp;
                
        deb('Первый сет запросов. Приводим поля снимка к нужному формату');
        
        run_query_set(p_objecttype, 1);
       

        deb('Второй сет запросов. Заполнение таблицы ошибок');
        -- Второй сет запросов, проверяем заполнение полей, определяем ошибки
       
        run_query_set(p_objecttype, 2);

        if p_objecttype=80 
        then
            deals_create_indexes;
        elsif p_objecttype=90 
        then
            demands_create_indexes;
        end if;
        
        -- отметим найденные ошибки. 
        WRITE_LOG_START;
        case p_objecttype
        when 80 then
            execute immediate
            'update dtxdeal_tmp set t_replstate=2 where t_dealid in (select t_objectid from dtx_error_dbt where t_objecttype=:1 and t_sessid=:2 and t_detailid=:3)' using p_objecttype, g_SESSION_ID, g_SESS_DETAIL_ID;
            --insert into dtxloadlog_dbt(T_MSGTIME, T_MSGCODE, T_SEVERITY, T_OBJTYPE, T_OBJECTID, T_SUBOBJNUM, T_FIELD, T_MESSAGE, T_CORRECTION, T_CORRUSER, T_CORRTIME, T_INSTANCEDATE)
        when 90 then
        execute immediate
            'update dtxdemand_tmp set t_replstate=2 where t_demandid in (select t_objectid from dtx_error_dbt where t_objecttype=:1 and t_sessid=:2 and t_detailid=:3)' using p_objecttype, g_SESSION_ID, g_SESS_DETAIL_ID;
            null;
        end case;
        WRITE_LOG_FINISH( 'Отмечаем в снимке найденные ошибки', p_objecttype, 0, 0);

        deb('Третий сет запросов. Обогащение идентификаторами из целевой системы');

        run_query_set(p_objecttype, 3);

        deb('Четвертый сет запросов. Проверка по бизнес-правилам');

        run_query_set(p_objecttype, 4);
        
        l_dur_interval := current_timestamp - l_perf_start;
        l_dur_min := extract(minute from l_dur_interval);
        l_dur_sec := extract(second from l_dur_interval); 
        deb('Завершена процедура RUN_ALL_QUERIES. Продолжительность - #1:#2', l_dur_min, l_dur_sec);
    end RUN_ALL_QUERIES;


    -- формирует записи в целевой системе на основе таблицы снимка
    procedure deals_create_records
    is
    begin
        deb('Запущена процедура DEALS_CREATE_RECORDS');
        
        -- Обработка вставок
        WRITE_LOG_START;
        deb('Заполнение DDL_TICK_DBT (t_action=1)');
        insert /*+ append */ into ddl_tick_dbt(
                T_DEALID, T_BOFFICEKIND, T_DEALTYPE, T_DEALGROUP, T_TRADESYSTEM, T_DEALCODE, T_DEALCODETS, T_TYPEDOC, T_USERTYPEDOC, T_PARTYID, T_BROKERID, T_CLIENTID, T_TRADERID, T_DEPOSITID, T_MARKETID, T_INDOCID, T_DEALDATE, T_REGDATE, T_DEALSTATUS, T_NUMBERPACK, 
                T_DEPARTMENT, T_OPER, T_ORIGINID, T_EXTERNID, T_FLAG1, T_FLAG2, T_FLAG3, T_FLAG4, T_FLAG5, T_USERFIELD1, T_USERFIELD2, T_USERFIELD3, T_USERFIELD4, T_COMMENT, T_CLOSEDATE, T_SHIELD, T_SHIELDSIZE, T_ISPERCENT, T_SCALE, T_POINTS, 
                T_REVRATE, T_COLLATERAL, T_DEALTIME, T_PORTFOLIOID, T_BUNDLE, T_CBRISKGROUP, T_RISKGROUP, T_ATTRIBUTES, T_PRODUCT, T_NETTING, T_DEALCODEPS, T_CONFTPID, T_LINKCHANNEL, T_NUMBER_COUPON, T_MARKETOFFICEID, T_CLIENTCONTRID, T_BROKERCONTRID, T_INDOCCODE, T_PREOUTLAY, T_PREOUTLAYFIID, 
                T_GROUNDID, T_BUYGOAL, T_COMMDATE, T_PAYMENTSMETHOD, T_FIXSUM, T_NUMBER_PARTLY, T_CHANGEDATE, T_INSTANCE, T_CHANGEKIND, T_PORTFOLIOID_2, T_ISPARTYCLIENT, T_PARTYCONTRID, T_BRANCH, T_AVOIRKIND, T_OFBU, T_MARKETSCHEMEID, T_DEPSETID, T_RETURNINCOMEKIND, T_REQUESTID, T_BLOCKED, 
                T_COUNTRY, T_PFI, T_ISINSTANCY, T_GENAGRID, T_PARENTID, T_ISNETTING, T_VERSION, T_CARRYWRT, T_COUPONNDFL, T_PROGNOS, T_ISTRADEFINANCE, T_SECTOR, T_INCLUDE_DAY, T_AUTOCLOSE, T_OPENDATE, T_KINDDEALPARTYCLIENT, T_CURDEAL, T_TAXHANDLE, T_CURPAY, T_CURGET, 
                T_TAXOWNBEGDATE, T_DISCONT, T_FACTRECEIVERID, T_ISCONFIRMED, T_ISREADY, T_SUMPAY, T_NEGATIVERATE, T_ASSIGNMENT, T_WITHPERCENT, T_LIMITCUR, T_DEBTLIMIT, T_ISSUANCELIMIT, T_TAX_AMOUNT, T_CREDIT_TAX_AMOUNT, T_CREDIT_TAX_CUR, T_CREDIT_TAX_TERM, T_PLACEMENT, T_OFFER, T_FLAGTYPEDEAL, T_CASHDECISION, 
                T_ISPFI, T_ADJDATETYPE, T_SUBORDINATEDDATE, T_CAPITALNOTINCLUDED, T_ISSPOT, T_CALCPFI, T_PAYMAGENT, T_AUTOPLACEMENT)
        select
                TGT_DEALID /*T_DEALID*/, TGT_BOFFICEKIND /*T_BOFFICEKIND*/, TGT_DEALKIND /*T_DEALTYPE*/, NULL /*T_DEALGROUP*/, NULL /*T_TRADESYSTEM*/, 
                T_CODE /*T_DEALCODE*/, T_EXTCODE /*T_DEALCODETS*/, t_tskind /*T_TYPEDOC*/, NULL /*T_USERTYPEDOC*/, TGT_PARTYID /*T_PARTYID*/, 
                TGT_BROKERID /*T_BROKERID*/, -1 /*T_CLIENTID*/, -1 /*T_TRADERID*/, -1 /*T_DEPOSITID*/, TGT_MARKETID /*T_MARKETID*/, 
                NULL /*T_INDOCID*/, T_DATE /*T_DEALDATE*/, T_DATE /*T_REGDATE*/, 2 /*T_DEALSTATUS*/, NULL /*T_NUMBERPACK*/, 
                tgt_department /*T_DEPARTMENT*/, g_oper /*T_OPER*/, NULL /*T_ORIGINID*/, NULL /*T_EXTERNID*/, case when tgt_marketid>0 then 'X' else chr(0) end /*T_FLAG1*/, 
                NULL /*T_FLAG2*/, NULL /*T_FLAG3*/, NULL /*T_FLAG4*/, case when T_COSTCHANGE=chr(88) then chr(88) else chr(0) end/*T_FLAG5*/, NULL /*T_USERFIELD1*/, 
                NULL /*T_USERFIELD2*/, NULL /*T_USERFIELD3*/, NULL /*T_USERFIELD4*/, NULL /*T_COMMENT*/, t_closedate /*T_CLOSEDATE*/, 
                NULL /*T_SHIELD*/, NULL /*T_SHIELDSIZE*/, NULL /*T_ISPERCENT*/, 1 /*T_SCALE*/, 4 /*T_POINTS*/, 
                NULL /*T_REVRATE*/, NULL /*T_COLLATERAL*/, T_TIME /*T_DEALTIME*/, TGT_PORTFOLIOID /*T_PORTFOLIOID*/, NULL /*T_BUNDLE*/, 
                NULL /*T_CBRISKGROUP*/, NULL /*T_RISKGROUP*/, NULL /*T_ATTRIBUTES*/, NULL /*T_PRODUCT*/, NULL /*T_NETTING*/, 
                NULL /*T_DEALCODEPS*/, NULL /*T_CONFTPID*/, NULL /*T_LINKCHANNEL*/, TGT_WARRANT_NUM /*T_NUMBER_COUPON*/, tgt_sector /*T_MARKETOFFICEID*/, 
                NULL /*T_CLIENTCONTRID*/, NULL /*T_BROKERCONTRID*/, NULL /*T_INDOCCODE*/, NULL /*T_PREOUTLAY*/, NULL /*T_PREOUTLAYFIID*/, 
                NULL /*T_GROUNDID*/, 0 /*T_BUYGOAL*/, NULL /*T_COMMDATE*/, NULL /*T_PAYMENTSMETHOD*/, NULL /*T_FIXSUM*/, 
                NULL /*T_NUMBER_PARTLY*/, NULL /*T_CHANGEDATE*/, NULL /*T_INSTANCE*/, NULL /*T_CHANGEKIND*/, TGT_PORTFOLIOID_2 /*T_PORTFOLIOID_2*/, 
                NULL /*T_ISPARTYCLIENT*/, NULL /*T_PARTYCONTRID*/, NULL /*T_BRANCH*/, TGT_AVOIRKIND /*T_AVOIRKIND*/, NULL /*T_OFBU*/, 
                NULL /*T_MARKETSCHEMEID*/, NULL /*T_DEPSETID*/, case when t_costchange=chr(88) then 2 end /*T_RETURNINCOMEKIND*/, NULL /*T_REQUESTID*/, NULL /*T_BLOCKED*/, 
                tgt_country /*T_COUNTRY*/, tgt_avoirissid /*T_PFI*/, NULL /*T_ISINSTANCY*/, NULL /*T_GENAGRID*/, NULL /*T_PARENTID*/, 
                NULL /*T_ISNETTING*/, 0 /*T_VERSION*/, NULL /*T_CARRYWRT*/, NULL /*T_COUPONNDFL*/, NULL /*T_PROGNOS*/, 
                NULL /*T_ISTRADEFINANCE*/, NULL /*T_SECTOR*/, NULL /*T_INCLUDE_DAY*/, NULL /*T_AUTOCLOSE*/, NULL /*T_OPENDATE*/, 
                NULL /*T_KINDDEALPARTYCLIENT*/, NULL /*T_CURDEAL*/, NULL /*T_TAXHANDLE*/, NULL /*T_CURPAY*/, NULL /*T_CURGET*/, 
                NULL /*T_TAXOWNBEGDATE*/, NULL /*T_DISCONT*/, NULL /*T_FACTRECEIVERID*/, NULL /*T_ISCONFIRMED*/, NULL /*T_ISREADY*/, 
                NULL /*T_SUMPAY*/, NULL /*T_NEGATIVERATE*/, NULL /*T_ASSIGNMENT*/, NULL /*T_WITHPERCENT*/, NULL /*T_LIMITCUR*/, 
                NULL /*T_DEBTLIMIT*/, NULL /*T_ISSUANCELIMIT*/, NULL /*T_TAX_AMOUNT*/, NULL /*T_CREDIT_TAX_AMOUNT*/, NULL /*T_CREDIT_TAX_CUR*/, 
                NULL /*T_CREDIT_TAX_TERM*/, NULL /*T_PLACEMENT*/, NULL /*T_OFFER*/, NULL /*T_FLAGTYPEDEAL*/, NULL /*T_CASHDECISION*/, 
                NULL /*T_ISPFI*/, NULL /*T_ADJDATETYPE*/, NULL /*T_SUBORDINATEDDATE*/, NULL /*T_CAPITALNOTINCLUDED*/, NULL /*T_ISSPOT*/, 
                NULL /*T_CALCPFI*/, NULL /*T_PAYMAGENT*/, NULL /*T_AUTOPLACEMENT*/
        from dtxdeal_tmp 
        where t_replstate=0 and t_action=1;   
        WRITE_LOG_FINISH('Заполнение DDL_TICK_DBT (t_action=1)', 80);
        
        WRITE_LOG_START;
        deb('Заполнение DDL_LEG_DBT по первой части (t_action=1)');
        insert /*+ parallel(4) */ into ddl_leg_dbt(
                T_DEALID, T_LEGID, T_PFI, T_CFI, T_START, T_MATURITY, T_EXPIRY, T_PRINCIPAL, T_PRICE, T_BASIS, T_DURATION, T_PITCH, T_COST, T_MODE, T_CLOSED, T_REFRATE, T_FACTOR, T_FORMULA, T_VERSION, 
                T_RESERVE0, T_PERIODNUMBER, T_PERIODTYPE, T_DIFF, T_PAYDAY, T_LEGKIND, T_SCALE, T_POINT, T_ISCALCUSED, T_LEGNUMBER, T_RELATIVEPRICE, T_NKD, T_TOTALCOST, T_MATURITYISPRINCIPAL, T_REGISTRAR, 
                T_INCOMERATE, T_INCOMESCALE, T_INCOMEPOINT, T_INTERESTSTART, T_RECEIPTAMOUNT, T_REGISTRARCONTRID, T_PRINCIPALBASE, T_PRINCIPALDIFF, T_STARTBASE, T_STARTDIFF, T_BASE, T_PAYREGTAX, T_RETURNINCOME, T_REJECTDATE, T_DELIVERINGFIID, 
                T_BITMASK, T_OPERSTATE, T_SUPPLYTIME, T_TABLEPERCENT, T_TYPEPERCENT, T_CAPTION, T_TYPEDATE, T_COUNTDAY, T_CORRECT, T_PAYFIID, T_NKDFIID, T_SROK, T_CLIRINGDATE, T_CLIRINGCHANGE, T_DEPOSITID, T_DVP, T_CLIRINGTIME, T_SESSION, T_D1, T_D2, 
                T_DMAXPAY, T_QRETURN, T_QRECEIVE, T_SUMINCOME, T_MOVEDATE, T_NOTICEDURATION, T_PLANMATURITY)
        select
                TGT_DEALID /*T_DEALID*/, 0 /*T_LEGID*/, TGT_AVOIRISSID /*T_PFI*/, tgt_currencyid /*T_CFI*/, 
                NULL /*T_START*/, TGT_MATURITY /*T_MATURITY*/, TGT_EXPIRY /*T_EXPIRY*/, t_amount /*T_PRINCIPAL*/, tgt_price /*T_PRICE*/, 
                case when T_REPOBASE>0 then T_REPOBASE - 1 else 0 end /*T_BASIS*/, NULL /*T_DURATION*/, NULL /*T_PITCH*/, T_COST /*T_COST*/, NULL /*T_MODE*/, 
                NULL /*T_CLOSED*/, NULL /*T_REFRATE*/, NULL /*T_FACTOR*/, TGT_FORMULA /*T_FORMULA*/, 0 /*T_VERSION*/, 
                NULL /*T_RESERVE0*/, NULL /*T_PERIODNUMBER*/, NULL /*T_PERIODTYPE*/, NULL /*T_DIFF*/, NULL /*T_PAYDAY*/, 
                0 /*T_LEGKIND*/, 1 /*T_SCALE*/, 4 /*T_POINT*/, NULL /*T_ISCALCUSED*/, NULL /*T_LEGNUMBER*/, 
                TGT_RELATIVEPRICE /*T_RELATIVEPRICE*/, T_NKD /*T_NKD*/, T_TOTALCOST /*T_TOTALCOST*/, TGT_MATURITYISPRINCIPAL /*T_MATURITYISPRINCIPAL*/, NULL /*T_REGISTRAR*/, 
                T_RATE /*T_INCOMERATE*/, 1 /*T_INCOMESCALE*/, 2 /*T_INCOMEPOINT*/, NULL /*T_INTERESTSTART*/, NULL /*T_RECEIPTAMOUNT*/, 
                NULL /*T_REGISTRARCONTRID*/, NULL /*T_PRINCIPALBASE*/, NULL /*T_PRINCIPALDIFF*/, NULL /*T_STARTBASE*/, NULL /*T_STARTDIFF*/, 
                NULL /*T_BASE*/, NULL /*T_PAYREGTAX*/, NULL /*T_RETURNINCOME*/, NULL /*T_REJECTDATE*/, NULL /*T_DELIVERINGFIID*/, 
                NULL /*T_BITMASK*/, NULL /*T_OPERSTATE*/, NULL /*T_SUPPLYTIME*/, NULL /*T_TABLEPERCENT*/, NULL /*T_TYPEPERCENT*/, 
                NULL /*T_CAPTION*/, NULL /*T_TYPEDATE*/, NULL /*T_COUNTDAY*/, NULL /*T_CORRECT*/, 0 /*T_PAYFIID*/, 
                NULL /*T_NKDFIID*/, NULL /*T_SROK*/, NULL /*T_CLIRINGDATE*/, NULL /*T_CLIRINGCHANGE*/, NULL /*T_DEPOSITID*/, 
                NULL /*T_DVP*/, NULL /*T_CLIRINGTIME*/, NULL /*T_SESSION*/, NULL /*T_D1*/, NULL /*T_D2*/, 
                NULL /*T_DMAXPAY*/, NULL /*T_QRETURN*/, NULL /*T_QRECEIVE*/, NULL /*T_SUMINCOME*/, NULL /*T_MOVEDATE*/, 
                NULL /*T_NOTICEDURATION*/, NULL /*T_PLANMATURITY*/
                from dtxdeal_tmp 
                where t_replstate=0 and t_action=1;
        WRITE_LOG_FINISH('Заполнение DDL_LEG_DBT по первой части (t_action=1)', 80);
        
        WRITE_LOG_START;
        deb('Заполнение DDL_LEG_DBT по второй части (t_action=1)');
        insert /*+ parallel(4) */ into ddl_leg_dbt(
                T_DEALID, T_LEGID, T_PFI, T_CFI, T_START, T_MATURITY, T_EXPIRY, T_PRINCIPAL, T_PRICE, T_BASIS, T_DURATION, T_PITCH, T_COST, T_MODE, T_CLOSED, T_REFRATE, T_FACTOR, T_FORMULA, T_VERSION, 
                T_RESERVE0, T_PERIODNUMBER, T_PERIODTYPE, T_DIFF, T_PAYDAY, T_LEGKIND, T_SCALE, T_POINT, T_ISCALCUSED, T_LEGNUMBER, T_RELATIVEPRICE, T_NKD, T_TOTALCOST, T_MATURITYISPRINCIPAL, T_REGISTRAR, 
                T_INCOMERATE, T_INCOMESCALE, T_INCOMEPOINT, T_INTERESTSTART, T_RECEIPTAMOUNT, T_REGISTRARCONTRID, T_PRINCIPALBASE, T_PRINCIPALDIFF, T_STARTBASE, T_STARTDIFF, T_BASE, T_PAYREGTAX, T_RETURNINCOME, T_REJECTDATE, T_DELIVERINGFIID, 
                T_BITMASK, T_OPERSTATE, T_SUPPLYTIME, T_TABLEPERCENT, T_TYPEPERCENT, T_CAPTION, T_TYPEDATE, T_COUNTDAY, T_CORRECT, T_PAYFIID, T_NKDFIID, T_SROK, T_CLIRINGDATE, T_CLIRINGCHANGE, T_DEPOSITID, T_DVP, T_CLIRINGTIME, T_SESSION, T_D1, T_D2, 
                T_DMAXPAY, T_QRETURN, T_QRECEIVE, T_SUMINCOME, T_MOVEDATE, T_NOTICEDURATION, T_PLANMATURITY)
        select
                TGT_DEALID /*T_DEALID*/, 0 /*T_LEGID*/, TGT_AVOIRISSID /*T_PFI*/, tgt_currencyid /*T_CFI*/, 
                NULL /*T_START*/, TGT_MATURITY2 /*T_MATURITY*/, TGT_EXPIRY2 /*T_EXPIRY*/, t_amount /*T_PRINCIPAL*/, t_price2 /*T_PRICE*/, 
                case when T_REPOBASE>0 then T_REPOBASE - 1 else 0 end  /*T_BASIS*/, NULL /*T_DURATION*/, NULL /*T_PITCH*/, T_COST2 /*T_COST*/, NULL /*T_MODE*/, 
                NULL /*T_CLOSED*/, NULL /*T_REFRATE*/, NULL /*T_FACTOR*/, TGT_FORMULA /*T_FORMULA*/, 0 /*T_VERSION*/, 
                NULL /*T_RESERVE0*/, NULL /*T_PERIODNUMBER*/, NULL /*T_PERIODTYPE*/, NULL /*T_DIFF*/, NULL /*T_PAYDAY*/, 
                2 /*T_LEGKIND*/, 1 /*T_SCALE*/, 4 /*T_POINT*/, NULL /*T_ISCALCUSED*/, NULL /*T_LEGNUMBER*/, 
                TGT_RELATIVEPRICE /*T_RELATIVEPRICE*/, T_NKD2 /*T_NKD*/, T_TOTALCOST2 /*T_TOTALCOST*/, TGT_MATURITYISPRINCIPAL2 /*T_MATURITYISPRINCIPAL*/, NULL /*T_REGISTRAR*/, 
                T_RATE /*T_INCOMERATE*/, 1 /*T_INCOMESCALE*/, 2 /*T_INCOMEPOINT*/, NULL /*T_INTERESTSTART*/, NULL /*T_RECEIPTAMOUNT*/, 
                NULL /*T_REGISTRARCONTRID*/, NULL /*T_PRINCIPALBASE*/, NULL /*T_PRINCIPALDIFF*/, NULL /*T_STARTBASE*/, NULL /*T_STARTDIFF*/, 
                NULL /*T_BASE*/, NULL /*T_PAYREGTAX*/, NULL /*T_RETURNINCOME*/, NULL /*T_REJECTDATE*/, NULL /*T_DELIVERINGFIID*/, 
                NULL /*T_BITMASK*/, NULL /*T_OPERSTATE*/, NULL /*T_SUPPLYTIME*/, NULL /*T_TABLEPERCENT*/, NULL /*T_TYPEPERCENT*/, 
                NULL /*T_CAPTION*/, NULL /*T_TYPEDATE*/, NULL /*T_COUNTDAY*/, NULL /*T_CORRECT*/, 0 /*T_PAYFIID*/, 
                NULL /*T_NKDFIID*/, NULL /*T_SROK*/, NULL /*T_CLIRINGDATE*/, NULL /*T_CLIRINGCHANGE*/, NULL /*T_DEPOSITID*/, 
                NULL /*T_DVP*/, NULL /*T_CLIRINGTIME*/, NULL /*T_SESSION*/, NULL /*T_D1*/, NULL /*T_D2*/, 
                NULL /*T_DMAXPAY*/, NULL /*T_QRETURN*/, NULL /*T_QRECEIVE*/, NULL /*T_SUMINCOME*/, NULL /*T_MOVEDATE*/, 
                NULL /*T_NOTICEDURATION*/, NULL /*T_PLANMATURITY*/
                from dtxdeal_tmp 
                where t_replstate=0 and t_action=1 and TGT_ISREPO=chr(88);
        WRITE_LOG_FINISH('Заполнение DDL_LEG_DBT по второй части (t_action=1)', 80);
        
        
        
        
        -- Обработка изменений        
        WRITE_LOG_START;
        deb('Изменение DDL_TICK_DBT (t_action=2)');
        
        MERGE INTO ddl_tick_dbt tgt
        USING (select * from dtxdeal_tmp where t_action=2 and t_replstate=0) sou on (tgt.t_dealid=sou.tgt_dealid)
        WHEN MATCHED THEN UPDATE SET 
                T_BOFFICEKIND=TGT_BOFFICEKIND /*T_BOFFICEKIND*/,                                                T_DEALTYPE=TGT_DEALKIND /*T_DEALTYPE*/,  
                T_DEALCODE=T_CODE /*T_DEALCODE*/,       T_DEALCODETS=T_EXTCODE /*T_DEALCODETS*/,                T_TYPEDOC=t_tskind /*T_TYPEDOC*/, 
                T_PARTYID=TGT_PARTYID /*T_PARTYID*/,    T_BROKERID=TGT_BROKERID /*T_BROKERID*/,                 T_MARKETID=TGT_MARKETID /*T_MARKETID*/, 
                T_DEALDATE=T_DATE /*T_DEALDATE*/,       T_REGDATE=T_DATE /*T_REGDATE*/,                         T_DEALSTATUS=2 /*T_DEALSTATUS*/,  
                T_DEPARTMENT=tgt_department /*T_DEPARTMENT*/,       T_OPER=g_oper /*T_OPER*/,                   T_FLAG1=case when tgt_marketid>0 then 'X' else chr(0) end /*T_FLAG1*/, 
                T_FLAG5=case when T_COSTCHANGE=chr(88) then chr(88) else chr(0) end/*T_FLAG5*/,  
                T_SCALE=1 /*T_SCALE*/,                  T_POINTS=4 /*T_POINTS*/,                                T_DEALTIME=T_TIME /*T_DEALTIME*/, 
                T_PORTFOLIOID=TGT_PORTFOLIOID /*T_PORTFOLIOID*/,                                                T_NUMBER_COUPON=TGT_WARRANT_NUM /*T_NUMBER_COUPON*/, 
                T_MARKETOFFICEID=tgt_sector /*T_MARKETOFFICEID*/,                                               T_PORTFOLIOID_2=TGT_PORTFOLIOID_2 /*T_PORTFOLIOID_2*/, 
                T_AVOIRKIND=TGT_AVOIRKIND /*T_AVOIRKIND*/,                                                      T_RETURNINCOMEKIND=case when t_costchange=chr(88) then 2 end /*T_RETURNINCOMEKIND*/,  
                T_COUNTRY=tgt_country /*T_COUNTRY*/,    T_PFI=tgt_avoirissid /*T_PFI*/;
                
                
        MERGE INTO (select * from ddl_leg_dbt where t_legkind=0) tgt
        USING (select * from dtxdeal_tmp where t_action=2 and t_replstate=0) sou on (tgt.t_dealid=sou.tgt_dealid)
        WHEN MATCHED THEN UPDATE SET 
                T_PFI=TGT_AVOIRISSID /*T_PFI*/,         T_CFI=tgt_currencyid /*T_CFI*/,                         T_MATURITY=TGT_MATURITY /*T_MATURITY*/, 
                T_EXPIRY=TGT_EXPIRY /*T_EXPIRY*/,       T_PRINCIPAL=t_amount /*T_PRINCIPAL*/,                   T_PRICE=tgt_price /*T_PRICE*/, 
                T_BASIS=case when T_REPOBASE>0 then T_REPOBASE - 1 else 0 end /*T_BASIS*/,                      T_COST=T_COST /*T_COST*/,  
                T_FORMULA=TGT_FORMULA /*T_FORMULA*/,    T_SCALE=1 /*T_SCALE*/,                                  T_POINT=4 /*T_POINT*/,  
                T_RELATIVEPRICE=TGT_RELATIVEPRICE /*T_RELATIVEPRICE*/,  T_NKD=T_NKD /*T_NKD*/,                  T_TOTALCOST=T_TOTALCOST /*T_TOTALCOST*/, 
                T_MATURITYISPRINCIPAL=TGT_MATURITYISPRINCIPAL /*T_MATURITYISPRINCIPAL*/,                        T_INCOMERATE=T_RATE /*T_INCOMERATE*/;
        
        MERGE INTO (select * from ddl_leg_dbt where t_legkind=2) tgt
        USING (select * from dtxdeal_tmp where t_action=2 and t_replstate=0) sou on (tgt.t_dealid=sou.tgt_dealid)
        WHEN MATCHED THEN UPDATE SET 
                T_PFI=TGT_AVOIRISSID /*T_PFI*/,         T_CFI=tgt_currencyid /*T_CFI*/,                         T_MATURITY=TGT_MATURITY2 /*T_MATURITY*/, 
                T_EXPIRY=TGT_EXPIRY2 /*T_EXPIRY*/,      T_PRINCIPAL=t_amount /*T_PRINCIPAL*/,                   T_PRICE=T_PRICE2 /*T_PRICE*/, 
                T_BASIS=case when T_REPOBASE>0 then T_REPOBASE - 1 else 0 end /*T_BASIS*/,                      T_COST=T_COST2 /*T_COST*/,  
                T_FORMULA=TGT_FORMULA /*T_FORMULA*/,    T_SCALE=1 /*T_SCALE*/,                                  T_POINT=4 /*T_POINT*/,  
                T_RELATIVEPRICE=TGT_RELATIVEPRICE /*T_RELATIVEPRICE*/,                                          T_NKD=T_NKD2 /*T_NKD*/,
                T_TOTALCOST=T_TOTALCOST2 /*T_TOTALCOST*/,                                                       T_MATURITYISPRINCIPAL=TGT_MATURITYISPRINCIPAL2 /*T_MATURITYISPRINCIPAL*/,                        
                T_INCOMERATE=T_RATE /*T_INCOMERATE*/;
        
        WRITE_LOG_FINISH('Изменение DDL_TICK_DBT (t_action=2)', 80);
        
        
        -- Обработка удалений
        WRITE_LOG_START;
        deb('Удаление из DDL_TICK_DBT (t_action=3)');    
        delete /*+ parallel(4) */ from ddl_tick_dbt where t_dealid in (select tgt_dealid from dtxdeal_tmp where t_replstate=0 and t_action=3);
        delete /*+ parallel(4) */ from ddl_leg_dbt where t_dealid in (select tgt_dealid from dtxdeal_tmp where t_replstate=0 and t_action=3); 
        WRITE_LOG_FINISH('Удаление из DDL_TICK_DBT (t_action=3)', 80);
        
        commit;
        ----------------------------------------------------------------------------
        -- Запись в DTXREPLOBJ_DBT
        WRITE_LOG_START;
        deb('Запись в DTXREPLOBJ_DBT');
        delete /*+ parallel(4) */ from dtxreplobj_dbt where T_OBJECTTYPE=80 and T_OBJECTID in (select t_dealid from dtxdeal_tmp where t_replstate=0);
        WRITE_LOG_FINISH('Запись в DTXREPLOBJ_DBT - подготовка', 80);
        commit;
        
        WRITE_LOG_START;
        update dtxreplobj_dbt set t_objstate=2 where T_OBJECTTYPE=90 and t_objectid in (select t_demandid from dtxdemand_tmp where t_replstate=0 and t_action=3);
        WRITE_LOG_FINISH('Запись в DTXREPLOBJ_DBT - удаления', 80);
        commit;
        
        WRITE_LOG_START;
        insert /*+ parallel(4) */ into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE)
        select 80, t_dealid, 0, tgt_dealid, 0, 0  from dtxdeal_tmp where t_replstate=0 and t_action=1;
        WRITE_LOG_FINISH('Запись в DTXREPLOBJ_DBT - вставки', 80);
        commit;
        
        
        
        -- Запись REPLSTATE в DTXDEAL_DBT
        WRITE_LOG_START;
        deb('Запись REPLSTATE в DTXDEAL_DBT');
        update /*+ parallel(4) */ dtxdeal_dbt tgt 
        set tgt.t_replstate=1
        where t_dealid in (select t_dealid from dtxdeal_tmp where t_replstate=0);
        WRITE_LOG_FINISH('Запись REPLSTATE в DTXDEAL_DBT', 80);
        commit;
        
        ----------------------------------------------------------------------------
        WRITE_LOG_START;
        deb('Запись REPLSTATE в DSPGROUND_DBT / DSPGRDOC_DBT');
        insert /*+ parallel(4) */ into dspground_dbt( t_kind, t_DocLog, t_Direction, t_Receptionist, t_References, t_AltXld, t_SignedDate, t_Party)
        select 26, 513, 2, 11, 1, T_CONTRNUM, max(T_CONTRDATE), max(TGT_PARTYID) from dtxdeal_tmp group by T_CONTRNUM; 
        
        delete /*+ parallel(4) */ dspgrdoc_dbt where (t_SourceDocKind, t_SourceDocID) in (select tgt_bofficekind, tgt_dealid from dtxdeal_tmp where t_replstate=0); 
        commit;
        
        insert into dspgrdoc_dbt( t_SourceDocKind, t_SourceDocID, t_SPGroundID)
        select tgt_bofficekind, tgt_dealid, t_SPGroundID
        from dtxdeal_tmp dl join dspground_dbt dc on ( dl.t_contrnum=dc.t_AltXld and dl.tgt_partyid=dc.t_Party);
        WRITE_LOG_FINISH('Запись REPLSTATE в DSPGROUND_DBT / DSPGRDOC_DBT', 80);
        commit;
        
        ----------------------------------------------------------------------------
        WRITE_LOG_START;
        deb('Запись примечаний к сделкам'); 
        
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 101, g_oper, t_date, date'0001-01-01', t_conditions, date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and t_conditions is not null;
        commit;
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 102, g_oper, t_date, date'0001-01-01', T_MARKETCODE, date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and T_MARKETCODE is not null;
        commit;
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 103, g_oper, t_date, date'0001-01-01', T_PARTYCODE, date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and T_PARTYCODE is not null;
        commit;
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 23, g_oper, t_date, date'0001-01-01', to_char(T_PRICE_CALC), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and T_PRICE_CALC > 0;
        commit;
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 27, g_oper, t_date, date'0001-01-01', to_char(T_PRICE_CALC_DEF), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and T_PRICE_CALC_DEF > 0;
        commit;        
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 28, g_oper, t_date, date'0001-01-01', to_char(T_PRICE_CALC_VAL), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and T_PRICE_CALC_VAL > 0;
        commit;        
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 116, g_oper, t_date, date'0001-01-01', to_char(T_PRICE_CALC_OUTLAY), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and T_PRICE_CALC_OUTLAY > 0;
        commit;
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 26, g_oper, t_date, date'0001-01-01', T_DOPCONTROL_NOTE, date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and T_DOPCONTROL_NOTE is not null;
        commit;
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 29, g_oper, t_date, date'0001-01-01', T_PRICE_CALC_MET_NOTE, date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and T_PRICE_CALC_MET_NOTE is not null;
        commit;
        insert /*+ append */ into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select 101, lpad( tgt_dealid, 10, '0'), 112, g_oper, t_date, date'0001-01-01', to_char(T_PAYMCUR), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0 and T_PAYMCUR > 0;
        WRITE_LOG_FINISH('Запись примечаний к сделкам', 80);
        commit;
        
               
        WRITE_LOG_START;
        deb('Запись категорий к сделкам');

        delete /*+ parallel(4) */ from dobjatcor_dbt where t_objecttype in (101,117) and t_object in (select lpad( tgt_dealid, 10, '0') from dtxdeal_tmp where t_replstate=0); 
        commit;
        -- Установка значения категории Сделка подлежит дополнительному контролю ( 32 )
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 32, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_DOPCONTROL, 1,1,  2,2,  3,3,  4,4,  5,5,  6,6,  7,9,  8,1,  9,92,  10,94,  11,95,  12,96,  13,13,  14,23,  15,25,  16,7,  17,8,  18,38,  19,938,  20,93,  21,913,  22,925,  T_DOPCONTROL) 
        from dtxdeal_tmp where t_replstate=0 and T_DOPCONTROL > 0 and tgt_BofficeKind in (101, 117);
        commit;

        -- Установка значения категории Признак изменения стоимости реализации/приобретения по 2 части Репо при выплате купонного дохода/дивидентов/частичного погашения ценных бумаг
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 21, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_COSTCHANGEONCOMP, chr(0), 0, 1) 
        from dtxdeal_tmp where t_replstate=0;
        commit;

        -- Установка значения категории Признак изменения стоимости реализации/приобретения по 2 части Репо при выплате купонного дохода/дивидентов/частичного погашения ценных бумаг
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 22, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_COSTCHANGEONAMOR, chr(0), 0, 1) 
        from dtxdeal_tmp where t_replstate=0;
        commit;        
        
        -- Установка значения категории Признак изменения стоимости реализации/приобретения по 2 части Репо при выплате купонного дохода/дивидентов/частичного погашения ценных бумаг
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 33, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        T_FISSKIND 
        from dtxdeal_tmp where t_replstate=0;
        commit;
 
        -- Установка значения категории Способ определения РЦ ( 34 )
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 34, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_PRICE_CALC_METHOD, 1,'10-00', 2,'10-80', 3,'11-00', 4,'11-01', 5,'12-50', 6,'12-51', 7,'12-80', 8,'12-81', 9,'19-00', 10,'20-80', 11,'21-00', 12,'21-01', 13,'22-10', 14,'22-11', 15,'22-20', 16,'22-21', 17,'22-30', 18,'22-31', 19,'22-40', 20,'22-41', 21,'22-50', 22,'22-51', 23,'22-60', 24,'22-61', 25,'22-70', 26,'22-71', 27,'22-80', 28,'22-81', 29,'29-00', 30,'31-00', 31,'31-01', 32,'32-50', 33,'32-51', 34,'99-00', 35,'11-80', 36,'11-81','null') 
        from dtxdeal_tmp where t_replstate=0 and T_PRICE_CALC_METHOD > 0;
        commit;       
        
        -- Признак урегулирования требований по ст.282 НК РФ.
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 19, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_ADJUSTMENT, chr(0), 0, 1) 
        from dtxdeal_tmp where t_replstate=0 and T_ADJUSTMENT <> chr(0);
        commit;   

        -- Признак ограничения прав покупателя по сделке РЕПО - "Блокировка ц/б"
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 103, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_LIMIT, chr(0), 0, 1) 
        from dtxdeal_tmp where t_replstate=0 and T_LIMIT <> chr(0);
        commit;           
        
        -- Признак изменения ставки - "Плавающая ставка"
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 26, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_CHRATE, chr(0), 0, 1) 
        from dtxdeal_tmp where t_replstate=0 and T_CHRATE <> chr(0);
        commit;           

        -- Признак изменения ставки - "Принадлежность дивидендов"
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 25, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_DIV, chr(0), 0, 1) 
        from dtxdeal_tmp where t_replstate=0 and T_DIV <> chr(0);
        commit;     
        
        -- Признак исполнения сделки в любой день
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 20, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_ATANYDAY, chr(0), 0, 1) 
        from dtxdeal_tmp where t_replstate=0 and T_ATANYDAY <> chr(0);
        commit;  

        -- "Место заключения сделки"
        insert /*+ parallel(4) */ into dobjatcor_dbt(t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select 101, 105, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        T_COUNTRY 
        from dtxdeal_tmp where t_replstate=0 and T_COUNTRY is not null and T_COUNTRY <> 158;
        commit;          
        
        WRITE_LOG_FINISH('Запись категорий к сделкам', 80); 
        
        -- TODO платежи по needdemand
        
        
        deb('Завершена процедура DEALS_CREATE_RECORDS');
    end deals_create_records;



    -- формирует записи в целевой системе на основе таблицы снимка
    procedure demands_create_records
    is
    begin
        deb('Запущена процедура DEMANDS_CREATE_RECORDS');
        execute immediate 'alter trigger DDLRQ_DBT_TBI disable';
        
        -- Обработка вставок
        
        WRITE_LOG_START;
        deb('Заполнение DDLRQ_DBT (t_action=1)');
        insert /*+ parallel(4) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL)
        select TGT_DEALID, TGT_DEALKIND /*T_DOCKIND*/, TGT_DEALID, T_PART, TGT_KIND/*T_KIND*/, TGT_SUBKIND, TGT_TYPE, 0 /*T_NUM*/, T_SUM, TGT_FIID, TGT_PARTY, -1, -1/*T_PLACEID*/, TGT_STATE, TGT_PLANDATE, TGT_FACTDATE, CHR(0), CHR(0)/*T_NETTING*/, null, 0, TGT_CHANGEDATE, 0 /*T_ACTION*/, 0, 2908, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0
        from dtxdemand_tmp where t_replstate=0 and t_action=1;
        WRITE_LOG_FINISH('Заполнение DDLRQ_DBT (t_action=1)', 90); 
        
        -- Обработка изменений
        
        WRITE_LOG_START;
        deb('Изменение DDLRQ_DBT (t_action=2)');
        merge /*+ parallel(4) */ into (select T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_AMOUNT, T_FIID, T_PARTY, T_PLANDATE, T_FACTDATE, T_STATE from ddlrq_dbt) rq 
        using (select * from dtxdemand_tmp where t_replstate=0 and t_action=2) sou on (sou.tgt_demandid=rq.t_id)
        when matched then update set
        RQ.T_DOCKIND=SOU.TGT_DEALKIND,          RQ.T_DOCID=SOU.TGT_DEALID,          RQ.T_DEALPART=SOU.T_PART, 
        RQ.T_KIND=SOU.TGT_KIND,                 RQ.T_SUBKIND=SOU.TGT_SUBKIND,       RQ.T_TYPE=SOU.TGT_TYPE, 
        RQ.T_AMOUNT=SOU.T_SUM,                  RQ.T_FIID=SOU.TGT_FIID,             RQ.T_PARTY=SOU.TGT_PARTY, 
        RQ.T_PLANDATE=SOU.TGT_PLANDATE,         RQ.T_FACTDATE=SOU.TGT_FACTDATE,     RQ.T_STATE=SOU.TGT_STATE;
        WRITE_LOG_FINISH('Изменение DDLRQ_DBT (t_action=2)', 90); 

        -- Обработка удалений

        WRITE_LOG_START;
        deb('Удаление из DDLRQ_DBT (t_action=3)');
        delete from ddlrq_dbt where t_id in (select TGT_DEMANDID from dtxdemand_tmp where t_action=3 and t_replstate=0); 
        WRITE_LOG_FINISH('Удаление из DDLRQ_DBT (t_action=3)', 90);         
        
        
        ----------------------------------------------------------------------------
        -- Запись в DTXREPLOBJ_DBT
        WRITE_LOG_START;
        deb('Запись платежей в DTXREPLOBJ_DBT - подготовка');
        delete /*+ parallel(4) */ from dtxreplobj_dbt where T_OBJECTTYPE=90 and T_OBJECTID in (select t_demandid from dtxdemand_tmp where t_replstate=0);
        WRITE_LOG_FINISH('Запись платежей в DTXREPLOBJ_DBT - подготовка', 90);
        commit;
        
        WRITE_LOG_START;
        update dtxreplobj_dbt set t_objstate=2 where T_OBJECTTYPE=90 and t_objectid in (select t_demandid from dtxdemand_tmp where t_replstate=0 and t_action=3);
        WRITE_LOG_FINISH('Запись платежей в DTXREPLOBJ_DBT - удаления', 90);
        commit;
        
        WRITE_LOG_START;
        insert /*+ parallel(4) */ into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE)
        select 90, t_demandid, 0, tgt_demandid, 0, 0 from dtxdemand_tmp where t_replstate=0 and t_action=1;
        WRITE_LOG_FINISH('Запись платежей в DTXREPLOBJ_DBT - вставки', 90);
        commit;
        
        
        
        -- Запись REPLSTATE в DTXDEMAND_DBT
        WRITE_LOG_START;
        deb('Запись REPLSTATE в DTXDEMAND_DBT');
        update /*+ parallel(4) */ dtxdemand_dbt tgt 
        set tgt.t_replstate=1
        where t_demandid in (select t_demandid from dtxdemand_tmp where t_replstate=0);
        WRITE_LOG_FINISH('Запись REPLSTATE в DTXDEAL_DBT', 90);
        commit;
        
        ----------------------------------------------------------------------------
        
        
        execute immediate 'alter trigger DDLRQ_DBT_TBI enable';
        deb('Завершена процедура DEMANDS_CREATE_RECORDS');
    end demands_create_records;


    procedure load_deals_by_period(p_startdate date, p_enddate date default null)
    is
        l_enddate date;
    begin
        deb('Запущена процедура LOAD_DEALS_BY_PERIOD');
        
        l_enddate := nvl( p_enddate, p_startdate + 1 - 1/24/60/60); 
    
        -- Если SESSION_ID был не зназначен, то есть, LOAD_DEALS вызвали вручную, назначим его
        if g_SESSION_ID is null then
            insert into dtx_session_dbt(t_startdate, t_enddate, t_user, t_status)
            values( p_startdate, l_enddate, user, 'R') returning t_sessid into g_SESSION_ID;
        end if;
        
        insert into dtx_sess_detail_dbt( T_SESSID, T_PROCEDURE, T_INSTANCEDATE, T_STARTDATE)
        values (g_SESSION_ID, 'LOAD_DEALS', p_startdate, sysdate) returning t_detailid into g_SESS_DETAIL_ID;
        commit;
    
        -- заполняет таблицу снимка (dtxdeal_tmp из dtxdeeal_dbt)
        deals_create_snapshot(p_startdate, l_enddate);
        
        -- прогоняет сеты запросов по таблице снимка dtxdeal_tmp
        run_all_queries( 80 );
        
        -- переносит ошбки в таблицу логов dtxloadlog_dbt
        write_errors_into_log;
        
        -- формирует записи в таблицы целевой системы (DDL_TICK_DBT, DDL_LEG_DBT, DNOTETEXT_DBT...) на основе таблицы снимка
        deals_create_records;

        deb('Завершена процедура LOAD_DEALS_BY_PERIOD');
    end load_deals_by_period;



    procedure load_demands_by_period(p_startdate date, p_enddate date default null)
    is
        l_enddate date;
    begin
        deb('Запущена процедура LOAD_DEMANDS_BY_PERIOD');
                
        l_enddate := nvl( p_enddate, p_startdate + 1 - 1/24/60/60); 
    
        -- Если SESSION_ID был не зназначен, то есть, LOAD_DEALS вызвали вручную, назначим его
        if g_SESSION_ID is null then
            insert into dtx_session_dbt(t_startdate, t_enddate, t_user, t_status)
            values( p_startdate, l_enddate, user, 'R') returning t_sessid into g_SESSION_ID;
        end if;
        
        insert into dtx_sess_detail_dbt( T_SESSID, T_PROCEDURE, T_INSTANCEDATE, T_STARTDATE)
        values (g_SESSION_ID, 'LOAD_DEMANDS', p_startdate, sysdate) returning t_detailid into g_SESS_DETAIL_ID;
        commit;
    
        -- заполняет таблицу снимка (dtxdeal_tmp из dtxdeeal_dbt)
        demands_create_snapshot(p_startdate, l_enddate);
        
        -- прогоняет сеты запросов по таблице снимка dtxdeal_tmp
        run_all_queries( 90 );
        
        -- переносит ошбки в таблицу логов dtxloadlog_dbt
        write_errors_into_log;
        
        -- формирует записи в таблицы целевой системы (DDL_RQ_DBT, DNOTETEXT_DBT...) на основе таблицы снимка
        demands_create_records;

        deb('Завершена процедура LOAD_DEMANDS_BY_PERIOD');
    end load_demands_by_period;




        function GetCurrentNom(p_fi number, p_date date) return number
        DETERMINISTIC
        is
            l_tmp number;
        begin
            l_tmp := RSB_FIINSTR.FI_GetNominalOnDate( p_fi, p_date, 0);
            return l_tmp;
        end GetCurrentNom; 
        

        function GetIsQuoted(p_fi number, p_date date) return char
        DETERMINISTIC
        is begin
            return case RSB_FIINSTR.FI_IsQuoted(p_fi, p_date) when 1 then chr(88) else chr(0) end;
        end GetIsQuoted;

        function GetIsKSU(p_fi number) return char
        DETERMINISTIC
        is begin
            return case RSB_FIINSTR.FI_IsKSU(p_fi) when 1 then chr(88) else chr(0) end;
        end GetIsKSU;






            -- перекодирует kind сделки в значение из домена целевой системы
        function GetDealKind( p_kind number, p_avoirissid number, p_market number, p_isbasket char, p_isksu char)    return number
        DETERMINISTIC
        is
                l_dealtype_tmp number;
                l_ismarket boolean;
                l_fiid  number;
            begin
                l_fiid := p_avoirissid;
                l_ismarket := case when p_market > 0 then true else false end;

                case p_kind
                when    10  then
                            if l_ismarket then
                                l_dealtype_tmp := 2143; -- покупка биржевая
                            else
                                l_dealtype_tmp := 2183; -- покупка внебиржевая
                            end if;
                when    20  then
                            if l_ismarket then
                                l_dealtype_tmp := 2153; -- продажа биржевая
                            else
                                l_dealtype_tmp := 2193; -- продажа внебиржевая
                            end if;
                when    30  then
                            if l_ismarket then
                                    if p_isbasket = chr(88) then
                                            l_dealtype_tmp := 2123; -- репо покупка биржевая КСУ
                                    else
                                            l_dealtype_tmp := 2122; -- репо покупка биржевая
                                    end if;
                            else
                                    l_dealtype_tmp := 2132; -- репо покупка внебиржевая
                            end if;
                when    40  then
                            if l_ismarket then
                                    if p_isbasket = chr(88) then
                                            l_dealtype_tmp := 2128; -- репо продажа биржевая КСУ
                                    else
                                            l_dealtype_tmp := 2127; -- репо продажа биржевая
                                    end if;
                            else
                                    l_dealtype_tmp := 2137; -- репо продажа внебиржевая
                            end if;
                when    50  then
                            l_dealtype_tmp := 2195; -- займ - привлечение
                when    60  then
                            l_dealtype_tmp := 2197; -- займ - размещение
                when    70  then
                            l_dealtype_tmp := 2021; -- погашение выпуска
                when    80  then
                            l_dealtype_tmp := 2022; -- погашение купона
                when    90  then
                            l_dealtype_tmp := 2027; -- погашение облигации частичное
                when    100 then
                            l_dealtype_tmp := 2105;
                else
                        l_dealtype_tmp := -1;
                end case;

                if p_isbasket=chr(88)
                then
                    if l_dealtype_tmp in (2122, 2132) then
                        l_dealtype_tmp := 2139; -- обратное РЕПО на корзину
                    elsif l_dealtype_tmp in (2127, 2137) then
                        l_dealtype_tmp := 2139; -- прямое РЕПО на корзину
                    else
                        l_dealtype_tmp := -1;
                    end if;
                end if;

                return l_dealtype_tmp;

            end GetDealKind;



  
    
    
    

    procedure load_deals( p_date date, p_action number)
    is

            --replobj_rec_arr             replobj_rec_arr_type;
            --replobj_rec_inx_arr         replobj_rec_inx_arr_type; -- индексная коллекция
            --replobj_tmp_arr             replobj_tmp_arr_type;            
            
            cursor m_cur(pp_date date, pp_action number) is select * from DTXDEAL_DBT where t_instancedate >= pp_date and t_instancedate < (pp_date+1-1/24/60/60) and t_replstate=0 and t_action = pp_action order by t_instancedate;

            type index_collection_type is table of number index by varchar2(100);  -- тип индексной коллекции, используется для поиска связей сущностей.

            -- запись для бумаг, кэшируем из таргета-------------
            type avr_add_record is record(
                                    r_fiid number,
                                    r_name varchar2(200),
                                    r_isin varchar2(50),
                                    r_facevaluefi number,
                                    r_current_nom number,
                                    r_type number,
                                    r_is_quoted number(1),
                                    r_is_ksu number(1),
                                    r_isbond number(1),
                                    r_coupon_number number,
                                    r_party_number  number,
                                    r_coupon_num_tgt number,
                                    r_party_num_tgt number
                                  );
            type avr_add_arr_type is table of avr_add_record index by pls_integer;

            avr_add_arr         avr_add_arr_type; -- коллекция бумаг, индексированная FIID


            -- коллекция для получения группы и признаков сделки
            type tmp_dealogroup_type  is record (
                                    r_dealid  number,
                                    r_group   number,
                                    r_isbuy   number(1),
                                    r_issale  number(1),
                                    r_isloan  number(1),
                                    r_isrepo  number(1)
                                    );
            type tmp_dealogroup_arr_type is table of tmp_dealogroup_type index by pls_integer;

            tmp_dealogroup_arr  tmp_dealogroup_arr_type;

            -- коллекции для временного буфера сделок.
            type ddl_tick_dbt_arr_type is table of ddl_tick_dbt%rowtype index by pls_integer;
            type ddl_leg_dbt_arr_type is table of ddl_leg_dbt%rowtype index by pls_integer;

            -- для загрузки записей из целевой системы.  После загрузки сразу переносятся в основные структуры
            tmp_ddl_tick_dbt_arr_in     ddl_tick_dbt_arr_type;
            tmp_ddl_leg_dbt_arr_in      ddl_leg_dbt_arr_type;

            -- для формирования выгружаемых записей
            ddl_tick_dbt_arr_out    ddl_tick_dbt_arr_type;
            ddl_leg_dbt_arr_out     ddl_leg_dbt_arr_type;
            ddl_leg2_dbt_arr_out    ddl_leg_dbt_arr_type;

            -- буфер таблицы договоров (dspground_dbt, dspgrdoc_dbt)
            type dspground_type is record (
                                    r_spgroundid    number,
                                    r_dealid        number,
                                    r_AltXld        varchar2(30),
                                    r_SignedDate    date,
                                    r_party         number,
                                    r_BofficeKind   number
                                    );
            type dspground_arr_type is table of dspground_type index by pls_integer;

            dspground_arr dspground_arr_type;


            -- пополняемая коллекция для купонов и чп. Индексируется T_ID, содержит t_number
            type warrant_arr_type is table of number index by pls_integer;
            warrant_arr  warrant_arr_type;


            -- Вспомогательные переменные
            good_deals_count    number; -- количество сделок, не исключенных клинингом
            main_tmp    DTXDEAL_dBT%ROWTYPE;
            add_tmp     deal_sou_add_type;
            tick_tmp    DDL_TICK_DBT%ROWTYPE;
            leg1_tmp     DDL_LEG_DBT%ROWTYPE;
            leg2_tmp     DDL_LEG_DBT%ROWTYPE;
            date_tmp    DATE;
            dealid_tmp  number;
            dealtype_tmp number;
            curnom_tmp  number;
            tmp_sou_id  number;
            avr_fiid_tmp    number;
            dspground_tmp   dspground_type;
            change_flag boolean := false;

            -- коллекции для поиска сделок в целевой системе по DEALID (для изменений-удалений) и DEALCODE (для вставок)
            -- все результаты поиска переносятся в коллекцию deal_sou_add_arr, поэтому ..TMP
            tmp_dealids         tmp_dealid_arr_type;
            tmp_dealcodes_in    tmp_dealcode_arr_type;
            tmp_dealcodes_out   tmp_dealcode_arr_type;
            tmp_dealcodes_back  tmp_varchar_back_arr_type;
            
            
            l_counter   number := 0; -- счетчик обработанных строк
            l_err_counter   number := 0; -- счетчик ошибок


            -- выявлены проблемы с записью. Логируем ошибку и исключаем запись из обработки.
            procedure pr_exclude(p_code number, p_objtype number, p_id number, p_subnum number := 0, p_text varchar2, p_counter number, p_action number, p_silent boolean := false)
            is
                text_corr varchar2(1000);
                v_row DTXDEAL_DBT%ROWTYPE;
            begin
                deb('Запущена процедура  PR_EXCLUDE',p_level=>5);
                v_row := deal_sou_arr(p_counter);
                text_corr := replace(p_text, '%act%',  (case p_action when 1 then 'Вставка' when 2 then 'изменение' when 3 then 'удаление' end) );
                /*
                text_corr := replace(text_corr, '%fiid%', v_row.t_fiid);
                text_corr := replace(text_corr, '%basefiid%', v_row.t_basefiid);
                text_corr := replace(text_corr, '%type%', v_row.t_type);
                text_corr := replace(text_corr, '%date%', to_char(v_row.t_ratedate,'dd.mm.yyyy'));
                */
                deb(text_corr || ', операция #1', p_id, p_level=>5);
                -- потом заменить на add_log_deferred
                if not p_silent
                then
                    add_log( p_code, p_objtype, p_id, p_subnum, text_corr, p_date);
                end if;

                -- исключаем элемент
                deal_sou_add_arr(p_counter).result := 2;
                -- уменьшаем счетчик сделок
                good_deals_count := good_deals_count - 1;
                l_err_counter := l_err_counter + 1;
                deb('Завершена процедура  PR_EXCLUDE', p_level=>5);
            end pr_exclude;

            -- запись обработана успешно.
            procedure pr_include( p_counter number)
            is
            begin
                deb('Запись обработана успешно! Процедура pr_include для записи номер #1', p_counter, p_level => 5);

                deal_sou_add_arr(p_counter).result := 1;
                l_counter := l_counter + 1;
            end pr_include;


            -- Записываем в таблицы из буфера все добавленные сделки (только для ACTION=1)
            procedure write_deals_to_ddltick
            is
                clop number;
                sou_dealid_tmp  number; -- dealid записи в исходной системе
                sou_index_tmp   number; -- индекс записи в исходной коллекции deal_sou_arr
                tgt_dealid_tmp  number; -- dealid записи в целевой системе
            begin
                deb('Запущена процедура WRITE_DEALS_TO_DDLTICK');
                if ddl_tick_dbt_arr_out.count = 0 
                then
                    deb('Нет записей для обработки');
                else
                    deb('Вставка тикетов сделок DDL_TICK_DBT, в коллекции #1 записей', ddl_tick_dbt_arr_out.count);
                    begin
                        forall i in indices of ddl_tick_dbt_arr_out SAVE EXCEPTIONS
                                insert into ddl_tick_dbt
                                values ddl_tick_dbt_arr_out(i);
                    exception when others
                    then
                        deb('Ошибка вставки сделки');
                        deb('count = ' || SQL%BULK_EXCEPTIONS.COUNT);
                        deb('Обработано = ' || SQL%ROWCOUNT);

                        for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                        loop
                            clop := SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1;
                            tgt_dealid_tmp := ddl_tick_dbt_arr_out(i).t_dealid;
                            sou_index_tmp  := deal_sou_back( tgt_dealid_tmp );
                            sou_dealid_tmp := deal_sou_arr( sou_index_tmp ).t_dealid;
                            --dbms_output.put_line('Ошибка вставки сделки (ddl_tick_dbt) c dealcode = ' || ddl_tick_dbt_arr_out( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).t_dealcode ||   ':  ' || SQLERRM( -1 * SQL%BULK_EXCEPTIONS(i).ERROR_CODE));
                            deb('Ошибка вставки сделки (ddl_tick_dbt) c dealcode = ' || ddl_tick_dbt_arr_out( clop ).t_dealcode ||   ':  ' || SQLERRM( -SQL%BULK_EXCEPTIONS(i).ERROR_CODE));
                            pr_exclude(568, c_OBJTYPE_DEAL, sou_index_tmp, 0, 'Ошибка при вставке сделки с dealcode : ' || ddl_tick_dbt_arr_out( clop ).t_dealcode || ':   ' || SQLERRM( -SQL%BULK_EXCEPTIONS(i).ERROR_CODE), i, 1);
                        end loop;
                    end;

                    deb('Выполнена вставка в DDL_TICK_DBT, добавлено - #1, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT);
                    commit;

                    deb('Вставка ценовых условий сделок DDL_LEG_DBT, в коллекции #1 записей', ddl_leg_dbt_arr_out.count);
                    forall i in indices of ddl_leg_dbt_arr_out
                            insert /*+ append_values */ into ddl_leg_dbt
                            values ddl_leg_dbt_arr_out(i);
                    commit;

                    deb('Выполнена вставка в DDL_LEG_DBT, добавлено - #1, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT);
                    deb('Очистка буфера сделок ddl_tick_dbt_arr_out');
                    ddl_tick_dbt_arr_out.delete;
                    deb('Очистка буфера сделок ddl_leg_dbt_arr_out');
                    ddl_leg_dbt_arr_out.delete;
                    DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
                end if;
                deb('Завершена процедура WRITE_DEALS_TO_DDLTICK');
            end write_deals_to_ddltick;


            -- после репликации порции сделок заполняет таблицу dtxreplobj
            procedure write_deals_to_replobj
            is
            begin
                deb('Запущена процедура  WRITE_DEALS_TO_REPLOBJ');

                if deal_sou_arr.count = 0 then
                    deb('Нет записей для обработки');
                else

                    for i in nvl(deal_sou_arr.first,0) .. nvl(deal_sou_arr.last, -1)
                    loop
                        if deal_sou_add_arr(i).tgt_dealid is null
                        then
                            deb('Ошибка! в буфере destid=NULL для сделки #1', deal_sou_arr(i).t_dealid);
                            return;
                        end if;
                    end loop;

                    -- todo forall переделать на table()
                    -- уже существующих подобных записей быть не должно. Это удаление мусора, если он есть.
                    forall i in indices of deal_sou_arr 
                        delete from dtxreplobj_dbt
                        where T_OBJECTTYPE = 80 and T_OBJECTID = deal_sou_arr(i).t_dealid and deal_sou_add_arr(i).result < 2 and deal_sou_arr(i).t_action = 1; -- только для успешно загруженных вставок
                    deb('Удаление мусора, обработано #1 записей', SQL%ROWCOUNT);
                    commit; 
                    
                    forall i in indices of deal_sou_arr 
                        insert into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE)
                        select 80, deal_sou_arr(i).t_dealid, 0, deal_sou_add_arr(i).tgt_dealid, 0, 0 from dual
                        where deal_sou_add_arr(i).result < 2 and deal_sou_arr(i).t_action = 1; -- только для успешно загруженных записей.
                    deb('Вставка в dtxreplobj_dbt (для t_action=1), обработано #1 записей', SQL%ROWCOUNT);
                    
                    -- здесь проставляем признак удаленной записи в dtxreplobj
                    forall i in indices of deal_sou_arr SAVE EXCEPTIONS
                        update dtxreplobj_dbt set T_OBJSTATE=2 where T_OBJECTTYPE=80 and T_OBJECTID=deal_sou_arr(i).t_dealid and T_DESTID=deal_sou_add_arr(i).tgt_dealid
                        and deal_sou_arr(i).t_action in (2,3);
                    deb('Обновление dtxreplobj_dbt (для t_action 1,2), обработано #1 записей', SQL%ROWCOUNT);

                end if;
                deb('Завершена процедура  WRITE_DEALS_TO_REPLOBJ');
            end write_deals_to_replobj;


            -- проставляет сделкам в dtxdeal признак удачной-неудачной репликации
            procedure update_dtxdeal
            is
            begin
                deb('Запущена процедура  UPDATE_DTXDEAL');
                begin
                    forall i in indices of deal_sou_arr SAVE EXCEPTIONS
                        update dtxdeal_dbt set t_replstate=deal_sou_add_arr(i).result
                        where deal_sou_add_arr(i).result in (1) and t_dealid=deal_sou_arr(i).t_dealid
                        and t_instancedate=deal_sou_arr(i).t_instancedate and t_replstate=0;
                exception when others then
                        for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                        loop
                            deb('Ошибка #2 обновления DTXDEAL_DBT, объект #1', deal_sou_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).t_dealid, SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),  p_level => 5);
                        end loop;
                end;
                deb('Обработано #1 записей', SQL%ROWCOUNT);
                deb('Завершена процедура  UPDATE_DTXDEAL');
            end update_dtxdeal;




            -- запись в БД (dspground_dbt, dspgrdoc_dbt) из буфера таблиц договоров
            procedure write_grounds
            is
            begin
                deb('Запущена процедура  WRITE_GROUNDS');
                if dspground_arr.count = 0 then
                    deb('Нет записей для вставки');
                else
                    deb('Количество записей в буфере dspground_arr - #1', dspground_arr.count);

                    begin
                        forall i in indices of dspground_arr save exceptions
                            insert into dspground_dbt( t_kind, t_DocLog, t_Direction, t_Receptionist, t_References, t_AltXld, t_SignedDate, t_Party)
                            values (26, 513, 2, g_oper, 1, dspground_arr(i).r_AltXld, dspground_arr(i).r_SignedDate, dspground_arr(i).r_party);
                    exception when others then
                        for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                        loop
                            deb('Ошибка #2 добавления в DSPGROUND_DBT, объект #1', dspground_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).r_AltXld, SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),  p_level => 5);
                        end loop;
                    end;
                    deb('Выполнена вставка в DSPGROUND_DBT, вставлено #1 записей, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT);

                    -- удаляем возможные привязки к документам
                    begin
                        forall i in indices of dspground_arr save exceptions
                            delete dspgrdoc_dbt where t_SourceDocKind=dspground_arr(i).r_BofficeKind and t_SourceDocID=dspground_arr(i).r_dealid and t_SPGroundID=dspground_arr(i).r_spgroundid;
                    exception when others then
                        for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                        loop
                            deb('Ошибка #2 удаления из DSPGRDOC_DBT, объект #1', dspground_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).r_AltXld, SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE),  p_level => 5);
                        end loop;
                    end;
                    deb('Удалены текущие привязки из DSPGRDOC_DBT');

                    -- и добавляем новые
                    begin
                        forall i in indices of dspground_arr save exceptions
                            insert into dspgrdoc_dbt( t_SourceDocKind, t_SourceDocID, t_SPGroundID)
                            values (dspground_arr(i).r_BofficeKind, dspground_arr(i).r_dealid, dspground_arr(i).r_spgroundid);
                    exception when others then
                        deb('!!!');
                        for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                        loop
                            dbms_output.put_line(nvl( dspground_arr(0).r_BofficeKind, -1)  );
                            dbms_output.put_line(nvl(dspground_arr(0).r_dealid,-1));
                            dbms_output.put_line(nvl(dspground_arr(0).r_spgroundid, -1));
                            if i>10 then
                                exit;
                            end if;
                         end loop;
                    end;
                    deb('Выполнена вставка в DSPGRDOC_DBT, вставлено #1 записей, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT);
                    commit;

                    dspground_arr.delete;
                    DBMS_SESSION.FREE_UNUSED_USER_MEMORY;

                end if;
                deb('Завершена процедура  WRITE_GROUNDS');
            end write_grounds;





            -- Процедура очистки данных.
            -- Принимает на вход номер текущей записи в массиве l_main_tmp, преобразует null`ы в основной записи, сохраняет перекодированные значения в дополнительной записи
            function deal_cleaning( i number ) return boolean
            is
                l_main_tmp DTXDEAL_DBT%ROWTYPE;
                l_add_tmp  deal_sou_add_type;
                l_warrantid number;
                l_partialid number;
                exp_wrong_date  exception;
            begin
                        deb('Запущена процедура  DEAL_CLEANING', p_level=>5);
                        l_main_tmp := deal_sou_arr(i);
                        l_add_tmp  := null;

                        l_add_tmp.tgt_dealid :=  replobj_get(c_OBJTYPE_DEAL, l_main_tmp.t_dealid).dest_id;
                        l_add_tmp.tgt_state  :=  replobj_get(c_OBJTYPE_DEAL, l_main_tmp.t_dealid).state;
                        deal_sou_back(l_add_tmp.tgt_dealid) :=  i; -- поисковая коллекция для обратной связи между DDL_TICK_DBT и DTXDEAL.

                        if l_add_tmp.tgt_state = 1
                        then
                                pr_exclude(206, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: объект находится в режиме ручного редактирования', i, p_action);
                                raise exp_wrong_date;
                        end if;

                        tick_tmp.T_DEALID       := l_add_tmp.tgt_dealid; -- ??? посмотрим
                        l_main_tmp.T_EXTCODE    := trim(l_main_tmp.t_extcode);
                        l_main_tmp.T_MARKETCODE := trim(l_main_tmp.t_marketcode);
                        l_main_tmp.T_PARTYCODE  := trim(l_main_tmp.T_PARTYCODE);
                        l_main_tmp.T_CODE       := trim(l_main_tmp.T_CODE);

                        if l_main_tmp.T_CODE is null
                        then
                                pr_exclude(539, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_CODE - код сделки', i, p_action);
                                raise exp_wrong_date;
                        end if;

                        if l_main_tmp.T_KIND is null or l_main_tmp.T_KIND = 0
                        then
                                pr_exclude(568, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_KIND - вид сделки', i, p_action);
                                raise exp_wrong_date;
                        else
                                l_add_tmp.tgt_existback := false;
                                l_add_tmp.tgt_objtype := 101;

                                case l_main_tmp.T_KIND
                                when 30 then
                                            l_add_tmp.tgt_existback := true;
                                            l_add_tmp.tgt_bofficekind := c_DL_SECURITYDOC;
                                when 40 then
                                            l_add_tmp.tgt_existback := true;
                                            l_add_tmp.tgt_bofficekind := c_DL_SECURITYDOC;
                                when 70 then
                                            l_add_tmp.tgt_bofficekind := c_DL_RETIREMENT;
                                            l_add_tmp.tgt_objtype     := 117;
                                when 80 then
                                            l_add_tmp.tgt_bofficekind := c_DL_RETIREMENT;
                                            l_add_tmp.tgt_objtype     := 117;
                                when 90 then
                                            l_add_tmp.tgt_bofficekind := c_DL_RETIREMENT;
                                            l_add_tmp.tgt_objtype     := 117;
                                when 100 then
                                            l_add_tmp.tgt_bofficekind := c_DL_GET_DIVIDEND;
                                when 110 then
                                            l_add_tmp.tgt_bofficekind := c_DL_NTGDOC;
                                else
                                            l_add_tmp.tgt_bofficekind := c_DL_SECURITYDOC;
                                end case;
                        end if;

                        l_main_tmp.T_DATE       := nvl( l_main_tmp.T_DATE, date'0001-01-01' );
                        l_main_tmp.T_TIME       := nvl( (l_main_tmp.T_TIME - trunc(l_main_tmp.T_TIME)) + date'0001-01-01', date'0001-01-01' );
                        l_main_tmp.T_CLOSEDATE  := nvl( l_main_tmp.T_CLOSEDATE, date'0001-01-01');
                        l_main_tmp.T_TSKIND     := trim( l_main_tmp.t_tskind);
                        l_main_tmp.T_ACCOUNTTYPE:= nvl( l_main_tmp.T_ACCOUNTTYPE, 0);
                        l_main_tmp.T_PARTYID    := case when l_main_tmp.T_PARTYID < 1 then null else l_main_tmp.T_PARTYID end;
                        l_main_tmp.T_PARTIALID  := nvl( l_main_tmp.T_PARTIALID, 0);
                        l_main_tmp.T_WARRANTID  := nvl( l_main_tmp.T_WARRANTID, 0);
                        l_main_tmp.T_AMOUNT     := nvl( l_main_tmp.T_AMOUNT, 0);

                        if l_main_tmp.T_AMOUNT is null
                        then
                                pr_exclude(553, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_AMOUNT - количество ценных бумаг', i, p_action);
                                raise exp_wrong_date;
                        end if;

                        l_main_tmp.T_PRICE      := nvl( l_main_tmp.T_PRICE, 0);
                        l_main_tmp.T_POINT      := nvl( l_main_tmp.T_POINT, 0);
                        l_main_tmp.T_COST       := nvl( l_main_tmp.T_COST, 0);
                        l_main_tmp.T_NKD        := nvl( l_main_tmp.T_NKD, 0);
                        l_main_tmp.T_TOTALCOST  := nvl( l_main_tmp.T_TOTALCOST, 0);
                        l_main_tmp.T_RATE       := nvl( l_main_tmp.T_RATE, 0);
                        l_main_tmp.T_REPOBASE   := nvl( l_main_tmp.T_REPOBASE, 0);
                        l_main_tmp.T_ISPFI_1    := nvl( l_main_tmp.T_ISPFI_1, 0);
                        l_main_tmp.T_ISPFI_2    := nvl( l_main_tmp.T_ISPFI_2, 0);
                        l_main_tmp.T_LIMIT      := nvl( l_main_tmp.T_LIMIT, 0);
                        l_main_tmp.T_CHRATE     := nvl( l_main_tmp.T_CHRATE, 0);
                        l_main_tmp.T_COUNTRY    := nvl( l_main_tmp.T_COUNTRY, 643);
                        l_main_tmp.T_CHAVR      := nvl( l_main_tmp.T_CHAVR, date'0001-01-01');
                        l_main_tmp.T_PRICE2     := nvl( l_main_tmp.T_PRICE2, 0);
                        l_main_tmp.T_COST2      := nvl( l_main_tmp.T_COST2, 0);
                        l_main_tmp.T_NKD2       := nvl( l_main_tmp.T_NKD2, 0);
                        l_main_tmp.T_TOTALCOST2 := nvl( l_main_tmp.T_TOTALCOST2, 0);
                        l_main_tmp.T_PAYDATE    := nvl( l_main_tmp.T_PAYDATE, date'0001-01-01');
                        l_main_tmp.T_SUPLDATE   := nvl( l_main_tmp.T_SUPLDATE, date'0001-01-01');
                        l_main_tmp.T_PAYDATE2   := nvl( l_main_tmp.T_PAYDATE2, date'0001-01-01');
                        l_main_tmp.T_SUPLDATE2  := nvl( l_main_tmp.T_SUPLDATE2, date'0001-01-01');

                        if l_main_tmp.T_PRICE + l_main_tmp.T_COST + l_main_tmp.T_TOTALCOST = 0
                        then
                            l_add_tmp.is_judicialoper := true;

                            if l_main_tmp.T_KIND not in (70,80,90,100,110)
                            then
                                pr_exclude(556, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_PRICE - цена за шт. ценной бумаги, не включая НКД', i, p_action);
                                raise exp_wrong_date;
                            end if;
                        end if;

                        if  not l_add_tmp.is_judicialoper
                        then
                            if l_main_tmp.T_COST = 0
                            then
                                pr_exclude(557, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_COST - стоимость ценных бумаг без НКД', i, p_action);
                                raise exp_wrong_date;
                            end if;
                            if l_main_tmp.T_TOTALCOST = 0
                            then
                                if main_tmp.t_kind <> 70
                                then
                                    pr_exclude(558, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_TOTALCOST - общая сумма сделки вкл. НКД в валюте сделки', i, p_action);
                                    raise exp_wrong_date;
                                else
                                    add_log(558, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_TOTALCOST - общая сумма сделки вкл. НКД в валюте сделки', l_main_tmp.t_instancedate);
                                end if;
                            end if;
                        end if;

                        if  l_add_tmp.tgt_existback
                        then
                            if l_main_tmp.T_COST2 = 0
                            then
                                pr_exclude(564, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_COST2 - стоимость ценных бумаг без НКД по 2-ой части РЕПО', i, p_action);
                                raise exp_wrong_date;
                            end if;
                            if l_main_tmp.T_PRICE2 = 0
                            then
                                pr_exclude(563, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_PRICE2 - цена за шт. ценной бумаги, не включая НКД по 2-ой части РЕПО', i, p_action);
                                raise exp_wrong_date;
                            end if;
                            if l_main_tmp.T_TOTALCOST2 = 0
                            then
                                pr_exclude(565, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_TOTALCOST2 - общая сумма сделки вкл. НКД в валюте сделки ао 2-ой части РЕПО', i, p_action);
                                return false;
                            end if;
                            if l_main_tmp.T_SUPLDATE2 = date'0001-01-01'
                            then
                                pr_exclude(566, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_SUPLDATE2 - плановая дата поставки в сделках', i, p_action);
                                raise exp_wrong_date;
                            end if;
                            if l_main_tmp.T_PAYDATE2 = date'0001-01-01'
                            then
                                pr_exclude(567, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_PAYDATE2 - плановая дата оплаты в сделках', i, p_action);
                                raise exp_wrong_date;
                            end if;

                        end if;

                        if l_main_tmp.T_AVOIRISSID = -20
                        then
                            date_tmp := l_main_tmp.T_PAYDATE;
                            l_main_tmp.T_PAYDATE := l_main_tmp.T_SUPLDATE;
                            l_main_tmp.T_SUPLDATE := date_tmp;
                            l_add_tmp.is_basket := true;
                            l_add_tmp.tgt_avoirissid  :=  g_fictfi;
                        end if;

                        l_main_tmp.T_PAYDATE2       := nvl( l_main_tmp.T_PAYDATE2, date'0001-01-01');
                        l_main_tmp.T_SUPLDATE2      := nvl( l_main_tmp.T_SUPLDATE2, date'0001-01-01');
                        l_main_tmp.T_CONTRNUM       := nvl( l_main_tmp.T_CONTRNUM, 0);
                        l_main_tmp.T_CONTRDATE      := nvl( l_main_tmp.T_CONTRDATE, date'0001-01-01');
                        l_main_tmp.T_COSTCHANGE     := nvl( l_main_tmp.T_COSTCHANGE, chr(0));
                        l_main_tmp.T_COSTCHANGEONCOMP := nvl( l_main_tmp.T_COSTCHANGEONCOMP, chr(0));
                        l_main_tmp.T_COSTCHANGEONAMOR := nvl( l_main_tmp.T_COSTCHANGEONAMOR, chr(0));

                        if l_main_tmp.T_PAYMCUR > 0
                        then
                            l_add_tmp.tgt_paymcur := replobj_get( c_OBJTYPE_MONEY, l_main_tmp.T_PAYMCUR).dest_id;
                        end if;

                        l_main_tmp.T_DOPCONTROL     := nvl( l_main_tmp.T_DOPCONTROL, 0);
                        l_main_tmp.T_FISSKIND       := nvl( l_main_tmp.T_FISSKIND, 0);
                        l_main_tmp.T_PRICE_CALC     := nvl( l_main_tmp.T_PRICE_CALC, 0);
                        l_main_tmp.T_PRICE_CALC_DEF := nvl( l_main_tmp.T_PRICE_CALC_DEF, 0);
                        l_main_tmp.T_PRICE_CALC_METHOD := nvl( l_main_tmp.T_PRICE_CALC_METHOD, 0);
                        l_main_tmp.T_PRICE_CALC_VAL := nvl( l_main_tmp.T_PRICE_CALC_VAL, -1);
                        l_main_tmp.T_PRICE_CALC_MET_NOTE := trim( l_main_tmp.T_PRICE_CALC_MET_NOTE);
                        l_main_tmp.T_CONDITIONS     := trim( l_main_tmp.T_CONDITIONS);
                        l_main_tmp.T_BALANCEDATE    := nvl( l_main_tmp.T_BALANCEDATE, date'0001-01-01');
                        l_main_tmp.T_ADJUSTMENT     := nvl( l_main_tmp.T_ADJUSTMENT, chr(0));
                        l_main_tmp.T_ATANYDAY       := nvl( l_main_tmp.T_ATANYDAY, chr(0));
                        l_main_tmp.T_DIV            := nvl( l_main_tmp.T_DIV, chr(0));
                        l_main_tmp.T_SUPLDATE       := nvl( l_main_tmp.T_SUPLDATE, date'0001-01-01');
                        l_main_tmp.T_SUPLDATE2      := nvl( l_main_tmp.T_SUPLDATE2, date'0001-01-01');


                        if l_main_tmp.T_PRICE_CALC_VAL = 0
                        then
                            l_main_tmp.T_PRICE_CALC_VAL := -7; -- проценты
                        else
                            l_main_tmp.T_PRICE_CALC_VAL := replobj_get( c_OBJTYPE_MONEY, l_main_tmp.T_PRICE_CALC_VAL).dest_id;
                        end if;

                        if (l_main_tmp.T_PARTYID is not null)
                        then
                            l_add_tmp.tgt_party   :=  replobj_get( c_OBJTYPE_PARTY, l_main_tmp.T_PARTYID).dest_id;
                            if  ( l_add_tmp.tgt_party = -1) and (p_action < 3 ) then
                                pr_exclude(525, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Не найден контрагент по внебиржевой сделке', i, p_action);
                                raise exp_wrong_date;
                            end if;
                        end if;

                        if ( l_main_tmp.T_DEPARTMENT = 0 )
                        then
                            l_add_tmp.tgt_department := g_department;
                        else
                            l_add_tmp.tgt_department := get_dep_code( replobj_get( c_OBJTYPE_PARTY, l_main_tmp.T_DEPARTMENT).dest_id );
                        end if;

                        if (l_main_tmp.T_KIND = 500) or (l_main_tmp.T_KIND = 510)
                        then
                                l_add_tmp.is_basket := true;
                        end if;

                        if (l_main_tmp.T_WARRANTID > 0)
                        then
                            l_warrantid   :=  replobj_get( c_OBJTYPE_WARRANT, l_main_tmp.T_WARRANTID).dest_id;
                            if  l_warrantid = -1  then
                                pr_exclude(525, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Ошибка: для погашения не найден купон (T_WARRANTID) = ' || l_main_tmp.T_WARRANTID, i, p_action);
                                raise exp_wrong_date;
                            end if;
                        end if;

                        if (l_main_tmp.T_PARTIALID > 0)
                        then
                            l_partialid   :=  replobj_get( c_OBJTYPE_PARTIAL, l_main_tmp.T_PARTIALID).dest_id;
                            if  l_partialid = -1  then
                                pr_exclude(525, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Ошибка: для погашения не найдено ЧП (T_PARTIALID) = ' || l_main_tmp.T_PARTIALID, i, p_action);
                                raise exp_wrong_date;
                            end if;
                        end if;

                        if (l_main_tmp.T_PARENTID > 0)
                        then
                            l_add_tmp.tgt_parentid   :=  replobj_get( c_OBJTYPE_DEAL, l_main_tmp.T_PARENTID).dest_id;
                            if  l_add_tmp.tgt_parentid = -1  then
                                pr_exclude(596, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Ошибка: не найдена сделка РЕПО на корзину (T_PARENTID) = ' || l_main_tmp.T_PARENTID, i, p_action);
                                raise exp_wrong_date;
                            end if;
                        end if;

                        if l_main_tmp.t_kind = 80
                        then
                            if l_main_tmp.T_WARRANTID <= 0
                            then
                                pr_exclude(548, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: для сделки погашения купона не найден купон', i, p_action);
                                raise exp_wrong_date;
                            elsif l_warrantid <= 0
                            then
                                pr_exclude(549, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не найден купон', i, p_action);
                                raise exp_wrong_date;
                            else
                                -- погашение купона - редкая операция, набираем в кэш поштучно
                                if not warrant_arr.exists( l_warrantid )
                                then
                                    begin
                                        select t_number into l_add_tmp.tgt_warrant_num from dfiwarnts_dbt where t_id = l_warrantid;
                                    exception when no_data_found
                                    then
                                        pr_exclude(549, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не найден купон в таблице dfiwarnts_dbt', i, p_action);
                                        raise exp_wrong_date;
                                    end;
                                else
                                    l_add_tmp.tgt_warrant_num := warrant_arr( l_warrantid );
                                end if;
                            end if;

                        end if;

                        if l_main_tmp.t_kind = 90
                        then
                            if l_main_tmp.T_PARTIALID <= 0
                            then
                                pr_exclude(550, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: для сделки частичного погашения облигации не найдено частичное погашение', i, p_action);
                                raise exp_wrong_date;
                            elsif l_partialid <= 0
                            then
                                pr_exclude(551, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не найдено частичное погашение', i, p_action);
                                raise exp_wrong_date;
                            else
                                -- погашение купона - редкая операция, набираем в кэш поштучно
                                if not warrant_arr.exists( l_partialid )
                                then
                                    begin
                                        select t_number into l_add_tmp.tgt_partial_num from dfiwarnts_dbt where t_id = l_partialid;
                                    exception
                                    when no_data_found then
                                        pr_exclude(549, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не найдено частичное погашение в таблице dfiwarnts_dbt', i, p_action);
                                        raise exp_wrong_date;
                                    end;
                                else
                                    l_add_tmp.tgt_partial_num := warrant_arr( l_partialid );
                                end if;
                            end if;
                        end if;

                        l_add_tmp.tgt_avoirissid  :=  replobj_get( c_OBJTYPE_AVOIRISS, l_main_tmp.t_avoirissid).dest_id;
                        if  ( l_add_tmp.tgt_avoirissid = -1) and (p_action < 3 ) and (l_main_tmp.t_avoirissid <> -20) then
                            pr_exclude(552, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не найдена ценная бумага (T_AVOIRISSID) = ' || l_main_tmp.t_avoirissid, i, p_action);
                            raise exp_wrong_date;
                        end if;

                        l_add_tmp.tgt_currencyid  :=  replobj_get( c_OBJTYPE_MONEY, l_main_tmp.t_currencyid).dest_id;
                        if  ( l_add_tmp.tgt_currencyid = -1 or nvl(l_main_tmp.t_currencyid,0) = 0) and (p_action < 3 ) then
                            pr_exclude(554, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не найдена валюта сделки (T_CURRENCYID) = ' || l_main_tmp.t_currencyid, i, p_action);
                            raise exp_wrong_date;
                        end if;

                        l_add_tmp.tgt_nkdfiid  :=  replobj_get( c_OBJTYPE_MONEY, l_main_tmp.t_nkdfiid).dest_id;
                        -- валюта НКД может быть не задана, тогда просто берем валюту номинала
                        if  ( l_add_tmp.tgt_nkdfiid = -1) and (p_action < 3 ) and (nvl(l_main_tmp.t_nkdfiid,0)>0 ) then
                            pr_exclude(554, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Ошибка: не найдена валюта НКД (T_NKDFIID) = ' || l_main_tmp.t_nkdfiid, i, p_action);
                            raise exp_wrong_date;
                        end if;

                        if l_main_tmp.T_MARKETID <= 0 and l_main_tmp.T_BROKERID <= 0
                        then
                            pr_exclude(547, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Ошибка: для внебиржевой сделки не найден контрагент, параметр T_PARTYID', i, p_action);
                            raise exp_wrong_date;
                        end if;

                        if l_main_tmp.T_MARKETID > 0  then
                            l_add_tmp.tgt_market   :=  replobj_get( c_OBJTYPE_MARKET, l_main_tmp.T_MARKETID).dest_id;
                            if  l_add_tmp.tgt_market = -1 then
                                pr_exclude(534, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Ошибка: не найден субъект T_MARKETID', i, p_action);
                                raise exp_wrong_date;
                            elsif  l_add_tmp.tgt_market = g_ourbank then
                                pr_exclude(542, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Ошибка: неверно задан параметр T_MARKETID, наш банк не может быть биржой', i, p_action);
                                raise exp_wrong_date;
                            end if;
                            l_add_tmp.tgt_ismarketoper := true;
                            add_type_to_subject( l_add_tmp.tgt_market, 'биржа' );
                        else
                            l_add_tmp.tgt_ismarketoper := false;
                        end if;

                        if l_main_tmp.T_BROKERID > 0  then
                            l_add_tmp.tgt_broker   :=  replobj_get( c_OBJTYPE_PARTY, l_main_tmp.T_BROKERID).dest_id;
                            if  l_add_tmp.tgt_broker = -1 then
                                pr_exclude(534, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Ошибка: не найден субъект T_BROKERID', i, p_action);
                                raise exp_wrong_date;
                            elsif  l_add_tmp.tgt_broker = g_ourbank then
                                pr_exclude(542, c_OBJTYPE_DEAL, l_main_tmp.t_dealid,  0, 'Ошибка: неверно задан параметр T_BROKERID, наш банк не может быть брокером', i, p_action);
                                raise exp_wrong_date;
                            end if;
                            add_type_to_subject( l_add_tmp.tgt_broker, 'брокер' );
                        end if;

                        l_add_tmp.tgt_sector  :=  replobj_get( c_OBJTYPE_SECTION, l_main_tmp.T_MARKETID, l_main_tmp.T_SECTOR).dest_id;
                        if  ( l_add_tmp.tgt_sector = -1) and ( l_main_tmp.T_SECTOR > 0 ) and (p_action > 1 ) then
                            pr_exclude(543, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не найден сектор биржи ( задано ' || l_main_tmp.T_MARKETID || ', ' || l_main_tmp.T_SECTOR || ', найдено ' || l_add_tmp.tgt_sector || ')', i, p_action);
                            raise exp_wrong_date;
                        end if;



                        l_add_tmp.tgt_repobase :=
                            case l_main_tmp.t_repobase
                            when 1 then 1
                            when 2 then 3
                            when 3 then 5
                            when 4 then 2
                            when 5 then 0
                            when 6 then 4
                            else -1
                            end;

                        l_add_tmp.is_loan_to_repo := false;

                        deb('Перекодируем страну из ' || l_main_tmp.t_country, p_level=>5);
                        if (l_main_tmp.t_country is not null) and (l_main_tmp.t_country <> '643') and (l_main_tmp.t_country <> '165')
                        then
                            l_add_tmp.tgt_country := country_arr(l_main_tmp.t_country);
                        else
                            l_add_tmp.tgt_country := null;
                        end if;

                        if l_main_tmp.t_kind in (50, 60)
                        then
                            l_add_tmp.is_loan := true;

                            if l_main_tmp.T_SUPLDATE = date'0001-01-01'
                            then
                                pr_exclude(559, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_SUPLDATE - плановая дата передачи/возврата в займе', i, p_action);
                                raise exp_wrong_date;
                            elsif l_main_tmp.T_SUPLDATE2 = date'0001-01-01'
                            then
                                pr_exclude(560, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_SUPLDATE2 - плановая дата передачи/возврата в займе', i, p_action);
                                raise exp_wrong_date;
                            end if;
                        else
                            if l_main_tmp.T_SUPLDATE = date'0001-01-01'
                            then
                                pr_exclude(561, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_SUPLDATE - плановая дата поставки в сделках', i, p_action);
                                raise exp_wrong_date;
                            elsif l_main_tmp.T_PAYDATE = date'0001-01-01'
                            then
                                pr_exclude(562, c_OBJTYPE_DEAL, l_main_tmp.t_dealid, 0, 'Ошибка: не задан параметр T_PAYDATE - плановая дата оплаты в сделках', i, p_action);
                                raise exp_wrong_date;
                            end if;
                        end if;


                        deal_sou_arr(i) := l_main_tmp;
                        deal_sou_add_arr(i) := l_add_tmp;

                    deb('Завершена процедура DEAL_CLEANING (успешно)', p_level=>5);
                    return true;
            exception when exp_wrong_date
                then
                    deb('Завершена процедура DEAL_CLEANING (по исключению)', p_level=>5);
                    return false;
            end deal_cleaning;


            -- Процедура собирает fiid всех бумаг, найденных в буфере сделок, и загружает по ним данные
            -- в буферную коллекцию avr_add_arr, индексированную fiid
            procedure  filling_avoiriss_buffer
            is
                l_tmp_arr           tmp_arr_type;
                l_avr_add_arr_tmp     avr_add_arr_type;
            begin
                -- проще взять из буфера replobj. Загружаем в вовременную коллекцию, затем переписываем в постоянную
                -- avr_add_arr, индекcируя по FIID
                deb('Запущена процедура FILLING_AVOIRISS_BUFFER');
                for j in replobj_rec_arr.first..replobj_rec_arr.last
                loop
                    if replobj_rec_arr(j).obj_type = c_OBJTYPE_AVOIRISS and replobj_rec_arr(j).DEST_ID > 0
                    then
                        l_tmp_arr( l_tmp_arr.count ) := replobj_rec_arr(j).DEST_ID;
                    end if;
                end loop;
                deb('Собрали ID бумаг, записей #1', l_tmp_arr.count);

                deb('Загружаем в буфер данные по бумагам из целевой системы');
                --#query
                select fi.t_fiid, fi.t_name, av.t_isin, fi.t_facevaluefi, -1, fi.t_avoirkind, RSB_FIINSTR.FI_IsQuoted(fi.t_fiid, p_date), RSB_FIINSTR.FI_IsKSU(fi.t_fiid), decode(RSB_SECUR.securkind(fi.T_AVOIRKIND), 17, 1, 0) , -1, -1, -1, -1
                bulk collect into l_avr_add_arr_tmp from dfininstr_dbt fi, davoiriss_dbt av where fi.t_fiid=av.t_fiid and fi.t_fiid in (select column_value from TABLE(l_tmp_arr));
                deb('Загрузили, записей #1', l_avr_add_arr_tmp.count);
                for j in 1..l_avr_add_arr_tmp.count
                loop
                    avr_add_arr( l_avr_add_arr_tmp(j).r_fiid ) := l_avr_add_arr_tmp(j);
                end loop;

                l_avr_add_arr_tmp.delete;
                l_tmp_arr.delete;
                DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
                deb('Завершена процедура FILLING_AVOIRISS_BUFFER');
            end filling_avoiriss_buffer;



            -- Добавляет платеж по NEEDDEMAND в буфер
            procedure add_auto_demands( p_main_tmp DTXDEAL_DBT%ROWTYPE, p_tick_tmp DDL_TICK_DBT%ROWTYPE, p_leg_tmp DDL_LEG_DBT%ROWTYPE)
            is
                demand_tmp  demand_type;
            begin
                deb('Запущена процедура ADD_AUTO_DEMANDS');
                -- постоянные поля
                demand_tmp.r_action         := 1;
                demand_tmp.r_oldobjectid    := p_main_tmp.t_dealid;
                demand_tmp.tgt_docid        := p_leg_tmp.t_dealid;
                demand_tmp.tgt_dockind      := p_tick_tmp.T_BOFFICEKIND;
                demand_tmp.r_isauto         := true;
                demand_tmp.r_isfact         := true;
                demand_tmp.r_destsubobjnum  := 1;  -- непонятно, зачем. так в исходном алгоритме

                if p_tick_tmp.t_marketid > 0
                then
                    demand_tmp.tgt_party := p_tick_tmp.t_marketid;
                else
                    demand_tmp.tgt_party := p_tick_tmp.t_partyid;
                end if;
                demand_tmp.r_note   := chr(1);
                demand_tmp.r_state  := 3;
                if p_leg_tmp.t_legkind=0
                then
                    demand_tmp.r_part   := 1;
                else
                    demand_tmp.r_part   := 2;
                end if;

                -- платеж по бумагам ------------------------------------------------------------------
                if  p_main_tmp.t_kind not in (80,90,110) and p_tick_tmp.T_PFI <> c_DEFAULT_FICTFI
                then
                    demand_tmp.r_fikind := 20; -- вид инструменты - ц/б
                    demand_tmp.r_kind   := 10; -- поставка ц/б
                    demand_tmp.tgt_fiid := p_leg_tmp.t_pfi;
                    if p_leg_tmp.T_MATURITYISPRINCIPAL = chr(88)
                    then
                        demand_tmp.r_date   := p_leg_tmp.t_maturity;
                    else
                        demand_tmp.r_date   := p_leg_tmp.t_expiry;
                    end if;

                    if p_main_tmp.t_kind = 10 or (p_main_tmp.t_kind in (30,60) and  p_leg_tmp.t_legkind=0)
                    then
                        demand_tmp.r_direction := 1; -- наше требование
                    else
                        demand_tmp.r_direction := 2; -- наше обязательство
                    end if;
                    demand_tmp.r_sum    := p_leg_tmp.t_principal;
                    demand_tmp.r_result := 0;

                    if demand_tmp.r_part = 1
                    then
                        demand_tmp.r_subobjnum  := 81;
                    else
                        demand_tmp.r_subobjnum  := 82;
                    end if;

                    add_demand( demand_tmp );
                end if;

                -- платеж по деньгам ------------------------------------------------------------------
                demand_tmp.r_fikind := 10;
                demand_tmp.r_kind   := 40;
                demand_tmp.tgt_fiid := p_leg_tmp.t_cfi;
                if p_leg_tmp.T_MATURITYISPRINCIPAL = chr(88)
                then
                    demand_tmp.r_date   := p_leg_tmp.t_expiry;
                else
                    demand_tmp.r_date   := p_leg_tmp.t_maturity;
                end if;
                demand_tmp.r_sum    := p_leg_tmp.t_totalcost;
                if p_main_tmp.t_kind = 10 or (p_main_tmp.t_kind in (30,60) and  p_leg_tmp.t_legkind=0)
                then
                    demand_tmp.r_direction := 2; -- наше обязательство
                else
                    demand_tmp.r_direction := 1; -- наше требование
                end if;
                demand_tmp.r_sum    := p_leg_tmp.t_totalcost;
                demand_tmp.r_result := 0;

                if demand_tmp.r_part = 1
                    then
                        demand_tmp.r_subobjnum  := 83;
                    else
                        demand_tmp.r_subobjnum  := 84;
                end if;

                add_demand( demand_tmp );
                deb('Завершена процедура ADD_AUTO_DEMANDS');
            end add_auto_demands;
            
            procedure fict_start
            is begin null;
            end;


--================================================================================================================
--================================================================================================================
--================================================================================================================
--================================================================================================================
    begin
        deb_empty('=');
        procedure_start_date := sysdate;
        
        -- Если SESSION_ID был не зназначен, то есть, LOAD_DEALS вызвали вручную, назначим его
        if g_SESSION_ID is null then
            insert into dtx_session_dbt(t_startdate, t_enddate, t_user, t_status)
            values( p_date, p_date+1-1/24/60/60, user, 'R') returning t_sessid into g_SESSION_ID;
        end if;
        
        insert into dtx_sess_detail_dbt( T_SESSID, T_PROCEDURE, T_INSTANCEDATE, T_STARTDATE)
        values (g_SESSION_ID, 'LOAD_DEALS', p_date, sysdate) returning t_detailid into g_SESS_DETAIL_ID;
        
        commit;
        
        
        l_err_counter := 0;
        l_counter := 0;
        deb('Инициализация буфера DBMS_OUTPUT');
        DBMS_OUTPUT.ENABLE (buffer_size => NULL);

        deb('Запущена процедура LOAD_DEALS за ' || to_char(p_date, 'dd.mm.yyyy') || ', тип действия ' || p_action);
        g_debug_level_current := -1;
        
        open m_cur(p_date, p_action);
        loop

            -- загрузка порции данных
            fetch m_cur bulk collect into deal_sou_arr limit g_limit;
            exit when deal_sou_arr.count=0;
            good_deals_count := m_cur%rowcount;
            -- устанавливаем диапазон регистрируемых записей для лога. Если в выборке < 20 записей, считаем ее отладочной и выводим полную информацию.
            if g_debug_level_current = -1
            then
                if good_deals_count < 20
                then
                    g_debug_level_current := 10;
                else
                    g_debug_level_current := g_debug_level_limit;
                end if;
            end if;
            deb('Загружены данные из DTXDEAL_DBT, #1 строк', m_cur%rowcount);
            deb('Установлен уровень логирования: #1', g_debug_level_current);

            -- регистрируем все сущности для загрузки из REPLOBJ
            deb_empty('=');
            tmp_arr.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
            
                        
            deb('Цикл 1 - регистрация кодов в буфере REPLOBJ');
            
            for i in deal_sou_arr.first .. deal_sou_arr.last
            loop

                -- SGS TODO  Убрать в процедуру

                main_tmp    := deal_sou_arr(i);

                if nvl(main_tmp.t_techtype, 0) <> 0
                then
                    deb('Обнаружена техническая сделка, dealid = #1', main_tmp.t_dealid);
                                                add_log(527, 80, main_tmp.t_dealid,  0, 'Предупреждение: техническая сделка в систему не реплицируется', main_tmp.t_instancedate);
                                                pr_include( i );
                                                continue;
                end if;

                -- собираем уникальные fiid
                replobj_add( c_OBJTYPE_MONEY, main_tmp.t_paymcur, 0 ); -- надо выяснить, зачем валютам subobjnum
                replobj_add( c_OBJTYPE_MONEY, main_tmp.t_currencyid, 0 );
                replobj_add( c_OBJTYPE_AVOIRISS, main_tmp.t_avoirissid );

                -- собираем уникальные MARKETID
                replobj_add( c_OBJTYPE_MARKET, main_tmp.T_MARKETID, p_comment => 'торговая площадка');
                replobj_add( c_OBJTYPE_SECTION, main_tmp.T_MARKETID, main_tmp.T_SECTOR);

                -- собираем уникальные BROKERID
                replobj_add( c_OBJTYPE_PARTY, main_tmp.T_BROKERID, p_comment => 'торговая площадка');

                -- собираем уникальные PARTYID
                replobj_add( c_OBJTYPE_PARTY, main_tmp.T_PARTYID, p_comment => 'торговая площадка');



                -- собственно, сделка
                if p_action > 1 then
                    replobj_add( c_OBJTYPE_DEAL, main_tmp.T_DEALID);
                end if;

                if main_tmp.t_department is not null
                then
                    -- Департамент. Если не указан, заменится константой g_department
                    replobj_add( c_OBJTYPE_PARTY, main_tmp.t_department);
                end if;

                if main_tmp.t_parentid is not null
                then
                    -- собственно, сделка
                    replobj_add( c_OBJTYPE_DEAL, main_tmp.T_PARENTID);
                end if;

                if main_tmp.t_partialid is not null
                then
                    -- если сделка - частичное погашение, регистрируем ЧП
                    replobj_add( c_OBJTYPE_PARTIAL, main_tmp.T_PARTIALID);
                end if;

                if main_tmp.t_warrantid is not null
                then
                    -- если сделка - погашение купона, регистрируем купон
                    replobj_add( c_OBJTYPE_WARRANT, main_tmp.T_WARRANTID);
                end if;


            end loop;
            deb('Собрали данные в буфер REPLOBJ, #1 записей', replobj_rec_arr.count);

            -- заполняем кэш из REPLOBJ -----------------------------------------------------------------
            replobj_load;

            -- очищаем основную коллекцию и начинаем заполнять дополнительную ---------------------------
            deb_empty;
            deb_empty('=');
            deb('Цикл 2 - перекодирование, проверка и очистка');
            
            for i in deal_sou_arr.first .. deal_sou_arr.last
            loop

                if not deal_cleaning(i)
                then
                        continue;
                end if;

            end loop;
            deb('Цикл 2 завершен');
            
            deb_empty('=');
            deb('Заполняем буферы из целевой системы');
            -- заполняем буфер бумаг ---------------------------------------------------------------------

            filling_avoiriss_buffer;

            deb('Бумаги загружены, буфер подготовлен, временные массивы очищены. #1 записей в буфере бумаг', avr_add_arr.count);
            ----------------------------------------------------------------------------------------------
            -- SGS TODO Убрать в процедуру

            -- заполняем буфер сделок
            -- для операций вставки нас будет интересовать, есть ли уже сделка с таким же T_CODE.
            -- Только эту информацию и достаем.
            -- Для операций обновления-удаления надо загрузить текущую сделку из целевой системы по DEALID из REPLOBJ

            -- Cначала надо вытащить DEALCODE и DEALID в более удобные коллекции
            -- индексируем элементы по индексам deal_sou_add_arr, вдруг пригодится
            deb_empty;
            deb('Собираем данные по сделкам в буфер');
            for j in deal_sou_add_arr.first .. deal_sou_add_arr.last
            loop
                if  main_tmp.t_action > 1  -- предполагаю убрать p_action из параметров, поэтому здесь, а не вокруг цикла
                then
                    tmp_dealids(j).T_DEALID := deal_sou_add_arr(j).tgt_dealid;
                    tmp_dealids(j).T_BOFFICEKIND := deal_sou_add_arr(j).tgt_bofficekind;
                    deb('tmp_dealids(#1).T_DEALID  =  #2, tmp_dealids(#1).T_DEALID  =  #3', j, tmp_dealids(j).T_DEALID, tmp_dealids(j).T_BOFFICEKIND);
                else
                    tmp_dealcodes_in(j).INDEX_NUM   := j;  -- привязаться по коду сделки мы не можем, придется тащить с собой этот индекс
                    tmp_dealcodes_in(j).T_DEALCODE  := main_tmp.t_code;
                    deb('tmp_dealcodes_in(#1).INDEX_NUM  =  #2, tmp_dealcodes_in(#1).T_DEALCODE  =  #3', j, tmp_dealcodes_in(j).INDEX_NUM, tmp_dealcodes_in(j).T_DEALCODE, p_level=>5);
                end if;
            end loop;
            deb('Записей в tmp_dealids - #1, записей в tmp_dealcodes_in - #2', tmp_dealids.count, tmp_dealcodes_in.count);
            deb('Собрали коды и ID сделок из загрузки. Загружаем из целевой системы..');


            -- теперь загружаем в кэш сделки из целевой системы --
            -- для операции вставки, сделки по кодам --- нас интересует только факт наличия сделок ---------------------------
            if tmp_dealcodes_in.count > 0
            then
                --#query
                select arr.INDEX_NUM, tk.t_dealcode bulk collect into tmp_dealcodes_out from ddl_tick_dbt tk, (select INDEX_NUM, T_DEALCODE from table(tmp_dealcodes_in)) arr where tk.t_dealcode=arr.T_DEALCODE;
                deb('Загружено сделок по кодам (DTXDEAL.T_CODE = DDL_TICK_DBT.T_DEALCODE) #1 записей из #2', tmp_dealcodes_out.count, tmp_dealcodes_in.count, p_level => 3);
                -- переносим в дополнительную коллекцию
                for j in 1 .. nvl(tmp_dealcodes_out.last,0)
                loop
                    deal_sou_add_arr( tmp_dealcodes_out(j).INDEX_NUM ).is_matched_by_code := true;
                end loop;
                tmp_dealcodes_in.delete;
                tmp_dealcodes_out.delete;
                DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
            end if;

            -- по dealid ---------------------------------------------------------
            -- тикеты сделок, загружаем во внутренний буфер
            --#query
            select tk.* bulk collect into tmp_ddl_tick_dbt_arr_in from ddl_tick_dbt tk, (select * from table(tmp_dealids)) arr where tk.t_dealid=arr.t_dealid and tk.t_bofficekind=arr.t_bofficekind and rownum < p_emergency_limit;
            deb('Загружено сделок (DDL_TICK_DBT) по ID (DTXREPLOBJ.T_DESTID = DDL_TICK_DBT.T_DEALID) #1 записей из #2', tmp_ddl_tick_dbt_arr_in.count, tmp_dealids.count, p_level => 3);
            if sql%rowcount = p_emergency_limit then
                deb('Ошибка! Превышение лимита при загрузке сделок');
            end if;
            -- переносим в дополнительную коллекцию ради индексации
            for j in 1..tmp_ddl_tick_dbt_arr_in.count
            loop
                dealid_tmp := tmp_ddl_tick_dbt_arr_in(j).t_dealid;
                tmp_sou_id := deal_sou_back( dealid_tmp );
                deal_sou_add_arr( tmp_sou_id ).DDL_TICK_BUF := tmp_ddl_tick_dbt_arr_in(j);
                deal_sou_add_arr( tmp_sou_id ).TGT_DEALID := dealid_tmp;
            end loop;
            -- временный буфер больше не нужен
            tmp_ddl_tick_dbt_arr_in.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;


            -- группы сделок
            --#query
            select dealid, gr, RSB_SECUR.IsBuy(gr), RSB_SECUR.IsSale(gr), RSB_SECUR.IsLoan(gr), RSB_SECUR.IsRepo(gr) bulk collect into tmp_dealogroup_arr from
            (select tk.t_Dealid dealid, RSB_SECUR.get_OperationGroup( op.t_systypes ) gr from ddl_tick_dbt tk, doprkoper_dbt op where (tk.t_dealid, tk.t_bofficekind) in (select t_dealid, t_bofficekind from table(tmp_dealids)) and rownum < p_emergency_limit
            and op.T_KIND_OPERATION = tk.T_DEALTYPE and op.T_DOCKIND = tk.T_BOFFICEKIND);
            deb('Загружено сделок (DDL_TICK_DBT) по ID (DTXREPLOBJ.T_DESTID = DDL_TICK_DBT.T_DEALID) #1 записей из #2', tmp_ddl_tick_dbt_arr_in.count, tmp_dealids.count, p_level => 3);
            if sql%rowcount = p_emergency_limit then
                deb('Ошибка! Превышение лимита при загрузке сделок');
            end if;
            -- переносим в дополнительную коллекцию
            for j in 1 .. nvl(tmp_dealogroup_arr.last, 0)
            loop
                dealid_tmp := tmp_dealogroup_arr(j).r_dealid;
                tmp_sou_id := deal_sou_back( dealid_tmp );
                deal_sou_add_arr( tmp_sou_id ).TGT_OGROUP   := tmp_dealogroup_arr(j).r_group;
                deal_sou_add_arr( tmp_sou_id ).is_buy   := CASE tmp_dealogroup_arr(j).r_isbuy     WHEN 1 THEN true ELSE false end;
                deal_sou_add_arr( tmp_sou_id ).is_sale  := CASE tmp_dealogroup_arr(j).r_issale    WHEN 1 THEN true ELSE false end;
                deal_sou_add_arr( tmp_sou_id ).is_loan  := CASE tmp_dealogroup_arr(j).r_isloan    WHEN 1 THEN true ELSE false end;
                deal_sou_add_arr( tmp_sou_id ).is_repo  := CASE tmp_dealogroup_arr(j).r_isrepo    WHEN 1 THEN true ELSE false end;
            end loop;
            tmp_arr1.delete;
            tmp_arr.delete;
            tmp_dealogroup_arr.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
            
            -- первая нога
            --#query
            select * bulk collect into tmp_ddl_leg_dbt_arr_in  from ddl_leg_dbt where t_dealid in (select t_dealid from table(tmp_dealids)) and t_legkind=0 and rownum < p_emergency_limit ;
            deb('Загружено сделок (DDL_LEG_DBT, часть 1) по ID (DTXREPLOBJ.T_DESTID = DDL_LEG_DBT.T_DEALID) #1 записей из #2', tmp_ddl_leg_dbt_arr_in.count, tmp_dealids.count, p_level => 3);
            if sql%rowcount = p_emergency_limit then
                deb('Ошибка! Превышение лимита при загрузке сделок');
            end if;

            for j in  1 .. nvl(tmp_ddl_leg_dbt_arr_in.last, 0)
            loop
                dealid_tmp := tmp_ddl_leg_dbt_arr_in(j).t_dealid;
                tmp_sou_id := deal_sou_back( dealid_tmp ); -- получаем индекс во входной коллекции по dealid из целевой системы
                deal_sou_add_arr( tmp_sou_id ).DDL_LEG1_BUF := tmp_ddl_leg_dbt_arr_in(j); -- сохраняем ногу в дополнительную коллекцию
            end loop;
            tmp_ddl_leg_dbt_arr_in.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;

            -- вторая нога
            --#query
            select * bulk collect into tmp_ddl_leg_dbt_arr_in  from ddl_leg_dbt where t_dealid in (select t_dealid from table(tmp_dealids)) and t_legkind=2 and rownum < p_emergency_limit ;
            deb('Загружено сделок (DDL_LEG_DBT, часть 2) по ID (DTXREPLOBJ.T_DESTID = DDL_LEG_DBT.T_DEALID) #1 записей из #2', tmp_ddl_leg_dbt_arr_in.count, tmp_dealids.count, p_level => 3);
            if sql%rowcount = p_emergency_limit then
                deb('Ошибка! Превышение лимита при загрузке сделок');
            end if;

            for j in 1 .. nvl(tmp_ddl_leg_dbt_arr_in.last, 0)
            loop
                dealid_tmp := tmp_ddl_leg_dbt_arr_in(j).t_dealid;
                tmp_sou_id := deal_sou_back( dealid_tmp );  -- получаем индекс во входной коллекции по dealid из целевой системы
                deal_sou_add_arr( tmp_sou_id ).DDL_LEG2_BUF := tmp_ddl_leg_dbt_arr_in(j); -- сохраняем ногу в дополнительную коллекцию
            end loop;
            tmp_ddl_leg_dbt_arr_in.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;

            deb('Запись в буфер данных из DDL_TICK и DDL_LEG завершена. Все буферы загружены. Вспомогательные массивы очищены.');
            deb_empty('=');
            ---------------------------------------------------------------------------------------

            deb('Цикл 3. Проверка наличия сделок в системе');
            
            -- 694 стр.
            for i in deal_sou_arr.first .. deal_sou_arr.last
            loop
                if deal_sou_add_arr(i).result = 2
                then continue;
                end if;

                main_tmp := deal_sou_arr(i);
                add_tmp  := deal_sou_add_arr(i);
                change_flag := false;

                if  main_tmp.t_action = 1
                then
                    if add_tmp.is_matched_by_code
                    then
                        deb('Ошибка: сделка #1 уже существует в целевой системе. Сделка исключена из обработки', main_tmp.t_dealid);
                        pr_exclude(421, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: сделка уже существует в целевой системе', i, main_tmp.t_action);
                        continue;
                    end if;

                elsif main_tmp.t_action = 2
                then
                    if add_tmp.DDL_TICK_BUF.T_DEALID is null
                    then
                        pr_exclude(422, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: сделка не найдена в целевой системе', i, main_tmp.t_action);
                        continue;
                    end if;

                    if add_tmp.tgt_bofficekind <> add_tmp.DDL_TICK_BUF.T_BOFFICEKIND
                    then
                        pr_exclude(547, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: при обновлении не совпадает переданный вид первичного документа сделки с таковым уже введенной сделки', i, main_tmp.t_action);
                        continue;
                    end if;

                    -- проверяем необходимость трансформации займа в операцию РЕПО
                    if add_tmp.is_loan and main_tmp.T_KIND in (30,40)
                    then
                        IF      main_tmp.T_KIND <> 30 and add_tmp.is_buy
                            THEN deb('Предупреждение: сделку "Займ, привлечение" нельзя трансформировать в "РЕПО, продажа"');
                        elsif   main_tmp.T_KIND <> 40 and add_tmp.is_sale
                            THEN deb('Предупреждение: сделку "Займ, размещение" нельзя трансформировать в "РЕПО, покупка"');
                        END IF;
                        deal_sou_add_arr(i).is_loan_to_repo := true;
                    end if;

                    if add_tmp.DDL_LEG1_BUF.T_DEALID is null
                    then
                        pr_exclude(650, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: не найдена запись с условиями сделки (ddl_leg_dbt)', i, main_tmp.t_action);
                        continue;
                    end if;

                    if add_tmp.tgt_existback and not add_tmp.is_loan_to_repo and (add_tmp.DDL_LEG2_BUF.T_DEALID is NULL)
                    then
                        pr_exclude(651, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: не найдена запись с условиями второй части сделки (ddl_leg_dbt)', i, main_tmp.t_action);
                        continue;
                    end if;

                elsif main_tmp.t_action = 3
                then
                    if add_tmp.DDL_TICK_BUF.T_DEALID is null
                    then
                        pr_exclude(423, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: сделка уже удалена в целевой системе', i, main_tmp.t_action);
                        continue;
                    end if;
                end if;

                -- для погашений облигации, купонов облигации проверяем номинал. Раньше не было инфо по бумагам.
                if add_tmp.tgt_BofficeKind = c_DL_RETIREMENT and avr_add_arr( add_tmp.tgt_avoirissid ).r_isbond = 1
                then
                    if main_tmp.t_kind in (70,90)
                    then
                        if avr_add_arr( add_tmp.tgt_avoirissid ).r_current_nom <= 0
                        then
                            pr_exclude(423, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Не определена величина номинала ц/б', i, main_tmp.t_action);
                            continue;
                        end if;
                    end if;
                end if;

            end loop;
            -- завершено



            deb_empty('=');
            -- заполняем структуры DDL_TICK и DDL_LEG. Пока на вставку будет оптимизация в виде FORALL, изменение и удаление будут идти по одной операции.
            -- Предполагаю, что их будет несравнимо меньше. Если что, всегда можно дописать.
            deb('Цикл 4. Формирование записей в целевой системе');
            
            for i in deal_sou_arr.first .. deal_sou_arr.last
            loop
                if deal_sou_add_arr(i).result = 2
                then continue;
                end if;
                -- заполняем временные записи, потом перенесем в коллекцию 
                
                main_tmp := deal_sou_arr(i);
                add_tmp  := deal_sou_add_arr(i);

                tick_tmp := null;
                leg1_tmp := null;
                leg2_tmp := null;

                if main_tmp.t_action in (2,3)
                then
                    tick_tmp.t_dealid := add_tmp.DDL_TICK_BUF.T_DEALID;
                end if;

                tick_tmp.t_dealstatus := 2;     tick_tmp.t_oper := g_oper;      tick_tmp.t_points := 4;
                tick_tmp.t_partyid := -1;       tick_tmp.t_clientid := -1;      tick_tmp.t_marketid := -1;
                tick_tmp.t_brokerid := -1;      tick_tmp.t_traderid := -1;      tick_tmp.t_depositid := -1;
                tick_tmp.t_buygoal := 0;  -- SGS заменить на BUYGOAL_RESALE

                -- определяем T_kind сделки. Нельзя сделать это в овремя очистки - нужны параметры бумаги, которые загружаются после.
                --tick_tmp.t_dealtype  := GetDealKind( main_tmp.t_kind,  add_tmp);


                tick_tmp.T_DealCodeTS := main_tmp.T_EXTCODE;
                tick_tmp.T_DealCode := main_tmp.T_CODE;
                tick_tmp.T_bofficekind := add_tmp.tgt_bofficekind;
                tick_tmp.t_dealdate := main_tmp.T_DATE;
                tick_tmp.t_regdate := main_tmp.T_DATE;
                tick_tmp.t_dealtime := main_tmp.t_time;
                tick_tmp.t_closedate := main_tmp.t_closedate;
                tick_tmp.t_typedoc := main_tmp.t_tskind;
                tick_tmp.t_portfolioid := main_tmp.T_PORTFOLIOID;
                tick_tmp.t_marketid := add_tmp.tgt_market;
                tick_tmp.t_marketofficeid := add_tmp.tgt_sector;
                tick_tmp.t_brokerid := add_tmp.tgt_broker;
                tick_tmp.t_partyid := add_tmp.tgt_party;

                tick_tmp.t_country := add_tmp.tgt_country;

                if add_tmp.tgt_market > 0 then
                    tick_tmp.t_flag1 := chr(88);
                end if;

                tick_tmp.t_department := add_tmp.tgt_department;
                tick_tmp.t_pfi := add_tmp.tgt_avoirissid;

                -- если это не операция займа, переопределим тип портфеля в зависимости от котируемости бумаг
                if not add_tmp.is_loan
                then
                    if avr_add_arr( tick_tmp.t_pfi ).r_is_quoted = 1
                    then
                        tick_tmp.t_portfolioid := c_KINDPORT_TRADE;
                        if add_tmp.tgt_existback then
                            tick_tmp.t_portfolioid_2 := c_KINDPORT_TRADE;
                        end if;
                    else
                        tick_tmp.t_portfolioid := c_KINDPORT_INVEST;
                        if add_tmp.tgt_existback then
                            tick_tmp.t_portfolioid_2 := c_KINDPORT_INVEST;
                        end if;
                    end if;
                end if;


                case main_tmp.t_kind
                when 80 then
                            tick_tmp.t_number_coupon := add_tmp.tgt_warrant_num;
                when 90 then
                            tick_tmp.t_number_partly := add_tmp.tgt_partial_num;
                else null;
                end case;

                if main_tmp.T_COSTCHANGE = CHR(88)
                then
                    tick_tmp.t_returnincomekind := 2;
                    tick_tmp.t_flag5 := CHR(88);
                end if;

                -- заполняем ценовые условия по первой части сделки


                CASE main_tmp.T_ACCOUNTTYPE
                when 1 then
                    leg1_tmp.t_formula := 50;  --"DVP"
                when 2 then
                    leg1_tmp.t_formula := 49;  --"DFP"
                when 3 then
                    leg1_tmp.t_formula := 52;  --"PP"
                when 4 then
                    leg1_tmp.t_formula := 51;  --"PD"
                else null;
                end case;

                leg1_tmp.t_pfi := add_tmp.tgt_avoirissid;
                leg1_tmp.t_cfi := add_tmp.tgt_currencyid;

                if avr_add_arr( leg1_tmp.t_pfi ).r_isbond = 1 and tick_tmp.t_bofficekind <> c_DL_RETIREMENT
                then
                    leg1_tmp.T_RELATIVEPRICE := chr(88);
                else
                    leg1_tmp.T_RELATIVEPRICE := chr(0);
                end if;


                leg1_tmp.t_principal := main_tmp.t_amount;
                leg1_tmp.T_PAYFIID := add_tmp.tgt_CURRENCYID;

                if main_tmp.t_price > 0
                then
                    leg1_tmp.t_price := main_tmp.t_price;
                end if;

                curnom_tmp := avr_add_arr( add_tmp.tgt_avoirissid ).r_current_nom;

                -- для погашений облигации, купонов облигации цена определяется иначе
                if add_tmp.tgt_BofficeKind = c_DL_RETIREMENT and avr_add_arr( add_tmp.tgt_avoirissid ).r_isbond = 1
                then
                    if main_tmp.t_kind in (70,90)
                    then
                        leg1_tmp.t_price := curnom_tmp;
                    elsif main_tmp.t_kind = 80
                    then
                        leg1_tmp.t_price := 0;
                    else
                        leg1_tmp.t_price := -13;  -- индикатор ошибки
                    end if;
                end if;

                leg1_tmp.t_scale := 1;
                if main_tmp.t_point > 0
                then
                    leg1_tmp.t_point := main_tmp.t_point;
                else
                    leg1_tmp.t_point := 4;
                end if;


                leg1_tmp.t_cost := main_tmp.t_cost;
                if not add_tmp.is_judicialoper
                then
                    if leg1_tmp.t_relativeprice = chr(88)
                    then
                            if Round(main_tmp.T_COST) <> Round(leg1_tmp.T_PRICE * curnom_tmp/100 * main_tmp.T_AMOUNT)
                            then
                                deb('Предупреждение: стоимость (T_COST) не равна произведению цены (T_PRICE) в валюте на количество бумаг (T_AMOUNT)');
                                add_log( 500,c_OBJTYPE_DEAL, main_tmp.t_dealid, 0, 'Предупреждение: стоимость (T_COST) не равна произведению цены (T_PRICE) в валюте на количество бумаг (T_AMOUNT)', main_tmp.t_instancedate);
                            end if;
                    else
                            if Round(main_tmp.T_COST) <> Round(leg1_tmp.T_PRICE * main_tmp.T_AMOUNT)
                            then
                                deb('Предупреждение: стоимость (T_COST) не равна произведению цены (T_PRICE) на количество бумаг (T_AMOUNT)');
                                add_log( 500,c_OBJTYPE_DEAL, main_tmp.t_dealid, 0, 'Предупреждение: стоимость (T_COST) не равна произведению цены (T_PRICE) на количество бумаг (T_AMOUNT)', main_tmp.t_instancedate);
                            end if;
                    end if;
                end if;

                leg1_tmp.t_nkd := main_tmp.t_nkd;
                leg1_tmp.t_totalcost := main_tmp.t_totalcost;
                leg1_tmp.t_incomerate := main_tmp.t_rate;
                leg1_tmp.t_IncomeScale := 1;
                leg1_tmp.t_IncomePoint := 2;


                if add_tmp.is_loan
                then
                    leg1_tmp.t_maturityIsPrincipal  := chr(88);
                    leg1_tmp.t_maturity             := main_tmp.T_SUPLDATE;
                    leg1_tmp.t_expiry               := main_tmp.T_SUPLDATE2;
                    -- так как в займе нет второго dl_leg, для займов вторая сумма НКД заносится в специфическое поле - ReceiptAmount
                    leg1_tmp.t_ReceiptAmount        := main_tmp.T_NKD2;

                end if;

                if main_tmp.T_KIND in (80,90) -- для погашений и ЧП
                then
                    leg1_tmp.t_MaturityIsPrincipal  := CHR(88);
                    leg1_tmp.t_Expiry               := main_tmp.T_BALANCEDATE;
                    leg1_tmp.t_Maturity             := main_tmp.T_BALANCEDATE;
                else
                    if main_tmp.T_SUPLDATE < main_tmp.T_PAYDATE
                    then
                        leg1_tmp.t_MaturityIsPrincipal  := CHR(88);
                        leg1_tmp.t_Maturity             := main_tmp.T_SUPLDATE;
                        leg1_tmp.t_Expiry               := main_tmp.T_PAYDATE;

                    else
                        leg1_tmp.t_MaturityIsPrincipal  := CHR(0);
                        leg1_tmp.t_Maturity             := main_tmp.T_PAYDATE;
                        leg1_tmp.t_Expiry               := main_tmp.T_SUPLDATE;
                    end if;
                end if;

                leg1_tmp.t_legkind   :=  0;
                leg1_tmp.t_LegID     :=  0;
                leg1_tmp.t_BASIS     :=  0;
                if main_tmp.T_REPOBASE > 0
                then
                    leg1_tmp.t_BASIS := main_tmp.T_REPOBASE - 1;
                end if;


                -- вторая часть сделки
                if add_tmp.tgt_existback
                then
                    leg2_tmp.t_legkind      := 2;
                    leg2_tmp.t_pfi          := add_tmp.tgt_avoirissid;
                    leg2_tmp.t_principal    := main_tmp.t_amount;
                    leg2_tmp.T_CFI          := main_tmp.t_CURRENCYID;
                    leg2_tmp.T_Price        := main_tmp.t_PRICE2;
                    leg2_tmp.t_cost         := main_tmp.t_cost2;
                    leg2_tmp.T_NKD          := main_tmp.T_NKD2;
                    leg2_tmp.T_TotalCost    := main_tmp.T_TOTALCOST2;
                    leg2_tmp.T_Formula      := leg1_tmp.t_Formula;
                    leg2_tmp.T_NKDFIID      := leg1_tmp.t_NKDFIID;
                    leg2_tmp.T_PAYFIID      := leg1_tmp.t_PAYFIID;
                    leg2_tmp.T_Scale        := leg1_tmp.T_Scale;
                    leg2_tmp.T_Point        := leg1_tmp.T_Point;
                    leg2_tmp.T_IncomeRate   := leg1_tmp.T_IncomeRate;
                    leg2_tmp.T_IncomeScale  := leg1_tmp.T_IncomeScale;
                    leg2_tmp.T_IncomePoint  := leg1_tmp.T_IncomePoint;
                    leg2_tmp.T_RELATIVEPRICE := leg1_tmp.T_RELATIVEPRICE;
                    leg2_tmp.t_legkind      :=  2;
                    leg2_tmp.t_LegID        :=  0;
                    leg2_tmp.t_BASIS        :=  leg1_tmp.t_BASIS;

                    if main_tmp.T_SUPLDATE2 < main_tmp.T_PAYDATE2
                    then
                        leg2_tmp.T_MaturityIsPrincipal := CHR(88);
                        leg2_tmp.T_Maturity := main_tmp.T_SUPLDATE2;
                        leg2_tmp.T_Expiry   := main_tmp.T_PAYDATE2;
                    else
                        leg2_tmp.T_MaturityIsPrincipal := CHR(0);
                        leg2_tmp.T_Maturity := main_tmp.T_PAYDATE2;
                        leg2_tmp.T_Expiry   := main_tmp.T_SUPLDATE2;
                    end if;

                    if leg2_tmp.t_relativeprice = chr(88)
                        then
                            if Round(leg2_tmp.t_cost) <> Round(leg2_tmp.T_Price * curnom_tmp/100 * main_tmp.T_AMOUNT)
                            then
                                deb('Предупреждение: стоимость по второй части сделки (T_COST2) не равна произведению цены (T_PRICE2) в валюте на количество бумаг (T_AMOUNT)', p_level=>5);
                                add_log( 500,c_OBJTYPE_DEAL, main_tmp.t_dealid, 0, 'Предупреждение: стоимость по второй части сделки (T_COST2) не равна произведению цены (T_PRICE2) в валюте на количество бумаг (T_AMOUNT)', main_tmp.t_instancedate);
                            end if;
                        else
                            if Round(leg2_tmp.t_cost) <> Round(leg2_tmp.T_Price * main_tmp.T_AMOUNT)
                            then
                                deb('Предупреждение: стоимость по второй части сделки (T_COST2) не равна произведению цены (T_PRICE2) на количество бумаг (T_AMOUNT)', p_level=>5);
                                add_log( 500,c_OBJTYPE_DEAL, main_tmp.t_dealid, 0, 'Предупреждение: стоимость по второй части сделки (T_COST2) не равна произведению цены (T_PRICE2) на количество бумаг (T_AMOUNT)', main_tmp.t_instancedate);
                            end if;
                    end if;

                end if;

                --------------------------------------------------------------------------------------
                -- заполняем структуры DDL_TICK и DDL_LEG. Пока на вставку будет оптимизация в виде FORALL, а изменение и удаление будут идти по одной операции.
                -- Предполагаю, что их будет несравнимо меньше. Если что, всегда можно дописать.
                if main_tmp.t_action = 1
                then
                    deb('Запись в буферы DDL_TICK_DBT и DDL_LEG_DBT записей на вставку', p_level=>5);
                    tick_tmp.t_dealid := ddl_tick_dbt_seq.nextval; -- новый dealid
                    leg1_tmp.t_dealid := tick_tmp.t_dealid;

                    ddl_tick_dbt_arr_out( ddl_tick_dbt_arr_out.count ) := tick_tmp;
                    ddl_leg_dbt_arr_out( ddl_leg_dbt_arr_out.count ) := leg1_tmp;

                    if add_tmp.tgt_existback then
                        leg2_tmp.t_dealid := tick_tmp.t_dealid;
                        ddl_leg2_dbt_arr_out( ddl_leg_dbt_arr_out.count ) := leg2_tmp;
                    end if;

                    deb('Запись DEALID = #1 сохранена', tick_tmp.t_dealid, p_level=>5);

                    -- добавим запись в буфер таблицы договоров
                    if main_tmp.T_CONTRNUM <> 0 and main_tmp.T_CONTRDATE <> date'0001-01-01'
                    then
                        dspground_tmp.r_spgroundid  := dspground_dbt_seq.nextval;
                        dspground_tmp.r_dealid      := tick_tmp.t_dealid;
                        dspground_tmp.r_AltXld      := SubStr( main_tmp.T_CONTRNUM, 1, 20 );
                        dspground_tmp.r_SignedDate  := main_tmp.T_CONTRDATE;
                        dspground_tmp.r_party       := tick_tmp.T_PARTYID;
                        dspground_tmp.r_BofficeKind := tick_tmp.T_BOFFICEKIND;

                        dspground_arr( dspground_arr.count ) := dspground_tmp;
                        deb('Запись SPGROUNDID = #1 сохранена в буфер dspground_arr', dspground_tmp.r_SPgroundID, p_level=>5);
                    end if;

                elsif main_tmp.t_action = 2
                then
                    deb('Запись в таблицы DDL_TICK_DBT и DDL_LEG_DBT записей на изменение', p_level=>5);
                    begin
                        savepoint a;
                        update ddl_tick_dbt set row=tick_tmp where t_dealid=tick_tmp.t_dealid;
                        deb('Запись в таблицы DDL_TICK_DBT и DDL_LEG_DBT записей на изменение', p_level=>5);
                    exception when others
                    then
                        pr_exclude(662, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: ошибка при обновлении в ddl_tick_dbt - ' || sqlcode || ' (' || sqlerrm || ')', i, main_tmp.t_action);
                        deb('Запись в таблицу DDL_TICK_DBT и DDL_LEG_DBT записей на изменение', p_level=>5);
                        continue;
                    end;

                    begin
                        leg1_tmp.t_dealid := tick_tmp.t_dealid;
                        update ddl_leg_dbt set row=leg1_tmp where t_dealid=tick_tmp.t_dealid and t_legkind=0;
                    exception when others
                    then
                        pr_exclude(662, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: ошибка при обновлении в ddl_leg_dbt - ' || sqlcode || ' (' || sqlerrm || ')', i, main_tmp.t_action);
                        rollback to savepoint a;
                        continue;
                    end;

                    if add_tmp.tgt_existback
                    then
                        if add_tmp.is_loan_to_repo
                        then
                            -- преобразование займа в репо, раньше второй части не было
                            begin
                                leg2_tmp.t_dealid := tick_tmp.t_dealid;
                                update ddl_leg_dbt set row=leg2_tmp where t_dealid=tick_tmp.t_dealid and t_legkind=2;
                            exception when others
                            then
                                pr_exclude(662, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: ошибка при обновлении 2й части сделки в ddl_leg_dbt - ' || sqlcode || ' (' || sqlerrm || ')', i, main_tmp.t_action);
                                rollback to savepoint a;
                            continue;
                            end;

                        else
                            -- обычное обновление второй части сделки
                            begin
                                leg2_tmp.t_dealid := tick_tmp.t_dealid;
                                update ddl_leg_dbt set row=leg2_tmp where t_dealid=tick_tmp.t_dealid and t_legkind=2;
                            exception when others
                            then
                                pr_exclude(665, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: выполняется трансформация займа в РЕПО. Ошибка при добавлении в ddl_leg_dbt данных по второй части сделки - ' || sqlcode || ' (' || sqlerrm || ')', i, main_tmp.t_action);
                                rollback to savepoint a;
                                continue;
                            end;
                        end if;
                    end if;
                    -- обработка снятия ранее установленного примечания "Замена ц/б", если приходит запись на обновление сделки с соответствующим признаком, то есть chavr==null*/
                    if main_tmp.T_CHAVR = date'0001-01-01'
                    then
                        delete from dnotetext_dbt note where note.t_ObjectType=3 /*???*/ and t_DocumentID=lpad(tick_tmp.t_dealid, '0', 10)  and note.t_notekind=104;
                    end if;

                elsif main_tmp.t_action = 3
                then

                    -- удаление всех примечаний
                    savepoint b;
                    delete from dnotetext_dbt note where note.t_ObjectType=3 /*???*/ and t_DocumentID=lpad(tick_tmp.t_dealid, '0', 10);
                    -- удаление договора и привязки к договору
                    -- пока только привязку. Договор может еще использоваться
                    begin
                        delete from dspgrdoc_dbt where t_SourceDocID = tick_tmp.t_dealid;
                    exception when others then
                        rollback to savepoint b;
                        pr_exclude(668, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: ошибка при удалении в dspgrdoc_dbt записи о привязке договора к сделке - ' || sqlcode || ' (' || sqlerrm || ')', i, main_tmp.t_action);
                        continue;
                    end;
                    -- удаление ценовых условий сделки
                    begin
                        delete from ddl_leg_dbt where t_dealid = tick_tmp.t_dealid;
                    exception when others then
                        rollback to savepoint b;
                        pr_exclude(669, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: ошибка при удалении в ddl_leg_dbt - ' || sqlcode || ' (' || sqlerrm || ')', i, main_tmp.t_action);
                        continue;
                    end;
                    -- удаление тикета сделки
                    begin
                        delete from ddl_tick_dbt where t_dealid = tick_tmp.t_dealid;
                    exception when others then
                        rollback to savepoint b;
                        pr_exclude(672, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: ошибка при удалении в ddl_leg_dbt - ' || sqlcode || ' (' || sqlerrm || ')', i, main_tmp.t_action);
                        continue;
                    end;
                    -- удаление лота
                    delete from dpmwrtsum_dbt where t_docid in (select t_id from ddlrq_dbt where t_docid = tick_tmp.t_dealid);
                    -- удаление платежа
                    delete from ddlrq_dbt where t_docid = tick_tmp.t_dealid;
                else
                    deb('>>>>>>> Ошибка! main_tmp.t_action для записи равен ' || main_tmp.t_action);
                end if;

                -- сделка обработана успешно. Проставляем код успешного выполнения и DEALID в дополнительную коллекцию
                if tick_tmp.t_dealid is NULL
                then
                    deb('Ошибка! tick_tmp.t_dealid для записи равен NULL (запись #1, t_action = #2)', main_tmp.t_dealid, main_tmp.t_action);
                    return;
                end if;
                pr_include(i);
                deal_sou_add_arr(i).TGT_DEALID := tick_tmp.t_dealid;
                deal_sou_back(tick_tmp.t_dealid) := i; -- может пригодиться, если будет ошибка при вставке записей в TICK
                add_tmp.TGT_DEALID := tick_tmp.t_dealid;


                if main_tmp.t_needdemand = chr(88)
                then
                    -- добавление платежей
                    add_auto_demands(main_tmp, tick_tmp, leg1_tmp);
                end if;

                if main_tmp.t_needdemand2 = chr(88)
                then
                    -- добавление платежей
                    add_auto_demands(main_tmp, tick_tmp, leg2_tmp);
                end if;

            end loop; -- конец цикла 4

            -- сохраняем все добавленные сделки
            write_deals_to_ddltick;
            write_deals_to_replobj;
            update_dtxdeal;

            -- вставляем платежи
            write_demands( p_action );
            -- сохраняем договоры по сделкам и привязку к сделкам
            write_grounds;
        end loop; -- основной цикл, по порциям данных из DTXDEAL_DBT

        deal_sou_arr.delete;
        deal_sou_add_arr.delete;
        deal_sou_back.delete;
        replobj_rec_arr.delete;
        replobj_rec_inx_arr.delete;
        replobj_tmp_arr.delete; 
        DBMS_SESSION.FREE_UNUSED_USER_MEMORY;

        deb('Запись новых типов субъектов в таблицу dpartyown_dbt');
        upload_subject_types;
        g_debug_level_current := g_debug_level_limit;
        
        deb('Завершена процедура LOAD_DEALS');
        procedure_exec_interval := systimestamp - procedure_start_date;
        deb('Время выполнения процедуры: #1:#2', extract(minute from procedure_exec_interval), extract(second from procedure_exec_interval));
    end load_deals;


    -- процедура загрузки платежей
    procedure load_demands(p_date date, p_action number)
    is
        cursor m_cur(pp_date date, pp_action number) is select * from DTXDEMAND_DBT where t_instancedate >= pp_date and t_instancedate < (pp_date+1) and t_replstate=0 and t_action = pp_action order by t_instancedate, t_action;

        -- коллекция t_dealid, индексация произвольная. Сюда собираем id сделок для загрузки данных по ним
        tick_by_demand_arr  tmp_arr_type;
        -- коллекция для временного и постоянного буферов сделок. Временный индексирован произвольно, постоянный - t_dealid
        type ddl_tick_dbt_arr_type is table of ddl_tick_dbt%rowtype index by pls_integer;
        deal_tmp                ddl_tick_dbt%rowtype;
        ddl_tick_dbt_arr_tmp    ddl_tick_dbt_arr_type;
        ddl_tick_dbt_arr        ddl_tick_dbt_arr_type;
        -- коллекция t_fiid, индексированная t_dealid
        -- fiid_by_tick_arr    tmp_arr_type;


        demand_tmp demand_type;
        main_tmp   dtxdemand_dbt%rowtype;
        error_found boolean := false;
        tmp_number  number;
        tmp_string  varchar2(500);
        
        l_counter   number := 0; -- счетчик обработанных строк
        l_err_counter   number := 0; -- счетчик ошибок

        --replobj_rec_arr             replobj_rec_arr_type;
        --replobj_rec_inx_arr         replobj_rec_inx_arr_type; -- индексная коллекция
        --replobj_tmp_arr             replobj_tmp_arr_type;

        -- выявлены проблемы с записью. Логируем ошибку и исключаем запись из обработки.
        procedure pr_exclude(p_errnum number, p_id number, p_text varchar2, p_counter number, p_action number, p_silent boolean := false)
            is
                text_corr varchar2(1000);
                v_row DTXDEMAND_DBT%ROWTYPE;
            begin
                deb('Запущена процедура  PR_EXCLUDE', p_level=>5);
                v_row := demand_sou_arr(p_counter);
                text_corr := replace(p_text, '%act%',  (case p_action when 1 then 'Вставка' when 2 then 'изменение' when 3 then 'удаление' end) );

                -- потом заменить на add_log_deferred
                if not p_silent
                then
                    add_log( p_errnum, 90, p_id, 0, text_corr, v_row.t_instancedate);
                end if;

                -- исключаем элемент
                --demand_sou_arr.delete(p_counter);
                demand_sou_arr(p_counter).t_replstate := 2;
                l_err_counter := l_err_counter + 1;
                deb('Завершена процедура  PR_EXCLUDE', p_level=>5);
            end pr_exclude;

--================================================================================================================
--================================================================================================================
--================================================================================================================
--================================================================================================================
    begin
        deb_empty('=');
        l_err_counter := 0;
        l_counter := 0;
        procedure_start_date := sysdate;
        deb('Инициализация буфера DBMS_OUTPUT');
        DBMS_OUTPUT.ENABLE (buffer_size => NULL);

        deb('Запущена процедура  LOAD_DEMANDS за ' || to_char(p_date, 'dd.mm.yyyy') || ', тип действия ' || p_action);

        g_debug_level_current := -1;

        open m_cur(p_date, p_action);
        loop
            -- загрузка порции данных
            fetch m_cur bulk collect into demand_sou_arr limit g_limit_demand;
            exit when demand_sou_arr.count=0;
            l_counter := l_counter + m_cur%rowcount;
            deb('Загружены данные из DTXDEMAND_DBT, #1 строк', m_cur%rowcount);
            
            -- устанавливаем диапазон регистрируемых записей для лога. Если в выборке < 20 записей, считаем ее отладочной и выводим полную информацию.
            if g_debug_level_current = -1
            then
                if m_cur%rowcount < 20
                then
                    g_debug_level_current := 10;
                else
                    g_debug_level_current := g_debug_level_limit;
                end if;
                deb('Установлен уровень логирования: #1', g_debug_level_current);
            end if;


            -- регистрируем все сущности для загрузки из REPLOBJ
            deb_empty('=');
            deb('Цикл 1 - очистка данных и регистрация кодов в буфере REPLOBJ');
            for i in 1..demand_sou_arr.count
            loop
                main_tmp    := demand_sou_arr(i);

                if p_action < 3
                then
                    tmp_string := null;
                    if      nvl(main_tmp.t_dealid,0) = 0
                    then
                        tmp_string := 'Не задан номер сделки для платежа';
                    elsif   nvl(main_tmp.t_part,0) = 0
                    then
                        tmp_string := 'Не задан номер части сделки для платежа';
                    elsif   nvl(main_tmp.t_kind,0) = 0
                    then
                        tmp_string := 'Не задан t_kind платежа';
                    elsif   nvl(main_tmp.t_direction,0) = 0
                    then
                        tmp_string := 'Не задано t_direction - направление платежа';
                    elsif   nvl(main_tmp.t_fikind,0) = 0
                    then
                        tmp_string := 'Не задано t_fikind  платежа';
                    elsif   nvl(main_tmp.t_date, date'0001-01-01') = date'0001-01-01'
                    then
                        tmp_string := 'Не задано t_date - дата платежа';
                    elsif   nvl(main_tmp.t_sum,0) = 0
                    then
                        tmp_string := 'Не задано t_sum - сумма платежа';
                    elsif   nvl(main_tmp.t_state,0) = 0
                    then
                        tmp_string := 'Не задано t_state - статус платежа';
                    elsif   nvl(main_tmp.t_paycurrencyid,0) = 0 and main_tmp.t_fikind=10
                    then
                        tmp_string := 'Не задано t_paycurrencyid - валюта платежа';
                    end if;

                    if tmp_string is not null
                    then
                        pr_exclude(207, main_tmp.t_demandid, tmp_string, i, p_action);
                        deb('Ошибка. Платеж ' || main_tmp.t_demandid || '.  ' || tmp_string, p_level=>5);
                        continue;
                    end if;

                    -- собираем уникальные fiid
                    if main_tmp.t_fikind=10
                    then
                        replobj_add( c_OBJTYPE_MONEY,   main_tmp.t_paycurrencyid, 0 );
                    end if;
                    -- сделка
                    replobj_add( c_OBJTYPE_DEAL,    main_tmp.T_DEALID);

                end if;

                -- собственно, платеж
                if p_action > 1 then
                    replobj_add( c_OBJTYPE_PAYMENT, main_tmp.T_DEMANDID);
                end if;

            end loop;
            deb('Собрали данные в буфер REPLOBJ, #1 записей', replobj_rec_arr.count);

            -- заполняем кэш из REPLOBJ -----------------------------------------------------------------
            replobj_load;

            -- очищаем данные и передаем в add_demand ---------------------------
            deb_empty;
            deb_empty('=');
            tick_by_demand_arr.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;

            deb('Цикл 2 - проверка качества данных');
            for i in 1..demand_sou_arr.count
            loop
                if demand_sou_arr(i).t_replstate = 2
                then continue;
                end if;
                
                main_tmp    := demand_sou_arr(i);
                error_found := false;

                demand_tmp.tgt_demandid :=  replobj_get(c_OBJTYPE_PAYMENT, main_tmp.t_demandid).dest_id;
                demand_tmp.tgt_docid    :=  replobj_get(c_OBJTYPE_DEAL, main_tmp.t_dealid).dest_id;
                tmp_number              :=  replobj_get(c_OBJTYPE_DEAL, main_tmp.t_dealid).state;
                if tmp_number = 1
                then
                    error_found := true;
                    deb('Ошибка: Т/О по сделке ' ||  main_tmp.t_dealid || ' находится в режиме ручного редактирования', p_level=>5);
                    add_log( 207, c_OBJTYPE_PAYMENT, main_tmp.t_demandid, 0, 'Ошибка: Т/О по сделке ' ||  main_tmp.t_dealid || ' находится в режиме ручного редактирования', main_tmp.t_instancedate);
                end if;

                if nvl(main_tmp.t_dealid,0) > 0 and demand_tmp.tgt_docid = -1
                then
                    error_found := true;
                    deb('Ошибка: отсутствует сделка #1 по платежу #2', main_tmp.t_dealid, main_tmp.t_demandid, p_level=>5);
                    add_log( 207, c_OBJTYPE_PAYMENT, main_tmp.t_demandid, 0, 'Ошибка: отсутствует сделка по платежу ' ||  main_tmp.t_dealid, main_tmp.t_instancedate);
                end if;

                if main_tmp.t_fikind = 10  -- денежный платеж
                then
                    demand_tmp.tgt_fiid     :=  replobj_get(c_OBJTYPE_MONEY, main_tmp.t_paycurrencyid).dest_id;
                    if demand_tmp.tgt_fiid = -1
                    then
                        error_found := true;
                        add_log( 207, c_OBJTYPE_PAYMENT, main_tmp.t_demandid, 0, 'Ошибка: не найдена валюта платежа ' || main_tmp.t_paycurrencyid || ' в Т/О по сделке ' ||  main_tmp.t_dealid , main_tmp.t_instancedate);
                    end if;
                end if;

                -- собираем ID сделок для загрузки в буфер
                if not error_found then
                    tick_by_demand_arr( tick_by_demand_arr.count ) := demand_tmp.tgt_docid;
                else
                    pr_exclude(207, main_tmp.t_demandid, 'Ошибка в параметрах платежа', i, p_action);
                end if;

            end loop; -- конец цикла 2

            deb('Загружаем тикеты сделок в буфер для определения fiid бумаг, #1 записей в запросе', tick_by_demand_arr.count);
            --#query
            select * bulk collect into ddl_tick_dbt_arr_tmp from ddl_tick_dbt where t_dealid in (select column_value from table(tick_by_demand_arr));
            deb('Загружено #1 уникальных тикетов сделок', sql%rowcount);
            
            -- Перегружаем fiid из тикетов в коллекцию, индексированную сделкой
            for j in 1..ddl_tick_dbt_arr_tmp.count
            loop
                ddl_tick_dbt_arr( ddl_tick_dbt_arr_tmp(j).t_dealid ) := ddl_tick_dbt_arr_tmp(j);
            end loop;
            -- временные коллекции больше не нужны
            ddl_tick_dbt_arr_tmp.delete;
            tick_by_demand_arr.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
            deb('Создан буфер сделок, #1 записей', ddl_tick_dbt_arr.count);

            deb_empty('=');
            deb('Цикл 3 - назначение валюты платежа (для бумаг) и передача платежа в add_demand');
            for i in 1..demand_sou_arr.count
            loop
                if main_tmp.t_replstate = 2
                then continue;
                end if;
                
                main_tmp  := demand_sou_arr(i);

                demand_tmp.tgt_demandid :=  replobj_get(c_OBJTYPE_PAYMENT, main_tmp.t_demandid).dest_id;
                demand_tmp.tgt_docid    :=  replobj_get(c_OBJTYPE_DEAL, main_tmp.t_dealid).dest_id;
                if ddl_tick_dbt_arr.exists( demand_tmp.tgt_docid )
                then
                    deal_tmp    := ddl_tick_dbt_arr( demand_tmp.tgt_docid );
                else
                    error_found := true;
                    deb('Ошибка: отсутствует сделка #1 по платежу #2. В RS должна быть с DEALID=#3', main_tmp.t_dealid, main_tmp.t_demandid, demand_tmp.tgt_docid, p_level=>5);
                    add_log( 207, c_OBJTYPE_PAYMENT, main_tmp.t_demandid, 0, 'Ошибка: отсутствует сделка по платежу ' ||  main_tmp.t_demandid || '. В целевой системе нет T_DEALID=' || demand_tmp.tgt_docid, main_tmp.t_instancedate);
                    continue;
                end if;

                if main_tmp.t_fikind=10
                then
                    demand_tmp.tgt_fiid     :=  replobj_get(c_OBJTYPE_MONEY, main_tmp.t_paycurrencyid).dest_id;
                else
                    demand_tmp.tgt_fiid     :=  deal_tmp.t_pfi;
                end if;

                demand_tmp.r_action         := p_action;
                demand_tmp.r_oldobjectid    := main_tmp.t_demandid;
                demand_tmp.tgt_dockind      := deal_tmp.T_BOFFICEKIND;
                demand_tmp.r_isauto         := false;
                demand_tmp.r_isfact         := case main_tmp.t_isfact when chr(88) then true else false end;
                demand_tmp.r_destsubobjnum  := 1;  -- непонятно, зачем. так в исходном алгоритме
                demand_tmp.r_kind           := main_tmp.t_kind;
                demand_tmp.r_direction      := main_tmp.t_direction;

                if deal_tmp.t_marketid > 0
                then
                    demand_tmp.tgt_party := deal_tmp.t_marketid;
                else
                    demand_tmp.tgt_party := deal_tmp.t_partyid;
                end if;

                demand_tmp.r_note   := main_tmp.t_note;
                demand_tmp.r_date   := main_tmp.t_date;
                demand_tmp.r_state  := 3;
                demand_tmp.r_part   := main_tmp.t_part;
                demand_tmp.r_sum    := main_tmp.t_sum;
                demand_tmp.r_subobjnum := 0;

                add_demand( demand_tmp );

            end loop; -- конец цикла 3


            ddl_tick_dbt_arr.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
            deb('Записываем платежи в БД');
            write_demands( p_action );

        end loop; -- конец главного цикла.

        g_debug_level_current := g_debug_level_limit;
        
        deb('Завершена процедура  LOAD_DEMANDS');
        procedure_exec_interval := systimestamp - procedure_start_date;
        deb('Время выполнения процедуры: #1:#2', extract(minute from procedure_exec_interval), extract(second from procedure_exec_interval));

    end load_demands;



    -- процедура добавления платежа в кэш DEMAND_RQ_ARR
    procedure   add_demand (p_demand   demand_type)
    is
        rq_tmp  ddlrq_dbt%rowtype;
        add_tmp demand_add_type;

        index_tmp    pls_integer;
    begin
        deb('Запущена процедура ADD_DEMAND', p_level=>5);

        if p_demand.r_action = 1
        then
            rq_tmp.t_id := ddlrq_dbt_seq.nextval;
        else
            rq_tmp.t_id := p_demand.tgt_demandid;
        end if;

        rq_tmp.t_docid      :=  p_demand.tgt_docid;
        rq_tmp.t_dealpart   :=  p_demand.r_part;
        rq_tmp.t_DocKind    :=  p_demand.tgt_dockind;
        rq_tmp.t_party      :=  p_demand.tgt_party;

        if p_demand.r_direction = 1
        then
            rq_tmp.t_kind   := 0; -- требование
        else
            rq_tmp.t_kind   := 1; -- обязательство
        end if;

        if p_demand.r_isfact
        then
            rq_tmp.t_State      := 2;
            rq_tmp.t_FactDate   := p_demand.r_date;
            rq_tmp.t_PlanDate   := date'0001-01-01';
            rq_tmp.t_changedate := date'0001-01-01';
            rq_tmp.t_num := 0;
        else
            rq_tmp.t_state      := 0;
            rq_tmp.t_factDate   := date'0001-01-01';
            rq_tmp.t_PlanDate   := p_demand.r_date;
            rq_tmp.t_changedate := p_demand.r_date;
            rq_tmp.t_num := 1; -- предполагаем, что с плановыми платежами всегда идут фактические. Сразу разнесем их по T_NUM, чтобы не обрабатывать потом ошибки и не терять время
            -- для справки: плановые и фактические платежи имеют одинаковые с точки зрения уникального индекса параметры, поэтому должны иметь разные T_NUM 
        end if;

        if p_demand.r_isnetting
        then
            rq_tmp.t_netting := chr(88);
        end if;
        rq_tmp.t_amount  :=  p_demand.r_sum;
        rq_tmp.t_fiid    :=  p_demand.tgt_fiid;

        rq_tmp.t_type    :=
        case p_demand.r_kind
            when 10 then 8 -- поставка
            when 20 then 0 -- задаток
            when 30 then 1 -- аванс
            when 40 then 2 -- оплата
            when 50 then 3 -- проценты
            when 60 then 8 -- итоговая поставка
            when 70 then 2 -- итоговая оплата
            when 80 then 4 -- выплата купонного дохода
            when 90 then 5 -- частичное погашение
            when 100 then 10 -- выплата дивидендов
            when 110 then 7  -- компенсационная оплата
            when 120 then 9  -- компенсационная поставка
            else -1
        end;

        if rq_tmp.t_type in (8,9)
        then
            rq_tmp.t_subkind := 1; -- ценные бумаги
        else
            rq_tmp.t_subkind := 0; -- деньги
        end if;

        rq_tmp.t_ID_Step       := 2908;
        rq_tmp.t_RqAccID       := -1;
        rq_tmp.t_PlaceID       := -1;
        rq_tmp.t_Instance      := 0;
        rq_tmp.t_Action        := 0;
        rq_tmp.t_ID_Operation  := 0;
        rq_tmp.t_Source        := 0;
        rq_tmp.t_SourceObjKind := -1;
        rq_tmp.t_SourceObjID   := 0;
        rq_tmp.t_factreceiverid:= -1;
        rq_tmp.t_taxratebuy    := 0;
        rq_tmp.t_taxsumbuy     := 0;
        rq_tmp.t_taxratesell   := 0;
        rq_tmp.t_taxsumsell    := 0;
        rq_tmp.t_source        := 0;

        -- здесь сохраняем дополнительные параметры, которые нужны для dtxdemand, dtxreplobj
        add_tmp.r_oldobjectid   := p_demand.r_oldobjectid;
        add_tmp.r_subobjnum     := p_demand.r_subobjnum;
        add_tmp.r_destsubobjnum := p_demand.r_destsubobjnum;
        add_tmp.r_result        := 0;
        add_tmp.r_action        := p_demand.r_action;

        -- запись в коллекцию
        index_tmp := demand_rq_arr.count;
        demand_rq_arr( index_tmp )  := rq_tmp;
        demand_add_arr( index_tmp ) := add_tmp;
        deb('Платеж добавлен (всего в буфере demand_rq_arr = #1): t_docid=#2, t_type=#3, t_fiid=' || rq_tmp.t_fiid || ', t_dealpart=' || rq_tmp.t_dealpart || ', t_state=' || rq_tmp.t_State || ', t_id=' || rq_tmp.t_id , demand_rq_arr.count, rq_tmp.t_docid, rq_tmp.t_type, p_level=>5);



        deb('Завершена процедура ADD_DEMAND', p_level=>5);
    end add_demand;

    -- процедура записи платежей из кэша в БД. Используется в load_demands и load_deals
    -- процедура не принимает разные t_action в разных записях - это очень усложняет обработку ошибок.
    procedure   write_demands( p_action number )
    is
        -- на ddlrq_dbt неправильно написанный триггер, обращающийся к таблицу, на которую навешен. Такие триггеры не принимают многострочные вставки, придется обходить.
        -- обходим ORA-04091
        insert_count  number := 1; -- количество пробных вставок. Если в течение их происходят ошибки, откатываемся, корректируем t_num и повторяем.
        insert_count_limit constant number := 6;  -- максимальное количество попыток записи. Если после этого остаются ошибки, просто фиксируем их
    begin
        execute immediate 'alter trigger DDLRQ_DBT_TBI disable';

        deb('Запущена процедура WRITE_DEMANDS');
        deb('Количество записей в буфере платежей - #1 (#2)',  demand_rq_arr.count, demand_add_arr.count);
        if demand_rq_arr.count > 0
        then
            case p_action
            when 1 then -- вставка

                savepoint spa;
                -- ошибка констрейнта IDX1 - платежи одного типа должны различаться полем T_NUM. Обычно его назначает триггер, но он был отключен, поскольку он ошибочный.
                -- так что будем пытаться это поле назначать при поимке ошибки.
                -- всего для каждого платежа можем сделать до 3 попыток. Предполагаем, что больше 3 одинаковых платежей под сделкой не бывает.
                while insert_count  < insert_count_limit
                loop
                    deb('Попытка #1', insert_count);
                    rollback to spa;  -- лучше сделать это при входе, из последней итерации будем выходить с грязной записью.
                    begin
                        forall i in indices of demand_rq_arr SAVE EXCEPTIONS
                            insert into ddlrq_dbt values demand_rq_arr(i);
                        deb('Попытка успешна');
                        exit;  -- если не было ошибок, не продолжаем цикл
                    exception
                        when others  -- предполагаем, что ошибки связаны только с t_num
                        then
                            deb('     ' || 'Ошибки (количество - #1) при вставке платежей. Перекодируем T_NUM', SQL%BULK_EXCEPTIONS.COUNT);
                            -- цикл по ошибкам
                            for i in 1..SQL%BULK_EXCEPTIONS.count
                            loop
                                deb( '     ' || i || '> платеж #1 (сделка #2) в ddlrq_dbt. t_num = #3', demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_id, demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_docid, demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_num,  p_level => 5);
                                demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_num := demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_num + 1;
                                -- если это последняя попылка, отмметим запись, как ошибочную
                                if insert_count = insert_count_limit-1 
                                then
                                    demand_add_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).r_result := 2; -- флаг ошибки для replobj и dtxdemand
                                end if;
                                                                
                            end loop;
                    end;
                    insert_count  := insert_count  + 1;
                end loop;
                -- выведем результат и отметим ошибочные записи, если остались
                deb('Выполнена вставка в DDLRQ_DBT, количество записей - #1, количество ошибок - #2', demand_rq_arr.count, SQL%BULK_EXCEPTIONS.COUNT);

            when 2 then -- изменение

                savepoint spa;

                while insert_count  < insert_count_limit
                loop
                    deb('Попытка #1', insert_count);
                    rollback to spa;  -- лучше сделать это при входе, из последней итерации будем выходить с грязной записью.
                    begin
                        forall i in indices of demand_rq_arr SAVE EXCEPTIONS
                            update ddlrq_dbt set row=demand_rq_arr(i) where t_id=demand_rq_arr(i).t_id;
                        deb('Попытка успешна');
                        exit;  -- если не было ошибок, не продолжаем цикл
                    exception
                        when others  -- предполагаем, что ошибки связаны только с t_num
                        then
                            deb('     ' || 'Ошибки (количество - #1) при изменении платежей. Перекодируем T_NUM', SQL%BULK_EXCEPTIONS.COUNT);
                            -- цикл по ошибкам
                            for i in 1..SQL%BULK_EXCEPTIONS.count
                            loop
                                deb( '     ' || i || '> платеж #1 (сделка #2) в ddlrq_dbt. t_num = #3', demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_id, demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_docid, demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_num,  p_level => 5);
                                demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_num := demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1).t_num + 1;
                                -- если это последняя попылка, отмметим запись, как ошибочную
                                if insert_count = insert_count_limit-1 
                                then
                                    demand_add_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).r_result := 2; -- флаг ошибки для replobj и dtxdemand
                                end if;                                
                            end loop;
                    end;
                    insert_count  := insert_count  + 1;
                end loop;
                -- выведем результат и отметим ошибочные записи, если остались
                deb('Выполнено изменение в DDLRQ_DBT, количество записей - #1, количество ошибок - #2', demand_rq_arr.count, SQL%BULK_EXCEPTIONS.COUNT);

            when 3 then -- удаление
                -- обработка записей на удаление
                begin
                    forall i in indices of demand_rq_arr
                        delete ddlrq_dbt where t_id=demand_rq_arr(i).t_id;
                exception 
                when others then
                    deb('Выполнена удаление из DDLRQ_DBT, количество записей - #1, количество ошибок - #2', demand_rq_arr.count, SQL%BULK_EXCEPTIONS.COUNT);
                    -- цикл по возможным ошибкам
                    for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                    loop
                        deb('Ошибка #3 удаления платежа #1 (сделка #2) в ddlrq_dbt', demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).t_id, demand_rq_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).t_docid,  SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
                        demand_add_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX-1 ).r_result := 2;
                    end loop;
                end;
            end case;

            commit;


            -- заполняем dtxreplobj_dbt. Только для успешно обработанных записей. Сначала на всякий случай убираем аналогичные существующие записи.
            deb('Выполняется вставка в DTXREPLOBJ');
            forall i in indices of demand_rq_arr
                delete from dtxreplobj_dbt
                where T_OBJECTTYPE=90 and T_OBJECTID=demand_add_arr(i).r_oldobjectid and T_SUBOBJNUM=demand_add_arr(i).r_subobjnum
                and demand_add_arr(i).r_result <> 2;

            for j in nvl(demand_rq_arr.first,0) .. nvl(demand_rq_arr.last, -1)
            loop
                deb('   > T_OBJECTID=#1, T_SUBOBJNUM=#2, T_DESTID=#3, RESULT='||demand_add_arr(j).r_result, demand_add_arr(j).r_oldobjectid, demand_add_arr(j).r_subobjnum, demand_rq_arr(j).t_id, p_level=>5);
            end loop;

            begin
                forall i in indices of demand_rq_arr
                    insert /*+ append_values */ into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE)
                    select 90, demand_add_arr(i).r_oldobjectid, demand_add_arr(i).r_subobjnum, demand_rq_arr(i).t_id, demand_add_arr(i).r_destsubobjnum, 0  from dual
                    where demand_add_arr(i).r_result <> 2;
            exception when others then 
                for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                loop
                    deb('Ошибка #3 вставки в DTXREPLOBJ_DBT записи о платеже #1 (сделка #2)', demand_rq_arr(i).t_id, demand_rq_arr(i).t_docid,  SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
                end loop;
            end;

            deb('Выполнена вставка в DTXREPLOBJ_DBT, количество записей - #1, количество ошибок - #2', demand_rq_arr.count, SQL%BULK_EXCEPTIONS.COUNT);

            -- проставляем replstate=1 для всех успешно обработанных платежей
            begin
                forall i in indices of demand_rq_arr
                    update dtxdemand_dbt
                    set t_replstate=1 where t_demandid = demand_add_arr(i).r_oldobjectid
                    and demand_add_arr(i).r_result <> 2 and demand_add_arr(i).r_subobjnum = 0;  -- для автоплатежей с r_subobjnum 81-84 не заполняем
            exception when others
            then
                for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                loop
                    deb('Ошибка #3 обновления DTXDEMAND_DBT.t_replstate для платежа #1 (сделка #2)', demand_rq_arr(i).t_id, demand_rq_arr(i).t_docid,  SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
                end loop;
            end;
            
            deb('Выполнено обновление t_replstate в DTXDEMAND_DBT, количество записей - #1, количество ошибок - #2', demand_rq_arr.count, SQL%BULK_EXCEPTIONS.COUNT);

            -- буферные коллекции больше не нужны
            demand_rq_arr.delete;
            demand_add_arr.delete;
            DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
        end if;
        
        execute immediate 'alter trigger DDLRQ_DBT_TBI enable';
        deb('Завершена процедура WRITE_DEMANDS');
--      exception when others then
--        execute immediate 'alter trigger DDLRQ_DBT_TBI enable';
--        deb('Завершена процедура WRITE_DEMANDS (ошибочно)');
    end write_demands;




    -- запись в лог
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

   
        
    
begin
    deb('=== Выполняется инициирующая загрузка в пакете ===');
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);

    rollback; -- посреди транзакции нельзя изменить выполнить следующую команду
    execute immediate 'alter session enable parallel dml';
    
    my_SID := to_number(SYS_CONTEXT('USERENV','SESSIONID'));
    
    deb('SID = #1', my_SID); 
    
    -- создание или переиспользование вспомогательных таблиц
    begin
        execute immediate 'create table dtx_error_records(t_instancedate date, t_action number(5), t_objecttype number(5), t_objectid number(15), t_errorcode number(5), t_level number(3)) ';
    exception when others then null;
    end;

    begin
        execute immediate 'create table dtxdeal_tmp nologging for exchange with table dtxdeal_dbt';
    exception when others then null;
    end;


    -- заполнение списка подразделений
    for i in (select t_code, t_partyid from ddp_dep_dbt)
    loop
        ddp_dep_dbt_cache( i.t_partyid ) := i.t_code;
    end loop;

    -- загрузка списка бирж, брокеров и контрагентов
    --#query
    select t_partyid bulk collect into tmp_arr from dpartyown_dbt where t_partykind = 1; -- ????
    for i in 1..tmp_arr.count
    loop
        list_of_stocks_arr( tmp_arr(i) ) := 0;
    end loop;

    select t_partyid bulk collect into tmp_arr from dpartyown_dbt where t_partykind = 1; -- ????
    for i in 1..tmp_arr.count
    loop
        list_of_brokers_arr( tmp_arr(i) ) := 0;
    end loop;

    select t_partyid bulk collect into tmp_arr from dpartyown_dbt where t_partykind = 1; -- ????
    for i in 1..tmp_arr.count
    loop
        list_of_contrs_arr( tmp_arr(i) ) := 0;
    end loop;

    -- ищем код ценной бумаги, соответствующий корзине
    begin
        SELECT T_FIID into g_fictfi FROM DFININSTR_DBT WHERE t_name like 'Корзина с%';
    exception when no_data_found
    then
        g_fictfi := c_DEFAULT_FICTFI;
    end;

    -- заполняем список кодов стран
    for j in (select t_countryid, t_codelat3, t_codenum3 from dcountry_dbt)
    loop
        --country_arr(j.t_codenum3) := j.t_codelat3;
        country_arr(j.t_countryid) := j.t_codelat3;
    end loop;

    -- заполняем список ISO кодов валют (для примечания Валюта платежа)
    for i in (select t_iso_number, t_fiid from dfininstr_dbt where t_fi_kind=1)
    loop
        currency_iso_arr(i.t_fiid) := i.t_iso_number;
    end loop;

    tmp_arr.delete;
    DBMS_SESSION.FREE_UNUSED_USER_MEMORY;
    g_debug_level_current := g_debug_level_limit;

    
    deb('=== Завершен исполняемый блок пакета ===');
end load_rss;
/
