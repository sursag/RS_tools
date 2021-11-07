CREATE OR REPLACE PACKAGE BODY GEB_20210823_TST.load_rss
is
    -- Сагиян С.Г. 11.2021
    -- Пакет загрузки данных в процессе репликации из таблиц DTX* в таблицы Rs-Bank 

    g_fictfi             number; -- код fiid, соответствующий в целевой системе корзине бумаг
    deb_flag             number := 0; -- для процедуры логирования. Уровень вложенности записей логов, для отсутпов

    write_query_log_start date; -- переменная для процедуры write_query_log


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
    
    
    
    -- процедура создает снимок по сделкам из DTXDEMAND_DBT в таблицу DTXDEMAND_TMP
    procedure courses_create_snapshot( p_startdate date, p_enddate date)
    is 
    begin
        deb('Запущена процедура COURSES_CREATE_SNAPSHOT c ' || to_char(p_startdate, 'dd.mm.yyyy hh24:mi') || ' по '  || to_char(p_enddate, 'dd.mm.yyyy hh24:mi'));
        -- получаем снимок из dtxdemand_dbt в dtxdemand_tmp - только тот набор записей, который необходимо реплицировать. Записываем его в таблицу, имеющею пустые поля для индетификаторов целевой системы.

        deb('Очистка таблицы DTXCOURSE_TMP и удаление индексов');
        WRITE_LOG_START;
        execute immediate 'truncate table dtxcourse_tmp';
        
            for j in (select index_name from user_indexes where table_name='DTXCOURSE_TMP')
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
            'insert /*+ append */ into DTXCOURSE_TMP(T_COURSEID, T_TYPE, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_BASEFIKIND, T_BASEFIID, T_FIID, T_MARKETID, T_MARKETSECTORID, T_POINT, T_SCALE, T_RATEDATE, T_RATE)
            select T_COURSEID, T_TYPE, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_BASEFIKIND, T_BASEFIID, T_FIID, T_MARKETID, T_MARKETSECTORID, T_POINT, T_SCALE, T_RATEDATE, T_RATE
            from dtxcourse_dbt where t_instancedate between :1 and :2 and t_replstate=0' using p_startdate, p_enddate;
        deb('Загружено в снимок #1 строк', sql%rowcount);
        
        WRITE_LOG_FINISH('Создаем снимок по курсам', 70);
        commit;
        
        deb('Завершена процедура COURSES_CREATE_SNAPSHOT');
    end courses_create_snapshot;    
        
    
    
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
    procedure courses_create_indexes
    is 
    begin
        deb('Запущена процедура COURSES_CREATE_INDEXES');
        WRITE_LOG_START;
        deb('Создаем индексы на снимке');
        begin
            execute immediate 'CREATE INDEX I_COURSEID ON DTXCOURSE_TMP(T_COURSEID)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXCOURSE_ACTION ON DTXCOURSE_TMP(T_ACTION)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXCOURSE_KIND ON DTXCOURSE_TMP(T_KIND)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXCOURSE_REPLSTATE ON DTXCOURSE_TMP(T_REPLSTATE)';
        exception when others then
            deb('Ошибка при создании индексов на снимке');
        end;
        
        deb('Считаем статистику на снимке');
        dbms_stats.gather_table_stats(user, 'DTXCOURSE_TMP', cascade=>true);
        
        WRITE_LOG_FINISH('Создаем индексы на снимке', 70);    
        
        deb('Завершена процедура COURSES_CREATE_INDEXES');
    end courses_create_indexes;



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
      



    procedure run_one_query_set( p_objecttype number, p_set number )
    is 
    begin
        deb('Запущена процедура RUN_ONE_QUERY_SET для OBJECTTYPE=#1 и сета #2', p_objecttype, p_set);
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
        deb('Завершена процедура RUN_ONE_QUERY_SET');
    end run_one_query_set;



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
        
        run_one_query_set(p_objecttype, 1);
       

        deb('Второй сет запросов. Заполнение таблицы ошибок');
        -- Второй сет запросов, проверяем заполнение полей, определяем ошибки
       
        run_one_query_set(p_objecttype, 2);

        if p_objecttype=80 then
                deals_create_indexes;
        elsif p_objecttype=90 then
                demands_create_indexes;
        elsif p_objecttype=70 then
                courses_create_indexes;
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
        when 70 then
        execute immediate
            'update dtxdemand_tmp set t_replstate=2 where t_demandid in (select t_objectid from dtx_error_dbt where t_objecttype=:1 and t_sessid=:2 and t_detailid=:3)' using p_objecttype, g_SESSION_ID, g_SESS_DETAIL_ID;
            null;            
        end case;
        
        WRITE_LOG_FINISH( 'Отмечаем в снимке найденные ошибки', p_objecttype, 0, 0);

        deb('Третий сет запросов. Обогащение идентификаторами из целевой системы');

        run_one_query_set(p_objecttype, 3);

        deb('Четвертый сет запросов. Проверка по бизнес-правилам');

        run_one_query_set(p_objecttype, 4);
        
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
        WRITE_LOG_FINISH('Обновление DDL_TICK_DBT (t_action=2)', 80);
                
        MERGE INTO (select * from ddl_leg_dbt where t_legkind=0) tgt
        USING (select * from dtxdeal_tmp where t_action=2 and t_replstate=0) sou on (tgt.t_dealid=sou.tgt_dealid)
        WHEN MATCHED THEN UPDATE SET 
                T_PFI=TGT_AVOIRISSID /*T_PFI*/,         T_CFI=tgt_currencyid /*T_CFI*/,                         T_MATURITY=TGT_MATURITY /*T_MATURITY*/, 
                T_EXPIRY=TGT_EXPIRY /*T_EXPIRY*/,       T_PRINCIPAL=t_amount /*T_PRINCIPAL*/,                   T_PRICE=tgt_price /*T_PRICE*/, 
                T_BASIS=case when T_REPOBASE>0 then T_REPOBASE - 1 else 0 end /*T_BASIS*/,                      T_COST=T_COST /*T_COST*/,  
                T_FORMULA=TGT_FORMULA /*T_FORMULA*/,    T_SCALE=1 /*T_SCALE*/,                                  T_POINT=4 /*T_POINT*/,  
                T_RELATIVEPRICE=TGT_RELATIVEPRICE /*T_RELATIVEPRICE*/,  T_NKD=T_NKD /*T_NKD*/,                  T_TOTALCOST=T_TOTALCOST /*T_TOTALCOST*/, 
                T_MATURITYISPRINCIPAL=TGT_MATURITYISPRINCIPAL /*T_MATURITYISPRINCIPAL*/,                        T_INCOMERATE=T_RATE /*T_INCOMERATE*/;
        WRITE_LOG_FINISH('Обновление DDL_LEG_DBT по первой части (t_action=2)', 80);
        
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
        WRITE_LOG_FINISH('Обновление DDL_LEG_DBT по второй части (t_action=2)', 80);
        
        
        -- Обработка удалений
        deb('Удаление из DDL_TICK_DBT (t_action=3)');    
        delete /*+ parallel(4) */ from ddl_tick_dbt where t_dealid in (select tgt_dealid from dtxdeal_tmp where t_replstate=0 and t_action=3);
        delete /*+ parallel(4) */ from ddl_leg_dbt where t_dealid in (select tgt_dealid from dtxdeal_tmp where t_replstate=0 and t_action=3); 
        WRITE_LOG_FINISH('Удаление из DDL_TICK_DBT/DDL_LEG_DBT (t_action=3)', 80);
        
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



    -- формирует записи в целевой системе на основе таблицы снимка
    procedure courses_create_records
    is
    begin
        deb('Запущена процедура COURSES_CREATE_RECORDS');
        
        -- Обработка вставок
        WRITE_LOG_START;
        deb('Заполнение DDL_TICK_DBT (t_action=1)');


        deb('Завершена процедура COURSES_CREATE_RECORDS');
    end courses_create_records;



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





    procedure load_courses_by_period(p_startdate date, p_enddate date default null)
    is
        l_enddate date;
    begin
        deb('Запущена процедура LOAD_COURSES_BY_PERIOD');
                
        l_enddate := nvl( p_enddate, p_startdate + 1 - 1/24/60/60); 
    
        -- Если SESSION_ID был не зназначен, то есть, LOAD_DEALS вызвали вручную, назначим его
        if g_SESSION_ID is null then
            insert into dtx_session_dbt(t_startdate, t_enddate, t_user, t_status)
            values( p_startdate, l_enddate, user, 'R') returning t_sessid into g_SESSION_ID;
        end if;
        
        insert into dtx_sess_detail_dbt( T_SESSID, T_PROCEDURE, T_INSTANCEDATE, T_STARTDATE)
        values (g_SESSION_ID, 'LOAD_COURSES', p_startdate, sysdate) returning t_detailid into g_SESS_DETAIL_ID;
        commit;
    
        -- заполняет таблицу снимка (dtxdeal_tmp из dtxdeeal_dbt)
        courses_create_snapshot(p_startdate, l_enddate);
        
        -- прогоняет сеты запросов по таблице снимка dtxdeal_tmp
        run_all_queries( 70 );
        
        -- переносит ошибки в таблицу логов dtxloadlog_dbt
        write_errors_into_log;
        
        -- формирует записи в таблицы целевой системы (DDL_RQ_DBT, DNOTETEXT_DBT...) на основе таблицы снимка
        courses_create_records;

        deb('Завершена процедура LOAD_COURSES_BY_PERIOD');
    end load_courses_by_period;




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


        -- перекодировка типов курсов
        function GetRateType( p_tp number ) return number
        DETERMINISTIC
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
        end GetRateType;
  
    
    
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

    rollback; -- посреди транзакции нельзя выполнить следующую команду
    execute immediate 'alter session enable parallel dml';
    
    g_my_SID := to_number(SYS_CONTEXT('USERENV','SESSIONID'));
    
    deb('SID = #1', g_my_SID); 
    
    -- создание или переиспользование вспомогательных таблиц
    begin
        execute immediate 'create table dtx_error_records(t_instancedate date, t_action number(5), t_objecttype number(5), t_objectid number(15), t_errorcode number(5), t_level number(3)) ';
    exception when others then null;
    end;

    -- ищем код ценной бумаги, соответствующий корзине
    begin
        SELECT T_FIID into g_fictfi FROM DFININSTR_DBT WHERE t_name like 'Корзина с%';
    exception when no_data_found
    then
        g_fictfi := c_DEFAULT_FICTFI;
    end;
    g_debug_level_current := g_debug_level_limit;

    
    deb('=== Завершен исполняемый блок пакета ===');
end load_rss;
/
