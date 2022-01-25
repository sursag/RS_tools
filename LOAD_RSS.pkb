CREATE OR REPLACE PACKAGE BODY load_rss
is
    -- Сагиян С.Г. 11.2021
    -- Пакет загрузки данных в процессе репликации из таблиц DTX* в таблицы Rs-Bank 

    g_fictfi             number; -- код fiid, соответствующий в целевой системе корзине бумаг
    deb_flag             number := 0; -- для процедуры логирования. Уровень вложенности записей логов, для отступов

    write_query_log_start date; -- переменная для процедуры write_query_log


    ---- для оценки производительности -----------------------------------------
    deb_start_timestamp timestamp;

    procedure initialize;

    -- сбрасывает счетчик времени. Отсчет начинается с момента запуска этой процедуры 
    -- Полезна, если надо пропустить какие-то запросы, пото замерить отдельный запрос.
    procedure WRITE_LOG_START
    is 
    begin
        write_query_log_start := sysdate;
    end WRITE_LOG_START;

    -- Сохраняет текст в лог производительности, потом сбрасывает счетчик времени. 
    -- Если интересующие нас запросы идут один за другим, можно не использовать отдельную процедуру WRITE_LOG_START 
    procedure WRITE_LOG_FINISH(L_TEXT varchar2, L_OBJECTYPE number, L_SET number := 0, L_NUM number := 0, p_count number := null)
    is
    pragma autonomous_transaction;
    l_cou number;
    begin
        l_cou := nvl(p_count, sql%rowcount);
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



    procedure deals_need_demand_generator 
    is
        l_count number := 0;
    begin
            l_count := 0;
            deb('Запись платежей по needdemand');
            WRITE_LOG_START;
            -- триггеры придется пока отключить, они мешают вставке в режиме direct path
            execute immediate 'alter table ddlrq_dbt disable all triggers';
            
            delete /*+parallel(16) */ from ddlrq_dbt where t_docid in  
            (select tgt_dealid from dtxdeal_tmp where t_replstate=0 and t_needdemand='X');
            WRITE_LOG_FINISH('Удаление старых платежей', 80);        
            commit;
            
            -- ============================================================================
            -- === Обработка NEEDDEMAND (обычные операции и 1-я часть по сделкам РЕПО) ====
            
            -- платеж по первой части по бумагам, плановый
            insert /*+parallel(16) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL, T_VERSION, T_FACTRECEIVERID)
            select ddlrq_dbt_seq.nextval, TGT_OBJTYPE /*T_DOCKIND*/, TGT_DEALID, 1 /*T_DEALPART*/, 
            case when t_kind in (10,30,60) then 0 /*требование*/ else 1/*обязательство*/ end /*T_KIND*/, 
            1 /*T_SUBKIND, бумаги*/, 8 /*TGT_TYPE, поставка бумаг*/, 0 /*T_NUM*/,
            T_AMOUNT /*T_AMOUNT*/, TGT_AVOIRISSID, nvl(TGT_MARKETID,TGT_PARTYID), -1, -1/*T_PLACEID*/, 
            0 /*tgt_state*/, 
            case when tgt_maturityisprincipal=chr(88) then tgt_maturity else tgt_expiry end /*T_PLANDATE*/, 
            date'0001-01-01', CHR(0), CHR(0)/*T_NETTING*/, null, 0, 
            case when tgt_maturityisprincipal=chr(88) then tgt_maturity else tgt_expiry end /*TGT_CHANGEDATE*/,
            0 /*T_ACTION*/, 0, 81, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0, 0, 0
            from dtxdeal_tmp where t_replstate=0 and t_needdemand='X' and t_kind not in (80,90,110) and t_action in (1,2);
            l_count := l_count + sql%rowcount;
            commit;
            
            -- платеж по первой части по бумагам, фактический
            insert /*+parallel(16) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL, T_VERSION, T_FACTRECEIVERID)
            select ddlrq_dbt_seq.nextval, TGT_OBJTYPE /*T_DOCKIND*/, TGT_DEALID, 1 /*T_DEALPART*/, 
            case when t_kind in (10,30,60) then 0 /*требование*/ else 1/*обязательство*/ end /*T_KIND*/, 
            1 /*T_SUBKIND, бумаги*/, 8 /*TGT_TYPE, поставка бумаг*/, 2 /*T_NUM*/,
            T_AMOUNT /*T_AMOUNT*/, TGT_AVOIRISSID, nvl(TGT_MARKETID,TGT_PARTYID), -1, -1/*T_PLACEID*/, 
            2 /*tgt_state*/,  date'0001-01-01',
            case when tgt_maturityisprincipal=chr(88) then tgt_maturity else tgt_expiry end /*T_PLANDATE*/, 
             CHR(0), CHR(0)/*T_NETTING*/, null, 0, date'0001-01-01' /*TGT_CHANGEDATE*/,
            0 /*T_ACTION*/, 0, 82, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0, 0, 0
            from dtxdeal_tmp where t_replstate=0 and t_needdemand='X' and t_kind not in (80,90,110) and t_action in (1,2);
            l_count := l_count + sql%rowcount;
            commit;
            
            -- платеж по первой части по деньгам, плановый
            insert /*+parallel(16) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL, T_VERSION, T_FACTRECEIVERID)
            select ddlrq_dbt_seq.nextval, TGT_OBJTYPE /*T_DOCKIND*/, TGT_DEALID, 1 /*T_DEALPART*/, 
            case when t_kind in (10,30,60) then 1/*обязательство*/ else 0 /*требование*/ end /*T_KIND*/, 
            0 /*T_SUBKIND, деньги*/, 2 /*TGT_TYPE, оплата*/, 0 /*T_NUM*/,
            T_TOTALCOST /*T_AMOUNT*/, TGT_CURRENCYID, nvl(TGT_MARKETID,TGT_PARTYID), -1, -1/*T_PLACEID*/, 
            0 /*tgt_state*/, 
            case when tgt_maturityisprincipal=chr(88) then tgt_expiry else tgt_maturity end /*T_PLANDATE*/, 
            date'0001-01-01', CHR(0), CHR(0)/*T_NETTING*/, null, 0, 
            case when tgt_maturityisprincipal=chr(88) then tgt_expiry else tgt_maturity end /*TGT_CHANGEDATE*/,
            0 /*T_ACTION*/, 0, 83, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0, 0, 0
            from dtxdeal_tmp where t_replstate=0 and t_needdemand='X' and t_action in (1,2);            
            l_count := l_count + sql%rowcount;
            commit;

            -- платеж по первой части по деньгам, фактический
            insert /*+parallel(16) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL, T_VERSION, T_FACTRECEIVERID)
            select ddlrq_dbt_seq.nextval, TGT_OBJTYPE /*T_DOCKIND*/, TGT_DEALID, 1 /*T_DEALPART*/, 
            case when t_kind in (10,30,60) then 1/*обязательство*/ else 0 /*требование*/ end /*T_KIND*/, 
            0 /*T_SUBKIND, деньги*/, 2 /*TGT_TYPE, оплата*/, 2 /*T_NUM*/,
            T_TOTALCOST /*T_AMOUNT*/, TGT_CURRENCYID, nvl(TGT_MARKETID,TGT_PARTYID), -1, -1/*T_PLACEID*/, 
            2 /*tgt_state*/, date'0001-01-01' /*T_PLANDATE*/, 
            case when tgt_maturityisprincipal=chr(88) then tgt_expiry else tgt_maturity end /*T_FACTDATE*/, 
            CHR(0), CHR(0)/*T_NETTING*/, null, 0, date'0001-01-01' /*TGT_CHANGEDATE*/,
            0 /*T_ACTION*/, 0, 84, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0, 0, 0
            from dtxdeal_tmp where t_replstate=0 and t_needdemand='X' and t_action in (1,2);        
            l_count := l_count + sql%rowcount;
            commit;    
            
            WRITE_LOG_FINISH('Заполнение DDLRQ_DBT, 1 часть (t_action 1,2)', 80, p_count=>l_count);
            
            
            
            
            -- ============================================================================
            -- ============ Обработка NEEDDEMAND2 (2-я часть по сделкам РЕПО) =============
            
            l_count := 0;
            -- платеж по второй части по бумагам, плановый
            insert /*+parallel(16) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL, T_VERSION, T_FACTRECEIVERID)
            select ddlrq_dbt_seq.nextval, TGT_OBJTYPE /*T_DOCKIND*/, TGT_DEALID, 2 /*T_DEALPART*/, 
            case when t_kind in (10,30,60) then 0 /*требование*/ else 1/*обязательство*/ end /*T_KIND*/, 
            1 /*T_SUBKIND, бумаги*/, 8 /*TGT_TYPE, поставка бумаг*/, 0 /*T_NUM*/,
            T_AMOUNT /*T_AMOUNT*/, TGT_AVOIRISSID, nvl(TGT_MARKETID,TGT_PARTYID), -1, -1/*T_PLACEID*/, 
            0 /*tgt_state*/, 
            case when tgt_maturityisprincipal2=chr(88) then tgt_maturity2 else tgt_expiry2 end /*T_PLANDATE*/, 
            date'0001-01-01', CHR(0), CHR(0)/*T_NETTING*/, null, 0, 
            case when tgt_maturityisprincipal2=chr(88) then tgt_maturity2 else tgt_expiry2 end /*TGT_CHANGEDATE*/,
            0 /*T_ACTION*/, 0, 85, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0, 0, 0
            from dtxdeal_tmp where t_replstate=0 and t_needdemand2='X' and t_kind not in (80,90,110) and t_action in (1,2) and TGT_ISREPO=chr(88);
            l_count := l_count + sql%rowcount;
            commit;
            
            -- платеж по второй части по бумагам, фактический
            insert /*+parallel(16) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL, T_VERSION, T_FACTRECEIVERID)
            select ddlrq_dbt_seq.nextval, TGT_OBJTYPE /*T_DOCKIND*/, TGT_DEALID, 2 /*T_DEALPART*/, 
            case when t_kind in (10,30,60) then 0 /*требование*/ else 1/*обязательство*/ end /*T_KIND*/, 
            1 /*T_SUBKIND, бумаги*/, 8 /*TGT_TYPE, поставка бумаг*/, 2 /*T_NUM*/,
            T_AMOUNT /*T_AMOUNT*/, TGT_AVOIRISSID, nvl(TGT_MARKETID,TGT_PARTYID), -1, -1/*T_PLACEID*/, 
            2 /*tgt_state*/,  date'0001-01-01',
            case when tgt_maturityisprincipal2=chr(88) then tgt_maturity2 else tgt_expiry2 end /*T_PLANDATE*/, 
             CHR(0), CHR(0)/*T_NETTING*/, null, 0, date'0001-01-01' /*TGT_CHANGEDATE*/,
            0 /*T_ACTION*/, 0, 86, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0, 0, 0
            from dtxdeal_tmp where t_replstate=0 and t_needdemand2='X' and t_kind not in (80,90,110) and t_action in (1,2) and TGT_ISREPO=chr(88);
            l_count := l_count + sql%rowcount;
            commit;
            
            -- платеж по второй части по деньгам, плановый
            insert /*+parallel(16) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL, T_VERSION, T_FACTRECEIVERID)
            select ddlrq_dbt_seq.nextval, TGT_OBJTYPE /*T_DOCKIND*/, TGT_DEALID, 2 /*T_DEALPART*/, 
            case when t_kind in (10,30,60) then 1/*обязательство*/ else 0 /*требование*/ end /*T_KIND*/, 
            0 /*T_SUBKIND, деньги*/, 2 /*TGT_TYPE, оплата*/, 0 /*T_NUM*/,
            T_TOTALCOST2 /*T_AMOUNT*/, TGT_CURRENCYID, nvl(TGT_MARKETID,TGT_PARTYID), -1, -1/*T_PLACEID*/, 
            0 /*tgt_state*/, 
            case when tgt_maturityisprincipal2=chr(88) then tgt_expiry2 else tgt_maturity2 end /*T_PLANDATE*/, 
            date'0001-01-01', CHR(0), CHR(0)/*T_NETTING*/, null, 0, 
            case when tgt_maturityisprincipal2=chr(88) then tgt_expiry2 else tgt_maturity2 end /*TGT_CHANGEDATE*/,
            0 /*T_ACTION*/, 0, 87, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0, 0, 0
            from dtxdeal_tmp where t_replstate=0 and t_needdemand2='X' and t_action in (1,2) and TGT_ISREPO=chr(88);            
            l_count := l_count + sql%rowcount;
            commit;

            -- платеж по второй части по деньгам, фактический
            insert /*+parallel(16) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL, T_VERSION, T_FACTRECEIVERID)
            select ddlrq_dbt_seq.nextval, TGT_OBJTYPE /*T_DOCKIND*/, TGT_DEALID, 2 /*T_DEALPART*/, 
            case when t_kind in (10,30,60) then 1/*обязательство*/ else 0 /*требование*/ end /*T_KIND*/, 
            0 /*T_SUBKIND, деньги*/, 2 /*TGT_TYPE, оплата*/, 2 /*T_NUM*/,
            T_TOTALCOST2 /*T_AMOUNT*/, TGT_CURRENCYID, nvl(TGT_MARKETID,TGT_PARTYID), -1, -1/*T_PLACEID*/, 
            2 /*tgt_state*/, date'0001-01-01' /*T_PLANDATE*/, 
            case when tgt_maturityisprincipal2=chr(88) then tgt_expiry2 else tgt_maturity2 end /*T_FACTDATE*/, 
            CHR(0), CHR(0)/*T_NETTING*/, null, 0, date'0001-01-01' /*TGT_CHANGEDATE*/,
            0 /*T_ACTION*/, 0, 88, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0, 0, 0
            from dtxdeal_tmp where t_replstate=0 and t_needdemand2='X' and t_action in (1,2) and TGT_ISREPO=chr(88);        
            l_count := l_count + sql%rowcount;
            commit;    
            
            WRITE_LOG_FINISH('Заполнение DDLRQ_DBT, 2 часть (t_action 1,2)', 80, p_count=>l_count);
            
            
            
            
            
            
            insert /*+parallel(16)*/into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE)
            select 90, tgt_dealid, p.t_id_step, p.t_id, 1, 0
            from dtxdeal_tmp d join ddlrq_dbt p on (d.tgt_dealid=p.t_docid)
            where d.t_replstate=0 and p.t_action in (1,2) and d.t_needdemand='X';
            commit;
            WRITE_LOG_FINISH('Заполнение DTXREPLOBJ по автоплатежам (t_action 1,2)', 80, p_count=>l_count);
            execute immediate 'alter table ddlrq_dbt enable all triggers';
        end;







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
            'insert /*+ '|| g_parallel_clause ||' */ into dtxdeal_tmp(T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, T_MARKETID, T_SECTOR, T_BROKERID, T_PARTYID, T_DEPARTMENT, T_AVOIRISSID, T_WARRANTID, T_PARTIALID, T_AMOUNT, T_CURRENCYID, T_PRICE, T_POINT, T_COST, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID, T_NETTING_DEALID_DEST)
            select T_DEALID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_KIND, T_EXTCODE, T_MARKETCODE, T_PARTYCODE, T_CODE, T_DATE, T_TIME, T_CLOSEDATE, T_TECHTYPE, T_TSKIND, T_ACCOUNTTYPE, T_MARKETID, T_SECTOR, T_BROKERID, T_PARTYID, T_DEPARTMENT, T_AVOIRISSID, T_WARRANTID, T_PARTIALID, T_AMOUNT, T_CURRENCYID, T_PRICE, T_POINT, T_COST, T_NKD, T_TOTALCOST, T_RATE, T_PRICE2, T_COST2, T_NKD2, T_TOTALCOST2, T_PAYDATE, T_SUPLDATE, T_PAYDATE2, T_SUPLDATE2, T_CONTRNUM, T_CONTRDATE, T_REPOBASE, T_COSTCHANGEONCOMP, T_COSTCHANGE, T_COSTCHANGEONAMOR, T_ADJUSTMENT, T_NEEDDEMAND, T_ATANYDAY, T_CONDITIONS, T_PAYMCUR, T_ISPFI_1, T_ISPFI_2, T_COUNTRY, T_NKDFIID, T_LIMIT, T_CHRATE, T_CHAVR, T_DIV, T_BALANCEDATE, T_DOPCONTROL, T_DOPCONTROL_NOTE, T_FISSKIND, T_PRICE_CALC_METHOD, T_PRICE_CALC, T_PRICE_CALC_VAL, T_PRICE_CALC_DEF, T_PRICE_CALC_OUTLAY, T_PARENTID, T_PRICE_CALC_MET_NOTE, T_NEEDDEMAND2, T_INITBUYDATE, T_CONTROL_DEAL_NOTE, T_CONTROL_DEAL_NOTE_DATE, T_REPO_PROC_ACCOUNT, T_PRIOR_PORTFOLIOID, T_PORTFOLIOID, T_NETTING_DEALID_DEST
            from dtxdeal_dbt where t_instancedate between :1 and :2 and t_replstate=0' using p_startdate, p_enddate;
        deb('Загружено в снимок #1 строк', sql%rowcount);
        
        WRITE_LOG_FINISH('Создаем снимок по сделкам  c ' || to_char(p_startdate, 'dd.mm.yyyy hh24:mi') || ' по '  || to_char(p_enddate, 'dd.mm.yyyy hh24:mi'), 80);
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
            'insert /*+ '|| g_parallel_clause ||' */ into dtxdemand_tmp(T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, T_BALANCERATE, T_NETTING, T_PLANDEMEND, T_NOTE, T_STATE)
            select T_DEMANDID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, T_PART, T_ISFACT, T_KIND, T_DIRECTION, T_FIKIND, T_DATE, T_SUM, T_PAYCURRENCYID, T_PAYSUM, T_PAYRATE, T_BALANCERATE, T_NETTING, T_PLANDEMEND, T_NOTE, T_STATE
            from dtxdemand_dbt where t_instancedate between :1 and :2 and t_replstate=0' using p_startdate, p_enddate;
        deb('Загружено в снимок #1 строк', sql%rowcount);
        
        WRITE_LOG_FINISH('Создаем снимок по платежам  c ' || to_char(p_startdate, 'dd.mm.yyyy hh24:mi') || ' по '  || to_char(p_enddate, 'dd.mm.yyyy hh24:mi'), 90);
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
            'insert /*+ '|| g_parallel_clause ||' */ into DTXCOURSE_TMP(T_COURSEID, T_TYPE, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_BASEFIKIND, T_BASEFIID, T_FIID, T_MARKETID, T_MARKETSECTORID, T_POINT, T_SCALE, T_RATEDATE, T_RATE)
            select T_COURSEID, T_TYPE, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_BASEFIKIND, T_BASEFIID, T_FIID, T_MARKETID, T_MARKETSECTORID, T_POINT, T_SCALE, T_RATEDATE, T_RATE
            from dtxcourse_dbt where t_instancedate between :1 and :2 and t_replstate=0' using p_startdate, p_enddate;
        deb('Загружено в снимок #1 строк', sql%rowcount);
        
        WRITE_LOG_FINISH('Создан снимок по курсам  c ' || to_char(p_startdate, 'dd.mm.yyyy hh24:mi') || ' по '  || to_char(p_enddate, 'dd.mm.yyyy hh24:mi'), 70);
        commit;
        
        deb('Завершена процедура COURSES_CREATE_SNAPSHOT');
    end courses_create_snapshot;    
        
    
    
   
    -- процедура создает снимок по сделкам из DTXDEMAND_DBT в таблицу DTXDEMAND_TMP
    procedure comiss_create_snapshot( p_startdate date, p_enddate date)
    is 
    begin
        deb('Запущена процедура COMISS_CREATE_SNAPSHOT c ' || to_char(p_startdate, 'dd.mm.yyyy hh24:mi') || ' по '  || to_char(p_enddate, 'dd.mm.yyyy hh24:mi'));
        -- получаем снимок из dtxdemand_dbt в dtxdemand_tmp - только тот набор записей, который необходимо реплицировать. Записываем его в таблицу, имеющею пустые поля для индетификаторов целевой системы.

        deb('Очистка таблицы DTXCOMISS_TMP и удаление индексов');
        WRITE_LOG_START;
        execute immediate 'truncate table dtxcomiss_tmp';
        
            for j in (select index_name from user_indexes where table_name='DTXCOMISS_TMP')
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
            'insert /*+ '|| g_parallel_clause ||' */ into DTXCOMISS_TMP(T_COMISSID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, T_TYPE, T_SUM, T_NDS, T_CURRENCYID, T_DATE)
            select T_COMISSID, T_INSTANCEDATE, T_ACTION, T_REPLSTATE, T_DEALID, T_TYPE, T_SUM, T_NDS, T_CURRENCYID, T_DATE
            from dtxcomiss_dbt where t_instancedate between :1 and :2 and t_replstate=0' using p_startdate, p_enddate;
        deb('Загружено в снимок #1 строк', sql%rowcount);
        
        WRITE_LOG_FINISH('Создан снимок по комиссиям  c ' || to_char(p_startdate, 'dd.mm.yyyy hh24:mi') || ' по '  || to_char(p_enddate, 'dd.mm.yyyy hh24:mi'), 100);
        commit;
        
        deb('Завершена процедура COMISS_CREATE_SNAPSHOT');
    end comiss_create_snapshot;     
    
    
    
    
    -- процедура записывает ошибки в лог из таблцы ошибок dtx_error_dbt
    -- только те, которые еще не были запсаны
    procedure write_errors_into_log( p_objecttype number )
    is
    begin
        deb('Запущена процедура WRITE_ERRORS_INTO_LOG');
        WRITE_LOG_START;
        -- обновим поле T_SEVERITY в таблице ошибок. Оно задается на запросе в таблице DTX_QUERY_DBT. Непосредственно при заполнении таблицы ошибок проставиться не может,
        -- поскольку сам запрос не знает своего t_severity, как и остальных своих метаданных
        -- перенесено на момент заполнения таблицы ошибок
        -- merge into (select * from dtx_error_dbt where is_logged is null and t_sessid=G_SESSION_ID and T_DETAILID=g_SESS_DETAIL_ID) tgt
        -- using dtx_query_dbt sou on (sou.t_screenid=tgt.t_queryid)
        -- when matched then update set tgt.T_SEVERITY = sou.T_SEVERITY;
        
        -- теперь внесем в лог ошибки и предупреждения
        insert /*+ append */ into dtxloadlog_dbt(T_MSGTIME, T_MSGCODE, T_SEVERITY, T_OBJTYPE, T_OBJECTID, T_SUBOBJNUM, T_FIELD, T_MESSAGE, T_INSTANCEDATE)
        select er.T_TIMESTAMP, er.T_ERRORCODE, T_SEVERITY, er.T_OBJECTTYPE, er.T_OBJECTID, 0, '', erk.T_DESC,  er.T_INSTANCEDATE
        from dtx_error_dbt er 
        join DTX_SESS_DETAIL_DBT ss on (er.T_SESSID=ss.T_SESSID and er.T_DETAILID=ss.T_DETAILID and er.T_SESSID=g_session_id and er.T_DETAILID=g_SESS_DETAIL_ID) 
        left join dtx_errorkinds_dbt erk on (er.t_errorcode=erk.t_code)
        where er.is_logged is null; 
        
        -- проставим признак записи на записи в таблице ошибок. Теперь можно повторно вызывать процедуру записи в течение сессии, ошибки в логе не будут дублироваться
        update dtx_error_dbt set is_logged=chr(88) where rowid in (select rowid from dtx_error_dbt where is_logged is null and t_sessid=G_SESSION_ID and T_DETAILID=g_SESS_DETAIL_ID);
        WRITE_LOG_FINISH('Переносим найденные ошибки в общий лог', p_objecttype);
        commit;         
        
        WRITE_LOG_START;
        case p_objecttype
        when 80 then
            execute immediate
            'update /*+' || g_parallel_clause || '*/ dtxdeal_tmp set t_replstate=2 where t_replstate=0 and (t_dealid, t_instancedate) in (select t_objectid, t_instancedate from dtx_error_dbt where t_severity=1 and t_objecttype=:1 and t_sessid=:2 and t_detailid=:3)' using p_objecttype, g_SESSION_ID, g_SESS_DETAIL_ID;
        when 90 then
        execute immediate
            'update /*+' || g_parallel_clause || '*/ dtxdemand_tmp set t_replstate=2 where t_replstate=0 and (t_demandid, t_instancedate) in (select t_objectid, t_instancedate from dtx_error_dbt where t_severity=1 and t_objecttype=:1 and t_sessid=:2 and t_detailid=:3)' using p_objecttype, g_SESSION_ID, g_SESS_DETAIL_ID;
        when 70 then
        execute immediate
            'update /*+' || g_parallel_clause || '*/ dtxcourse_tmp set t_replstate=2 where t_replstate=0 and (t_courseid, t_instancedate) in (select t_objectid, t_instancedate from dtx_error_dbt where t_severity=1 and t_objecttype=:1 and t_sessid=:2 and t_detailid=:3)' using p_objecttype, g_SESSION_ID, g_SESS_DETAIL_ID;
        when 100 then
        execute immediate
            'update /*+' || g_parallel_clause || '*/ dtxcomiss_tmp set t_replstate=2 where t_replstate=0 and (t_comissid, t_instancedate) in (select t_objectid, t_instancedate from dtx_error_dbt where t_severity=1 and t_objecttype=:1 and t_sessid=:2 and t_detailid=:3)' using p_objecttype, g_SESSION_ID, g_SESS_DETAIL_ID;
        end case;
        WRITE_LOG_FINISH( 'Отмечаем найденные ошибки в снимке', p_objecttype);
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
            execute immediate 'CREATE INDEX I_PARENTID ON DTXDEAL_TMP(T_PARENTID)';
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
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXCOURSE_ISLASTDATE ON DTXCOURSE_TMP(TGT_ISLASTDATE)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXCOURSE_ISNOMINAL ON DTXCOURSE_TMP(TGT_ISNOMINAL)';
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
            execute immediate 'CREATE INDEX BTREE_DTXDEMAND_DEMANDID ON DTXDEMAND_TMP(T_DEMANDID)';
            execute immediate 'CREATE INDEX BTREE_DTXDEMAND_DEALID ON DTXDEMAND_TMP(T_DEALID)';
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
      


    -- процедура создает индексы на снимке и считает статистику
    procedure comiss_create_indexes
    is 
    begin
        deb('Запущена процедура COMISS_CREATE_INDEXES');
        WRITE_LOG_START;
        deb('Создаем индексы на снимке');
        begin
            execute immediate 'CREATE INDEX I_COMISSID ON DTXCOMISS_TMP(T_COMISSID)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXCOMISS_ACTION ON DTXCOMISS_TMP(T_ACTION)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXCOMISS_KIND ON DTXCOMISS_TMP(T_KIND)';
            execute immediate 'CREATE BITMAP INDEX BMPI_DTXCOMISS_REPLSTATE ON DTXCOMISS_TMP(T_REPLSTATE)';

        exception when others then
            deb('Ошибка при создании индексов на снимке');
        end;
        
        deb('Считаем статистику на снимке');
        dbms_stats.gather_table_stats(user, 'DTXCOMISS_TMP', cascade=>true);
        
        WRITE_LOG_FINISH('Создаем индексы на снимке', 100);    
        
        deb('Завершена процедура COMISS_CREATE_INDEXES');
    end comiss_create_indexes;



    procedure run_one_query_set( p_objecttype number, p_set number )
    is 
        l_text varchar2(2000 char);
    begin
        deb('Запущена процедура RUN_ONE_QUERY_SET для OBJECTTYPE=#1 и сета #2', p_objecttype, p_set);
        -- выполняем заданный сет запросов
        for q_rec in (select * from dtx_query_dbt where t_objecttype=p_objecttype and t_set=p_set and t_in_use='X' order by t_num)
        loop
                begin
                    WRITE_LOG_START;
                    l_text := replace( q_rec.t_text, '#', g_parallel_clause);
                    if q_rec.t_use_bind = 'X' then
                        execute immediate l_text using g_SESSION_ID, g_SESS_DETAIL_ID, q_rec.t_screenid, q_rec.t_severity;
                    else
                        execute immediate l_text;
                    end if;
                    deb('    ' || q_rec.t_name || '.  #1 записей', sql%rowcount);
                    WRITE_LOG_FINISH( q_rec.T_NAME, q_rec.T_OBJECTTYPE, q_rec.T_SET, q_rec.T_NUM);
                    commit; 
                exception
                when others then
                    rollback;
                    deb('Ошибка в запросе: вид объекта ' || p_objecttype || ', номер запроса #1 в сете #2, id запроса #3', q_rec.T_NUM, q_rec.T_SET, q_rec.T_SCREENID);
                    WRITE_LOG_FINISH( '! Ошибка: '  || q_rec.T_NAME, q_rec.T_OBJECTTYPE, q_rec.T_SET, q_rec.T_NUM);
                    -- при выводе ошибочного запроса его параметры заменяются на нужные константы, чтобы можно было выполнить запрос в sql
                    WRITE_LOG_FINISH( replace(replace(replace(replace(l_text,':1',g_SESSION_ID),':2', g_SESS_DETAIL_ID),':2', q_rec.t_screenid),':2', q_rec.t_severity), q_rec.T_OBJECTTYPE, q_rec.T_SET, q_rec.T_NUM);
                    raise;
                end; 
        end loop;
        commit;
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
       
        -- построение индексов вынесено после первого сета, поскольку он правит поля, на которых многие из индеков строятся.
        -- Все запросы первого сета проводятся full table scan`ом
        if    p_objecttype=80 then
                deals_create_indexes;
        elsif p_objecttype=90 then
                demands_create_indexes;
        elsif p_objecttype=70 then
                courses_create_indexes;
        elsif p_objecttype=100 then
                comiss_create_indexes;
        end if;


        deb('Второй сет запросов. Заполнение таблицы ошибок');
        -- Второй сет запросов, проверяем заполнение полей, определяем ошибки
       
        run_one_query_set(p_objecttype, 2);

        -- переносим ошибки в таблицу логов dtxloadlog_dbt и проставляем признак ошибки в таблиицу tmp (t_replstate=2)
        write_errors_into_log( p_objecttype );        

        deb('Третий сет запросов. Обогащение идентификаторами из целевой системы');

        run_one_query_set(p_objecttype, 3);

        deb('Четвертый сет запросов. Проверка по бизнес-правилам');

        run_one_query_set(p_objecttype, 4);

        -- переносим ошибки в таблицу логов dtxloadlog_dbt и проставляем признак ошибки в таблиицу tmp (t_replstate=2)
        -- только новые ошибки
        write_errors_into_log( p_objecttype );        
        
        deb('Пятый сет запросов.');

        run_one_query_set(p_objecttype, 5);        
        
        
        l_dur_interval := current_timestamp - l_perf_start;
        l_dur_min := extract(minute from l_dur_interval);
        l_dur_sec := extract(second from l_dur_interval); 
        deb('Завершена процедура RUN_ALL_QUERIES. Продолжительность - #1:#2', l_dur_min, l_dur_sec);
    end RUN_ALL_QUERIES;



    procedure deals_create_basket_records
    is
    begin
    
        return;
        -- обработка движений обеспечения по сделкам РЕПО на корзину
        execute immediate 'alter table DDL_TICK_ENS_DBT disable all triggers';
        execute immediate 'alter table DDLRQ_DBT disable all triggers';
        WRITE_LOG_START;
        insert /*+parallel(16) */ into DDL_TICK_ENS_DBT(T_ID, T_DEALID, T_FIID, T_DATE, T_KIND, T_PRINCIPAL, T_TOTALCOST, T_COSTFIID, T_NKD)
        select  TGT_DEALID /*T_ID*/, TGT_PARENTID /*T_DEALID*/, TGT_AVOIRISSID /*T_FIID*/, T_DATE /*T_DATE*/, TGT_BS_DIRECTION /*T_KIND*/,
                T_AMOUNT, T_TOTALCOST, TGT_CURRENCYID, T_NKD
        from dtxdeal_tmp where t_replstate=0 and tgt_bs_isfictive = chr(88) and t_action = 1;
        COMMIT;
        
        
        
        
        WRITE_LOG_FINISH('Вставка в DDL_TICK_ENS_DBT (РЕПО с корзиной)', 80);        
        execute immediate 'alter table DDLRQ_DBT enable all triggers';
        execute immediate 'alter table DDL_TICK_ENS_DBT enable all triggers';
    
    end deals_create_basket_records;






    -- формирует записи в целевой системе на основе таблицы снимка
    procedure DEALS_CREATE_RECORDS
    is
        l_count number := 0; -- будет счетчиком обработанных строк
    begin
        deb('Запущена процедура DEALS_CREATE_RECORDS');
        
        -- Обработка вставок
        WRITE_LOG_START;
        
        -- триггеры придется пока отключить, они мешают вставке в режиме direct path
        execute immediate 'alter table DDL_TICK_DBT disable all triggers';
        execute immediate 'alter table DDL_LEG_DBT disable all triggers';
        execute immediate 'alter table DDLRQ_DBT disable all triggers';
        
        
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
                TGT_DEALID /*T_DEALID*/, TGT_BOFFICEKIND /*T_BOFFICEKIND*/, TGT_DEALKIND /*T_DEALTYPE*/, 0 /*T_DEALGROUP*/, 0 /*T_TRADESYSTEM*/, 
                T_CODE /*T_DEALCODE*/, T_EXTCODE /*T_DEALCODETS*/, t_tskind /*T_TYPEDOC*/, chr(1) /*T_USERTYPEDOC*/, nvl(TGT_PARTYID,-1) /*T_PARTYID*/, 
                nvl(TGT_BROKERID,-1) /*T_BROKERID*/, -1 /*T_CLIENTID*/, -1 /*T_TRADERID*/, -1 /*T_DEPOSITID*/, nvl(TGT_MARKETID,-1) /*T_MARKETID*/, 
                0 /*T_INDOCID*/, T_DATE /*T_DEALDATE*/, T_DATE /*T_REGDATE*/, 10 /*T_DEALSTATUS*/, 0 /*T_NUMBERPACK*/, 
                tgt_department /*T_DEPARTMENT*/, g_oper /*T_OPER*/, 0 /*T_ORIGINID*/, chr(1) /*T_EXTERNID*/, case when tgt_marketid>0 then 'X' else chr(0) end /*T_FLAG1*/, 
                NULL /*T_FLAG2*/, NULL /*T_FLAG3*/, NULL /*T_FLAG4*/, case when T_COSTCHANGE=chr(88) then chr(88) else chr(0) end/*T_FLAG5*/, chr(1) /*T_USERFIELD1*/, 
                chr(1) /*T_USERFIELD2*/, chr(1) /*T_USERFIELD3*/, chr(1) /*T_USERFIELD4*/, chr(1) /*T_COMMENT*/, t_closedate /*T_CLOSEDATE*/, 
                chr(1) /*T_SHIELD*/, 0 /*T_SHIELDSIZE*/, NULL /*T_ISPERCENT*/, 0 /*T_SCALE*/, 4 /*T_POINTS*/, 
                NULL /*T_REVRATE*/, 0 /*T_COLLATERAL*/, T_TIME /*T_DEALTIME*/, TGT_PORTFOLIOID /*T_PORTFOLIOID*/, 0 /*T_BUNDLE*/, 
                0 /*T_CBRISKGROUP*/, 0 /*T_RISKGROUP*/, 0 /*T_ATTRIBUTES*/, 0 /*T_PRODUCT*/, 0 /*T_NETTING*/, 
                chr(1) /*T_DEALCODEPS*/, 0 /*T_CONFTPID*/, 0 /*T_LINKCHANNEL*/, nvl(to_char(TGT_WARRANT_NUM),chr(1)) /*T_NUMBER_COUPON*/, nvl(tgt_sector,0) /*T_MARKETOFFICEID*/, 
                0 /*T_CLIENTCONTRID*/, 0 /*T_BROKERCONTRID*/, chr(1) /*T_INDOCCODE*/, 0 /*T_PREOUTLAY*/, 0 /*T_PREOUTLAYFIID*/, 
                0 /*T_GROUNDID*/, 1 /*T_BUYGOAL*/, date'0001-01-01' /*T_COMMDATE*/, 0 /*T_PAYMENTSMETHOD*/, 0 /*T_FIXSUM*/, 
                chr(1) /*T_NUMBER_PARTLY*/, date'0001-01-01' /*T_CHANGEDATE*/, 0 /*T_INSTANCE*/, 0 /*T_CHANGEKIND*/, TGT_PORTFOLIOID_2 /*T_PORTFOLIOID_2*/, 
                NULL /*T_ISPARTYCLIENT*/, 0 /*T_PARTYCONTRID*/, 0 /*T_BRANCH*/, 0 /*T_AVOIRKIND*/, NULL /*T_OFBU*/, 
                0 /*T_MARKETSCHEMEID*/, 0 /*T_DEPSETID*/, case when t_costchange=chr(88) then 2 else 0 end /*T_RETURNINCOMEKIND*/, 0 /*T_REQUESTID*/, NULL /*T_BLOCKED*/, 
                nvl(tgt_country,chr(1)) /*T_COUNTRY*/, tgt_avoirissid /*T_PFI*/, NULL /*T_ISINSTANCY*/, 0 /*T_GENAGRID*/, 0 /*T_PARENTID*/, 
                NULL /*T_ISNETTING*/, 0 /*T_VERSION*/, NULL /*T_CARRYWRT*/, NULL /*T_COUPONNDFL*/, NULL /*T_PROGNOS*/, 
                NULL /*T_ISTRADEFINANCE*/, NULL /*T_SECTOR*/, NULL /*T_INCLUDE_DAY*/, NULL /*T_AUTOCLOSE*/, NULL /*T_OPENDATE*/, 
                0 /*T_KINDDEALPARTYCLIENT*/, 0 /*T_CURDEAL*/, NULL /*T_TAXHANDLE*/, 0 /*T_CURPAY*/, 0 /*T_CURGET*/, 
                date'0001-01-01' /*T_TAXOWNBEGDATE*/, 0 /*T_DISCONT*/, 0 /*T_FACTRECEIVERID*/, NULL /*T_ISCONFIRMED*/, NULL /*T_ISREADY*/, 
                0 /*T_SUMPAY*/, NULL /*T_NEGATIVERATE*/, NULL /*T_ASSIGNMENT*/, NULL /*T_WITHPERCENT*/, 0 /*T_LIMITCUR*/, 
                0 /*T_DEBTLIMIT*/, 0 /*T_ISSUANCELIMIT*/, 0 /*T_TAX_AMOUNT*/, 0 /*T_CREDIT_TAX_AMOUNT*/, 0 /*T_CREDIT_TAX_CUR*/, 
                0 /*T_CREDIT_TAX_TERM*/, NULL /*T_PLACEMENT*/, NULL /*T_OFFER*/, 0 /*T_FLAGTYPEDEAL*/, NULL /*T_CASHDECISION*/, 
                NULL /*T_ISPFI*/, 0 /*T_ADJDATETYPE*/, date'0001-01-01' /*T_SUBORDINATEDDATE*/, NULL /*T_CAPITALNOTINCLUDED*/, NULL /*T_ISSPOT*/, 
                0 /*T_CALCPFI*/, 0 /*T_PAYMAGENT*/, NULL /*T_AUTOPLACEMENT*/
        from dtxdeal_tmp 
        where t_replstate=0 and t_action=1 and tgt_bs_isfictive is null;   
        WRITE_LOG_FINISH('Заполнение DDL_TICK_DBT (t_action=1)', 80);
        
        deb('Заполнение DDL_LEG_DBT по первой части (t_action=1)');
        insert /*+parallel(16)*/ into ddl_leg_dbt(
                T_ID, T_DEALID, T_LEGID, T_PFI, T_CFI, T_START, T_MATURITY, T_EXPIRY, T_PRINCIPAL, T_PRICE, T_BASIS, T_DURATION, T_PITCH, T_COST, T_MODE, T_CLOSED, T_REFRATE, T_FACTOR, T_FORMULA, T_VERSION, 
                T_RESERVE0, T_PERIODNUMBER, T_PERIODTYPE, T_DIFF, T_PAYDAY, T_LEGKIND, T_SCALE, T_POINT, T_ISCALCUSED, T_LEGNUMBER, T_RELATIVEPRICE, T_NKD, T_TOTALCOST, T_MATURITYISPRINCIPAL, T_REGISTRAR, 
                T_INCOMERATE, T_INCOMESCALE, T_INCOMEPOINT, T_INTERESTSTART, T_RECEIPTAMOUNT, T_REGISTRARCONTRID, T_PRINCIPALBASE, T_PRINCIPALDIFF, T_STARTBASE, T_STARTDIFF, T_BASE, T_PAYREGTAX, T_RETURNINCOME, T_REJECTDATE, T_DELIVERINGFIID, 
                T_BITMASK, T_OPERSTATE, T_SUPPLYTIME, T_TABLEPERCENT, T_TYPEPERCENT, T_CAPTION, T_TYPEDATE, T_COUNTDAY, T_CORRECT, T_PAYFIID, T_NKDFIID, T_SROK, T_CLIRINGDATE, T_CLIRINGCHANGE, T_DEPOSITID, T_DVP, T_CLIRINGTIME, T_SESSION, T_D1, T_D2, 
                T_DMAXPAY, T_QRETURN, T_QRECEIVE, T_SUMINCOME, T_MOVEDATE, T_NOTICEDURATION, T_PLANMATURITY)
        select
                DDL_LEG_DBT_SEQ.nextval, TGT_DEALID /*T_DEALID*/, 0 /*T_LEGID*/, TGT_AVOIRISSID /*T_PFI*/, tgt_currencyid /*T_CFI*/, 
                date'0001-01-01' /*T_START*/, TGT_MATURITY /*T_MATURITY*/, TGT_EXPIRY /*T_EXPIRY*/, t_amount /*T_PRINCIPAL*/, tgt_price /*T_PRICE*/, 
                case when T_REPOBASE>0 then T_REPOBASE else 0 end /*T_BASIS*/, 0 /*T_DURATION*/, 0 /*T_PITCH*/, T_COST /*T_COST*/, 0 /*T_MODE*/, 
                date'0001-01-01' /*T_CLOSED*/, 0 /*T_REFRATE*/, 0 /*T_FACTOR*/, TGT_FORMULA /*T_FORMULA*/, 0 /*T_VERSION*/, 
                chr(1) /*T_RESERVE0*/, 0 /*T_PERIODNUMBER*/, 0 /*T_PERIODTYPE*/, 0 /*T_DIFF*/, 0 /*T_PAYDAY*/, 
                0 /*T_LEGKIND*/, 1 /*T_SCALE*/, 4 /*T_POINT*/, NULL /*T_ISCALCUSED*/, chr(1) /*T_LEGNUMBER*/, 
                TGT_RELATIVEPRICE /*T_RELATIVEPRICE*/, T_NKD /*T_NKD*/, T_TOTALCOST /*T_TOTALCOST*/, TGT_MATURITYISPRINCIPAL /*T_MATURITYISPRINCIPAL*/, 0 /*T_REGISTRAR*/, 
                T_RATE /*T_INCOMERATE*/, 1 /*T_INCOMESCALE*/, 2 /*T_INCOMEPOINT*/, date'0001-01-01' /*T_INTERESTSTART*/, 0 /*T_RECEIPTAMOUNT*/, 
                0 /*T_REGISTRARCONTRID*/, 0 /*T_PRINCIPALBASE*/, 0 /*T_PRINCIPALDIFF*/, 0 /*T_STARTBASE*/, 0 /*T_STARTDIFF*/, 
                0 /*T_BASE*/, NULL /*T_PAYREGTAX*/, 0 /*T_RETURNINCOME*/, date'0001-01-01' /*T_REJECTDATE*/, -1 /*T_DELIVERINGFIID*/, 
                0 /*T_BITMASK*/, 0 /*T_OPERSTATE*/, date'0001-01-01' /*T_SUPPLYTIME*/, NULL /*T_TABLEPERCENT*/, 0 /*T_TYPEPERCENT*/, 
                0 /*T_CAPTION*/, 0 /*T_TYPEDATE*/, 0 /*T_COUNTDAY*/, 0 /*T_CORRECT*/, 0 /*T_PAYFIID*/, 
                0 /*T_NKDFIID*/, NULL /*T_SROK*/, date'0001-01-01' /*T_CLIRINGDATE*/, NULL /*T_CLIRINGCHANGE*/, 0 /*T_DEPOSITID*/, 
                NULL /*T_DVP*/, date'0001-01-01' /*T_CLIRINGTIME*/, 0 /*T_SESSION*/, 0 /*T_D1*/, 0 /*T_D2*/, 
                date'0001-01-01' /*T_DMAXPAY*/, 0 /*T_QRETURN*/, 0 /*T_QRECEIVE*/, 0 /*T_SUMINCOME*/, date'0001-01-01' /*T_MOVEDATE*/, 
                0 /*T_NOTICEDURATION*/, date'0001-01-01' /*T_PLANMATURITY*/
                from dtxdeal_tmp 
                where t_replstate=0 and t_action=1 and tgt_bs_isfictive is null;
        WRITE_LOG_FINISH('Заполнение DDL_LEG_DBT по первой части (t_action=1)', 80);
        commit;
        
        deb('Заполнение DDL_LEG_DBT по второй части (t_action=1)');
        insert /*+parallel(16) */ into ddl_leg_dbt(
                T_ID, T_DEALID, T_LEGID, T_PFI, T_CFI, T_START, T_MATURITY, T_EXPIRY, T_PRINCIPAL, T_PRICE, T_BASIS, T_DURATION, T_PITCH, T_COST, T_MODE, T_CLOSED, T_REFRATE, T_FACTOR, T_FORMULA, T_VERSION, 
                T_RESERVE0, T_PERIODNUMBER, T_PERIODTYPE, T_DIFF, T_PAYDAY, T_LEGKIND, T_SCALE, T_POINT, T_ISCALCUSED, T_LEGNUMBER, T_RELATIVEPRICE, T_NKD, T_TOTALCOST, T_MATURITYISPRINCIPAL, T_REGISTRAR, 
                T_INCOMERATE, T_INCOMESCALE, T_INCOMEPOINT, T_INTERESTSTART, T_RECEIPTAMOUNT, T_REGISTRARCONTRID, T_PRINCIPALBASE, T_PRINCIPALDIFF, T_STARTBASE, T_STARTDIFF, T_BASE, T_PAYREGTAX, T_RETURNINCOME, T_REJECTDATE, T_DELIVERINGFIID, 
                T_BITMASK, T_OPERSTATE, T_SUPPLYTIME, T_TABLEPERCENT, T_TYPEPERCENT, T_CAPTION, T_TYPEDATE, T_COUNTDAY, T_CORRECT, T_PAYFIID, T_NKDFIID, T_SROK, T_CLIRINGDATE, T_CLIRINGCHANGE, T_DEPOSITID, T_DVP, T_CLIRINGTIME, T_SESSION, T_D1, T_D2, 
                T_DMAXPAY, T_QRETURN, T_QRECEIVE, T_SUMINCOME, T_MOVEDATE, T_NOTICEDURATION, T_PLANMATURITY)
        select
                DDL_LEG_DBT_SEQ.nextval, TGT_DEALID /*T_DEALID*/, 0 /*T_LEGID*/, TGT_AVOIRISSID /*T_PFI*/, tgt_currencyid /*T_CFI*/, 
                NULL /*T_START*/, TGT_MATURITY2 /*T_MATURITY*/, TGT_EXPIRY2 /*T_EXPIRY*/, t_amount /*T_PRINCIPAL*/, t_price2 /*T_PRICE*/, 
                case when T_REPOBASE>0 then T_REPOBASE  else 0 end  /*T_BASIS*/, 0 /*T_DURATION*/, 0 /*T_PITCH*/, T_COST2 /*T_COST*/, 0 /*T_MODE*/, 
                date'0001-01-01' /*T_CLOSED*/, 0 /*T_REFRATE*/, 0 /*T_FACTOR*/, TGT_FORMULA /*T_FORMULA*/, 0 /*T_VERSION*/, 
                chr(1) /*T_RESERVE0*/, 0 /*T_PERIODNUMBER*/, 0 /*T_PERIODTYPE*/, 0 /*T_DIFF*/, 0 /*T_PAYDAY*/, 
                2 /*T_LEGKIND*/, 1 /*T_SCALE*/, 4 /*T_POINT*/, NULL /*T_ISCALCUSED*/, chr(1) /*T_LEGNUMBER*/, 
                TGT_RELATIVEPRICE /*T_RELATIVEPRICE*/, T_NKD2 /*T_NKD*/, T_TOTALCOST2 /*T_TOTALCOST*/, TGT_MATURITYISPRINCIPAL2 /*T_MATURITYISPRINCIPAL*/, 0 /*T_REGISTRAR*/, 
                T_RATE /*T_INCOMERATE*/, 1 /*T_INCOMESCALE*/, 2 /*T_INCOMEPOINT*/, date'0001-01-01' /*T_INTERESTSTART*/, 0 /*T_RECEIPTAMOUNT*/, 
                0 /*T_REGISTRARCONTRID*/, 0 /*T_PRINCIPALBASE*/, 0 /*T_PRINCIPALDIFF*/, 0 /*T_STARTBASE*/, 0 /*T_STARTDIFF*/, 
                0 /*T_BASE*/, NULL /*T_PAYREGTAX*/, 0 /*T_RETURNINCOME*/, date'0001-01-01' /*T_REJECTDATE*/, -1 /*T_DELIVERINGFIID*/, 
                0 /*T_BITMASK*/, 0 /*T_OPERSTATE*/, date'0001-01-01' /*T_SUPPLYTIME*/, NULL /*T_TABLEPERCENT*/, 0 /*T_TYPEPERCENT*/, 
                0 /*T_CAPTION*/, 0 /*T_TYPEDATE*/, 0 /*T_COUNTDAY*/, 0 /*T_CORRECT*/, 0 /*T_PAYFIID*/, 
                0 /*T_NKDFIID*/, NULL /*T_SROK*/, date'0001-01-01' /*T_CLIRINGDATE*/, NULL /*T_CLIRINGCHANGE*/, 0 /*T_DEPOSITID*/, 
                NULL /*T_DVP*/, date'0001-01-01' /*T_CLIRINGTIME*/, 0 /*T_SESSION*/, 0 /*T_D1*/, 0 /*T_D2*/, 
                date'0001-01-01' /*T_DMAXPAY*/, 0 /*T_QRETURN*/, 0 /*T_QRECEIVE*/, 0 /*T_SUMINCOME*/, date'0001-01-01' /*T_MOVEDATE*/, 
                0 /*T_NOTICEDURATION*/, date'0001-01-01' /*T_PLANMATURITY*/
                from dtxdeal_tmp 
                where t_replstate=0 and t_action=1 and TGT_ISREPO=chr(88) and tgt_bs_isfictive is null;
        WRITE_LOG_FINISH('Заполнение DDL_LEG_DBT по второй части (t_action=1)', 80);
        commit;      
        
        
        -- Обработка изменений        
        deb('Изменение DDL_TICK_DBT (t_action=2)');
        
        MERGE /*+parallel(16)*/INTO ddl_tick_dbt tgt
        USING (select * from dtxdeal_tmp where t_action=2 and t_replstate=0 and tgt_bs_isfictive is null) sou on (tgt.t_dealid=sou.tgt_dealid)
        WHEN MATCHED THEN UPDATE SET 
                T_BOFFICEKIND=TGT_BOFFICEKIND /*T_BOFFICEKIND*/,                                                T_DEALTYPE=TGT_DEALKIND /*T_DEALTYPE*/,  
                T_DEALCODE=T_CODE /*T_DEALCODE*/,       T_DEALCODETS=T_EXTCODE /*T_DEALCODETS*/,                T_TYPEDOC=t_tskind /*T_TYPEDOC*/, 
                T_PARTYID=nvl(TGT_PARTYID,-1) /*T_PARTYID*/,    T_BROKERID=nvl(TGT_BROKERID,-1) /*T_BROKERID*/, T_MARKETID=nvl(TGT_MARKETID,-1) /*T_MARKETID*/, 
                T_DEALDATE=T_DATE /*T_DEALDATE*/,       T_REGDATE=T_DATE /*T_REGDATE*/,                         T_DEALSTATUS=2 /*T_DEALSTATUS*/,  
                T_DEPARTMENT=tgt_department /*T_DEPARTMENT*/,       T_OPER=g_oper /*T_OPER*/,                   T_FLAG1=case when tgt_marketid>0 then 'X' else chr(0) end /*T_FLAG1*/, 
                T_FLAG5=case when T_COSTCHANGE=chr(88) then chr(88) else chr(0) end/*T_FLAG5*/,  
                T_SCALE=1 /*T_SCALE*/,                  T_POINTS=4 /*T_POINTS*/,                                T_DEALTIME=T_TIME /*T_DEALTIME*/, 
                T_PORTFOLIOID=TGT_PORTFOLIOID /*T_PORTFOLIOID*/,                                                T_NUMBER_COUPON=TGT_WARRANT_NUM /*T_NUMBER_COUPON*/, 
                T_MARKETOFFICEID=tgt_sector /*T_MARKETOFFICEID*/,                                               T_PORTFOLIOID_2=TGT_PORTFOLIOID_2 /*T_PORTFOLIOID_2*/, 
                T_AVOIRKIND=TGT_AVOIRKIND /*T_AVOIRKIND*/,                                                      T_RETURNINCOMEKIND=case when t_costchange=chr(88) then 2 end /*T_RETURNINCOMEKIND*/,  
                T_COUNTRY=tgt_country /*T_COUNTRY*/,    T_PFI=tgt_avoirissid /*T_PFI*/;
        WRITE_LOG_FINISH('Обновление DDL_TICK_DBT (t_action=2)', 80);
        commit;
                
        MERGE /*+parallel(16)*/INTO (select * from ddl_leg_dbt where t_legkind=0) tgt
        USING (select * from dtxdeal_tmp where t_action=2 and t_replstate=0 and tgt_bs_isfictive is null) sou on (tgt.t_dealid=sou.tgt_dealid)
        WHEN MATCHED THEN UPDATE SET 
                T_PFI=TGT_AVOIRISSID /*T_PFI*/,         T_CFI=tgt_currencyid /*T_CFI*/,                         T_MATURITY=TGT_MATURITY /*T_MATURITY*/, 
                T_EXPIRY=TGT_EXPIRY /*T_EXPIRY*/,       T_PRINCIPAL=t_amount /*T_PRINCIPAL*/,                   T_PRICE=tgt_price /*T_PRICE*/, 
                T_BASIS=case when T_REPOBASE>0 then T_REPOBASE - 1 else 0 end /*T_BASIS*/,                      T_COST=T_COST /*T_COST*/,  
                T_FORMULA=TGT_FORMULA /*T_FORMULA*/,    T_SCALE=1 /*T_SCALE*/,                                  T_POINT=4 /*T_POINT*/,  
                T_RELATIVEPRICE=TGT_RELATIVEPRICE /*T_RELATIVEPRICE*/,  T_NKD=T_NKD /*T_NKD*/,                  T_TOTALCOST=T_TOTALCOST /*T_TOTALCOST*/, 
                T_MATURITYISPRINCIPAL=TGT_MATURITYISPRINCIPAL /*T_MATURITYISPRINCIPAL*/,                        T_INCOMERATE=T_RATE /*T_INCOMERATE*/;
        WRITE_LOG_FINISH('Обновление DDL_LEG_DBT по первой части (t_action=2)', 80);
        commit;
        
        MERGE /*+parallel(16)*/INTO (select * from ddl_leg_dbt where t_legkind=2) tgt
        USING (select * from dtxdeal_tmp where t_action=2 and t_replstate=0 and tgt_bs_isfictive is null) sou on (tgt.t_dealid=sou.tgt_dealid)
        WHEN MATCHED THEN UPDATE SET 
                T_PFI=TGT_AVOIRISSID /*T_PFI*/,         T_CFI=tgt_currencyid /*T_CFI*/,                         T_MATURITY=TGT_MATURITY2 /*T_MATURITY*/, 
                T_EXPIRY=TGT_EXPIRY2 /*T_EXPIRY*/,      T_PRINCIPAL=t_amount /*T_PRINCIPAL*/,                   T_PRICE=T_PRICE2 /*T_PRICE*/, 
                T_BASIS=case when T_REPOBASE>0 then T_REPOBASE - 1 else 0 end /*T_BASIS*/,                      T_COST=T_COST2 /*T_COST*/,  
                T_FORMULA=TGT_FORMULA /*T_FORMULA*/,    T_SCALE=1 /*T_SCALE*/,                                  T_POINT=4 /*T_POINT*/,  
                T_RELATIVEPRICE=TGT_RELATIVEPRICE /*T_RELATIVEPRICE*/,                                          T_NKD=T_NKD2 /*T_NKD*/,
                T_TOTALCOST=T_TOTALCOST2 /*T_TOTALCOST*/,                                                       T_MATURITYISPRINCIPAL=TGT_MATURITYISPRINCIPAL2 /*T_MATURITYISPRINCIPAL*/,                        
                T_INCOMERATE=T_RATE /*T_INCOMERATE*/;
        WRITE_LOG_FINISH('Обновление DDL_LEG_DBT по второй части (t_action=2)', 80);
        commit;
        
        -- Обработка удалений
        deb('Удаление из DDL_TICK_DBT (t_action=3)');    
        delete /*+parallel(16) */ from ddl_tick_dbt where t_dealid in (select tgt_dealid from dtxdeal_tmp where t_replstate=0 and t_action=3 and tgt_bs_isfictive is null);
        delete /*+parallel(16) */ from ddl_leg_dbt where t_dealid in (select tgt_dealid from dtxdeal_tmp where t_replstate=0 and t_action=3 and tgt_bs_isfictive is null);
        delete /*+parallel(16) */ from ddlrq_dbt where t_docid in (select tgt_dealid from dtxdeal_tmp where t_replstate=0 and t_action=3 and tgt_bs_isfictive is null); 
        WRITE_LOG_FINISH('Удаление из DDL_TICK_DBT/DDL_LEG_DBT (t_action=3)', 80);
        commit;
        
        execute immediate 'alter table DDL_TICK_DBT enable all triggers';
        execute immediate 'alter table DDL_LEG_DBT enable all triggers';
        execute immediate 'alter table DDLRQ_DBT enable all triggers';
        ----------------------------------------------------------------------------

        -- Запись движений обеспечения по сделкам РЕПО на корзину.
        deals_create_basket_records;

        ----------------------------------------------------------------------------
        -- Запись отсутствующих договоров обслуживания
        
        WRITE_LOG_START;
        insert into dsfcontr_dbt(T_ID, T_OBJECTTYPE, T_FIID, T_OBJECT, T_PARTYID, T_NUMBER, T_DATEBEGIN, T_DATEPROLONG, T_DATECLOSE, T_PAYMETHOD, T_NAME, T_DATECONC, T_CONTRACTORID, T_SERVKIND, T_SERVKINDSUB, T_SETACCSEARCHALG, T_INVMETHOD, T_PAYFIID, T_PAYRATEID, T_PAYRATEPERCENT, T_PAYRATEDATEKIND, T_INVOICEDURATION, T_DEPARTMENT, T_ACCCODE, T_BRANCH, T_UNIONCONTRID, T_PREACPTID)
        select dsfcontr_dbt_seq.nextval, 0, -1, null /*T_OBJECT*/, tgt_partyid /*T_PARTYID*/, 0, date'2000-01-01' /*T_DATEBEGIN*/, date'0001-01-01' /*T_DATEPROLONG*/, date'0001-01-01' /*T_DATECLOSE*/, 1, 'Договор обслуживания '||t_shortname, date'2000-01-01' /*T_DATECONC*/,
        g_ourbank /*T_CONTRACTORID*/, 1, 0, 1, 1, 0 /*T_PAYFIID*/, 7, 0, 2, 7, 1, dsfcontr_dbt_seq.currval /*T_ACCCODE*/, 0, 0, 0
        from (select tgt_partyid from dtxdeal_tmp where t_replstate=0 and tgt_bs_isfictive is null and t_action in (1,2) group by tgt_partyid) dl, dparty_dbt pt where dl.tgt_partyid=pt.t_partyid
        and tgt_partyid not in (select t_partyid from dsfcontr_dbt);
        WRITE_LOG_FINISH('Запись отсутствующих договоров обслуживания', 80);
        commit;

        ----------------------------------------------------------------------------

        -- Запись в DTXREPLOBJ_DBT
        WRITE_LOG_START;
        deb('Запись в DTXREPLOBJ_DBT');
        delete /*+parallel(16) */ from dtxreplobj_dbt where T_OBJECTTYPE=80 and T_OBJECTID in (select t_dealid from dtxdeal_tmp where t_replstate=0 and t_action=1);
        WRITE_LOG_FINISH('Запись в DTXREPLOBJ_DBT - подготовка', 80);
        commit;
        
        WRITE_LOG_START;
        update /*+parallel(16) */ dtxreplobj_dbt set t_objstate=2 where T_OBJECTTYPE=80 and t_objectid in (select t_dealid from dtxdeal_tmp where t_replstate=0 and t_action=3);
        WRITE_LOG_FINISH('Запись в DTXREPLOBJ_DBT - удаления', 80);
        commit;
        
        WRITE_LOG_START;
        insert /*+parallel(16) */ into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE)
        select 80, t_dealid, 0, tgt_dealid, 0, 0  from dtxdeal_tmp where t_replstate=0 and t_action=1;
        WRITE_LOG_FINISH('Запись в DTXREPLOBJ_DBT - вставки', 80);
        commit;
        
        
        
        -- Запись REPLSTATE в DTXDEAL_DBT
        WRITE_LOG_START;
        deb('Запись REPLSTATE в DTXDEAL_DBT');
        update /*+parallel(16) */ dtxdeal_dbt tgt 
        set tgt.t_replstate=1
        where (t_dealid, t_instancedate) in (select t_dealid, t_instancedate from dtxdeal_tmp where t_replstate=0);
        WRITE_LOG_FINISH('Запись REPLSTATE в DTXDEAL_DBT', 80);
        commit;
        
        ----------------------------------------------------------------------------
        WRITE_LOG_START;
        deb('Запись в DSPGROUND_DBT / DSPGRDOC_DBT');
        execute immediate 'alter table DSPGROUND_DBT disable all triggers';
        execute immediate 'alter table DSPGRDOC_DBT disable all triggers';
        delete /*+parallel(16)*/from dspground_dbt where t_kind=26 and t_DocLog=513 and  t_AltXld in (select T_CONTRNUM from dtxdeal_tmp where t_replstate=0 and t_action in (1,2) );
        commit;
        insert /*+parallel(16)*/into dspground_dbt( t_spgroundid, t_kind, t_DocLog, t_Direction, t_Receptionist, t_References, t_AltXld, t_SignedDate, t_Party)
        select dspground_dbt_seq.nextval, 26, 513, 2, 11, 1, T_CONTRNUM, md, mp from (
        select T_CONTRNUM, max(T_CONTRDATE) md, max(TGT_PARTYID) mp from dtxdeal_tmp where t_replstate=0 and t_action in (1,2) group by T_CONTRNUM);
        commit;
        
        delete /*+parallel(16) */ dspgrdoc_dbt where (t_SourceDocKind, t_SourceDocID) in (select tgt_bofficekind, tgt_dealid from dtxdeal_tmp where t_replstate=0); 
        commit;
        
        insert /*+parallel(16)*/ into dspgrdoc_dbt( t_SourceDocKind, t_SourceDocID, t_SPGroundID)
        select tgt_bofficekind, tgt_dealid, t_SPGroundID
        from dtxdeal_tmp dl join dspground_dbt dc on ( dl.t_contrnum=dc.t_AltXld and dl.tgt_partyid=dc.t_party and t_replstate=0 and t_action in (1,2) );
        WRITE_LOG_FINISH('Запись в DSPGROUND_DBT / DSPGRDOC_DBT', 80);
        commit;
        execute immediate 'alter table DSPGROUND_DBT enable all triggers';
        execute immediate 'alter table DSPGRDOC_DBT enable all triggers';
        
        --==========================================================================
        --==========================================================================
        
        WRITE_LOG_START;
        deb('Запись примечаний к сделкам'); 
        
        -- триггеры придется пока отключить, они мешают вставке строк в режиме direct path
        execute immediate 'alter table dnotetext_dbt disable all triggers';        
        
        delete /*+parallel(16) */ from dnotetext_dbt where T_OBJECTTYPE=101 and T_DOCUMENTID in (
        select lpad( tgt_dealid, 34, '0') from dtxdeal_tmp where t_replstate=0 and t_action in (3,2) ); 
        WRITE_LOG_FINISH('Удаление старых примечаний к сделкам', 80);
        commit;
        
        l_count := 0; -- счетчик обработанных строк
        
        -- Примечание 23 "Расчетная цена"
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 23, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(to_char(T_PRICE_CALC)),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_PRICE_CALC > 0;
        l_count := l_count + sql%rowcount;
        commit;
        -- Примечание 26 "Комментарий к допконтролю"
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 26, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(to_char(T_DOPCONTROL_NOTE)),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_DOPCONTROL_NOTE is not null;
        l_count := l_count + sql%rowcount;
        commit;
        -- Примечание 27 "Отклонение от расчетной цены"
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 27, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(to_char(T_PRICE_CALC_DEF)),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_PRICE_CALC_DEF > 0;
        l_count := l_count + sql%rowcount;
        commit;
        -- Примечание 28 "Единица измерения расчетной цены"        
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 28, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(to_char(T_PRICE_CALC_VAL)),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_PRICE_CALC_VAL > 0;
        l_count := l_count + sql%rowcount;
        commit;
        -- Примечание 29 "Способ определения РЦ"
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 29, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(T_PRICE_CALC_MET_NOTE),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_PRICE_CALC_MET_NOTE is not null;
        l_count := l_count + sql%rowcount;
        commit;
        -- Примечание 49 "Приоритетный портфель"
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 49, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(case T_PRIOR_PORTFOLIOID when '5' then '4' when '6' then '5' else T_PRIOR_PORTFOLIOID end),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_PRIOR_PORTFOLIOID > 0;
        l_count := l_count + sql%rowcount;
        commit;
        -- Примечание 101 "Дополнительные условия по сделке"
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 101, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(t_conditions),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and t_conditions is not null;
        l_count := l_count + sql%rowcount;
        commit;
        -- Примечание 102 "Код на бирже"
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 102, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(T_MARKETCODE),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_MARKETCODE is not null;
        l_count := l_count + sql%rowcount;
        commit;
        -- Примечание 103 "Код у контрагента"
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 103, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(T_PARTYCODE),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_PARTYCODE is not null;
        l_count := l_count + sql%rowcount;
        commit;        
        -- Примечание 112 "Валюта платежа"
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 112, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(to_char(T_PAYMCUR)),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_PAYMCUR > 0;
        l_count := l_count + sql%rowcount;
        commit;
        -- Примечание 116 "Расходы по оценке рыночной стоимости"        
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 101, lpad( tgt_dealid, 34, '0'), 116, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(to_char(T_PRICE_CALC_OUTLAY)),1500,0), date'4000-01-01'
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_PRICE_CALC_OUTLAY > 0;
        l_count := l_count + sql%rowcount;
        commit;
        WRITE_LOG_FINISH('Запись новых примечаний к сделкам', 80, p_count => l_count);        
        
        execute immediate 'alter table DNOTETEXT_DBT enable all triggers';        
        
        --==========================================================================
        --==========================================================================
               
        WRITE_LOG_START;
        deb('Запись категорий к сделкам');
        
        -- триггеры придется пока отключить, они мешают вставке в режиме direct path
        execute immediate 'alter table DOBJATCOR_DBT disable all triggers';
        

        delete /*+parallel(16) */ from dobjatcor_dbt where t_objecttype in (101,117) and t_object in (select lpad( tgt_dealid, 10, '0') from dtxdeal_tmp where t_replstate=0 and t_action in (3,2) ); 
        WRITE_LOG_FINISH('Удаление старых категорий сделок', 80);
        commit;
        
        l_count := 0;
        -- Установка значения категории Сделка подлежит дополнительному контролю ( 32 )
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 32, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_DOPCONTROL, 1,1,  2,2,  3,3,  4,4,  5,5,  6,6,  7,9,  8,1,  9,92,  10,94,  11,95,  12,96,  13,13,  14,23,  15,25,  16,7,  17,8,  18,38,  19,938,  20,93,  21,913,  22,925,  T_DOPCONTROL) 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_DOPCONTROL > 0 and tgt_BofficeKind in (101, 117);
        l_count := l_count + sql%rowcount;
        commit;

        -- Установка значения категории Признак изменения стоимости реализации/приобретения по 2 части Репо при выплате купонного дохода/дивидентов/частичного погашения ценных бумаг
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 21, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 1 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_COSTCHANGEONCOMP = CHR(88);
        l_count := l_count + sql%rowcount;
        commit;

        -- Установка значения категории Признак изменения стоимости реализации/приобретения по 2 части Репо при выплате купонного дохода/дивидентов/частичного погашения ценных бумаг
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 22, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 1 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_COSTCHANGEONAMOR = chr(88);
        l_count := l_count + sql%rowcount;
        commit;        
        
        -- Установка значения категории Признак изменения стоимости реализации/приобретения по 2 части Репо при выплате купонного дохода/дивидентов/частичного погашения ценных бумаг
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 33, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', T_FISSKIND 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2);
        l_count := l_count + sql%rowcount;
        commit;
 
        -- Установка значения категории Способ определения РЦ ( 34 )
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 34, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 
        decode(T_PRICE_CALC_METHOD, 1,'10-00', 2,'10-80', 3,'11-00', 4,'11-01', 5,'12-50', 6,'12-51', 7,'12-80', 8,'12-81', 9,'19-00', 10,'20-80', 11,'21-00', 12,'21-01', 13,'22-10', 14,'22-11', 15,'22-20', 16,'22-21', 17,'22-30', 18,'22-31', 19,'22-40', 20,'22-41', 21,'22-50', 22,'22-51', 23,'22-60', 24,'22-61', 25,'22-70', 26,'22-71', 27,'22-80', 28,'22-81', 29,'29-00', 30,'31-00', 31,'31-01', 32,'32-50', 33,'32-51', 34,'99-00', 35,'11-80', 36,'11-81','null') 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_PRICE_CALC_METHOD > 0;
        l_count := l_count + sql%rowcount;
        commit;       
        
        -- Признак урегулирования требований по ст.282 НК РФ.
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 19, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 1 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_ADJUSTMENT = chr(88);
        l_count := l_count + sql%rowcount;
        commit;   

        -- Признак ограничения прав покупателя по сделке РЕПО - "Блокировка ц/б"
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 103, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 1 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_LIMIT = chr(88);
        l_count := l_count + sql%rowcount;
        commit;           
        
        -- Признак изменения ставки - "Плавающая ставка"
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 26, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 1 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_CHRATE = chr(88);
        l_count := l_count + sql%rowcount;
        commit;           

        -- Признак изменения ставки - "Принадлежность дивидендов"
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 25, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31',  1 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_DIV = chr(88);
        l_count := l_count + sql%rowcount;
        commit;     
        
        -- Признак исполнения сделки в любой день
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 20, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', 1 
        from dtxdeal_tmp where t_replstate=0  and t_action in (1,2) and T_ATANYDAY = chr(88);
        l_count := l_count + sql%rowcount;
        commit;  

        -- "Место заключения сделки"
        insert /*+parallel(16)*/ into dobjatcor_dbt(t_id, t_objecttype, t_groupid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE, t_attrid)
        select dobjatcor_dbt_seq.nextval, 101, 105, lpad( tgt_dealid, 10, '0'), chr(88), g_oper, date'0001-01-01', date'9999-12-31', T_COUNTRY 
        from dtxdeal_tmp where t_replstate=0 and T_COUNTRY is not null and T_COUNTRY not in (158,165)  and t_action in (1,2) ;
        l_count := l_count + sql%rowcount;
        WRITE_LOG_FINISH('Запись новых категорий к сделкам', 80, p_count => l_count); 
        commit;          

        execute immediate 'alter table DOBJATCOR_DBT enable all triggers';
        
        --==========================================================================
        --==========================================================================
               
        
        -- TODO платежи по needdemand
        if  g_use_needdemand then
            deals_need_demand_generator;
        end if;
        
        
        deb('Завершена процедура DEALS_CREATE_RECORDS');
    end deals_create_records;



    -- формирует записи в целевой системе на основе таблицы снимка
    procedure DEMANDS_CREATE_RECORDS
    is
        l_counter number := 0;  -- счетчик обработанных строк
    begin
        deb('Запущена процедура DEMANDS_CREATE_RECORDS');
        execute immediate 'alter trigger DDLRQ_DBT_TBI disable';
        
        -- Обработка вставок
        
        WRITE_LOG_START;
        
        -- триггеры придется пока отключить, они мешают вставке в режиме direct path
        execute immediate 'alter table ddlrq_dbt disable all triggers';
                
        deb('Заполнение DDLRQ_DBT (t_action=1)');
        insert /*+parallel(16) */ into ddlrq_dbt(T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_NUM, T_AMOUNT, T_FIID, T_PARTY, T_RQACCID, T_PLACEID, T_STATE, T_PLANDATE, T_FACTDATE, T_USENETTING, T_NETTING, T_CLIRING, T_INSTANCE, T_CHANGEDATE, T_ACTION, T_ID_OPERATION, T_ID_STEP, T_SOURCE, T_SOURCEOBJKIND, T_SOURCEOBJID, T_TAXRATEBUY, T_TAXSUMBUY, T_TAXRATESELL, T_TAXSUMSELL, T_VERSION, T_FACTRECEIVERID)
        select TGT_DEMANDID, TGT_BOFFICEKIND /*T_DOCKIND*/, TGT_DEALID, T_PART, TGT_KIND/*T_KIND*/, TGT_SUBKIND, TGT_TYPE, TGT_NUM /*T_NUM*/, T_SUM, TGT_FIID, TGT_PARTY, -1, -1/*T_PLACEID*/, TGT_STATE, TGT_PLANDATE, TGT_FACTDATE, CHR(0), CHR(0)/*T_NETTING*/, null, 0, TGT_CHANGEDATE, 0 /*T_ACTION*/, 0, 2908, 0, -1, 0 /*T_SOURCEOBJID*/, 0, 0, 0, 0, 0, 0
        from dtxdemand_tmp where t_replstate=0 and t_action=1;
        WRITE_LOG_FINISH('Заполнение DDLRQ_DBT (t_action=1)', 90); 
        COMMIT;
        
        -- Обработка изменений
        
        WRITE_LOG_START;
        deb('Изменение DDLRQ_DBT (t_action=2)');
        merge /*+parallel(16) */ into (select T_ID, T_DOCKIND, T_DOCID, T_DEALPART, T_KIND, T_SUBKIND, T_TYPE, T_AMOUNT, T_FIID, T_PARTY, T_PLANDATE, T_FACTDATE, T_STATE from ddlrq_dbt) rq 
        using (select * from dtxdemand_tmp where t_replstate=0 and t_action=2) sou on (sou.tgt_demandid=rq.t_id)
        when matched then update set
        RQ.T_DOCKIND=SOU.TGT_DEALKIND,          RQ.T_DOCID=SOU.TGT_DEALID,          RQ.T_DEALPART=SOU.T_PART, 
        RQ.T_KIND=SOU.TGT_KIND,                 RQ.T_SUBKIND=SOU.TGT_SUBKIND,       RQ.T_TYPE=SOU.TGT_TYPE, 
        RQ.T_AMOUNT=SOU.T_SUM,                  RQ.T_FIID=SOU.TGT_FIID,             RQ.T_PARTY=SOU.TGT_PARTY, 
        RQ.T_PLANDATE=SOU.TGT_PLANDATE,         RQ.T_FACTDATE=SOU.TGT_FACTDATE,     RQ.T_STATE=SOU.TGT_STATE;
        WRITE_LOG_FINISH('Изменение DDLRQ_DBT (t_action=2)', 90); 
        COMMIT;

        -- Обработка удалений

        WRITE_LOG_START;
        deb('Удаление из DDLRQ_DBT (t_action=3)');
        delete /*+parallel(16)*/ from ddlrq_dbt where t_id in (select TGT_DEMANDID from dtxdemand_tmp where t_action=3 and t_replstate=0);
        WRITE_LOG_FINISH('Удаление из DDLRQ_DBT (t_action=3)', 90);         
        commit;
        
        execute immediate 'alter table ddlrq_dbt enable all triggers';
        
        ----------------------------------------------------------------------------
        -- Запись в DTXREPLOBJ_DBT
        WRITE_LOG_START;
        deb('Запись платежей в DTXREPLOBJ_DBT - подготовка');
        delete /*+parallel(16) */ from dtxreplobj_dbt where T_OBJECTTYPE=90 and T_OBJECTID in (select t_demandid from dtxdemand_tmp where t_replstate=0  and t_action=1);
        WRITE_LOG_FINISH('Запись платежей в DTXREPLOBJ_DBT - подготовка', 90);
        commit;
        
        WRITE_LOG_START;
        update /*+parallel(16)*/ dtxreplobj_dbt set t_objstate=2 where T_OBJECTTYPE=90 and t_objectid in (select t_demandid from dtxdemand_tmp where t_replstate=0 and t_action=3);
        WRITE_LOG_FINISH('Запись платежей в DTXREPLOBJ_DBT - удаления', 90);
        commit;
        
        WRITE_LOG_START;
        insert /*+parallel(16) */ into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE)
        select 90, t_demandid, 0, tgt_demandid, 0, 0 from dtxdemand_tmp where t_replstate=0 and t_action=1;
        WRITE_LOG_FINISH('Запись платежей в DTXREPLOBJ_DBT - вставки', 90);
        commit;
        
        
        
        -- Запись REPLSTATE в DTXDEMAND_DBT
        WRITE_LOG_START;
        deb('Запись REPLSTATE в DTXDEMAND_DBT');
        update /*+parallel(16) */ dtxdemand_dbt tgt 
        set tgt.t_replstate=1
        where (t_demandid, t_instancedate) in (select t_demandid, t_instancedate from dtxdemand_tmp where t_replstate=0);
        WRITE_LOG_FINISH('Запись REPLSTATE в DTXDEMAND_DBT', 90);
        commit;
        
        ----------------------------------------------------------------------------
        
        
        WRITE_LOG_START;
        deb('Запись примечаний к сделкам'); 
                
        -- триггеры придется пока отключить, они мешают вставке в режиме direct path
        execute immediate 'alter table dnotetext_dbt disable all triggers'; 
       
        
        delete /*+parallel(16) */ from dnotetext_dbt where T_OBJECTTYPE=993 and T_DOCUMENTID in (
        select lpad( tgt_demandid, 10, '0') from dtxdemand_tmp where t_replstate=0  and t_action in (3,2) ); 
        WRITE_LOG_FINISH('Удаление старых примечаний к платежам', 90);
        commit;
        
        -- t_note примечание по платежу
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 993, lpad( tgt_demandid, 10, '0'), 101, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(T_NOTE),1500,0), date'4000-01-01'
        from dtxdemand_tmp where t_replstate=0  and t_action in (1,2) and T_NOTE is not null;
        l_counter := l_counter + sql%rowcount;
        commit;
        
        -- t_note курс постановки на баланс
        insert /*+parallel(16) */ into dnotetext_dbt(T_ID, T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
        select dnotetext_dbt_seq.nextval, 993, lpad( tgt_demandid, 10, '0'), 101, g_oper, t_date, date'0001-01-01', rpad(utl_raw.cast_to_raw(to_char(T_BALANCERATE)),1500,0), date'4000-01-01'
        from dtxdemand_tmp where t_replstate=0  and t_action in (1,2) and T_BALANCERATE is not null;
        l_counter := l_counter + sql%rowcount;
        WRITE_LOG_FINISH('Запись новых примечаний к платежам', 90, p_count => l_counter);        
        commit;
                
        execute immediate 'alter table dnotetext_dbt enable all triggers';        
        
        
        execute immediate 'alter trigger DDLRQ_DBT_TBI enable';
        deb('Завершена процедура DEMANDS_CREATE_RECORDS');
    end demands_create_records;



    -- формирует записи в целевой системе на основе таблицы снимка
    procedure COURSES_CREATE_RECORDS
    is
        l_count number := 0;  -- счетчик обработанных строк
    begin
        deb('Запущена процедура COURSES_CREATE_RECORDS');
        
        -- Обработка вставок
        WRITE_LOG_START;
        deb('Заполнение DRATEDEF_DBT');

        -- Значение из DRATEDEF_DBT преходит в историю, если среди новых записей по этому курсу есть одна с флажком TGT_ISLASTDATE
        insert /*+parallel(16)*/ into dratehist_dbt(T_RATEID, T_ISINVERSE, T_RATE, T_SCALE, T_POINT, T_INPUTDATE, T_INPUTTIME, T_OPER, T_SINCEDATE, T_ISMANUALINPUT)
        select T_RATEID, T_ISINVERSE, T_RATE, T_SCALE, T_POINT, T_INPUTDATE, T_INPUTTIME, T_OPER, T_SINCEDATE, T_ISMANUALINPUT  
        from  dratedef_dbt where t_rateid in (select tgt_rateid from dtxcourse_tmp where TGT_ISLASTDATE=chr(88) and t_action=1 and t_replstate=0 and tgt_isnominal is null);
        --l_counter := l_counter + sql%rowcount;
        WRITE_LOG_FINISH('Перенос DRATEDEF в DRATEHIST', 70); 
        commit;

        -- сработает только для вставки (TGT_NEWRATEID есть только у нее)
        -- если курс в dratedef_dbt отсутствует вообще, добавим его. tgt_rateid уже есть
        -- пойдет строка с TGT_ISLASTDATE = 'X' и TGT_NEWRATEID IS NOT NULL
        insert /*+parallel(16)*/ into dratedef_dbt(T_RATEID, T_FIID, T_OTHERFI, T_NAME, T_DEFINITION, T_TYPE, T_ISDOMINANT, T_ISRELATIVE, T_INFORMATOR, T_MARKET_PLACE, T_ISINVERSE, T_RATE, T_SCALE, T_POINT, T_INPUTDATE, T_INPUTTIME, T_OPER, T_SINCEDATE, T_SECTION)
        select TGT_NEWRATEID, TGT_FIID, TGT_BASEFIID, 'Курс для ' || TGT_BASEFIID, chr(1), TGT_TYPE, TGT_ISDOMINANT, TGT_ISRELATIVE, TGT_MARKETID, TGT_MARKETID, chr(0), TGT_RATE, T_SCALE, T_POINT, t_instancedate, date'0001-01-01', g_oper, t_ratedate, TGT_SECTORID     
        from dtxcourse_tmp where TGT_ISLASTDATE = 'X' and TGT_NEWRATEID IS NOT NULL AND T_REPLSTATE=0 and tgt_isnominal is null  and t_action in (1,2) ;
        WRITE_LOG_FINISH('Вставка в DRATEDEF', 70);
        commit;
        
        -- для всех остальных (вставок и изменений) обновляем значение в dratedef_dbt
        merge /*+parallel(16)*/ into dratedef_dbt tgt
        using (select * from dtxcourse_tmp where TGT_ISLASTDATE = 'X' and t_replstate=0  and t_action in (1,2) and tgt_isnominal is null) sou
        on (sou.tgt_rateid = tgt.t_rateid)
        when matched then update 
        set tgt.T_RATE=sou.TGT_RATE, tgt.T_SCALE=sou.T_SCALE, tgt.T_POINT=sou.T_POINT, tgt.T_INPUTDATE=sou.T_INSTANCEDATE, 
            tgt.T_INPUTTIME=date'0001-01-01', tgt.T_OPER=g_oper, tgt.T_SINCEDATE=sou.T_RATEDATE;
        WRITE_LOG_FINISH('Обновление DRATEDEF', 70);
        commit;

        -- вставка в dratehist_Dbt
        insert /*+parallel(16)*/ into dratehist_dbt(T_RATEID, T_ISINVERSE, T_RATE, T_SCALE, T_POINT, T_INPUTDATE, T_INPUTTIME, T_OPER, T_SINCEDATE, T_ISMANUALINPUT)
        select nvl(TGT_RATEID,TGT_NEWRATEID), chr(0),  TGT_RATE, T_SCALE, T_POINT, T_INSTANCEDATE, date'0001-01-01', g_oper, T_RATEDATE, CHR(0)
        from dtxcourse_tmp where t_replstate=0 and t_action=1 and TGT_ISLASTDATE is null;
        WRITE_LOG_FINISH('Вставка в DRATEHIST', 70);
        commit;

        -- обновляем значение в dratehist_dbt для action=2
        merge /*+parallel(16)*/into dratehist_dbt tgt
        using (select * from dtxcourse_tmp where TGT_ISLASTDATE is null and t_action=2 and t_replstate=0 and tgt_isnominal is null) sou
        on (sou.tgt_rateid = tgt.t_rateid and tgt.T_SINCEDATE=sou.T_RATEDATE)
        when matched then update 
        set tgt.T_RATE=sou.TGT_RATE, tgt.T_SCALE=sou.T_SCALE, tgt.T_POINT=sou.T_POINT, tgt.T_INPUTDATE=sou.T_INSTANCEDATE, 
            tgt.T_INPUTTIME=date'0001-01-01', tgt.T_OPER=g_oper;
        WRITE_LOG_FINISH('Обновление DRATEHIST', 70);        
        commit;
        
        -- удаляем значение из dratehist_dbt для acction=3
        delete /*+parallel(16)*/ from dratehist_dbt where (t_rateid, t_sincedate) in
        (select tgt_rateid, T_RATEDATE from dtxcourse_tmp where t_replstate=0 and t_action=3 and tgt_isnominal is null);
        WRITE_LOG_FINISH('Удаление из DRATEHIST', 70);
        commit;
        
        -- потом добавить удаление последнего значения курса - из dratedef_dbt. Исходное решение на макросах этого не умело, значит, не критично

        --=====================================================================================
        -- обработка записей на изменение номинала
        l_count := 0;
        insert into DFIVLHIST_DBT (T_ID, T_FIID, T_VALKIND, T_ENDDATE, T_VALUE, T_INTVALUE) 
        select TGT_RATEID, TGT_BASEFIID, 1, T_RATEDATE, T_RATE, 0 from DTXCOURSE_TMP 
        where t_action =1 and TGT_ISNOMINAL=chr(88) and t_replstate=0;
        l_count := l_count + sql%rowcount;
        commit;
        
        merge /*+parallel(16) */ into  DFIVLHIST_DBT tgt
        using (select TGT_RATEID, TGT_BASEFIID, T_RATEDATE, T_RATE from DTXCOURSE_TMP where t_action=2 and t_replstate=0 and TGT_ISNOMINAL=chr(88)) sou
        on (tgt.t_id = sou.tgt_rateid)
        when matched then update set T_FIID=TGT_BASEFIID, T_ENDDATE=T_RATEDATE, T_VALUE=T_RATE;
        l_count := l_count + sql%rowcount;
        commit;
        
        delete /*+parallel(16) */ from DFIVLHIST_DBT where T_ID in 
        (select TGT_RATEID from DTXCOURSE_TMP where t_action=3 and t_replstate=0 and TGT_ISNOMINAL=chr(88));
        l_count := l_count + sql%rowcount;
        commit;
        WRITE_LOG_FINISH('Обработка изменений номинала', 70, p_count=>l_count);

        --=====================================================================================
        --=====================================================================================
        
        deb('Заполняем dtxreplobj_Dbt');
        delete /*+parallel(16)*/ from dtxreplobj_dbt where t_objecttype=70 and t_objectid in 
        (select t_courseid from dtxcourse_tmp where t_replstate=0 and t_action = 1);
        commit;

        insert /*+parallel(16)*/into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE) 
        select 70, t_courseid, t_type, nvl(tgt_rateid, tgt_newrateid), 1, 0  
        from dtxcourse_tmp where t_replstate=0 and t_action=1;
        commit;
        
        update /*+parallel(16)*/ dtxreplobj_dbt set t_objstate=2 where (T_OBJECTTYPE, T_OBJECTID) in   
        (select 70, t_courseid from dtxcourse_tmp where t_replstate=0 and t_action=3);
        WRITE_LOG_FINISH('Вставка записей в DTXREPLOBJ', 70);
        commit;
        
        update /*+parallel(16)*/ dtxcourse_dbt set t_replstate=1 where (t_courseid, t_instancedate) in
        (select t_courseid, t_instancedate from dtxcourse_tmp where t_replstate=0); 
        commit;

        deb('Завершена процедура COURSES_CREATE_RECORDS');
    end courses_create_records;



    -- формирует записи в целевой системе на основе таблицы снимка
    procedure COMISS_CREATE_RECORDS
    is
        l_counter number := 0;  -- счетчик обработанных строк
    begin
        deb('Запущена процедура COMISS_CREATE_RECORDS');
        
        -- Обработка вставок
                
        
        -- добавление вида комиссии
        WRITE_LOG_START;

	insert into dsfcomiss_dbt( T_FEETYPE, T_CODE, T_NAME, T_CALCPERIODTYPE, T_CALCPERIODNUM, T_DATE, T_PAYNDS, T_FIID_COMM, T_RECEIVERID, T_SERVICEKIND, T_SERVICESUBKIND, T_FIID_PAYSUM, T_DATEBEGIN, T_DATEEND, T_SUMMIN, T_SUMMAX, T_RATETYPE, T_INCFEETYPE, T_INCCOMMNUMBER, T_FORMALG, T_CALCCOMISSSUMALG, T_SETACCSEARCHALG, T_INSTANTPAYMENT,T_PRODUCTID,T_NDSCATEG,T_PARENTCOMISSID )
	select 1, 'Авто'||TGT_COMMCODE /*T_CODE*/, 
            DECODE( T_TYPE, 1, 'Комиссия ТС', 2, 'Клиринговая комиссия', 3, 'Комиссия брокера', 4, 'Комиссия депозитария', 5, 'Прочие расходы', 6, 'Комиссия за ИТС') || ', по валюте ' || TGT_CURRENCYID  /*T_NAME*/,   
            0 /*T_CALCPERIODTYPE*/, 0 /*T_CALCPERIODNUM*/, date'0001-01-01' /*T_DATE*/, 1 /*T_PAYNDS*/, TGT_CURRENCYID, 1 /*T_RECEIVERID*/, 1 /*T_SERVICEKIND*/, 0 /*T_SERVICESUBKIND*/, -1 /*T_FIID_PAYSUM*/, date'0001-01-01' /*T_DATEBEGIN*/, date'0001-01-01' /*T_DATEEND*/, 0,0,0,0,0,1,1,1,chr(88),0,0,0
        from DTXCOMISS_DBT where t_replstate=0 and TGT_COMMNUMBER is null and t_action in (1,2) group by TGT_COMMCODE, TGT_CURRENCYID, T_TYPE
        WRITE_LOG_FINISH('Вставка новых видов комиссий (dsfcomiss_dbt)', 100);
        commit;


        -- триггеры придется пока отключить, они мешают вставке в режиме direct path
        execute immediate 'alter table DDLCOMIS_DBT disable all triggers';        

                
        -- вставка комиссий общая
        insert /*+parallel(16)*/ into DDLCOMIS_DBT(T_ID, T_DOCKIND, T_DOCID, T_CONTRACT, T_FEETYPE, T_COMNUMBER, T_SUM, T_NDS, T_DATE, T_PLANPAYDATE, T_FACTPAYDATE, T_INPUTMEDIUM)
        select TGT_COMISSID, 101, TGT_DEALID, TGT_CONTRACTID, 1, TGT_COMMNUMBER, T_SUM, T_NDS, T_DATE, T_DATE, T_DATE, 'P'
        from DTXCOMISS_TMP where t_replstate=0 and t_action=1 and TGT_COMMNUMBER is not null;
        WRITE_LOG_FINISH('Вставка комиссий общая', 100);
        commit;

        -- вставка комиссий новых видов (номер берем из DSFCOMISS_DBT по коду)
        insert /*+parallel(16)*/ into DDLCOMIS_DBT(T_ID, T_DOCKIND, T_DOCID, T_CONTRACT, T_FEETYPE, T_COMNUMBER, T_SUM, T_NDS, T_DATE, T_PLANPAYDATE, T_FACTPAYDATE, T_INPUTMEDIUM)
        select com.TGT_COMISSID, 101, com.TGT_DEALID, com.TGT_CONTRACTID, 1, sf.T_NUMBER, com.T_SUM, com.T_NDS, com.T_DATE, com.T_DATE, com.T_DATE, 'P'
        from DTXCOMISS_TMP com JOIN DSFCOMISS_DBT sf ON (com.TGT_COMMCODE=sf.T_CODE and sf.T_FEETYPE=1) where t_replstate=0 and t_action=1 and TGT_COMMNUMBER is null;
        WRITE_LOG_FINISH('Вставка комиссий общая', 100);
        commit;
        
        -- изменение комиссий
        merge /*+parallel(16)*/ into DDLCOMIS_DBT tgt
        using (select * from DTXCOMISS_TMP where t_replstate=0 and t_action=2) sou on (sou.tgt_comissid=tgt.t_id)
        when matched then update set tgt.T_COMNUMBER=sou.TGT_COMMNUMBER, tgt.T_SUM=sou.T_SUM, tgt.T_NDS=sou.T_NDS, tgt.T_DATE=sou.T_DATE, tgt.T_PLANPAYDATE=sou.T_DATE, tgt.T_FACTPAYDATE=sou.T_DATE;
        WRITE_LOG_FINISH('Изменение комиссий', 100);
        commit;        
        
        -- удаление комиссий
        delete /*+parallel(16)*/ from DDLCOMIS_DBT where t_id in ( select tgt_comissid from DTXCOMISS_TMP where t_replstate=0 and t_action=3 );
        WRITE_LOG_FINISH('Удаление комиссий', 100);
        commit; 
                
        execute immediate 'alter table DDLCOMIS_DBT enable all triggers';   
        --=====================================================================================
        --=====================================================================================
        
        deb('Заполняем dtxreplobj_Dbt');
        WRITE_LOG_START;        
        delete /*+parallel(16)*/ from dtxreplobj_dbt where t_objecttype=100 and t_objectid in 
        (select t_comissid from dtxcomiss_tmp where t_replstate=0 and t_action = 1);
        commit;
        
        insert /*+parallel(16)*/ into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE) 
        select 100, t_comissid, t_type, tgt_comissid, 1, 0  
        from dtxcomiss_tmp where t_replstate=0 and t_action=1;
        WRITE_LOG_FINISH('Вставка записей в DTXREPLOBJ', 100);
        commit;
        
        update /*+parallel(16)*/ dtxreplobj_dbt set t_objstate=2 where (T_OBJECTTYPE, T_OBJECTID) in   
        (select 100, t_comissid from dtxcomiss_tmp where t_replstate=0 and t_action=3);
        commit;
        
        update /*+parallel(16)*/ dtxcomiss_dbt set t_replstate=1 where (t_comissid, t_instancedate) in
        (select t_comissid, t_instancedate from dtxcomiss_tmp where t_replstate=0);         
        WRITE_LOG_FINISH('Запись T_REPLSTATE в DTXCOMISS_DBT', 100);
        commit;
        deb('Завершена процедура COMISS_CREATE_RECORDS');
    end comiss_create_records;


    procedure load_deals_by_period(p_startdate date, p_enddate date default null, p_stage number default null)
    is
        l_enddate date;
        l_count number := 0;
    begin
        deb('Запущена процедура LOAD_DEALS_BY_PERIOD');
        initialize;
        
        if p_stage not in (1,2) then
            deb('Некорректное значение параметра p_stage. Завершение процедуры.');
            WRITE_LOG_FINISH('Некорректное значение параметра p_stage', 80);
            return;
        end if;
        
        l_enddate := nvl( p_enddate, p_startdate + 1 - 1/24/60/60); 
    
        -- Если SESSION_ID был не зназначен, то есть, LOAD_DEALS вызвали вручную, назначим его
        if g_SESSION_ID is null then
            insert into dtx_session_dbt(t_startdate, t_enddate, t_user, t_status)
            values( p_startdate, l_enddate, user, 'R') returning t_sessid into g_SESSION_ID;
        end if;
        
        insert into dtx_sess_detail_dbt( T_SESSID, T_PROCEDURE, T_STARTDATE)
        values (g_SESSION_ID, 'LOAD_DEALS', sysdate) returning t_detailid into g_SESS_DETAIL_ID;
        commit;
        --==================================================================================================
        
        if p_stage is null  or  p_stage = 1   
        then    

            -- заполняет таблицу снимка (dtxdeal_tmp из dtxdeeal_dbt)
            deals_create_snapshot(p_startdate, l_enddate);
        
            -- выполняет общие проверки. Посколько запросы из dtx_query_dbt не уполномочены менять t_replstate записей, придется делать это здесь
            WRITE_LOG_START;
            -- исключает дубли записей внутри одного дня
            update dtxdeal_tmp set t_replstate=2 where rowid in 
            (select lag(rowid) over(partition by t_dealid, t_instancedate order by null) from dtxdeal_tmp);
            l_count := l_count + sql%rowcount;
            
            -- исключает лишние операции обновления, оставляя толко последнюю из них по каждой dealid 
            update dtxdeal_tmp set t_replstate=2 where rowid in (select lead(rowid) over(partition by t_dealid order by t_instancedate desc) from dtxdeal_tmp where t_action = 2);
            l_count := l_count + sql%rowcount;
            
            -- исключает вставки-изменения записи, если последней идет операция удаления по этому dealid. Это важный момент, поскольку вставка в таблицы не завязана на последовательность записей 
            update dtxdeal_tmp s set t_replstate=2
            where exists (select 1 from dtxdeal_tmp  where t_dealid=s.t_dealid and rowid<>s.rowid and t_action=3 and t_instancedate>s.t_instancedate);
            l_count := l_count + sql%rowcount; 
            WRITE_LOG_FINISH('Общая проверка записей', 80, p_count=>l_count);
            commit;
        
            -- прогоняет сеты запросов по таблице снимка dtxdeal_tmp
            run_all_queries( 80 );
        end if;
        
        if p_stage is null  or  p_stage = 2   
        then
            -- формирует записи в таблицы целевой системы (DDL_TICK_DBT, DDL_LEG_DBT, DNOTETEXT_DBT...) на основе таблицы снимка
            deals_create_records;
        end if;

        deb('Завершена процедура LOAD_DEALS_BY_PERIOD');
    end load_deals_by_period;



    procedure load_demands_by_period(p_startdate date, p_enddate date default null, p_stage number default null)
    is
        l_enddate date;
        l_count number := 0;
    begin
        deb('Запущена процедура LOAD_DEMANDS_BY_PERIOD');
        initialize;
                
        if p_stage not in (1,2) then
            deb('Некорректное значение параметра p_stage. Завершение процедуры.');
            WRITE_LOG_FINISH('Некорректное значение параметра p_stage', 90);
            return;
        end if;
        
        l_enddate := nvl( p_enddate, p_startdate + 1 - 1/24/60/60); 
    
        -- Если SESSION_ID был не зназначен, то есть, LOAD_DEALS вызвали вручную, назначим его
        if g_SESSION_ID is null then
            insert into dtx_session_dbt(t_startdate, t_enddate, t_user, t_status)
            values( p_startdate, l_enddate, user, 'R') returning t_sessid into g_SESSION_ID;
        end if;
        
        insert into dtx_sess_detail_dbt( T_SESSID, T_PROCEDURE, T_STARTDATE)
        values (g_SESSION_ID, 'LOAD_DEMANDS', sysdate) returning t_detailid into g_SESS_DETAIL_ID;
        commit;
        --===========================================================================
    
        if p_stage is null  or  p_stage = 1   
        then
        
            -- заполняет таблицу снимка (dtxdeal_tmp из dtxdeeal_dbt)
            demands_create_snapshot(p_startdate, l_enddate);
        
            -- выполняем общие проверки. Посколько запросы из dtx_query_dbt не уполномочены менять t_replstate записей, придется делать это здесь
            WRITE_LOG_START;
            -- исключает дубли записей внутри одного дня
            update dtxdemand_tmp set t_replstate=2 where rowid in 
            (select lag(rowid) over(partition by t_demandid, t_instancedate order by null) from dtxdemand_tmp);
            l_count := l_count + sql%rowcount;
            
            -- исключает лишние операции обновления, оставляя только последнюю из них по каждому t_demandid 
            update dtxdemand_tmp set t_replstate=2 where rowid in (select lead(rowid) over(partition by t_demandid order by t_instancedate desc) from dtxdemand_tmp where t_action = 2);
            l_count := l_count + sql%rowcount;
            
            -- исключает вставки-изменения записи, если последней идет операция удаления по этому t_demandid. Это важный момент, поскольку вставка в таблицы не завязана на последовательность записей 
            update dtxdemand_tmp s set t_replstate=2
            where exists (select 1 from dtxdemand_tmp  where t_demandid=s.t_demandid and rowid<>s.rowid and t_action=3 and t_instancedate>s.t_instancedate);
            l_count := l_count + sql%rowcount; 
            WRITE_LOG_FINISH('Общая проверка записей', 90, p_count=>l_count);
            commit;
        
            -- прогоняет сеты запросов по таблице снимка dtxdeal_tmp
            run_all_queries( 90 );
        end if;
        
        if p_stage is null or p_stage = 2  
        then
            -- формирует записи в таблицы целевой системы (DDL_RQ_DBT, DNOTETEXT_DBT...) на основе таблицы снимка
            demands_create_records;
        end if;

        deb('Завершена процедура LOAD_DEMANDS_BY_PERIOD');
    end load_demands_by_period;





    procedure load_courses_by_period(p_startdate date, p_enddate date default null, p_stage number default null)
    is
        l_enddate date;
        l_count number := 0;
    begin
        deb('Запущена процедура LOAD_COURSES_BY_PERIOD');
        initialize;
        
        if p_stage not in (1,2) then
            deb('Некорректное значение параметра p_stage. Завершение процедуры.');
            WRITE_LOG_FINISH('Некорректное значение параметра p_stage', 70);
            return;
        end if;
                
        l_enddate := nvl( p_enddate, p_startdate + 1 - 1/24/60/60); 
    
        -- Если SESSION_ID был не зназначен, то есть, LOAD_DEALS вызвали вручную, назначим его
        if g_SESSION_ID is null then
            insert into dtx_session_dbt(t_startdate, t_enddate, t_user, t_status)
            values( p_startdate, l_enddate, user, 'R') returning t_sessid into g_SESSION_ID;
        end if;
        
        insert into dtx_sess_detail_dbt( T_SESSID, T_PROCEDURE, T_STARTDATE)
        values (g_SESSION_ID, 'LOAD_COURSES', sysdate) returning t_detailid into g_SESS_DETAIL_ID;
        commit;
        --============================================================================
        
        if p_stage is null  or  p_stage = 1   
        then       
            -- заполняет таблицу снимка (dtxdeal_tmp из dtxdeeal_dbt)
            courses_create_snapshot(p_startdate, l_enddate);
        
            -- выполняем общие проверки. Посколько запросы из dtx_query_dbt не уполномочены менять t_replstate записей, придется делать это здесь
            WRITE_LOG_START;
            -- исключает дубли записей внутри одного дня
            update dtxcourse_tmp set t_replstate=2 where rowid in 
            (select lag(rowid) over(partition by t_courseid, t_instancedate order by null) from dtxcourse_tmp);
            l_count := l_count + sql%rowcount;
        
            -- исключает лишние операции обновления, оставляя только последнюю из них по каждому t_courseid 
            update dtxcourse_tmp set t_replstate=2 where rowid in (select lead(rowid) over(partition by t_courseid order by t_instancedate desc) from dtxcourse_tmp where t_action = 2);
            l_count := l_count + sql%rowcount;
        
            -- исключает вставки-изменения записи, если последней идет операция удаления по этому t_courseid. Это важный момент, поскольку вставка в таблицы не завязана на последовательность записей 
            update dtxcourse_tmp s set t_replstate=2
            where exists (select 1 from dtxcourse_tmp  where t_courseid=s.t_courseid and rowid<>s.rowid and t_action=3 and t_instancedate>s.t_instancedate);
            l_count := l_count + sql%rowcount; 
            WRITE_LOG_FINISH('Общая проверка записей', 70, p_count=>l_count);
            commit;
        
            -- прогоняет сеты запросов по таблице снимка dtxdeal_tmp
            run_all_queries( 70 );
        end if;
        
        if p_stage is null  or  p_stage = 2   
        then   
            -- формирует записи в таблицы целевой системы (DRATEDEF_DBT, DRATEHIST_DBT...) на основе таблицы снимка
            courses_create_records;
        end if;

        deb('Завершена процедура LOAD_COURSES_BY_PERIOD');
    end load_courses_by_period;




    procedure load_comiss_by_period(p_startdate date, p_enddate date default null, p_stage number default null)
    is
        l_enddate date;
        l_count number := 0;
    begin
        deb('Запущена процедура LOAD_COMISS_BY_PERIOD');
        initialize;
        
        if p_stage not in (1,2) then
            deb('Некорректное значение параметра p_stage. Завершение процедуры.');
            WRITE_LOG_FINISH('Некорректное значение параметра p_stage', 100);
            return;
        end if;
                
        l_enddate := nvl( p_enddate, p_startdate + 1 - 1/24/60/60); 
    
        -- Если SESSION_ID был не зназначен, то есть, LOAD_DEALS вызвали вручную, назначим его
        if g_SESSION_ID is null then
            insert into dtx_session_dbt(t_startdate, t_enddate, t_user, t_status)
            values( p_startdate, l_enddate, user, 'R') returning t_sessid into g_SESSION_ID;
        end if;
        
        insert into dtx_sess_detail_dbt( T_SESSID, T_PROCEDURE,  T_STARTDATE)
        values (g_SESSION_ID, 'LOAD_COURSES', sysdate) returning t_detailid into g_SESS_DETAIL_ID;
        commit;
        --=================================================================================
    
        if p_stage is null  or  p_stage = 1   
        then   
            -- заполняет таблицу снимка (dtxcomiss_tmp из dtxcomiss_dbt)
            comiss_create_snapshot(p_startdate, l_enddate);
        
            -- выполняет общие проверки. Посколько запросы из dtx_query_dbt не уполномочены менять t_replstate записей, придется делать это здесь
            WRITE_LOG_START;
            l_count := 0;
            -- исключает дубли записей внутри одного дня
            update dtxcomiss_tmp set t_replstate=2 where rowid in 
            (select lag(rowid) over(partition by t_comissid, t_instancedate order by null) from dtxcomiss_tmp);
            l_count := l_count + sql%rowcount;
        
            -- исключает лишние операции обновления, оставляя только последнюю из них по каждому t_comissid 
            update dtxcomiss_tmp set t_replstate=2 where rowid in (select lead(rowid) over(partition by t_comissid order by t_instancedate desc) from dtxcomiss_tmp where t_action = 2);
            l_count := l_count + sql%rowcount;
            
            -- исключает вставки-изменения записи, если последней идет операция удаления по этому t_comissid. Это важный момент, поскольку вставка в таблицы не завязана на последовательность записей 
            update dtxcomiss_tmp s set t_replstate=2
            where exists (select 1 from dtxcomiss_tmp  where t_comissid=s.t_comissid and rowid<>s.rowid and t_action=3 and t_instancedate>s.t_instancedate);
            l_count := l_count + sql%rowcount; 
            WRITE_LOG_FINISH('Общая проверка записей', 100, p_count=>l_count);
            commit;
        
            -- прогоняет сеты запросов по таблице снимка dtxcomiss_tmp
            run_all_queries( 100 );
        end if;
        
        if p_stage is null  or  p_stage = 2   
        then   
            -- формирует записи в таблицы целевой системы на основе таблицы снимка
            comiss_create_records;
        end if;

        deb('Завершена процедура LOAD_COMISS_BY_PERIOD');
    end load_comiss_by_period;





        function GetCurrentNom(p_fi number, p_date date) return number
        DETERMINISTIC PARALLEL_ENABLE
        is
            l_tmp number;
        begin
            l_tmp := RSB_FIINSTR.FI_GetNominalOnDate( p_fi, p_date, 0);
            return l_tmp;
        end GetCurrentNom; 
        

        function GetIsQuoted(p_fi number, p_date date) return char
        DETERMINISTIC PARALLEL_ENABLE
        is 
            n number;
        begin
            select 1 into n from dratedef_dbt where t_otherfi=p_fi and t_sincedate > p_date-30 and rownum=1;
            if n=1 
            then 
                return chr(88);
            else 
                return chr(0);
            end if;
        end GetIsQuoted;

        function GetFictContract return number 
        DETERMINISTIC PARALLEL_ENABLE
        is
        begin
            return g_fictive_comiss_contract;
        end;

        function GetBasketFI return number
        DETERMINISTIC PARALLEL_ENABLE
        is
        begin
            return g_fictfi;
        end GetBasketFI;


        -- перекодирует kind сделки в значение из домена целевой системы
        function GetDealKind( p_kind number, p_avoirissid number, p_market number, p_isbasket char, p_isksu char)    return number
        PARALLEL_ENABLE
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
        DETERMINISTIC PARALLEL_ENABLE
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

    -- нельзя использовать исполняемый блок для инициализации пакета, 
    -- он начиинает вызываться для каждого sql-потока при вызове функций из sql.
    -- придется запускать вручную.
    procedure initialize
    is 
    begin
        if g_is_initialized
        then return;
        end if;
        
        g_is_initialized := true;
        
        deb('=== Выполняется инициирующая загрузка в пакете ===');
        DBMS_OUTPUT.ENABLE (buffer_size => NULL);
    
        --execute immediate 'alter sequence ddl_tick_dbt_seq cache 200';
        --execute immediate 'alter sequence ddl_leg_dbt_seq cache 200';
        --execute immediate 'alter sequence ddlrq_dbt_seq cache 200';
        --execute immediate 'alter sequence dnotetext_dbt_seq cache 200';
        --execute immediate 'alter sequence dobjatcor_dbt_seq cache 200';
    
        rollback;
    
        -- обход ошибки ora-00979: not a group by.. при использовании INSERT-SELECT-GROUP BY
        -- на новой базе попробовать отключить, если ошбка не появится, оставить
        -- https://support.oracle.com/knowledge/Oracle%20Database%20Products/2804026_1.html 
        execute immediate 'ALTER SESSION SET "_complex_view_merging" = FALSE';
    
        execute immediate 'alter session enable parallel dml';
        
        g_my_SID := to_number(SYS_CONTEXT('USERENV','SESSIONID'));
        
        deb('SID = #1', g_my_SID); 
        
        -- ищем код ценной бумаги, соответствующий корзине
        begin
            SELECT T_FIID into g_fictfi FROM DFININSTR_DBT WHERE lower(t_name) like 'корзина с%';
        exception when no_data_found
        then
            g_fictfi := c_DEFAULT_FICTFI;
        end;
        
        select t_id into g_fictive_comiss_contract from dsfcontr_dbt where rownum=1;
        
        deb('=== Завершен инициализирующий блок пакета ===');        
    end initialize;
 
    procedure start_replication(p_startdate date default null, p_enddate date default null)
    is
        l_enddate date;
        l_startdate date;
    begin
        deb('Запущена процедура START_REPLICATION');
                
        l_startdate := nvl( p_startdate, date'0001-01-01');
        l_enddate := nvl( p_enddate, date'4000-01-01'); 
    
        -- Назначение SESSION_ID
        insert into dtx_session_dbt(t_startdate, t_enddate, t_user, t_status)
        values( p_startdate, l_enddate, user, 'R') returning t_sessid into g_SESSION_ID;
        WRITE_LOG_START;
        WRITE_LOG_FINISH('Запущена процедура START_REPLICATION', 0);
        
        load_courses_by_period(l_startdate, l_enddate);
        load_deals_by_period(l_startdate, l_enddate);
        load_demands_by_period(l_startdate, l_enddate);
        load_comiss_by_period(l_startdate, l_enddate);
        
        deb('Завершена процедура START_REPLICATION');
        WRITE_LOG_FINISH('Завершена процедура START_REPLICATION', 0);
        g_SESSION_ID := null;
        
    end start_replication;
       
end load_rss;
/