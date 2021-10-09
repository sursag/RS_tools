CREATE OR REPLACE PACKAGE BODY GEB_20210823.load_rss
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
    
    -- коды стран из таблицы dcountry_dbt. Индекс - t_codenum3, значение - t_codelat3
    type country_arr_type is table of varchar2(3) index by varchar2(3);
    country_arr  country_arr_type;
    
    -- коды iso валют
    type currency_iso_arr_type is table of varchar2(5) index by pls_integer;
    currency_iso_arr    currency_iso_arr_type;
    
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
        forall i in indices of dpartyown_arr SAVE EXCEPTIONS
                insert into dpartyown_dbt(T_PARTYID, T_PARTYKIND, T_SUPERIOR, T_SUBKIND)
                values( dpartyown_arr(i).T_PARTYID, dpartyown_arr(i).T_PARTYKIND, dpartyown_arr(i).T_SUPERIOR, dpartyown_arr(i).T_SUBKIND);
        commit;
        deb('Выполнена вставка в DPARTYOWN_DBT, количество ошибок - #1', SQL%BULK_EXCEPTIONS.COUNT); 
        for i in 1..SQL%BULK_EXCEPTIONS.COUNT
        loop
            deb('Ошибка #3 добавления субъекту #1 типа #2', dpartyown_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).t_partyid, dpartyown_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).T_PARTYKIND, SQL%BULK_EXCEPTIONS(i).ERROR_CODE, p_level => 5);
        end loop;
        deb('Завершена процедура UPLOAD_SUBJECT_TYPES');
    end  upload_subject_types;

    -- примечание к объекту добавляем в массив, чтобы потом записать в базу сразу порцию.
    procedure add_note( p_objtype number, p_objectid  number, p_kind number, p_value varchar2, p_date  date)
    is
            note_tmp    note_type;
            l_struniid  varchar2(50);
            l_objtype   number;
    begin
    
            deb('add_note: p_objtype = ' || p_objtype || ', p_objectid = ' || p_objectid || ', p_kind = ' || p_kind || ', p_value = ' || p_value || ', p_date = ' || p_date , p_level=>5);
    
            case p_objtype
            when 80 then    l_struniid  := lpad( p_objectid, 10, '0');
                            l_objtype   := 101;   
            else l_struniid := null;
            end case;
            note_tmp.r_objtype  := l_objtype;
            note_tmp.r_struniid := l_struniid;
            note_tmp.r_kind     := p_kind;
            note_tmp.r_value    := p_value;
            note_tmp.r_date     := p_date;
            
            note_arr( note_arr.count ) := note_tmp; 
    end add_note;
        
    -- примечание к объекту добавляем в массив, чтобы потом записать в базу сразу порцию.
    procedure add_categ( p_objtype number, p_objectid  number, p_kind number, p_value varchar2, p_date  date)
        is
            categ_tmp    categ_type;
            p_struniid  varchar2(50);
        begin
            case p_objtype
            when 101 then p_struniid := lpad( p_objectid, '0', 10);
            else p_struniid := null;
            end case;
            
            categ_tmp.r_objtype  := p_objtype;
            categ_tmp.r_struniid := p_struniid;
            categ_tmp.r_kind     := p_kind;
            categ_tmp.r_value    := p_value;
            categ_tmp.r_date     := p_date;
            
            categ_arr( categ_arr.count ) := categ_tmp; 
        end add_categ;        

    -- все категории объектов из буфера записываются в базу. Буфер очищается.
    procedure write_categs
    is 
    begin
        deb('Запущена процедура WRITE_CATEGS');
        deb('Количество записей в буфере - #1', categ_arr.count);
        forall i in indices of categ_arr SAVE EXCEPTIONS
            insert into dobjatcor_dbt(t_objecttype, t_groupid, t_attrid, t_object, t_general, t_oper, T_VALIDFROMDATE, T_VALIDTODATE)
            select t_objecttype, t_groupid, t_attrid,  categ_arr(i).r_struniid, chr(88), g_oper, date'0001-01-01', date'9999-12-31' from dobjattr_dbt 
            where t_name = categ_arr(i).r_value and t_groupid=categ_arr(i).r_kind and t_objecttype=categ_arr(i).r_objtype;
            
        deb('Выполнена вставка в DOBJATCOR_DBT, вставлено #1 записей, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT); 
        for i in 1..SQL%BULK_EXCEPTIONS.COUNT
        loop
            deb('Ошибка #2 добавления объекту #1 категории #3', note_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).r_objtype || ' - ' || note_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).r_struniid, SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  note_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).r_kind  ,p_level => 5);
        end loop;
        
        commit;
        note_arr.delete;
        deb('Завершена процедура WRITE_CATEGS');
    end write_categs;        
        
    
    -- все примечания к объектам из буфера записываются в базу. Буфер очищается.
    procedure write_notes
    is
        note_tmp note_type;
    begin
        deb('Запущена процедура WRITE_NOTES');
        deb('Количество записей в буфере - #1', note_arr.count);
        
        for i in nvl(note_arr.first,0) .. nvl(note_arr.last,-1)
        loop
            deb('Индекс ' || i || ', r_objtype = ' || note_arr(i).r_objtype || ', r_struniid = ' || note_arr(i).r_struniid || ', r_kind = ' || note_arr(i).r_kind || ', r_date = ' || note_arr(i).r_date  || ', r_value = ' || note_arr(i).r_value, p_level=>5);
        end loop;
        
        forall i in indices of note_arr SAVE EXCEPTIONS   
                delete from dnotetext_dbt
                where T_OBJECTTYPE=note_arr(i).r_objtype and T_DOCUMENTID=note_arr(i).r_struniid and T_NOTEKIND=note_arr(i).r_kind;

        forall i in indices of note_arr SAVE EXCEPTIONS
                insert into dnotetext_dbt(T_OBJECTTYPE, T_DOCUMENTID, T_NOTEKIND, T_OPER, T_DATE, T_TIME, T_TEXT, T_VALIDTODATE)
                values( note_arr(i).r_objtype, note_arr(i).r_struniid, note_arr(i).r_kind, g_oper, note_arr(i).r_date, date'0001-01-01', note_arr(i).r_value, date'4000-01-01');
        
        deb('Выполнена вставка в DNOTETEXT_DBT, вставлено #1 записей, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT); 
        for i in 1..SQL%BULK_EXCEPTIONS.COUNT
        loop
            deb('Ошибка #2 добавления объекту #1 примечания #3', note_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).r_objtype || ' - ' || note_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).r_struniid, SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  note_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).r_kind  ,p_level => 5);
        end loop;
        
        commit;
        note_arr.delete;
        deb('Завершена процедура WRITE_NOTES');
    end write_notes;    
    
    
    -- вызывается при обработке вставки/изменения сделки. Добавляет все категории/примечания в буферы
    procedure add_all_notes_categs(main_tmp DTXDEAL_DBT%ROWTYPE, add_tmp deal_sou_add_type)        
    is
        l_SetCatID number;
        l_SetCatSTR varchar2(50);
    begin
        deb('Запущена процедура ADD_ALL_NOTES_CATEGS');
    
        if add_tmp.tgt_dealid is null
        then
            deb('Ошибка! add_tmp.tgt_dealid == NULL');
            return;
        end if;

        /*дополнительные условия по сделке*/
        if main_tmp.t_conditions is not null
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 101, main_tmp.t_conditions, main_tmp.t_instancedate);
        end if;    
    
        /*Код на бирже*/
        if main_tmp.T_MARKETCODE is not null
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 102, main_tmp.T_MARKETCODE, main_tmp.t_instancedate);
        end if;

        /*Код у контрагента*/
        if main_tmp.T_PARTYCODE is not null
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 103, main_tmp.T_PARTYCODE, main_tmp.t_instancedate);
        end if;
        
        /* Расчетная цена (23) */
        if main_tmp.T_PRICE_CALC <> 0
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 23, main_tmp.T_PRICE_CALC, main_tmp.t_instancedate);
        end if;
        
        /* Отклонение от расчетной цены (27) */
        if main_tmp.T_PRICE_CALC_DEF > 0
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 27, main_tmp.T_PRICE_CALC_DEF, main_tmp.t_instancedate);
        end if;
        
        /* Единица измерения расчетной цены (28) */
        if main_tmp.T_PRICE_CALC_VAL > 0
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 28, main_tmp.T_PRICE_CALC_VAL, main_tmp.t_instancedate);
        end if;
        
        /* Примечание Расходы по оценке рыночной стоимости (116)*/
        if main_tmp.T_PRICE_CALC_OUTLAY > 0
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 116, main_tmp.T_PRICE_CALC_OUTLAY, main_tmp.t_instancedate);
        end if;
        
        /* Комментарий к допконтролю (26)*/
        if main_tmp.T_DOPCONTROL_NOTE is not null
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 26, main_tmp.T_DOPCONTROL_NOTE, main_tmp.t_instancedate);
        end if;
        
        /* Комментарий к способу определения РЦ (29)*/
        if main_tmp.T_PRICE_CALC_MET_NOTE is not null
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 29, main_tmp.T_PRICE_CALC_MET_NOTE, main_tmp.t_instancedate);
        end if;
        
        /* Валюта платежа*/
        if main_tmp.T_PAYMCUR > 0 and currency_iso_arr.exists(main_tmp.T_PAYMCUR)
        then
            add_note( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 112, currency_iso_arr(main_tmp.T_PAYMCUR), main_tmp.t_instancedate);
        end if;
        
        -------------------------------------------------------------------------------------------------------------------
        -- Теперь категории
        
        -- Установка значения категории Сделка подлежит дополнительному контролю ( 32 )
        if main_tmp.T_DOPCONTROL > 0 and add_tmp.tgt_BofficeKind in (c_DL_SECURITYDOC, c_DL_RETIREMENT)
        then
            l_SetCatID :=
            case main_tmp.T_DOPCONTROL
            when  1 then 1
            when  2 then 2
            when  3 then 3
            when  4 then 4
            when  5 then 5
            when  6 then 6
            when  7 then 9
            when  8 then 91
            when  9 then 92
            when  10 then 94
            when  11 then 95
            when  12 then 96
            when  13 then 13
            when  14 then 23
            when  15 then 25
            when  16 then 7
            when  17 then 8
            when  18 then 38
            when  19 then 938
            when  20 then 93
            when  21 then 913
            when  22 then 925
            else main_tmp.T_DOPCONTROL
            end;
            
            add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 32, l_SetCatID, main_tmp.t_instancedate);
        end if;
    
        -- Установка значения категории Признак изменения стоимости реализации/приобретения по 2 части Репо при выплате купонного дохода/дивидентов/частичного погашения ценных бумаг
        if main_tmp.T_COSTCHANGEONCOMP = chr(0)
        then
            l_SetCatID := 0;
        else
            l_SetCatID := 1;
        end if; 
        add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 21, l_SetCatID, main_tmp.t_instancedate);
        

        -- Установка значения категории Признак изменения стоимости реализации/приобретения по 2 части Репо при выплате купонного дохода/дивидентов/частичного погашения ценных бумаг
        if main_tmp.T_COSTCHANGEONAMOR = chr(0)
        then
            l_SetCatID := 0;
        else
            l_SetCatID := 1;
        end if; 
        add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 22, l_SetCatID, main_tmp.t_instancedate);        
        

        -- Установка значения категории Вид сделок ФИСС ( 33 )
        if main_tmp.T_FISSKIND > 0
        then
            add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 33, main_tmp.T_FISSKIND, main_tmp.t_instancedate);
        end if;
        
        
        -- Установка значения категории Способ определения РЦ ( 34 )
        if main_tmp.T_PRICE_CALC_METHOD > 0
        then
            l_SetCatSTR := case main_tmp.T_PRICE_CALC_METHOD
                      when 1  then '10-00'
                      when 2  then '10-80'                      
                      when 3  then '11-00'        
                      when 4  then '11-01'        
                      when 5  then '12-50'
                      when 6  then '12-51'
                      when 7  then '12-80'
                      when 8  then '12-81'
                      when 9  then '19-00'
                      when 10 then '20-80'
                      when 11 then '21-00'
                      when 12 then '21-01'
                      when 13 then '22-10'
                      when 14 then '22-11'
                      when 15 then '22-20'
                      when 16 then '22-21'                      
                      when 17 then '22-30'        
                      when 18 then '22-31'        
                      when 19 then '22-40'
                      when 20 then '22-41'
                      when 21 then '22-50'
                      when 22 then '22-51'
                      when 23 then '22-60'
                      when 24 then '22-61'
                      when 25 then '22-70'
                      when 26 then '22-71'
                      when 27 then '22-80'
                      when 28 then '22-81'
                      when 29 then '29-00'
                      when 30 then '31-00'
                      when 31 then '31-01'
                      when 32 then '32-50'
                      when 33 then '32-51'
                      when 34 then '99-00'
                      when 35 then '11-80'
                      when 36 then '11-81'                                                                                                                                                                                
                      else 'null'
                      end;
            add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 34, l_SetCatSTR, main_tmp.t_instancedate);
        end if;
        
        -- Признак урегулирования требований по ст.282 НК РФ.
        if main_tmp.T_ADJUSTMENT is not null
        then
            if main_tmp.T_ADJUSTMENT = chr(0) then 
                l_SetCatSTR := 0;
            else
                l_SetCatSTR := 1;
            end if;
            add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 19, l_SetCatSTR, main_tmp.t_instancedate);
        end if;
        
        -- Признак ограничения прав покупателя по сделке РЕПО - "Блокировка ц/б" 
        if main_tmp.T_LIMIT is not null
        then
            if main_tmp.T_LIMIT = chr(0) then 
                l_SetCatSTR := 0;
            else
                l_SetCatSTR := 1;
            end if;
            add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 103, l_SetCatSTR, main_tmp.t_instancedate);
        end if;
        
        -- Признак изменения ставки - "Плавающая ставка" 
        if main_tmp.T_CHRATE is not null
        then
            if main_tmp.T_CHRATE = chr(0) then 
                l_SetCatSTR := 0;
            else
                l_SetCatSTR := 1;
            end if;
            add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 26, l_SetCatSTR, main_tmp.t_instancedate);
        end if;
        
        -- Признак изменения ставки - "Принадлежность дивидендов"  
        if main_tmp.T_DIV is not null
        then
            if main_tmp.T_DIV = chr(0) then 
                l_SetCatSTR := 0;
            else
                l_SetCatSTR := 1;
            end if;
            add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 25, l_SetCatSTR, main_tmp.t_instancedate);
        end if;           
        
        -- Признак исполнения сделки в любой день  
        if main_tmp.T_ATANYDAY is not null
        then
            if main_tmp.T_ATANYDAY = chr(0) then 
                l_SetCatSTR := 0;
            else
                l_SetCatSTR := 1;
            end if;
            add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 20, l_SetCatSTR, main_tmp.t_instancedate);
        end if;  
        
        -- "Место заключения сделки"  
        if main_tmp.T_COUNTRY is not null and add_tmp.tgt_bofficekind=c_DL_SECURITYDOC and main_tmp.T_COUNTRY <> 158
        then
            add_categ( c_OBJTYPE_DEAL, add_tmp.tgt_dealid, 105, main_tmp.T_COUNTRY, main_tmp.t_instancedate);
        end if;      
        
        deb('Завершена процедура ADD_ALL_NOTES_CATEGS');            
             
    end add_all_notes_categs;
    
    
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
        
        if g_debug_level_limit > 3 then
            deb( '>  Количество записей в буфере replobj ПЕРЕД загрузкой: ' ||load_rss.replobj_rec_arr.count() );
            v_counter := load_rss.replobj_rec_arr.first; 
            while (v_counter is not null)
            loop
                deb( '> Номер - тип - object_id - destid:      ' || v_counter || '\t\t' || load_rss.replobj_rec_arr(v_counter).obj_type || '\t\t' || rpad(load_rss.replobj_rec_arr(v_counter).obj_id, 20, ' ') || '\t\t' || load_rss.replobj_rec_arr(v_counter).dest_id);
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
        SELECT ro.* BULK COLLECT INTO replobj_tmp_arr
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
            deb( '>  Индекс_в_массиве - Поисковая_строка - Значение_t_destid: \t\t' ||v_search_idx || '\t\t' || rpad(v_search_str, 20, ' ') || '\t\t' || replobj_tmp_arr(i).T_DESTID);
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
        
        if g_debug_level_limit > 3 then
            deb( '>  Количество записей в буфере replobj ПОСЛЕ загрузки: ' ||load_rss.replobj_rec_arr.count() );
            v_counter := load_rss.replobj_rec_arr.first; 
            while (v_counter is not null)
            loop
                deb( '> Номер - тип - object_id - destid:      ' || v_counter || '\t\t' || load_rss.replobj_rec_arr(v_counter).obj_type || '\t\t' || rpad(load_rss.replobj_rec_arr(v_counter).obj_id, 20, ' ') || '\t\t' || load_rss.replobj_rec_arr(v_counter).dest_id);
                v_counter := replobj_rec_arr.next(v_counter);
            end loop;
            deb( '>  Количество записей в поисковом индексе replobj ПОСЛЕ загрузки: ' ||load_rss.replobj_rec_inx_arr.count() );
            v_counter_str := load_rss.replobj_rec_inx_arr.first; 
            while (v_counter_str is not null)
            loop
                deb( '> Индекс - значение:      ' || v_counter_str || '\t\t' || load_rss.replobj_rec_inx_arr(v_counter_str));
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
    begin
        -- для производительности
        -- лимит важности в спецификации пакета. Если переданное значение выше, сообщение не записывается.
        if not (g_debug_output or g_debug_table) or (p_level > g_debug_level_limit)
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
        cursor m_cur(pp_date date, pp_action number) is select * from DTXCOURSE_DBT where t_instancedate between pp_date and pp_date+1 and t_replstate=0 and t_action = pp_action order by t_instancedate, t_action, t_basefiid, t_marketsectorid, t_type;

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
            deb('Завершена процедура  pr_exclude');
        end pr_exclude;
        
        -- запись обработана успешно.
        procedure pr_include( p_counter number)
        is
        begin
            deb('Запись обработана успешно! Процедура pr_include для записи номер #1', p_counter, p_level => 3);
            
            rate_sou_add_arr(p_counter).result := 1;

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
        procedure execute_rate( i pls_integer)
        is 
        begin
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
            deb('Загрузка буфера dratedef_dbt');
            select * bulk collect into dratedef_arr_tmp from dratedef_dbt where t_rateid in ( select tgt_rate_id from table(rate_sou_add_arr) );
            for i in 1..dratedef_arr_tmp.count
            loop
                -- переиндексируем коллекцию по rate_id
                dratedef_arr( dratedef_arr_tmp(i).t_rateid ) := dratedef_arr_tmp(i);
            end loop;
            -- временная больше не нужна
            dratedef_arr_tmp.delete;

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






    procedure load_deals( p_date date, p_action number)
    is
            
       
            cursor m_cur(pp_date date, pp_action number) is select * from DTXDEAL_DBT where t_instancedate between pp_date and pp_date+1 and t_replstate=0 and t_action = pp_action order by t_instancedate, t_action;

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
            
            
            -- сохраняем все добавленные сделки
                -- все примечания к объектам из буфера записываются в базу. Буфер очищается.
            procedure write_deals_to_ddltick
            is
            begin
                deb('Запущена процедура WRITE_DEALS_TO_DDLTICK');
                if ddl_tick_dbt_arr_out.count = 0 then
                    deb('Нет записей для обработки');
                else
                    deb('Вставка тикетов сделок DDL_TICK_DBT, в коллекции #1 записей', ddl_tick_dbt_arr_out.count);
                    forall i in indices of ddl_tick_dbt_arr_out SAVE EXCEPTIONS
                            insert into ddl_tick_dbt
                            values ddl_tick_dbt_arr_out(i);
                    
                    deb('Выполнена вставка в DDL_TICK_DBT, добавлено - #1, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT); 
                    for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                    loop
                        deb('Ошибка вставки сделки (ddl_tick_dbt) #2:  #1 ', SQL%BULK_EXCEPTIONS(i).ERROR_CODE,   ddl_tick_dbt_arr_out( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).t_dealid, p_level => 5);
                    end loop;
                    
                    deb('Вставка ценовых условий сделок DDL_LEG_DBT, в коллекции #1 записей', ddl_leg_dbt_arr_out.count);
                    forall i in indices of ddl_leg_dbt_arr_out SAVE EXCEPTIONS
                            insert into ddl_leg_dbt
                            values ddl_leg_dbt_arr_out(i);
                    
                    deb('Выполнена вставка в DDL_LEG_DBT, добавлено - #1, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT);  
                    for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                    loop
                        deb('Ошибка вставки сделки (ddl_leg_dbt) #2:  #1 ', SQL%BULK_EXCEPTIONS(i).ERROR_CODE,   ddl_leg_dbt_arr_out( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).t_dealid, p_level => 5);
                    end loop;
                    
                    deb('Очистка буфера сделок ddl_tick_dbt_arr_out');
                    ddl_tick_dbt_arr_out.delete;
                    deb('Очистка буфера сделок ddl_leg_dbt_arr_out');
                    ddl_leg_dbt_arr_out.delete;
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
                    
                    -- todo переделать на table()
                    forall i in indices of deal_sou_arr SAVE EXCEPTIONS
                        delete from dtxreplobj_dbt
                        where T_OBJECTTYPE = 80 and T_OBJECTID = deal_sou_arr(i).t_dealid and deal_sou_add_arr(i).result < 2; -- только для успешно загруженных записей
                        
                    forall i in indices of deal_sou_arr SAVE EXCEPTIONS
                        insert into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE)
                        select 80, deal_sou_arr(i).t_dealid, 0, deal_sou_add_arr(i).tgt_dealid, 0, 0 from dual
                        where deal_sou_add_arr(i).result < 2; -- только для успешно загруженных записей
                    deb('Обработано #1 записей', SQL%ROWCOUNT);
                end if;
                deb('Завершена процедура  WRITE_DEALS_TO_REPLOBJ');    
            end write_deals_to_replobj;

           
            -- проставляет сделкам в dtxdeal признак удачной-неудачной репликации
            procedure update_dtxdeal
            is
            begin
                deb('Запущена процедура  UPDATE_DTXDEAL');
                forall i in indices of deal_sou_arr SAVE EXCEPTIONS
                    update dtxdeal_dbt set t_replstate=deal_sou_add_arr(i).result 
                    where deal_sou_add_arr(i).result in (1) and t_dealid=deal_sou_arr(i).t_dealid 
                    and t_instancedate=deal_sou_arr(i).t_instancedate and t_replstate=0;
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
                    
                    forall i in indices of dspground_arr save exceptions
                        insert into dspground_dbt( t_kind, t_DocLog, t_Direction, t_Receptionist, t_References, t_AltXld, t_SignedDate, t_Party)
                        values (26, 513, 2, g_oper, 1, dspground_arr(i).r_AltXld, dspground_arr(i).r_SignedDate, dspground_arr(i).r_party);
    
                    deb('Выполнена вставка в DSPGROUND_DBT, вставлено #1 записей, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT); 
                    for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                    loop
                        deb('Ошибка #2 добавления в DSPGROUND_DBT, объект #1', dspground_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).r_AltXld, SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
                    end loop;
                
                    -- удаляем возможные привязки к документам
                    forall i in indices of dspground_arr save exceptions
                        delete dspgrdoc_dbt where t_SourceDocKind=dspground_arr(i).r_BofficeKind and t_SourceDocID=dspground_arr(i).r_dealid and t_SPGroundID=dspground_arr(i).r_spgroundid;
                        
                    -- и добавляем новые
                    forall i in indices of dspground_arr save exceptions
                        insert into dspgrdoc_dbt( t_SourceDocKind, t_SourceDocID, t_SPGroundID)
                        values (dspground_arr(i).r_BofficeKind, dspground_arr(i).r_dealid, dspground_arr(i).r_spgroundid);
    
                    deb('Выполнена вставка в DSPGRDOC_DBT, вставлено #1 записей, количество ошибок - #2', SQL%ROWCOUNT, SQL%BULK_EXCEPTIONS.COUNT); 
                    for i in 1..SQL%BULK_EXCEPTIONS.COUNT
                    loop
                        deb('Ошибка #2 вставки в DSPGRDOC_DBT, объект #1', dspground_arr( SQL%BULK_EXCEPTIONS(i).ERROR_INDEX ).r_dealid, SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
                    end loop;
                
                end if;
                deb('Завершена процедура  WRITE_GROUNDS');
            end write_grounds;
            
            
            -- выявлены проблемы с записью. Логируем ошибку и исключаем запись из обработки.
            procedure pr_exclude(p_code number, p_objtype number, p_id number, p_subnum number := 0, p_text varchar2, p_counter number, p_action number, p_silent boolean := false)
            is
                text_corr varchar2(1000);
                v_row DTXDEAL_DBT%ROWTYPE;
            begin
                deb('Запущена процедура  PR_EXCLUDE');
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
                deb('Завершена процедура  PR_EXCLUDE');
            end pr_exclude;
            
            -- запись обработана успешно.
            procedure pr_include( p_counter number)
            is
            begin
                deb('Запись обработана успешно! Процедура pr_include для записи номер #1', p_counter, p_level => 3);
                
                deal_sou_add_arr(p_counter).result := 1;
    
            end pr_include;


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
                        deb('Запущена процедура  DEAL_CLEANING');
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
                                when 40 then
                                            l_add_tmp.tgt_existback := true;
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
                        l_main_tmp.T_PRICE_CALC_VAL := nvl( l_main_tmp.T_FISSKIND, -1);
                        l_main_tmp.T_PRICE_CALC_VAL := nvl( l_main_tmp.T_FISSKIND, -1);
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

                    deb('Завершена процедура DEAL_CLEANING (успешно)');
                    return true;
            exception when exp_wrong_date
                then
                    deb('Завершена процедура DEAL_CLEANING (по исключению)');
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
                select fi.t_fiid, fi.t_name, av.t_isin, fi.t_facevaluefi, -1, fi.t_avoirkind, RSB_FIINSTR.FI_IsQuoted(fi.t_fiid, p_date), RSB_FIINSTR.FI_IsKSU(fi.t_fiid), decode(RSB_SECUR.securkind(fi.T_AVOIRKIND), 17, 1, 0) , -1, -1, -1, -1
                bulk collect into l_avr_add_arr_tmp from dfininstr_dbt fi, davoiriss_dbt av where fi.t_fiid=av.t_fiid and fi.t_fiid in (select column_value from TABLE(l_tmp_arr));
            
                deb('Загрузили, записей #1', l_avr_add_arr_tmp.count);
                for j in 1..l_avr_add_arr_tmp.count
                loop
                    avr_add_arr( l_avr_add_arr_tmp(j).r_fiid ) := l_avr_add_arr_tmp(j);
                end loop;
            
                l_avr_add_arr_tmp.delete;
                l_tmp_arr.delete;
                deb('Завершена процедура FILLING_AVOIRISS_BUFFER');
            end filling_avoiriss_buffer;
        
            -- перекодирует kind сделки в значение из домена целевой системы
            function GetDealKind( p_kind number, p_add_tmp  deal_sou_add_type)    return number
            is
                l_dealtype_tmp number;
                l_ismarket boolean;
                l_fiid  number;
            begin
                l_fiid := p_add_tmp.tgt_avoirissid;
                l_ismarket := case when p_add_tmp.tgt_market > 0 then true else false end;
            
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
                                    if avr_add_arr(l_fiid).r_is_ksu = 1 then
                                            l_dealtype_tmp := 2123; -- репо покупка биржевая КСУ
                                    else
                                            l_dealtype_tmp := 2122; -- репо покупка биржевая
                                    end if;
                            else
                                    l_dealtype_tmp := 2132; -- репо покупка внебиржевая
                            end if;            
                when    40  then
                            if l_ismarket then
                                    if avr_add_arr(l_fiid).r_is_ksu = 1 then
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
                
                if p_add_tmp.is_basket 
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


--================================================================================================================
--================================================================================================================
--================================================================================================================
--================================================================================================================
    begin
        deb_empty('=');

        deb('Запущена процедура LOAD_DEALS за ' || to_char(p_date, 'dd.mm.yyyy') || ', тип действия ' || p_action);
        
        open m_cur(p_date, p_action);
        loop

            -- загрузка порции данных
            fetch m_cur bulk collect into deal_sou_arr limit g_limit;
            exit when deal_sou_arr.count=0;
            good_deals_count := m_cur%rowcount;
            deb('Загружены данные из DTXDEAL_DBT, #1 строк', m_cur%rowcount);
            
            -- регистрируем все сущности для загрузки из REPLOBJ
            deb_empty('=');
            tmp_arr.delete;
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
                    tmp_dealcodes_in(j).INDEX_NUM   := j;  -- привязаться по коду сделки мы не можем.
                    tmp_dealcodes_in(j).T_DEALCODE  := main_tmp.t_code;
                    deb('tmp_dealcodes_in(#1).INDEX_NUM  =  #2, tmp_dealcodes_in(#1).T_DEALCODE  =  #3', j, tmp_dealcodes_in(j).INDEX_NUM, tmp_dealcodes_in(j).T_DEALCODE);
                end if;
            end loop;
            deb('Записей в tmp_dealids - #1, записей в tmp_dealcodes_in - #2', tmp_dealids.count, tmp_dealcodes_in.count);
            deb('Собрали коды и ID сделок из загрузки. Загружаем из целевой системы..');
            
            
            -- теперь загружаем в кэш сделки из целевой системы --
            -- для операции вставки, сделки по кодам --- нас интересует только факт наличия сделок ---------------------------
            if tmp_dealcodes_in.count > 0
            then
                select arr.INDEX_NUM, tk.t_dealcode bulk collect into tmp_dealcodes_out from ddl_tick_dbt tk, (select * from table(tmp_dealcodes_in)) arr where tk.t_dealcode=arr.T_DEALCODE and rownum < p_emergency_limit;
                deb('Загружено сделок по кодам (DTXDEAL.T_CODE = DDL_TICK_DBT.T_DEALCODE) #1 записей из #2', tmp_dealcodes_out.count, tmp_dealcodes_in.count, p_level => 3);
                -- переносим в дополнительную коллекцию 
                deb('tmp_dealcodes_out.first = #1, tmp_dealcodes_out.last = #2', tmp_dealcodes_out.first, tmp_dealcodes_out.last);
                for j in 1 .. nvl(tmp_dealcodes_out.last,0)
                loop
                    deb ( tmp_dealcodes_out(j).INDEX_NUM );
                    deal_sou_add_arr( tmp_dealcodes_out(j).INDEX_NUM ).is_matched_by_code := true;
                end loop;
                tmp_dealcodes_in.delete;
                tmp_dealcodes_out.delete;
            end if;
            
            -- по dealid ---------------------------------------------------------
            -- тикеты сделок, загружаем во внутренний буфер
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
            
            -- группы сделок
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
                        
            -- первая нога
            select * bulk collect into tmp_ddl_leg_dbt_arr_in  from ddl_leg_dbt where t_dealid in (select t_dealid from table(tmp_dealids)) and t_legkind=0 and rownum < p_emergency_limit ;
            deb('Загружено сделок (DDL_LEG_DBT, часть 1) по ID (DTXREPLOBJ.T_DESTID = DDL_LEG_DBT.T_DEALID) #1 записей из #2', tmp_ddl_leg_dbt_arr_in.count, tmp_dealids.count, p_level => 3);
            if sql%rowcount = p_emergency_limit then
                deb('Ошибка! Превышение лимита при загрузке сделок');
            end if;
            
            for j in  1 .. nvl(tmp_ddl_leg_dbt_arr_in.last, 0)
            loop
                dealid_tmp := tmp_ddl_leg_dbt_arr_in(j).t_dealid;
                tmp_sou_id := deal_sou_back( dealid_tmp ); 
                deal_sou_add_arr( tmp_sou_id ).DDL_LEG1_BUF := tmp_ddl_leg_dbt_arr_in(j);
            end loop;
            tmp_ddl_leg_dbt_arr_in.delete;
            
            -- вторая нога
            select * bulk collect into tmp_ddl_leg_dbt_arr_in  from ddl_leg_dbt where t_dealid in (select t_dealid from table(tmp_dealids)) and t_legkind=2 and rownum < p_emergency_limit ;
            deb('Загружено сделок (DDL_LEG_DBT, часть 2) по ID (DTXREPLOBJ.T_DESTID = DDL_LEG_DBT.T_DEALID) #1 записей из #2', tmp_ddl_leg_dbt_arr_in.count, tmp_dealids.count, p_level => 3);
            if sql%rowcount = p_emergency_limit then
                deb('Ошибка! Превышение лимита при загрузке сделок');
            end if;                        
            
            for j in 1 .. nvl(tmp_ddl_leg_dbt_arr_in.last, 0)
            loop
                dealid_tmp := tmp_ddl_leg_dbt_arr_in(j).t_dealid;
                tmp_sou_id := deal_sou_back( dealid_tmp ); 
                deal_sou_add_arr( tmp_sou_id ).DDL_LEG2_BUF := tmp_ddl_leg_dbt_arr_in(j);
            end loop;    
            tmp_ddl_leg_dbt_arr_in.delete;
            
            deal_sou_back.delete; -- тоже больше не нужен
                        
            deb('Запись в буфер данных из DDL_TICK и DDL_LEG завершена. Все буферы загружены. Вспомогательные массивы очищены.');                 
            deb_empty('=');
            ---------------------------------------------------------------------------------------

            deb('Цикл 3. Проверка наличия сделки в системе');
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
                tick_tmp.t_dealtype  := GetDealKind( main_tmp.t_kind,  add_tmp);
                
                               
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
                                deb('Предупреждение: стоимость по второй части сделки (T_COST2) не равна произведению цены (T_PRICE2) в валюте на количество бумаг (T_AMOUNT)');
                                add_log( 500,c_OBJTYPE_DEAL, main_tmp.t_dealid, 0, 'Предупреждение: стоимость по второй части сделки (T_COST2) не равна произведению цены (T_PRICE2) в валюте на количество бумаг (T_AMOUNT)', main_tmp.t_instancedate);
                            end if;
                        else
                            if Round(leg2_tmp.t_cost) <> Round(leg2_tmp.T_Price * main_tmp.T_AMOUNT)
                            then
                                deb('Предупреждение: стоимость по второй части сделки (T_COST2) не равна произведению цены (T_PRICE2) на количество бумаг (T_AMOUNT)');
                                add_log( 500,c_OBJTYPE_DEAL, main_tmp.t_dealid, 0, 'Предупреждение: стоимость по второй части сделки (T_COST2) не равна произведению цены (T_PRICE2) на количество бумаг (T_AMOUNT)', main_tmp.t_instancedate);
                            end if;
                    end if;
                    
                end if;
                
                --------------------------------------------------------------------------------------
                -- заполняем структуры DDL_TICK и DDL_LEG. Пока на вставку будет оптимизация в виде FORALL, изменение и удаление будут идти по одной операции.
                -- Предполагаю, что их будет несравнимо меньше. Если что, всегда можно дописать. 
                deb('!  main_tmp.t_action для записи равен ' || main_tmp.t_action);
                if main_tmp.t_action = 1
                then
                    deb('Запись в буферы DDL_TICK_DBT и DDL_LEG_DBT записей на вставку');
                    tick_tmp.t_dealid := ddl_tick_dbt_seq.nextval;
                    leg1_tmp.t_dealid := tick_tmp.t_dealid;

                    ddl_tick_dbt_arr_out( ddl_tick_dbt_arr_out.count ) := tick_tmp; 
                    ddl_leg_dbt_arr_out( ddl_leg_dbt_arr_out.count ) := leg1_tmp;
                    
                    if add_tmp.tgt_existback then
                        leg2_tmp.t_dealid := tick_tmp.t_dealid;
                        ddl_leg2_dbt_arr_out( ddl_leg_dbt_arr_out.count ) := leg2_tmp;
                    end if;
                   
                    deb('Запись DEALID = #1 сохранена', tick_tmp.t_dealid);                
                
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
                        deb('Запись SPGROUNDID = #1 сохранена в буфер dspground_arr', dspground_tmp.r_SPgroundID);
                    end if;
                
                elsif main_tmp.t_action = 2
                then
                    deb('Запись в таблицы DDL_TICK_DBT и DDL_LEG_DBT записей на изменение');
                    begin
                        savepoint a;
                        update ddl_tick_dbt set row=tick_tmp where t_dealid=tick_tmp.t_dealid;
                        deb('Запись в таблицы DDL_TICK_DBT и DDL_LEG_DBT записей на изменение');
                    exception when others
                    then
                        pr_exclude(662, c_OBJTYPE_DEAL, main_tmp.t_dealid,  0, 'Ошибка: ошибка при обновлении в ddl_tick_dbt - ' || sqlcode || ' (' || sqlerrm || ')', i, main_tmp.t_action);
                        deb('Запись в таблицу DDL_TICK_DBT и DDL_LEG_DBT записей на изменение');
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
                add_tmp.TGT_DEALID := tick_tmp.t_dealid;
                
                -- добавление примечаний и категорий объектов по сделке в буфер
                if main_tmp.t_action in (1,2)
                then
                    add_all_notes_categs(main_tmp, add_tmp);
                end if;
            
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
            -- сохраняем все добавленные примечания и категории
            write_notes;
            write_categs;           
            -- вставляем платежи
            write_demands;
            -- сохраняем договоры по сделкам и привязку к сделкам
            write_grounds;
        end loop; -- основной цикл, по порциям данных из DTXDEAL_DBT
        
        deb('Запись новых типов субъектов в таблицу dpartyown_dbt');
        upload_subject_types;
        deb('Завершена процедура LOAD_DEALS');
    end load_deals; 


    -- процедура загрузки платежей
    procedure load_demands(p_date date, p_action number)
    is
        cursor m_cur(pp_date date, pp_action number) is select * from DTXDEMAND_DBT where t_instancedate between pp_date and pp_date+1 and t_replstate=0 and t_action = pp_action order by t_instancedate, t_action; 
        demand_tmp demand_type;
        main_tmp   dtxdemand_dbt%rowtype;
        error_found boolean := false;
        tmp_number  number;
        tmp_string  varchar2(500);
        
        -- выявлены проблемы с записью. Логируем ошибку и исключаем запись из обработки.
        procedure pr_exclude(p_errnum number, p_id number, p_text varchar2, p_counter number, p_action number, p_silent boolean := false)
            is
                text_corr varchar2(1000);
                v_row DTXDEMAND_DBT%ROWTYPE;
            begin
                deb('Запущена процедура  PR_EXCLUDE');
                v_row := demand_sou_arr(p_counter);
                text_corr := replace(p_text, '%act%',  (case p_action when 1 then 'Вставка' when 2 then 'изменение' when 3 then 'удаление' end) );

                -- потом заменить на add_log_deferred
                if not p_silent
                then
                    add_log( p_errnum, 90, p_id, 0, text_corr, v_row.t_instancedate);
                end if;
    
                -- исключаем элемент
                demand_sou_arr.delete(p_counter);
                deb('Завершена процедура  PR_EXCLUDE');
            end pr_exclude;
        
--================================================================================================================
--================================================================================================================
--================================================================================================================
--================================================================================================================
    begin
        deb_empty('=');

        deb('Запущена процедура  LOAD_DEMANDS за ' || to_char(p_date, 'dd.mm.yyyy') || ', тип действия ' || p_action);
        
        open m_cur(p_date, p_action);
        loop
            -- загрузка порции данных
            fetch m_cur bulk collect into demand_sou_arr limit g_limit;
            exit when demand_sou_arr.count=0;
            deb('Загружены данные из DTXDEMAND_DBT, #1 строк', m_cur%rowcount);

            -- регистрируем все сущности для загрузки из REPLOBJ
            deb_empty('=');
            deb('Цикл 1 - очистка данных и регистрация кодов в буфере REPLOBJ');
            for i in 1..demand_sou_arr.count
            loop
                main_tmp    := demand_sou_arr(i);
                
                if main_tmp.t_action < 3
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
                    elsif   nvl(main_tmp.t_direction,0) = 0
                    then
                        tmp_string := 'Не задано t_direction - направление платежа';                            
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
                        tmp_string := 'Не задано t_paycurrencyid - статус платежа';
                    end if;
                    
                    if tmp_string is not null
                    then
                        pr_exclude(207, main_tmp.t_demandid, tmp_string, i, main_tmp.t_action);
                        deb('Ошибка. Платеж ' || main_tmp.t_demandid || '.  ' || tmp_string);
                        continue;
                    end if;        

                    -- собираем уникальные fiid
                    replobj_add( c_OBJTYPE_MONEY,   main_tmp.t_paycurrencyid, 0 );
                    -- сделка
                    replobj_add( c_OBJTYPE_DEAL, main_tmp.T_DEALID);

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
            
            deb('Цикл 2 - проверка данных в целевой системе и передача платажа в add_demand');
            for i in 1..demand_sou_arr.count
            loop
                main_tmp    := demand_sou_arr(i);
                
                demand_tmp.tgt_demandid :=  replobj_get(c_OBJTYPE_PAYMENT, main_tmp.t_demandid).dest_id;
                demand_tmp.tgt_docid    :=  replobj_get(c_OBJTYPE_DEAL, main_tmp.t_dealid).dest_id;
                tmp_number              :=  replobj_get(c_OBJTYPE_DEAL, main_tmp.t_dealid).state;
                if tmp_number = 1 
                then 
                    error_found := true;
                    add_log( 207, c_OBJTYPE_PAYMENT, main_tmp.t_demandid, 0, 'Ошибка: Т/О по сделке ' ||  main_tmp.t_dealid || ' находится в режиме ручного редактирования', main_tmp.t_instancedate);
                end if;
                
                if nvl(main_tmp.t_demandid,0) > 0 and demand_tmp.tgt_docid = -1 
                then 
                    error_found := true;
                    add_log( 207, c_OBJTYPE_PAYMENT, main_tmp.t_demandid, 0, 'Ошибка: отсутствует сделка по платежу ' ||  main_tmp.t_dealid, main_tmp.t_instancedate);
                end if;
                
                if nvl(main_tmp.t_paycurrencyid,0) > 0 
                then
                    demand_tmp.tgt_fiid     :=  replobj_get(c_OBJTYPE_MONEY, main_tmp.t_paycurrencyid).dest_id;
                    if demand_tmp.tgt_fiid = -1 
                    then    
                        error_found := true;
                        add_log( 207, c_OBJTYPE_PAYMENT, main_tmp.t_demandid, 0, 'Ошибка: не найдена валюта платежа ' || main_tmp.t_paycurrencyid || ' в Т/О по сделке ' ||  main_tmp.t_dealid , main_tmp.t_instancedate);
                    end if;
                end if;                
                
                
            end loop; -- конец цикла 2

                -- собираем сделки в коллекцию
                
                -- достаем фининструмент из сделок
                
                -- переносим в более удобную коллекцию, индексируем по dealid
                
                -- назначаем фининструмент и передаем в add_demand             

        end loop; -- конец главного цикла.

        deb('Записываем платежи в БД');
        write_demands;
        deb('Завершена процедура  LOAD_DEMANDS');
        
    end load_demands;



    -- процедура добавления платежа в кэш DEMAND_RQ_ARR
    procedure   add_demand (p_demand   demand_type)
    is
        rq_tmp  ddlrq_dbt%rowtype;
        add_tmp demand_add_type;
        
        index_tmp    pls_integer;
    begin
        deb('Запущена процедура ADD_DEMAND');
        
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
        else
            rq_tmp.t_state      := 0;
            rq_tmp.t_factDate   := date'0001-01-01';
            rq_tmp.t_PlanDate   := p_demand.r_date;
            rq_tmp.t_changedate := p_demand.r_date;  
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
        rq_tmp.t_Num           := 0;
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
        
        deb('Завершена процедура ADD_DEMAND');
    end add_demand;
    
    -- процедура записи платежей из кэша в БД. Используется в load_demands и load_deals
    procedure   write_demands
    is
        -- на ddlrq_dbt неправильно написанный триггер, обращающийся к таблицу, на которую навешен. Такие триггеры не принимают многострочные вставки, придется обходить.
        -- обходим ORA-04091
        type indexes_by_type is table of pls_integer index by pls_integer;
        indexes_insert  indexes_by_type;
        indexes_update  indexes_by_type;
        indexes_delete  indexes_by_type;
    begin
        execute immediate 'alter trigger DDLRQ_DBT_TBI disable';
        
        deb('Запущена процедура WRITE_DEMANDS');
        deb('Количество записей в буфере платежей - #1 (#2)',  demand_rq_arr.count, demand_add_arr.count);
        if demand_rq_arr.count > 0
        then
        
            -- есть три коллекции (выше), которые будут содержать индексы элементов из demand_rq_arr, разделенные по действиям
            -- заполняем их. 
            for i in demand_rq_arr.first..demand_rq_arr.last
            loop
                case demand_add_arr(i).r_action
                    when 1 then indexes_insert( indexes_insert.count ) := i;
                    when 2 then indexes_update( indexes_update.count ) := i;
                    when 3 then indexes_delete( indexes_delete.count ) := i;
                end case;
            end loop;
            
             
            -- вставка. потом выводим сообщение об ошибке и проставляем флаги ошибок в дополнительную коллекцию
            forall i in values of indexes_insert
                delete ddlrq_dbt where T_DOCKIND=demand_rq_arr(i).T_DOCKIND and T_DOCID=demand_rq_arr(i).T_DOCID and T_DEALPART=demand_rq_arr(i).T_DEALPART and T_TYPE=demand_rq_arr(i).T_TYPE and T_FIID=demand_rq_arr(i).T_FIID;
            forall i in values of indexes_insert
                insert into ddlrq_dbt values demand_rq_arr(i);
            
            deb('Выполнена вставка в DDLRQ_DBT, количество записей - #1, количество ошибок - #2', indexes_insert.count, SQL%BULK_EXCEPTIONS.COUNT); 
            for i in 1..SQL%BULK_EXCEPTIONS.COUNT
            loop
                deb('Ошибка #3 вставки платежа #1 (сделка #2) в ddlrq_dbt', demand_rq_arr( indexes_insert(i)).t_id, demand_rq_arr( indexes_insert(i)).t_docid,  SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
                demand_add_arr( indexes_insert(i) ).r_result := 2; -- флаг ошибки
            end loop;
            
            -- изменение
            forall i in values of indexes_update
                update ddlrq_dbt set row=demand_rq_arr(i) where t_id=demand_rq_arr(i).t_id;
    
            deb('Выполнено изменение в DDLRQ_DBT, количество записей - #1, количество ошибок - #2', indexes_insert.count, SQL%BULK_EXCEPTIONS.COUNT); 
            for i in 1..SQL%BULK_EXCEPTIONS.COUNT
            loop
                deb('Ошибка #3 изменения платежа #1 (сделка #2) в ddlrq_dbt', demand_rq_arr( indexes_insert(i)).t_id, demand_rq_arr( indexes_insert(i)).t_docid,  SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
                demand_add_arr( indexes_update(i) ).r_result := 2;            
            end loop;
            
            -- удаление
            forall i in values of indexes_delete
                delete ddlrq_dbt where t_id=demand_rq_arr(i).t_id;
            
            deb('Выполнена удаление из DDLRQ_DBT, количество записей - #1, количество ошибок - #2', indexes_insert.count, SQL%BULK_EXCEPTIONS.COUNT); 
            for i in 1..SQL%BULK_EXCEPTIONS.COUNT
            loop
                deb('Ошибка #3 удаления платежа #1 (сделка #2) в ddlrq_dbt', demand_rq_arr( indexes_insert(i)).t_id, demand_rq_arr( indexes_insert(i)).t_docid,  SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
                demand_add_arr( indexes_delete(i) ).r_result := 2;
            end loop;

            commit;
        
            -- заполняем dtxreplobj_dbt. Только для успешно обработанных записей. Сначала на всякий случай убираем аналогичные записи - на случай перезагрузки.
            forall i in indices of demand_rq_arr
                delete from dtxreplobj_dbt
                where T_OBJECTTYPE=90 and T_OBJECTID=demand_add_arr(i).r_oldobjectid and T_SUBOBJNUM=demand_add_arr(i).r_subobjnum
                and demand_add_arr(i).r_result <> 2;

            forall i in indices of demand_rq_arr
                insert into dtxreplobj_dbt(T_OBJECTTYPE, T_OBJECTID, T_SUBOBJNUM, T_DESTID, T_DESTSUBOBJNUM, T_OBJSTATE)
                select 90, demand_add_arr(i).r_oldobjectid, demand_add_arr(i).r_subobjnum, demand_rq_arr(i).t_id, demand_add_arr(i).r_destsubobjnum, 0  from dual
                where demand_add_arr(i).r_result <> 2;
                
            deb('Выполнена вставка в DTXREPLOBJ_DBT, количество записей - #1, количество ошибок - #2', demand_rq_arr.count, SQL%BULK_EXCEPTIONS.COUNT); 
            for i in 1..SQL%BULK_EXCEPTIONS.COUNT
            loop
                deb('Ошибка #3 вставки в DTXREPLOBJ_DBT записи о платеже #1 (сделка #2)', demand_rq_arr(i).t_id, demand_rq_arr(i).t_docid,  SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
            end loop;            
        
            -- проставляем replstate=1 для всех успешно обработанных платежей
            forall i in indices of demand_rq_arr
                update dtxdemand_dbt 
                set t_replstate=1 where t_demandid = demand_add_arr(i).r_oldobjectid  
                and demand_add_arr(i).r_result <> 2 and demand_add_arr(i).r_subobjnum = 0;  -- для автоплатежей с r_subobjnum 81-84 не заполняем 
    
            deb('Выполнено обновление t_replstate в DTXDEMAND_DBT, количество записей - #1, количество ошибок - #2', demand_rq_arr.count, SQL%BULK_EXCEPTIONS.COUNT); 
            for i in 1..SQL%BULK_EXCEPTIONS.COUNT
            loop
                deb('Ошибка #3 обновления DTXDEMAND_DBT.t_replstate для платежа #1 (сделка #2)', demand_rq_arr(i).t_id, demand_rq_arr(i).t_docid,  SQL%BULK_EXCEPTIONS(i).ERROR_CODE,  p_level => 5);
            end loop;  
    
            -- буферные коллекции больше не нужны
            demand_rq_arr.delete;
            demand_add_arr.delete;

        end if; 
        execute immediate 'alter trigger DDLRQ_DBT_TBI enable';
        deb('Завершена процедура WRITE_DEMANDS');
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

        -- запись в лог
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


/*  Примечания
        TODO  в процедуру LOAD_RATE

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

begin
    deb('=== Выполняется инициирующая загрузка в пакете ===');
    -- заполнение списка подразделений
    for i in (select t_code, t_partyid from ddp_dep_dbt)
    loop
        ddp_dep_dbt_cache( i.t_partyid ) := i.t_code;
    end loop; 

    -- загрузка списка бирж, брокеров и контрагентов
    
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
    
    deb('=== Завершен исполняемый блок пакета ===');
end load_rss;
/
