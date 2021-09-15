CREATE OR REPLACE PACKAGE GEB_20210823.load_rss
is
    -- параметры
    g_limit number := 1000;             -- количество одновременно загружаемых записей источника
    g_debug_output boolean := true;     -- записывать отладочную информацию в буфер DBMS_OUTPUT
    g_debug_table  boolean := false;    -- записывать отладочную информацию в таблицу
    g_debug_level_limit pls_integer := 5;    -- максимальный уровень важности сообщений, который будет зафиксирован. важность уменьшается от 0 до 10.
    g_oper constant number := 1;  -- операционист, который будет прописываться во все таблицы
    g_ourbank constant number := 12;     -- Код нашего банка. Проверить
    g_department  constant number := 1;  -- Департамент по умолчанию. См. ddp_dep_dbt
        
    -- константы типов
    c_OBJTYPE_MONEY     constant number  := 10;
    c_OBJTYPE_AVOIRISS  constant number  := 20;
    c_OBJTYPE_PARTY     constant number  := 30;
    c_OBJTYPE_MARKET    constant number  := 30;
    c_OBJTYPE_SECTION   constant number  := 40;
    c_OBJTYPE_WARRANT   constant number  := 50;
    c_OBJTYPE_PARTIAL   constant number  := 60;
    c_OBJTYPE_RATE      constant number  := 70;
    c_OBJTYPE_DEAL      constant number  := 80;
    c_OBJTYPE_PAYMENT   constant number  := 90;

    -- специальные виды курсов
    c_RATE_TYPE_NKDONDATE       constant number := 15;        -- тип курса "НКД на дату"
    c_RATE_TYPE_NOMINALONDATE   constant number :=  100;  -- репликация номиналов ц/б на дату (реплицируются из таблицы курсов)

    -- виды портфелей
    c_KINDPORT_TRADE    constant number := 1;
    c_KINDPORT_LOAN     constant number := 0; -- надо задать
    c_KINDPORT_INVEST   constant number := 5;

    -- коды субъектов в dpartyown_dbt
    c_PTK_CONTR         constant number := 7; 
    c_PTK_BROKER        constant number := 22;
    c_PTK_MARKETPLACE   constant number := 3;
    
    -- коды бэкофисов
    c_DL_NTGDOC         constant number := 140; 
    c_DL_RETIREMENT     constant number := 117;
    c_DL_SECURITYDOC    constant number := 101;
    c_DL_GET_DIVIDEND   constant number := 158;

    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- оставляем в спецификации только те типы, которые используются в конструкции TABLE()
    -- кэш таблицы котировок из источника
    type rate_sou_arr_type is table of DTXCOURSE_DBT%ROWTYPE;
    type rate_sou_add_type is record(   -- дополнительные поля, привязываются к строкам коллекци rate_sou_arr_type по индексам
                                    tgt_rate_id number,  -- id курса в целевой системе, 0 если нет
                                    tgt_rate_val number, -- значение курса за дату в целевой системе
                                    tgt_state   number,  -- статус курса из replobj: 1-ручное изменение, 2-удален
                                    rate_date date,      -- дата текущего значения курса
                                    type_id  number,     -- перекодированный тип
                                    market_id number,    -- перекодированный ID биржи
                                    section_id number,   -- перекодированный ID сектора биржи
                                    base_fi number,      -- перекодированный базовый FI
                                    fi number,           -- перекодированный котируемый id
                                    isdominant char,     -- признак основного курса валюты, курс ЦБ
                                    isrelative char,     -- признак относительного курса
                                    result number(1)     -- будущий t_replstate. 1 - обработан успешно, 2 - ошибка
                                  );
    type rate_sou_add_arr_type is table of rate_sou_add_type index by pls_integer;

    rate_sou_arr        rate_sou_arr_type;
    rate_sou_add_arr    rate_sou_add_arr_type;
    
    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- кэш таблицы сделок 
    type deal_sou_arr_type is table of DTXDEAL_DBT%ROWTYPE;
    type deal_sou_add_type is record (
                                    tgt_dealid  number,
                                    tgt_avoirissid  number,
                                    tgt_currencyid  number,
                                    tgt_ogroup  number,
                                    tgt_paymcur number,
                                    tgt_nkdfiid number,
                                    tgt_type    number,
                                    tgt_market  number,
                                    tgt_sector  number,
                                    tgt_broker  number,
                                    tgt_party   number,
                                    tgt_parentid number,
                                    tgt_portfolioid number,
                                    tgt_repobase    number,
                                    tgt_warrantid   number,
                                    tgt_partialid   number,
                                    tgt_state   number,
                                    tgt_department number,
                                    tgt_bofficekind number,
                                    tgt_ismarketoper boolean,
                                    tgt_existback boolean,
                                    tgt_objtype number,
                                    is_judicialoper boolean := false, -- сделка по решениею суда, параметры могут быть нулевые
                                    is_basket   char(1) := chr(0),
                                    is_maindeal char(1) := chr(0),  -- главная сделка в операции РЕПО с корзиной. TODO оценить. нужно ли
                                    is_matched_by_code boolean,
                                    begindate   date,
                                    enddate     date,
                                    DDL_TICK_BUF    ddl_tick_dbt%rowtype,  -- найденная в целевой системе сделка и ее ценовые параметры
                                    DDL_LEG1_BUF    ddl_LEG_dbt%rowtype,
                                    DDL_LEG2_BUF    ddl_LEG_dbt%rowtype,
                                    result      number(1)  -- 2 - ошибка
                                    );
    type deal_sou_add_arr_type  is table of deal_sou_add_type index by pls_integer;
    type deal_sou_back_arr_type is table of number index by pls_integer; -- поисковая коллекция. Индексируется DDL_TICK_DBT.DEALID, содержит индекс буфера сделок
    
    type tmp_dealid_type    is record (T_DEALID number, T_BOFFICEKIND number );
    type tmp_dealid_arr_type is table of tmp_dealid_type  index by pls_integer;    
    
    deal_sou_arr        deal_sou_arr_type;
    deal_sou_add_arr    deal_sou_add_arr_type;
    deal_sou_back       deal_sou_back_arr_type; -- поисковая коллекция.
    ---------------------------------------------------------------------------------------------------------------------------------------------
    

    
                                                          
                                   


    -- основная процедура --
    procedure load_rates(p_date date, p_action number);
    procedure load_deals(p_date date, p_action number);

    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- работа с кэшем replobj
    -- основная таблица должна индексироваться числом, иначе невозможно будет использовать ее в выражении TABLE
    -- но для ускорения поиска при вводе добавляется отдельнаю индекснаю коллекция replobj_rec_inx_arr
    type replobj_rec_type            is record(obj_type number, obj_id number, obj_sub_id number, dest_id number, state number, comment varchar2(100));
    type replobj_rec_arr_type        is table of replobj_rec_type index by pls_integer;  -- основная коллекция
    type replobj_rec_inx_arr_type    is table of pls_integer index by varchar2(200);     -- индексная коллекция, указывает на индекс основной, индексируется конкатенацией составного ключа (objtype,obj_id,obj_sub_id)
    type replobj_tmp_arr_type        is table of dtxreplobj_dbt%rowtype;                 -- временный буфер для BULK COLLECT, пока не разбросаем по коллекциям выше

    replobj_rec_arr             replobj_rec_arr_type;
    replobj_rec_inx_arr         replobj_rec_inx_arr_type; -- индексная коллекция
    replobj_tmp_arr             replobj_tmp_arr_type;

    procedure replobj_add(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number := 0, p_comment varchar2 := null, p_destid number := -1 ); -- добавление входного ID в коллекцию
    procedure replobj_load; -- загрузка коллекции
    function  replobj_get(p_objtype pls_integer, p_obj_id number, p_obj_sub_id number := 0) return replobj_rec_type; -- возврат значения из кэша REPLOBJ
    -- procedure savestat;   -- записать в лог статистику по объему кэша в разрезе типов
    -- procedure replobj_clear; -- очистка всех коллекций
    ----------------------------------------------------------------------------------------------------------------------------------------------

    ---------------------------------------------------------------------------------------------------------------------------------------------
    -- работа с логом
    type log_rows_type is table of dtxloadlog_dbt%rowtype;  -- для отложенной записи в лог.
    log_rows  log_rows_type;
    -- процедура непосредственной записи в лог
    procedure add_log( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2, p_date date); -- запись в лог
    -- процедура отложенной записи. Когда ожидаем большой объем логирования с чисто логическими ошибками, можно накопить записи в коллекцию, потом протолкнуть в лог одним блоком
    -- procedure add_log_deferred( p_code number, p_objtype number, p_id number, p_subnum number, p_text varchar2(1000), p_date date); -- запись в лог
    -- procedure add_log_exec; -- сохраняем в таблицу лога. Очищаем буферную коллекцию.
    ---------------------------------------------------------------------------------------------------------------------------------------------

    procedure deb(p_text varchar2, num1 number default null, num2 number default null, num3 number default null, p_level pls_integer := 1);
    
    procedure deb_initialize(p_output boolean, p_table boolean);

    type tmp_arr_type is table of number index by pls_integer;
    type tmp_reverse_arr_type is table of number index by varchar2(100);    
    type dpartyown_arr_type is table of DPARTYOWN_DBT%ROWTYPE index by pls_integer; -- для новых записей в таблицу  DPARTYOWN_DBT
    
    type tmp_varchar_arr_type is table of varchar2(100) index by pls_integer;  -- очень вспомогательная
    type tmp_varchar_back_arr_type is table of pls_integer index by varchar2(100);  -- очень вспомогательная
    
end load_rss;
/
