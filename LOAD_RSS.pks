CREATE OR REPLACE PACKAGE GEB_20210823_TST.load_rss
is
    -- параметры
    g_limit constant number := 200000;             -- количество одновременно загружаемых записей источника
    g_limit_demand constant number := 400000;      -- количество одновременно загружаемых записей источника, отдельно для платежей
    g_debug_output boolean := true;     -- записывать отладочную информацию в буфер DBMS_OUTPUT
    g_debug_table  boolean := false;    -- записывать отладочную информацию в таблицу
    g_debug_level_limit constant pls_integer := 3;    -- максимальный уровень важности сообщений, который будет зафиксирован. важность уменьшается от 0 до 10.
                                             -- на уровне до 5 запрещена информация в логе по отдельным строкам, только агрегатная.
                                             -- считаем, что до 10 строк в обработке - отладочная выборка.
                                             -- При количестве записей в обработке < 10 этот параметр временно автоматически переключается в 10, после обработки восстанавливается
    g_debug_level_current pls_integer;
    g_oper constant number := 1;  -- операционист, который будет прописываться во все таблицы
    g_ourbank constant number := 12;     -- Код нашего банка. Проверить
    g_department  constant number := 1;  -- Департамент по умолчанию. См. ddp_dep_dbt
    
    c_DEFAULT_FICTFI  constant number := 2192;  -- Фиктивный FIID для сделки с корзиной, по умолчанию.
        
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
                                    tgt_warrant_num   number,
                                    tgt_partial_num   number,
                                    tgt_state   number,
                                    tgt_department number,
                                    tgt_bofficekind number,
                                    tgt_ismarketoper boolean,
                                    tgt_existback boolean,
                                    tgt_objtype number,
                                    tgt_portfolio number,
                                    tgt_kind    number,
                                    tgt_country varchar2(3),
                                    is_judicialoper boolean := false, -- сделка по решениею суда, параметры могут быть нулевые
                                    is_basket   boolean := false,
                                    is_maindeal char(1) := chr(0),  -- главная сделка в операции РЕПО с корзиной. TODO оценить. нужно ли
                                    is_matched_by_code boolean,
                                    is_buy  boolean,
                                    is_sale boolean,
                                    is_loan boolean,
                                    is_repo boolean,
                                    is_loan_to_repo boolean,
                                    begindate   date,
                                    enddate     date,
                                    DDL_TICK_BUF    ddl_tick_dbt%rowtype,  -- найденная в целевой системе сделка и ее ценовые параметры
                                    DDL_LEG1_BUF    ddl_LEG_dbt%rowtype,
                                    DDL_LEG2_BUF    ddl_LEG_dbt%rowtype,
                                    result      number(1) := 0 -- 2 - ошибка
                                    );
    type deal_sou_add_arr_type  is table of deal_sou_add_type index by pls_integer;
    type deal_sou_back_arr_type is table of number index by pls_integer; -- поисковая коллекция. Индексируется DDL_TICK_DBT.DEALID, содержит индекс буфера сделок
    
    type tmp_dealid_type        is record (T_DEALID number, T_BOFFICEKIND number );
    type tmp_dealid_arr_type    is table of tmp_dealid_type  index by pls_integer;
    type tmp_dealcode_type      is record (INDEX_NUM number, T_DEALCODE varchar2(30)); 
    type tmp_dealcode_arr_type  is table of tmp_dealcode_type  index by pls_integer;    
    
    deal_sou_arr        deal_sou_arr_type;      -- основная коллекция сделок
    deal_sou_add_arr    deal_sou_add_arr_type;  -- дополнительная коллекция сделок
    deal_sou_back       deal_sou_back_arr_type; -- поисковая коллекция.
    ---------------------------------------------------------------------------------------------------------------------------------------------
    
    
    -------------------------------------------------------------------------------------------------------------------------------------------
    -- кэш таблицы платежей 
    
    type demand_sou_arr_type is table of DTXDEMAND_DBT%ROWTYPE;
    demand_sou_arr      demand_sou_arr_type;
    -- вместо вспомогательной коллекции сразу формируем запись, вмещающую все необходимые параметры,
    -- уже перекодированные. Нужно, поскольку платежи могут формироваться не только из DTXDEMAND_DBT,
    -- но и из сделок. Нужна минимальная структура для передачи параметров.
    
    -- то есть, для остальных типов объектов присутствует основная коллекция из источника (входной буфер), дополнительная коллекция,
    -- индексированная так же, и коллекция, содержащая записи в формате целевой системы ( назовем ее выходной буфер )
    -- здесь же дополнительная коллекция сопровождает не входной буфер, а выходной. И задача процедуры load_demand - только передать
    -- данные в add_demand, последующая их обработка не на ней.
    type demand_type is record(
                            tgt_demandid    number,
                            tgt_docid   number,
                            tgt_party   number,
                            tgt_fiid    number,
                            tgt_dockind number,
                            r_action    number(1),
                            r_isauto    boolean,  
                            r_isfact    boolean,
                            r_isnetting boolean,
                            r_part      number(1),
                            r_kind      number(2), -- в кодировке таблицы dtxdemand: 10-поставка ц/б, 40-оплата, 
                            r_direction number(1), -- 1-требования, 2-обязательства
                            r_fikind    number(2), -- 10-деньги, 20-бумаги
                            r_state     number(2),
                            r_sum       number,
                            r_note      varchar2(500),
                            r_balancerate number,
                            r_date      date,
                            r_oldobjectid number,  -- номер из dtxdemand для простановки replstate
                            r_subobjnum number,    -- что должно проставиться в dtxreplobj.T_SUBOBJNUM
                            r_destsubobjnum number,-- что должно проставиться в dtxreplobj.T_DESTSUBOBJNUM
                            r_result    number(1)  -- статус. 0 - нормально, 2 - ошибка в параметрах платежа
                            );
                                    

    -- выходная структура. Заполняется, затем разом записывается    
    type demand_rq_arr_type is table of ddlrq_dbt%rowtype index by pls_integer;
    demand_rq_arr   demand_rq_arr_type;
    
    -- дополнительная коллекция записей, которая сопровождает структуру RQ для работы с DTXDEMAND_DBT и DTXREPLOBJ_DBT. Где-то надо держать DTXDEMAND_DBT.t_demandid 
    type demand_add_type is record(
                            r_oldobjectid number,
                            r_subobjnum number,
                            r_destsubobjnum number,
                            r_action    number,
                            r_result    number(1) -- статус. 0 - нормально, 2 - техническая ошибка при вставке платежа
                            );
    type demand_add_arr_type is table of demand_add_type index by pls_integer;
    demand_add_arr      demand_add_arr_type; 
                       
                                      
    function GetDealKind( p_kind number, p_avoirissid number, p_market number, p_isbasket char, p_isksu char)    return number DETERMINISTIC;
                    
    -- процедура добавления платежа в кэш DEMAND_RQ_ARR
    procedure   add_demand (p_demand   demand_type);
    -- процедура записи платежей из кэша в БД. Используется в load_demands и load_deals
    procedure   write_demands( p_action number );                             


    -- основная процедура --
    procedure load_rates(p_date date, p_action number);
    procedure load_deals(p_date date, p_action number);
    procedure load_demands(p_date date, p_action number);
    

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

    -- работа с категориями объектов и примечаниями.
    -- группируем в коллекцию, потом массово вставляем
    type note_type is record(
                        r_objtype   number(5),
                        r_struniid  varchar2(50),
                        r_kind      number,
                        r_value     varchar2(500),
                        r_date      date
                        );
    type note_arr_type  is table of note_type index by pls_integer;
    note_arr    note_arr_type;
    
    type categ_type is record(
                        r_objtype   number(5),
                        r_struniid  varchar2(50),
                        r_kind      number,
                        r_value     varchar2(500),
                        r_date      date
                        );
    type categ_arr_type  is table of categ_type index by pls_integer;
    categ_arr    categ_arr_type;
    
    -- и процедуры к ним
    procedure add_note( p_objtype number, p_objectid number, p_kind number, p_value varchar2, p_date  date);
    procedure write_notes;
    procedure add_categ( p_objtype number, p_objectid number, p_kind number, p_value varchar2, p_date  date);
    procedure write_categs;
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

    my_SID  number; -- для сбора статистики по пореблению памяти и т.д.
    g_SESSION_ID number(10);
    g_SESS_DETAIL_ID number(10);
    
end load_rss;
/
