CREATE OR REPLACE PACKAGE load_rss
is
    -- параметры
    g_debug_output boolean := true;              -- записывать отладочную информацию в буфер DBMS_OUTPUT
    g_debug_table  boolean := false;             -- записывать отладочную информацию в таблицу
    
    g_ourbank         constant number := 13;     -- Код нашего банка. Проверить
    g_department      constant number := 1;      -- Департамент по умолчанию. См. ddp_dep_dbt
    g_use_needdemand  constant boolean := true;  -- Использовать NEEDDEMAND в сделках. Поскольку многие банки не используют его в принципе, нет смысла прогонять каждый раз запросы
    
    g_is_initialized  boolean := false;          -- флаг успешно проеденной иницализации. Чтобы не проводить ее повторно. Инициализрующий код в исполняемой части тела пакета использовать не получится
    g_fictive_comiss_contract number := null;    -- фиктивный контракт комиссии ( dsfcontr_dbt.t_id )
    g_oper            number := 1;               -- операционист, который будет прописываться во все таблицы. Может подменяться при вызове start_replication
    
    
    g_parallel_clause varchar2(100) := 'parallel(16)';  -- уровеь параллелизма, который применяется ко всем динамическим запросам.
    
    c_default_fictfi  number := 2192;  -- Фиктивный FIID для сделки с корзиной, по умолчанию.
        
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
    c_RATE_TYPE_NOMINALONDATE   constant number := 100;       -- репликация номиналов ц/б на дату (реплицируются из таблицы курсов)

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


                                      
    -- Функции для вызова из SQL
    function GetDealKind( p_kind number, p_avoirissid number, p_market number, p_isbasket char, p_isksu char)    return number PARALLEL_ENABLE ;  -- перекодирует тип сделки в домен целевой системы.
    function GetIsQuoted(p_fi number, p_date date) return char DETERMINISTIC PARALLEL_ENABLE;          -- возвращает признак котируемости ц/б.
    function GetCurrentNom(p_fi number, p_date date) return number DETERMINISTIC PARALLEL_ENABLE;      -- возвращает номинал ценной бумаги на дату
    function GetRateType( p_tp number ) return number DETERMINISTIC PARALLEL_ENABLE;    -- перекодирует тип курса из домена буферной схемы в домен целевой системы
    function GetFictContract return number DETERMINISTIC PARALLEL_ENABLE;  -- возвращает фиктивный контракт для комиссии. Устарело.
    function GetBasketFI return number DETERMINISTIC PARALLEL_ENABLE;    -- возвращает код фиктивной бумаги для сделки РЕПО с корзиной
    function GetDepartment return number DETERMINISTIC PARALLEL_ENABLE;  -- возвращает g_department - код департамента по умолчанию

    -- стартовые функции
    -- p_startdate определяет начальную дату репликации
    -- p_enddate определяет конечную дату репликации. Записи будут реплицированы по эу дату включительнно. Если не задано, она принимается равной (p_startdate + 1 день - 1 секунда)
    -- p_stage признак раздельного запуска, для отладки. Если = 1, будет выполнено только заполнение таблицы TMP. Если = 2, то только перенос данных в целевые таблицы
    -- Если null - будут выполнены оба шага. Любое другое значение - ошибка.
    procedure load_deals_by_period(p_startdate date, p_enddate date default null, p_stage number default null);
    procedure load_demands_by_period(p_startdate date, p_enddate date default null, p_stage number default null);
    procedure load_courses_by_period(p_startdate date, p_enddate date default null, p_stage number default null);
    procedure load_comiss_by_period(p_startdate date, p_enddate date default null, p_stage number default null);
    
    -- точка входа для макроса репликации
    -- если не заданы граничные даты, реплицирует за всё время
    procedure start_replication(p_startdate date default null, p_enddate date default null, p_oper number default null);

    g_my_SID  number; -- для сбора статистики по пореблению памяти и т.д.
    g_SESSION_ID number(10); -- номер сеанса для логов
    g_SESS_DETAIL_ID number(10); -- номер запуска, создается при вход в каждую стартовую процедуру. Для логов
    
end load_rss;
/