CREATE OR REPLACE PACKAGE GEB_20210823_TST.load_rss
is
    -- параметры
    g_debug_output boolean := true;         -- записывать отладочную информацию в буфер DBMS_OUTPUT
    g_debug_table  boolean := false;        -- записывать отладочную информацию в таблицу
    g_debug_level_limit constant pls_integer := 3;    -- максимальный уровень важности сообщений, который будет зафиксирован. важность уменьшается от 0 до 10.
                                            -- на уровне до 5 запрещена информация в логе по отдельным строкам, только агрегатная.
                                            -- считаем, что до 10 строк в обработке - отладочная выборка.
                                            -- При количестве записей в обработке < 10 этот параметр временно автоматически переключается в 10, после обработки восстанавливается
    g_debug_level_current pls_integer;
    g_oper constant number := 1;            -- операционист, который будет прописываться во все таблицы
    g_ourbank constant number := 12;        -- Код нашего банка. Проверить
    g_department  constant number := 1;     -- Департамент по умолчанию. См. ddp_dep_dbt
    
    g_use_needdemand constant boolean := true;  -- Использовать NEEDDEMAND в сделках. Поскольку многие банки не используют его в принципе, нет смысла прогонять каждый раз запросы
    
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
    c_RATE_TYPE_NOMINALONDATE   constant number := 100;  -- репликация номиналов ц/б на дату (реплицируются из таблицы курсов)

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
    function GetDealKind( p_kind number, p_avoirissid number, p_market number, p_isbasket char, p_isksu char)    return number DETERMINISTIC;
    function GetIsQuoted(p_fi number, p_date date) return char DETERMINISTIC;
    function GetIsKSU(p_fi number) return char DETERMINISTIC;
    function GetCurrentNom(p_fi number, p_date date) return number DETERMINISTIC;
    function GetRateType( p_tp number ) return number DETERMINISTIC;    
    
    -- стартовые функции
    procedure load_deals_by_period(p_startdate date, p_enddate date default null);
    procedure load_demands_by_period(p_startdate date, p_enddate date default null);
    procedure load_courses_by_period(p_startdate date, p_enddate date default null);


    g_my_SID  number; -- для сбора статистики по пореблению памяти и т.д.
    g_SESSION_ID number(10); -- номер сеанса для логов
    g_SESS_DETAIL_ID number(10); -- номер запуска, создается при вход в каждую стартовую процедуру. Для логов
    
end load_rss;
/
