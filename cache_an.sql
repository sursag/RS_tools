DECLARE 
    -- параметры --
    g_trace_f boolean := true;
    g_fasttrace_f boolean := false; -- быстрая трассировка. Журнал формируется в памяти, сбрасывается на диск только при ошибке или завершении --TODO
    g_fasttrace_flush_f boolean := false; -- быстрая трассировка. Журнал формируется в памяти, сбрасывается на диск или в бд при завершении процедуры. --TODO
    
    type tt is table of number index by pls_integer;
    t tt;

    --g_sql_str varchar2(2000) := 'select * from orders where order_id in (:param)';
    g_sql_str varchar2(2000) := 'select * from orders ';
    g_input_ID_type number(1) := 2;  -- Тип идентификаторов:  1 - varchar2, 2 - number
    
    ----
    
    
    err_code number;
    err number;
    
    -- типы входного набора идентификаторов
    type input_id_inv_num_arr_type is table of char(1) index by pls_integer; -- инверсный массив для ускорения поиска 
    type input_id_inv_char_arr_type is table of char(1) index by varchar2(500); 

--    type input_id_num_arr_type is table of number index by pls_integer; -- инверсный массив для ускорения поиска 
--    type input_id_char_arr_type is table of varchar2(500) index by pls_integer; -- инверсный массив для ускорения поиска
    
    -- типы датасета
    type field_type is record(
        val_num_arr DBMS_SQL.NUMBER_TABLE,
        val_char_arr DBMS_SQL.Varchar2_Table,
        val_date_arr DBMS_SQL.Date_Table,
        field_name varchar2(30),
        field_type number(1)
    );  
    type fields_type is table of field_type;       
    type dataset_type is record(
        field_count number,
        field_desc_arr DBMS_SQL.DESC_TAB,
        row_count number,
        row_limit number := 5000,
        val_arr fields_type     
    );
    
    dataset dataset_type; -- переменная набора данных
    input_id_num_arr    DBMS_SQL.NUMBER_TABLE; -- переменная списка идентификаторов, если они имеют тип number
    input_id_char_arr   DBMS_SQL.VARCHAR2_TABLE; -- переменная списка идентификаторов, если они имеют тип varchar2
    
    input_id_inv_num_arr    input_id_inv_num_arr_type; -- инверсные массивы для ускорения поиска
    input_id_inv_char_arr    input_id_inv_char_arr_type; -- инверсные массивы для ускорения поиска   
    
    -- вспомогательные процедуры
    PROCEDURE trc(p_line varchar2);
    PROCEDURE trc2(p_line varchar2, p_num number);
    PROCEDURE trc_dt(p_line varchar2);
    PROCEDURE errlog(p_err_num number, p_err_line varchar2);
    
    -- процедура заполнения входного массива, для разных типов идентификаторов
    PROCEDURE add_id( p_id number );
    PROCEDURE add_id( p_id varchar2 );

    -- процедура инициализации
    --PROCEDURE init( sqltext varchar2, id
    -- процедура запуска загрузки
    
       
    -- процедура чтения данных
    --PROCEDURE cursor_preparation(p_ds IN OUT dataset_type);
    
    -- процедура очистки массива
    
    
    PROCEDURE trc(p_line varchar2)
    is
    BEGIN
        if g_trace_f then
            dbms_output.put_line(p_line);
        END if;
    END trc;
    
    PROCEDURE trc2(p_line varchar2, p_num number)
    is
    BEGIN
        trc( p_line || ':  ' || to_char(p_num));
    END trc2;
    
    PROCEDURE trc_dt(p_line varchar2)
    is
    BEGIN
        trc( to_char(sysdate, 'hh24:mi:ss') || ':  ' || p_line );
    END trc_dt;
    
    PROCEDURE errlog(p_err_num number, p_err_line varchar2)
    is
    BEGIN
        -- пока так
        trc( 'Ошибка >>  ' || to_char(p_err_num) || '  >> ' || p_err_line);
    END;
    
    ----------------------------------
    
    PROCEDURE add_id( p_id number )
    is
    BEGIN
        IF g_input_ID_type = 2 THEN   /* number */
            IF not input_id_inv_num_arr.exists( p_id ) THEN
                input_id_num_arr( input_id_num_arr.count() ) := p_id;
                input_id_inv_num_arr( p_id ) := ' ';
            END IF;        
        ELSE 
            errlog(4, 'Некорректный тип входного ID, передается NUMBER');             
        END IF;
    
    END add_id;
    
    
    PROCEDURE add_id( p_id varchar2 )
    is
    BEGIN
        IF g_input_ID_type = 2 THEN   /* number */
            IF not input_id_inv_char_arr.exists( p_id ) THEN
                input_id_char_arr( input_id_num_arr.count ) := p_id;
                input_id_inv_char_arr( p_id ) := ' ';
            END IF;        
        ELSE 
            errlog(4, 'Некорректный тип входного ID, передается NUMBER');             
        END IF;
    
    END add_id;
    
   
    PROCEDURE cursor_preparation(p_ds IN OUT dataset_type)  
    is
        u_incorrect_field_type exception;
        col_cnt number;
        rec_cnt number;
        cur number;
        PROCEDURE cursor_closing( c in out number )
        IS BEGIN
            IF DBMS_SQL.IS_OPEN(c) THEN
               DBMS_SQL.CLOSE_CURSOR(c);
            END IF;
        END cursor_closing;        
    BEGIN
    
    input_id_num_arr(0) := 1;
    input_id_num_arr(1) := 2;
    input_id_num_arr(2) := 3;
    input_id_num_arr(3) := 4;
    input_id_num_arr(4) := 5;
    input_id_num_arr(5) := 6;
    input_id_num_arr(6) := 7;
    input_id_num_arr(7) := 8;
      
    
    
        -- todo  добавить связь с массивом входных данных. Может быть, разбить на две процедуры с передачей курсора
        trc_dt('cursor_preparation запущена');
        cur := dbms_sql.open_cursor;
        dbms_sql.parse(cur, g_sql_str, dbms_sql.native);
        /*
        CASE g_input_ID_type
        WHEN 1  THEN  -- VARCHAR2  
                DBMS_SQL.BIND_ARRAY(cur, ':param', input_id_char_arr);
        WHEN 2  THEN  -- NUMBER  
                DBMS_SQL.BIND_ARRAY(cur, ':param', input_id_num_arr);
        END CASE;
        */
        
        err_code := DBMS_SQL.EXECUTE( cur );
        DBMS_SQL.DESCRIBE_COLUMNS(cur, col_cnt, p_ds.field_desc_arr);
        trc('Определены поля');
        p_ds.val_arr := fields_type();
        FOR i in 1..col_cnt
        LOOP
            p_ds.val_arr.extend();
            trc2(p_ds.field_desc_arr(i).col_name, p_ds.field_desc_arr(i).col_type);
            
            CASE p_ds.field_desc_arr(i).col_type
            WHEN 1  /* VARCHAR */ THEN 
                DBMS_SQL.DEFINE_ARRAY( cur, i, p_ds.val_arr(i).val_char_arr, p_ds.row_limit, 0);
            WHEN 2  /* NUMBER */ THEN 
                dbms_output.put_line('--'||i);
                DBMS_SQL.DEFINE_ARRAY( cur, i, p_ds.val_arr(i).val_num_arr, p_ds.row_limit, 0);
            WHEN 12 /* DATE */ THEN 
                DBMS_SQL.DEFINE_ARRAY( cur, i, p_ds.val_arr(i).val_date_arr, p_ds.row_limit, 0);
            ELSE err := 3;
                raise u_incorrect_field_type;
            END CASE;
        END LOOP;
        
        trc_dt('cursor_preparation, чтение строк');
        p_ds.row_count := DBMS_SQL.FETCH_ROWS(cur);
        trc2('Считано строк', p_ds.row_count );
        
        DBMS_SQL.CLOSE_CURSOR(cur);
        trc_dt('cursor_preparation, чтение завершено');
/*
    exception 
        when u_incorrect_field_type then    
            cursor_closing(cur);
            errlog('Некорректный тип', 2);
            RAISE;
        when others then
            cursor_closing(cur);
            errlog( SQLERRM, SQLCODE);
            RAISE;
            */
    END cursor_preparation;
    
    
BEGIN
  cursor_preparation(dataset);
  dbms_output.put_line(  dataset.row_count);
  dbms_output.put_line(  dataset.val_arr(1).val_num_arr.count );
  dbms_output.put_line(  dataset.val_arr(2).val_num_arr.count );
  
END;