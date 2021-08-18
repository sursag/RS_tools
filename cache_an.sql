DECLARE 
    -- параметры --
    g_trace_f boolean := true;
    
    g_sql_str varchar2(2000) := 'select * from orders';
    

    ----
    
    
    err_code number;
    err number;
    
    -- типы входного набора идентификаторов
    type input_id_arr_type is table of number;
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
    input_id_arr input_id_arr_type; -- переменная списка идентификаторов 
    
    -- вспомогательные процедуры
    PROCEDURE trc(p_line varchar2);
    PROCEDURE trc2(p_line varchar2, p_num number);
    
    -- процедура заполнения входного массива
    -- PROCEDURE add_id( p_id number );

    -- процедура запуска загрузки
    
       
    -- процедура чтения данных
    PROCEDURE cursor_preparation(p_ds IN OUT dataset_type);
    
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
        -- todo  добавить связь с массивом входных данных. Может быть, разбить на две процедуры с передачей курсора
    
        cur := dbms_sql.open_cursor;
        dbms_sql.parse(cur, g_sql_str, dbms_sql.native);
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
                DBMS_SQL.DEFINE_ARRAY( cur, i, p_ds.val_arr(i).val_num_arr, p_ds.row_limit, 0);
            WHEN 12 /* DATE */ THEN 
                DBMS_SQL.DEFINE_ARRAY( cur, i, p_ds.val_arr(i).val_date_arr, p_ds.row_limit, 0);
            ELSE err := 3;
                raise u_incorrect_field_type;
            END CASE;
        END LOOP;
        
        trc('Считываются строки');
        p_ds.row_count := DBMS_SQL.FETCH_ROWS(cur);
        trc2('Считано строк', p_ds.row_count );
        
        DBMS_SQL.CLOSE_CURSOR(cur);

    exception 
        when u_incorrect_field_type then    
            cursor_closing(cur);
            RAISE;
        when others then
            cursor_closing(cur);
            RAISE;
    END cursor_preparation;
    
    
BEGIN
  cursor_preparation(dataset);
 

END;