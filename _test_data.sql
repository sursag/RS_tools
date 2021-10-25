-- ���� ��������� �������� ������ ��� ������������ ������������
-- ������������ ������ �� ������ � �������� � �������� DTX*
-- ���� ����������� ������� ��������� id ��� ������ ���������. ������ ����� �����������, ������� � ���� �������.
-- ����� �������� ��������� ������ ������� ������� ��� ������ �� DTX � ������� �������, ������� ���������� � ��������� �������
-- ��� ��� ����, ����� �������� ������� � ���������������� ������ � ������ ������ ��������.
-- ���������! ������� ���������� � ������� � ���������� ��������� ���������.

declare
    -- ���������
    p_start_date      date    := date'2021-08-01'; -- ���� ������ ������������
    p_end_date        date    := date'2021-08-31'; -- ���� ��������� ������������
    p_dealid_init     number := 20000000000; -- ��������� ����� dealid
    p_demandid_init   number := 20000000000; -- ��������� ����� demandid
    p_type_buy_count  number  := 50000;  -- ���������� ������ ������� �� ����
    p_type_sale_count number := 50000;  -- ���������� ������ ������� �� ����
    p_type_repo_count number := 100000;  -- ���������� ������ ���� �� ����
    p_use_needdemand  boolean := false;   -- ������������ t_needdemand;
    p_market_id       number  := 10000178951; -- partyid �����
    p_sector_id       number  := 10000000000; -- id �������
    p_currency_id     number  := 2;     -- id ��� ������ ������ � ������� (�����) 
        
    -- ����������
    deal_tmp        dtxdeal_dbt%rowtype;
    demand_tmp      dtxdemand_dbt%rowtype; 
    avr_tmp         dtxavoiriss_dbt%rowtype;
    type avr_arr_type is table of dtxavoiriss_dbt%rowtype;
    avr_arr avr_arr_type;
    isshare         boolean;
    dealid_count    number;
    demandid_count  number; 
    curdate         date;
    avr_count       number; -- ���������� ��������� �����
    clop            number;
    dealtype        number; -- 10,20,40
    
    type deal_arr_type is table of dtxdeal_dbt%rowtype index by pls_integer;
    type demand_arr_type is table of dtxdemand_dbt%rowtype index by pls_integer;
    deal_arr deal_arr_type;
    demand_arr demand_arr_type;
    
    -- ������������� ����� �������� ����, ����� �������� �� PGA
    procedure flush_buffer
    is
    begin
        forall i in indices of deal_arr
            insert into dtxdeal_dbt values deal_arr(i);
        
        forall i in indices of demand_arr
            insert into dtxdemand_dbt values demand_arr(i);
        commit;
        deal_arr.delete;
        demand_arr.delete;
    end flush_buffer;
    
begin
    -- ������� ����
    delete from ddlrq_dbt  where t_docid in (select t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objectid >= p_dealid_init);
    delete from ddl_leg_dbt where t_dealid in (select t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objectid >= p_dealid_init);
    delete from ddl_tick_dbt where t_dealid in (select t_destid from dtxreplobj_dbt where t_objecttype=80 and t_objectid >= p_dealid_init);
    delete from dnotetext_dbt;
    delete from dobjatcor_dbt;
    delete from dtxreplobj_dbt where t_objecttype=80 and t_objectid >= p_dealid_init;
    delete from dtxreplobj_dbt where t_objecttype=90 and t_objectid >= p_demandid_init;
    delete from dtxdeal_dbt where t_dealid >= p_dealid_init;
    delete from dtxdemand_dbt where t_demandid >= p_demandid_init;
    commit;
    -- ���� ����� ��������� ����� ����������� � �����, ����� ������� �������� ��������� �������
    select * bulk collect into avr_arr from dtxavoiriss_dbt where t_avoirissid in (select t_objectid from dtxreplobj_dbt where t_objecttype=20 and t_objstate=0) and T_FACEVALUE > 1;
    
    deal_tmp.t_tskind := 'P';
    deal_tmp.t_marketid := p_market_id;
    deal_tmp.t_partyid := p_market_id;
    deal_tmp.t_sector := p_sector_id;
    deal_tmp.t_action := 1;
    deal_tmp.t_replstate := 0;
    deal_tmp.T_CURRENCYID := p_currency_id;
    deal_tmp.T_POINT := 4;
    deal_tmp.T_PORTFOLIOID := 2;
    deal_tmp.T_COUNTRY := 165;
    deal_tmp.T_PAYMCUR := p_currency_id;
    dealid_count := p_dealid_init;
    demandid_count := p_demandid_init;
    if p_use_needdemand then 
        deal_tmp.t_needdemand := chr(88);
    end if;     
    
    -- ������� ���� �� ����
    for l_date in 0..(p_end_date-p_start_date)
    loop
        curdate := p_start_date + l_date - 1;
        dbms_output.put_line(curdate);
        deal_tmp.T_INSTANCEDATE := curdate + 1; 
        deal_tmp.T_DATE := curdate;
        deal_tmp.T_PAYDATE := curdate;
        deal_tmp.T_SUPLDATE := curdate;
        deal_tmp.T_PAYDATE2 := null;
        deal_tmp.T_SUPLDATE2 := null;
        deal_tmp.T_PRICE2 := null;
        deal_tmp.T_COST2 := null;
        deal_tmp.T_NEEDDEMAND2 := null;
        deal_tmp.T_TOTALCOST2 := null;
        deal_tmp.T_NKD2 := null;
        deal_tmp.T_RATE := null;
        deal_tmp.T_BALANCEDATE := curdate;
        deal_tmp.T_CONTRNUM := to_char(mod(abs(dbms_random.random),100000));
        deal_tmp.T_CONTRDATE := curdate;
        
        for i in 1..( p_type_buy_count + p_type_sale_count + p_type_repo_count)
        loop
            -- ����� ��������� �� ����, ������� ������
            if deal_arr.count > 10000
            then
                flush_buffer;
            end if;
        
            deal_tmp.T_DEALID := dealid_count;
            dealtype := case when i between 1 and p_type_buy_count then 10
                             when i between p_type_buy_count+1 and p_type_buy_count+p_type_sale_count then 20
                             when i > p_type_buy_count + p_type_sale_count then 40
                        end;
        
            deal_tmp.T_TIME  := mod(abs(dbms_random.random)/1000,1) + date'0001-01-01';
            deal_tmp.T_MARKETCODE := to_char(abs(dbms_random.random));
            deal_tmp.T_EXTCODE := to_char(abs(dbms_random.random));
            deal_tmp.T_CODE := deal_tmp.T_EXTCODE;
            deal_tmp.T_KIND := dealtype;
            if p_use_needdemand then
                deal_tmp.t_needdemand := chr(88);
            else
                deal_tmp.t_needdemand := chr(0);
            end if;
            
            -- ����� ������������ ������
            clop := mod(abs(dbms_random.random), avr_arr.count-1) + 1;
            avr_tmp := avr_arr(clop);  
            deal_tmp.t_avoirissid := avr_tmp.t_avoirissid;
            
            if avr_tmp.t_kind in (1,2,7,10,20)
            then 
                isshare := true;
                deal_tmp.t_nkd := 0;
            else 
                isshare := false;
                deal_tmp.t_nkd := mod( abs(dbms_random.random), 100 ); 
            end if; 
            
            deal_tmp.t_amount := mod( abs(dbms_random.random), 1000 ) + 1;
            
            
            if isshare 
            then
                deal_tmp.t_price := round( mod( dbms_random.random, avr_tmp.t_facevalue/3 ) + avr_tmp.t_facevalue, 2); -- ������� ���� � ��������� +- 1/3 �� ��������
                deal_tmp.t_cost := round(deal_tmp.t_amount * deal_tmp.t_price, 2);
            else
                deal_tmp.t_price := round(mod( abs(dbms_random.random), 200 )/10,2) + 90; -- � ��������� �� ��������. ����� � ��������� 90-110%
                deal_tmp.t_cost := round(deal_tmp.t_amount * deal_tmp.t_price/100 * avr_tmp.t_facevalue, 2);
            end if; 
            deal_tmp.t_totalcost := deal_tmp.t_cost + deal_tmp.t_nkd;    
            
            if dealtype > 20 -- ����
            then
                clop := mod( abs(dbms_random.random), 60 ) + 10;
                deal_tmp.t_paydate2 := deal_tmp.t_paydate +  clop;
                deal_tmp.t_supldate2 := deal_tmp.t_paydate +  clop;
                deal_tmp.t_rate := round(mod( abs(dbms_random.random), 200 )/10,2);
                deal_tmp.t_price2 := deal_tmp.t_price * (1 + deal_tmp.t_rate/100);
                deal_tmp.t_cost2 := round(deal_tmp.t_cost * (1 + deal_tmp.t_rate/100), 2);
                deal_tmp.t_totalcost2 := deal_tmp.t_cost2;
            end if;  
            
            --insert into dtxdeal_dbt values deal_tmp;
            deal_arr(deal_arr.count) := deal_tmp;
            
            if not p_use_needdemand then 
                demand_tmp.t_action := 1;
                demand_tmp.t_replstate := 0;
                demand_tmp.t_instancedate := deal_tmp.t_instancedate;
                demand_tmp.t_date := deal_tmp.t_date;
                demand_tmp.t_dealid := deal_tmp.t_dealid;
                
                -- �� ������, 1 �����
                demand_tmp.t_part := 1;
                demand_tmp.t_demandid := demandid_count;
                demand_tmp.t_kind := 10; -- ��������
                demand_tmp.t_fikind := 20; -- ������
                demand_tmp.t_sum := deal_tmp.t_amount;
                demand_tmp.t_paysum := null;
                demand_tmp.t_paycurrencyid := null;
                demand_tmp.t_payrate := null;
                demand_tmp.t_balancerate := 1;
                if  deal_tmp.t_kind = 10 -- �������
                then
                    demand_tmp.t_direction := 1; -- ����������
                else    
                    demand_tmp.t_direction := 2; -- �������������
                end if;
                demand_tmp.t_isfact := chr(88);
                demand_tmp.t_state := 3;
                demand_tmp.t_plandemend := demandid_count + 1; 
                
                -- ������� ������������ ������� �� �������
                --insert into dtxdemand_dbt values demand_tmp;
                demand_arr(demand_arr.count) := demand_tmp;
                
                demandid_count := demandid_count + 1;
                demand_tmp.t_demandid := demandid_count;
                demand_tmp.t_isfact := chr(0);
                demand_tmp.t_state := 1;
                demand_tmp.t_balancerate := null;
                demand_tmp.t_plandemend := null;
                
                -- ������� ��������� ������� �� �������
                --insert into dtxdemand_dbt values demand_tmp;
                demand_arr(demand_arr.count) := demand_tmp;
                
                demandid_count := demandid_count + 1;
                demand_tmp.t_demandid := demandid_count;
                
                -- �� �������, 1 �����
                demand_tmp.t_isfact := chr(88);
                demand_tmp.t_state := 3;
                demand_tmp.t_kind := 40; --������
                demand_tmp.t_direction := case when demand_tmp.t_direction = 1 then 2 else 1 end;  -- �������������� 
                demand_tmp.t_fikind := 10; --������
                demand_tmp.t_sum := deal_tmp.t_totalcost;
                demand_tmp.t_paysum := deal_tmp.t_totalcost; 
                demand_tmp.t_paycurrencyid := p_currency_id;
                demand_tmp.t_balancerate := 1;
                demand_tmp.t_plandemend := demandid_count + 1;
                
                -- ������� ������������ ������� �� �������
                --insert into dtxdemand_dbt values demand_tmp; 
                demand_arr(demand_arr.count) := demand_tmp; 
                
                demandid_count := demandid_count + 1;
                demand_tmp.t_demandid := demandid_count;
                demand_tmp.t_isfact := chr(0);
                demand_tmp.t_state := 1;
                demand_tmp.t_balancerate := null;
                demand_tmp.t_plandemend := null;
                
                -- ������� ��������� ������� �� �������
                --insert into dtxdemand_dbt values demand_tmp;  
                demand_arr(demand_arr.count) := demand_tmp; 

                demandid_count := demandid_count + 1; 
                
                if  deal_tmp.t_kind = 40 -- ���� ������
                then                                   
                    if p_use_needdemand then 
                        deal_tmp.t_needdemand2 := chr(88);
                    end if; 
                
                    -- �� ������, 2 �����
                    demand_tmp.t_part := 2;
                    demand_tmp.t_demandid := demandid_count;
                    demand_tmp.t_kind := 10; -- ��������
                    demand_tmp.t_fikind := 20; -- ������
                    demand_tmp.t_sum := deal_tmp.t_amount;
                    demand_tmp.t_paysum := null;
                    demand_tmp.t_paycurrencyid := null;
                    demand_tmp.t_payrate := null;
                    if  deal_tmp.t_kind = 40 -- ���� ������
                    then
                        demand_tmp.t_direction := 1; -- ����������
                    else    
                        demand_tmp.t_direction := 2; -- �������������
                    end if;
                    demand_tmp.t_isfact := chr(88);
                    demand_tmp.t_state := 3;
                    demand_tmp.t_plandemend := demandid_count + 1;
                    
                    -- ������� ������������ ������� �� �������, 2 �����
                    --insert into dtxdemand_dbt values demand_tmp;
                    demand_arr(demand_arr.count) := demand_tmp;
                    
                    demandid_count := demandid_count + 1;
                    demand_tmp.t_demandid := demandid_count;
                    demand_tmp.t_isfact := chr(0);
                    demand_tmp.t_state := 1;
                    demand_tmp.t_balancerate := null;
                    demand_tmp.t_plandemend := null;
                    
                    -- ������� ��������� ������� �� �������, 2 �����
                    --insert into dtxdemand_dbt values demand_tmp;
                    demand_arr(demand_arr.count) := demand_tmp;
                    
                    demandid_count := demandid_count + 1;
                    demand_tmp.t_demandid := demandid_count;
                    
                    -- �� �������, 2 �����
                    demand_tmp.t_isfact := chr(88);
                    demand_tmp.t_state := 3;
                    demand_tmp.t_kind := 40; --������
                    demand_tmp.t_direction := case when demand_tmp.t_direction = 1 then 2 else 1 end;  -- �������������� 
                    demand_tmp.t_fikind := 10; --������
                    demand_tmp.t_sum := deal_tmp.t_totalcost;
                    demand_tmp.t_paysum := deal_tmp.t_totalcost; 
                    demand_tmp.t_paycurrencyid := p_currency_id;
                    demand_tmp.t_balancerate := 1;
                    demand_tmp.t_plandemend := demandid_count + 1;
                    
                    -- ������� ������������ ������� �� �������
                    --insert into dtxdemand_dbt values demand_tmp; 
                    demand_arr(demand_arr.count) := demand_tmp; 
                    
                    demandid_count := demandid_count + 1;
                    demand_tmp.t_demandid := demandid_count;
                    demand_tmp.t_isfact := chr(0);
                    demand_tmp.t_state := 1;
                    demand_tmp.t_balancerate := null;
                    demand_tmp.t_plandemend := null;
                    
                    -- ������� ��������� ������� �� �������
                    --insert into dtxdemand_dbt values demand_tmp;  
                    demand_arr(demand_arr.count) := demand_tmp; 
    
                    demandid_count := demandid_count + 1;                 
                
                end if; -- ����
            end if; -- needdemand
            
            dealid_count := dealid_count + 1;
        end loop;
        -- ������� �������
        flush_buffer;
        
    end loop;
    
end; 
