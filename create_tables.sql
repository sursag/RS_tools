-- ������� �������
CREATE TABLE DTX_SESSION_DBT
(
  T_SESSID     NUMBER(10) GENERATED ALWAYS AS IDENTITY ( START WITH 21 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NOCYCLE CACHE 20 NOORDER NOKEEP NOSCALE) NOT NULL,
  T_STARTDATE  DATE,
  T_ENDDATE    DATE,
  T_USER       VARCHAR2(100 CHAR),
  T_STATUS     CHAR(1 CHAR)
);

-- ������� ������� ������
CREATE TABLE GEB_20210823_TST.DTX_SESS_DETAIL_DBT
(
  T_DETAILID      NUMBER(10) GENERATED ALWAYS AS IDENTITY ( START WITH 41 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NOCYCLE CACHE 20 NOORDER NOKEEP NOSCALE) NOT NULL,
  T_SESSID        NUMBER(5),
  T_PROCEDURE     VARCHAR2(128 CHAR),
  T_INSTANCEDATE  DATE,
  T_STARTDATE     DATE,
  T_ENDDATE       DATE,
  T_DURATION      NUMBER(5),
  T_RESULT        CHAR(1 CHAR),
  T_TOTALROWS     NUMBER(10),
  T_ERRORS        NUMBER(10)
);




-- ������� ��������
CREATE TABLE DTX_QUERY_DBT
(
  T_SCREENID    NUMBER(5) GENERATED ALWAYS AS IDENTITY ( START WITH 61 MAXVALUE 9999999999999999999999999999 MINVALUE 1 NOCYCLE CACHE 20 NOORDER NOKEEP NOSCALE) NOT NULL,
  T_OBJECTTYPE  NUMBER(5),
  T_SET         NUMBER(1),
  T_NUM         NUMBER(5),
  T_TEXT        VARCHAR2(2048 CHAR),
  T_SEVERITY    NUMBER(2),
  T_DESC        VARCHAR2(2048 BYTE),
  T_NAME        VARCHAR2(200 BYTE),
  T_USE_BIND    CHAR(1 CHAR),
  T_IN_USE      CHAR(1 CHAR)                    DEFAULT chr(88)
);

CREATE UNIQUE INDEX DTX_QUERY_DBT_ID1 ON DTX_QUERY_DBT (T_OBJECTTYPE,T_SET, T_NUM);


-- �������� �������� ������
CREATE TABLE DTX_ERROR_DBT
(
  T_SESSID      NUMBER(10),
  T_DETAILID    NUMBER(10),
  T_OBJECTTYPE  NUMBER(5),
  T_OBJECTID    NUMBER(15),
  T_QUERYID     NUMBER(5),
  T_ERRORCODE   NUMBER(4),
  T_TIMESTAMP   DATE
);



CREATE TABLE DTXDEAL_TMP
(
  T_DEALID                  NUMBER(15)          NOT NULL,
  T_INSTANCEDATE            DATE                NOT NULL,
  T_ACTION                  NUMBER(5),
  T_REPLSTATE               NUMBER(5),
  T_KIND                    NUMBER(5)           NOT NULL,
  T_EXTCODE                 VARCHAR2(30 CHAR),
  T_MARKETCODE              VARCHAR2(30 CHAR),
  T_PARTYCODE               VARCHAR2(30 CHAR),
  T_CODE                    VARCHAR2(30 CHAR),
  T_DATE                    DATE,
  T_TIME                    DATE,
  T_CLOSEDATE               DATE,
  T_TECHTYPE                NUMBER(5),
  T_TSKIND                  CHAR(1 CHAR),
  T_ACCOUNTTYPE             NUMBER(5),
  T_MARKETID                NUMBER(15),
  T_SECTOR                  NUMBER(15),
  T_BROKERID                NUMBER(10),
  T_PARTYID                 NUMBER(15),
  T_DEPARTMENT              NUMBER(10),
  T_AVOIRISSID              NUMBER(15),
  T_WARRANTID               NUMBER(15),
  T_PARTIALID               NUMBER(15),
  T_AMOUNT                  NUMBER(32,12),
  T_CURRENCYID              NUMBER(15),
  T_PRICE                   NUMBER(32,12),
  T_POINT                   NUMBER(5),
  T_COST                    NUMBER(32,12),
  T_NKD                     NUMBER(32,12),
  T_TOTALCOST               NUMBER(32,12),
  T_RATE                    FLOAT(53),
  T_PRICE2                  NUMBER(32,12),
  T_COST2                   NUMBER(32,12),
  T_NKD2                    NUMBER(32,12),
  T_TOTALCOST2              NUMBER(32,12),
  T_PAYDATE                 DATE,
  T_SUPLDATE                DATE,
  T_PAYDATE2                DATE,
  T_SUPLDATE2               DATE,
  T_CONTRNUM                VARCHAR2(15 CHAR),
  T_CONTRDATE               DATE,
  T_REPOBASE                NUMBER(5),
  T_COSTCHANGEONCOMP        CHAR(1 CHAR),
  T_COSTCHANGE              CHAR(1 CHAR),
  T_COSTCHANGEONAMOR        CHAR(1 CHAR),
  T_ADJUSTMENT              CHAR(1 CHAR),
  T_NEEDDEMAND              CHAR(1 CHAR),
  T_ATANYDAY                CHAR(1 CHAR),
  T_CONDITIONS              VARCHAR2(1500 CHAR),
  T_PAYMCUR                 NUMBER(15),
  T_ISPFI_1                 CHAR(1 CHAR),
  T_ISPFI_2                 CHAR(1 CHAR),
  T_COUNTRY                 NUMBER(5),
  T_NKDFIID                 NUMBER(15),
  T_LIMIT                   CHAR(1 CHAR),
  T_CHRATE                  CHAR(1 CHAR),
  T_CHAVR                   DATE,
  T_DIV                     CHAR(1 CHAR),
  T_BALANCEDATE             DATE,
  T_DOPCONTROL              NUMBER(5),
  T_DOPCONTROL_NOTE         VARCHAR2(255 CHAR),
  T_FISSKIND                NUMBER(5),
  T_PRICE_CALC_METHOD       NUMBER(5),
  T_PRICE_CALC              NUMBER(32,12),
  T_PRICE_CALC_VAL          NUMBER(10),
  T_PRICE_CALC_DEF          NUMBER(32,12),
  T_PRICE_CALC_OUTLAY       NUMBER(32,12),
  T_PARENTID                NUMBER(15),
  T_PRICE_CALC_MET_NOTE     VARCHAR2(1000 CHAR),
  T_NEEDDEMAND2             CHAR(1 CHAR),
  T_INITBUYDATE             DATE,
  T_CONTROL_DEAL_NOTE       NUMBER(32,12),
  T_CONTROL_DEAL_NOTE_DATE  DATE,
  T_REPO_PROC_ACCOUNT       VARCHAR2(20 CHAR),
  T_PRIOR_PORTFOLIOID       CHAR(1 CHAR),
  T_PORTFOLIOID             CHAR(1 CHAR),
  T_NETTING_DEALID_DEST     NUMBER(15),
  TGT_DEALID                NUMBER(15),
  TGT_BOFFICEKIND           NUMBER(3),
  TGT_PAYMCUR               NUMBER(15),
  TGT_CURRENCYID            NUMBER(15),
  TGT_NKDFIID               NUMBER(15),
  TGT_AVOIRISSID            NUMBER(15),
  TGT_AVOIRKIND             NUMBER(3),
  TGT_CURNOM                NUMBER,
  TGT_MARKETID              NUMBER(15),
  TGT_SECTOR                NUMBER(15),
  TGT_BROKERID              NUMBER(15),
  TGT_PARENTID              NUMBER(15),
  TGT_WARRANTID             NUMBER(15),
  TGT_PARTIALID             NUMBER(15),
  TGT_WARRANT_NUM           NUMBER(5),
  TGT_PARTIAL_NUM           NUMBER(5),
  TGT_PORTFOLIOID           NUMBER(5),
  TGT_PORTFOLIOID_2         NUMBER(2),
  TGT_REPOBASE              NUMBER(5),
  TGT_STATE                 NUMBER(5),
  TGT_DEPARTMENT            NUMBER(5),
  TGT_DEALKIND              NUMBER(5),
  TGT_GROUP                 NUMBER(5),
  TGT_ISBUY                 CHAR(1 CHAR),
  TGT_ISSALE                CHAR(1 CHAR),
  TGT_ISLOAN                CHAR(1 CHAR),
  TGT_ISREPO                CHAR(1 CHAR),
  TGT_ISBASKET              CHAR(1 CHAR),
  TGT_ISLOANTOREPO          CHAR(1 CHAR),
  TGT_ISBOND                CHAR(1 CHAR),
  TGT_ISQUOTED              CHAR(1 CHAR),
  TGT_ISKSU                 CHAR(1 CHAR),
  TGT_RELATIVEPRICE         CHAR(1 CHAR),
  TGT_COUNTRY               VARCHAR2(3 CHAR),
  TGT_PARTYID               NUMBER(15),
  TGT_OBJTYPE               NUMBER(3),
  TGT_PRICE                 NUMBER(32,12),
  TGT_MATURITY              DATE,
  TGT_EXPIRY                DATE,
  TGT_MATURITYISPRINCIPAL   CHAR(1 CHAR),
  TGT_MATURITY2             DATE,
  TGT_EXPIRY2               DATE,
  TGT_MATURITYISPRINCIPAL2  CHAR(1 CHAR),
  TGT_FORMULA               NUMBER(3),
  TGT_RECEIPTAMOUNT         NUMBER
) compress nologging;



CREATE TABLE DTXDEMAND_TMP
(
  T_DEMANDID       NUMBER(15)                   NOT NULL,
  T_INSTANCEDATE   DATE                         NOT NULL,
  T_ACTION         NUMBER(5),
  T_REPLSTATE      NUMBER(5),
  T_DEALID         NUMBER(15)                   NOT NULL,
  T_PART           NUMBER(5),
  T_ISFACT         CHAR(1 CHAR),
  T_KIND           NUMBER(5),
  T_DIRECTION      NUMBER(5),
  T_FIKIND         NUMBER(5),
  T_DATE           DATE,
  T_SUM            NUMBER(32,12),
  T_PAYCURRENCYID  NUMBER(15),
  T_PAYSUM         NUMBER(32,12),
  T_PAYRATE        FLOAT(53),
  T_BALANCERATE    FLOAT(53),
  T_NETTING        NUMBER(15),
  T_PLANDEMEND     NUMBER(15),
  T_NOTE           VARCHAR2(1500 CHAR),
  T_STATE          NUMBER(5),
  TGT_DEMANDID     NUMBER(15),
  TGT_DEALID       NUMBER(15),
  TGT_PARTY        NUMBER(15),
  TGT_DEALKIND     NUMBER(5),
  TGT_FIID         NUMBER(15),
  TGT_BOFFICEKIND  NUMBER(3),
  TGT_KIND         NUMBER(1), -- 0-����������, 1-�������������
  TGT_PLANDATE     DATE,
  TGT_FACTDATE     DATE,
  TGT_TYPE         NUMBER(2), -- 8-��������, 2-������
  TGT_SUBKIND      NUMBER(1) -- ��� �� � ��: 0-������, 1-������ ������
) compress nologging;



CREATE TABLE DTXCOURSE_TMP
(
  T_COURSEID        NUMBER(15)                  NOT NULL,
  T_TYPE            NUMBER(5)                   NOT NULL,
  T_INSTANCEDATE    DATE                        NOT NULL,
  T_ACTION          NUMBER(5),
  T_REPLSTATE       NUMBER(5),
  T_BASEFIKIND      NUMBER(5),
  T_BASEFIID        NUMBER(15),
  T_FIID            NUMBER(15),
  T_MARKETID        NUMBER(15),
  T_MARKETSECTORID  NUMBER(15),
  T_POINT           NUMBER(5),
  T_SCALE           NUMBER(10),
  T_RATEDATE        DATE,
  T_RATE            FLOAT(53),
  TGT_RATEID        NUMBER(15),
  TGT_TYPE          NUMBER(5),
  TGT_MARKETID      NUMBER(15),
  TGT_SECTORID      NUMBER(15),
  TGT_BASEFIID      NUMBER(15),
  TGT_FIID          NUMBER(15),
  TGT_ISLAST        CHAR(1 BYTE)
) compress nologging;





create table dtx_errorkinds_dbt (t_code number(4) primary key, t_desc varchar2(1024 char));

CREATE TABLE DTX_QUERYLOG_DBT
(
  T_SESSION     NUMBER(10),
  T_SESSDETAIL  NUMBER(10),
  T_STARTTIME   DATE,
  T_DURATION    NUMBER(5),
  T_TEXT        VARCHAR2(300 CHAR),  
  T_OBJECTYPE   NUMBER(2),
  T_SET         NUMBER(5),
  T_NUM         NUMBER(5),
  T_RESULT      CHAR(1 CHAR),
  T_TOTALROWS   NUMBER(10),
  T_EXECROWS    NUMBER(10)
);



create or replace view v_log as
select t_starttime start_time, trunc(t_duration/60)||':'||lpad(mod(t_duration,60),2,'0') Exec_Time, t_text,  t_set, t_num, trunc(total/60)||':'||lpad(mod(total,60),2,'0') Total_Time from (
select a.*, sum(t_duration) over() total from DTX_QUERYLOG_DBT a where t_sessdetail=(select max(t_sessdetail) from DTX_QUERYLOG_DBT ))  
order by 1, t_set, t_num;

create view v_err as
select T_TIMESTAMP, T_OBJECTTYPE, T_OBJECTID, T_QUERYID, IS_LOGGED, T_ERRORCODE, T_DESC 
from dtx_error_dbt left join dtx_errorkinds_dbt on (T_ERRORCODE=t_code)
where t_detailid=(select max(t_detailid) from dtx_error_dbt) order by 1,3;