-- Скрипт создает таблицы снимков
-- Все таблицы постоянные, постфикс TMP сложился исторически, ничего не значит

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
  TGT_PAYMCUR               NUMBER(15),
  TGT_CURRENCYID            NUMBER(15),
  TGT_AVOIRISSID            NUMBER(15),
  TGT_AVOIRKIND             NUMBER(3),
  TGT_MARKETID              NUMBER(15),
  TGT_SECTOR                NUMBER(15),
  TGT_BROKERID              NUMBER(15),
  TGT_PARENTID              NUMBER(15),
  TGT_PARTIALID             NUMBER(15),
  TGT_PORTFOLIOID           NUMBER(5),
  TGT_REPOBASE              NUMBER(5),
  TGT_WARRANT_NUM           NUMBER(5),
  TGT_PARTIAL_NUM           NUMBER(5),
  TGT_STATE                 NUMBER(5),
  TGT_DEPARTMENT            NUMBER(5),
  TGT_BOFFICEKIND           NUMBER(3),
  TGT_WARRANTID             NUMBER(15),
  TGT_DEALKIND              NUMBER(5),
  TGT_ISLOAN                CHAR(1 CHAR),
  TGT_ISREPO                CHAR(1 CHAR),
  TGT_ISBASKET              CHAR(1 CHAR),
  TGT_ISLOANTOREPO          CHAR(1 CHAR),
  TGT_COUNTRY               VARCHAR2(3 CHAR),
  TGT_PARTYID               NUMBER(15),
  TGT_NKDFIID               NUMBER(15),
  TGT_OBJTYPE               NUMBER(3),
  TGT_PRICE                 NUMBER(32,12),
  TGT_MATURITY              DATE,
  TGT_EXPIRY                DATE,
  TGT_MATURITYISPRINCIPAL   CHAR(1 CHAR),
  TGT_MATURITY2             DATE,
  TGT_EXPIRY2               DATE,
  TGT_MATURITYISPRINCIPAL2  CHAR(1 CHAR),
  TGT_FORMULA               NUMBER(3),
  TGT_PORTFOLIOID_2         NUMBER(2),
  TGT_CURNOM                NUMBER,
  TGT_RELATIVEPRICE         CHAR(1 CHAR),
  TGT_RECEIPTAMOUNT         NUMBER,
  TGT_ISBOND                CHAR(1 CHAR),
  TGT_ISQUOTED              CHAR(1 CHAR),
  TGT_ISKSU                 CHAR(1 CHAR),
  TGT_BS_ISFICTIVE          CHAR(1 CHAR),
  TGT_BS_DIRECTION          NUMBER(1),
  TGT_BS_AMOUNT             NUMBER,
  TGT_BS_AMOUNT_AFTER       NUMBER,
  TGT_BS_RETDATE            DATE,
  TGT_BS_RQTYPE             NUMBER(1),
  TGT_BS_BEGDATE            DATE,
  TGT_BS_PAYER              NUMBER,
  TGT_BS_DEALKIND           NUMBER
)
LOGGING 
MONITORING;

COMMENT ON COLUMN DTXDEAL_TMP.TGT_PARTYID IS 'Перекодированный T_PARTYID из DPARTY_DBT';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_NKDFIID IS 'Перекодированный T_NKDFIID, берется из DFININSTR_DBT';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_PRICE IS 'Цена, дял погашений переопределяется, для остальных операций берется из T_PRICE';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_MATURITY IS 'Дата первого платежа';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_EXPIRY IS 'Дата второго платежа';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_MATURITYISPRINCIPAL IS 'Признак того, что первый платеж по бумагам, второй - по деньгам';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_MATURITY2 IS 'Дата первого платежа по второй части РЕПО';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_EXPIRY2 IS 'Дата второго платежа по второй части РЕПО';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_MATURITYISPRINCIPAL2 IS 'Первый платеж - по бумаге (для второй части РЕПО)';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_FORMULA IS 'Оплата против поставки и т.д.';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_PORTFOLIOID_2 IS 'Портфель для второй части по сделке РЕПО';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_CURNOM IS 'Текущий номинал бумаги, определяет цену для погашений';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_ISBOND IS 'Бумага является облигацией';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_ISQUOTED IS 'Признак котируемой бумаги';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_BS_ISFICTIVE IS 'Фиктивная операция - отражает движение обеспечения в сделке с корзиной';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_BS_DIRECTION IS 'Для движений по корзине:  направлеение движения обеспечения (0-ввод обеспечения, 1-вывод)';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_BS_AMOUNT IS 'Для движений по корзине:  количество бумаг в обеспечении до этой операции';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_BS_AMOUNT_AFTER IS 'Для движений по корзине: количество бумаг в обеспечении после обработки сделки';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_BS_RETDATE IS 'Для движений по корзине: дата окончательного возврата бумаг';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_BS_RQTYPE IS 'Для движений по корзине: тип платежа в ddlrq_dbt';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_BS_BEGDATE IS 'Для движений по корзине: дата начальной поставки бумаг';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_BS_PAYER IS 'Для движений по корзине: плательщик по плитежу в ddlrq_dbt';

COMMENT ON COLUMN DTXDEAL_TMP.TGT_BS_DEALKIND IS 'Для движений по корзине: вид базовой сделки';







CREATE TABLE DTXCOURSE_TMP
(
  T_COURSEID          NUMBER(15)                NOT NULL,
  T_TYPE              NUMBER(5)                 NOT NULL,
  T_INSTANCEDATE      DATE                      NOT NULL,
  T_ACTION            NUMBER(5),
  T_REPLSTATE         NUMBER(5),
  T_BASEFIKIND        NUMBER(5),
  T_BASEFIID          NUMBER(15),
  T_FIID              NUMBER(15),
  T_MARKETID          NUMBER(15),
  T_MARKETSECTORID    NUMBER(15),
  T_POINT             NUMBER(5),
  T_SCALE             NUMBER(10),
  T_RATEDATE          DATE,
  T_RATE              NUMBER,
  TGT_RATEID          NUMBER(15),
  TGT_TYPE            NUMBER(5),
  TGT_MARKETID        NUMBER(15),
  TGT_SECTORID        NUMBER(15),
  TGT_BASEFIID        NUMBER(15),
  TGT_FIID            NUMBER(15),
  TGT_ISLASTDATE      CHAR(1 CHAR),
  TGT_ISNOMINAL       CHAR(1 CHAR),
  TGT_ISDOMINANT      CHAR(1 CHAR),
  TGT_ISRELATIVE      CHAR(1 CHAR),
  TGT_FACEVALUE_FIID  NUMBER(15),
  TGT_BASEFIKIND      NUMBER,
  TGT_LAST_DATE_RSS   DATE,
  TGT_LAST_DATE_DTX   DATE,
  TGT_NEWRATEID       NUMBER(15),
  TGT_RATE            NUMBER
)
LOGGING 
MONITORING;

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_RATEID IS 'dratedef_dbt.t_rateid';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_TYPE IS 'Тип курса из Rs-Bank';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_MARKETID IS 'Код биржи из DPARTY_DBT';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_SECTORID IS 'Код сектора биржи';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_BASEFIID IS 'Код базового инструмента FI из DFININSTR_DBT';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_FIID IS 'Код котируемого инструмента FI из DFININSTR_DBT';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_ISLASTDATE IS 'Заполняется X, если дата в этой записи превышает остальные и превышает дату в DRATEDEF_DBT. То есть, если это значение должно попасть в DRATEDEF_DBT';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_ISNOMINAL IS 'Является номиналом для бумаги с индексируемым ном.';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_ISRELATIVE IS 'Курс задан в процентах от номинала';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_FACEVALUE_FIID IS 'Валюта номинала, если это курс для бумаги (T_BASEFIKIND=20)';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_BASEFIKIND IS 'Вид базового иинсструмента, если это бумага';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_LAST_DATE_RSS IS 'Последняя дата курса, заданная в dratedef.';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_LAST_DATE_DTX IS 'Максимальная дата по курсу среди находящихся в DTXCOUSE';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_NEWRATEID IS 'Поле для сгенерированного ID для DRATEDEF_DBT, если такого курса еще не было вообще';

COMMENT ON COLUMN DTXCOURSE_TMP.TGT_RATE IS 'Значение T_RATE, отмасштабированное по t_point';
                                                                                             









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
  TGT_KIND         NUMBER(1),
  TGT_PLANDATE     DATE,
  TGT_FACTDATE     DATE,
  TGT_CHANGEDATE   DATE,
  TGT_TYPE         NUMBER(2),
  TGT_SUBKIND      NUMBER(1),
  TGT_STATE        NUMBER(1)
)
LOGGING 
MONITORING;





CREATE TABLE DTXCOMISS_TMP
(
  T_COMISSID      NUMBER(15)                    NOT NULL,
  T_INSTANCEDATE  DATE                          NOT NULL,
  T_ACTION        NUMBER(5),
  T_REPLSTATE     NUMBER(5),
  T_DEALID        NUMBER(15)                    NOT NULL,
  T_TYPE          NUMBER(5),
  T_SUM           NUMBER(32,12),
  T_NDS           NUMBER(32,12),
  T_CURRENCYID    NUMBER(15),
  T_DATE          DATE,
  TGT_COMISSID    NUMBER(15),
  TGT_COMMNUMBER  NUMBER(5),
  TGT_DEALID      NUMBER(15),
  TGT_NDS         NUMBER,
  TGT_CONTRACTID  NUMBER,
  TGT_CURRENCYID  NUMBER
)
LOGGING 
MONITORING;