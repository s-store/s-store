---------------------
--TABLES--
---------------------

CREATE TABLE DimTrade
(
    TradeID          bigint      NOT NULL,
    SK_BrokerID      bigint      ,
    SK_CreateDateID  bigint      NOT NULL,
    SK_CreateTimeID  bigint      NOT NULL,
    SK_CloseDateID   bigint      ,
    SK_CloseTimeID   bigint      ,
    Status           char(10)    NOT NULL,
    Type             char(12)    NOT NULL,
    CashFlag         smallint    NOT NULL,
    SK_SecurityID    bigint      NOT NULL,
    SK_CompanyID     bigint      NOT NULL,
    Quantity         int         NOT NULL,
    BidPrice         float     NOT NULL,
    SK_CustomerID    bigint      NOT NULL,
    SK_AccountID     bigint      NOT NULL,
    ExecutedBy       varchar(64) NOT NULL,
    TradePrice       float     ,
    Fee              float     ,
    Commission       float     ,
    Tax              float     ,
    BatchID          bigint      ,
    part_id          int         NOT NULL,
 CONSTRAINT PK_DimTrade PRIMARY KEY
  (
    TradeID
  )
);

CREATE TABLE TradeType
(
    TT_ID            varchar(3)   NOT NULL,
    TT_NAME          varchar(12)  NOT NULL,
    TT_IS_SELL       smallint  NOT NULL,
    TT_IS_MRKT       smallint  NOT NULL,
    part_id          int       NOT NULL,
    CONSTRAINT PK_TradeType PRIMARY KEY (TT_ID)
);

CREATE TABLE StatusType
(
    ST_ID            varchar(4)   NOT NULL,
    ST_NAME          varchar(10)  NOT NULL,
    part_id          int       NOT NULL,
    CONSTRAINT PK_StatusType PRIMARY KEY (ST_ID)
);

CREATE TABLE DimSecurity
(
    SK_SecurityID    bigint    NOT NULL,
    Symbol           char(15)  NOT NULL,
    SK_CompanyID     bigint    NOT NULL,
    part_id          int       NOT NULL,
    CONSTRAINT PK_DimSecurity PRIMARY KEY (SK_SecurityID)
);
CREATE INDEX IX_Security on DimSecurity(Symbol);

CREATE TABLE DimCustomer
(
    SK_CustomerID    bigint    NOT NULL,
    CustomerID       bigint    NOT NULL,
    part_id          int       NOT NULL,
    CONSTRAINT PK_DimCustomer PRIMARY KEY (SK_CustomerID)
);

CREATE TABLE DimCompany
(
    SK_CompanyID    bigint    NOT NULL,
    CompanyID       bigint    NOT NULL,
    part_id          int       NOT NULL,
    CONSTRAINT PK_DimCompany PRIMARY KEY (SK_CompanyID)
);

CREATE TABLE DimAccount
(
    SK_AccountID    bigint    NOT NULL,
    AccountID       bigint    NOT NULL,
    SK_BrokerID     bigint    NOT NULL,
    SK_CustomerID   bigint    NOT NULL,
    part_id          int       NOT NULL,
    CONSTRAINT PK_DimAccount PRIMARY KEY (AccountID)
);

CREATE TABLE DimBroker
(
    SK_BrokerID    bigint    NOT NULL,
    BrokerID       bigint    NOT NULL,
    part_id          int       NOT NULL,
    CONSTRAINT PK_DimBroker PRIMARY KEY (SK_BrokerID)
);

CREATE TABLE DiMessages
(
    --SK_MessageID           bigint    NOT NULL,
    --MessageDateAndTime     timestamp NOT NULL,
    BatchID                bigint    NOT NULL,
    MessageSource          varchar(30),
    MessageText            varchar(50)  NOT NULL,
    MessageType            varchar(12)  NOT NULL,
    MessageData            varchar(100),
    part_id                int       NOT NULL,
    --CONSTRAINT PK_MessageID PRIMARY KEY (SK_MessageID)
);

CREATE TABLE DimDate
(
    SK_DateID              bigint    NOT NULL,
    DateValue		   varchar(10) NOT NULL,
    CalendarYearID         smallint  NOT NULL,
    CalendarMonthID        integer   NOT NULL,
    CalendarWeekID         integer   NOT NULL,
    DayOfWeekNum           smallint  NOT NULL,
    FiscalYearID           smallint  NOT NULL,
    FiscalQuarterID        smallint  NOT NULL,
    part_id                int       NOT NULL,
    CONSTRAINT PK_DimDate PRIMARY KEY (SK_DateID)
);
CREATE INDEX IX_DimDate on DimDate(DateValue);

CREATE TABLE DimTime
(
    SK_TimeID              bigint    NOT NULL,
    HourID                 smallint  NOT NULL,
    MinuteID               smallint  NOT NULL,
    SecondID               smallint  NOT NULL,
    part_id                int       NOT NULL,
    CONSTRAINT PK_TimeID PRIMARY KEY (SK_TimeID)
);

CREATE INDEX IX_DimTime on DimTime(HourID,MinuteID,SecondID);

--CREATE TABLE state_tbl (
--   row_id       integer NOT NULL,
--   batch_id    bigint  NOT NULL,
--   part_id      bigint  NOT NULL,
--   ts_delta_client_sp1     bigint NOT NULL,
--   ts_delta_sp1_insert     bigint NOT NULL,
--   ts_delta_sp1_sp2     bigint NOT NULL,
--   ts_delta_sp2_insert     bigint NOT NULL,
--   ts_delta_total     bigint NOT NULL,
--   total_tuples bigint NOT NULL,
--   total_batches bigint NOT NULL,
-- CONSTRAINT PK_state PRIMARY KEY
--  (
--    part_id, row_id
--  )
--);

----------------------------------
--STREAMS--
----------------------------------

CREATE STREAM SP1out
(
    T_ID             bigint     NOT NULL,
    T_DTS            varchar(30)   NOT NULL,
    T_ST_ID          varchar(4)    NOT NULL,
    T_TT_ID          varchar(3)    NOT NULL,
    T_IS_CASH        smallint        NOT NULL,
    T_S_SYMB         varchar(15)   NOT NULL,
    T_QTY            int        NOT NULL,
    T_BID_PRICE      float    NOT NULL,
    T_CA_ID          int     NOT NULL,
    T_EXEC_NAME      varchar(49) NOT NULL,
    T_TRADE_PRICE    float    ,
    T_CHRG           float    ,
    T_COMM           float    ,
    T_TAX            float    ,
    batch_id         bigint     NOT NULL,
    part_id          int        NOT NULL
);

CREATE INDEX IX_SP1out on SP1out(batch_id);

CREATE STREAM SP2out
(
    T_ID             bigint     NOT NULL,
    SK_CreateDateID  bigint      NOT NULL,
    SK_CreateTimeID  bigint      NOT NULL,
    SK_CloseDateID   bigint      ,
    SK_CloseTimeID   bigint      ,
    Status           varchar(10)    NOT NULL,
    Type             varchar(12)    NOT NULL,
    T_IS_CASH        smallint        NOT NULL,
    T_S_SYMB         varchar(15)   NOT NULL,
    T_QTY            int        NOT NULL,
    T_BID_PRICE      float    NOT NULL,
    T_CA_ID          int     NOT NULL,
    T_EXEC_NAME      varchar(49) NOT NULL,
    T_TRADE_PRICE    float    ,
    T_CHRG           float    ,
    T_COMM           float    ,
    T_TAX            float    ,
    batch_id         bigint     NOT NULL,
    part_id          int        NOT NULL
);

CREATE INDEX IX_SP2out on SP2out(batch_id);

CREATE STREAM SP3out
(
    T_ID             bigint     NOT NULL,
    SK_CreateDateID  bigint      NOT NULL,
    SK_CreateTimeID  bigint      NOT NULL,
    SK_CloseDateID   bigint      ,
    SK_CloseTimeID   bigint      ,
    Status           varchar(10)    NOT NULL,
    Type             varchar(12)    NOT NULL,
    T_IS_CASH        smallint        NOT NULL,
    SK_SecurityID    bigint    NOT NULL,
    SK_CompanyID     bigint    NOT NULL,
    T_QTY            int        NOT NULL,
    T_BID_PRICE      float    NOT NULL,
    T_CA_ID          int     NOT NULL,
    T_EXEC_NAME      varchar(49) NOT NULL,
    T_TRADE_PRICE    float    ,
    T_CHRG           float    ,
    T_COMM           float    ,
    T_TAX            float    ,
    batch_id         bigint     NOT NULL,
    part_id          int        NOT NULL
);

CREATE INDEX IX_SP3out on SP3out(batch_id);

CREATE STREAM SP4out
(
    T_ID             bigint     NOT NULL,
    SK_CreateDateID  bigint      NOT NULL,
    SK_CreateTimeID  bigint      NOT NULL,
    SK_CloseDateID   bigint      ,
    SK_CloseTimeID   bigint      ,
    Status           varchar(10)    NOT NULL,
    Type             varchar(12)    NOT NULL,
    T_IS_CASH        smallint        NOT NULL,
    SK_SecurityID    bigint    NOT NULL,
    SK_CompanyID     bigint    NOT NULL,
    T_QTY            int        NOT NULL,
    T_BID_PRICE      float    NOT NULL,
    SK_AccountID     bigint   NOT NULL,
    SK_CustomerID    bigint   NOT NULL,
    SK_BrokerID      bigint   NOT NULL,
    T_EXEC_NAME      varchar(49) NOT NULL,
    T_TRADE_PRICE    float    ,
    T_CHRG           float    ,
    T_COMM           float    ,
    T_TAX            float    ,
    batch_id         bigint     NOT NULL,
    part_id          int        NOT NULL
);

CREATE INDEX IX_SP4out on SP4out(batch_id);

----------------------------------
--WINDOWS--
----------------------------------






