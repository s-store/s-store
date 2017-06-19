CREATE TABLE T_TINYINT(
ID TINYINT, 
VALUE VARCHAR(255) DEFAULT NULL,
PRIMARY KEY (ID)
); 

CREATE TABLE T_SMALLINT(
ID SMALLINT, 
VALUE VARCHAR(255) DEFAULT NULL,
PRIMARY KEY (ID)
);


CREATE TABLE T_INTEGER(
ID INTEGER, 
VALUE VARCHAR(255) DEFAULT NULL,
PRIMARY KEY (ID)
); 

CREATE TABLE T_BIGINT(
ID BIGINT, 
VALUE VARCHAR(255) DEFAULT NULL,
PRIMARY KEY (ID)
); 

CREATE TABLE T_FLOAT(
ID FLOAT, 
VALUE VARCHAR(255) DEFAULT NULL,
PRIMARY KEY (ID)
); 

CREATE TABLE T_DECIMAL(
ID DECIMAL, 
VALUE VARCHAR(255) DEFAULT NULL,
PRIMARY KEY (ID)
); 

CREATE TABLE T_VARCHAR(
ID VARCHAR(100), 
VALUE VARCHAR(255) DEFAULT NULL,
PRIMARY KEY (ID)
); 

CREATE TABLE T_TIMESTAMP(
ID TIMESTAMP, 
VALUE VARCHAR(255) DEFAULT NULL,
PRIMARY KEY (ID)
);

CREATE TABLE contestants( 
 contestant_number integer     NOT NULL, 
contestant_name   varchar(50) NOT NULL, 
CONSTRAINT PK_contestants PRIMARY KEY  (    contestant_number  )
);

CREATE TABLE votes(  
phone_number       bigint     NOT NULL, 
state              varchar(2) NOT NULL, 
contestant_number  integer    NOT NULL,
PRIMARY KEY (phone_number)
);

CREATE TABLE area_code_state( 
 area_code smallint   NOT NULL,
 state     varchar(2) NOT NULL, 
CONSTRAINT PK_area_code_state PRIMARY KEY  (    area_code  )
);


CREATE VIEW v_votes_by_phone_number(
phone_number, num_votes) 
AS   SELECT phone_number, COUNT(*) FROM votes GROUP BY phone_number;

CREATE VIEW v_votes_by_contestant_number_state( 
 contestant_number, state, num_votes) 
AS SELECT contestant_number , state , COUNT(*)  FROM votes GROUP BY contestant_number, state;


CREATE TABLE drop_table( 
contestant_number integer     NOT NULL, 
contestant_name   varchar(50) NOT NULL,
PRIMARY KEY (contestant_number)
);

CREATE TABLE drop_table1( 
contestant_number integer     NOT NULL, 
contestant_name   varchar(50) NOT NULL,
PRIMARY KEY (contestant_number)
);

CREATE TABLE drop_table2(  
contestant_number integer     NOT NULL, 
contestant_name   varchar(50) NOT NULL,
PRIMARY KEY (contestant_number)
);


