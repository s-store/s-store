CREATE TABLE cur_position (
	part_id		smallint NOT NULL,
	xway		smallint NOT NULL,
	vid		INT NOT NULL,
	seg		smallint NOT NULL,
	dir		smallint NOT NULL,
	lane		smallint NOT NULL,
	pos		INT NOT NULL,
	speed		smallint NOT NULL,
	ts		INT NOT NULL,
	tod		smallint NOT NULL,
	count_at_pos	smallint NOT NULL,
	toll_to_charge	INT NOT NULL,
	PRIMARY KEY (part_id, xway, vid)
);

CREATE TABLE cur_seg_stats (
	part_id		smallint NOT NULL,
	xway		smallint NOT NULL,
	tod		smallint NOT NULL,
	seg		smallint NOT NULL,
	dir		smallint NOT NULL,
	num_cars	INT NOT NULL,
	total_spd	INT NOT NULL,
	lav4		INT NOT NULL,
	PRIMARY KEY (part_id, xway, tod, seg, dir)
);

CREATE TABLE cur_avg_speeds (
	part_id		smallint NOT NULL,
	xway		smallint NOT NULL,
	seg		smallint NOT NULL,
	dir		smallint NOT NULL,
	row_id		smallint NOT NULL,
	avg_spd		INT NOT NULL,
	PRIMARY KEY (part_id, xway, seg, dir, row_id)
);

CREATE TABLE potential_accidents (
	part_id		smallint NOT NULL,
	xway		smallint NOT NULL,
	dir		smallint NOT NULL,
	seg		smallint NOT NULL,
	pos		INT NOT NULL,
	num_cars	INT NOT NULL,
	started_tod	INT NOT NULL,
	ended_tod	INT NOT NULL,
	vid		INT NOT NULL,
	PRIMARY KEY (part_id, xway, dir, seg, pos)
);
CREATE INDEX IX_accidents on potential_accidents(part_id, xway, ended_tod, started_tod);

CREATE TABLE segment_history (
   part_id smallint NOT NULL,
   absday  smallint NOT NULL,
   tod     smallint NOT NULL,
   XWay    smallint NOT NULL,
   dir     smallint NOT NULL,
   seg     smallint NOT NULL,
   lav     smallint NOT NULL,
   cnt     int      NOT NULL,
   toll    int      NOT NULL,
   dow     smallint NOT NULL,
   PRIMARY KEY (part_id, xway, dir, seg, dow, tod, absday)
);

CREATE TABLE tolls_per_vehicle (
   part_id 	smallint NOT NULL,
   vid    	INT NOT NULL,
   tollday	smallint NOT NULL,
   xway    	smallint NOT NULL,
   tolls   	int NOT NULL,
   PRIMARY KEY (part_id, vid, xway, tollday)
);

CREATE TABLE current_ts (
   part_id 	smallint NOT NULL,
   xway     	smallint NOT NULL,
   tod       	INT   NOT NULL,
   ts        	INT   NOT NULL,
   PRIMARY KEY (part_id, XWay)
);

CREATE TABLE query_audit_tbl (
   part_id smallint  NOT NULL,
   proc_id smallint  NOT NULL,
   proc_name varchar(50),
   query_id smallint NOT NULL, 
   total_query_time bigint NOT NULL,
   total_query_count bigint NOT NULL,
   avg_query_time bigint NOT NULL,
   PRIMARY KEY (part_id, proc_id, query_id)
);

CREATE STREAM start_seg_stats (
	part_id 	smallint NOT NULL,   
	xway    	smallint NOT NULL,
   	tod     	INT      NOT NULL,
   	ts      	INT      NOT NULL,
	seg		smallint NOT NULL,
	dir		smallint NOT NULL,
	add_count	smallint NOT NULL,
	add_speed	smallint NOT NULL
);
--CREATE INDEX IX_start_seg_stats ON start_seg_stats(part_id, xway, tod, ts);

CREATE TABLE prev_input (
	part_id		smallint NOT NULL,
	xway		smallint NOT NULL,
	vid		INT NOT NULL,
	seg		smallint NOT NULL,
	dir		smallint NOT NULL,
	lane		smallint NOT NULL,
	pos		INT NOT NULL,
	spd		smallint NOT NULL,
	ts		INT NOT NULL,
	PRIMARY KEY (part_id, xway)
);
