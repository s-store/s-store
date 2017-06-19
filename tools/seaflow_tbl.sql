DROP TABLE IF EXISTS rv_tbl;
CREATE TABLE IF NOT EXISTS rv_tbl (
    r_id                bigint UNIQUE NOT NULL,
    r_cruise            VARCHAR(100),
    r_date              VARCHAR(100),
    r_lat               decimal,
    r_lon               decimal,
    r_salinity          decimal,
    r_ocean_tmp         decimal,
    r_par               decimal,
    r_epoch_ms          bigint);
    
CREATE INDEX rv_cruise_idx ON rv_tbl (r_cruise);
CREATE INDEX rv_date_idx ON rv_tbl (r_date);
CREATE INDEX rv_epoch_ms_idx ON rv_tbl (r_epoch_ms);

DROP TABLE IF EXISTS bac_tbl;
CREATE TABLE IF NOT EXISTS bac_tbl (
    b_id                bigint UNIQUE NOT NULL,
    b_cruise            VARCHAR(100),
    b_date              VARCHAR(100),
    b_prochloro_conc    decimal,
    b_synecho_conc      decimal,
    b_picoeuk_conc      decimal,
    b_beads_conc        decimal,
    b_prochloro_size    decimal,
    b_synecho_size      decimal,
    b_picoeuk_size      decimal,
    b_beads_size        decimal);

DROP TABLE IF EXISTS sfl_tbl;
CREATE TABLE IF NOT EXISTS sfl_tbl (
    s_id                bigint UNIQUE NOT NULL,
    s_cruise            VARCHAR(100),
    s_date              VARCHAR(100),
    s_lat               decimal,
    s_lon               decimal,
    s_salinity          decimal,
    s_ocean_tmp         decimal,
    s_par               decimal,
    s_epoch_ms          bigint,
    s_prochloro_conc    decimal,
    s_synecho_conc      decimal,
    s_picoeuk_conc      decimal,
    s_beads_conc        decimal,
    s_prochloro_size    decimal,
    s_synecho_size      decimal,
    s_picoeuk_size      decimal,
    s_beads_size        decimal);

CREATE INDEX s_cruise_idx ON sfl_tbl (s_cruise);
CREATE INDEX s_date_idx ON sfl_tbl (s_date);
CREATE INDEX s_epoch_ms_idx ON sfl_tbl (s_epoch_ms);

