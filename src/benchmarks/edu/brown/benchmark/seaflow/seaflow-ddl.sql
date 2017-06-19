
-- Table to hold ARGO temp and salinity data
CREATE TABLE argo_tbl
(
  a_lat       float     NOT NULL
, a_lon       float     NOT NULL
, a_month     int       NOT NULL
, a_depth     int       NOT NULL
, a_temp      float
, a_salinity  float
, a_oxysat    float
, CONSTRAINT PK_latlong PRIMARY KEY
  (
    a_lat, a_lon, a_month, a_depth
  )
);

CREATE INDEX IX_argo on argo_tbl(a_month, a_depth, a_lat, a_lon);

CREATE TABLE sflfull_tbl (
    s_id                bigint NOT NULL,
    s_cruise            VARCHAR(100) NOT NULL,
    s_date              VARCHAR(100) NOT NULL,
    s_lat               float,
    s_lon               float,
    s_salinity          float,
    s_ocean_tmp         float,
    s_par               float,
    s_prochloro_conc    float,
    s_synecho_conc      float,
    s_picoeuk_conc      float,
    s_beads_conc        float,
    s_prochloro_size    float,
    s_synecho_size      float,
    s_picoeuk_size      float,
    s_beads_size        float,
    s_epoch_ms          bigint
);

CREATE INDEX sflfull_cruise_idx ON sflfull_tbl (s_id,s_cruise,s_date);
CREATE INDEX sflfull_date_idx ON sflfull_tbl (s_date);
CREATE INDEX sflfull_epoch_ms_idx ON sflfull_tbl (s_epoch_ms);

CREATE TABLE sflavg_tbl (
    s_id                bigint NOT NULL,
    s_cruise            VARCHAR(100) NOT NULL,
    s_date              VARCHAR(100) NOT NULL,
    s_lat               float,
    s_lon               float,
    s_salinity          float,
    s_ocean_tmp         float,
    s_par               float,
    s_prochloro_conc    float,
    s_synecho_conc      float,
    s_picoeuk_conc      float,
    s_beads_conc        float,
    s_prochloro_size    float,
    s_synecho_size      float,
    s_picoeuk_size      float,
    s_beads_size        float,
    s_epoch_ms          bigint
);

CREATE TABLE cur_location_tbl (
      c_id        int       NOT NULL
    , c_lat       float     NOT NULL
    , c_lon       float     NOT NULL
    , c_month     int       NOT NULL
);

CREATE TABLE steering_tbl (
    st_id                           int       NOT NULL,

    st_rotation                     float     NOT NULL,
    st_neg_temp_rotation            float,
    st_sal_rotation                 float,
    st_neg_sal_rotation             float,

    st_pos_temp_pos_sal_rotation    float,
    st_pos_temp_neg_sal_rotation    float,
    st_neg_temp_pos_sal_rotation    float,
    st_neg_temp_neg_sal_rotation    float
);

CREATE STREAM sflfull_str (
    s_id                bigint NOT NULL,
    s_cruise            VARCHAR(100) NOT NULL,
    s_date              VARCHAR(100) NOT NULL,
    s_lat               float,
    s_lon               float,
    s_salinity          float,
    s_ocean_tmp         float,
    s_par               float,
    s_prochloro_conc    float,
    s_synecho_conc      float,
    s_picoeuk_conc      float,
    s_beads_conc        float,
    s_prochloro_size    float,
    s_synecho_size      float,
    s_picoeuk_size      float,
    s_beads_size        float,
    s_epoch_ms          bigint,
    ts                  bigint,
    WSTART              integer,
    WEND                integer
);

CREATE WINDOW sflfull_win ON sflfull_str ROWS 10 SLIDE 10;
CREATE WINDOW sflfullhour_win ON sflfull_str ROWS 3600 SLIDE 3600;

CREATE TABLE sfltojson_tbl (
    datatype            VARCHAR(10) NOT NULL,
    lat               float,
    lon               float,
    salinity          float,
    temp              float,
    par               float,
    epoch_ms          bigint
);

CREATE INDEX IX_sfltojson ON sfltojson_tbl(epoch_ms);

CREATE TABLE bactojson_tbl (
    datatype                VARCHAR(10) NOT NULL,
    fsc_small               float,
    abundance               float,
    pop                     VARCHAR(20) NOT NULL,
    epoch_ms          bigint
);

CREATE INDEX IX_bactojson ON bactojson_tbl(epoch_ms);


