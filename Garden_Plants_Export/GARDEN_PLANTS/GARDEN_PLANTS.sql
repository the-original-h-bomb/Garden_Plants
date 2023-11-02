create or replace database GARDEN_PLANTS;

create or replace schema VEGGIES;

create or replace tag COST_CENTER COMMENT='cost_center tag'
;
create or replace TABLE LU_SOIL_TYPE (
	SOIL_TYPE_ID NUMBER(38,0),
	SOIL_TYPE VARCHAR(15),
	SOIL_DESCRIPTION VARCHAR(75)
);
create or replace event table MY_EVENTS;
CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	NULL_IF = ('NULL', 'null')
	COMPRESSION = gzip
;