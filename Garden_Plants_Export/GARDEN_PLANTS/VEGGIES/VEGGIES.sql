create or replace schema VEGGIES;

create or replace sequence SEQ1 start with 1 increment by 1 order;
create or replace TABLE LU_SOIL_TYPE (
	SOIL_TYPE_ID NUMBER(38,0),
	SOIL_TYPE VARCHAR(15),
	SOIL_DESCRIPTION VARCHAR(75)
);
create or replace event table MY_EVENTS;
create or replace TABLE NAMES (
	ID NUMBER(38,0),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216)
);
create or replace TABLE POTATOES (
	ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	TYPE VARCHAR(100)
);
create or replace TABLE RAW (
	VAR VARIANT
);
create or replace TABLE ROOT_DEPTH (
	ROOT_DEPTH_ID NUMBER(1,0),
	ROOT_DEPTH_CODE VARCHAR(1),
	ROOT_DEPTH_NAME VARCHAR(7),
	UNIT_OF_MEASURE VARCHAR(2),
	RANGE_MIN NUMBER(2,0),
	RANGE_MAX NUMBER(2,0)
);
create or replace TABLE TABLE1 (
	ID NUMBER(38,0),
	ID2 NUMBER(38,0),
	ID3 NUMBER(38,0),
	ID4 NUMBER(38,0),
	ID5 NUMBER(38,0),
	ID6 NUMBER(38,0),
	ID7 NUMBER(38,0),
	ID8 NUMBER(38,0),
	ID9 NUMBER(38,0),
	ID10 NUMBER(38,0),
	ID11 NUMBER(38,0),
	ID12 NUMBER(38,0),
	ID13 NUMBER(38,0)
);
create or replace TABLE TABLE2 (
	ID NUMBER(38,0),
	ID2 NUMBER(38,0),
	ID3 NUMBER(38,0),
	ID4 NUMBER(38,0),
	ID5 NUMBER(38,0),
	ID6 NUMBER(38,0),
	ID7 NUMBER(38,0),
	ID8 NUMBER(38,0),
	ID9 NUMBER(38,0),
	ID10 NUMBER(38,0),
	ID11 NUMBER(38,0),
	ID12 NUMBER(38,0),
	ID13 NUMBER(38,0)
);
create or replace TABLE TABLE3 (
	ID NUMBER(38,0),
	ID2 NUMBER(38,0),
	ID3 NUMBER(38,0),
	ID4 NUMBER(38,0),
	ID5 NUMBER(38,0),
	ID6 NUMBER(38,0),
	ID7 NUMBER(38,0),
	ID8 NUMBER(38,0),
	ID9 NUMBER(38,0),
	ID10 NUMBER(38,0),
	ID11 NUMBER(38,0),
	ID12 NUMBER(38,0),
	ID13 NUMBER(38,0)
);
create or replace TABLE TABLE4 (
	ID NUMBER(38,0),
	ID2 NUMBER(38,0),
	ID3 NUMBER(38,0),
	ID4 NUMBER(38,0),
	ID5 NUMBER(38,0),
	ID6 NUMBER(38,0),
	ID7 NUMBER(38,0),
	ID8 NUMBER(38,0),
	ID9 NUMBER(38,0),
	ID10 NUMBER(38,0),
	ID11 NUMBER(38,0),
	ID12 NUMBER(38,0),
	ID13 NUMBER(38,0)
);
create or replace TABLE TABLE5 (
	ID NUMBER(38,0),
	ID2 NUMBER(38,0),
	ID3 NUMBER(38,0),
	ID4 NUMBER(38,0),
	ID5 NUMBER(38,0),
	ID6 NUMBER(38,0),
	ID7 NUMBER(38,0),
	ID8 NUMBER(38,0),
	ID9 NUMBER(38,0),
	ID10 NUMBER(38,0),
	ID11 NUMBER(38,0),
	ID12 NUMBER(38,0),
	ID13 NUMBER(38,0)
);
create or replace TABLE VEGETABLES (
	ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	NAME VARCHAR(100)
);
create or replace TABLE VEGETABLE_DETAILS (
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);
create or replace TABLE VEGETABLE_DETAILS_PLANT_HEIGHT (
	PLANT_NAME VARCHAR(25),
	UOM VARCHAR(16777216),
	LOW_END_OF_RANGE NUMBER(38,0),
	HIGH_END_OF_RANGE NUMBER(38,0)
);
create or replace TABLE VEGETABLE_DETAILS_SOIL_TYPE (
	PLANT_NAME VARCHAR(25),
	SOIL_TYPE NUMBER(1,0)
);
create or replace dynamic table VISITORS_DT(
	ID,
	FIRST_NAME,
	LAST_NAME
) lag = '1 minute' warehouse = COMPUTE_WH
 as
SELECT var:id::int id, var:fname::string first_name,
var:lname::string last_name FROM raw;
create or replace view POTATOES_V(
	ID,
	TYPE
) as SELECT ID, TYPE FROM VEGGIES.POTATOeS 
	WHERE TYPE LIKE '%red%';
CREATE OR REPLACE FILE FORMAT MY_CSV_FORMAT
	FIELD_DELIMITER = '|'
	SKIP_HEADER = 1
	NULL_IF = ('NULL', 'null')
	COMPRESSION = gzip
;
CREATE OR REPLACE PROCEDURE "SIMPLE_EXAMPLE"()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS 'select''Hello, this is a simple stored procedure in Snowflake.'';';
create or replace stream DATA_CHECK on table ROOT_DEPTH;
create or replace stream RAWSTREAM1 on table "GARDEN_PLANTS.VEGGIES.RAW";
create or replace task RAW_TO_NAMES
	warehouse=COMPUTE_WH
	schedule='1 minute'
	when SYSTEM$STREAM_HAS_DATA('rawstream1')
	as MERGE INTO names n
USING (
SELECT var:id id, var:fname fname,
var:lname lname FROM rawstream1
) r1 ON n.id = TO_NUMBER(r1.id)
WHEN MATCHED AND metadata$action = 'DELETE' THEN
DELETE
WHEN MATCHED AND metadata$action = 'INSERT' THEN
UPDATE SET n.first_name = r1.fname, n.last_name = r1.lname
WHEN NOT MATCHED AND metadata$action = 'INSERT' THEN
INSERT (id, first_name, last_name)
VALUES (r1.id, r1.fname, r1.lname);
create or replace alert MYALERT
	warehouse=COMPUTE_WH
	schedule='1 minute'
	if (exists(
		SELECT gauge_value FROM gauge WHERE gauge_value>200
	))
	then
	INSERT INTO gauge_value_exceeded_history VALUES (current_timestamp());