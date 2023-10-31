create or replace database GARDEN_PLANTS;

create or replace schema PUBLIC;

create or replace schema SCHEMACHANGE;

create or replace TABLE CHANGE_HISTORY (
	VERSION VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	SCRIPT VARCHAR(16777216),
	SCRIPT_TYPE VARCHAR(16777216),
	CHECKSUM VARCHAR(16777216),
	EXECUTION_TIME NUMBER(38,0),
	STATUS VARCHAR(16777216),
	INSTALLED_BY VARCHAR(16777216),
	INSTALLED_ON TIMESTAMP_LTZ(9)
);
create or replace schema VEGGIES;

create or replace TABLE LU_SOIL_TYPE (
	SOIL_TYPE_ID NUMBER(38,0),
	SOIL_TYPE VARCHAR(15),
	SOIL_DESCRIPTION VARCHAR(75)
);
create or replace TABLE POTATOES (
	ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	TYPE VARCHAR(100)
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
create or replace view POTATOES_V(
	ID,
	TYPE
) as SELECT ID, TYPE FROM VEGGIES.POTATOeS 
	WHERE TYPE LIKE '%red%';