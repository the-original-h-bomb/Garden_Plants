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