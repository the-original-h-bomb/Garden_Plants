create or replace dynamic table VISITORS_DT(
	ID,
	FIRST_NAME,
	LAST_NAME
) lag = '5 minutes' warehouse = COMPUTE_WH
 as
SELECT var:id::int id, var:fname::string first_name,
var:lname::string last_name FROM raw;