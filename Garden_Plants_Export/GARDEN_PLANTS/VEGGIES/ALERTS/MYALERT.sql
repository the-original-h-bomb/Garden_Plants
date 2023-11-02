create or replace alert MYALERT
	warehouse=COMPUTE_WH
	schedule='1 minute'
	if (exists(
		SELECT gauge_value FROM gauge WHERE gauge_value>200
	))
	then
	INSERT INTO gauge_value_exceeded_history VALUES (current_timestamp());