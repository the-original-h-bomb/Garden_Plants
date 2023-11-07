create or replace dynamic table ROOT_DEPTH_DT(
	ROOT_DEPTH_ID,
	ROOT_DEPTH_CODE,
	ROOT_DEPTH_NAME
) lag = '3 minutes' warehouse = COMPUTE_WH
 as
SELECT ROOT_DEPTH_ID, ROOT_DEPTH_CODE, ROOT_DEPTH_NAME FROM VEGGIES.ROOT_DEPTH;