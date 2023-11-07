CREATE OR REPLACE PROCEDURE VEGGIES.GreetWithName(NAME STRING)
    RETURNS STRING
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
AS
$$
try {
    var message = "Happy Day";

    if (NAME) {
        message = "Happy Day, " + NAME + "!";
    }

    return message;
} catch (err) {
    return "Error: " + err;
}
$$;


CREATE OR REPLACE DYNAMIC TABLE ROOT_DEPTH_DT
TARGET_LAG = '5 minutes'
WAREHOUSE = 'COMPUTE_WH'
AS
SELECT ROOT_DEPTH_ID, ROOT_DEPTH_CODE, ROOT_DEPTH_NAME FROM VEGGIES.ROOT_DEPTH;
