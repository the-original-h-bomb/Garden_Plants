CREATE OR REPLACE PROCEDURE "GREETWITHNAME"("NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    var message = "Happy Day";

    if (NAME) {
        message = "Happy Day, " + NAME + "!";
    }

    return message;
} catch (err) {
    return "Error: " + err;
}
';