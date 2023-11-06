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