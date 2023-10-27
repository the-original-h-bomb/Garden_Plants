import os
import snowflake.connector

# Retrieve secrets from GitHub Actions environment variables
account = os.environ.get("SF_PRD_ACCOUNT")
user = os.environ.get("SF_PRD_USERNAME")
password = os.environ.get("SF_PRD_PASSWORD")
warehouse = os.environ['SF_PRD_WAREHOUSE']
database = os.environ['SF_PRD_DATABASE']

# Snowflake connection
conn = snowflake.connector.connect(
    account=account,
    user=user,
    password=password,
    warehouse=warehouse,
    database=database
)

# Query to retrieve a list of databases
get_databases_query = "SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME NOT IN ('SNOWFLAKE', 'INFORMATION_SCHEMA', 'SNOWFLAKE_SAMPLE_DATA');"

with conn.cursor() as cursor:
    cursor.execute(get_databases_query)
    databases = [row[0] for row in cursor]

# Iterate over databases
for database in databases:
    # Create a directory for the database
    os.makedirs(database, exist_ok=True)
    
    print({database})
    # Switch to the database
    conn.cursor().execute(f"USE DATABASE {database}")

    # Define the DDL export query
    ddl_query = "SHOW TABLES;"
    
    # Execute the query and export DDL
    with conn.cursor() as cursor:
        cursor.execute(ddl_query)
        ddl_statements = [row[2] for row in cursor]

    with open(os.path.join(database, "snowflake_ddl.sql"), "w") as ddl_file:
        for statement in ddl_statements:
            ddl_file.write(f"{statement}\n")

conn.close()
