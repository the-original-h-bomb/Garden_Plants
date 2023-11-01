import snowflake.connector
import subprocess
import os
import csv

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

# Specify the folder path within your repository
folder_path = 'Garden_Plants_Export'

# Snowflake cursor
cursor = conn.cursor()
use_db_query = 'Use database garden_plants;'
cursor.execute(use_db_query)

### security table dump ####

gtr_export_path = os.path.join(folder_path, "GRANTS_TO_ROLES.csv")
gtr_query = f"SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES;"
cursor.execute(gtr_query)
gtr_file = cursor.fetchall()

with open(gtr_export_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([x[0] for x in cursor.description])  # write header
    for row in gtr_file:
        writer.writerow(row)

# Commit to GitHub
    subprocess.call(['git', 'add', gtr_export_path])
    subprocess.call(['git', 'commit', '-m', f'GRANTS_TO_ROLES.csv'])

# Export databases and artifacts - delivered databases contain some items that cannot be exported out
db_query = f"select * from information_schema.databases where database_NAME not like 'SNOWFLAKE%' AND TYPE = 'STANDARD';"

# Execute the query to fetch all databases
cursor.execute(db_query)

# Fetch all the databases
databases = cursor.fetchall()

for db in databases:
    db_name = db[0]
    os.makedirs(os.path.join(folder_path, db_name), exist_ok=True)
    db_export_path = os.path.join(folder_path, db_name)
    db_ddl_export_path = os.path.join(db_export_path, db_name + ".sql")
    db_export_query = f"SELECT GET_DDL('DATABASE','{db_name}')"
    cursor.execute(db_export_query)
    db_create_statement = cursor.fetchone()[0]

    with open(db_ddl_export_path, 'w') as db_file:
        db_file.write(db_create_statement)

    # Commit to GitHub
    subprocess.call(['git', 'add', db_ddl_export_path])
    subprocess.call(['git', 'commit', '-m', f'Commit {db_name} DDL'])

    # Export schemas
    schema_query = f"select * from  {db_name}.information_schema.schemata " \
                   f"where schema_name not in ('SCHEMACHANGE', 'INFORMATION_SCHEMA')"
    cursor.execute(schema_query)
    schemas = cursor.fetchall()

    for schema in schemas:
        schema_name = schema[1]
        schema_export_path = os.path.join(db_export_path, schema_name)
        os.makedirs(schema_export_path, exist_ok=True)
        schema_ddl_export_path = os.path.join(schema_export_path, schema_name + ".sql")
        schema_export_query = f"SELECT GET_DDL('SCHEMA','{db_name}.{schema_name}')"
        cursor.execute(schema_export_query)
        schema_create_statement = cursor.fetchone()[0]

        with open(schema_ddl_export_path, 'w') as schema_file:
            schema_file.write(schema_create_statement)
    
    # Commit Schemas to GitHub
    subprocess.call(['git', 'add', schema_ddl_export_path])
    subprocess.call(['git', 'commit', '-m', f'Commit {schema_name} DDL'])
