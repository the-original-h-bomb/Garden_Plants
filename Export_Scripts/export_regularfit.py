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

os.makedirs(os.path.join(folder_path, 'Security_Tables'), exist_ok=True)
gtr_export_path = os.path.join(folder_path, 'Security_Tables', "GRANTS_TO_ROLES.csv")
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

###### Fetch all the databases
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

###### Export schemas
    schema_query = f"select * from  {db_name}.information_schema.schemata " \
                   f"where schema_name not in ('SCHEMACHANGE', 'INFORMATION_SCHEMA')"
    cursor.execute(schema_query)
    schemas = cursor.fetchall()

    for schema in schemas:
        schema_name = schema[1]
        schema_folder_path = os.path.join(db_export_path, schema_name)
        os.makedirs(schema_folder_path, exist_ok=True)
        schema_export_path = os.path.join(schema_folder_path, schema_name + ".sql")
        schema_export_query = f"SELECT GET_DDL('SCHEMA','{db_name}.{schema_name}')"
        cursor.execute(schema_export_query)
        schema_create_statement = cursor.fetchone()[0]

        with open(schema_export_path, 'w') as schema_file:
            schema_file.write(schema_create_statement)
    
###### Export tables
        table_query = f"SHOW TABLES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(table_query)
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[1]
            table_folder_path = os.path.join(schema_folder_path, "TABLES")
            os.makedirs(table_folder_path, exist_ok=True)
            table_export_path = os.path.join(table_folder_path, table_name + ".sql")
            table_export_query = f"SELECT GET_DDL('TABLE','{db_name}.{schema_name}.{table_name}')"
            cursor.execute(table_export_query)
            table_create_statement = cursor.fetchone()[0]

            with open(table_export_path, 'w') as table_file:
                table_file.write(table_create_statement)
       
            # Commit Tables to GitHub
            subprocess.call(['git', 'add', table_export_path])
            subprocess.call(['git', 'commit', '-m', f'Commit {table_name} DDL'])

###### Export views
        view_query = f"SHOW VIEWS IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(view_query)
        views = cursor.fetchall()

        for view in views:
            view_name = view[1]
            view_folder_path = os.path.join(schema_folder_path, "VIEWS")
            view_export_path = os.path.join(view_folder_path, view_name + ".sql")
            view_export_query = f"SELECT GET_DDL('VIEW','{db_name}.{schema_name}.{view_name}')"
            cursor.execute(view_export_query)
            view_create_statement = cursor.fetchone()[0]

            with open(view_export_path, 'w') as view_file:
                view_file.write(view_create_statement)
            
            # Commit Views to GitHub
            subprocess.call(['git', 'add', view_export_path])
            subprocess.call(['git', 'commit', '-m', f'Commit {view_name} DDL'])

##############    
    # Commit Schemas to GitHub
    subprocess.call(['git', 'add', schema_export_path])
    subprocess.call(['git', 'commit', '-m', f'Commit {schema_name} DDL'])
