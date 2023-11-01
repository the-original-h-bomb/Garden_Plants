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

###### Export alerts
        alert_query = f"SHOW ALERTS IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(alert_query)
        alerts = cursor.fetchall()

        for alert in alerts:
            alert_name = alert[1]
            alert_folder_path = s.path.join(schema_folder_path, "ALERTS")
            os.makedirs(alert_folder_path, exist_ok=True)
            alert_export_path = os.path.join(alert_folder_path, alert_name + ".sql")
            alert_export_query = f"SELECT GET_DDL('ALERT','{db_name}.{schema_name}.{alert_name}')"
            cursor.execute(alert_export_query)
            alert_create_statement = cursor.fetchone()[0]

            with open(alert_export_path, 'w') as alert_file:
                alert_file.write(alert_create_statement)    

            # Commit Tables to GitHub
            subprocess.call(['git', 'add', alert_export_path])
            subprocess.call(['git', 'commit', '-m', f'Commit {alert_name} DDL'])

###### Export dynamic_tables
        dynamic_table_query = f"SHOW DYNAMIC TABLES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(dynamic_table_query)
        dynamic_tables = cursor.fetchall()

        for dynamic_table in dynamic_tables:
            dynamic_table_name = dynamic_table[1]
            dynamic_table_folder_path = s.path.join(schema_folder_path, "DYNAMIC_TABLES")
            os.makedirs(dynamic_table_folder_path, exist_ok=True)
            dynamic_table_export_path = os.path.join(dynamic_table_folder_path, dynamic_table_name + ".sql")
            dynamic_table_export_query = f"SELECT GET_DDL('DYNAMIC_TABLE','{db_name}.{schema_name}.{dynamic_table_name}')"
            cursor.execute(dynamic_table_export_query)
            dynamic_table_create_statement = cursor.fetchone()[0]

            with open(dynamic_table_export_path, 'w') as dynamic_table_file:
                dynamic_table_file.write(dynamic_table_create_statement)
            
            # Commit Tables to GitHub
            subprocess.call(['git', 'add', dynamic_table_export_path])
            subprocess.call(['git', 'commit', '-m', f'Commit {dynamic_table_name} DDL'])
            
###### Export Event Tables
        event_table_query = f"SHOW EVENT TABLES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(event_table_query)
        event_tables = cursor.fetchall()

        for event_table in event_tables:
            event_table_name = event_table[1]
            event_table_folder_path = s.path.join(schema_folder_path, "EVENT_TABLES")
            os.makedirs(event_table_folder_path, exist_ok=True)
            event_table_export_path = os.path.join(event_table_folder_path, event_table_name + ".sql")
            event_table_export_query = f"SELECT GET_DDL('EVENT TABLE','{db_name}.{schema_name}.{event_table_name}')"
            cursor.execute(event_table_export_query)
            event_table_create_statement = cursor.fetchone()[0]

            with open(event_table_export_path, 'w') as event_table_file:
                event_table_file.write(event_table_create_statement)
            
            # Commit Tables to GitHub
            subprocess.call(['git', 'add', event_table_export_path])
            subprocess.call(['git', 'commit', '-m', f'Commit {event_table_name} DDL'])
        
###### Export External Tables
        external_table_query = f"SHOW EXTERNAL TABLES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(external_table_query)
        external_tables = cursor.fetchall()

        for external_table in external_tables:
            external_table_name = external_table[1]
            external_table_folder_path = s.path.join(schema_folder_path, "EXTERNAL_TABLES")
            os.makedirs(external_table_folder_path, exist_ok=True)
            external_table_export_path = os.path.join(external_table_folder_path, external_table_name + ".sql")
            external_table_export_query = f"SELECT GET_DDL('EXTERNAL TABLE','{db_name}.{schema_name}.{external_table_name}')"
            cursor.execute(external_table_export_query)
            external_table_create_statement = cursor.fetchone()[0]

            with open(external_table_export_path, 'w') as external_table_file:
                external_table_file.write(external_table_create_statement)
            
            # Commit Tables to GitHub
            subprocess.call(['git', 'add', external_table_export_path])
            subprocess.call(['git', 'commit', '-m', f'Commit {external_table_name} DDL'])
        
###### Export File Formats
        file_formats_query = f"SHOW FILE FORMATS IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(file_formats_query)
        file_formats = cursor.fetchall()

        for file_format in file_formats:
            file_format_name = file_format[1]
            file_format_folder_path = s.path.join(schema_folder_path, "FILE_FORMATS")
            os.makedirs(external_table_folder_path, exist_ok=True)
            file_format_export_path = os.path.join(file_formats_folder_path, file_format_name + ".sql")
            file_format_export_query = f"SELECT GET_DDL('FILE FORMATS','{db_name}.{schema_name}.{file_format_name}')"
            cursor.execute(file_format_export_query)
            file_format_create_statement = cursor.fetchone()[0]

            with open(file_format_export_path, 'w') as file_format_file:
                file_format_file.write(file_format_create_statement)

            # Commit Tables to GitHub
            subprocess.call(['git', 'add', file_format_export_path])
            subprocess.call(['git', 'commit', '-m', f'Commit {file_format_name} DDL'])
        
###### Export Pipes

        pipes_query = f"SHOW PIPES IN SCHEMA {db_name}.{schema_name}"
        cursor.execute(pipes_query)
        pipes = cursor.fetchall()

        for pipe in pipes:
            pipe_name = pipe[1]
            pipe_format_folder_path = s.path.join(schema_folder_path, "PIPES")
            os.makedirs(pipe_folder_path, exist_ok=True)
            pipe_export_path = os.path.join(pipes_folder_path, pipe_name + ".sql")
            pipe_export_query = f"SELECT GET_DDL('PIPES','{db_name}.{schema_name}.{pipe_name}')"
            cursor.execute(pipe_export_query)
            pipe_create_statement = cursor.fetchone()[0]

            with open(pipe_export_path, 'w') as pipe_file:
                pipe_file.write(pipe_create_statement)
            
            # Commit Tables to GitHub
            subprocess.call(['git', 'add', pipe_export_path])
            subprocess.call(['git', 'commit', '-m', f'Commit {pipe_name} DDL'])
        

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
            os.makedirs(view_folder_path, exist_ok=True)
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
