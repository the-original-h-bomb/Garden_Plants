import snowflake.connector
import subprocess
import os

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

# Export databases and artifacts - delivered databases contain some items that cannot be exported out
db_query = f"select * from information_schema.databases where database_NAME not like 'SNOWFLAKE%' AND TYPE = 'STANDARD';"

# Execute the query to fetch all databases
cursor.execute(db_query)

# Fetch all the databases
databases = cursor.fetchall()

for db in databases:
    db_name = db[0]
    os.makedirs(db_name, exist_ok=True)
    db_ddl_export_path = os.path.join(folder_path+"/"+db_name, db_name + ".sql")
    db_export_query = f"SELECT GET_DDL('DATABASE','{db_name}')"
    cursor.execute(db_export_query)
    db_create_statement = cursor.fetchone()[0]

    with open(db_ddl_export_path, 'w') as db_file:
        db_file.write(db_create_statement)

# Commit to GitHub
# Replace 'commit message' with your desired commit message
subprocess.call(['git', 'add', ddl_file_path])
subprocess.call(['git', 'commit', '-m', 'commit message'])
subprocess.call(['git', 'push'])
