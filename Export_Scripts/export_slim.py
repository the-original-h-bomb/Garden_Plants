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

# Export DDL to a file
ddl_query = 'SHOW TABLES;'
cursor = con.cursor()
cursor.execute(ddl_query)
ddl_results = cursor.fetchall()
ddl_file_path = os.path.join(folder_path, 'ddl_export.sql')

with open(ddl_file_path, 'w') as ddl_file:
    for row in ddl_results:
        ddl_file.write(row[0] + '\n')

# Commit to GitHub
# Replace 'commit message' with your desired commit message
subprocess.call(['git', 'add', ddl_file_path])
subprocess.call(['git', 'commit', '-m', 'commit message'])
subprocess.call(['git', 'push'])
