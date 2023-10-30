import snowflake.connector
import subprocess
import os

# Snowflake connection parameters
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

# Export DDL to a file
ddl_query = 'SHOW TABLES;'
cursor = conn.cursor()
cursor.execute(ddl_query)
ddl_results = cursor.fetchall()
with open('ddl_export.sql', 'w') as ddl_file:
    for row in ddl_results:
        ddl_file.write(row[0] + '\n')

# Commit to GitHub
# Replace 'commit message' with your desired commit message
subprocess.call(['git', 'add', 'ddl_export.sql'])
subprocess.call(['git', 'commit', '-m', 'commit message'])
subprocess.call(['git', 'push'])
