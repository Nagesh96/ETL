
from sqlalchemy import create engine
def test_sql_auth_connection():
try:
connection string = ("mssql+pyodbc://"
"ccwuser: resum3@"
"sdcsqld07/ccwdb?"
"driver=ODBC+Driver+17+for+SQL+Server")
engine create engine (connection_string)
connection = engine.connect()
print("SQL server authentication connection successful")
connection.close()
except Exception as e:
print(f"Authentication failed: {e}")
if_name == main":
test sql auth_connection()
