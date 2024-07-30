from sqlalchemy import create_engine

def test_sql_auth_connection():
    try:
        connection_string = (
            "mssql+pyodbc://"
            "ccwuser:resum3@"
            "sdcsqld07/ccwdb?"
            "driver=ODBC+Driver+17+for+SQL+Server"
        )
        engine = create_engine(connection_string)
        connection = engine.connect()
        print("SQL Server Authentication connection successful")
        connection.close()
    except Exception as e:
        print(f"Authentication failed: {e}")

if __name__ == "__main__":
    test_sql_auth_connection()
