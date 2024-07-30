from sqlalchemy import create_engine

def test_sql_auth_connection():
    try:
        # Connection string using SQL Server Authentication
        connection_string = (
            "mssql+pyodbc://"
            "your_sql_username:your_sql_password@"
            "your_sql_server_name/your_database_name?"
            "driver=ODBC+Driver+17+for+SQL+Server"
        )
        
        # Creating engine and establishing the connection
        engine = create_engine(connection_string)
        connection = engine.connect()
        print("SQL Server Authentication connection successful!")
        connection.close()
    except Exception as e:
        print(f"SQL Server Authentication connection failed: {e}")

if __name__ == "__main__":
    test_sql_auth_connection()
