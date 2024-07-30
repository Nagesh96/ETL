from sqlalchemy import create_engine

def test_windows_auth_connection():
    try:
        # Connection string using Windows Authentication
        connection_string = (
            "mssql+pyodbc://"
            "@your_sql_server_name/your_database_name?"
            "driver=ODBC+Driver+17+for+SQL+Server;"
            "Trusted_Connection=yes;"
        )
        
        # Creating engine and establishing the connection
        engine = create_engine(connection_string)
        connection = engine.connect()
        print("Windows Authentication connection successful!")
        connection.close()
    except Exception as e:
        print(f"Windows Authentication connection failed: {e}")

if __name__ == "__main__":
    test_windows_auth_connection()
