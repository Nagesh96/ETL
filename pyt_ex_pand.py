import pandas as pd
from sqlalchemy import create_engine

# Define the path to the Excel file and the table name in MS SQL
excel_file_path = 'path_to_your_excel_file.xlsx'
table_name = 'your_sql_table_name'

# Load the Excel file into a pandas DataFrame
df = pd.read_excel(excel_file_path)

# Assuming you have a connection string to your MS SQL database
# Replace 'your_database_name', 'your_username', 'your_password', and 'your_server' with your actual credentials
connection_string = 'mssql+pyodbc://your_username:your_password@your_server/your_database_name?driver=ODBC+Driver+17+for+SQL+Server'

# Create an SQLAlchemy engine to connect to the database
engine = create_engine(connection_string)

# Insert the DataFrame into the MS SQL database table
df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

print(f"Data from '{excel_file_path}' loaded into '{table_name}' table in MS SQL database.")
