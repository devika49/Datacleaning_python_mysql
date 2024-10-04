import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


db_connection = mysql.connector.connect(
    host="localhost",
    user="root",  
    password="1999@devi", 
 
)
print("Connection Successfully!")

#creating the cursor object,cursor object used to excute the MySQL queries
cursor = db_connection.cursor()

#sql command for creating the database name, use the database
cursor.execute("CREATE DATABASE IF NOT EXISTS political_data")
cursor.execute("USE political_data")


#creating the table
create_table_query = """
CREATE TABLE IF NOT EXISTS politicians (
    id INT AUTO_INCREMENT PRIMARY KEY,
    index INT,
    user_id VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    sex VARCHAR(10),
    email VARCHAR(255),
    phone VARCHAR(50),
    age INT,
    job_title VARCHAR(255)
);
"""


print("Executing SQL Query:", create_table_query)

# Executing <create table query> with error handling
try:
    cursor.execute(create_table_query)
except mysql.connector.Error as err:
    print(f"Error: {err}")

# Load CSV file into pandas (DataFrame)
file_path = 'people.csv'  
df = pd.read_csv(file_path)


# print the column names
print("Columns in the DataFrame:", df.columns)

# Calculate age from 'Date of birth' column (ensure it's in a valid date format)
df['date_of_birth'] = pd.to_datetime(df['Date of birth'], errors='coerce')
df['age'] = df['date_of_birth'].apply(lambda dob: datetime.now().year - dob.year if pd.notnull(dob) else None)

# Fill missing values
df.ffill(inplace=True)  

# Converts all column names to lowercase and replaces spaces with underscores
df.columns = df.columns.str.lower().str.replace(' ', '_')


# Select the relevant columns from the table
df_cleaned = df[['index','user_id', 'first_name', 'last_name', 'sex', 'email', 'phone', 'age', 'job_title']]


# SQLAlchemy create a engine that allows tha python to connect to the MySQL database.
engine = create_engine('mysql+mysqlconnector://root:1999%40devi@localhost/political_data') 



# Insert Data into MySQL table
try:
    df_cleaned.to_sql(name='politicians', con=engine, if_exists='append', index=False)
    print("Data inserted successfully.")
except Exception as e:
    print(f"Error while inserting data: {e}")


# Query the data from the politicians table
try:
    query = "SELECT * FROM politicians;"
    df_query_result = pd.read_sql(query, db_connection)

    # Print the DataFrame in a tabular format
    print("Data from politicians table:")
    print(df_query_result)
except mysql.connector.Error as err:
    print(f"Error fetching data: {err}")


# Close connection
cursor.close()
db_connection.close()
