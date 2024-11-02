import requests
import pandas as pd
import psycopg2
import json
import psycopg2
from dotenv import load_dotenv
import os

url = "https://realty-mole-property-api.p.rapidapi.com/randomProperties"

querystring = {"limit":"500"}

headers = {
	"x-rapidapi-key": "e64c48a3b5mshfe9cdd04e4c4a29p18e80bjsn55c3acbad527",
	"x-rapidapi-host": "realty-mole-property-api.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

# convert the data from jason format to tableu format
print(response.json())

data = response.json()
file_name = 'real_estate.json'

with open(file_name, 'w') as file:
    json.dump(data, file, indent=4)

real_estate_df =  pd.read_json('real_estate.json')
real_estate_df.head()

# REPLACE MISSING VALUES WITH APPROPRIATE REPLACEMENTS
real_estate_df.fillna({
        'bathrooms': 0.0,
        'bedrooms' : 0.0,
        'squareFootage' :0.0,
        'county': 'uknown',
        'propertyType': 'unknown',
        'yearBuilt' : 0.0,  
        'assessorID' : 'unkmown', 
        'legalDescription' : 'unknown',   
        'subdivision' :'unknown',
        'lotSize' : 0.0, 
        'ownerOccupied' : 0.0,
        'features' : 'unknown',
        'taxAssessment' : 'unknown',
        'propertyTaxes' : 'unknown',  
        'owner'         : 'unknown',
        'zoning'        : 'unknown', 
        'lastSalePrice' : 0.0,
        'addressLine2' : 'unknown'
},inplace=True)

location_dim = real_estate_df[['addressLine1', 'city', 'state', 'zipCode', 'formattedAddress','county',
                              'longitude', 'latitude','addressLine2']].copy().drop_duplicates().reset_index(drop=True)
# to add a column that was not in existence use the below code
location_dim.index.name = 'location_id'
# USE THIS CODE TO MAKE THE NEW CREATED INDEX TO APPEAR ON THE COLUMN HEADER
location_dim = location_dim.reset_index()

sales_dim = real_estate_df[['lastSaleDate','lastSalePrice']].copy().drop_duplicates().reset_index(drop=True)
sales_dim.index.name = 'sales_id'
sales_dim = sales_dim.reset_index()

sales_dim

# the features table has other tables in it that is in form of dictionary
# SOLUTION : this table has to be converted to a string or you expand the dictionary

real_estate_df['features'] = real_estate_df['features'].astype(str)
real_estate_df['taxAssessment'] = real_estate_df['taxAssessment'].astype(str)
real_estate_df['propertyTaxes'] = real_estate_df['propertyTaxes'].astype(str)

features_dim = real_estate_df[['bedrooms','bathrooms','squareFootage','lotSize',
                               'features']].copy().drop_duplicates().reset_index(drop=True)
features_dim.index.name = 'features_id'
features_dim = features_dim.reset_index()

# PROPERTY FACT TABLE
# remember to pick the colums you need from the property table after merging the dimension tables
property_fact_table = real_estate_df.merge(sales_dim, on=['lastSaleDate','lastSalePrice'],how = 'left')\
                                    .merge(location_dim, on=['addressLine1', 'city', 'state', 'zipCode', 'formattedAddress','county','longitude', 'latitude','addressLine2'],how = 'left')\
                                    .merge(features_dim, on=['bedrooms','bathrooms','squareFootage','lotSize','features'],how='left')\
                                    [['id','sales_id','location_id', 'features_id','yearBuilt', 'assessorID','legalDescription', 'ownerOccupied','propertyType','taxAssessment', 'propertyTaxes','subdivision','zoning']]

location_dim.to_csv('location_dim.csv', index=False)
sales_dim.to_csv('sales_dim.csv', index=False)
features_dim.to_csv('features_dim.csv', index=False)
property_fact_table.to_csv('property_fact_table.csv', index=False)

# DEVELOPE A FUNCTION TO GET THE DATABASE CONNECTION
# Load environment variables from .env file
# Load environment variables from .env file
load_dotenv()

def get_db_connection():
    connection = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        port=os.getenv('DB_PORT')  # Add the port here
    ) 
    return connection

# connect to database
conn = get_db_connection()

# CREATE A FUNCTION THAT SETS UP THE SCHEMA AND TABLE
def create_tables():
    conn = get_db_connection()
    cursor =conn.cursor()
    create_table_query = '''
                            CREATE SCHEMA IF NOT EXISTS ZANKU_REALTOR;

                            DROP TABLE IF EXISTS ZANKU_REALTOR.location_dim CASCADE;
                            DROP TABLE IF EXISTS ZANKU_REALTOR.sales_dim CASCADE;
                            DROP TABLE IF EXISTS ZANKU_REALTOR.features_dim CASCADE;
                            DROP TABLE IF EXISTS ZANKU_REALTOR.property_fact_table CASCADE;

                            CREATE TABLE ZANKU_REALTOR.location_dim(
                            location_id INTEGER PRIMARY KEY,
                            addressLine1 VARCHAR(10000),
                            city VARCHAR(1000),
                            state VARCHAR(1000),
                            zipCode INTEGER,
                            formattedAddress VARCHAR(10000),
                            county VARCHAR(1000),
                            longitude FLOAT,
                            latitude FLOAT,
                            addressLine2 VARCHAR(1000)
                            );

                            CREATE TABLE ZANKU_REALTOR.sales_dim(
                            sales_id INTEGER PRIMARY KEY,
                            lastSaleDate  VARCHAR(10000),
                            lastSalePrice VARCHAR(1000)
                            );

                            CREATE TABLE ZANKU_REALTOR.features_dim(
                            features_id INTEGER PRIMARY KEY,
                            bedrooms FLOAT,
                            bathrooms FLOAT,
                            squareFootage FLOAT,
                            lotSize FLOAT,
                            features  VARCHAR(10000)
                            );

                            CREATE TABLE ZANKU_REALTOR.property_fact_table(
                            id VARCHAR(10000) PRIMARY KEY,
                            sales_id INTEGER,
                            location_id INTEGER,
                            features_id INTEGER,
                            yearBuilt  FLOAT,
                            assessorID VARCHAR(10000),
                            legalDescription VARCHAR(1000),
                            ownerOccupied FLOAT,
                            propertyType VARCHAR(1000),
                            taxAssessment VARCHAR(1000),
                            propertyTaxes VARCHAR(1000),
                            subdivision VARCHAR(1000),
                            zoning VARCHAR(1000),
                            FOREIGN KEY (location_id) REFERENCES ZANKU_REALTOR.location_dim(location_id),
                            FOREIGN KEY (sales_id) REFERENCES ZANKU_REALTOR.sales_dim(sales_id),
                            FOREIGN KEY (features_id) REFERENCES ZANKU_REALTOR.features_dim(features_id)
                            );
                            '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

    create_tables()

import logging
import psycopg2  # or the relevant database adapter

# Configure logging to log errors
logging.basicConfig(filename='db_insert_errors.log', level=logging.ERROR)

# Initialize the database connection and cursor
try:
    conn = psycopg2.connect(
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )
    cursor = conn.cursor()

    # Iterate over the rows in the DataFrame and insert them into the database
    for _, row in property_fact_table.iterrows():
        try:
    # Check if location_id exists in location_dim
            cursor.execute('SELECT 1 FROM ZANKU_REALTOR.location_dim WHERE id = %s', (row['location_id'],))
            if cursor.fetchone() is None:
                raise ValueError(f"location_id {row['location_id']} does not exist in location_dim")

            # If the check passes, insert the row
            cursor.execute(
                '''INSERT INTO ZANKU_REALTOR.property_fact_table(id, sales_id, location_id, features_id, yearBuilt,
                assessorID, legalDescription, ownerOccupied, propertyType, taxAssessment, propertyTaxes, subdivision, zoning)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (row['id'], row['sales_id'], row['location_id'], row['features_id'], row['yearBuilt'], row['assessorID'],
                 row['legalDescription'], row['ownerOccupied'], row['propertyType'], row['taxAssessment'], row['propertyTaxes'], row['subdivision'], row['zoning'])
            )
        except Exception as e:
            # Log the error and continue with the next row
            logging.error(f"Error inserting row with id {row['id']}: {e}")

    # Commit changes
    conn.commit()

except Exception as e:
    # Log any connection errors
    logging.error(f"Database connection failed: {e}")

finally:
    # Ensure that the cursor and connection are properly closed
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()

# Loading data into db tables
conn = get_db_connection()
cursor = conn.cursor()

# Insert the dataframe into SQL tables using executemany for better performance
cursor.executemany(
    '''INSERT INTO ZANKU_REALTOR.location_dim(location_id, addressLine1, city, state, zipCode, formattedAddress, county,
                        longitude, latitude, addressLine2)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
    location_dim.values.tolist()
)

cursor.executemany(
    '''INSERT INTO ZANKU_REALTOR.sales_dim(sales_id, lastSaleDate, lastSalePrice)
        VALUES (%s, %s, %s)''',
    sales_dim[['sales_id', 'lastSaleDate', 'lastSalePrice']].values.tolist()
)

cursor.executemany(
    '''INSERT INTO ZANKU_REALTOR.features_dim(features_id, bedrooms, bathrooms, squareFootage, lotSize, features)
        VALUES (%s, %s, %s, %s, %s, %s)''',
    features_dim[['features_id', 'bedrooms', 'bathrooms', 'squareFootage', 'lotSize', 'features']].values.tolist()
)

cursor.executemany(
    '''INSERT INTO ZANKU_REALTOR.property_fact_table(id, sales_id, location_id, features_id, yearBuilt,
    assessorID, legalDescription, ownerOccupied, propertyType, taxAssessment, propertyTaxes, subdivision, zoning)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
    property_fact_table.values.tolist()
)

# Commit changes
conn.commit()

# Close connection
cursor.close()
conn.close()
