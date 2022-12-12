#Project 1: CMPS 5443 SPATIAL DATABASE
#BY MD ABUBAKKAR
#INSTRUCTOR: DR. TERRY GRIFFIN

#Tools and language used:
#PGAdmin4
#Postgres
#Python
#HTML


# Dependencies
# pip install psycopg2

# Command to run
# python3 load_database.py

# for regular expression
import re

# for connecting to the database
import psycopg2

# Connect to the database
# Change the connection string accordingly (e.g. username, password, database name)
connection = psycopg2.connect(
    database="Project 1 -MD", #I had too many databases so had to make it unique ;(
    user='postgres',
    password='111', #I wanted to keep it simple
    host='127.0.0.1',
    port='5432'
)

# Create a cursor to perform database operations
cursor = connection.cursor()

# from where to read data
filename = 'earthquakes.csv' #I cleaned it up little bit in excel before importing


def create_table():
    # Create a table in the database (if it does not exist)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS EARTHQUAKE (
        earthquake_id TEXT PRIMARY KEY NOT NULL,
        occurred_on TEXT,
        latitude NUMERIC,
        longitude NUMERIC,
        depth NUMERIC,
        magnitude NUMERIC,
        calculation_method TEXT,
        network_id TEXT,
        place TEXT,
        cause TEXT
    )
    ''')
    # Commit the changes to the database
    connection.commit()


def insert_into_table(earthquake_id, occurred_on, latitude, longitude, depth, magnitude, calculation_method, network_id, place, cause):
    # Insert data into the table
    cursor.execute('''
    INSERT INTO EARTHQUAKE (earthquake_id, occurred_on, latitude, longitude, depth, magnitude, calculation_method, network_id, place, cause)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (earthquake_id, occurred_on, latitude, longitude, depth, magnitude, calculation_method, network_id, place, cause))

    # Commit the changes to the database
    connection.commit()


def main():

    # craete table EARTHQUAKE (if it does not already exist)
    create_table()

    # open file in read mode
    file = open(filename, 'r')

    # loop counter
    counter = 0

    # read line by line
    for line in file:
        counter += 1
        print(counter)

        # to skip the first line
        if counter != 1:
            # split the line into columns but ignore the commas inside the double quotes
            values = re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', line)

            # remove the double quotes
            values = [value.replace('"', '') for value in values]

            # insert the values into the database
            insert_into_table(values[0], values[1], values[2], values[3],
                              values[4], values[5], values[6], values[7], values[8], values[9])

    # close the file
    file.close()

    # close database connection
    connection.close()


# call the main function
main()
