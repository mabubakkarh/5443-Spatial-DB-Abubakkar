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
# pip install fastapi
# pip install uvicorn

# command to run the program
# uvicorn md:app --reload

# for connect to the database
import psycopg2

# for generating API in python
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

# create a FastAPI object
app = FastAPI()

# Connect to the database
# Change the connection string accordingly (e.g. username, password, database name)
connection = psycopg2.connect(
    database="Project 1 -MD",
    user='postgres',
    password='111',
    host='127.0.0.1',
    port='5432'
)
# Create a cursor to perform database operations
cursor = connection.cursor()


@app.get("/", response_class=HTMLResponse)
# homepage
def homepage():
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8" />
        <meta http-equiv="X-UA-Compatible" content="IE=edge" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Document</title>
        <style>
        table,
        th,
        td {
            border: 1px solid black;
            padding: 10px;
        }

        table {
            border-collapse: collapse;
            width: 100%;
        }
        </style>
    </head>
    <body>
        <h1>Welcome to Earthquake API Home Page</h1>

        <h2>Available Endpoints</h2>
        <!-- create a table of endpoints here -->
        <table>
        <tr>
            <th>API Endpoint</th>
            <th>API Link</th>
            <th>API Description</th>
        </tr>
        <tr>
            <td>/findAll</td>
            <td>
            <a href="http://127.0.0.1:8000/findAll"
                >http://127.0.0.1:8000/findAll</a
            >
            </td>
            <td>Find all earthquakes data saved in the database</td>
        </tr>
        <tr>
            <td>/findOne</td>
            <td>
            <a href="http://127.0.0.1:8000/findOne?earthquake_id=1"
                >http://127.0.0.1:8000/findOne</a
            >
            </td>
            <td>
            Find one earthquake data saved in the database, based on particular
            parameters
            </td>
        </tr>
        <tr>
            <td>/findClosest</td>
            <td>
            <a href="http://127.0.0.1:8000/findClosest?lat=28%20&lon=-99"
                >http://127.0.0.1:8000/findClosest</a
            >
            </td>
            <td>
            Find the closest earthquake data saved in the database, based on the
            given latitude and longitude
            </td>
        </tr>
        </table>

        <br />
        <hr />

        <ul>
        <li>
            <h2>findAll</h2>
            <p>
            This endpoint returns all the earthquakes data saved in the database.
            The data is returned in JSON format.
            </p>
            <p>
            <strong>Link:</strong>
            <a href="http://127.0.0.1:8000/findAll" target="_blank"
                >http://127.0.0.1:8000/findAll</a
            >
            </p>
        </li>
        <li>
            <h2>findOne</h2>
            <p>
            This endpoint returns one earthquake data saved in the database, based
            on particular parameters. The data is returned in JSON format.
            </p>
            <p>
            Find by <strong>earthquake_id</strong>:
            <a
                href="http://127.0.0.1:8000/findOne?earthquake_id=1"
                target="_blank"
                >http://127.0.0.1:8000/findOne?earthquake_id=1</a
            >
            </p>
            <p>
            Find by <strong>occured_on</strong>:
            <a
                href="http://127.0.0.1:8000/findOne?occurred_on=1969-01-01%209:07:06"
                target="_blank"
                >http://127.0.0.1:8000/findOne?occurred_on=1969-01-01%209:07:06</a
            >
            </p>
            <p>
            Find by <strong>depth</strong>:
            <a href="http://127.0.0.1:8000/findOne?depth=45" target="_blank"
                >http://127.0.0.1:8000/findOne?depth=45</a
            >
            </p>
        </li>
        <li>
            <h2>findClosest</h2>
            <p>
            This endpoint returns the closest earthquake data saved in the
            database, based on the given latitude and longitude. The data is
            returned in JSON format.
            </p>
            <p>
            <strong>Link:</strong>
            <a
                href="http://127.0.0.1:8000/findClosest?lat=28%20&lon=-99"
                target="_blank"
                >http://127.0.0.1:8000/findClosest?lat=28%20&lon=-99</a
            >
            </p>
        </li>
        </ul>
    </body>
    </html>
    """


@app.get("/findAll")
# to get all the data from the database
# http://127.0.0.1:8000/findAll
def findAll():
    all_data = cursor.execute("SELECT * FROM EARTHQUAKE")
    all_data = cursor.fetchall()
    data = []
    for row in all_data:
        data.append({
            'earthquake_id': row[0],
            'occurred_on': row[1],
            'latitude': row[2],
            'longitude': row[3],
            'depth': row[4],
            'magnitude': row[5],
            'calculation_method': row[6],
            'network_id': row[7],
            'place': row[8],
            'cause': row[9]
        })
    return data


@app.get("/findOne")
# to find a particular data from the database (based on column key-value)
# http://127.0.0.1:8000/findOne?earthquake_id=1
def findOne(earthquake_id=None, occurred_on=None, depth=None):
    json_data = {}
    if earthquake_id is not None:
        get_data = cursor.execute(
            "SELECT * FROM EARTHQUAKE WHERE earthquake_id = %s", (earthquake_id,))
        get_data = cursor.fetchone()
    elif occurred_on is not None:
        get_data = cursor.execute(
            "SELECT * FROM EARTHQUAKE WHERE occurred_on = %s", (occurred_on,))
        get_data = cursor.fetchone()
    elif depth is not None:
        get_data = cursor.execute(
            "SELECT * FROM EARTHQUAKE WHERE depth = %s", (depth,))
        get_data = cursor.fetchone()
    else:
        return "Invalid parameter"
    json_data = {
        'earthquake_id': get_data[0],
        'occurred_on': get_data[1],
        'latitude': get_data[2],
        'longitude': get_data[3],
        'depth': get_data[4],
        'magnitude': get_data[5],
        'calculation_method': get_data[6],
        'network_id': get_data[7],
        'place': get_data[8],
        'cause': get_data[9]
    }
    return json_data


@app.get("/findClosest")
# to find the closest data from the database (based on latitude and longitude)
# http://127.0.0.1:8000/findClosest?lat=28%20&lon=-99
def findClosest(lat=None, lon=None):
    if lat is not None and lon is not None:
        get_data = cursor.execute(
            "SELECT * FROM EARTHQUAKE ORDER BY (latitude - %s)^2 + (longitude - %s)^2 LIMIT 1", (lat, lon))
        get_data = cursor.fetchone()
        json_data = {
            'earthquake_id': get_data[0],
            'occurred_on': get_data[1],
            'latitude': get_data[2],
            'longitude': get_data[3],
            'depth': get_data[4],
            'magnitude': get_data[5],
            'calculation_method': get_data[6],
            'network_id': get_data[7],
            'place': get_data[8],
            'cause': get_data[9]
        }
        return json_data
    else:
        return "Invalid parameter"
