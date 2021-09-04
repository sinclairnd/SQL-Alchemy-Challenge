from flask import Flask, jsonify
import pandas as pd
import datetime as dt
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

app = Flask(__name__)

# create engine to hawaii.sqlite
engine = create_engine('sqlite:///hawaii.sqlite')

# Create our session (link) from Python to the DB
session = Session(engine)


@app.route("/")
def home_page():
    return ("<h1>Homepage</h1>"
            "<ul>" 
                '<li><a href="/api/v1.0/precipitation">/api/v1.0/precipitation</a></li>'
                '<li><a href="/api/v1.0/stations">/api/v1.0/stations</a></li>'
                '<li><a href="/api/v1.0/tobs">/api/v1.0/tobs</a></li>'
                '<li>/api/v1.0/&lt;start&gt;</li>'
                '<li>/api/v1.0/&lt;start&gt;/&lt;end&gt;</li>'
            "</ul>")

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Design a query to retrieve the last 12 months of precipitation data and plot the results
    data_precip = pd.read_sql("SELECT * FROM measurement WHERE date >= DATE('2017-08-23', '-12 month')", engine)

    # Sort the dataframe by date
    data_precip['date'] = pd.to_datetime(data_precip['date'])
    precip_sort = data_precip.sort_values('date')
    precip_sort['date'] = precip_sort['date'].astype(str)

    precip_dict = {}
    for i,l in precip_sort[["date", "prcp"]].iterrows():
        precip_dict[l["date"]] = l["prcp"]
    
    return jsonify(precip_dict)

@app.route("/api/v1.0/stations")
def stations():
    # Design a query to find the most active stations (i.e. what stations have the most rows?)
    data_station = pd.read_sql("SELECT DISTINCT station FROM measurement", engine)
    station_list = data_station["station"].tolist()
    return jsonify(station_list)


@app.route("/api/v1.0/tobs")
def tobs():
    data = pd.read_sql("SELECT * FROM measurement\
                    WHERE date >= DATE('2017-08-23', '-12 month')\
                    AND station = 'USC00519281'", engine)
    data['date'] = pd.to_datetime(data['date'])
    tobs_sort = data.sort_values('date')
    tobs_sort['date'] = tobs_sort['date'].astype(str)
    tobs_list = tobs_sort["tobs"].tolist()
    return jsonify(tobs_list)

@app.route("/api/v1.0/<start>")
def agg_without_end(start):
    # Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
    data = pd.read_sql(f"SELECT MIN(tobs) AS Lowest,\
                            MAX(tobs) AS Highest,\
                            AVG(tobs) AS Average\
                        FROM measurement\
                        WHERE station = 'USC00519281'\
                            AND date > {start}", engine)
    return jsonify([data["Lowest"][0], data["Highest"][0], data["Average"][0]])


@app.route("/api/v1.0/<start>/<end>")
def agg_with_end(start, end):
        # Using the most active station id from the previous query, calculate the lowest, highest, and average temperature.
    data = pd.read_sql(f"SELECT MIN(tobs) AS Lowest,\
                            MAX(tobs) AS Highest,\
                            AVG(tobs) AS Average\
                        FROM measurement\
                        WHERE station = 'USC00519281'\
                            AND date BETWEEN {start} AND {end}", engine)
    return jsonify([data["Lowest"][0], data["Highest"][0], data["Average"][0]])

if __name__ == "__main__": 
    app.run()
    