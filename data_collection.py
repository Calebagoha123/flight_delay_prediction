import pandas as pd
from pandas import json_normalize
from pyflightdata import FlightData

def scrape_flights(f=FlightData()):
    #Collect data on flight departures from Schipol Airport, Amsterdam
    flights = f.get_airport_departures(iata='AMS', page=20, earlier_data=True)
    data = json_normalize(flights)

    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df


def scrape_weather_hist(f=FlightData()):
    weather = f.get_airport_metars_hist('AMS')
    weather = list(weather.items())
    df = pd.DataFrame(weather, columns=['Timestamp', 'WeatherInfo'])
    return df
 


# Testing   
if __name__ == '__main__':
    flight_df = scrape_flights()
    flight_df.to_csv('flights.csv', index=False)
    
    weather_df = scrape_weather_hist()
    weather_df.to_csv('hist_weather.csv', index=False)