import pandas as pd
import data_collection as dco
import data_cleaning as dcl

if __name__ == "__main__":
    flights_df = dco.scrape_flights()
    flights_df = dcl.clean_flight_columns(flights_df)
    flights_df = dcl.convert_flight_time(flights_df)

    weather_df = dco.scrape_weather_hist()
    weather_df = dcl.convert_weather_time(weather_df)
    
    past_data = dcl.merge_datasets(flights_df, weather_df)
    past_data = past_data[past_data['flight.status.generic.status.text'] == 'departed']
    past_data = dcl.create_features(past_data)
    
    past_data = past_data.dropna()   
    past_data.to_csv('past_data.csv', index=False)
    
    