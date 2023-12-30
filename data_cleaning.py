import pandas as pd
from metar import Metar
import numpy as np

def clean_flight_columns(df):
    df = df.drop(columns=['flight.identification.number.alternative', 
                          'flight.status.estimated', 
                          'flight.status.ambiguous', 
                          'flight.status.generic.status.type', 
                          'flight.status.generic.status.diverted', 
                          'flight.status.generic.eventTime.utc_millis',
                          'flight.status.generic.eventTime.utc',
                          'flight.status.generic.eventTime.local_millis',
                          'flight.status.generic.eventTime.local_date',
                          'flight.status.generic.eventTime.local_time',
                          'flight.status.generic.eventTime.local',
                          'flight.aircraft.model.text',
                          'flight.aircraft.registration',
                          'flight.aircraft.country.name',
                          'flight.aircraft.country.alpha2',
                          'flight.aircraft.country.alpha3',
                          'flight.aircraft.restricted',
                          'flight.aircraft.serialNo',
                          'flight.aircraft.age.availability',
                          'flight.aircraft.availability.serialNo',
                          'flight.aircraft.availability.age',
                          'flight.owner.name',
                          'flight.owner.code.iata',
                          'flight.owner.code.icao',
                          'flight.airline.code.iata',
                          'flight.airline.code.icao',
                          'flight.airline.name',
                          'flight.airport.origin.timezone.name',
                          'flight.airport.origin.timezone.offset',
                          'flight.airport.origin.timezone.abbr',
                          'flight.airport.origin.timezone.abbrName',
                          'flight.airport.origin.timezone.isDst',
                          'flight.airport.origin.info.terminal',
                          'flight.airport.origin.info.baggage',
                          'flight.airport.origin.info.gate',
                          'flight.airport.destination.code.iata',
                          'flight.airport.destination.code.icao',
                          'flight.airport.destination.timezone.name',
                          'flight.airport.destination.timezone.offset',
                          'flight.airport.destination.timezone.abbr',
                          'flight.airport.destination.timezone.abbrName',
                          'flight.airport.destination.timezone.isDst',
                          'flight.airport.destination.info.terminal',
                          'flight.airport.destination.info.baggage',
                          'flight.airport.destination.info.gate',
                          'flight.airport.destination.name',
                          'flight.airport.destination.position.latitude',
                          'flight.airport.destination.position.longitude',
                          'flight.airport.destination.position.country.name',
                          'flight.airport.destination.position.country.code',
                          'flight.airport.destination.position.region.city',
                          'flight.airport.destination.visible',
                          'flight.airport.real',
                          'flight.time.scheduled.departure_millis',
                          'flight.time.scheduled.arrival_millis',
                          'flight.time.scheduled.arrival_date',
                          'flight.time.scheduled.arrival_time',
                          'flight.time.scheduled.arrival',
                          'flight.time.real.arrival',
                          'flight.time.estimated.departure_millis',
                          'flight.time.estimated.departure_date',
                          'flight.time.estimated.departure_time',
                          'flight.time.estimated.departure',
                          'flight.time.estimated.arrival',
                          'flight.time.other.eta',
                          'flight.time.other.duration',
                          'flight.identification.codeshare',
                          'flight.aircraft.images',
                          'flight.owner',
                          'flight.time.real.departure_millis',
                          'flight.time.estimated.arrival_millis',
                          'flight.time.estimated.arrival_date',
                          'flight.time.estimated.arrival_time',
                          'flight.time.other.eta_millis',
                          'flight.time.other.eta_date',
                          'flight.time.other.eta_time',
                          'flight.time.real.arrival_millis',
                          'flight.time.real.arrival_date',
                          'flight.time.real.arrival_time',
                          'flight.status.text',
                          'flight.status.generic.eventTime.utc_date',
                          'flight.status.generic.eventTime.utc_time',
                          'flight.airport.real.name', 'flight.airport.real.code.iata',
                          'flight.airport.real.code.icao',
                          'flight.airport.real.position.latitude',
                          'flight.airport.real.position.longitude',
                          'flight.airport.real.position.country.name',
                          'flight.airport.real.position.country.code',
                          'flight.airport.real.position.region.city',
                          'flight.airport.real.timezone.name',
                          'flight.airport.real.timezone.offset',
                          'flight.airport.real.timezone.abbr',
                          'flight.airport.real.timezone.abbrName',
                          'flight.airport.real.timezone.isDst',
                          'flight.airport.real.visible'
                          ])
    return df


def convert_flight_time(df):
    #convert to approproate dtypes
    cols_to_convert = ['flight.time.scheduled.departure_date', 'flight.time.scheduled.departure_time']
    df[cols_to_convert] = df[cols_to_convert].astype(str)
    df['Timestamp'] = pd.to_datetime(df['flight.time.scheduled.departure_date'] + df['flight.time.scheduled.departure_time'], format='%Y%m%d%H%M')

    df = df.sort_values('Timestamp')
    return df
   
    
def convert_weather_time(df):
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%Y-%m-%d %H:%M:%S')
    df = df.sort_values('Timestamp') 
    return df


def merge_datasets(flight_df, weather_df):     
    #merge datasets on timestamp 
    merged_df = pd.merge_asof(flight_df,
                              weather_df,
                              on='Timestamp',
                              direction='backward',
                              tolerance=pd.Timedelta('30m')
                              )
    return merged_df

def parse_metar(weather):
    try:
        obs = Metar.Metar(weather)
        return pd.Series({
            'temp': obs.temp,
            'dew.pt': obs.dewpt,
            'vis': obs.vis,
            'pressure': obs.press,
            'wind.speed': obs.wind_speed
        })
    except:
        return pd.Series({
            'temp': np.nan,
            'dew.pt': np.nan,
            'vis': np.nan,
            'pressure': np.nan,
            'wind.speed': np.nan
        })

def create_features(df):
    #Time of day
    df['time.of.day'] = df['Timestamp'].dt.hour
    bins = [0, 6, 12, 18, 24]
    time_labels = ['Night', 'Morning', 'Afternoon', 'Evening']
    df['time.of.day'] = pd.cut(df['time.of.day'], bins=bins, labels=time_labels, include_lowest=True, right=False)
    
    #Season
    df['month'] = df['Timestamp'].dt.month
    spring = (df['month'] >= 3) & (df['month'] < 6)
    summer = (df['month'] >= 6) & (df['month'] < 9)
    autumn = (df['month'] >= 9) & (df['month'] < 12)
    winter = (df['month'] == 12) | ((df['month'] >= 1) & (df['month'] < 3))
    df.loc[spring, 'season'] = 'Spring'
    df.loc[summer, 'season'] = 'Summer'
    df.loc[autumn, 'season'] = 'Autumn'
    df.loc[winter, 'season'] = 'Winter'
    
    #Day of Week
    df['day.of.week'] = df['Timestamp'].dt.dayofweek
    day_labels = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df['day.of.week'] = df['day.of.week'].map(dict(enumerate(day_labels)))
    
    #Weather
    metar_df = df['WeatherInfo'].apply(parse_metar)
    df = pd.concat([df, metar_df], axis=1)
    
    #check if flight was delayed or not
    color_mapping = {'green': 0, 'yellow': 1, 'red': 1}
    df['isDelayed?'] = df['flight.status.generic.status.color'].map(color_mapping)
    df = df.drop(columns=['flight.status.generic.status.color'])

    return df

# Testing    
if __name__ == '__main__':
    flight_df = pd.read_csv('flights.csv')
    weather_df = pd.read_csv('hist_weather.csv')
    
    flight_df = clean_flight_columns(flight_df)
    
    flight_df = convert_flight_time(flight_df)
    weather_df = convert_weather_time(weather_df)
    

    merged_df = merge_datasets(flight_df, weather_df)
    merged_df = create_features(merged_df)

    merged_df.to_csv('merged.csv', index=False)
