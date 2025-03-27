import pandas as pd 
import requests 
import json 
from itertools import combinations
import datetime 
import numpy as np
import boto3

kinesis_client = boto3.client('kinesis', region_name='us-east-1')
stream_name = 'flightdatastream'

def lambda_handler(event, context):

    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%d")
    airports = [
    "ATL", "LAX","DFW", "DEN", "ORD", "JFK", "MCO", 
    "LAS", "CLT", "MIA","EWR", "SFO", "PHX", "LGA", "DTW"
]
    
    route_dict = {
        f"route_comb_{i+1}": f"{a}-{b}"
            for i, (a, b) in enumerate(combinations(airports, 2))
                }
    
    flight_data_df = []

    for key, route in route_dict.items():
        flights_info = []
        airport_1 = route[0:3]
        airport_2 = route[4:7]
        api_key = '459f3b207c0062530edfd074297a0739'
        url = f"https://api.aviationstack.com/v1/flights?access_key={api_key}"
        query_string = { 
            "flight_date": today_str,
            "flight_status": "scheduled",
            "dep_iata": airport_1,
            "arr_iata": airport_2
        }
        headers = {
         'User-Agent': 'Mozilla/5.0 (compatible; AWS Lambda Python)'
        }
        response = requests.get(url, params=query_string, headers=headers)


        data = response.json()

        for flight in data.get('data', []):
            try:
                flights_info.append({
                    'flight_route': f"{flight['departure']['iata']} - {flight['arrival']['iata']}",
                    'airline_code': (flight.get('codeshared', {}).get('airline_iata') or flight['airline'].get('iata', 'N/A')).upper(),
                    'flight_number': (flight.get('codeshared', {}).get('flight_number') or flight['flight'].get('number', 'N/A')),
                    'registration': flight.get('aircraft', {}).get('registration', 'N/A'),
                    'aircraft_type': flight.get('aircraft', {}).get('iata', 'N/A'),
                    'dep_airport': flight['departure']['airport'],
                    'arr_airport': flight['arrival']['airport'],
                    'scheduled_dep': flight['departure'].get('scheduled', 'N/A'),
                    'scheduled_arr': flight['arrival'].get('scheduled', 'N/A'),
                    'estimated_arr': flight['arrival'].get('estimated', 'N/A'),
                    'actual_arr': flight['arrival'].get('actual', 'N/A'),
                    'flight_status': flight.get('flight_status', 'N/A'),
                    'latitude': flight.get('live', {}).get('latitude', 'N/A'),
                    'longitude': flight.get('live', {}).get('longitude', 'N/A'),
                    'altitude': flight.get('live', {}).get('altitude', 'N/A'),
                    'speed_vertical': flight.get('live', {}).get('speed_vertical', 'N/A'),
                    'speed_horizontal': flight.get('live', {}).get('speed_horizontal', 'N/A'),
                    'is_ground': flight.get('live', {}).get('is_ground', 'N/A')
                })
            except Exception as e:
                print(f"No Live Flights for {airport_1}-{airport_2} :", e)

        df = pd.DataFrame(flights_info)
        if df.empty:
            continue

        df['scheduled_dep'] = pd.to_datetime(df['scheduled_dep'], utc=True, errors='coerce')
        df['scheduled_dep_timestamp'] = pd.to_datetime(df['scheduled_dep'], utc=True, errors = 'coerce').dt.time
        df['scheduled_arr'] = pd.to_datetime(df['scheduled_arr'], utc=True, errors='coerce')
        df['estimated_arr'] = pd.to_datetime(df['estimated_arr'], utc=True, errors='coerce')
        df['actual_arr'] = pd.to_datetime(df['actual_arr'], utc=True, errors='coerce')
        df['flight_date'] = df['scheduled_dep'].dt.date.fillna('N/A')
        df['flight_num_code'] = df['airline_code'].astype(str) + df['flight_number'].astype(str)
        df['arrival_delay_min'] = (df['actual_arr'] - df['scheduled_arr']).dt.total_seconds().div(60).fillna('N/A')

        df = df[[
            'flight_route', 'flight_num_code', 'airline_code', 'flight_number',
            'registration', 'aircraft_type', 'dep_airport', 'arr_airport',
            'scheduled_dep','scheduled_dep_timestamp', 'scheduled_arr', 'estimated_arr', 'actual_arr',
            'flight_status', 'is_ground', 'speed_vertical', 'speed_horizontal',
            'latitude', 'longitude', 'arrival_delay_min'
        ]]
        datetime_cols = [
        'scheduled_dep',
        'scheduled_arr',
        'estimated_arr',
        'actual_arr',
        'flight_date',
        'scheduled_dep_timestamp'
        ]
        for col in datetime_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x.isoformat() if pd.notna(x) else None)
        flight_data_df.append(df)
    full_df = pd.concat(flight_data_df, ignore_index=True)

    if flight_data_df:
        full_df = pd.concat(flight_data_df, ignore_index=True)
        for _, row in full_df.iterrows():
            record = row.to_dict()
            for key, value in record.items():
                if isinstance(value, (pd.Timestamp, datetime.date, datetime.datetime,np.datetime64)):
                    record[key] = pd.to_datetime(value).isoformat()
                elif pd.isna(value): 
                    record[key] = None
                else: 
                    record[key] = str(value)
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(record),
                PartitionKey=str(record.get('flight_route', 'default-key'))
            )
            
    return {
        'statusCode': 200,
        'body': json.dumps('Ingested Live Flight Data Stream')
    }
