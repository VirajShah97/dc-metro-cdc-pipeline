import requests, csv, os
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv('WMATA_API_KEY')
url = f'https://api.wmata.com/Rail.svc/json/jStations?api_key={api_key}'
resp = requests.get(url).json()

with open('dc_metro_dbt/seeds/dim_stations.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['station_code', 'station_name', 'latitude', 'longitude'])
    for s in resp['Stations']:
        writer.writerow([s['Code'], s['Name'], s['Lat'], s['Lon']])

print(f"Wrote {len(resp['Stations'])} stations")