import pandas as pd
import kaggle

taxi_zones_df = pd.read_csv('tlc-nyc-taxi-zones/taxi_zones.csv')
# fhvhv_df = pd.read_parquet('nyc-fhvhv-data/<filename>.parquet')

try:
    taxi_zones_df = pd.read_csv('tlc-nyc-taxi-zones/taxi_zones.csv')
    print("Taxi Zones CSV loaded successfully!")
    print("Shape:", taxi_zones_df.shape) # should give (265, 4) i think
    print(taxi_zones_df.head())
except Exception as e:
    print("Failed to load taxi_zones.csv:", e)

# Test reading FHVHV Parquet file (update the filename as needed)
try:
    #   fhvhv_df = pd.read_parquet('nyc-fhvhv-data/fhvhv_tripdata_2020-01.parquet')
    print("\nFHVHV Parquet file loaded successfully!")
    print("Shape:", fhvhv_df.shape) #(10000000+, 17)
    print(fhvhv_df.head())
except Exception as e:
    print("Failed to load FHVHV Parquet file:", e)