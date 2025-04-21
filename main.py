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
    fhvhv_df = pd.read_parquet('fhvhv_tripdata_2022-11.parquet')
    print("\nFHVHV Parquet file loaded successfully!")
    print("Shape:", fhvhv_df.shape) #(10000000+, 17)
    print(fhvhv_df.head())
except Exception as e:
    print("Failed to load FHVHV Parquet file:", e)



# === Base code to company mapping (simplified example) ===
base_mapping = {
    'B02510': 'Uber',
    'B02512': 'Juno',
    'B02511': 'Via',
    'B02598': 'Lyft'
    # Add more mappings as needed
}

# === Add company info ===
fhvhv_df['service'] = fhvhv_df['dispatching_base_num'].map(base_mapping)
fhvhv_df = fhvhv_df[fhvhv_df['service'].notna()]

# === Preprocess time columns ===
fhvhv_df['pickup_datetime'] = pd.to_datetime(fhvhv_df['pickup_datetime'])
fhvhv_df['dropoff_datetime'] = pd.to_datetime(fhvhv_df['dropoff_datetime'])
fhvhv_df['trip_time_minutes'] = (fhvhv_df['dropoff_datetime'] - fhvhv_df['pickup_datetime']).dt.total_seconds() / 60

# === User-defined function to recommend service ===
def recommend_service(pickup_zone, dropoff_zone, priority='fastest'):
    try:
        pickup_id = taxi_zones_df[taxi_zones_df['zone'] == pickup_zone]['LocationID'].values[0]
        dropoff_id = taxi_zones_df[taxi_zones_df['zone'] == dropoff_zone]['LocationID'].values[0]
    except IndexError:
        return f"Could not find one or both zones: {pickup_zone}, {dropoff_zone}"

    # Filter trips
    trips = fhvhv_df[(fhvhv_df['PULocationID'] == pickup_id) &
                     (fhvhv_df['DOLocationID'] == dropoff_id)]

    if trips.empty:
        return "No matching trips found."

    # Group by service and compute stats
    grouped = trips.groupby('service').agg({
        'trip_time_minutes': 'mean',
        'hvfhs_license_num': 'count'
    }).rename(columns={'hvfhs_license_num': 'trip_count'}).reset_index()

    if priority == 'fastest':
        recommended = grouped.sort_values('trip_time_minutes').iloc[0]
    elif priority == 'most_popular':
        recommended = grouped.sort_values('trip_count', ascending=False).iloc[0]
    else:
        return "Invalid priority. Choose 'fastest' or 'most_popular'."

    return f"Recommended service: {recommended['service']} (Avg. Time: {recommended['trip_time_minutes']:.2f} min, Trips: {recommended['trip_count']})"

# === Example usage ===
user_pickup = "East Village"
user_dropoff = "Crown Heights North"
print(recommend_service(user_pickup, user_dropoff, priority='fastest'))
