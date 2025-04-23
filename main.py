import pandas as pd
import heapq
from datetime import datetime
#import geopandas as gpd

# Load trip data
df_trips = pd.read_parquet("fhvhv_tripdata_2022-11.parquet")

# Load zone mapping
df_zones = pd.read_csv("tlc-nyc-taxi-zones/taxi_zones.csv")

# maybe could use this for visualization later on
#gdf_zones = gpd.read_file("Data/tlc-nyc-taxi-zones/NYC Taxi Zones.geojson")

# Confirm it worked
#print("Trips:", df_trips.shape)
#print("Zones:", df_zones.shape)
# Detect column name case (Zone vs zone)
zone_col = 'Zone' if 'Zone' in df_zones.columns else 'zone'

# Lowercase for search
df_zones['zone_lower'] = df_zones[zone_col].str.lower()

#function to get location id and give option to choose from values if there are more than 1
#for example, if someone types "east" it will show all the locations with east and then ask user to pick one
def get_location_id(zone_name):
    matches = []
    zone_name = zone_name.strip().lower()

    for _, row in df_zones.iterrows():
        if zone_name in row['zone_lower']:
            matches.append((row['LocationID'], row[zone_col]))

    if len(matches) == 0:
        print(f"No match found for '{zone_name}'")
        return None
    elif len(matches) == 1:
        loc_id, name = matches[0]
        print(f"Match found: {name} → LocationID: {loc_id}")
        return loc_id
    else:
        print(f"\nMultiple matches for '{zone_name}':")
        for i, (loc_id, name) in enumerate(matches):
            print(f"  {i+1}. {name} (LocationID: {loc_id})")
        selection = input("Enter the number of the correct match: ").strip()
        if selection.isdigit():
            index = int(selection) - 1
            if 0 <= index < len(matches):
                return matches[index][0]
        print("Invalid selection.")
        return None

# save LocationIDs for future algorithms
# Loop until valid start location is found
start_id = None
while start_id is None:
    start_name = input("Enter your START location: ").strip().lower()
    start_id = get_location_id(start_name)

# Loop until valid end location is found
end_id = None
while end_id is None:
    end_name = input("Enter your END location: ").strip().lower()
    end_id = get_location_id(end_name)
#check
#print(f"\nStart ID: {start_id}")
#print(f"End ID: {end_id}")

#check
#print(f"\nStart ID: {start_id}")
#print(f"End ID: {end_id}")
#first lets sort the data by id so that we can use the search algorithm
#the built in tim sort takes like O(nlogn)
df_trips.sort_values(by=['PULocationID', 'DOLocationID'], inplace=True)
df_trips.reset_index(drop=True, inplace=True)  #Reset index for easier iteration

#map license codes to company names
company_map = {
    'HV0002': 'Juno',
    'HV0003': 'Uber',
    'HV0004': 'Via',
    'HV0005': 'Lyft'
}

# get zone names from LocationID
zone_col = 'Zone' if 'Zone' in df_zones.columns else 'zone'
zone_lookup = dict(zip(df_zones['LocationID'], df_zones[zone_col]))

start_zone = zone_lookup.get(start_id, f"ID {start_id}")
end_zone = zone_lookup.get(end_id, f"ID {end_id}")

#since now the data is sorted, lets run the data through a binary search algorithm
def lower_bound(trips, start_id, end_id):
    low = 0
    high = len(trips) - 1
    result = -1

    while low <= high:
        mid = (low + high) // 2
        pickup = trips.iloc[mid]['PULocationID']
        dropoff = trips.iloc[mid]['DOLocationID']

        if (pickup, dropoff) < (start_id, end_id):
            low = mid + 1
        else:
            if (pickup, dropoff) == (start_id, end_id):
                result = mid  #save first match index
            high = mid - 1

    return result

#collecting all values that match start and end location
def collect_all_matches(trips, start_idx, start_id, end_id):
    results = []
    i = start_idx
    while i < len(trips):
        row = trips.iloc[i]
        if row['PULocationID'] == start_id and row['DOLocationID'] == end_id:
            company = company_map.get(row['hvfhs_license_num'], 'Unknown')
            results.append((row['pickup_datetime'], row['dropoff_datetime'], company))
        else:
            break  #since it's sorted, we can stop early
        i += 1
    return results

#using everything to get results
#binary search
first_match_idx = lower_bound(df_trips, start_id, end_id)

if first_match_idx != -1:
    matches = collect_all_matches(df_trips, first_match_idx, start_id, end_id)
    print(f"\nFound {len(matches)} matching trips from {start_zone} to {end_zone}:\n")
    for i, (pickup, dropoff, company) in enumerate(matches, 1):
        print(f"{i}. From: {start_zone} → To: {end_zone}  |  Pickup: {pickup}  |  Dropoff: {dropoff}  |  Company: {company}")
else:
    print(f"No matching trips found from {start_zone} to {end_zone}.")
#setting matches to something if there is nothing given
# Collect matching trips based on start_id and end_id
first_match_idx = lower_bound(df_trips, start_id, end_id)

if first_match_idx != -1:
    matches = collect_all_matches(df_trips, first_match_idx, start_id, end_id)
else:
    matches = []  #define it even if no matches are found

# get avg durations from the matched trip rows
def compute_average_durations(matches):
    company_durations = {}
    company_counts = {}

    for pickup, dropoff, company in matches:
        # Ensure datetime format
        if isinstance(pickup, str):
            pickup = datetime.fromisoformat(pickup)
        if isinstance(dropoff, str):
            dropoff = datetime.fromisoformat(dropoff)

        duration = (dropoff - pickup).total_seconds() / 60  #in minutes

        if company not in company_durations:
            company_durations[company] = 0
            company_counts[company] = 0

        company_durations[company] += duration
        company_counts[company] += 1

    # Average durations
    avg_durations = {}
    for company in company_durations:
        avg = company_durations[company] / company_counts[company]
        avg_durations[company] = round(avg, 2)

    return avg_durations

# Dijkstra's for best(shortest) option ---
def dijkstra_company_choice(start_zone, end_zone, avg_durations):
    graph = {
        start_zone: [(end_zone, duration, company) for company, duration in avg_durations.items()]
    }

    #Min-heap: (duration, to, company)
    pq = [(duration, to_zone, company) for to_zone, duration, company in graph[start_zone]]
    heapq.heapify(pq)

    if pq:
        best_duration, destination, best_company = heapq.heappop(pq)
        return {
            "company": best_company,
            "duration": best_duration,
            "from": start_zone,
            "to": destination
        }
    else:
        return None

# check so that matches doesnt give error with refreshed kernel or 0 match case
if not matches or len(matches) == 0:
    print(f"\n No valid trips found from {start_zone} to {end_zone}.")
else:
    # avg total times
    avg_times = compute_average_durations(matches)

    # shortest
    ranked = sorted(avg_times.items(), key=lambda x: x[1])  # (company, avg_time)

    # show
    print(f"\nCompanies offering trips from {start_zone} to {end_zone} (ranked by avg time):\n")
    for i, (company, avg_time) in enumerate(ranked, 1):
        print(f"{i}. {company:} → {avg_time} minutes")
