# rideServiceComparison
Our project aims to give users a way to pick the perfect ride service for travelling in New York City. The four main services available within NYC are Juno, Via, Lyft and Uber. Our project utlizes efficent search algorithms and a public FHV dataset to compare the ride services across 1.8 million rides across 263 NYC taxi zones. 

How the code works:
The user is first prompted to pick the FROM and TO locations for their trip. 
If there is any ambiguity with the user's response, they will be prompted to specify their locations. I.E. If the user picks Astoria as their location, there are 3 locations with similar names (Astoria, Astoria Park, Old Astoria), so the user will be asked to specify the location from the given list. 
After the 2 locations are properly selected, the program searches through the dataset and outputs the average time and money for the trip for each organization. Ultimately, the user will be able to pick the best ride service for them based on quickest travel time or lowest price for the trip.



To be able to run the code: The program uses two different datasets. The first for the NYC taxi zones is already uploaded to this repository. The second dataset, however, is too large to upload. So here is how to download it. 
https://www.kaggle.com/datasets/jeffsinsel/nyc-fhvhv-data?select=taxi_zones
This link will take you to the kaggle website which has the data set. On the website, scroll down till you see the list of files under Data Explorer. Scroll down in the list, click 21 more, find the fhvhv_tripdata_2022-11.parquet file. Download this file and place it inside the project folder. Nothing in the code needs to be changed, unless there is an issue with the path of the file, in which case change the path of the file to match where the file is in the code. 


Libraries needed for code:
Ensure the following libraries are installed. Pandas, heapq, datetime, geopandas. 
