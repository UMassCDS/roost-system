import csv
import os
import numpy as np

windfarm_database = "./uswtdb_v1_3_20190107.csv"
wind_farm_coors = []
if not os.path.exists(windfarm_database):
    print("Cannot find the wind farm database.")
    sys.exit(1)
with open(windfarm_database, "r") as f:
    reader = csv.reader(f)
    wind_farm_list = list(reader)
for entity in wind_farm_list[1:]:
    lon = float(entity[-2])
    lat = float(entity[-1])
    wind_farm_coors.append((lat, lon))

print(len(wind_farm_coors))
np.savez_compressed("wind_farm_database.npz", coordinates=np.array(wind_farm_coors))
