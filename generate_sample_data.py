"""
Generates rich, authentic Formula 1 sample datasets across all 8 required formats:
1. circuits: CSV
2. races: CSV
3. constructors: single-line JSON (json lines)
4. drivers: nested JSON
5. results: single-line JSON (json lines)
6. pitstops: multi-line formatted JSON
7. laptimes: split CSV files (lap_times_split_1.csv, lap_times_split_2.csv)
8. qualifying: split multi-line JSON files (qualifying_split_1.json, qualifying_split_2.json)
"""

import os
import json
import csv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DIR = os.path.join(BASE_DIR, "data", "sample")

os.makedirs(os.path.join(SAMPLE_DIR, "lap_times"), exist_ok=True)
os.makedirs(os.path.join(SAMPLE_DIR, "qualifying"), exist_ok=True)

# 1. Circuits (CSV)
circuits_data = [
    {"circuitId": 1, "circuitRef": "silverstone", "name": "Silverstone Circuit", "location": "Silverstone", "country": "UK", "lat": 52.0786, "lng": -1.01694, "alt": 153, "url": "https://en.wikipedia.org/wiki/Silverstone_Circuit"},
    {"circuitId": 2, "circuitRef": "monza", "name": "Autodromo Nazionale di Monza", "location": "Monza", "country": "Italy", "lat": 45.6156, "lng": 9.28111, "alt": 162, "url": "https://en.wikipedia.org/wiki/Autodromo_Nazionale_di_Monza"},
    {"circuitId": 3, "circuitRef": "bahrain", "name": "Bahrain International Circuit", "location": "Sakhir", "country": "Bahrain", "lat": 26.0325, "lng": 50.5106, "alt": 7, "url": "https://en.wikipedia.org/wiki/Bahrain_International_Circuit"},
    {"circuitId": 4, "circuitRef": "yas_marina", "name": "Yas Marina Circuit", "location": "Abu Dhabi", "country": "UAE", "lat": 24.4672, "lng": 54.6031, "alt": 3, "url": "https://en.wikipedia.org/wiki/Yas_Marina_Circuit"},
    {"circuitId": 5, "circuitRef": "spa", "name": "Circuit de Spa-Francorchamps", "location": "Spa", "country": "Belgium", "lat": 50.4372, "lng": 5.97139, "alt": 401, "url": "https://en.wikipedia.org/wiki/Circuit_de_Spa-Francorchamps"}
]

with open(os.path.join(SAMPLE_DIR, "circuits.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(circuits_data[0].keys()))
    writer.writeheader()
    writer.writerows(circuits_data)

# 2. Races (CSV)
races_data = [
    {"raceId": 1052, "year": 2021, "round": 1, "circuitId": 3, "name": "Bahrain Grand Prix", "date": "2021-03-28", "time": "15:00:00", "url": "https://en.wikipedia.org/wiki/2021_Bahrain_Grand_Prix"},
    {"raceId": 1061, "year": 2021, "round": 10, "circuitId": 1, "name": "British Grand Prix", "date": "2021-07-18", "time": "14:00:00", "url": "https://en.wikipedia.org/wiki/2021_British_Grand_Prix"},
    {"raceId": 1065, "year": 2021, "round": 14, "circuitId": 2, "name": "Italian Grand Prix", "date": "2021-09-12", "time": "13:00:00", "url": "https://en.wikipedia.org/wiki/2021_Italian_Grand_Prix"},
    {"raceId": 1073, "year": 2021, "round": 22, "circuitId": 4, "name": "Abu Dhabi Grand Prix", "date": "2021-12-12", "time": "13:00:00", "url": "https://en.wikipedia.org/wiki/2021_Abu_Dhabi_Grand_Prix"},
    {"raceId": 1074, "year": 2022, "round": 1, "circuitId": 3, "name": "Bahrain Grand Prix", "date": "2022-03-20", "time": "15:00:00", "url": "https://en.wikipedia.org/wiki/2022_Bahrain_Grand_Prix"},
    {"raceId": 1084, "year": 2022, "round": 10, "circuitId": 1, "name": "British Grand Prix", "date": "2022-07-03", "time": "14:00:00", "url": "https://en.wikipedia.org/wiki/2022_British_Grand_Prix"}
]

with open(os.path.join(SAMPLE_DIR, "races.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(races_data[0].keys()))
    writer.writeheader()
    writer.writerows(races_data)

# 3. Constructors (single-line JSON / JSON Lines)
constructors_data = [
    {"constructorId": 1, "constructorRef": "mclaren", "name": "McLaren", "nationality": "British", "url": "https://en.wikipedia.org/wiki/McLaren"},
    {"constructorId": 6, "constructorRef": "ferrari", "name": "Ferrari", "nationality": "Italian", "url": "https://en.wikipedia.org/wiki/Scuderia_Ferrari"},
    {"constructorId": 9, "constructorRef": "red_bull", "name": "Red Bull", "nationality": "Austrian", "url": "https://en.wikipedia.org/wiki/Red_Bull_Racing"},
    {"constructorId": 131, "constructorRef": "mercedes", "name": "Mercedes", "nationality": "German", "url": "https://en.wikipedia.org/wiki/Mercedes-Benz_in_Formula_One"},
    {"constructorId": 117, "constructorRef": "aston_martin", "name": "Aston Martin", "nationality": "British", "url": "https://en.wikipedia.org/wiki/Aston_Martin_in_Formula_One"}
]

with open(os.path.join(SAMPLE_DIR, "constructors.json"), "w", encoding="utf-8") as f:
    for item in constructors_data:
        f.write(json.dumps(item) + "\n")

# 4. Drivers (nested JSON - object with "name": {"forename": "...", "surname": "..."})
drivers_data = [
    {"driverId": 1, "driverRef": "hamilton", "number": 44, "code": "HAM", "name": {"forename": "Lewis", "surname": "Hamilton"}, "dob": "1985-01-07", "nationality": "British", "url": "https://en.wikipedia.org/wiki/Lewis_Hamilton"},
    {"driverId": 830, "driverRef": "max_verstappen", "number": 33, "code": "VER", "name": {"forename": "Max", "surname": "Verstappen"}, "dob": "1997-09-30", "nationality": "Dutch", "url": "https://en.wikipedia.org/wiki/Max_Verstappen"},
    {"driverId": 844, "driverRef": "leclerc", "number": 16, "code": "LEC", "name": {"forename": "Charles", "surname": "Leclerc"}, "dob": "1997-10-16", "nationality": "Monegasque", "url": "https://en.wikipedia.org/wiki/Charles_Leclerc"},
    {"driverId": 846, "driverRef": "norris", "number": 4, "code": "NOR", "name": {"forename": "Lando", "surname": "Norris"}, "dob": "1999-11-13", "nationality": "British", "url": "https://en.wikipedia.org/wiki/Lando_Norris"},
    {"driverId": 832, "driverRef": "sainz", "number": 55, "code": "SAI", "name": {"forename": "Carlos", "surname": "Sainz"}, "dob": "1994-09-01", "nationality": "Spanish", "url": "https://en.wikipedia.org/wiki/Carlos_Sainz_Jr."},
    {"driverId": 815, "driverRef": "perez", "number": 11, "code": "PER", "name": {"forename": "Sergio", "surname": "Perez"}, "dob": "1990-01-26", "nationality": "Mexican", "url": "https://en.wikipedia.org/wiki/Sergio_P%C3%A9rez"},
    {"driverId": 4, "driverRef": "alonso", "number": 14, "code": "ALO", "name": {"forename": "Fernando", "surname": "Alonso"}, "dob": "1981-07-29", "nationality": "Spanish", "url": "https://en.wikipedia.org/wiki/Fernando_Alonso"},
    {"driverId": 847, "driverRef": "russell", "number": 63, "code": "RUS", "name": {"forename": "George", "surname": "Russell"}, "dob": "1998-02-15", "nationality": "British", "url": "https://en.wikipedia.org/wiki/George_Russell"}
]

with open(os.path.join(SAMPLE_DIR, "drivers.json"), "w", encoding="utf-8") as f:
    for item in drivers_data:
        f.write(json.dumps(item) + "\n")

# 5. Results (single-line JSON / JSON Lines - FACT table)
results_data = [
    # 2021 Bahrain (1052)
    {"resultId": 25001, "raceId": 1052, "driverId": 1, "constructorId": 131, "number": 44, "grid": 2, "position": 1, "positionText": "1", "positionOrder": 1, "points": 25.0, "laps": 56, "time": "1:32:03.897", "milliseconds": 5523897, "fastestLap": 44, "rank": 2, "fastestLapTime": "1:34.015", "fastestLapSpeed": "207.235", "statusId": 1},
    {"resultId": 25002, "raceId": 1052, "driverId": 830, "constructorId": 9, "number": 33, "grid": 1, "position": 2, "positionText": "2", "positionOrder": 2, "points": 18.0, "laps": 56, "time": "+0.745", "milliseconds": 5524642, "fastestLap": 41, "rank": 3, "fastestLapTime": "1:34.090", "fastestLapSpeed": "207.070", "statusId": 1},
    {"resultId": 25003, "raceId": 1052, "driverId": 846, "constructorId": 1, "number": 4, "grid": 7, "position": 4, "positionText": "4", "positionOrder": 4, "points": 12.0, "laps": 56, "time": "+46.487", "milliseconds": 5570384, "fastestLap": 38, "rank": 5, "fastestLapTime": "1:34.396", "fastestLapSpeed": "206.398", "statusId": 1},
    {"resultId": 25004, "raceId": 1052, "driverId": 844, "constructorId": 6, "number": 16, "grid": 4, "position": 6, "positionText": "6", "positionOrder": 6, "points": 8.0, "laps": 56, "time": "+59.902", "milliseconds": 5583799, "fastestLap": 39, "rank": 7, "fastestLapTime": "1:34.988", "fastestLapSpeed": "205.112", "statusId": 1},

    # 2021 British GP (1061)
    {"resultId": 25101, "raceId": 1061, "driverId": 1, "constructorId": 131, "number": 44, "grid": 2, "position": 1, "positionText": "1", "positionOrder": 1, "points": 25.0, "laps": 52, "time": "1:58:23.284", "milliseconds": 7103284, "fastestLap": 45, "rank": 2, "fastestLapTime": "1:29.699", "fastestLapSpeed": "236.430", "statusId": 1},
    {"resultId": 25102, "raceId": 1061, "driverId": 844, "constructorId": 6, "number": 16, "grid": 4, "position": 2, "positionText": "2", "positionOrder": 2, "points": 18.0, "laps": 52, "time": "+3.871", "milliseconds": 7107155, "fastestLap": 48, "rank": 4, "fastestLapTime": "1:30.556", "fastestLapSpeed": "234.193", "statusId": 1},
    {"resultId": 25103, "raceId": 1061, "driverId": 846, "constructorId": 1, "number": 4, "grid": 5, "position": 4, "positionText": "4", "positionOrder": 4, "points": 12.0, "laps": 52, "time": "+28.573", "milliseconds": 7131857, "fastestLap": 43, "rank": 5, "fastestLapTime": "1:30.711", "fastestLapSpeed": "233.793", "statusId": 1},
    {"resultId": 25104, "raceId": 1061, "driverId": 830, "constructorId": 9, "number": 33, "grid": 1, "position": None, "positionText": "R", "positionOrder": 20, "points": 0.0, "laps": 0, "time": None, "milliseconds": None, "fastestLap": None, "rank": None, "fastestLapTime": None, "fastestLapSpeed": None, "statusId": 4},

    # 2021 Abu Dhabi (1073)
    {"resultId": 25201, "raceId": 1073, "driverId": 830, "constructorId": 9, "number": 33, "grid": 1, "position": 1, "positionText": "1", "positionOrder": 1, "points": 26.0, "laps": 58, "time": "1:30:17.345", "milliseconds": 5417345, "fastestLap": 39, "rank": 1, "fastestLapTime": "1:26.103", "fastestLapSpeed": "220.800", "statusId": 1},
    {"resultId": 25202, "raceId": 1073, "driverId": 1, "constructorId": 131, "number": 44, "grid": 2, "position": 2, "positionText": "2", "positionOrder": 2, "points": 18.0, "laps": 58, "time": "+2.256", "milliseconds": 5419601, "fastestLap": 43, "rank": 2, "fastestLapTime": "1:26.615", "fastestLapSpeed": "219.495", "statusId": 1},
    {"resultId": 25203, "raceId": 1073, "driverId": 832, "constructorId": 6, "number": 55, "grid": 5, "position": 3, "positionText": "3", "positionOrder": 3, "points": 15.0, "laps": 58, "time": "+5.173", "milliseconds": 5422518, "fastestLap": 48, "rank": 4, "fastestLapTime": "1:27.420", "fastestLapSpeed": "217.474", "statusId": 1},

    # 2022 Bahrain (1074)
    {"resultId": 25301, "raceId": 1074, "driverId": 844, "constructorId": 6, "number": 16, "grid": 1, "position": 1, "positionText": "1", "positionOrder": 1, "points": 26.0, "laps": 57, "time": "1:37:33.584", "milliseconds": 5853584, "fastestLap": 51, "rank": 1, "fastestLapTime": "1:34.570", "fastestLapSpeed": "206.018", "statusId": 1},
    {"resultId": 25302, "raceId": 1074, "driverId": 832, "constructorId": 6, "number": 55, "grid": 3, "position": 2, "positionText": "2", "positionOrder": 2, "points": 18.0, "laps": 57, "time": "+5.598", "milliseconds": 5859182, "fastestLap": 52, "rank": 3, "fastestLapTime": "1:35.740", "fastestLapSpeed": "203.499", "statusId": 1},
    {"resultId": 25303, "raceId": 1074, "driverId": 1, "constructorId": 131, "number": 44, "grid": 5, "position": 3, "positionText": "3", "positionOrder": 3, "points": 15.0, "laps": 57, "time": "+9.675", "milliseconds": 5863259, "fastestLap": 53, "rank": 4, "fastestLapTime": "1:36.241", "fastestLapSpeed": "202.439", "statusId": 1},
    {"resultId": 25304, "raceId": 1074, "driverId": 830, "constructorId": 9, "number": 33, "grid": 2, "position": None, "positionText": "R", "positionOrder": 19, "points": 0.0, "laps": 54, "time": None, "milliseconds": None, "fastestLap": 51, "rank": 2, "fastestLapTime": "1:35.440", "fastestLapSpeed": "204.138", "statusId": 31}
]

with open(os.path.join(SAMPLE_DIR, "results.json"), "w", encoding="utf-8") as f:
    for item in results_data:
        f.write(json.dumps(item) + "\n")

# 6. Pit Stops (Multi-line formatted JSON array)
pitstops_data = [
    {"raceId": 1052, "driverId": 1, "stop": 1, "lap": 13, "time": "15:24:12", "duration": "24.120", "milliseconds": 24120},
    {"raceId": 1052, "driverId": 1, "stop": 2, "lap": 28, "time": "15:47:33", "duration": "24.580", "milliseconds": 24580},
    {"raceId": 1052, "driverId": 830, "stop": 1, "lap": 17, "time": "15:29:45", "duration": "23.950", "milliseconds": 23950},
    {"raceId": 1052, "driverId": 830, "stop": 2, "lap": 39, "time": "16:03:10", "duration": "23.810", "milliseconds": 23810},
    {"raceId": 1061, "driverId": 1, "stop": 1, "lap": 27, "time": "14:45:00", "duration": "34.200", "milliseconds": 34200},
    {"raceId": 1061, "driverId": 844, "stop": 1, "lap": 29, "time": "14:48:15", "duration": "24.350", "milliseconds": 24350},
    {"raceId": 1073, "driverId": 830, "stop": 1, "lap": 13, "time": "13:21:40", "duration": "23.700", "milliseconds": 23700},
    {"raceId": 1073, "driverId": 830, "stop": 2, "lap": 36, "time": "13:56:15", "duration": "23.650", "milliseconds": 23650},
    {"raceId": 1073, "driverId": 830, "stop": 3, "lap": 53, "time": "14:21:05", "duration": "23.200", "milliseconds": 23200},
    {"raceId": 1073, "driverId": 1, "stop": 1, "lap": 14, "time": "13:23:05", "duration": "23.900", "milliseconds": 23900}
]

with open(os.path.join(SAMPLE_DIR, "pit_stops.json"), "w", encoding="utf-8") as f:
    json.dump(pitstops_data, f, indent=2)

# 7. Lap Times (Split CSV files: lap_times_split_1.csv, lap_times_split_2.csv)
laptimes_part1 = [
    {"raceId": 1052, "driverId": 1, "lap": 1, "position": 2, "time": "1:36.500", "milliseconds": 96500},
    {"raceId": 1052, "driverId": 1, "lap": 2, "position": 2, "time": "1:35.400", "milliseconds": 95400},
    {"raceId": 1052, "driverId": 1, "lap": 3, "position": 2, "time": "1:35.250", "milliseconds": 95250},
    {"raceId": 1052, "driverId": 830, "lap": 1, "position": 1, "time": "1:36.100", "milliseconds": 96100},
    {"raceId": 1052, "driverId": 830, "lap": 2, "position": 1, "time": "1:35.300", "milliseconds": 95300},
    {"raceId": 1052, "driverId": 830, "lap": 3, "position": 1, "time": "1:35.150", "milliseconds": 95150}
]

laptimes_part2 = [
    {"raceId": 1073, "driverId": 1, "lap": 1, "position": 1, "time": "1:28.400", "milliseconds": 88400},
    {"raceId": 1073, "driverId": 1, "lap": 2, "position": 1, "time": "1:27.900", "milliseconds": 87900},
    {"raceId": 1073, "driverId": 830, "lap": 1, "position": 2, "time": "1:29.100", "milliseconds": 89100},
    {"raceId": 1073, "driverId": 830, "lap": 2, "position": 2, "time": "1:28.200", "milliseconds": 88200},
    {"raceId": 1073, "driverId": 830, "lap": 58, "position": 1, "time": "1:26.103", "milliseconds": 86103},
    {"raceId": 1073, "driverId": 1, "lap": 58, "position": 2, "time": "1:28.359", "milliseconds": 88359}
]

with open(os.path.join(SAMPLE_DIR, "lap_times", "lap_times_split_1.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(laptimes_part1[0].keys()))
    writer.writeheader()
    writer.writerows(laptimes_part1)

with open(os.path.join(SAMPLE_DIR, "lap_times", "lap_times_split_2.csv"), "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(laptimes_part2[0].keys()))
    writer.writeheader()
    writer.writerows(laptimes_part2)

# 8. Qualifying (Split Multi-line JSON files: qualifying_split_1.json, qualifying_split_2.json)
qualifying_part1 = [
    {"qualifyId": 8501, "raceId": 1052, "driverId": 830, "constructorId": 9, "number": 33, "position": 1, "q1": "1:30.499", "q2": "1:30.318", "q3": "1:28.997"},
    {"qualifyId": 8502, "raceId": 1052, "driverId": 1, "constructorId": 131, "number": 44, "position": 2, "q1": "1:30.617", "q2": "1:30.085", "q3": "1:29.385"},
    {"qualifyId": 8503, "raceId": 1052, "driverId": 844, "constructorId": 6, "number": 16, "position": 4, "q1": "1:30.691", "q2": "1:30.010", "q3": "1:29.549"}
]

qualifying_part2 = [
    {"qualifyId": 8601, "raceId": 1073, "driverId": 830, "constructorId": 9, "number": 33, "position": 1, "q1": "1:23.322", "q2": "1:22.800", "q3": "1:22.109"},
    {"qualifyId": 8602, "raceId": 1073, "driverId": 1, "constructorId": 131, "number": 44, "position": 2, "q1": "1:22.845", "q2": "1:23.145", "q3": "1:22.480"},
    {"qualifyId": 8603, "raceId": 1073, "driverId": 832, "constructorId": 6, "number": 55, "position": 5, "q1": "1:23.487", "q2": "1:23.174", "q3": "1:22.992"}
]

with open(os.path.join(SAMPLE_DIR, "qualifying", "qualifying_split_1.json"), "w", encoding="utf-8") as f:
    json.dump(qualifying_part1, f, indent=2)

with open(os.path.join(SAMPLE_DIR, "qualifying", "qualifying_split_2.json"), "w", encoding="utf-8") as f:
    json.dump(qualifying_part2, f, indent=2)

print(f"Sample F1 historical datasets successfully generated in {SAMPLE_DIR}")
