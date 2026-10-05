# Formula 1 Lakehouse Data Dictionary

## 1. Silver Layer Tables

### `silver.circuits` (Dimension)
| Column Name | Data Type | Nullable | Primary Key | Description |
| :--- | :--- | :--- | :--- | :--- |
| `circuit_id` | INT | No | Yes | Unique circuit identifier |
| `circuit_ref` | STRING | No | No | Natural key string |
| `circuit_name` | STRING | No | No | Official circuit name |
| `location` | STRING | Yes | No | City / region |
| `country` | STRING | No | No | Country name |
| `latitude` | DOUBLE | Yes | No | GPS latitude |
| `longitude` | DOUBLE | Yes | No | GPS longitude |
| `altitude` | INT | Yes | No | Altitude above sea level in meters |
| `url` | STRING | Yes | No | Wikipedia URL |
| `ingestion_date` | TIMESTAMP | No | No | Record transformation timestamp |

### `silver.races` (Dimension)
| Column Name | Data Type | Nullable | Primary Key | Description |
| :--- | :--- | :--- | :--- | :--- |
| `race_id` | INT | No | Yes | Unique race event identifier |
| `race_year` | INT | No | No | Season year |
| `round` | INT | No | No | Calendar round number |
| `circuit_id` | INT | No | FK | Foreign key to `silver.circuits` |
| `race_name` | STRING | No | No | Official Grand Prix name |
| `race_date` | STRING | No | No | Race date (YYYY-MM-DD) |
| `race_time` | STRING | Yes | No | Race start time |
| `race_timestamp` | TIMESTAMP | Yes | No | Combined race start timestamp |
| `url` | STRING | Yes | No | Wikipedia URL |
| `ingestion_date` | TIMESTAMP | No | No | Record transformation timestamp |

### `silver.drivers` (Dimension)
| Column Name | Data Type | Nullable | Primary Key | Description |
| :--- | :--- | :--- | :--- | :--- |
| `driver_id` | INT | No | Yes | Unique driver identifier |
| `driver_ref` | STRING | No | No | Natural driver key |
| `number` | INT | Yes | No | Permanent racing number |
| `code` | STRING | Yes | No | 3-letter broadcast code (e.g. HAM, VER) |
| `forename` | STRING | No | No | Driver first name |
| `surname` | STRING | No | No | Driver last name |
| `full_name` | STRING | No | No | Concatenated full name |
| `dob` | DATE | Yes | No | Date of birth (PII protected by Unity Catalog mask) |
| `nationality` | STRING | Yes | No | Driver nationality |
| `url` | STRING | Yes | No | Driver bio URL |

### `silver.results` (Fact Table)
| Column Name | Data Type | Nullable | Primary Key | Description |
| :--- | :--- | :--- | :--- | :--- |
| `result_id` | INT | No | Yes | Unique result record identifier |
| `race_id` | INT | No | FK | Foreign key to `silver.races` |
| `driver_id` | INT | No | FK | Foreign key to `silver.drivers` |
| `constructor_id` | INT | No | FK | Foreign key to `silver.constructors` |
| `grid` | INT | Yes | No | Starting grid position |
| `position` | INT | Yes | No | Final classified finishing position |
| `position_text` | STRING | Yes | No | Classification text ('1', '2', 'R', 'D') |
| `points` | DOUBLE | No | No | Championship points awarded |
| `laps` | INT | No | No | Total completed laps |
| `time` | STRING | Yes | No | Finishing time delta |
| `milliseconds` | INT | Yes | No | Finishing time in milliseconds |
| `fastest_lap` | INT | Yes | No | Lap number of driver's fastest lap |
| `rank` | INT | Yes | No | Fastest lap rank in race |
| `fastest_lap_speed` | DOUBLE | Yes | No | Fastest lap average speed (km/h) |
| `record_hash` | STRING | No | No | SHA-256 hash for Delta MERGE change detection |
| `create_date` | TIMESTAMP | No | No | Initial insertion timestamp |
| `update_date` | TIMESTAMP | No | No | Last MERGE update timestamp |

---

## 2. Gold Analytical Marts

### `gold.driver_standings`
- **Partition Key**: `race_year`
- **Granularity**: `race_year`, `driver_id`
- **Key Columns**: `race_year`, `driver_id`, `full_name`, `driver_nationality`, `team_name`, `total_points`, `wins`, `podiums`, `races_entered`, `rank`.

### `gold.constructor_standings`
- **Partition Key**: `race_year`
- **Granularity**: `race_year`, `constructor_id`
- **Key Columns**: `race_year`, `constructor_id`, `team_name`, `team_nationality`, `total_points`, `wins`, `podiums`, `rank`.
