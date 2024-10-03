# taxi-data-ingestion 0.1.0
## Introduction
A sbt spark-scala data-ingestion implementation to 
1. load Taxi-trip datasets
2. perform cleaning steps,
3. enrich the data at hand using a provdied Taxi-zone dataset
4. and store the output enriched trip data in Parquet format in s3 provided bucket.

### Documentation - Explanation:

 After receiving the execution parameters from the user, Taxi Trips and Taxi Zones data are loaded into DataFrames in spark using the 
provided credentials. Zones data are loaded using .json() method and necessary columns for enrichment are retrieved. The trip dataset is
then cleaned by performing a series of reasonable tests and augmented by applying a defined schema. Trips dataframe is
further enriched through, first a join with the zone data on Pickup Location id to retrieve information about the Pickup location and then
on Dropoff location accordingly. 
    
Pickup location join seems to be of more importance since the aim of the project is to 
suggest a new pickup location to optimize drivers profits. The smaller Zones Dataframe is broadcasted in order to be available
at all workers during joining.
Finally, enriched dataframe is persisted as parquet for efficient storage and querying.

## Build,
```sbt +clean +package```

## Usage
| Parameter            | Type    | Default Value                                                          | Description                               |
|----------------------|---------|------------------------------------------------------------------------|-------------------------------------------|
| `--s3_path`          | String  | `"s3a://mobito-de-take-home-assignment/input_data/"`                   | S3 bucket path.                           |
| `--input_data_file`  | String  | `"2017_Yellow_Taxi_Trip_Data.csv"`                                     | File containing taxi trip data to ingest. |
| `--load_all_trips`   | Boolean | `false`                                                                | Load all trip csv files if enabled.       |
| `--enrichment_file`  | String  | `"NYC_Taxi_Zones.geojson"`                                             | GeoJson file containing enrichment data.  |
| `--partition_column` | String  | `"date"`                                                               | Partition Column(s), comma-separated.     |
| `--overwrite`        | Boolean | `false`                                                                | Overwrite output parquet.                 |
| `--limit`            | Int     | `null`                                                                 | Limit the trip data to parse if provided  |
| `--output_path`      | Double  | `"s3a://mobito-de-take-home-assignment/candidates/minas_kalosynakis/"` | Full Output path.                         |

### Run in Intellij terminal:
Requirements: spark-submit local installation
```
spark-submit 
 --master <master-url>
 --conf "spark.hadoop.fs.s3a.access.key=<access_key>"
 --conf "spark.hadoop.fs.s3a.secret.key=<secret_key>"
 --class com.mobito.bdi.TaxiDataIngestion 
 --packages org.rogach:scallop_2.12:3.0.3,org.apache.hadoop:hadoop-aws:3.2.0
 target\scala-2.12\taxi-data-ingestion_2.12-0.1.0.jar --overwrite 
```

### Example Usage
```
spark-submit
--master "local[*]"
 --conf "spark.hadoop.fs.s3a.access.key=<access_key>"
 --conf "spark.hadoop.fs.s3a.secret.key=<secret_key>"
--class com.mobito.bdi.TaxiDataIngestion
--packages org.rogach:scallop_2.12:3.0.3,org.apache.hadoop:hadoop-aws:3.2.0
target\scala-2.12\taxi-data-ingestion_2.12-0.1.0.jar --overwrite --output_path "C:\\local\\output\\path\\" --limit 400
```

### Alternative - Run in linux environment (untested):
Modify script variables as necessary:

 ```./taxi_data_ingest.sh --overwrite```

### Notes:
* Current parquet output is written in folders such as '<partition_column>=<partition>', e.g. `date=20240501` or 
`pickup_borough=Queens`. In a production solution, the sub-folders could be named `/2024/05/01`, or `/Queens` but that
would require higher permission access to the s3 bucket to modify the output partitions to such formats.
* Project was tested to write locally by providing `--output_path` due to error during persisting output dataframes in 
s3 bucket probably due to insufficient permissions
* Attempted to use GeoSpark/Sedona library to properly read and handle geoJson coordinates field as an object but encountered 
difficulty when reading .format("geojson").
* Tested the project locally, and due to limited resources, included the `--limit` parameter to only ingest a sample of 
Taxi Trips dataset.
  




