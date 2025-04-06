# Utilities

Various utilities and scripts

### Data Setup Utility

This utility helps in setting up data for Parquet, Iceberg and Delta tables.

#### Give input in `trino_input.csv`

#### Then Execute `python TrinoQueryGen.py`. This will generate `output_data.csv` which will queries to run.

#### Then Execute `python ExecuteQueryOnTrino.py`. This will execute those queries on the trino setup.

#### Now data should be generated.


### Trino Docker Package
This package contains docker compose file for setting local data lake using `Trino`, `Minio` and `Hive Metastore`. It also uses `postgres` as database for metastore.
Two handy scripts are provided to start and stop dockerized trino setup.

#### `start_trino.sh` - This setups trino + Minio + Hive Metastore cluster. 
#### `stop_trino.sh` - This destroys above created setup.

