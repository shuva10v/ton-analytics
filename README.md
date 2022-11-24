# Ton Analytics code

Various code for TON blockchain analysis.

## [ETL](./etl)

Initial blockchain data extraction is performed with [ton-indexer](https://github.com/toncenter/ton-indexer). PostgreSQL
is not good enough for huge analytical workload so data loaded into S3-based datalake. Airflow is used for incremental 
ETL process:
* **E**: extract data from psql sliced by date intervals
* **T**: convert results to parquet file format
* **L**: load it into S3 object storage

Datalake tables:

| Table        | Prefix                                |
|--------------|---------------------------------------|
| accounts     | dwh/staging/accounts/date=YYYYMM/     |
| transactions | dwh/staging/transactions/date=YYYYMM/ |
| messages     | dwh/staging/messages/date=YYYYMM/     |

After each incremental upload log entry created on psql table ``increment_state```

## [Parsers](./parsers)

Smart-contract data/messages parser for extracting meaningful data, for example, NFT owners.
