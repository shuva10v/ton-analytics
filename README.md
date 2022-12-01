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

Smart-contract data/messages parser for extracting meaningful data, for example, NFT owners. Example:
```shell
POSTGRES_URI=postgres://postgres:pass@localhost:5432/ton_index \
  python3 contracts_parser.py -c jetton -d cache/ -e http://localhost:9090/execute \
  jetton_contract_data.csv jetton_parsed.csv
```

Supported contract types:
* DNS contract ([TEP-81](https://github.com/ton-blockchain/TEPs/blob/master/text/0081-dns-standard.md))
* Jetton master contract ([TEP-74](https://github.com/ton-blockchain/TEPs/blob/master/text/0074-jettons-standard.md))
with [TEP-64](https://github.com/ton-blockchain/TEPs/blob/master/text/0064-token-data-standard.md) support

## Utils

* [contracts_executor](./utils/contracts_executor) - wrapper around [ton-contract-executor](https://github.com/ton-community/ton-contract-executor)
for executing arbitrary get methods on contracts providing code and data cells. Used in parser to extract information.

## Analysis

Some analysis scripts:
* [TON_events_plot.ipynb](analysis/TON_events_plot.ipynb) - plot gantt chart visualising TON scam (for [this research](https://telegra.ph/Analiz-skama-v-TON-11-25))
