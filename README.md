# Discogs-ETL
A tool to download and parse the discogs data dump from https://discogs-data-dumps.s3.us-west-2.amazonaws.com, convert to parquet file and store on S3. 

## AWS Credential Setup

* Set up the AWS credential in `~/.aws/config` and/or as the environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`. 
* S3 strucutre is like this:

```
s3://your-bucket-name/
│
├── artists/
│   ├── year=2017/
│   │   ├── month=01/
│   │   │   └── artists_20170101.parquet
│   │   ├── month=02/
│   │   │   └── artists_20170201.parquet
│   │   └── ...
│   └── ...
│
├── labels/
│   ├── year=2017/
│   │   ├── month=01/
│   │   │   └── labels_20170101.parquet
│   │   ├── month=02/
│   │   │   └── labels_20170201.parquet
│   │   └── ...
│   └── ...
│
├── masters/
│   ├── year=2017/
│   │   ├── month=01/
│   │   │   └── masters_20170101.parquet
│   │   ├── month=02/
│   │   │   └── masters_20170201.parquet
│   │   └── ...
│   └── ...
│
└── releases/
    ├── year=2017/
    │   ├── month=01/
    │   │   └── releases_20170101.parquet
    │   ├── month=02/
    │   │   └── releases_20170201.parquet
    │   └── ...
    └── ...
```

## Usage

### Running the ETL
```python
from discogs_etl.etl import stream_xml_to_parquet_s3
from discogs_etl.s3 import list_s3_files, organize_discogs_files
import os

year = 2019
discogs_bucket_name = "discogs-data-dumps"
prefix = f"data/{year}"
base_url = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com"
yearly_list = organize_discogs_files(
    file_list=list_s3_files(bucket_name=discogs_bucket_name, prefix=prefix),
    base_url=base_url,
)
for monthly_dump in yearly_list:
    print(f"Monthly Dump: {monthly_dump.pop('year_month')}")
    for data_type, value in monthly_dump.items():
        url = monthly_dump[data_type]['url']
        checksum = monthly_dump[data_type]['checksum']
        date = monthly_dump[data_type]['date']
        print(f"Currently Processing: {url} | Date: {date}")
        stream_xml_to_parquet_s3(
            input_file=url,
            bucket_name="discogs-data",
            checksum=checksum,
            chunk_size=5000,
            download_chunk_size=1024*1024*10,
        )
```

### Do Some Analytics with duckdb

```sql
D SELECT *
  FROM read_parquet('s3://discogs-data/releases/year=*/month=*/*.parquet')
  WHERE array_contains(genres, 'Jazz') and len(genres) > 1
  LIMIT 20;
┌───────┬──────────┬──────────────────────┬─────────────────┬───┬──────────────────────┬──────────────────────┬─────────┬───────┐
│  id   │  status  │        title         │     country     │ … │        genres        │        styles        │  month  │ year  │
│ int64 │ varchar  │       varchar        │     varchar     │   │      varchar[]       │      varchar[]       │ varchar │ int64 │
├───────┼──────────┼──────────────────────┼─────────────────┼───┼──────────────────────┼──────────────────────┼─────────┼───────┤
│   377 │ Accepted │ Mike & Rich          │ UK              │ … │ [Electronic, Jazz]   │ [Lounge, Abstract,…  │ 09      │  2009 │
│   430 │ Accepted │ Somehow, Somewhere…  │ US              │ … │ [Electronic, Jazz]   │ [Jazz-Funk, Deep H…  │ 09      │  2009 │
│   560 │ Accepted │ Espaces Baroques     │ France          │ … │ [Electronic, Jazz]   │ [Future Jazz, Cont…  │ 09      │  2009 │
│  1147 │ Accepted │ Substance            │ UK              │ … │ [Electronic, Jazz]   │ [Abstract, Future …  │ 09      │  2009 │
│  1225 │ Accepted │ Jazzanova EP         │ Germany         │ … │ [Electronic, Jazz,…  │ [Bossanova, Future…  │ 09      │  2009 │
│  1226 │ Accepted │ Tourist              │ Europe          │ … │ [Electronic, Jazz]   │ [Deep House, Futur…  │ 09      │  2009 │
│  1255 │ Accepted │ Jam On The Beat      │ US              │ … │ [Electronic, Jazz]   │ [Soul-Jazz, Deep H…  │ 09      │  2009 │
│  1259 │ Accepted │ Music                │ US              │ … │ [Electronic, Jazz]   │ [Jazz-Funk, Deep H…  │ 09      │  2009 │
│  1265 │ Accepted │ Red Hook Project 1   │ US              │ … │ [Electronic, Jazz]   │ [Future Jazz, Down…  │ 09      │  2009 │
│  1324 │ Accepted │ Music Is Rotted On…  │ UK              │ … │ [Electronic, Jazz]   │ [Fusion, IDM]        │ 09      │  2009 │
│  1729 │ Accepted │ Sacrebleu            │ France          │ … │ [Electronic, Jazz]   │ [House, Downtempo,…  │ 09      │  2009 │
│  2113 │ Accepted │ Nation 2 Nation      │ US              │ … │ [Electronic, Jazz]   │ [House, Smooth Jaz…  │ 09      │  2009 │
│  2455 │ Accepted │ Post                 │ US              │ … │ [Electronic, Jazz]   │ [Progressive House…  │ 09      │  2009 │
│  2795 │ Accepted │ DJ-Kicks             │ Germany         │ … │ [Electronic, Jazz]   │ [Dub, Future Jazz,…  │ 09      │  2009 │
│  2820 │ Accepted │ Tourist              │ UK, Europe & US │ … │ [Electronic, Jazz]   │ [House, Future Jaz…  │ 09      │  2009 │
│  3007 │ Accepted │ Presents A Tribute…  │ US              │ … │ [Electronic, Jazz]   │ [Deep House, Afrob…  │ 09      │  2009 │
│  3179 │ Accepted │ Modulor Mix          │ UK              │ … │ [Electronic, Jazz]   │ [Downtempo]          │ 09      │  2009 │
│  3249 │ Accepted │ After Hours Volume…  │ US              │ … │ [Electronic, Jazz]   │ [Acid Jazz, Downte…  │ 09      │  2009 │
│  3333 │ Accepted │ Groove Collective    │ US              │ … │ [Electronic, Jazz]   │ [Acid Jazz]          │ 09      │  2009 │
│  3477 │ Accepted │ The Mirror Conspir…  │ US              │ … │ [Electronic, Jazz]   │ [Bossa Nova, Dub, …  │ 09      │  2009 │
├───────┴──────────┴──────────────────────┴─────────────────┴───┴──────────────────────┴──────────────────────┴─────────┴───────┤
│ 20 rows                                                                                                  14 columns (8 shown) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

