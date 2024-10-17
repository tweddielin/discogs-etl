# Discogs-ETL

## S3 Structure

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