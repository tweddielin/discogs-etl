from discogs_etl.etl import process_xml_to_parquet_s3


if __name__ == "__main__":
    
    # discogs_artist_20180101 = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2018/discogs_20180101_artists.xml.gz"
    # artist_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2008/discogs_20080309_artists.xml.gz"
    artist_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2019/discogs_20190501_artists.xml.gz"
    # data_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2008/discogs_20080309_releases.xml.gz"
    process_xml_to_parquet_s3(
        input_file=artist_url,
        bucket_name="discogs-data",
        chunk_size=3000,
    )