from discogs_etl.etl import process_xml_to_parquet_s3, stream_xml_to_parquet_s3


if __name__ == "__main__":
    
    # discogs_artist_20180101 = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2018/discogs_20180101_artists.xml.gz"
    # artist_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2008/discogs_20080309_artists.xml.gz"
    artist_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2019/discogs_20190501_artists.xml.gz"
    label_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2008/discogs_20080309_labels.xml.gz"
    label201201_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2012/discogs_20120101_labels.xml.gz"
    master_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2016/discogs_20161001_masters.xml.gz"
    release_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2010/discogs_20100902_releases.xml.gz"
    local_release_url = "/Users/tweddielin/Downloads/discogs_20100902_releases.xml.gz"
    # data_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2008/discogs_20080309_releases.xml.gz"
    
    # process_xml_to_parquet_s3(
    #     input_file=label201201_url,
    #     bucket_name="discogs-data",
    #     chunk_size=3000,
    #     download_chunk_size=1024*1024*4 # ~4MB,
    # )

    stream_xml_to_parquet_s3(
        input_file=local_release_url,
        bucket_name="discogs-data",
        chunk_size=1000,
        download_chunk_size=1024*1024*4 # ~4MB,
    )
