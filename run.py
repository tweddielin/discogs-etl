from discogs_etl.etl import process_xml_to_parquet_s3, stream_xml_to_parquet_s3
import os


def run():
    # discogs_artist_20180101 = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2018/discogs_20180101_artists.xml.gz"
    # artist_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2008/discogs_20080309_artists.xml.gz"
    artist_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2019/discogs_20190501_artists.xml.gz"
    label_url = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2008/discogs_20080309_labels.xml.gz"
    label_201009 = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2010/discogs_20100902_labels.xml.gz"
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
        input_file=release_url,
        bucket_name="discogs-data",
        chunk_size=1000,
        download_chunk_size=1024*1024*4 # ~4MB,
    )

def lambda_handler(event, context):
    # Get parameters from environment variables or event
    input_file = os.environ.get('INPUT_FILE') or event.get('input_file')
    bucket_name = os.environ.get('BUCKET_NAME') or event.get('bucket_name')
    chunk_size = int(os.environ.get('CHUNK_SIZE', 1000))
    download_chunk_size = int(os.environ.get('DOWNLOAD_CHUNK_SIZE', 1024*1024*4))

    if not input_file or not bucket_name:
        return {
            'statusCode': 400,
            'body': 'Missing required parameters: input_file and bucket_name'
        }

    try:
        stream_xml_to_parquet_s3(
            input_file=input_file,
            bucket_name=bucket_name,
            chunk_size=chunk_size,
            download_chunk_size=download_chunk_size
        )
        return {
            'statusCode': 200,
            'body': f'Successfully processed {input_file} to {bucket_name}'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error processing file: {str(e)}'
        }

# This part is optional and only used when running the script locally
if __name__ == "__main__":
    # Example local execution
    event = {
        'input_file': "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2010/discogs_20100902_releases.xml.gz",
        'bucket_name': "discogs-data"
    }
    print(lambda_handler(event, None))    
