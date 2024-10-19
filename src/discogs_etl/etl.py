from typing import Optional,Any 
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import tempfile
import os
import boto3
import tempfile
from discogs_etl.s3 import get_default_region, get_s3_output_path, upload_to_s3
from discogs_etl.parser import create_arrays_from_chunk
from discogs_etl.schema import SCHEMAS
from discogs_etl.utils import (
    detect_data_type,
)
from discogs_etl.process import process_large_xml, process_large_xml_label

def stream_xml_to_parquet_s3(input_file: str, bucket_name: str, region: Optional[str] = None, chunk_size: int = 1000, download_chunk_size=1024*1024, use_tqdm: bool = True) -> None:
    """
    Stream an XML file to Parquet format directly to S3.

    Args:
        input_file (str): The input XML file path or URL.
        bucket_name (str): The name of the S3 bucket to store the Parquet file.
        region (Optional[str]): The AWS region for the S3 bucket. If None, uses the default region.
        chunk_size (int): The number of records to process in each chunk.
        download_chunk_size (int): The chunk size for downloading the XML file.
        use_tqdm (bool): Whether to use tqdm for progress tracking.

    Raises:
        Exception: If any error occurs during the processing or uploading.
    """
    data_type = detect_data_type(input_file)
    print(f"Detected data type: {data_type}")
    
    if region is None:
        region = get_default_region()
        print(f"No region specified. Using default region: {region}")
    else:
        print(f"Using specified region: {region}")

    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=region)

    try:
        if data_type == 'label':
            parser = process_large_xml_label(
                file_path=input_file, 
                data_type=data_type, 
                chunk_size=chunk_size, 
                download_chunk_size=download_chunk_size, 
                use_tqdm=use_tqdm
            )
        else:
            parser = process_large_xml(
                file_path=input_file, 
                data_type=data_type, 
                chunk_size=chunk_size, 
                download_chunk_size=download_chunk_size, 
                use_tqdm=use_tqdm
            )
        
        # Get the schema
        schema = SCHEMAS[data_type]
        
        # Generate S3 key
        s3_key = get_s3_output_path(input_file, bucket_name)
        
        # Initialize multipart upload
        multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
        upload_id = multipart_upload['UploadId']
        
        parts = []
        part_number = 1
        
        # Create an in-memory buffer
        buffer = io.BytesIO()
        
        # Open a ParquetWriter that writes to the buffer
        with pq.ParquetWriter(buffer, schema) as writer:
            total_rows = 0
            for i, chunk in enumerate(parser):
                processed_chunk = create_arrays_from_chunk(chunk, schema)
                table = pa.Table.from_pydict(processed_chunk, schema=schema)
                writer.write_table(table)
                total_rows += len(chunk)
                print(f"Processed chunk {i} ({len(chunk)} rows)")
                
                # Check if the buffer size is large enough to upload
                if buffer.tell() > 5 * 1024 * 1024:  # 5MB minimum for multipart upload
                    buffer.seek(0)
                    # Upload parts as buffer fills
                    part = s3_client.upload_part(
                        Bucket=bucket_name,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer.read()
                    )
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    part_number += 1
                    buffer.seek(0)
                    buffer.truncate() # Clear the buffer but keep using the same writer
        
        # Upload any remaining data
        if buffer.tell() > 0:
            buffer.seek(0)
            part = s3_client.upload_part(
                Bucket=bucket_name,
                Key=s3_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=buffer.read()
            )
            parts.append({
                'PartNumber': part_number,
                'ETag': part['ETag']
            })
        
        # Complete the multipart upload
        result = s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        print(f"Successfully uploaded {total_rows} rows to s3://{bucket_name}/{s3_key} with ETag: {result['ETag']}")
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        # Abort the multipart upload if it was initiated
        if 'upload_id' in locals():
            s3_client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=s3_key,
                UploadId=upload_id
            )
        raise

def process_xml_to_parquet_s3(input_file: str, bucket_name: str, region: Optional[str] = None, chunk_size: int = 1000, download_chunk_size=1024*1024, use_tqdm: str = True) -> None:
    """
    Process an XML file to Parquet format and upload it to S3.

    Args:
        input_file (str): The input XML file path or URL.
        bucket_name (str): The name of the S3 bucket to store the Parquet file.
        region (Optional[str]): The AWS region for the S3 bucket. If None, uses the default region.
        chunk_size (int): The number of records to process in each chunk.

    Raises:
        Exception: If any error occurs during the processing or uploading.
    """
    data_type = detect_data_type(input_file)
    print(f"Detected data type: {data_type}")
    
    if region is None:
        region = get_default_region()
        print(f"No region specified. Using default region: {region}")
    else:
        print(f"Using specified region: {region}")

    # Create bucket and initialize structure
    # create_bucket_if_not_exists(bucket_name, region)
    # initialize_bucket_structure(bucket_name)

    # Create a temporary file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
        temp_output_file = temp_file.name
        print(f"Created temporary file: {temp_output_file}")

    try:
        parser = process_large_xml(
            file_path=input_file, 
            data_type=data_type, 
            chunk_size=chunk_size, 
            # download_chunk_size=download_chunk_size, 
            use_tqdm=use_tqdm
    )
        # Get the schema
        schema = SCHEMAS[data_type]
        
        # Open a ParquetWriter
        with pq.ParquetWriter(temp_output_file, schema) as writer:
            total_rows = 0
            # Process and write the remaining chunks
            for i, chunk in enumerate(parser):
                processed_chunk = create_arrays_from_chunk(chunk, schema)
                table = pa.Table.from_pydict(processed_chunk, schema=schema)
                writer.write_table(table)
                total_rows += len(chunk)
                print(f"Processed chunk {i} ({len(chunk)} rows)")
        
        # Upload the file to S3
        s3_key = get_s3_output_path(input_file, bucket_name)
        upload_to_s3(temp_output_file, bucket_name, s3_key)
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_output_file):
            os.unlink(temp_output_file)
            print(f"Deleted temporary file: {temp_output_file}")


def process_xml_to_parquet(input_file: str, output_file: str, chunk_size: int = 1000, download_chunk_size=1024*1024, use_tqdm: str = True) -> None:
    """
    Process an XML file and convert it to Parquet format.

    Args:
        input_file (str): The path or URL of the input XML file.
        output_file (str): The path where the output Parquet file will be saved.
        chunk_size (int, optional): The number of records to process in each chunk. Defaults to 1000.

    Raises:
        ValueError: If the input file is empty or contains no valid data.
    """
    data_type = detect_data_type(input_file)
    print(f"Detected data type: {data_type}")

    parser = process_large_xml(
        file_path=input_file, 
        data_type=data_type, 
        chunk_size=chunk_size, 
        download_chunk_size=download_chunk_size, 
        use_tqdm=use_tqdm
    )

    schema = SCHEMAS[data_type]
    
    with pq.ParquetWriter(output_file, schema) as writer:
        total_rows = 0
        for i, chunk in enumerate(parser):
            processed_chunk = create_arrays_from_chunk(chunk, schema)
            table = pa.Table.from_pydict(processed_chunk, schema=schema)
            writer.write_table(table)
            total_rows += len(chunk)
            print(f"Processed chunk {i} ({len(chunk)} rows)")
            
               
    print(f"Total rows written: {total_rows}")
    print(f"Parquet file saved to: {output_file}")
    print(f"Schema: {schema}")