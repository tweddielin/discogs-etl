from lxml import etree
from urllib.parse import urlparse
from tqdm import tqdm
from typing import Dict, List, Optional, Generator, Iterator, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import gzip
import requests
import tempfile
import os
import re
import boto3
import json
from discogs_etl.s3 import get_default_region, get_s3_output_path, upload_to_s3, stream_to_s3
from discogs_etl.parser import XMLParser, create_arrays_from_chunk
from discogs_etl.schema import SCHEMAS

# Configuration for different Discogs data types
DISCOGS_CONFIGS = {
    'artists': {'root_tag': 'artists', 'item_tag': 'artist'},
    'releases': {'root_tag': 'releases', 'item_tag': 'release'},
    'masters': {'root_tag': 'masters', 'item_tag': 'master'},
    'labels': {'root_tag': 'labels', 'item_tag': 'label'}
}

LIST_FIELDS = ['urls', 'images', 'sublabels', 'genres', 'styles']

def clean_xml_content(content):
    def replace_char(match):
        char = match.group()
        code = ord(char)
        if code < 0x20 and code not in (0x09, 0x0A, 0x0D):
            return " "  # Replace with space
        return char

    invalid_xml_char_regex = re.compile(r'[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF]')
    return invalid_xml_char_regex.sub(replace_char, content.decode('utf-8', errors='replace')).encode('utf-8')

def detect_data_type(url):
    for data_type in DISCOGS_CONFIGS.keys():
        if data_type in url:
            return data_type
    raise ValueError(f"Unable to detect data type from URL: {url}")

def is_url(path: str) -> bool:
    """
    Check if the given path is a valid URL.

    Args:
        path (str): The path to check.

    Returns:
        bool: True if the path is a valid URL, False otherwise.
    """
    try:
        result = urlparse(path)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def get_file_content(file_path: str, use_tqdm: bool = True, chunk_size=1024*1024) -> bytes:
    """
    Retrieve the content of a file, either from a URL or local file system.

    Args:
        file_path (str): The path or URL of the file to retrieve.
        use_tqdm (bool): Flag whether to use tqdm
    Returns:
        bytes: The content of the file.

    Raises:
        requests.HTTPError: If there's an error downloading the file from a URL.
        IOError: If there's an error reading the local file.
    """
    if is_url(file_path):
        response = requests.get(file_path, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        
        content = b''
        if use_tqdm:
            progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, desc='Downloading')
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                content += chunk
                if use_tqdm:
                    progress_bar.update(len(chunk))
        if use_tqdm:
            progress_bar.close()
    else:
        with open(file_path, 'rb') as file:
            content = file.read()
    
    # Check if the content is gzip-compressed
    if content[:2] == b'\x1f\x8b':
        print("Decompressing gzip content...")
        content = gzip.decompress(content)
    
    print("Cleaning XML content...")
    content = clean_xml_content(content)

    return content

# def get_file_content(file_path: str, use_tqdm: bool = True, chunk_size=1024*1024) -> io.BytesIO:
#     """
#     Retrieve the content of a file, either from a URL or local file system.
#     Handles gzip compression and streams the content.

#     Args:
#         file_path (str): The path or URL of the file to retrieve.
#         use_tqdm (bool): Flag whether to use tqdm for progress display.
#         chunk_size (int): Size of chunks to read at a time.

#     Returns:
#         io.BytesIO: A BytesIO object containing the file content.

#     Raises:
#         requests.HTTPError: If there's an error downloading the file from a URL.
#         IOError: If there's an error reading the local file.
#     """
#     content = io.BytesIO()

#     if is_url(file_path):
#         response = requests.get(file_path, stream=True)
#         response.raise_for_status()
#         total_size = int(response.headers.get('content-length', 0))
        
#         if use_tqdm:
#             progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, desc='Downloading')
        
#         for chunk in response.iter_content(chunk_size=chunk_size):
#             if chunk:
#                 content.write(chunk)
#                 if use_tqdm:
#                     progress_bar.update(len(chunk))
        
#         if use_tqdm:
#             progress_bar.close()
#     else:
#         with open(file_path, 'rb') as file:
#             content.write(file.read())

#     content.seek(0)

#     # Check if the content is gzip-compressed
#     if content.read(2) == b'\x1f\x8b':
#         print("Decompressing gzip content...")
#         content.seek(0)
#         decompressed = io.BytesIO()
#         try:
#             with gzip.GzipFile(fileobj=content, mode='rb') as gz:
#                 while True:
#                     chunk = gz.read(chunk_size)
#                     if not chunk:
#                         break
#                     decompressed.write(chunk)
#         except (gzip.BadGzipFile, OSError) as e:
#             print(f"Warning: Encountered error while decompressing: {e}")
#             print("Attempting to continue with partial data...")
        
#         content = decompressed

#     content.seek(0)
#     print("Cleaning XML content...")
#     cleaned_content = io.BytesIO(clean_xml_content(content.read()))

#     return cleaned_content

# def fix_xml_structure(content: bytes, root_tag: str) -> io.BytesIO:
#     """
#     Fix the XML structure by adding a root element and XML declaration if necessary.

#     Args:
#         content (bytes): The original XML content.

#     Returns:
#         io.BytesIO: A file-like object containing the fixed XML content.
#     """
#     # Check if the content already has a root element 
#     if not content.strip().startswith(b'<?xml') and not content.strip().startswith(f'<{root_tag}>'.encode()):
#         # Add root element and XML declaration
#         fixed_content = f'<?xml version="1.0" encoding="UTF-8"?>\n<{root_tag}>\n'.encode() + content + f'\n</{root_tag}>'.encode()
#     else:
#         fixed_content = content
#     # Return a file-like object containing the fixed content
#     return io.BytesIO(fixed_content)

def parse_large_xml_to_df(file_path: str, data_type: str, chunk_size: int = 1000, download_chunk_size=1024*1024, use_tqdm: bool = True) -> Generator[pd.DataFrame, None, None]:
    """
    Parse a large XML file into chunks of pandas DataFrames.

    Args:
        file_path (str): The path or URL of the XML file to parse.
        chunk_size (int, optional): The number of records to include in each chunk. Defaults to 1000.

    Yields:
        pd.DataFrame: Chunks of the parsed XML data as pandas DataFrames.
    """
    config = DISCOGS_CONFIGS.get(data_type)
    if not config:
        raise ValueError(f"Unknown data type: {data_type}")
    
    content = get_file_content(
        file_path=file_path, 
        use_tqdm=use_tqdm, 
        chunk_size=download_chunk_size,
    )
    # Fix XML structure
    fixed_xml = fix_xml_structure(content, config['root_tag'])
    
    context = etree.iterparse(fixed_xml, events=('end',))
    parser = XMLParser(data_type=data_type)
    chunk = []
    for event, elem in context:
        if elem.tag == config['item_tag'] and elem.getparent().tag == config['root_tag']:
            item_data = parser.parse_element(elem)
            if item_data:  # Only add non-empty dictionaries
                chunk.append(item_data)
            if len(chunk) == chunk_size:
                yield pd.DataFrame(chunk)
                chunk = []
            elem.clear()
        if chunk:
            yield pd.DataFrame(chunk)

def parse_large_xml(file_path: str, data_type: str, chunk_size: int = 1000, download_chunk_size=1024*1024, use_tqdm: bool = True) -> Generator[List[Dict[str, Optional[str]]], None, None]:
    """
    Parse a large XML file into chunks of dictionaries.

    Args:
        file_path (str): The path or URL of the XML file to parse.
        chunk_size (int, optional): The number of records to include in each chunk. Defaults to 1000.

    Yields:
        List[Dict[str, Optional[str]]]: Chunks of the parsed XML data as lists of dictionaries.
    """
    config = DISCOGS_CONFIGS.get(data_type)
    if not config:
        raise ValueError(f"Unknown data type: {data_type}")
    
    content = get_file_content(
        file_path=file_path, 
        use_tqdm=use_tqdm, 
        chunk_size=download_chunk_size,
    )
    fixed_xml = fix_xml_structure(content, config['root_tag'])
    
    context = etree.iterparse(fixed_xml, events=('end',))
    parser = XMLParser(data_type=data_type)
    chunk = []
    for event, elem in context:
        if elem.tag == config['item_tag'] and elem.getparent().tag == config['root_tag']:
            item_data = parser.parse_element(elem)
            if item_data:  # Only add non-empty dictionaries
                chunk.append(item_data)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
            elem.clear()
    if chunk:
        yield chunk

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

    parser = parse_large_xml(
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
        parser = parse_large_xml(
            file_path=input_file, 
            data_type=data_type, 
            chunk_size=chunk_size, 
            download_chunk_size=download_chunk_size, 
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
        parser = parse_large_xml(
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