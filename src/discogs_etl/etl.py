from lxml import etree
from urllib.parse import urlparse
from tqdm import tqdm
from typing import Dict, List, Optional, Generator
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import gzip
import requests
import tempfile
import os
import re
from discogs_etl.s3 import get_default_region, get_s3_output_path, upload_to_s3

# Configuration for different Discogs data types
DISCOGS_CONFIGS = {
    'artists': {'root_tag': 'artists', 'item_tag': 'artist'},
    'releases': {'root_tag': 'releases', 'item_tag': 'release'},
    'masters': {'root_tag': 'masters', 'item_tag': 'master'},
    'labels': {'root_tag': 'labels', 'item_tag': 'label'}
}

def clean_xml_content(content):
    def replace_char(match):
        char = match.group()
        code = ord(char)
        if code < 0x20 and code not in (0x09, 0x0A, 0x0D):
            return " "  # Replace with space
        return char

    invalid_xml_char_regex = re.compile(r'[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF]')
    return invalid_xml_char_regex.sub(replace_char, content.decode('utf-8', errors='replace')).encode('utf-8')


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

def get_file_content(file_path: str, use_tqdm: bool = True) -> bytes:
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
        for chunk in response.iter_content(chunk_size=8192):
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

def fix_xml_structure(content: bytes, root_tag: str) -> io.BytesIO:
    """
    Fix the XML structure by adding a root element and XML declaration if necessary.

    Args:
        content (bytes): The original XML content.

    Returns:
        io.BytesIO: A file-like object containing the fixed XML content.
    """
    # Check if the content already has a root element 
    if not content.strip().startswith(b'<?xml') and not content.strip().startswith(f'<{root_tag}>'.encode()):
        # Add root element and XML declaration
        fixed_content = f'<?xml version="1.0" encoding="UTF-8"?>\n<{root_tag}>\n'.encode() + content + f'\n</{root_tag}>'.encode()
    else:
        fixed_content = content
    # Return a file-like object containing the fixed content
    return io.BytesIO(fixed_content)

def parse_large_xml_to_df(file_path: str, data_type: str, chunk_size: int = 1000, use_tqdm: bool = True) -> Generator[pd.DataFrame, None, None]:
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
    
    content = get_file_content(file_path, use_tqdm)
    # Fix XML structure
    fixed_xml = fix_xml_structure(content, config['root_tag'])
    
    context = etree.iterparse(fixed_xml, events=('end',), tag=config['item_tag'])
    chunk = []
    for event, elem in context:
        artist_data = {child.tag: child.text for child in elem}
        chunk.append(artist_data)
        if len(chunk) == chunk_size:
            yield pd.DataFrame(chunk)
            chunk = []
        elem.clear()
    if chunk:
        yield pd.DataFrame(chunk)

def parse_large_xml(file_path: str, data_type: str, chunk_size: int = 1000, use_tqdm: bool = True) -> Generator[List[Dict[str, Optional[str]]], None, None]:
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
    
    content = get_file_content(file_path, use_tqdm)
    fixed_xml = fix_xml_structure(content, config['root_tag'])
    
    context = etree.iterparse(fixed_xml, events=('end',), tag=config['item_tag'])
    chunk = []
    for event, elem in context:
        artist_data = {child.tag: child.text for child in elem}
        chunk.append(artist_data)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
        elem.clear()
    if chunk:
        yield chunk

def process_xml_to_parquet(input_file: str, output_file: str, data_type: str,  chunk_size: int = 1000, use_tqdm: str = True) -> None:
    """
    Process an XML file and convert it to Parquet format.

    Args:
        input_file (str): The path or URL of the input XML file.
        output_file (str): The path where the output Parquet file will be saved.
        chunk_size (int, optional): The number of records to process in each chunk. Defaults to 1000.

    Raises:
        ValueError: If the input file is empty or contains no valid data.
    """
    parser = parse_large_xml(input_file, data_type, chunk_size, use_tqdm)
    
    # Write the first chunk to determine the schema
    first_chunk = next(parser, None)
    if first_chunk is None:
        print("The file is empty or contains no valid data.")
        return
    
    table = pa.Table.from_pylist(first_chunk)
    schema = table.schema
    
    # Open a ParquetWriter
    with pq.ParquetWriter(output_file, schema) as writer:
        # Write the first chunk
        writer.write_table(table)
        
        # Process and write the remaining chunks
        for i, chunk in enumerate(parser, 1):
            table = pa.Table.from_pylist(chunk, schema=schema)
            writer.write_table(table)
            print(f"Processed chunk {i}")

def process_xml_to_parquet_s3(input_file: str, data_type: str, bucket_name: str, region: Optional[str] = None, chunk_size: int = 1000, use_tqdm: str = True) -> None:
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
        parser = parse_large_xml(input_file, data_type, chunk_size, use_tqdm)
        
        # Write the first chunk to determine the schema
        first_chunk = next(parser, None)
        if first_chunk is None:
            print("The file is empty or contains no valid data.")
            return
        
        table = pa.Table.from_pylist(first_chunk)
        schema = table.schema
        
        # Open a ParquetWriter
        with pq.ParquetWriter(temp_output_file, schema) as writer:
            # Write the first chunk
            writer.write_table(table)
            
            # Process and write the remaining chunks
            for i, chunk in enumerate(parser, 1):
                table = pa.Table.from_pylist(chunk, schema=schema)
                writer.write_table(table)
                print(f"Processed chunk {i}")
        
        # Upload the file to S3
        s3_key = get_s3_output_path(input_file, bucket_name)
        upload_to_s3(temp_output_file, bucket_name, s3_key)
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_output_file):
            os.unlink(temp_output_file)
            print(f"Deleted temporary file: {temp_output_file}")

# Usage
discogs_artist_20180101 = "https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2018/discogs_20180101_artists.xml.gz"
# input_file = '/Users/tweddielin/Downloads/discogs_20080309_artists.xml'
# output_file = '/Users/tweddielin/Downloads/discogs_20080309_artists.parquet'
# process_xml_to_parquet(input_file, output_file)


# Usage
# file_path = '/Users/tweddielin/Downloads/discogs_20080309_artists.xml'
# parser = parse_large_xml(file_path)