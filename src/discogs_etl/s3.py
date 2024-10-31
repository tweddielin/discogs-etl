import boto3
import requests
import re
from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse
from typing import List, Optional, Callable, Dict
from botocore.exceptions import ClientError
from botocore import UNSIGNED
from botocore.config import Config

def get_default_region() -> str:
    """
    Get the default AWS region from the current session.

    Returns:
        str: The default AWS region name.
    """
    return boto3.Session().region_name

def parse_input_url(url: str) -> tuple[str, str, str]:
    """
    Parse the input URL to extract year, month, and data type.

    Args:
        url (str): The input URL of the Discogs data dump.

    Returns:
        tuple[str, str, str]: A tuple containing year, month, and data type.
    """
    parsed = urlparse(url)
    path_parts = parsed.path.split('/')
    filename = path_parts[-1]
    date_str = filename.split('_')[1]
    year = date_str[:4]
    month = date_str[4:6]
    data_type = filename.split('_')[-1].split('.')[0]
    return year, month, data_type

def get_s3_output_path(input_url: str, bucket_name: str) -> str:
    """
    Generate the S3 output path for the processed Parquet file.

    Args:
        input_url (str): The input URL of the Discogs data dump.
        bucket_name (str): The name of the S3 bucket.

    Returns:
        str: The S3 key for the output Parquet file.
    """
    year, month, data_type = parse_input_url(input_url)
    return f"{data_type}/year={year}/month={month}/{data_type}_{year}{month}01.parquet"

def upload_to_s3(local_file_path: str, bucket_name: str, s3_key: str) -> None:
    """
    Upload a local file to S3.

    Args:
        local_file_path (str): The path to the local file to upload.
        bucket_name (str): The name of the S3 bucket.
        s3_key (str): The S3 key (path) where the file will be uploaded.

    Raises:
        ClientError: If an error occurs during the upload process.
    """
    s3 = boto3.client('s3')
    print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")
    s3.upload_file(local_file_path, bucket_name, s3_key)
    print("Upload complete")

def check_bucket_exists(s3_client: boto3.client, bucket_name: str) -> bool:
    """
    Check if an S3 bucket exists and is accessible.

    Args:
        s3_client (boto3.client): The boto3 S3 client.
        bucket_name (str): The name of the bucket to check.

    Returns:
        bool: True if the bucket exists and is accessible, False otherwise.

    Raises:
        ClientError: If an unexpected error occurs while checking the bucket.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            print(f"Bucket {bucket_name} exists, but you don't have permission to access it.")
            return True
        elif error_code == 404:
            print(f"Bucket {bucket_name} does not exist.")
            return False
        else:
            print(f"Unexpected error checking bucket {bucket_name}: {e}")
            raise

def create_bucket_if_not_exists(bucket_name: str, region: Optional[str] = None) -> None:
    """
    Create an S3 bucket if it doesn't already exist.

    Args:
        bucket_name (str): The name of the bucket to create.
        region (Optional[str]): The AWS region in which to create the bucket. 
                                If None, the default region will be used.

    Raises:
        ClientError: If an unexpected error occurs while creating the bucket.
        Exception: If the bucket creation fails or cannot be verified.
    """
    s3_client = boto3.client('s3', region_name=region)
    
    # Check if the bucket already exists
    if check_bucket_exists(s3_client, bucket_name):
        print(f"Bucket {bucket_name} already exists.")
        return
    
    # If the bucket doesn't exist, create it
    try:
        if region is None or region == 'us-east-1':
            # 'us-east-1' is a special case where LocationConstraint should not be specified
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        print(f"Bucket {bucket_name} created successfully.")
    except ClientError as e:
        print(f"Unexpected error creating bucket {bucket_name}: {e}")
        raise

    # Verify bucket was created and is accessible
    if check_bucket_exists(s3_client, bucket_name):
        print(f"Confirmed creation and ownership of bucket {bucket_name}")
    else:
        print(f"Failed to create bucket {bucket_name}")
        raise Exception(f"Failed to create bucket {bucket_name}")

def check_structure_exists(s3_client: boto3.client, bucket_name: str, data_types: List[str]) -> bool:
    """
    Check if the expected folder structure exists in the S3 bucket.

    Args:
        s3_client (boto3.client): The boto3 S3 client.
        bucket_name (str): The name of the bucket to check.
        data_types (List[str]): List of data types to check for in the bucket structure.

    Returns:
        bool: True if any of the expected folders exist, False otherwise.

    Raises:
        ClientError: If an error occurs while checking the bucket structure.
    """
    try:
        for data_type in data_types:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{data_type}/", MaxKeys=1)
            if 'Contents' in response:
                return True
        return False
    except ClientError as e:
        print(f"Error checking bucket structure: {e}")
        raise

def initialize_bucket_structure(bucket_name: str) -> None:
    """
    Initialize the folder structure in the S3 bucket for storing Discogs data.

    Args:
        bucket_name (str): The name of the bucket to initialize.

    Raises:
        ClientError: If an error occurs while initializing the bucket structure.
    """
    s3 = boto3.client('s3')
    data_types = ['artists', 'labels', 'masters', 'releases']
    
    for data_type in data_types:
        # Create an empty object to represent the "folder"
        key = f"{data_type}/"
        try:
            s3.put_object(Bucket=bucket_name, Key=key)
            print(f"Initialized structure for {data_type}")
        except ClientError as e:
            print(f"Error initializing {data_type}: {e}")

def stream_to_s3(bucket_name: str, s3_key: str, data_generator: Callable, region: Optional[str] = None):
    """
    Stream data to S3 using multipart upload.

    Args:
        bucket_name (str): The name of the S3 bucket.
        s3_key (str): The S3 object key.
        data_generator (Callable): A generator function that yields data chunks.
        region (Optional[str]): The AWS region for the S3 bucket. If None, uses the default region.

    Returns:
        str: The ETag of the uploaded object.

    Raises:
        Exception: If any error occurs during the upload process.
    """
    s3_client = boto3.client('s3', region_name=region)
    
    try:
        # Initialize multipart upload
        multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
        upload_id = multipart_upload['UploadId']
        
        parts = []
        part_number = 1

        for chunk in data_generator:
            import ipdb
            ipdb.set_trace()
            part = s3_client.upload_part(
                Bucket=bucket_name,
                Key=s3_key,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=chunk
            )
            parts.append({
                'PartNumber': part_number,
                'ETag': part['ETag']
            })
            part_number += 1

        # Complete the multipart upload
        result = s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=s3_key,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        
        return result['ETag']

    except Exception as e:
        print(f"An error occurred during multipart upload: {str(e)}")
        # Abort the multipart upload if it was initiated
        if 'upload_id' in locals():
            s3_client.abort_multipart_upload(
                Bucket=bucket_name,
                Key=s3_key,
                UploadId=upload_id
            )
        raise


def list_s3_files(bucket_name, prefix):
    """
    List all files in an S3 bucket with a specific prefix.
    Uses anonymous access for public buckets.

    Example:
        bucket_name = "discogs-data-dumps"
        prefix = "data/2019/"

        files = list_s3_files(bucket_name, prefix)
    
    Args:
        bucket_name (str): Name of the S3 bucket
        prefix (str): Prefix to filter objects
        
    Returns:
        list: List of file names matching the prefix
    """
    # Create an S3 client with anonymous access
    s3_client = boto3.client(
        's3',
        config=Config(signature_version=UNSIGNED)
    )
    
    try:
        # List objects in the bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        files = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    files.append(obj['Key'])
        
        return files
        
    except Exception as e:
        print(f"Error accessing S3 bucket: {e}")
        return []
    
def parse_checksum_file(url: str) -> Dict[str, str]:
    """
    Parse a CHECKSUM.txt file from Discogs and return a dictionary of filename to checksum.
    Handles both formats:
    - "<checksum> *<filename>"
    - "<checksum> <filename>"
    
    Args:
        url: URL to the CHECKSUM.txt file
        
    Returns:
        Dictionary mapping filenames to their checksums
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        checksums = {}
        for line in response.text.splitlines():
            if not line.strip():
                continue
                
            # Split on whitespace
            parts = line.strip().split()
            
            # Handle case where there might be multiple spaces
            if len(parts) >= 2:
                checksum = parts[0]
                # Join remaining parts and remove any asterisk
                filename = ' '.join(parts[1:]).replace('*', '').strip()
                checksums[filename] = checksum
                
        return checksums
    except Exception as e:
        print(f"Error parsing checksum file {url}: {e}")
        return {}

def organize_discogs_files(
    file_list: List[str],
    base_url: str = "https://discogs-data-dumps.s3.us-west-2.amazonaws.com"
) -> List[Dict[str, Dict[str, str]]]:
    """
    Organize Discogs data files into structured format grouped by year-month.
    Takes the latest file for each type within the month.
    
    Args:
        file_list: List of file paths from S3 bucket
        base_url: Base URL of the S3 bucket
        
    Returns:
        List of dictionaries containing organized file information
    """
    # Regular expressions for parsing file names
    date_pattern = r"discogs_(\d{4})(\d{2})(\d{2})_"
    file_type_pattern = r"discogs_\d{8}_(\w+)\.xml\.gz"
    
    # Group files by year-month
    monthly_files: Dict[str, Dict[str, Dict]] = defaultdict(lambda: defaultdict(dict))
    monthly_checksums: Dict[str, Dict[str, str]] = defaultdict(dict)
    
    # First pass: collect all files and organize by year-month
    for file_path in file_list:
        date_match = re.search(date_pattern, file_path)
        if not date_match:
            continue
            
        year, month, day = date_match.groups()
        year_month = f"{year}-{month}"
        full_date = f"{year}{month}{day}"
        
        # Handle checksum files
        if file_path.endswith('CHECKSUM.txt'):
            if full_date not in monthly_checksums[year_month] or full_date > monthly_checksums[year_month].get('date', ''):
                monthly_checksums[year_month] = {
                    'date': full_date,
                    'path': file_path
                }
            continue
            
        type_match = re.search(file_type_pattern, file_path)
        if not type_match:
            continue
            
        file_type = type_match.group(1)
        
        # Store file info with its date for later comparison
        current_info = {
            'date': full_date,
            'path': file_path
        }
        
        # Update only if this is the first file of its type or if it's newer
        if (file_type not in monthly_files[year_month] or 
            current_info['date'] > monthly_files[year_month][file_type]['date']):
            monthly_files[year_month][file_type] = current_info
    
    # Process each year-month and create final structure
    result = []
    type_mapping = {
        'artists': 'artist',
        'masters': 'master',
        'labels': 'label',
        'releases': 'release'
    }
    
    for year_month in sorted(monthly_files.keys()):
        # Get checksums if available for this month
        checksums = {}
        if year_month in monthly_checksums:
            checksum_path = monthly_checksums[year_month]['path']
            checksum_url = f"{base_url}/{checksum_path}"
            checksums = parse_checksum_file(checksum_url)
        
        # Create entry for this month
        entry = {}
        
        for file_type, file_info in monthly_files[year_month].items():
            simple_type = type_mapping.get(file_type)
            if simple_type:
                file_path = file_info['path']
                filename = file_path.split('/')[-1]
                entry[simple_type] = {
                    'url': f"{base_url}/{file_path}",
                    'checksum': checksums.get(filename, ''),
                    'date': datetime.strptime(file_info['date'], '%Y%m%d').strftime('%Y-%m-%d')
                }
        
        if entry:  # Only add if we have any files
            # Add metadata
            entry['year_month'] = year_month
            result.append(entry)
    
    return result