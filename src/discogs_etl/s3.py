import boto3
from urllib.parse import urlparse
from typing import List, Optional, Callable
from botocore.exceptions import ClientError
from datetime import datetime

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