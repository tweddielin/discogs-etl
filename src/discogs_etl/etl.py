import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import tempfile
import logging
import os
import glob
import boto3
from typing import Optional, List, Dict, Any, Generator, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from discogs_etl.s3 import get_default_region, get_s3_output_path, upload_to_s3
from discogs_etl.parser import create_arrays_from_chunk
from discogs_etl.schema import SCHEMAS
from discogs_etl.utils import (
    detect_data_type,
    is_url
)
from discogs_etl.process import process_large_xml, process_large_xml_label, download_file_with_checksum
from discogs_etl.io import OptimizedDownloader


# Configure logger
logger = logging.getLogger(__name__)


# S3 multipart upload constants
MIN_PART_SIZE = 5 * 1024 * 1024  # AWS minimum requirement (5MB)
OPTIMAL_PART_SIZE = 25 * 1024 * 1024  # Optimal part size for better performance (25MB)
MAX_PARTS = 10000  # AWS maximum number of parts

def stream_xml_to_parquet_s3_optimized(
        input_file: str, 
        bucket_name: str, 
        checksum: str = None, 
        region: Optional[str] = None, 
        chunk_size: int = 1000, 
        download_chunk_size=1024*1024, 
        upload_buffer_size=20 * 1024 * 1024,
        max_workers: int = 8,
        part_size: int = OPTIMAL_PART_SIZE,  # New parameter for configurable part size
        use_tqdm: bool = True) -> None:
    """
    Stream an XML file to Parquet format directly to S3.

    Args:
        input_file (str): The input XML file path or URL.
        bucket_name (str): The name of the S3 bucket to store the Parquet file.
        checksum (str): The expected checksum for file verification.
        region (Optional[str]): The AWS region for the S3 bucket. If None, uses the default region.
        chunk_size (int): The number of records to process in each chunk.
        download_chunk_size (int): The chunk size for downloading the XML file.
        upload_buffer_size (int): The buffer size for S3 multipart uploads (must be at least 5MB).
        max_workers (int): Maximum number of workers for parallel operations.
        part_size (int): Size of each part for multipart uploads. Must be at least 5MB.
                         Larger values can improve performance on high-bandwidth connections.
        use_tqdm (bool): Whether to use tqdm for progress tracking.

    Raises:
        Exception: If any error occurs during the processing or uploading.
    """
    temp_downloaded_file = None
    upload_id = None
    s3_key = None
    parquet_file_path = None
    
    # Ensure part_size is at least MIN_PART_SIZE
    if part_size < MIN_PART_SIZE:
        logger.warning(f"Specified part_size {part_size/(1024*1024):.2f}MB is below the minimum of {MIN_PART_SIZE/(1024*1024)}MB. Using minimum value.")
        part_size = MIN_PART_SIZE
    
    try:
        data_type = detect_data_type(input_file)
        logger.info(f"Detected data type: {data_type}")
        
        if region is None:
            region = get_default_region()
            logger.info(f"No region specified. Using default region: {region}")
        else:
            logger.info(f"Using specified region: {region}")

        # Initialize S3 client
        s3_client = boto3.client('s3', region_name=region)
        
        # Use a context manager to ensure the temp file is deleted
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_downloaded_file = temp_file.name
            logger.info(f"Created temporary file: {temp_downloaded_file}")
            
            # Download the file if it's a URL
            if is_url(input_file):
                if data_type in ["release", "master", "artist"]:
                    downloader = OptimizedDownloader(
                        url=input_file,
                        output_path=temp_downloaded_file,
                        max_workers=max_workers,
                        chunk_size=8 * 1024 * 1024,
                        read_timeout=300,
                        algorithm="sha256",
                    )
                    downloader.download()
                    if checksum:
                        downloader.verify_checksum(checksum)
                elif data_type == "label":
                    download_file_with_checksum(
                        url=input_file,
                        output_file=temp_downloaded_file,
                        algorithm="sha256",
                        expected_checksum=checksum,
                        chunk_size=download_chunk_size,
                    )
                else:
                    raise NotImplementedError(f"{data_type} unrecognized.")
            else:
                temp_downloaded_file = input_file
                logger.info(f"Using local file: {temp_downloaded_file}")
            
            # Get the schema
            schema = SCHEMAS[data_type]
            
            # Generate S3 key
            s3_key = get_s3_output_path(input_file, bucket_name)
            logger.info(f"Will upload to s3://{bucket_name}/{s3_key}")
            
            # Initialize parser based on data type - keep as generator, don't materialize
            if data_type == 'label':
                parser = process_large_xml_label(
                    file_path=temp_downloaded_file, 
                    data_type=data_type, 
                    chunk_size=chunk_size, 
                    use_tqdm=use_tqdm
                )
            else:
                parser = process_large_xml(
                    file_path=temp_downloaded_file, 
                    data_type=data_type, 
                    chunk_size=chunk_size, 
                    use_tqdm=use_tqdm
                )
            
            # Create a temporary file for the Parquet data
            with tempfile.NamedTemporaryFile(delete=False) as parquet_temp_file:
                parquet_file_path = parquet_temp_file.name
                logger.info(f"Created temporary Parquet file: {parquet_file_path}")
                
                # Process chunks in a streaming fashion
                total_rows = 0
                with pq.ParquetWriter(parquet_file_path, schema) as writer:
                    for i, chunk in enumerate(parser):  # Process chunks as they come, don't materialize the whole generator
                        try:
                            processed_chunk = create_arrays_from_chunk(chunk, schema)
                            table = pa.Table.from_pydict(processed_chunk, schema=schema)
                            writer.write_table(table)
                            total_rows += len(chunk)
                            if i % 100 == 0:
                                logger.info(f"Processed chunk {i} with {100*len(chunk)} rows. Total rows so far: {total_rows}")
                        except Exception as e:
                            logger.error(f"Error processing chunk {i}: {str(e)}")
                            if chunk:
                                logger.error(f"First few records of problematic chunk: {chunk[:2]}")
                            raise
                
                logger.info(f"Finished processing {total_rows} total rows")
                
                # Get the total size of the Parquet file
                total_size = os.path.getsize(parquet_file_path)
                logger.info(f"Total Parquet data size: {total_size / (1024 * 1024):.2f} MB")
                
                # If the total size is less than MIN_PART_SIZE, use a single upload
                if total_size < MIN_PART_SIZE:
                    logger.info(f"Data size is less than {MIN_PART_SIZE/(1024*1024)}MB, using single upload")
                    with open(parquet_file_path, 'rb') as f:
                        s3_client.put_object(
                            Bucket=bucket_name,
                            Key=s3_key,
                            Body=f.read()
                        )
                    logger.info(f"Successfully uploaded {total_rows} rows to s3://{bucket_name}/{s3_key}")
                else:
                    # Use multipart upload for larger files
                    logger.info(f"Data size is {total_size / (1024 * 1024):.2f} MB, using multipart upload with {part_size/(1024*1024)}MB parts")
                    
                    # Initialize multipart upload
                    multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
                    upload_id = multipart_upload['UploadId']
                    logger.info(f"Initiated multipart upload with ID: {upload_id}")
                    
                    # Calculate how many parts we need based on the specified part_size
                    num_parts = (total_size + part_size - 1) // part_size  # Ceiling division
                    
                    # Check if we exceed the maximum number of parts
                    if num_parts > MAX_PARTS:
                        adjusted_part_size = (total_size + MAX_PARTS - 1) // MAX_PARTS
                        logger.warning(f"Calculated {num_parts} parts exceeds AWS limit of {MAX_PARTS}. Adjusting part size to {adjusted_part_size/(1024*1024):.2f}MB")
                        part_size = adjusted_part_size
                        num_parts = (total_size + part_size - 1) // part_size
                    
                    logger.info(f"Splitting data into {num_parts} parts of approximately {part_size / (1024 * 1024):.2f} MB each")
                    
                    # Function to upload a part from the file
                    def upload_part(part_number: int, start_byte: int, end_byte: int) -> Dict[str, Any]:
                        """Upload a part of the file to S3"""
                        try:
                            # Calculate actual size of this part
                            part_size_actual = end_byte - start_byte
                            
                            # Ensure minimum part size (except for the last part)
                            if part_number < num_parts and part_size_actual < MIN_PART_SIZE:
                                logger.warning(f"Part {part_number} size ({part_size_actual/(1024*1024):.2f}MB) is below minimum. This should not happen.")
                            
                            logger.info(f"Uploading part {part_number}/{num_parts} ({part_size_actual / (1024 * 1024):.2f} MB)")
                            
                            with open(parquet_file_path, 'rb') as f:
                                f.seek(start_byte)
                                part_data = f.read(part_size_actual)
                                
                                part = s3_client.upload_part(
                                    Bucket=bucket_name,
                                    Key=s3_key,
                                    PartNumber=part_number,
                                    UploadId=upload_id,
                                    Body=part_data
                                )
                                
                                return {
                                    'PartNumber': part_number,
                                    'ETag': part['ETag'],
                                    'Size': part_size_actual
                                }
                        except Exception as e:
                            logger.error(f"Error uploading part {part_number}: {str(e)}")
                            raise
                    
                    # Prepare part information
                    part_info = []
                    for i in range(num_parts):
                        start_byte = i * part_size
                        end_byte = min((i + 1) * part_size, total_size)
                        part_info.append((i + 1, start_byte, end_byte))
                    
                    # Upload parts in parallel
                    parts = []
                    with ThreadPoolExecutor(max_workers=min(max_workers, num_parts)) as executor:
                        # Submit all upload tasks
                        future_to_part = {
                            executor.submit(upload_part, part_number, start_byte, end_byte): part_number
                            for part_number, start_byte, end_byte in part_info
                        }
                        
                        # Process results as they complete
                        for future in as_completed(future_to_part):
                            part_number = future_to_part[future]
                            try:
                                result = future.result()
                                parts.append({
                                    'PartNumber': result['PartNumber'],
                                    'ETag': result['ETag']
                                })
                                logger.info(f"Successfully uploaded part {part_number} ({result['Size'] / (1024 * 1024):.2f} MB)")
                            except Exception as e:
                                logger.error(f"Part {part_number} upload failed: {str(e)}")
                                raise
                    
                    # Sort parts by part number before completing the upload
                    parts.sort(key=lambda x: x['PartNumber'])
                    
                    # Complete the multipart upload
                    result = s3_client.complete_multipart_upload(
                        Bucket=bucket_name,
                        Key=s3_key,
                        UploadId=upload_id,
                        MultipartUpload={'Parts': parts}
                    )
                    
                    logger.info(f"Successfully uploaded {total_rows} rows to s3://{bucket_name}/{s3_key}")
                    logger.info(f"Upload complete with ETag: {result['ETag']}")
                
                # Clean up the temporary Parquet file
                try:
                    os.unlink(parquet_file_path)
                    logger.info(f"Deleted temporary Parquet file: {parquet_file_path}")
                except Exception as cleanup_error:
                    logger.error(f"Failed to delete temporary Parquet file: {str(cleanup_error)}")
            
    except Exception as e:
        logger.exception(f"Error in stream_xml_to_parquet_s3: {str(e)}")
        # Abort the multipart upload if it was initiated
        if upload_id and s3_key:
            try:
                s3_client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=s3_key,
                    UploadId=upload_id
                )
                logger.info(f"Aborted multipart upload with ID: {upload_id}")
            except Exception as abort_error:
                logger.error(f"Failed to abort multipart upload: {str(abort_error)}")
        raise
    finally:
        # Clean up temporary files if they were created and still exist
        if temp_downloaded_file and temp_downloaded_file != input_file and os.path.exists(temp_downloaded_file):
            try:
                os.unlink(temp_downloaded_file)
                logger.info(f"Deleted temporary file: {temp_downloaded_file}")
            except Exception as cleanup_error:
                logger.error(f"Failed to delete temporary file: {str(cleanup_error)}")
        
        if parquet_file_path and os.path.exists(parquet_file_path):
            try:
                os.unlink(parquet_file_path)
                logger.info(f"Deleted temporary Parquet file in finally block: {parquet_file_path}")
            except Exception as cleanup_error:
                logger.error(f"Failed to delete temporary Parquet file in finally block: {str(cleanup_error)}")


def stream_xml_to_parquet_s3(
        input_file: str, 
        bucket_name: str, 
        checksum: str = None, 
        region: Optional[str] = None, 
        chunk_size: int = 1000, 
        download_chunk_size=1024*1024, 
        upload_buffer_size=5 * 1024 * 1024,
        use_tqdm: bool = True) -> None:
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
    
    # Track if we created a temporary file
    temp_downloaded_file = None
    is_temp_file = False
    upload_id = None
    s3_key = None

    try:
        # Only create a temporary file if we're downloading from a URL
        if is_url(input_file):
            # Create a temporary file that will be automatically deleted when closed
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            temp_downloaded_file = temp_file.name
            temp_file.close()  # Close the file handle but keep the file
            is_temp_file = True
            
            print(f"Created temporary file: {temp_downloaded_file}")
            
            if data_type in ["release", "master", "artist"]:
                downloader = OptimizedDownloader(
                    url=input_file,
                    output_path=temp_downloaded_file,
                    max_workers=8,
                    chunk_size=8 * 1024 * 1024,
                    read_timeout=300,
                    algorithm="sha256",
                )
                downloader.download()
                downloader.verify_checksum(checksum)
            elif data_type == "label":
                download_file_with_checksum(
                    url = input_file,
                    output_file=temp_downloaded_file,
                    algorithm="sha256",
                    expected_checksum=checksum,
                    chunk_size=download_chunk_size,
                )
            else:
                raise NotImplementedError(f"{data_type} unrecognized.")
        else:
            temp_downloaded_file = input_file
            is_temp_file = False
        
        if data_type == 'label':
            parser = process_large_xml_label(
                file_path=temp_downloaded_file, 
                data_type=data_type, 
                chunk_size=chunk_size, 
                use_tqdm=use_tqdm
            )
        else:
            parser = process_large_xml(
                file_path=temp_downloaded_file, 
                data_type=data_type, 
                chunk_size=chunk_size, 
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
                try:
                    processed_chunk = create_arrays_from_chunk(chunk, schema)
                # If an error occurs, print the chunk and raise an exception
                except Exception as e:
                    print(f"Error processing chunk {i}: {chunk}")
                    raise e
                table = pa.Table.from_pydict(processed_chunk, schema=schema)
                writer.write_table(table)
                total_rows += len(chunk)
                if i % 100 == 0:
                    logger.info(f"Processed chunk {i} with {100*len(chunk)} rows. Total rows so far: {total_rows}")
                # Check if the buffer size is large enough to upload
                if buffer.tell() > upload_buffer_size:  # 5MB minimum for multipart upload
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
        
        print(f"\nSuccessfully uploaded {total_rows} rows to s3://{bucket_name}/{s3_key} with ETag: {result['ETag']}")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        # Abort the multipart upload if it was initiated
        if upload_id and s3_key:
            try:
                s3_client.abort_multipart_upload(
                    Bucket=bucket_name,
                    Key=s3_key,
                    UploadId=upload_id
                )
                logger.info("Aborted multipart upload")
            except Exception as abort_error:
                logger.error(f"Failed to abort multipart upload: {str(abort_error)}")
        raise
    
    finally:
        # Clean up temporary files if they were created
        if is_temp_file and temp_downloaded_file and os.path.exists(temp_downloaded_file):
            try:
                os.unlink(temp_downloaded_file)
                logger.info(f"Deleted temporary file: {temp_downloaded_file}")
            except Exception as cleanup_error:
                logger.error(f"Failed to delete temporary file {temp_downloaded_file}: {str(cleanup_error)}")
                # Try to force delete with additional error information
                try:
                    import stat
                    # Check file permissions
                    file_stat = os.stat(temp_downloaded_file)
                    logger.error(f"File permissions: {stat.filemode(file_stat.st_mode)}")
                    logger.error(f"File owner: {file_stat.st_uid}, group: {file_stat.st_gid}")
                    
                    # Try to change permissions and delete
                    os.chmod(temp_downloaded_file, stat.S_IWUSR | stat.S_IRUSR)
                    os.unlink(temp_downloaded_file)
                    logger.info(f"Successfully deleted file after changing permissions")
                except Exception as force_cleanup_error:
                    logger.error(f"Force delete failed: {str(force_cleanup_error)}")
        
        # Check for any other temporary files that might have been created
        temp_dir = tempfile.gettempdir()
        try:
            # List all files in the temp directory that might be related to this process
            pid = os.getpid()
            temp_pattern = f"tmp*{pid}*"
            for temp_file in glob.glob(os.path.join(temp_dir, temp_pattern)):
                try:
                    os.unlink(temp_file)
                    logger.info(f"Deleted additional temporary file: {temp_file}")
                except Exception as e:
                    logger.error(f"Failed to delete additional temporary file {temp_file}: {str(e)}")
        except Exception as e:
            logger.error(f"Error while cleaning up additional temporary files: {str(e)}")

# def stream_xml_to_parquet_s3(
#         input_file: str, 
#         bucket_name: str, 
#         checksum: str = None, 
#         region: Optional[str] = None, 
#         chunk_size: int = 1000, 
#         download_chunk_size=1024*1024, 
#         upload_buffer_size=5 * 1024 * 1024,
#         use_tqdm: bool = True) -> None:
#     """
#     Stream an XML file to Parquet format directly to S3.

#     Args:
#         input_file (str): The input XML file path or URL.
#         bucket_name (str): The name of the S3 bucket to store the Parquet file.
#         region (Optional[str]): The AWS region for the S3 bucket. If None, uses the default region.
#         chunk_size (int): The number of records to process in each chunk.
#         download_chunk_size (int): The chunk size for downloading the XML file.
#         use_tqdm (bool): Whether to use tqdm for progress tracking.

#     Raises:
#         Exception: If any error occurs during the processing or uploading.
#     """
#     data_type = detect_data_type(input_file)
#     print(f"Detected data type: {data_type}")
    
#     if region is None:
#         region = get_default_region()
#         print(f"No region specified. Using default region: {region}")
#     else:
#         print(f"Using specified region: {region}")

#     # Initialize S3 client
#     s3_client = boto3.client('s3', region_name=region)
    

#     with tempfile.NamedTemporaryFile(delete=False) as temp_file:
#         temp_downloaded_file = temp_file.name
#         print(f"Created temporary file: {temp_downloaded_file}")
        
#         if is_url(input_file):
#             if data_type in ["release", "master", "artist"]:
#                 downloader = OptimizedDownloader(
#                     url=input_file,
#                     output_path=temp_downloaded_file,
#                     max_workers=8,
#                     chunk_size=8 * 1024 * 1024,
#                     read_timeout=300,
#                     algorithm="sha256",
#                 )
#                 downloader.download()
#                 downloader.verify_checksum(checksum)
#             elif data_type == "label":
#                 download_file_with_checksum(
#                     url = input_file,
#                     output_file=temp_downloaded_file,
#                     algorithm="sha256",
#                     expected_checksum=checksum,
#                     chunk_size=download_chunk_size,
#                 )
#             else:
#                 raise NotImplementedError(f"{data_type} unrecognized.")
#         else:
#             temp_downloaded_file = input_file
        
#         try:
#             if data_type == 'label':
#                 parser = process_large_xml_label(
#                     file_path=temp_downloaded_file, 
#                     data_type=data_type, 
#                     chunk_size=chunk_size, 
#                     use_tqdm=use_tqdm
#                 )
#             else:
#                 parser = process_large_xml(
#                     file_path=temp_downloaded_file, 
#                     data_type=data_type, 
#                     chunk_size=chunk_size, 
#                     use_tqdm=use_tqdm
#                 )
            
#             # Get the schema
#             schema = SCHEMAS[data_type]
            
#             # Generate S3 key
#             s3_key = get_s3_output_path(input_file, bucket_name)
            
#             # Initialize multipart upload
#             multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
#             upload_id = multipart_upload['UploadId']
            
#             parts = []
#             part_number = 1
            
#             # Create an in-memory buffer
#             buffer = io.BytesIO()
            
#             # Open a ParquetWriter that writes to the buffer
#             with pq.ParquetWriter(buffer, schema) as writer:
#                 total_rows = 0
#                 for i, chunk in enumerate(parser):
#                     try:
#                         processed_chunk = create_arrays_from_chunk(chunk, schema)
#                     # If an error occurs, print the chunk and raise an exception
#                     except Exception as e:
#                         print(f"Error processing chunk {i}: {chunk}")
#                         raise e
#                     table = pa.Table.from_pydict(processed_chunk, schema=schema)
#                     writer.write_table(table)
#                     total_rows += len(chunk)
#                     if i % 100 == 0:
#                         logger.info(f"Processed chunk {i} with {100*len(chunk)} rows. Total rows so far: {total_rows}")
#                     # print(f"\rProgress: chunk {i} | "
#                     #       f"Rows: {total_rows}", end="")
#                     # Check if the buffer size is large enough to upload
#                     if buffer.tell() > upload_buffer_size:  # 5MB minimum for multipart upload
#                         buffer.seek(0)
#                         # Upload parts as buffer fills
#                         part = s3_client.upload_part(
#                             Bucket=bucket_name,
#                             Key=s3_key,
#                             PartNumber=part_number,
#                             UploadId=upload_id,
#                             Body=buffer.read()
#                         )
#                         parts.append({
#                             'PartNumber': part_number,
#                             'ETag': part['ETag']
#                         })
#                         part_number += 1
#                         buffer.seek(0)
#                         buffer.truncate() # Clear the buffer but keep using the same writer
            
#             # Upload any remaining data
#             if buffer.tell() > 0:
#                 buffer.seek(0)
#                 part = s3_client.upload_part(
#                     Bucket=bucket_name,
#                     Key=s3_key,
#                     PartNumber=part_number,
#                     UploadId=upload_id,
#                     Body=buffer.read()
#                 )
#                 parts.append({
#                     'PartNumber': part_number,
#                     'ETag': part['ETag']
#                 })
            
#             # Complete the multipart upload
#             result = s3_client.complete_multipart_upload(
#                 Bucket=bucket_name,
#                 Key=s3_key,
#                 UploadId=upload_id,
#                 MultipartUpload={'Parts': parts}
#             )
            
#             print(f"\nSuccessfully uploaded {total_rows} rows to s3://{bucket_name}/{s3_key} with ETag: {result['ETag']}")
            
#             # delete temp file
#             os.unlink(temp_downloaded_file)
#             print(f"Deleted temporary file: {temp_downloaded_file}")

#         except Exception as e:
#             print(f"An error occurred: {str(e)}")
#             # Abort the multipart upload if it was initiated
#             if 'upload_id' in locals():
#                 s3_client.abort_multipart_upload(
#                     Bucket=bucket_name,
#                     Key=s3_key,
#                     UploadId=upload_id
#                 )
#             raise
        
#         finally:
#             # Clean up temporary files if they were created and still exist
#             if temp_downloaded_file and temp_downloaded_file != input_file and os.path.exists(temp_downloaded_file):
#                 try:
#                     os.unlink(temp_downloaded_file)
#                     logger.info(f"Deleted temporary file: {temp_downloaded_file}")
#                 except Exception as cleanup_error:
#                     logger.error(f"Failed to delete temporary file: {str(cleanup_error)}")
        
           

# def process_xml_to_parquet_s3(input_file: str, bucket_name: str, region: Optional[str] = None, chunk_size: int = 1000, download_chunk_size=1024*1024, use_tqdm: str = True) -> None:
#     """
#     Process an XML file to Parquet format and upload it to S3.

#     Args:
#         input_file (str): The input XML file path or URL.
#         bucket_name (str): The name of the S3 bucket to store the Parquet file.
#         region (Optional[str]): The AWS region for the S3 bucket. If None, uses the default region.
#         chunk_size (int): The number of records to process in each chunk.

#     Raises:
#         Exception: If any error occurs during the processing or uploading.
#     """
#     data_type = detect_data_type(input_file)
#     print(f"Detected data type: {data_type}")
    
#     if region is None:
#         region = get_default_region()
#         print(f"No region specified. Using default region: {region}")
#     else:
#         print(f"Using specified region: {region}")

#     # Create bucket and initialize structure
#     # create_bucket_if_not_exists(bucket_name, region)
#     # initialize_bucket_structure(bucket_name)

#     # Create a temporary file
#     with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as temp_file:
#         temp_output_file = temp_file.name
#         print(f"Created temporary file: {temp_output_file}")

#     try:
#         parser = process_large_xml(
#             file_path=input_file, 
#             data_type=data_type, 
#             chunk_size=chunk_size, 
#             # download_chunk_size=download_chunk_size, 
#             use_tqdm=use_tqdm
#     )
#         # Get the schema
#         schema = SCHEMAS[data_type]
        
#         # Open a ParquetWriter
#         with pq.ParquetWriter(temp_output_file, schema) as writer:
#             total_rows = 0
#             # Process and write the remaining chunks
#             for i, chunk in enumerate(parser):
#                 processed_chunk = create_arrays_from_chunk(chunk, schema)
#                 table = pa.Table.from_pydict(processed_chunk, schema=schema)
#                 writer.write_table(table)
#                 total_rows += len(chunk)
#                 print(f"Processed chunk {i} ({len(chunk)} rows)")
        
#         # Upload the file to S3
#         s3_key = get_s3_output_path(input_file, bucket_name)
#         upload_to_s3(temp_output_file, bucket_name, s3_key)
    
#     finally:
#         # Clean up the temporary file
#         if os.path.exists(temp_output_file):
#             os.unlink(temp_output_file)
#             print(f"Deleted temporary file: {temp_output_file}")


# def process_xml_to_parquet(input_file: str, output_file: str, chunk_size: int = 1000, download_chunk_size=1024*1024, use_tqdm: str = True) -> None:
#     """
#     Process an XML file and convert it to Parquet format.

#     Args:
#         input_file (str): The path or URL of the input XML file.
#         output_file (str): The path where the output Parquet file will be saved.
#         chunk_size (int, optional): The number of records to process in each chunk. Defaults to 1000.

#     Raises:
#         ValueError: If the input file is empty or contains no valid data.
#     """
#     data_type = detect_data_type(input_file)
#     print(f"Detected data type: {data_type}")

#     parser = process_large_xml(
#         file_path=input_file, 
#         data_type=data_type, 
#         chunk_size=chunk_size, 
#         download_chunk_size=download_chunk_size, 
#         use_tqdm=use_tqdm
#     )

#     schema = SCHEMAS[data_type]
    
#     with pq.ParquetWriter(output_file, schema) as writer:
#         total_rows = 0
#         for i, chunk in enumerate(parser):
#             processed_chunk = create_arrays_from_chunk(chunk, schema)
#             table = pa.Table.from_pydict(processed_chunk, schema=schema)
#             writer.write_table(table)
#             total_rows += len(chunk)
#             print(f"Processed chunk {i} ({len(chunk)} rows)")
            
               
#     print(f"Total rows written: {total_rows}")
#     print(f"Parquet file saved to: {output_file}")
#     print(f"Schema: {schema}")