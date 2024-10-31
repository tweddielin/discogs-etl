from lxml import etree
from typing import Dict, List, Optional, Generator, Any, Literal
import io
import gzip
import requests
import tempfile
import hashlib
import re
from discogs_etl.s3 import get_default_region, get_s3_output_path, upload_to_s3, stream_to_s3
from discogs_etl.parser import XMLParser
from discogs_etl.utils import (
    clean_xml_content, 
    clean_xml_bytes,
    is_url, 
)
from discogs_etl.config import DISCOGS_CONFIGS
from discogs_etl.io import BufferedStreamReader, StreamingXMLHandler, GzipStreamReader, OptimizedDownloader
from pathlib import Path
from urllib.parse import urlparse


class XMLFixerStreamReader:
    def __init__(self, stream: Generator[bytes, None, None], data_type: str):
        self.stream = stream
        self.buffer = b''
        self.in_release = False
        self.data_type = data_type
        self.target_tag = f"</{data_type}>".encode()

    def __iter__(self):
        for chunk in self.stream:
            self.buffer += chunk
            while self.target_tag in self.buffer:
                record_end = self.buffer.index(self.target_tag) + len(self.target_tag)
                record_xml = self.buffer[:record_end]
                self.buffer = self.buffer[record_end:]
                
                # Remove document tags if present
                record_xml = re.sub(b'</?documents?>|</?document[^>]*>', b'', record_xml)
                
                yield record_xml

        # Handle any remaining content
        if self.buffer:
            yield self.buffer

def lenient_gzip_decompress(data):
    """
    Attempt to decompress gzip data, ignoring CRC check failures.
    """
    try:
        return gzip.decompress(data)
    except gzip.BadGzipFile as e:
        if "CRC check failed" in str(e):
            print("Warning: CRC check failed, attempting lenient decompression...")
            buffer = io.BytesIO(data)
            decompressor = gzip.GzipFile(fileobj=buffer)
            try:
                return decompressor.read()
            except Exception as inner_e:
                print(f"Lenient decompression failed: {inner_e}")
                raise
        else:
            raise

def get_file_content_streaming(file_path: str, chunk_size: int = 1024 * 1024) -> Generator[bytes, None, None]:
    """
    Retrieve the content of a file, either from a URL or local file system, in a streaming fashion.

    Args:
        file_path (str): The path or URL of the file to retrieve.
        chunk_size (int): Size of chunks to yield at a time.

    Yields:
        bytes: Raw chunks of the file content.

    Raises:
        requests.HTTPError: If there's an error downloading the file from a URL.
        IOError: If there's an error reading the local file.
    """
    if is_url(file_path):
        with requests.get(file_path, stream=True) as response:
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive new chunks
                    yield chunk
    else:
        with open(file_path, 'rb') as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                yield chunk

def download_file_with_checksum(
    url: str,
    output_file: str,
    expected_checksum: Optional[str] = None,
    algorithm: Literal["md5", "sha1", "sha256", "sha512"] = "sha256",
    chunk_size: int = 8192
) -> tuple[str, bool]:
    """
    Download a file while simultaneously calculating its checksum.
    
    Args:
        url: URL of the file to download
        output_file: Path where the file should be saved
        expected_checksum: Expected checksum to verify against (optional)
        algorithm: Hash algorithm to use (md5, sha1, sha256, or sha512)
        chunk_size: Size of chunks to download and process
        
    Returns:
        tuple: (calculated_checksum, verification_result)
        verification_result will be None if no expected_checksum was provided
    """
    # Initialize appropriate hasher
    hash_algorithms = {
        "md5": hashlib.md5(),
        "sha1": hashlib.sha1(),
        "sha256": hashlib.sha256(),
        "sha512": hashlib.sha512()
    }
    
    if algorithm.lower() not in hash_algorithms:
        raise ValueError(f"Unsupported algorithm. Choose from: {', '.join(hash_algorithms.keys())}")
    
    hasher = hash_algorithms[algorithm.lower()]
    
    # Create output directory if it doesn't exist
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Get file size for progress tracking
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_size = int(response.headers.get('content-length', 0))
    
    # Download and calculate checksum simultaneously
    downloaded_size = 0
    
    print(f"Downloading: {Path(urlparse(url).path).name}")
    print(f"Destination: {output_file}")
    print(f"Total size: {total_size / (1024*1024):.1f} MB")
    
    with open(output_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                hasher.update(chunk)
                downloaded_size += len(chunk)
                
                # Print progress
                if total_size > 0:
                    progress = (downloaded_size / total_size) * 100
                    print(f"\rProgress: {progress:.1f}% | "
                          f"Downloaded: {downloaded_size / (1024*1024):.1f} MB", end="")
    
    print("\nDownload complete!")
    
    # Get final checksum
    calculated_checksum = hasher.hexdigest()
    
    # Verify if expected checksum was provided
    verification_result = None
    if expected_checksum:
        verification_result = calculated_checksum.lower() == expected_checksum.lower()
        print(f"\nChecksum verification {'successful' if verification_result else 'failed'}!")
        print(f"Expected: {expected_checksum}")
        print(f"Actual:   {calculated_checksum}")
    else:
        print(f"\n{algorithm.upper()} Checksum: {calculated_checksum}")
    
    return calculated_checksum, verification_result
                


def get_file_content(file_path: str, use_tqdm: bool = True, chunk_size=1000, stream=False):
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
        response = requests.get(file_path)
        response.raise_for_status()
        content = response.content
    else:
        with open(file_path, 'rb') as file:
            content = file.read()

    # Check if the content is gzip-compressed
    if content[:2] == b'\x1f\x8b':
        print("Decompressing gzip content...")
        try:
            content = lenient_gzip_decompress(content)
        except Exception as e:
            print(f"Decompression failed: {e}")
            print("Proceeding with compressed content...")
    print("Done.")
    return content
    
def create_generator(content, chunk_size):
    # Yield content in chunks
    offset = 0
    
    while True:
        chunk = content[offset:offset + chunk_size]
        if not chunk:
            break
        yield chunk
        offset += chunk_size

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


def process_large_xml_label(file_path: str, data_type: str, chunk_size: int = 1000, use_tqdm: bool = True) -> Generator[List[Dict[str, Optional[str]]], None, None]:
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
    
    # content = file_path
    content = get_file_content(
        file_path=file_path, 
        use_tqdm=use_tqdm, 
        chunk_size=chunk_size,
    )
    print("Cleaning XML content...")
    content = clean_xml_content(content)
    print("Done cleaning")
    # Fix XML structure
    fixed_xml = fix_xml_structure(content, config['root_tag'])
    
    context = etree.iterparse(fixed_xml, events=('end',))
    parser = XMLParser(data_type=data_type)
    chunk = []
    for event, elem in context:
        if elem.tag == config['item_tag'] and elem.getparent().tag == config['root_tag']:
            item_data = parser.parse_element(elem)
            # import ipdb
            # ipdb.set_trace()
            if item_data:  # Only add non-empty dictionaries
                chunk.append(item_data)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
            elem.clear()
    if chunk:
        yield chunk

def process_large_xml(file_path: str, data_type: str, chunk_size: int = 1000, use_tqdm: bool = True) -> Generator[List[Dict[str, Optional[str]]], None, None]:
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
    
    # content = file_path
    # content = get_file_content(
    #     file_path=file_path, 
    #     use_tqdm=use_tqdm, 
    #     chunk_size=download_chunk_size,
    # )
    # content_generator = create_generator(content, chunk_size=chunk_size)
    
    gzip_reader = GzipStreamReader(file_path)
    buffered_reader = BufferedStreamReader(gzip_reader)
    xml_handler = StreamingXMLHandler(buffered_reader)

    # buffered_reader = BufferedStreamReader(content_generator)
    # xml_handler = StreamingXMLHandler(buffered_reader)
    xml_fixer = XMLFixerStreamReader(xml_handler, data_type=data_type)
    # content = fix_xml_structure(content, config['root_tag'])
    # context = etree.iterparse(xml_handler, events=('end',), recover=True)
    element_parser = XMLParser(data_type=data_type)

    chunk = []  

    for i, xml_chunk in enumerate(xml_fixer):
        parser = etree.XMLPullParser(events=('end',), recover=True)
        parser.feed(clean_xml_bytes(xml_chunk))
        # import ipdb
        # ipdb.set_trace()
        for event, elem in parser.read_events():
            if elem.tag == config['item_tag'] or elem.getparent().tag == config['root_tag']:
                # Clean the element
                # for child in elem.iter():
                #     if child.text:
                #         child.text = clean_text(child.text)
                #     if child.tail:
                #         child.tail = clean_text(child.tail)
                
                # Parse the cleaned element
                item_data = element_parser.parse_element(elem)
                if item_data:  # Only add non-empty dictionaries
                    chunk.append(item_data)
                
                if len(chunk) == chunk_size:
                    yield chunk
                    chunk = []
                
                # Clear the element to free up memory
                elem.clear()

    # Yield any remaining items
    if chunk:
        yield chunk
    # chunk = []
    # for event, elem in context:
    #     if elem.tag == config['item_tag'] or elem.getparent().tag == config['root_tag']:
    #         item_data = parser.parse_element(elem)
    #         if item_data:  # Only add non-empty dictionaries
    #             chunk.append(item_data)
    #         if len(chunk) == chunk_size:
    #             yield chunk
    #             chunk = []
    #         elem.clear()
    # if chunk:
    #     yield chunk







