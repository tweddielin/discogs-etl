import gzip
import requests
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Tuple, Dict, Literal
import hashlib
from dataclasses import dataclass
from pathlib import Path
import aiohttp
import asyncio
import logging, coloredlogs
from tqdm import tqdm
import tempfile
import shutil
import math
from collections import deque
from typing import Dict, List, Optional, Generator, Iterator, Any, Union

logger = logging.getLogger(__name__)
coloredlogs.install(level='INFO')

class BufferedStreamReader:
    def __init__(self, stream: Generator[bytes, None, None], buffer_size: int = 1024 * 1024):
        self.stream = stream
        self.buffer = deque()
        self.buffer_size = buffer_size
        self.total_size = 0
        self.position = 0
        self._stream_exhausted = False

    def read(self, size: int = -1) -> bytes:
        if size == -1:
            size = self.buffer_size

        # Try to fill buffer if needed and stream isn't exhausted
        while not self._stream_exhausted and (self.total_size - self.position < size):
            try:
                chunk = next(self.stream)
                if chunk:  # Only append if we got actual data
                    self.buffer.append(chunk)
                    self.total_size += len(chunk)
            except StopIteration:
                self._stream_exhausted = True
                break
            except Exception as e:
                self._stream_exhausted = True
                raise e

        result = b''
        while size > 0 and self.buffer:
            chunk = self.buffer[0]
            if len(chunk) <= size:
                result += chunk
                size -= len(chunk)
                self.position += len(chunk)
                self.buffer.popleft()
            else:
                result += chunk[:size]
                self.buffer[0] = chunk[size:]
                self.position += size
                size = 0

        return result


class StreamingXMLHandler:
    def __init__(self, reader: BufferedStreamReader):
        self.reader = reader
        self.buffer = b''
        self.error_count = 0
        self.max_errors = 5  # Maximum number of errors before giving up

    def read(self, size: int = -1) -> bytes:
        while len(self.buffer) < size:
            chunk = self.reader.read(size)
            if not chunk:
                break
            self.buffer += chunk

        if size == -1:
            result, self.buffer = self.buffer, b''
        else:
            result, self.buffer = self.buffer[:size], self.buffer[size:]

        return result

    def __iter__(self) -> Generator[bytes, None, None]:
        while True:
            chunk = self.read(8192)  # Read in 8KB chunks
            if not chunk:
                break
            yield chunk

class GzipStreamReader:
    """
    A streaming reader for gzip-compressed files that decompresses data on the fly.
    """
    def __init__(self, file_path: str, chunk_size: int = 8192):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.raw_file = None
        self.gzip_file = None
        self._initialize_stream()

    def _initialize_stream(self):
        self.raw_file = open(self.file_path, 'rb')
        self.gzip_file = gzip.GzipFile(fileobj=self.raw_file)

    def __iter__(self) -> Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        if self.gzip_file is None:
            raise ValueError("Stream not initialized or already closed")
        
        try:
            chunk = self.gzip_file.read(self.chunk_size)
            if not chunk:
                self.close()
                raise StopIteration
            return chunk
        except Exception as e:
            self.close()
            raise e

    def close(self):
        if self.gzip_file:
            try:
                self.gzip_file.close()
            finally:
                self.gzip_file = None
        
        if self.raw_file:
            try:
                self.raw_file.close()
            finally:
                self.raw_file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

@dataclass
class DownloadChunk:
    start: int
    end: int
    data: Optional[bytes] = None
    downloaded: bool = False

class OptimizedDownloader:
    def __init__(
        self,
        url: str,
        output_path: str,
        chunk_size: int = 8 * 1024 * 1024,  # 8MB default chunk size
        max_workers: int = 4,
        max_retries: int = 5,
        connect_timeout: int = 30,
        read_timeout: int = 300,  # 5 minutes read timeout
        verify_ssl: bool = True,
        max_chunks_in_memory: int = 100,  # Control memory usage
        algorithm: Literal["md5", "sha1", "sha256", "sha512"] = "sha256",
    ):
        self.url = url
        self.output_path = output_path
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.verify_ssl = verify_ssl
        self.max_chunks_in_memory = max_chunks_in_memory
        self.temp_dir = tempfile.mkdtemp()
        self._setup_logging()
        self.algorithm = algorithm
        self.hasher = self._get_hasher()

    def _get_hasher(self):
        hash_algorithms = {
            "md5": hashlib.md5(),
            "sha1": hashlib.sha1(),
            "sha256": hashlib.sha256(),
            "sha512": hashlib.sha512()
        }
        if self.algorithm.lower() not in hash_algorithms:
            raise ValueError(f"Unsupported algorithm. Choose from: {', '.join(hash_algorithms.keys())}")
    
        hasher = hash_algorithms[self.algorithm.lower()]
        return hasher
    
    def _setup_logging(self):
        # self.logger = logging.getLogger(__name__)
        self.logger = logger
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    async def _get_file_size(self) -> int:
        """Get total file size with retry logic"""
        timeout = aiohttp.ClientTimeout(total=self.connect_timeout)
        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.head(self.url, ssl=self.verify_ssl) as response:
                        if response.status == 200:
                            return int(response.headers.get('content-length', 0))
            except Exception as e:
                if attempt == self.max_retries - 1:
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        raise Exception("Failed to get file size")

    def _split_chunks(self, total_size: int) -> list[DownloadChunk]:
        """Split download into optimally sized chunks"""
        chunks = []
        # Calculate optimal chunk size based on file size and max workers
        optimal_chunks = self.max_workers * 4  # Keep workers busy
        chunk_size = max(
            min(
                self.chunk_size,
                math.ceil(total_size / optimal_chunks)
            ),
            1024 * 1024  # Minimum 1MB chunk
        )
        
        for start in range(0, total_size, chunk_size):
            end = min(start + chunk_size - 1, total_size - 1)
            chunks.append(DownloadChunk(start=start, end=end))
        
        return chunks

    async def _download_chunk_with_retries(
        self,
        session: aiohttp.ClientSession,
        chunk: DownloadChunk,
        chunk_file: Path,
        progress_bar: tqdm
    ) -> bool:
        """Download a chunk with retry logic"""
        headers = {'Range': f'bytes={chunk.start}-{chunk.end}'}
        timeout = aiohttp.ClientTimeout(
            total=None,  # No total timeout
            connect=self.connect_timeout,
            sock_read=self.read_timeout
        )

        for attempt in range(self.max_retries):
            try:
                async with session.get(
                    self.url,
                    headers=headers,
                    timeout=timeout,
                    ssl=self.verify_ssl
                ) as response:
                    if response.status == 206:  # Partial content
                        # Stream the chunk directly to a file
                        with open(chunk_file, 'wb') as f:
                            async for data in response.content.iter_chunks():
                                f.write(data[0])
                                progress_bar.update(len(data[0]))
                        return True
                    else:
                        self.logger.warning(f"Unexpected status code: {response.status}")
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Failed to download chunk after {self.max_retries} attempts: {e}")
                    raise
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        return False

    async def _download_chunks(self, chunks: list[DownloadChunk], total_size: int):
        """Download chunks with controlled concurrency"""
        temp_dir = Path(self.temp_dir)
        progress_bar = tqdm(total=total_size, unit='B', unit_scale=True)
        
        # Create semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(self.max_workers)
        timeout = aiohttp.ClientTimeout(
            total=None,  # No total timeout
            connect=self.connect_timeout,
            sock_read=self.read_timeout
        )
        
        async def download_chunk(chunk: DownloadChunk):
            chunk_file = temp_dir / f"chunk_{chunk.start}"
            async with semaphore:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    await self._download_chunk_with_retries(
                        session, chunk, chunk_file, progress_bar
                    )
                    return chunk.start, chunk_file

        # Download all chunks
        tasks = [download_chunk(chunk) for chunk in chunks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        progress_bar.close()
        
        # Check for any failures
        failures = [r for r in results if isinstance(r, Exception)]
        if failures:
            raise Exception(f"Failed to download {len(failures)} chunks: {failures[0]}")
        
        return dict(results)

    def _combine_chunks(self, chunk_files: Dict[int, Path]):
        """Combine chunks into final file"""
        with open(self.output_path, 'wb') as outfile:
            for start in sorted(chunk_files.keys()):
                chunk_file = chunk_files[start]
                with open(chunk_file, 'rb') as infile:
                    shutil.copyfileobj(infile, outfile)

    async def _download_with_chunking(self):
        """Main download method with chunking"""
        try:
            # Get file size
            total_size = await self._get_file_size()
            self.logger.info(f"Total file size: {total_size / (1024*1024):.2f} MB")
            
            # Split into chunks
            chunks = self._split_chunks(total_size)
            self.logger.info(f"Split into {len(chunks)} chunks")
            
            # Download chunks
            chunk_files = await self._download_chunks(chunks, total_size)
            
            # Combine chunks
            self.logger.info("Combining chunks...")
            self._combine_chunks(chunk_files)
            
            return True
        except Exception as e:
            self.logger.error(f"Download failed: {e}")
            raise
        finally:
            # Cleanup
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def download(self) -> str:
        """
        Start the download process
        Returns: SHA256 checksum of downloaded file
        """
        try:
            asyncio.run(self._download_with_chunking())
            
            # Calculate checksum
            self.calculated_checksum = self._calculate_checksum()
            return self.calculated_checksum
            
        except Exception as e:
            self.logger.error(f"Download failed: {e}")
            if Path(self.output_path).exists():
                Path(self.output_path).unlink()
            raise
    
    def _calculate_checksum(self) -> str:
        """Calculate SHA256 checksum of downloaded file"""
        with open(self.output_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                self.hasher.update(byte_block)
        return self.hasher.hexdigest() 
    
    def verify_checksum(self, expected_checksum=None) -> None:
        # Verify if expected checksum was provided
        verification_result = None
        if expected_checksum:
            verification_result = self.calculated_checksum.lower() == expected_checksum.lower()
            print(f"\nChecksum verification {'successful' if verification_result else 'failed'}!")
            print(f"Expected: {expected_checksum}")
            print(f"Actual:   {self.calculated_checksum}")
        else:
            print(f"\n{self.algorithm.upper()} Checksum: {self.calculated_checksum}")
    
        return self.calculated_checksum, verification_result

