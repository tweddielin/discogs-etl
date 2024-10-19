import re
from urllib.parse import urlparse
from typing import Dict, List, Optional, Any
from discogs_etl.config import DISCOGS_CONFIGS


def clean_xml_bytes(xml_bytes: bytes) -> bytes:
    """
    Clean XML byte string by removing unnecessary whitespace and handling escape characters.
    
    Args:
        xml_bytes (bytes): The input XML as bytes.
    
    Returns:
        bytes: Cleaned XML bytes.
    """
    # Decode bytes to string (UTF-8 is assumed, adjust if necessary)
    xml_str = xml_bytes.decode('utf-8', errors='replace')
    
    # Remove leading/trailing whitespace
    xml_str = xml_str.strip()
    
    # Replace multiple whitespace characters with a single space
    xml_str = re.sub(r'\s+', ' ', xml_str)
    
    # Preserve newlines within <notes> tags
    # xml_str = re.sub(r'(<notes>.*?</notes>)', lambda m: m.group(1).replace(' ', '\n'), xml_str, flags=re.DOTALL)
    
    # Handle escape characters
    # xml_str = xml_str.replace('&', '&amp;')  # Must be first
    # xml_str = xml_str.replace('<', '&lt;')
    # xml_str = xml_str.replace('>', '&gt;')
    # xml_str = xml_str.replace('"', '&quot;')
    # xml_str = xml_str.replace("'", '&apos;')
    
    # Convert back to bytes
    return xml_str

def clean_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    # Remove control characters except for \t, \n, and \r
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', text)
    # Convert XML entities to characters
    text = text.encode('utf-8').decode('unicode_escape')
    return text


def clean_xml_content(content):
    def replace_char(match):
        char = match.group()
        code = ord(char)
        if code < 0x20 and code not in (0x09, 0x0A, 0x0D):
            return " "  # Replace with space
        return char

    invalid_xml_char_regex = re.compile(r'[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF]')
    return invalid_xml_char_regex.sub(replace_char, content.decode('utf-8', errors='replace')).encode('utf-8')

def is_gzipped(content):
    return content[:2] == b'\x1f\x8b'


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