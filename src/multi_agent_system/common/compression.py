"""Message compression to reduce bandwidth."""

import gzip
import logging
import json
from typing import Optional

logger = logging.getLogger('compression')


class MessageCompressor:
    """Compresses and decompresses messages."""

    def __init__(self, min_size: int = 1024, level: int = 6):
        """
        Args:
            min_size: Minimum payload size to compress (bytes)
            level: Compression level (1-9), 6 is balanced
        """
        self.min_size = min_size
        self.level = level

    def compress(self, data: str) -> bytes:
        """
        Compress string data using gzip.
        Returns raw compressed bytes.
        """
        return gzip.compress(data.encode('utf-8'), level=self.level)

    def decompress(self, compressed_data: bytes) -> str:
        """Decompress data back to string."""
        return gzip.decompress(compressed_data).decode('utf-8')

    def compress_json(self, obj: dict) -> dict:
        """
        Compress JSON-serializable object.
        Returns dict with compressed data and metadata.
        """
        json_str = json.dumps(obj)
        json_bytes = json_str.encode('utf-8')

        # Only compress if above minimum size
        if len(json_bytes) < self.min_size:
            return {'_raw': True, 'data': json_str}

        compressed = self.compress(json_str)
        return {
            '_compressed': True,
            '_size_original': len(json_bytes),
            '_size_compressed': len(compressed),
            'data': compressed.hex()  # Store as hex string
        }

    def decompress_json(self, obj: dict) -> dict:
        """
        Decompress JSON object from compress_json format.
        Auto-detects if compressed or raw.
        """
        if '_raw' in obj:
            return json.loads(obj['data'])

        if '_compressed' in obj:
            compressed_bytes = bytes.fromhex(obj['data'])
            json_str = self.decompress(compressed_bytes)
            return json.loads(json_str)

        # Not compressed, return as-is
        return obj


class PayloadCompressor:
    """Compresses large message payloads."""

    def __init__(self, compressor: MessageCompressor = None):
        self._compressor = compressor or MessageCompressor()

    def compress_message(self, message: dict) -> dict:
        """Compress message payload if large enough."""
        import copy
        compressed = copy.deepcopy(message)

        # Check if payload should be compressed
        if 'payload' in compressed:
            payload = compressed['payload']
            if isinstance(payload, dict):
                # Compress the payload
                payload_str = json.dumps(payload)
                if len(payload_str) >= self._compressor.min_size:
                    compressed_payload = self._compressor.compress_json(payload)
                    compressed['payload'] = compressed_payload
                    compressed['_payload_compressed'] = True

        return compressed

    def decompress_message(self, message: dict) -> dict:
        """Decompress message payload if compressed."""
        import copy
        decompressed = copy.deepcopy(message)

        if decompressed.get('_payload_compressed') and 'payload' in decompressed:
            payload = decompressed['payload']
            if isinstance(payload, dict) and '_compressed' in payload:
                decompressed['payload'] = self._compressor.decompress_json(payload)
                del decompressed['_payload_compressed']

        return decompressed


class StreamingCompressor:
    """Streaming compression for large data."""

    def __init__(self, chunk_size: int = 8192):
        self.chunk_size = chunk_size

    def compress_stream(self, data: str) -> bytes:
        """Compress data in chunks."""
        compressed_parts = []
        compressor = gzip.GzipFile(mode='wb', fileobj=None, compresslevel=6)

        # Write to BytesIO
        import io
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=6) as f:
            f.write(data.encode('utf-8'))

        return buf.getvalue()

    def decompress_stream(self, compressed: bytes) -> str:
        """Decompress streamed data."""
        import io
        buf = io.BytesIO(compressed)
        with gzip.GzipFile(fileobj=buf, mode='rb') as f:
            return f.read().decode('utf-8')


# Global compressor instance
_compressor = MessageCompressor()


def get_compressor() -> MessageCompressor:
    return _compressor


def estimate_compression_ratio(original_size: int) -> float:
    """Estimate compression ratio for text data."""
    # Rough estimate based on typical compression ratios
    if original_size < 1024:
        return 1.0
    elif original_size < 1024 * 100:
        return 0.5  # 50% size
    else:
        return 0.3  # 30% size for large data