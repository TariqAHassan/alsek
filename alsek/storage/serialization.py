"""

    Serialization

"""

import json
import gzip
from abc import ABC, abstractmethod
from base64 import b64decode, b64encode
from typing import Any, Optional

import dill

from alsek.utils.printing import auto_repr


class Serializer(ABC):
    """Base Serializer Class."""

    def __init__(self, compression_level: Optional[int] = None) -> None:
        self.compression_level = compression_level

    def __repr__(self) -> str:
        return auto_repr(self)

    @abstractmethod
    def encode(self, value: Any) -> str:
        """Compress a value

        Args:
            value (Any): value to compress

        Returns:
            Any: compressed value

        """
        raise NotImplementedError()

    @abstractmethod
    def decode(self, value: Any) -> Any:
        """Decompress the given value.

        Args:
            value (Any): value to decompress

        Returns:
            Any: decompressed value

        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def forward_engine(obj: Any) -> Any:
        """Engine to encode an object for backend serialization.

        Args:
            obj (Any): an object to encode

        Returns:
            encoded (Any): encoded object

        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def reverse_engine(obj: Any) -> Any:
        """Engine to decode an object.

        Args:
            obj (Any): an object to decode

        Returns:
            decoded (Any): decoded object

        """
        raise NotImplementedError()

    def forward(self, obj: Any) -> Any:
        """Encode an object for backend serialization.

        Args:
            obj (Any): an object to encode

        Returns:
            encoded (Any): encoded object

        """
        fwd = self.forward_engine(obj)
        return self.encode(fwd)

    def reverse(self, obj: Any) -> Any:
        """Engine to decode an object.

        Args:
            obj (Any): an object to decode

        Returns:
            decoded (Any): decoded object

        """
        fwd = self.decode(obj)
        return self.reverse_engine(fwd)


class JsonSerializer(Serializer):
    """JSON serialization."""

    def encode(self, value: str) -> str:
        """Encode a value using gzip if compression_level is set.

        Args:
            value (Any): value to encode

        Returns:
            str: encoded value (base64 encoded) or original value

        """
        if self.compression_level is None:
            return value

        # Compress and encode as base64
        compressed = gzip.compress(
            value.encode("utf-8"),
            compresslevel=self.compression_level,
        )
        return b64encode(compressed).decode("utf-8")

    def decode(self, value: Any) -> Any:
        """Decode the given value.

        Args:
            value (Any): value to decode

        Returns:
            Any: decoded value

        """
        if self.compression_level is None:
            return value

        try:
            # Decode base64 and decompress
            compressed_bytes = b64decode(value.encode("utf-8"))
            return gzip.decompress(compressed_bytes).decode("utf-8")
        except Exception:  # noqa
            return value  # fallback

    @staticmethod
    def forward_engine(obj: Any) -> Any:
        """Engine to encode an object.

        Args:
            obj (Any): an object to encode

        Returns:
            encoded (Any): JSON encoded object

        """
        return json.dumps(obj)

    @staticmethod
    def reverse_engine(obj: Any) -> Any:
        """Engine to decode an object.

        Args:
            obj (Any): an object to decode

        Returns:
            decoded (Any): JSON decoded object

        """
        if obj is None:
            return None
        return json.loads(obj)


class BinarySerializer(Serializer):
    """Binary serialization with optional gzip compression."""

    def _compress(self, data: bytes) -> bytes:
        if self.compression_level is not None:
            compressed_data = gzip.compress(data, compresslevel=self.compression_level)
            return compressed_data if len(compressed_data) < len(data) else data
        return data

    @staticmethod
    def _maybe_decompress(data: bytes) -> bytes:
        # Gzip files start with the magic header 0x1f, 0x8b
        if data.startswith(b"\x1f\x8b"):
            try:
                return gzip.decompress(data)
            except OSError:
                pass  # fall through if not actually gzipped
        return data

    def encode(self, value: bytes) -> str:
        """Encode a value using gzip if compression_level is set.

        Args:
            value (Any): value to encode

        Returns:
            str: encoded value

        """
        return b64encode(self._compress(value)).decode("utf-8")

    def decode(self, value: str | None) -> Optional[bytes]:
        """Decode a value using gzip if compression_level is set.

        Args:
            value (Any): value to decode

        Returns:
            bytes: decoded value

        """
        if value is None:
            return None
        raw = b64decode(value.encode("utf-8"))
        return self._maybe_decompress(raw)

    @staticmethod
    def forward_engine(obj: Any) -> bytes:
        """Encode an object.

        Args:
            obj (Any): an object to encode

        Returns:
            encoded (Any): base64 encoded ``dill``-serialized object

        """
        return dill.dumps(obj)

    @staticmethod
    def reverse_engine(obj: bytes | None) -> Any:
        """Decode an object.

        Args:
            obj (Any): an object to decode

        Returns:
            decoded (Any): ``dill``-deserialized object from base64 encoded string

        """
        if obj is None:
            return None
        return dill.loads(obj)
