"""

    Serialization

"""
import json
from abc import ABC, abstractmethod
from base64 import b64decode, b64encode
from typing import Any

import dill

from alsek.utils.printing import auto_repr


class Serializer(ABC):
    """Base Serializer Class."""

    def __repr__(self) -> str:
        return auto_repr(self)

    @staticmethod
    @abstractmethod
    def forward(obj: Any) -> Any:
        """Encode an object for backend serialization.

        Args:
            obj (Any): an object to encode

        Returns:
            encoded (Any): encoded object

        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def reverse(obj: Any) -> Any:
        """Decode an object.

        Args:
            obj (Any): an object to decode

        Returns:
            decoded (Any): decoded object

        """
        raise NotImplementedError()


class JsonSerializer(Serializer):
    """JSON serialization."""

    @staticmethod
    def forward(obj: Any) -> Any:
        """Encode an object.

        Args:
            obj (Any): an object to encode

        Returns:
            encoded (Any): JSON encoded object

        """
        return json.dumps(obj)

    @staticmethod
    def reverse(obj: Any) -> Any:
        """Decode an object.

        Args:
            obj (Any): an object to decode

        Returns:
            decoded (Any): JSON decoded object

        """
        if obj is None:
            return None
        return json.loads(obj)


class BinarySerializer(Serializer):
    """Binary serialization."""

    @staticmethod
    def forward(obj: Any) -> Any:
        """Encode an object.

        Args:
            obj (Any): an object to encode

        Returns:
            encoded (Any): base64 encoded ``dill``-serialized object

        """
        dill_bytes = dill.dumps(obj)
        return b64encode(dill_bytes).decode("utf-8")

    @staticmethod
    def reverse(obj: Any) -> Any:
        """Decode an object.

        Args:
            obj (Any): an object to decode

        Returns:
            decoded (Any): ``dill``-deserialized object from base64 encoded string

        """
        if obj is None:
            return None
        base64_bytes = obj.encode("utf-8")
        return dill.loads(b64decode(base64_bytes))
