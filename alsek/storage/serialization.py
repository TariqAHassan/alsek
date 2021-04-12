"""

    Serialization

"""
import json
from abc import ABC, abstractmethod
from typing import Any

from alsek._utils.printing import auto_repr


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
