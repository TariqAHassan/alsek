"""

    Test Async Backends

"""
import asyncio
from typing import Any

import pytest

from alsek.storage.backends import AsyncBackend


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "test_data",
    [
        42,
        {"key": "value"},
    ],
)
async def test_pub_sub_serialization_async(test_data: Any, rolling_async_backend: AsyncBackend) -> None:
    """Test async pub/sub with various data types to ensure serialization works."""
    if not rolling_async_backend.SUPPORTS_PUBSUB:
        pytest.skip("Backend does not support pub/sub")

    channel = f"test-serialization-{type(test_data).__name__}"
    received_messages = list()

    async def publisher() -> None:
        # Small delay to ensure subscriber is ready
        await asyncio.sleep(0.1)
        await rolling_async_backend.pub(channel, test_data)

    # Start publisher as background task
    publisher_task = asyncio.create_task(publisher())

    # Subscribe and collect messages
    async for msg in rolling_async_backend.sub(channel):  # noqa
        if isinstance(msg, dict) and msg.get("type") == "message":
            received_messages.append(msg["data"])
            break

    # Wait for publisher to complete
    await asyncio.wait_for(publisher_task, timeout=2.0)

    assert len(received_messages) == 1
    assert received_messages[0] == test_data
    assert type(received_messages[0]) == type(test_data)
