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
        [],
        [42],
        [42, {"key": "value"}],
    ],
)
async def test_pub_sub_serialization_async(test_data: list[Any], rolling_async_backend: AsyncBackend) -> None:
    """Test async pub/sub with various data types to ensure serialization works."""
    if not rolling_async_backend.SUPPORTS_PUBSUB:
        pytest.skip("Backend does not support pub/sub")

    channel = f"test-serialization-{len(test_data)}"
    received_messages = list()

    async def publisher() -> None:
        # Small delay to ensure subscriber is ready
        await asyncio.sleep(0.1)
        for i in test_data:
            await rolling_async_backend.pub(channel, i)

    # Start publisher as background task
    publisher_task = asyncio.create_task(publisher())

    # Subscribe and collect messages
    if test_data:
        async for payload in rolling_async_backend.sub(channel):  # noqa
            if isinstance(payload, dict) and payload.get("type") == "message":
                received_messages.append(payload["data"])
            if len(received_messages) == len(test_data):
                break

    # Wait for publisher to complete
    await asyncio.wait_for(publisher_task, timeout=2.0)
    assert len(received_messages) == len(test_data)
    assert received_messages == test_data
