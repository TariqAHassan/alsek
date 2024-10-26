"""

    Test Exceptions

"""

from typing import Type

import pytest

from alsek.exceptions import AlsekError


@pytest.mark.parametrize("exception", AlsekError.__subclasses__())
def test_exceptions(exception: Type[BaseException]) -> None:
    try:
        raise exception()
    except BaseException as error:
        assert isinstance(error, AlsekError)
