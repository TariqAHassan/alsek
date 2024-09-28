"""

    Test Exceptions

"""

import pytest

from alsek.exceptions import AlsekError


@pytest.mark.parametrize("exception", AlsekError.__subclasses__())
def test_exceptions(exception: BaseException) -> None:
    try:
        raise exception()
    except BaseException as error:
        assert isinstance(error, AlsekError)
