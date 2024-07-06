"""

    Backoff Algorithms

"""
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, Dict, Optional, Type

from alsek._utils.printing import auto_repr

BackoffSettingsType = Dict[str, Any]


class Backoff(ABC):
    """Base backoff class.

    Args:
        floor (int, optional): minimum backoff in milliseconds
        ceiling (int, optional): maximum backoff in milliseconds
        zero_override (bool): override backoff to zero if the number
            of ``incidents`` is zero.

    """

    def __init__(
        self,
        floor: Optional[int] = 60 * 1000,
        ceiling: Optional[int] = 60 * 60 * 1000,
        zero_override: bool = True,
    ) -> None:
        self.floor = floor
        self.ceiling = ceiling
        self.zero_override = zero_override

        if floor is not None and ceiling is not None:
            if floor > ceiling:
                raise ValueError(f"floor ({floor} greater than ceiling ({ceiling})")
            if ceiling < floor:
                raise ValueError(f"ceiling ({ceiling}) less than floor ({floor})")

    @property
    @abstractmethod
    def parameters(self) -> Dict[str, Optional[int]]:
        """Parameters of the current instance which uniquely
        characterize it.

        Returns:
            params (dict): backoff parameters

        """
        raise NotImplementedError()

    def __repr__(self) -> str:
        return auto_repr(self, **self.parameters)

    @property
    def settings(self) -> BackoffSettingsType:
        """Settings the current algorithm.

        Returns:
            serialization (BackoffSettingsType): summary
                of the current algorithm and parameters with
                sufficient information to reconstruct it.

        """
        return dict(algorithm=self.__class__.__name__, parameters=self.parameters)

    @abstractmethod
    def formula(self, incidents: int) -> int:
        """Implementation of the formula for computing the backoff.

        Args:
            incidents (int): current number of incidents

        Returns:
            int

        """
        raise NotImplementedError()

    def _clipper(self, amount: int) -> int:
        if self.floor is not None and amount < self.floor:
            return self.floor
        elif self.ceiling is not None and amount > self.ceiling:
            return self.ceiling
        else:
            return amount

    def get(self, incidents: int) -> int:
        """Get the backoff.

        Args:
            incidents (int): current number of incidents

        Returns:
            backoff (int): backoff in milliseconds

        """
        if self.zero_override and incidents == 0:
            return 0
        return self._clipper(self.formula(incidents))


class ConstantBackoff(Backoff):
    """Constant backoff.

    Args:
        constant (int): amount of time (in milliseconds) to backoff.
        **kwargs (Keyword Args): keyword arguments to pass to
            ``Backoff``

    """

    def __init__(self, constant: int = 60 * 1000, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.constant = constant

    @property
    def parameters(self) -> Dict[str, Optional[int]]:
        """Parameters of the current ``ConstantBackoff``
        instance which uniquely characterize it.

        Returns:
            params (dict): backoff parameters

        """
        return dict(
            constant=self.constant,
            floor=self.floor,
            ceiling=self.ceiling,
            zero_override=self.zero_override,
        )

    def formula(self, incidents: int) -> int:
        """Constant backoff formula.

        Implements:

        $$c$$

        where $c$ is `constant`.

        Args:
            incidents (int): current number of incidents

        Returns:
            backoff (int): backoff in milliseconds

        """
        return self.constant


class LinearBackoff(Backoff):
    """Linear backoff.

    Args:
        factor (int): amount of time (in milliseconds) to add to backoff
            after each retry.
        **kwargs (Keyword Args): keyword arguments to pass to
            ``Backoff``

    """

    def __init__(self, factor: int = 30 * 1000, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.factor = factor

    @property
    def parameters(self) -> Dict[str, Optional[int]]:
        """Parameters of the current ``LinearBackoff``
        instance which uniquely characterize it.

        Returns:
            params (dict): backoff parameters

        """
        return dict(
            factor=self.factor,
            floor=self.floor,
            ceiling=self.ceiling,
            zero_override=self.zero_override,
        )

    def formula(self, incidents: int) -> int:
        """Linear backoff formula.

        Implements:

        $$f * i$$

        where $f$ is `factor` and $i$ is the number of `incidents`.

        Args:
            incidents (int): current number of incidents

        Returns:
            backoff (int): backoff in milliseconds

        """
        return int(self.factor * incidents)


class ExponentialBackoff(Backoff):
    """Exponential backoff.

    Args:
        base (int): the base of the exponential (milliseconds)
        factor (int): factor to multiply the result by
        **kwargs (Keyword Args): keyword arguments to pass to
            ``Backoff``

    """

    def __init__(self, base: int = 4, factor: int = 10_000, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.base = base
        self.factor = factor

    @property
    def parameters(self) -> Dict[str, Optional[int]]:
        """Parameters of the current ``ExponentialBackoff``
        instance which uniquely characterize it.

        Returns:
            params (dict): backoff parameters

        """
        return dict(
            base=self.base,
            factor=self.factor,
            floor=self.floor,
            ceiling=self.ceiling,
            zero_override=self.zero_override,
        )

    def formula(self, incidents: int) -> int:
        """Exponential backoff formula.

        Implements:

        $$f * (b^{i})$$

        where $f$ is `factor`, $b$ is `base` and $i$ is the number of `incidents`.

        Args:
            incidents (int): current number of incidents

        Returns:
            backoff (int): backoff in milliseconds

        """
        return int(self.factor * (self.base**incidents))


@lru_cache(maxsize=8)
def _get_algorithm(algorithm: str) -> Type[Backoff]:
    for s in Backoff.__subclasses__():
        if s.__name__ == algorithm:
            return s
    else:
        raise KeyError(f"No algorithm '{algorithm}' found")


def settings2backoff(settings: BackoffSettingsType) -> Backoff:
    """Convert backoff settings to a ``Backoff`` instance.

    Args:
        settings (BackoffSettingsType): backoff settings of
            the form ``{"algorithm": str, "parameters": dict}``.

    Returns:
        backoff (Backoff): a backoff instance

    """
    algorithm = _get_algorithm(settings["algorithm"])
    return algorithm(**settings["parameters"])
