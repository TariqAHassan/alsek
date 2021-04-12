"""

    Exceptions

"""


class AlsekError(Exception):
    """Base Alsek error."""


class MessageDoesNotExistsError(AlsekError):
    """Message does not exists in backend."""


class MessageAlreadyExistsError(AlsekError):
    """Message already exists in backend."""


class TaskNameCollisionError(AlsekError):
    """Duplicate task detected."""


class NoTasksFoundError(AlsekError):
    """No tasks found."""


class MultipleBrokersError(AlsekError):
    """Multiple brokers in use."""


class SchedulingError(AlsekError):
    """Error scheduling work."""


class ValidationError(AlsekError):
    """Data validation failed."""


class TerminationError(AlsekError):
    """Alsek termination error."""
