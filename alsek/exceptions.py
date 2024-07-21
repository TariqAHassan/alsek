"""

    Exceptions

"""


class AlsekError(Exception):
    """Base Alsek error."""


class MessageAlreadyExistsError(AlsekError):
    """Message already exists in backend."""


class MessageDoesNotExistsError(AlsekError):
    """Message does not exists in backend."""


class MultipleBrokersError(AlsekError):
    """Multiple brokers in use."""


class NoTasksFoundError(AlsekError):
    """No tasks found."""


class ValidationError(AlsekError):
    """Data validation failed."""


class SchedulingError(AlsekError):
    """Error scheduling work."""


class TaskNameCollisionError(AlsekError):
    """Duplicate task detected."""


class TerminationError(AlsekError):
    """Alsek termination error."""


class AbortedError(AlsekError):
    """Alsek task aborted error."""
