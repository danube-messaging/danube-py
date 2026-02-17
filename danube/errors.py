class DanubeError(Exception):
    """Base exception for Danube client errors."""


class UnrecoverableError(DanubeError):
    """An error that cannot be resolved by retrying."""


class LookupError(DanubeError):
    """Topic lookup failed."""
