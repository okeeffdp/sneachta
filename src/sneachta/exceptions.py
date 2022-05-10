
"""Exceptions module."""


class SnowflakeError(Exception):
    """Base for the Snowflake package."""

    pass


class SnowflakeConnectionError(SnowflakeError):
    """Raised for errors related to connecting to Snowflake."""

    pass


class SnowflakeExecutionError(SnowflakeError):
    """Raised when something failed with the execution of a query."""

    pass
