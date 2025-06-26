"""
DataHub client exceptions.
"""


class DataHubError(Exception):
    """Base exception for DataHub client errors."""
    pass


class DataHubConnectionError(DataHubError):
    """Raised when connection to DataHub fails."""
    pass


class DataHubGraphQLError(DataHubError):
    """Raised when GraphQL query/mutation fails."""

    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors or []


class DataHubAuthenticationError(DataHubError):
    """Raised when authentication fails."""
    pass


class DataHubNotFoundError(DataHubError):
    """Raised when a requested resource is not found."""
    pass


class DataHubValidationError(DataHubError):
    """Raised when data validation fails."""
    pass
