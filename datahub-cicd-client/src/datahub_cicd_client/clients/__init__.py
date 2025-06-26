"""
Client services that combine multiple domain services for specific use cases.
These provide high-level interfaces suitable for CLI tools and CI/CD pipelines.
"""

from .graphql_connection import GraphQLConnection

__all__ = ["GraphQLConnection"]
