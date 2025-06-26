"""
MCP Builders Package.

This package contains builders for creating Metadata Change Proposals (MCPs)
for different DataHub entity types with comprehensive aspect support.
"""

from .base import BaseMCPBuilder
from .data_products import DataProductMCPBuilder
from .tags import TagMCPBuilder

# TODO: Import other builders as they're implemented
# from .domains import DomainMCPBuilder
# from .glossary import GlossaryMCPBuilder
# from .properties import StructuredPropertyMCPBuilder
# from .data_contracts import DataContractMCPBuilder
# from .assertions import AssertionMCPBuilder
# from .tests import TestMCPBuilder

__all__ = [
    "BaseMCPBuilder",
    "DataProductMCPBuilder",
    "TagMCPBuilder",
    # TODO: Add other builders as they're implemented
    # "DomainMCPBuilder",
    # "GlossaryMCPBuilder",
    # "StructuredPropertyMCPBuilder",
    # "DataContractMCPBuilder",
    # "AssertionMCPBuilder",
    # "TestMCPBuilder",
]
