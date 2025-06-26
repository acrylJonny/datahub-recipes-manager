"""
GraphQL queries for DataHub operations.

This module exports all GraphQL query constants used by the DataHub CICD Client services.
All queries are organized by entity type and operation for better maintainability.
"""

# Analytics queries
from .analytics import *

# Assertion queries
from .assertions import *

# Data contract queries
from .data_contracts import *

# Data product queries
from .data_products import *

# Domain queries
from .domains import *

# Editable entity queries
from .edited_data import *

# Glossary queries
from .glossary import *

# Group queries
from .groups import *

# Ingestion queries
from .ingestion import *

# Ownership type queries
from .ownership_types import *

# Policy queries
from .policies import *

# Property queries
from .properties import *

# Recipe queries
from .recipes import *

# Schema queries
from .schema import *

# Tag queries
from .tags import *

# Test queries
from .tests import *

# User queries
from .users import *
