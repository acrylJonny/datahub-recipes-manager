#!/usr/bin/env python3
"""
Utility functions for handling DataHub authentication tokens.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def get_token() -> Optional[str]:
    """
    Get the DataHub authentication token from environment variables.

    Returns:
        The token if available, None otherwise
    """
    token = os.environ.get("DATAHUB_TOKEN")

    # Empty or default token is treated as not provided
    if not token or token == "your_datahub_pat_token_here":
        logger.warning(
            "Warning: DATAHUB_TOKEN is not set. Will attempt to connect without authentication."
        )
        logger.warning(
            "This will only work if your DataHub instance doesn't require authentication."
        )
        return None

    return token
