#!/usr/bin/env python3
"""
Utilities for DataHub authentication token handling.
"""

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def get_token_from_env(env_var_name: str = "DATAHUB_TOKEN") -> Optional[str]:
    """
    Get DataHub authentication token from environment variables

    Args:
        env_var_name: Name of the environment variable containing the token

    Returns:
        Token string or None if not found
    """
    return os.environ.get(env_var_name)


def get_token() -> Optional[str]:
    """
    Get the DataHub authentication token from environment variables.

    Returns:
        The token if available, None otherwise
    """
    token = get_token_from_env()

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
