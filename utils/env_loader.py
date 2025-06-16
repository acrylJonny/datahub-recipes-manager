"""
Utility module for loading environment variables from .env files
"""

import os
from dotenv import load_dotenv


def load_env_file(env_file=".env"):
    """
    Load environment variables from the specified .env file

    Args:
        env_file (str): Path to the .env file to load. Defaults to ".env"

    Returns:
        bool: True if the .env file was loaded successfully, False otherwise
    """
    if os.path.exists(env_file):
        load_dotenv(env_file)
        return True
    return False
