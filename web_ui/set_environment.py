#!/usr/bin/env python3
"""
Simple script to set the ENVIRONMENT variable for testing assertion functionality.
This ensures the add_assertion_to_pr function uses the correct environment.
"""

import os
import sys

def set_environment(env_name="dev"):
    """Set the ENVIRONMENT variable"""
    os.environ["ENVIRONMENT"] = env_name
    print(f"Environment set to: {env_name}")
    
    # Verify it's set
    print(f"ENVIRONMENT variable: {os.getenv('ENVIRONMENT')}")

if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "dev"
    set_environment(env) 