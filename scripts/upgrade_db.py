#!/usr/bin/env python
"""
Database upgrade script for DataHub CI/CD Manager.
This script handles database migrations and upgrades when a new version is deployed.
"""

import os
import sys
import logging
import sqlite3
import subprocess
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('db_upgrade')

# Define the path to the database
DB_URL = os.getenv('DATABASE_URL', 'sqlite:///data/recipes_manager.db')
if DB_URL.startswith('sqlite:///'):
    DB_PATH = DB_URL[10:]
else:
    logger.error("Only SQLite databases are supported for automatic upgrades")
    sys.exit(1)

def ensure_directory_exists(db_path):
    """Ensure the directory for the database exists."""
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
        logger.info(f"Ensured database directory exists: {db_dir}")

def run_django_migrations():
    """Run Django migrations to update the database schema."""
    logger.info("Running Django migrations...")
    
    try:
        # Change to the Django project directory
        os.chdir('web_ui')
        
        # Run migrations
        result = subprocess.run(
            ['python', 'manage.py', 'migrate', '--noinput'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("Migrations completed successfully")
        else:
            logger.error(f"Migration failed: {result.stderr}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Error running migrations: {e}")
        sys.exit(1)
    finally:
        # Return to the original directory
        os.chdir('..')

def create_version_table(db_path):
    """Create a version table if it doesn't exist."""
    logger.info("Checking for version table...")
    
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check if the table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='app_version'"
        )
        if not cursor.fetchone():
            logger.info("Creating app_version table...")
            cursor.execute('''
                CREATE TABLE app_version (
                    id INTEGER PRIMARY KEY,
                    version TEXT NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            # Insert initial version
            cursor.execute(
                "INSERT INTO app_version (version) VALUES (?)",
                ["1.0.0"]  # Initial version
            )
            conn.commit()
            logger.info("Version table created and initialized")
        else:
            logger.info("Version table already exists")
    except Exception as e:
        logger.error(f"Error creating version table: {e}")
    finally:
        conn.close()

def get_current_version(db_path):
    """Get the current database version."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            "SELECT version FROM app_version ORDER BY id DESC LIMIT 1"
        )
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting current version: {e}")
        return None
    finally:
        conn.close()

def update_version(db_path, new_version):
    """Update the database version."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            "INSERT INTO app_version (version) VALUES (?)",
            [new_version]
        )
        conn.commit()
        logger.info(f"Updated database version to {new_version}")
    except Exception as e:
        logger.error(f"Error updating version: {e}")
    finally:
        conn.close()

def run_version_specific_upgrades(db_path, current_version):
    """Run version-specific upgrade logic."""
    # Define version upgrades in order
    upgrades = [
        # Add upgrade functions for specific versions here
        # Example: ('1.0.0', upgrade_to_1_0_0),
        # Each upgrade function should take the database path as an argument
    ]
    
    # Get applicable upgrades
    applicable_upgrades = []
    found_current = False
    
    for version, upgrade_func in upgrades:
        if not found_current:
            if version == current_version:
                found_current = True
            continue
        applicable_upgrades.append((version, upgrade_func))
    
    # Run applicable upgrades
    for version, upgrade_func in applicable_upgrades:
        logger.info(f"Running upgrade to version {version}...")
        try:
            upgrade_func(db_path)
            update_version(db_path, version)
            logger.info(f"Upgrade to version {version} completed")
        except Exception as e:
            logger.error(f"Error upgrading to version {version}: {e}")
            return False
    
    return True

def main():
    """Main entry point for the database upgrade script."""
    logger.info("Starting database upgrade process")
    
    # Ensure the database directory exists
    ensure_directory_exists(DB_PATH)
    
    # Create the version table if it doesn't exist
    create_version_table(DB_PATH)
    
    # Get the current version
    current_version = get_current_version(DB_PATH)
    logger.info(f"Current database version: {current_version}")
    
    # Run Django migrations
    run_django_migrations()
    
    # Run version-specific upgrades
    if current_version:
        success = run_version_specific_upgrades(DB_PATH, current_version)
        if not success:
            logger.error("Database upgrade failed")
            sys.exit(1)
    
    logger.info("Database upgrade completed successfully")

if __name__ == "__main__":
    main() 