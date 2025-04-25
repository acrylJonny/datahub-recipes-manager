#!/usr/bin/env python3
"""
Setup script for the DataHub Recipes Manager Web UI.
This script initializes the Django application, creates necessary directories, 
and provides instructions for running the web interface.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path

def print_step(message):
    """Print a step in the setup process with formatting."""
    print(f"\n\033[1;34m==> {message}\033[0m")

def print_success(message):
    """Print a success message with formatting."""
    print(f"\033[1;32m✓ {message}\033[0m")

def print_error(message):
    """Print an error message with formatting."""
    print(f"\033[1;31m✗ {message}\033[0m")

def print_warning(message):
    """Print a warning message with formatting."""
    print(f"\033[1;33m! {message}\033[0m")

def print_command(command):
    """Print a command with formatting."""
    print(f"\033[1;36m$ {command}\033[0m")

def run_command(command, check=True):
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            check=check, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            text=True
        )
        return result.returncode == 0, result.stdout
    except subprocess.CalledProcessError as e:
        print_error(f"Command failed: {e}")
        return False, str(e)

def check_python_version():
    """Check if the Python version is compatible."""
    print_step("Checking Python version")
    if sys.version_info < (3, 8):
        print_error("Python 3.8 or higher is required")
        sys.exit(1)
    print_success(f"Python {sys.version.split()[0]} detected")

def check_django_installed():
    """Check if Django is installed."""
    print_step("Checking Django installation")
    try:
        import django
        print_success(f"Django {django.get_version()} is installed")
        return True
    except ImportError:
        print_warning("Django not found, will be installed later")
        return False

def create_env_file():
    """Create a .env file if it doesn't exist."""
    print_step("Setting up environment file")
    env_path = Path(".env")
    
    if env_path.exists():
        print_warning(".env file already exists, not overwriting")
        return
    
    with open(env_path, "w") as f:
        f.write("""# DataHub connection settings
DATAHUB_GMS_URL=http://localhost:8080
DATAHUB_TOKEN=your_token_here

# Add your data source credentials below
# POSTGRES_PASSWORD=your_password
# MYSQL_PASSWORD=your_password
# SNOWFLAKE_PASSWORD=your_password
""")
    
    print_success(".env file created at ./.env")
    print_warning("Remember to update the .env file with your DataHub credentials")

def install_requirements():
    """Install required Python packages."""
    print_step("Installing required packages")
    
    requirements = [
        "django>=4.0.0",
        "python-dotenv>=0.19.0",
        "acryl-datahub>=0.10.0",
        "pyyaml>=6.0",
        "jinja2>=3.0.0",
        "requests>=2.25.0"
    ]
    
    for req in requirements:
        print_command(f"pip install {req}")
        success, output = run_command(f"pip install {req}")
        if not success:
            print_error(f"Failed to install {req}")
            return False
    
    print_success("All required packages installed")
    return True

def setup_web_ui_directories():
    """Create necessary directories for the web UI."""
    print_step("Setting up web UI directories")
    
    directories = [
        "web_ui/static",
        "web_ui/static/css",
        "web_ui/static/js",
        "web_ui/static/images",
        "web_ui/logs",
        "web_ui/media",
        "policies",
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print_success(f"Created directory: {directory}")
    
    return True

def initialize_django_project():
    """Initialize the Django project."""
    print_step("Initializing Django project")
    
    if not os.path.exists("web_ui/manage.py"):
        print_command("cd web_ui && python -m django startproject web_ui .")
        success, output = run_command("cd web_ui && python -m django startproject web_ui .")
        if not success:
            print_error("Failed to initialize Django project")
            return False
    else:
        print_warning("Django project already initialized")
    
    return True

def create_django_apps():
    """Create necessary Django apps."""
    print_step("Creating Django apps")
    
    apps = [
        "policy_manager",
        "script_runner",
        "template_manager",
        "test_runner"
    ]
    
    for app in apps:
        if not os.path.exists(f"web_ui/{app}"):
            print_command(f"cd web_ui && python manage.py startapp {app}")
            success, output = run_command(f"cd web_ui && python manage.py startapp {app}")
            if not success:
                print_error(f"Failed to create app: {app}")
                continue
            print_success(f"Created app: {app}")
        else:
            print_warning(f"App {app} already exists")
    
    return True

def run_django_migrations():
    """Run Django migrations."""
    print_step("Running Django migrations")
    
    print_command("cd web_ui && python manage.py makemigrations")
    success, output = run_command("cd web_ui && python manage.py makemigrations")
    if not success:
        print_error("Failed to create migrations")
        return False
    
    print_command("cd web_ui && python manage.py migrate")
    success, output = run_command("cd web_ui && python manage.py migrate")
    if not success:
        print_error("Failed to apply migrations")
        return False
    
    print_success("Migrations applied successfully")
    return True

def main():
    """Main function to set up the web UI."""
    print("\n" + "=" * 80)
    print(" DataHub Recipes Manager Web UI Setup ".center(80, "="))
    print("=" * 80 + "\n")
    
    check_python_version()
    django_installed = check_django_installed()
    create_env_file()
    
    if not django_installed:
        if not install_requirements():
            print_error("Failed to install required packages. Aborting setup.")
            sys.exit(1)
    
    setup_web_ui_directories()
    
    if initialize_django_project() and create_django_apps():
        run_django_migrations()
    
    print("\n" + "=" * 80)
    print(" Setup Complete ".center(80, "="))
    print("=" * 80 + "\n")
    
    print("To start the web UI:")
    print_command("cd web_ui && python manage.py runserver")
    
    print("\nAccess the web UI at: http://localhost:8000")
    print("\nRemember to update your .env file with your DataHub credentials.")
    print("\nFor more information, see README.md")

if __name__ == "__main__":
    main() 