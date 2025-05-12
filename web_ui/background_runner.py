#!/usr/bin/env python
"""
Background runner for DataHub scripts.

This script is executed as a separate process to run scripts in the background,
without blocking the web server. It loads the Django environment, reads the run ID
from a file, then executes the script synchronously.
"""

import os
import sys
import uuid
import django
import logging
from pathlib import Path

# Add the project directory to the Python path
BASE_DIR = Path(__file__).resolve().parent
sys.path.append(str(BASE_DIR.parent))

# Set up Django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web_ui.settings')
django.setup()

# Import Django models and functions after Django setup
from web_ui.models import ScriptRun
from web_ui.runner import _run_script_sync

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=os.path.join(BASE_DIR, 'logs', 'background_runner.log'),
    filemode='a'
)
logger = logging.getLogger('background_runner')

def main():
    """Run the script specified in the run file."""
    # Check command line arguments
    if len(sys.argv) != 2:
        logger.error("Usage: background_runner.py run_file")
        sys.exit(1)
    
    run_file = sys.argv[1]
    
    # Read the run ID from the file
    try:
        with open(run_file, 'r') as f:
            run_id = f.read().strip()
        
        # Delete the run file
        os.unlink(run_file)
        
        # Get the ScriptRun object
        try:
            script_run = ScriptRun.objects.get(id=uuid.UUID(run_id))
            logger.info(f"Running script in background: {script_run.script_name}")
            
            # Run the script
            _run_script_sync(script_run)
            logger.info(f"Script completed: {script_run.script_name}")
            
        except ScriptRun.DoesNotExist:
            logger.error(f"ScriptRun not found: {run_id}")
        except Exception as e:
            logger.exception(f"Error running script: {str(e)}")
            
    except Exception as e:
        logger.exception(f"Error reading run file {run_file}: {str(e)}")

if __name__ == '__main__':
    main() 