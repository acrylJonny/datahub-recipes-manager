import os
import sys
import uuid
import json
import subprocess
import logging
import tempfile
from datetime import datetime
from django.conf import settings
from pathlib import Path
from .models import ScriptRun, ScriptResult, Artifact

logger = logging.getLogger(__name__)

def run_script(script_name, parameters, background=False):
    """
    Run a script with the given parameters.
    
    Args:
        script_name: Name of the script (without .py extension)
        parameters: Dictionary of parameters to pass to the script
        background: If True, run the script in the background
        
    Returns:
        ScriptRun object with the run details
    """
    # Create a ScriptRun record
    script_run = ScriptRun.objects.create(
        id=uuid.uuid4(),
        script_name=script_name,
        parameters=parameters,
        status='pending',
        created_at=datetime.now()
    )
    
    if background:
        # Start background process 
        logger.info(f"Starting background script: {script_name}")
        _run_script_in_background(script_run)
        return script_run
    else:
        # Run the script and wait for completion
        _run_script_sync(script_run)
        return script_run

def _run_script_sync(script_run):
    """Run script synchronously and update the run record with results."""
    script_path = os.path.join(settings.SCRIPTS_DIR, f"{script_run.script_name}.py")
    
    # Check if script exists
    if not os.path.exists(script_path):
        logger.error(f"Script not found: {script_path}")
        script_run.status = 'failed'
        script_run.completed_at = datetime.now()
        script_run.save()
        
        # Create result record
        ScriptResult.objects.create(
            script_run=script_run,
            output="",
            error=f"Script not found: {script_run.script_name}.py"
        )
        return script_run
    
    # Prepare command arguments
    args = [sys.executable, script_path]  # Use the Python executable
    
    # Add parameters as CLI arguments
    for key, value in script_run.parameters.items():
        if value:  # Skip empty parameters
            args.append(f"--{key}")
            args.append(str(value))
    
    # Create temporary directory for artifacts
    artifacts_dir = tempfile.mkdtemp(prefix="datahub_artifacts_")
    
    # Update run status
    script_run.status = 'running'
    script_run.started_at = datetime.now()
    script_run.save()
    
    try:
        # Run the process
        logger.info(f"Running script: {' '.join(args)}")
        env = os.environ.copy()
        env['PYTHONPATH'] = os.path.dirname(settings.SCRIPTS_DIR)
        env['ARTIFACTS_DIR'] = artifacts_dir
        
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env
        )
        
        # Capture output
        stdout, stderr = process.communicate()
        
        # Update run status
        script_run.status = 'success' if process.returncode == 0 else 'failed'
        script_run.completed_at = datetime.now()
        script_run.save()
        
        # Create result record
        result = ScriptResult.objects.create(
            script_run=script_run,
            output=stdout,
            error=stderr
        )
        
        # Process any artifacts
        _process_artifacts(result, artifacts_dir)
        
        return script_run
    
    except Exception as e:
        logger.exception(f"Error running script {script_run.script_name}: {str(e)}")
        
        # Update run status
        script_run.status = 'failed'
        script_run.completed_at = datetime.now()
        script_run.save()
        
        # Create result record
        ScriptResult.objects.create(
            script_run=script_run,
            output="",
            error=f"Error running script: {str(e)}"
        )
        
        return script_run

def _run_script_in_background(script_run):
    """Start a background process to run the script."""
    script_path = os.path.join(settings.SCRIPTS_DIR, f"{script_run.script_name}.py")
    
    # Check if script exists
    if not os.path.exists(script_path):
        logger.error(f"Script not found: {script_path}")
        script_run.status = 'failed'
        script_run.completed_at = datetime.now()
        script_run.save()
        
        # Create result record
        ScriptResult.objects.create(
            script_run=script_run,
            output="",
            error=f"Script not found: {script_run.script_name}.py"
        )
        return
    
    # Use subprocess.Popen to run without waiting
    try:
        # Start a new process that runs _run_script_sync
        runner_script = os.path.join(settings.BASE_DIR, 'background_runner.py')
        
        # Write a temp file with the run ID
        run_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        run_file.write(str(script_run.id))
        run_file.close()
        
        subprocess.Popen(
            [sys.executable, runner_script, run_file.name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True
        )
        
        logger.info(f"Started background process for script: {script_run.script_name}")
    except Exception as e:
        logger.exception(f"Error starting background process: {str(e)}")
        
        # Update run status
        script_run.status = 'failed'
        script_run.completed_at = datetime.now()
        script_run.save()
        
        # Create result record
        ScriptResult.objects.create(
            script_run=script_run,
            output="",
            error=f"Error starting background process: {str(e)}"
        )

def _process_artifacts(result, artifacts_dir):
    """Process any artifacts generated by the script."""
    artifacts_path = Path(artifacts_dir)
    
    # Check if the artifacts directory exists
    if not artifacts_path.exists():
        return
    
    # Process each file in the artifacts directory
    for file_path in artifacts_path.glob('*'):
        if file_path.is_file():
            # Determine content type based on extension
            if file_path.suffix == '.json':
                content_type = 'application/json'
            elif file_path.suffix == '.csv':
                content_type = 'text/csv'
            elif file_path.suffix == '.yml' or file_path.suffix == '.yaml':
                content_type = 'application/x-yaml'
            elif file_path.suffix == '.txt':
                content_type = 'text/plain'
            elif file_path.suffix in ['.png', '.jpg', '.jpeg', '.gif']:
                content_type = f'image/{file_path.suffix[1:]}'
            else:
                content_type = 'application/octet-stream'
            
            # Extract description if metadata file exists
            description = ""
            metadata_path = file_path.with_suffix('.meta.json')
            if metadata_path.exists():
                try:
                    with open(metadata_path, 'r') as f:
                        metadata = json.load(f)
                        description = metadata.get('description', '')
                except Exception as e:
                    logger.warning(f"Error reading metadata for {file_path.name}: {str(e)}")
            
            # Create an Artifact record
            try:
                artifact = Artifact(
                    script_result=result,
                    name=file_path.name,
                    description=description,
                    filename=file_path.name,
                    content_type=content_type
                )
                
                # Save the file content
                with open(file_path, 'rb') as f:
                    artifact.file.save(file_path.name, f)
                
                artifact.save()
                logger.info(f"Saved artifact: {file_path.name}")
            except Exception as e:
                logger.exception(f"Error saving artifact {file_path.name}: {str(e)}")
    
    # Clean up the artifacts directory
    try:
        for file_path in artifacts_path.glob('*'):
            file_path.unlink()
        artifacts_path.rmdir()
    except Exception as e:
        logger.warning(f"Error cleaning up artifacts directory: {str(e)}") 