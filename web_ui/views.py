from django.shortcuts import render, redirect, get_object_or_404
from django.http import HttpResponse, JsonResponse
from django.contrib import messages
import os
import json
import subprocess
import logging
from datetime import datetime
from .models import ScriptRun, ScriptResult, Artifact
from . import runner
from pathlib import Path

logger = logging.getLogger(__name__)

# Project root directory
BASE_DIR = Path(__file__).resolve().parent.parent

def dashboard(request):
    """Main dashboard showing overview of the system."""
    recent_runs = ScriptRun.objects.order_by('-created_at')[:5]
    script_categories = {
        'Recipes': ['list_recipes', 'pull_recipe', 'push_recipe', 'validate_recipe', 'patch_recipe', 'delete_recipe'],
        'Secrets': ['list_secrets', 'create_secret', 'update_secret', 'delete_secret'],
        'Policies': ['list_policies', 'get_policy', 'create_policy', 'update_policy', 'delete_policy', 'export_policy', 'import_policy'],
        'Connections': ['test_connection']
    }
    
    stats = {
        'total_runs': ScriptRun.objects.count(),
        'successful_runs': ScriptRun.objects.filter(status='success').count(),
        'failed_runs': ScriptRun.objects.filter(status='failed').count(),
    }
    
    return render(request, 'dashboard.html', {
        'recent_runs': recent_runs,
        'script_categories': script_categories,
        'stats': stats
    })

def script_list(request):
    """Display a list of available scripts."""
    script_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts')
    scripts = []
    
    category = request.GET.get('category', '')
    
    script_categories = {
        'recipes': ['list_recipes', 'pull_recipe', 'push_recipe', 'validate_recipe', 'patch_recipe', 'delete_recipe'],
        'secrets': ['list_secrets', 'create_secret', 'update_secret', 'delete_secret'],
        'policies': ['list_policies', 'get_policy', 'create_policy', 'update_policy', 'delete_policy', 'export_policy', 'import_policy'],
        'connections': ['test_connection']
    }
    
    for filename in os.listdir(script_dir):
        if filename.endswith('.py') and not filename.startswith('__'):
            script_name = filename[:-3]
            # Filter by category if specified
            if category and script_name not in script_categories.get(category, []):
                continue
            
            # Read script file to extract description
            try:
                with open(os.path.join(script_dir, filename), 'r') as f:
                    content = f.read()
                    description = ""
                    for line in content.split('\n'):
                        if line.strip().startswith('"""') or line.strip().startswith("'''"):
                            description = line.strip()[3:].strip()
                            if description.endswith('"""') or description.endswith("'''"):
                                description = description[:-3].strip()
                            break
            except Exception as e:
                logger.error(f"Error reading script {filename}: {e}")
                description = "No description available"
            
            scripts.append({
                'name': script_name,
                'description': description,
                'filename': filename
            })
    
    return render(request, 'script_list.html', {
        'scripts': scripts,
        'categories': script_categories.keys(),
        'selected_category': category
    })

def script_history(request):
    """Display history of script runs."""
    runs = ScriptRun.objects.order_by('-created_at')
    return render(request, 'script_history.html', {'runs': runs})

def run_script(request, script_name):
    """Form to run a specific script with parameters."""
    if request.method == 'POST':
        # Collect parameters from the form
        params = {}
        for key, value in request.POST.items():
            if key != 'csrfmiddlewaretoken' and key != 'script_name':
                params[key] = value
        
        # Run the script
        try:
            result = runner.run_script(script_name, params)
            return redirect('script_result', result_id=result.id)
        except Exception as e:
            messages.error(request, f"Error running script: {str(e)}")
            return redirect('script_list')
    
    # GET request - display the form
    # Parse script to identify parameters
    script_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts', f"{script_name}.py")
    
    if not os.path.exists(script_path):
        messages.error(request, f"Script {script_name} not found")
        return redirect('script_list')
    
    # Extract parameter information from script
    parameters = []
    try:
        # Execute subprocess to get parameter details
        cmd = ['python', '-c', f"import sys; sys.path.append('.'); from scripts.{script_name} import parse_args; parser = parse_args(); print(json.dumps({{action.dest: {{ 'help': action.help, 'required': action.required, 'default': action.default }} for action in parser._actions if action.dest != 'help'}}))"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        params_info = json.loads(result.stdout)
        
        for param_name, param_info in params_info.items():
            parameters.append({
                'name': param_name,
                'help': param_info.get('help', ''),
                'required': param_info.get('required', False),
                'default': param_info.get('default', None)
            })
    except Exception as e:
        logger.error(f"Error parsing script parameters: {e}")
        parameters = []
    
    return render(request, 'run_script.html', {
        'script_name': script_name,
        'parameters': parameters
    })

def script_result(request, result_id):
    """Display the result of a script run."""
    result = get_object_or_404(ScriptResult, id=result_id)
    artifacts = Artifact.objects.filter(script_result=result)
    
    return render(request, 'script_result.html', {
        'result': result,
        'artifacts': artifacts
    })

def export_result(request, result_id):
    """Export script result as JSON."""
    result = get_object_or_404(ScriptResult, id=result_id)
    data = {
        'script_name': result.script_run.script_name,
        'status': result.script_run.status,
        'created_at': result.script_run.created_at.isoformat(),
        'completed_at': result.script_run.completed_at.isoformat() if result.script_run.completed_at else None,
        'parameters': result.script_run.parameters,
        'output': result.output,
        'error': result.error
    }
    
    response = HttpResponse(json.dumps(data, indent=2), content_type='application/json')
    response['Content-Disposition'] = f'attachment; filename="{result.script_run.script_name}_{result_id}.json"'
    return response

def view_artifact(request, artifact_id):
    """View an artifact generated by a script."""
    artifact = get_object_or_404(Artifact, id=artifact_id)
    
    # Handle different content types appropriately
    if artifact.content_type.startswith('image/'):
        return render(request, 'view_artifact.html', {'artifact': artifact})
    elif artifact.content_type in ['application/json', 'text/plain', 'text/html', 'text/csv']:
        content = artifact.file.read().decode('utf-8')
        return render(request, 'view_artifact.html', {
            'artifact': artifact,
            'content': content
        })
    else:
        # For other file types, suggest download
        return render(request, 'view_artifact.html', {
            'artifact': artifact,
            'download_only': True
        })

def download_artifact(request, artifact_id):
    """Download an artifact file."""
    artifact = get_object_or_404(Artifact, id=artifact_id)
    response = HttpResponse(artifact.file, content_type=artifact.content_type)
    response['Content-Disposition'] = f'attachment; filename="{artifact.filename}"'
    return response

def connection_settings(request):
    """Manage DataHub connection settings."""
    if request.method == 'POST':
        # Update settings
        # This is a simplified example - in production, use proper secrets management
        with open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'), 'w') as f:
            f.write(f"DATAHUB_GMS_URL={request.POST.get('gms_url', '')}\n")
            f.write(f"DATAHUB_TOKEN={request.POST.get('token', '')}\n")
        
        messages.success(request, "Connection settings updated successfully")
        return redirect('dashboard')
    
    # GET request - display current settings
    env_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
    settings = {
        'gms_url': '',
        'token': ''
    }
    
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    if key == 'DATAHUB_GMS_URL':
                        settings['gms_url'] = value
                    elif key == 'DATAHUB_TOKEN':
                        settings['token'] = value
    
    return render(request, 'connection_settings.html', {'settings': settings})

# Additional view functions for templates, policies, etc.
def list_templates(request):
    """List available templates."""
    # Implementation for listing templates
    return render(request, 'list_templates.html', {})

def edit_template(request, template_name):
    """Edit a specific template."""
    # Implementation for editing templates
    return render(request, 'edit_template.html', {'template_name': template_name})

def list_scripts(request):
    """List all available scripts"""
    scripts_dir = BASE_DIR / 'scripts'
    scripts = []
    
    for script_file in scripts_dir.glob('*.py'):
        if script_file.name.startswith('__'):
            continue
        
        name = script_file.stem
        description = ""
        
        # Read the first 10 lines to look for docstring
        with open(script_file, 'r') as f:
            lines = [f.readline() for _ in range(10)]
            for line in lines:
                if '"""' in line or "'''" in line:
                    description = line.strip().strip("'\"")
                    break
        
        scripts.append({
            'name': name,
            'path': script_file,
            'description': description
        })
    
    return render(request, 'scripts/list.html', {'scripts': scripts})

def list_policies(request):
    """List all policies"""
    try:
        # Run script to list policies
        result = subprocess.run(
            ['python', '-m', 'scripts.manage_policy', '--list'],
            capture_output=True,
            text=True,
            cwd=BASE_DIR
        )
        
        policies = []
        if result.returncode == 0:
            # Parse output to extract policies
            lines = result.stdout.splitlines()
            for line in lines:
                if line.startswith('Policy ID:'):
                    policy_id = line.split('Policy ID:')[1].strip()
                    policies.append({'id': policy_id})
        
        return render(request, 'policies/list.html', {
            'policies': policies,
            'output': result.stdout,
            'error': result.stderr
        })
    except Exception as e:
        return render(request, 'policies/list.html', {
            'policies': [],
            'error': str(e)
        })

def edit_policy(request, policy_id):
    """Edit a specific policy"""
    if request.method == 'POST':
        policy_name = request.POST.get('name')
        policy_description = request.POST.get('description')
        policy_type = request.POST.get('type')
        policy_state = request.POST.get('state')
        resources = request.POST.get('resources', '[]')
        privileges = request.POST.get('privileges', '[]')
        actors = request.POST.get('actors', '[]')
        
        # Construct command to update policy
        cmd = [
            'python', '-m', 'scripts.manage_policy', '--update',
            '--id', policy_id,
            '--name', policy_name,
            '--description', policy_description,
            '--type', policy_type,
            '--state', policy_state,
            '--resources', resources,
            '--privileges', privileges,
            '--actors', actors
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=BASE_DIR
            )
            if result.returncode == 0:
                return redirect('list_policies')
            else:
                return render(request, 'policies/edit.html', {
                    'policy': {
                        'id': policy_id,
                        'name': policy_name,
                        'description': policy_description,
                        'type': policy_type,
                        'state': policy_state,
                        'resources': resources,
                        'privileges': privileges,
                        'actors': actors
                    },
                    'error': result.stderr
                })
        except Exception as e:
            return render(request, 'policies/edit.html', {
                'policy': {
                    'id': policy_id,
                    'name': policy_name,
                    'description': policy_description,
                    'type': policy_type,
                    'state': policy_state,
                    'resources': resources,
                    'privileges': privileges,
                    'actors': actors
                },
                'error': str(e)
            })
    
    # GET request - load policy data
    try:
        # Run script to get policy details
        result = subprocess.run(
            ['python', '-m', 'scripts.manage_policy', '--get', '--id', policy_id],
            capture_output=True,
            text=True,
            cwd=BASE_DIR
        )
        
        policy = {
            'id': policy_id,
            'name': '',
            'description': '',
            'type': '',
            'state': '',
            'resources': '[]',
            'privileges': '[]',
            'actors': '[]'
        }
        
        if result.returncode == 0:
            # Try to parse JSON from output
            try:
                output_lines = result.stdout.strip().split('\n')
                json_str = '\n'.join([line for line in output_lines if line.strip().startswith('{')])
                if json_str:
                    policy_data = json.loads(json_str)
                    policy.update({
                        'name': policy_data.get('name', ''),
                        'description': policy_data.get('description', ''),
                        'type': policy_data.get('type', ''),
                        'state': policy_data.get('state', ''),
                        'resources': json.dumps(policy_data.get('resources', [])),
                        'privileges': json.dumps(policy_data.get('privileges', [])),
                        'actors': json.dumps(policy_data.get('actors', []))
                    })
            except json.JSONDecodeError:
                pass
        
        return render(request, 'policies/edit.html', {
            'policy': policy,
            'output': result.stdout,
            'error': result.stderr
        })
    except Exception as e:
        return render(request, 'policies/edit.html', {
            'policy': {'id': policy_id},
            'error': str(e)
        })

def list_tests(request):
    """List all available tests"""
    test_dir = BASE_DIR / 'test'
    tests = []
    
    # List all shell scripts in test directory
    for test_file in test_dir.glob('*.sh'):
        tests.append({
            'name': test_file.name,
            'path': test_file
        })
    
    return render(request, 'tests/list.html', {'tests': tests})

def run_test(request, test_name):
    """Run a specific test"""
    test_path = BASE_DIR / 'test' / test_name
    
    try:
        # Run the test script
        result = subprocess.run(
            ['bash', str(test_path)],
            capture_output=True,
            text=True,
            cwd=BASE_DIR
        )
        return render(request, 'tests/result.html', {
            'test_name': test_name,
            'output': result.stdout,
            'error': result.stderr,
            'exit_code': result.returncode
        })
    except Exception as e:
        return render(request, 'tests/result.html', {
            'test_name': test_name,
            'error': str(e),
            'exit_code': 1
        })

def connection_status(request):
    """Display the current connection status to DataHub."""
    return render(request, 'connection_status.html', {
        'datahub_config': request.datahub_config,
        'connection_status': request.datahub_connection,
    })

def api_scripts(request):
    """API endpoint to get the list of available scripts."""
    script_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts')
    scripts = []
    
    for filename in os.listdir(script_dir):
        if filename.endswith('.py') and not filename.startswith('__'):
            script_name = filename[:-3]
            
            # Read script file to extract description
            try:
                with open(os.path.join(script_dir, filename), 'r') as f:
                    content = f.read()
                    description = ""
                    for line in content.split('\n'):
                        if line.strip().startswith('"""') or line.strip().startswith("'''"):
                            description = line.strip()[3:].strip()
                            if description.endswith('"""') or description.endswith("'''"):
                                description = description[:-3].strip()
                            break
            except Exception as e:
                logger.error(f"Error reading script {filename}: {e}")
                description = "No description available"
            
            # Get parameter info
            parameters = []
            try:
                # Execute subprocess to get parameter details
                cmd = ['python', '-c', f"import sys; sys.path.append('.'); from scripts.{script_name} import parse_args; parser = parse_args(); print(json.dumps({{action.dest: {{ 'help': action.help, 'required': action.required, 'default': action.default }} for action in parser._actions if action.dest != 'help'}}))"]
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                params_info = json.loads(result.stdout)
                
                for param_name, param_info in params_info.items():
                    parameters.append({
                        'name': param_name,
                        'help': param_info.get('help', ''),
                        'required': param_info.get('required', False),
                        'default': param_info.get('default', None)
                    })
            except Exception as e:
                logger.error(f"Error parsing script parameters: {e}")
            
            scripts.append({
                'name': script_name,
                'description': description,
                'parameters': parameters
            })
    
    return JsonResponse({'scripts': scripts})

def api_run_status(request, run_id):
    """API endpoint to get the status of a script run."""
    try:
        script_run = ScriptRun.objects.get(id=run_id)
        
        # Get the result if available
        result = None
        try:
            result_obj = ScriptResult.objects.get(script_run=script_run)
            result = {
                'output': result_obj.output,
                'error': result_obj.error
            }
        except ScriptResult.DoesNotExist:
            pass
        
        # Get artifacts if available
        artifacts = []
        if result:
            for artifact in Artifact.objects.filter(script_result=result_obj):
                artifacts.append({
                    'id': str(artifact.id),
                    'name': artifact.name,
                    'description': artifact.description,
                    'filename': artifact.filename,
                    'content_type': artifact.content_type,
                    'url': request.build_absolute_uri(f'/artifacts/{artifact.id}/download/')
                })
        
        data = {
            'id': str(script_run.id),
            'script_name': script_run.script_name,
            'status': script_run.status,
            'created_at': script_run.created_at.isoformat(),
            'started_at': script_run.started_at.isoformat() if script_run.started_at else None,
            'completed_at': script_run.completed_at.isoformat() if script_run.completed_at else None,
            'parameters': script_run.parameters,
            'result': result,
            'artifacts': artifacts
        }
        
        return JsonResponse(data)
    except ScriptRun.DoesNotExist:
        return JsonResponse({'error': 'Run not found'}, status=404) 