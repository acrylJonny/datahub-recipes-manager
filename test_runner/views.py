import os
import inspect
import importlib.util
from pathlib import Path
import subprocess
import sys
from django.http import JsonResponse
from django.shortcuts import render

def get_available_scripts():
    """Discover all executable scripts in the scripts directory."""
    # Get the correct path to the scripts directory
    base_dir = Path(__file__).resolve().parent.parent.parent
    scripts_dir = base_dir / "scripts"
    
    scripts = []
    
    for file in scripts_dir.glob("*.py"):
        if file.name.startswith("__"):
            continue
            
        try:
            # Load the module to get details
            spec = importlib.util.spec_from_file_location(file.stem, file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Get description from docstring
            description = inspect.getdoc(module) or "No description available"
            
            # Check if it has a main function
            has_main = hasattr(module, "main")
            
            # Get the parameters if main exists
            args = []
            if has_main:
                try:
                    args = inspect.getfullargspec(module.main).args
                except (TypeError, ValueError):
                    pass
            
            scripts.append({
                "name": file.stem,
                "path": str(file),
                "description": description,
                "has_main": has_main,
                "args": args
            })
        except Exception as e:
            # Skip files that can't be imported
            print(f"Error importing {file}: {e}")
    
    return scripts 

def run_test(request):
    if request.method == 'POST':
        test_script = request.POST.get('test_script')
        params = {}
        
        # Collect parameters from form
        for key, value in request.POST.items():
            if key.startswith('param_'):
                param_name = key[6:]  # Remove 'param_' prefix
                params[param_name] = value
                
        # Create environment variables for the test
        env = os.environ.copy()
        for key, value in params.items():
            env[key] = value
            
        # Run the test with custom environment
        process = subprocess.Popen(
            [sys.executable, test_script],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        stdout, _ = process.communicate()
        success = process.returncode == 0
        
        return JsonResponse({
            'success': success,
            'output': stdout,
            'exit_code': process.returncode
        })
        
    # GET request: show test form
    script_path = request.GET.get('script')
    test_scripts = get_available_scripts()
    
    # If a specific script is selected, filter the list
    selected_script = None
    if script_path:
        for script in test_scripts:
            if script['path'] == script_path:
                selected_script = script
                break
    
    if selected_script:
        test_scripts = [selected_script]
    
    return render(request, 'test_runner/run_test.html', {
        'test_scripts': test_scripts
    })

def list_tests(request):
    test_scripts = get_available_scripts()
    return render(request, 'test_runner/list_tests.html', {
        'test_scripts': test_scripts
    })
