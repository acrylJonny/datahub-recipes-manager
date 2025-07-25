#!/usr/bin/env python
"""
Test runner script for web_ui tests.

Usage:
    python run_tests.py all           # Run all tests
    python run_tests.py python        # Run Python view tests only
    python run_tests.py javascript    # Run JavaScript/Selenium tests only
    python run_tests.py html          # Run HTML template tests only
    python run_tests.py pages         # Run comprehensive page tests
    python run_tests.py integration   # Run integration tests only
    python run_tests.py unit          # Run unit tests only
    python run_tests.py fast          # Run fast tests (exclude slow/selenium)
    python run_tests.py coverage      # Run with coverage report
"""

import os
import sys
import subprocess
import argparse


def run_command(command):
    """Run a command and return the result."""
    print(f"Running: {command}")
    result = subprocess.run(command, shell=True, capture_output=False)
    return result.returncode


def main():
    parser = argparse.ArgumentParser(description='Run web_ui tests')
    parser.add_argument('test_type', nargs='?', default='all',
                       choices=['all', 'python', 'javascript', 'html', 'pages', 
                               'integration', 'unit', 'fast', 'coverage'],
                       help='Type of tests to run')
    
    args = parser.parse_args()
    
    # Base pytest command
    base_cmd = "python -m pytest tests/"
    
    # Test type specific commands
    commands = {
        'all': f"{base_cmd} -v",
        'python': f"{base_cmd}test_python_views.py -v",
        'javascript': f"{base_cmd}test_javascript_functionality.py -v -m selenium",
        'html': f"{base_cmd}test_html_templates.py -v",
        'pages': f"{base_cmd}test_pages.py -v",
        'integration': f"{base_cmd} -v -m integration",
        'unit': f"{base_cmd} -v -m unit",
        'fast': f"{base_cmd} -v -m 'not slow and not selenium'",
        'coverage': f"{base_cmd} -v --cov=web_ui --cov-report=html --cov-report=term"
    }
    
    # Run the selected test type
    command = commands.get(args.test_type, commands['all'])
    exit_code = run_command(command)
    
    if args.test_type == 'coverage':
        print("\nCoverage report generated in htmlcov/index.html")
    
    sys.exit(exit_code)


if __name__ == '__main__':
    main() 