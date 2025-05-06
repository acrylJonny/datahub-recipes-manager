#!/bin/bash
# Script to run the DataHub CI/CD Manager Web UI

# Set the current directory to the script's directory
cd "$(dirname "$0")"

# Check if web_ui directory exists
if [ ! -d "web_ui" ]; then
    echo "Web UI directory not found. Running setup script..."
    python setup_web_ui.py
fi

# Check if Django is installed
if ! python -c "import django" &>/dev/null; then
    echo "Django not found. Installing requirements..."
    pip install -r requirements.txt
fi

# Start the Django development server
echo "Starting web server..."
cd web_ui
python manage.py runserver 