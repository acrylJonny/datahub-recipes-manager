#!/bin/bash
# Script to run the DataHub CI/CD Manager Web UI

# Set the current directory to the script's directory
cd "$(dirname "$0")"

# Set default DataHub environment variables
if [ -z "$DATAHUB_GMS_URL" ]; then
    export DATAHUB_GMS_URL="http://localhost:8080"
    echo "Set default DATAHUB_GMS_URL to http://localhost:8080"
fi

if [ -z "$DATAHUB_TOKEN" ]; then
    # Prompt for DataHub token if not set
    echo "DataHub Personal Access Token not set."
    read -p "Enter your DataHub token (or press Enter to use placeholder): " token
    if [ -n "$token" ]; then
        export DATAHUB_TOKEN="$token"
        echo "DataHub token set."
    else
        # Use a placeholder token - this will prevent actual connections to DataHub
        export DATAHUB_TOKEN="your_datahub_token_here" 
        echo "Using placeholder token. You won't be able to connect to DataHub."
    fi
fi

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

# Run migrations to create/update tables
echo "Running migrations..."
cd web_ui
python manage.py migrate --no-input

# Start the Django development server
echo "Starting web server..."
python manage.py runserver