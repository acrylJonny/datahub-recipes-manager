#!/bin/bash

# DataHub Recipes Manager - Web UI Startup Script
# This script starts the Django web interface for managing DataHub recipes and metadata

echo "Starting DataHub Recipes Manager Web UI..."

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Creating one..."
    python3 -m venv venv
fi

# Activate virtual environment
source venv/bin/activate

# Install/upgrade dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Navigate to web_ui directory
cd web_ui

# Run Django migrations
echo "Running database migrations..."
python manage.py migrate

# Collect static files
echo "Collecting static files..."
python manage.py collectstatic --noinput

# Start the Django development server
echo "Starting Django development server..."
echo "Web UI will be available at: http://localhost:8000"
echo "Configure DataHub connection in the Settings page after startup."
echo ""
python manage.py runserver 0.0.0.0:8000 