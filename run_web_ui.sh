#!/bin/bash

# DataHub Recipes Manager - Web UI Startup Script
# This script starts the Django web interface for managing DataHub recipes and metadata
# Supports both development (Django) and production (uvicorn) modes

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default configuration
DEFAULT_HOST="0.0.0.0"
DEFAULT_PORT="8000"
DEFAULT_WORKERS="4"
DEFAULT_MODE="development"

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}❌${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Function to show help
show_help() {
    echo "DataHub Recipes Manager - Web UI Startup Script"
    echo ""
    echo "Usage: $0 [MODE] [OPTIONS]"
    echo ""
    echo "Modes:"
    echo "  development, dev    Start Django development server (default)"
    echo "  production, prod    Start uvicorn ASGI production server"
    echo "  setup              Setup database and static files only"
    echo ""
    echo "Options for development mode:"
    echo "  --host HOST        Host to bind (default: $DEFAULT_HOST)"
    echo "  --port PORT        Port to bind (default: $DEFAULT_PORT)"
    echo ""
    echo "Options for production mode:"
    echo "  --host HOST        Host to bind (default: $DEFAULT_HOST)"
    echo "  --port PORT        Port to bind (default: $DEFAULT_PORT)"
    echo "  --workers NUM      Number of workers (default: $DEFAULT_WORKERS)"
    echo "  --reload           Enable auto-reload (single worker)"
    echo "  --ssl-key FILE     SSL private key file"
    echo "  --ssl-cert FILE    SSL certificate file"
    echo "  --log-level LEVEL  Log level: debug, info, warning, error (default: info)"
    echo ""
    echo "Examples:"
    echo "  $0                              # Start in development mode"
    echo "  $0 development --port 8001      # Development on port 8001"
    echo "  $0 production                   # Production with defaults"
    echo "  $0 production --workers 2       # Production with 2 workers"
    echo "  $0 setup                        # Setup only"
    echo ""
}

# Function to check virtual environment
check_venv() {
    if [ ! -d "venv" ]; then
        print_info "Virtual environment not found. Creating one..."
        python3 -m venv venv
    fi

    print_info "Activating virtual environment..."
    source venv/bin/activate
}

# Function to install dependencies
install_dependencies() {
    print_info "Installing/upgrading dependencies..."
    pip install -r requirements-web.txt
    print_status "Dependencies installed"
}

# Function to setup database and static files
setup_database_and_static() {
    print_info "Setting up database and static files..."
    
    # Navigate to web_ui directory
    cd web_ui

    # Run Django migrations
    print_info "Running database migrations..."
    python manage.py migrate
    print_status "Database migrations completed"

    # Collect static files
    print_info "Collecting static files..."
    python manage.py collectstatic --noinput
    print_status "Static files collected"
    
    # Go back to root directory
    cd ..
}

# Function to check if uvicorn is available for production mode
check_uvicorn() {
    if ! python3 -c "import uvicorn" 2>/dev/null; then
        print_error "uvicorn is not installed!"
        print_info "Installing uvicorn for production mode..."
        pip install 'uvicorn[standard]>=0.24.0'
        print_status "uvicorn installed"
    fi
}

# Function to start development server
start_development() {
    local host=${1:-$DEFAULT_HOST}
    local port=${2:-$DEFAULT_PORT}
    
    print_info "Starting Django development server..."
    print_info "Host: $host"
    print_info "Port: $port"
    print_info "Web UI will be available at: http://$host:$port"
    print_warning "This is a development server. Do not use in production!"
    echo ""
    
    cd web_ui
    python manage.py runserver "$host:$port"
}

# Function to start production server
start_production() {
    local host=${1:-$DEFAULT_HOST}
    local port=${2:-$DEFAULT_PORT}
    local workers=${3:-$DEFAULT_WORKERS}
    local reload=${4:-false}
    local ssl_key=${5:-""}
    local ssl_cert=${6:-""}
    local log_level=${7:-"info"}
    
    check_uvicorn
    
    print_info "Starting uvicorn ASGI production server..."
    print_info "Host: $host"
    print_info "Port: $port"
    
    # Change to web_ui directory like the development server
    cd web_ui
    
    # Build uvicorn command - use the correct settings module path
    local uvicorn_cmd="uvicorn asgi:application"
    uvicorn_cmd="$uvicorn_cmd --host $host --port $port --log-level $log_level"
    uvicorn_cmd="$uvicorn_cmd --server-header --date-header"
    
    if [ "$reload" = "true" ]; then
        uvicorn_cmd="$uvicorn_cmd --reload"
        print_warning "Running in development mode with auto-reload"
    else
        uvicorn_cmd="$uvicorn_cmd --workers $workers"
        print_info "Workers: $workers"
    fi
    
    # SSL configuration
    if [ -n "$ssl_key" ] && [ -n "$ssl_cert" ]; then
        if [ ! -f "$ssl_key" ] || [ ! -f "$ssl_cert" ]; then
            print_error "SSL certificate files not found!"
            print_info "Expected files: $ssl_key, $ssl_cert"
            exit 1
        fi
        uvicorn_cmd="$uvicorn_cmd --ssl-keyfile $ssl_key --ssl-certfile $ssl_cert"
        print_info "SSL enabled - Web UI will be available at: https://$host:$port"
    else
        print_info "Web UI will be available at: http://$host:$port"
    fi
    
    print_status "Starting production server..."
    echo ""
    
    # Execute uvicorn command from web_ui directory
    exec $uvicorn_cmd
}

# Parse command line arguments
MODE="$DEFAULT_MODE"
HOST="$DEFAULT_HOST"
PORT="$DEFAULT_PORT"
WORKERS="$DEFAULT_WORKERS"
RELOAD="false"
SSL_KEY=""
SSL_CERT=""
LOG_LEVEL="info"

# Parse first argument as mode if it's a known mode
case "$1" in
    "development"|"dev"|"production"|"prod"|"setup"|"help"|"-h"|"--help")
        MODE="$1"
        shift
        ;;
esac

# Parse remaining arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            HOST="$2"
            shift 2
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --reload)
            RELOAD="true"
            shift
            ;;
        --ssl-key)
            SSL_KEY="$2"
            shift 2
            ;;
        --ssl-cert)
            SSL_CERT="$2"
            shift 2
            ;;
        --log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            print_error "Unknown argument: $1"
            show_help
            exit 1
            ;;
    esac
done

# Main execution
echo "Starting DataHub Recipes Manager Web UI..."

case "$MODE" in
    "help"|"-h"|"--help")
        show_help
        exit 0
        ;;
    "setup")
        check_venv
        install_dependencies
        setup_database_and_static
        print_status "Setup completed! You can now start the server."
        ;;
    "development"|"dev")
        check_venv
        install_dependencies
        setup_database_and_static
        print_info "Configure DataHub connection in the Settings page after startup."
        start_development "$HOST" "$PORT"
        ;;
    "production"|"prod")
        check_venv
        install_dependencies
        setup_database_and_static
        print_info "Configure DataHub connection in the Settings page after startup."
        start_production "$HOST" "$PORT" "$WORKERS" "$RELOAD" "$SSL_KEY" "$SSL_CERT" "$LOG_LEVEL"
        ;;
    *)
        print_error "Unknown mode: $MODE"
        show_help
        exit 1
        ;;
esac 