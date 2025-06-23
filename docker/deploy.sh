#!/bin/bash

# DataHub Recipes Manager - Docker Deployment Script
# This script helps deploy the application using Docker Compose

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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
    echo "DataHub Recipes Manager - Docker Deployment Script"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  up, start           Start the application (development mode)"
    echo "  prod                Start in production mode with nginx"
    echo "  down, stop          Stop the application"
    echo "  restart             Restart the application"
    echo "  logs                Show application logs"
    echo "  build               Build the Docker image"
    echo "  clean               Clean up containers and volumes"
    echo "  setup               Initial setup (create directories, build image)"
    echo ""
    echo "Options:"
    echo "  --build             Force rebuild of Docker image"
    echo "  --detach, -d        Run in detached mode"
    echo ""
    echo "Examples:"
    echo "  $0 setup                    # Initial setup"
    echo "  $0 up                       # Start in development mode"
    echo "  $0 prod                     # Start in production mode"
    echo "  $0 logs                     # View logs"
    echo ""
}

# Function to setup directories
setup_directories() {
    print_info "Creating required directories..."
    
    # Create directories if they don't exist
    mkdir -p data
    mkdir -p ../metadata-manager
    mkdir -p nginx/logs
    mkdir -p nginx/ssl
    
    # Set proper permissions
    chmod 755 data ../metadata-manager nginx/logs
    
    print_status "Directories created"
}

# Function to build image
build_image() {
    print_info "Building Docker image..."
    docker-compose build
    print_status "Docker image built"
}

# Function to start development mode
start_dev() {
    local detach_flag=""
    if [ "$1" = "--detach" ] || [ "$1" = "-d" ]; then
        detach_flag="-d"
    fi
    
    print_info "Starting DataHub Recipes Manager in development mode..."
    print_info "Using run_web_ui.sh script for consistent startup process"
    docker-compose up $detach_flag
}

# Function to start production mode
start_prod() {
    local detach_flag=""
    if [ "$1" = "--detach" ] || [ "$1" = "-d" ]; then
        detach_flag="-d"
    fi
    
    print_info "Starting DataHub Recipes Manager in production mode..."
    print_info "Using run_web_ui.sh script for consistent startup process"
    print_warning "Make sure SSL certificates are in place at nginx/ssl/"
    docker-compose -f docker-compose.prod.yml up $detach_flag
}

# Function to stop services
stop_services() {
    print_info "Stopping DataHub Recipes Manager..."
    docker-compose down
    docker-compose -f docker-compose.prod.yml down 2>/dev/null || true
    print_status "Services stopped"
}

# Function to show logs
show_logs() {
    print_info "Showing application logs..."
    docker-compose logs -f
}

# Function to clean up
cleanup() {
    print_warning "This will remove all containers and volumes. Are you sure? (y/N)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        print_info "Cleaning up containers and volumes..."
        docker-compose down -v
        docker-compose -f docker-compose.prod.yml down -v 2>/dev/null || true
        docker system prune -f
        print_status "Cleanup completed"
    else
        print_info "Cleanup cancelled"
    fi
}

# Parse arguments
BUILD_FLAG=""
DETACH_FLAG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --build)
            BUILD_FLAG="--build"
            shift
            ;;
        --detach|-d)
            DETACH_FLAG="--detach"
            shift
            ;;
        *)
            break
            ;;
    esac
done

# Main command handling
case "${1:-up}" in
    "help"|"-h"|"--help")
        show_help
        exit 0
        ;;
    "setup")
        setup_directories
        build_image
        print_status "Setup completed! You can now run: $0 up"
        ;;
    "up"|"start")
        setup_directories
        if [ -n "$BUILD_FLAG" ]; then
            build_image
        fi
        start_dev "$DETACH_FLAG"
        ;;
    "prod"|"production")
        setup_directories
        if [ -n "$BUILD_FLAG" ]; then
            build_image
        fi
        start_prod "$DETACH_FLAG"
        ;;
    "down"|"stop")
        stop_services
        ;;
    "restart")
        stop_services
        sleep 2
        start_dev "$DETACH_FLAG"
        ;;
    "logs")
        show_logs
        ;;
    "build")
        build_image
        ;;
    "clean")
        cleanup
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac 