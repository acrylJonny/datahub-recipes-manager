#!/bin/bash
set -e

# Configuration
CONTAINER_NAME="datahub_test_postgres"
NETWORK_NAME="datahub_network"
DATAHUB_GMS_SERVICE="datahub-gms"

echo "=== Verifying Docker Network Connectivity ==="

# Check if datahub_network exists
if ! docker network ls | grep -q $NETWORK_NAME; then
  echo "ERROR: DataHub network ($NETWORK_NAME) does not exist!"
  echo "Please run setup_test_env.sh first or start your DataHub environment."
  exit 1
else
  echo "✅ DataHub network ($NETWORK_NAME) exists"
fi

# Check if our container is running
if ! docker ps | grep -q $CONTAINER_NAME; then
  echo "ERROR: Test container ($CONTAINER_NAME) is not running!"
  echo "Please run setup_test_env.sh first."
  exit 1
else
  echo "✅ Test container ($CONTAINER_NAME) is running"
fi

# Check if our container is connected to the datahub_network
if ! docker network inspect $NETWORK_NAME | grep -q "\"$CONTAINER_NAME\""; then
  echo "ERROR: Test container ($CONTAINER_NAME) is not connected to the DataHub network!"
  echo "Connecting container to network..."
  docker network connect $NETWORK_NAME $CONTAINER_NAME
  echo "✅ Container connected to network"
else
  echo "✅ Test container is properly connected to the DataHub network"
fi

# Check if DataHub GMS service is running
if docker ps | grep -q $DATAHUB_GMS_SERVICE; then
  echo "✅ DataHub GMS service is running"
  
  # Test network connectivity from our container to DataHub GMS
  echo "Testing connectivity from $CONTAINER_NAME to $DATAHUB_GMS_SERVICE..."
  if docker exec $CONTAINER_NAME ping -c 2 $DATAHUB_GMS_SERVICE > /dev/null 2>&1; then
    echo "✅ Connection successful! Containers can communicate."
  else
    echo "⚠️ Cannot ping DataHub GMS. This might be expected if ping is not installed."
    echo "Trying to connect to DataHub GMS port 8080 instead..."
    
    # Try connecting to port 8080 using netcat or curl (if available in container)
    if docker exec $CONTAINER_NAME which nc > /dev/null 2>&1; then
      if docker exec $CONTAINER_NAME nc -zv $DATAHUB_GMS_SERVICE 8080 -w 5 > /dev/null 2>&1; then
        echo "✅ Network connection successful! Port 8080 is reachable."
      else
        echo "❌ Failed to connect to port 8080. Containers may not be able to communicate."
      fi
    elif docker exec $CONTAINER_NAME which curl > /dev/null 2>&1; then
      if docker exec $CONTAINER_NAME curl -s $DATAHUB_GMS_SERVICE:8080 > /dev/null 2>&1; then
        echo "✅ Network connection successful! Port 8080 is reachable."
      else
        echo "❌ Failed to connect to port 8080. Containers may not be able to communicate."
      fi
    else
      echo "⚠️ Cannot test connection, neither nc nor curl is available in the container."
      echo "Consider installing them for network testing."
    fi
  fi
else
  echo "⚠️ DataHub GMS service is not running, skipping connectivity test."
  echo "Start your DataHub environment to test connectivity."
fi

echo ""
echo "=== Environment Variables ==="
echo "Setting the following environment variables will ensure proper Docker networking for tests:"
echo "DATAHUB_TEST_ENV=true"
echo "DOCKER_COMPOSE_MODE=true"
echo ""
echo "These variables have been added to the .env file by the setup_test_env.sh script."
echo "They enable automatic container network resolution in the utils/docker_utils.py module."

echo ""
echo "=== Network Verification Complete ===" 