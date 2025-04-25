#!/usr/bin/env python3
"""
Docker utilities for DataHub Recipe Manager.
This module helps with Docker networking and database connections
when running DataHub in a Docker Compose environment.
IMPORTANT: These utilities should only be used in testing environments.
"""

import os
import socket
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def is_testing_environment():
    """Check if we're running in a testing environment"""
    testing_indicators = [
        "TESTING",
        "TEST_MODE",
        "DATAHUB_TEST_ENV",
        "CI",
        "GITHUB_ACTIONS",
    ]

    for indicator in testing_indicators:
        if os.environ.get(indicator, "").lower() in ["true", "1", "yes"]:
            return True

    # Check for specific test directories in path
    current_path = os.getcwd()
    test_dirs = ["/test/", "/tests/", "/testing/"]
    for test_dir in test_dirs:
        if test_dir in current_path:
            return True

    return False


def is_in_docker():
    """Check if we're running inside a Docker container"""
    # Method 1: Check for /.dockerenv file
    if os.path.exists("/.dockerenv"):
        return True

    # Method 2: Check for cgroup info
    try:
        with open("/proc/1/cgroup", "r") as f:
            return "docker" in f.read()
    except (IOError, FileNotFoundError):
        # Method 3: Check for environment variable
        return os.environ.get("RUNNING_IN_DOCKER", "").lower() in ["true", "1", "yes"]


def should_apply_docker_networking():
    """
    Determine if Docker networking adaptations should be applied.
    Only returns true if we're both in a testing environment and in/using Docker.
    """
    return is_testing_environment() and (
        is_in_docker()
        or os.environ.get("DOCKER_COMPOSE_MODE", "").lower() in ["true", "1", "yes"]
    )


def resolve_docker_host(
    host: str, default_port: Optional[int] = None
) -> Dict[str, Any]:
    """
    Resolve a hostname in a Docker-aware way.

    When running in Docker, service names can be used as hostnames
    due to Docker's networking. This function helps resolve such hostnames
    correctly whether running inside Docker or not.

    Args:
        host: The hostname to resolve
        default_port: Optional default port if not specified in connection info

    Returns:
        Dict with host and port information
    """
    # Default connection info
    connection_info = {"host": host, "port": default_port}

    # Only apply Docker networking in test environments
    if not should_apply_docker_networking():
        logger.debug("Not applying Docker networking (not in testing environment)")
        return connection_info

    logger.info(
        "Test environment with Docker detected, applying network adaptations..."
    )

    # Check if we're in Docker environment
    docker_mode = is_in_docker()

    # Handle common database host names in Docker Compose
    docker_service_map = {
        # Database services
        "postgres": {"host": "postgres", "port": 5432},
        "postgresql": {"host": "postgres", "port": 5432},
        "mysql": {"host": "mysql", "port": 3306},
        "mssql": {"host": "mssql", "port": 1433},
        "sqlserver": {"host": "mssql", "port": 1433},
        "sql-server": {"host": "mssql", "port": 1433},
        "oracle": {"host": "oracle", "port": 1521},
        "mongodb": {"host": "mongodb", "port": 27017},
        "mongo": {"host": "mongodb", "port": 27017},
        "redis": {"host": "redis", "port": 6379},
        "elasticsearch": {"host": "elasticsearch", "port": 9200},
        "elastic": {"host": "elasticsearch", "port": 9200},
        # DataHub services
        "datahub-gms": {"host": "datahub-gms", "port": 8080},
        "datahub-frontend": {"host": "datahub-frontend", "port": 9002},
        "datahub-actions": {"host": "datahub-actions", "port": 8081},
        "datahub-mae-consumer": {"host": "datahub-mae-consumer", "port": None},
        "datahub-mce-consumer": {"host": "datahub-mce-consumer", "port": None},
        # Our test container
        "datahub_test_postgres": {"host": "datahub_test_postgres", "port": 5432},
    }

    # If we're in Docker mode and the host is a known service name
    if docker_mode and host.lower() in docker_service_map:
        service_info = docker_service_map[host.lower()]
        connection_info["host"] = service_info["host"]
        if default_port is None and "port" in service_info:
            connection_info["port"] = service_info["port"]

    # If we're not in Docker mode but using a Docker service name,
    # we need to use localhost instead
    elif not docker_mode and host.lower() in docker_service_map:
        logger.info(
            f"Not in Docker but using Docker service name '{host}'. Using 'localhost' instead."
        )
        connection_info["host"] = "localhost"
        if default_port is None and "port" in service_info:
            connection_info["port"] = service_info["port"]

    # For localhost in Docker, we need to use the host gateway
    # (host.docker.internal on modern Docker)
    elif docker_mode and host.lower() in ["localhost", "127.0.0.1"]:
        # On modern Docker, host.docker.internal works
        try:
            socket.gethostbyname("host.docker.internal")
            connection_info["host"] = "host.docker.internal"
            logger.info(
                "Using host.docker.internal to access host from Docker container"
            )
        except socket.gaierror:
            # Fallback to Docker gateway (older Docker)
            try:
                # Try to get the host gateway from /etc/hosts
                with open("/etc/hosts", "r") as hosts_file:
                    for line in hosts_file:
                        if "host-gateway" in line:
                            parts = line.strip().split()
                            if parts and parts[0]:
                                connection_info["host"] = parts[0]
                                logger.info(
                                    f"Using host gateway {parts[0]} to access host from Docker container"
                                )
                                break
            except Exception as e:
                logger.warning(f"Failed to determine Docker host gateway: {str(e)}")
                # Keep localhost as is, but warn
                logger.warning(
                    "Using 'localhost' within Docker may not work as expected"
                )

    logger.debug(f"Resolved connection info: {connection_info}")
    return connection_info


def update_connection_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update connection parameters to work correctly in Docker environment

    Args:
        params: Original connection parameters with host, port, etc.

    Returns:
        Updated connection parameters
    """
    # Only apply in testing environments
    if not should_apply_docker_networking():
        return params

    if "host" not in params:
        return params

    host = params.get("host", "localhost")
    port = params.get("port")

    # Resolve host considering Docker networking
    connection_info = resolve_docker_host(host, port)

    # Update the params with resolved info
    params["host"] = connection_info["host"]
    if connection_info["port"] is not None:
        params["port"] = connection_info["port"]

    return params
