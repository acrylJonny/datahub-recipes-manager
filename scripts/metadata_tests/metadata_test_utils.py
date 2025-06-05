#!/usr/bin/env python3
"""
Utilities for testing metadata quality and integrity.
"""

import json
import logging
import os
import sys
from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple

# Add the parent directory to the sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from utils.datahub_metadata_api import DataHubMetadataApiClient

logger = logging.getLogger(__name__)


class TestSeverity(Enum):
    """Severity level for test results"""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class TestResult:
    """Class representing the result of a metadata test"""

    def __init__(
        self,
        test_name: str,
        success: bool,
        message: str,
        entity_urn: Optional[str] = None,
        severity: TestSeverity = TestSeverity.ERROR,
        details: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize a test result

        Args:
            test_name: Name of the test
            success: Whether the test passed
            message: Message describing the test result
            entity_urn: URN of the entity being tested
            severity: Severity level of the test
            details: Additional details about the test result
        """
        self.test_name = test_name
        self.success = success
        self.message = message
        self.entity_urn = entity_urn
        self.severity = severity
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert test result to dictionary"""
        return {
            "testName": self.test_name,
            "success": self.success,
            "message": self.message,
            "entityUrn": self.entity_urn,
            "severity": self.severity.value,
            "details": self.details,
        }

    def __str__(self) -> str:
        """String representation of test result"""
        status = "PASS" if self.success else "FAIL"
        entity_info = f" - {self.entity_urn}" if self.entity_urn else ""
        return f"[{status}] {self.test_name}{entity_info}: {self.message}"


class MetadataTest(ABC):
    """Abstract base class for metadata tests"""

    def __init__(self, client: DataHubMetadataApiClient, name: Optional[str] = None):
        """
        Initialize a metadata test

        Args:
            client: DataHub metadata client
            name: Name of the test (defaults to class name)
        """
        self.client = client
        self.name = name or self.__class__.__name__

    @abstractmethod
    def run(self) -> List[TestResult]:
        """
        Run the test

        Returns:
            List of test results
        """
        pass

    def create_result(
        self,
        success: bool,
        message: str,
        entity_urn: Optional[str] = None,
        severity: TestSeverity = TestSeverity.ERROR,
        details: Optional[Dict[str, Any]] = None,
    ) -> TestResult:
        """
        Create a test result

        Args:
            success: Whether the test passed
            message: Message describing the test result
            entity_urn: URN of the entity being tested
            severity: Severity level of the test
            details: Additional details about the test result

        Returns:
            Test result object
        """
        return TestResult(
            test_name=self.name,
            success=success,
            message=message,
            entity_urn=entity_urn,
            severity=severity,
            details=details,
        )


class MetadataTestSuite:
    """Class representing a suite of metadata tests"""

    def __init__(self, name: str, client: DataHubMetadataApiClient):
        """
        Initialize a test suite

        Args:
            name: Name of the test suite
            client: DataHub metadata client
        """
        self.name = name
        self.client = client
        self.tests: List[MetadataTest] = []

    def add_test(self, test: MetadataTest) -> None:
        """
        Add a test to the suite

        Args:
            test: Test to add
        """
        self.tests.append(test)

    def run_all(self) -> Dict[str, List[TestResult]]:
        """
        Run all tests in the suite

        Returns:
            Dictionary mapping test names to test results
        """
        results: Dict[str, List[TestResult]] = {}

        for test in self.tests:
            logger.info(f"Running test: {test.name}")
            test_results = test.run()
            results[test.name] = test_results

            # Log results
            for result in test_results:
                log_level = logging.INFO if result.success else logging.ERROR
                logger.log(log_level, str(result))

        return results

    def to_dict(self, results: Dict[str, List[TestResult]]) -> Dict[str, Any]:
        """
        Convert test suite results to dictionary

        Args:
            results: Dictionary mapping test names to test results

        Returns:
            Dictionary representation of test suite results
        """
        test_results = {}
        for test_name, test_results_list in results.items():
            test_results[test_name] = [r.to_dict() for r in test_results_list]

        return {
            "name": self.name,
            "testCount": len(self.tests),
            "results": test_results,
        }

    def save_results(
        self, results: Dict[str, List[TestResult]], output_file: str
    ) -> None:
        """
        Save test suite results to file

        Args:
            results: Dictionary mapping test names to test results
            output_file: Path to output file
        """
        result_dict = self.to_dict(results)

        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_file, "w") as f:
            json.dump(result_dict, f, indent=4)

        logger.info(f"Test results saved to {output_file}")


# Common test functions
def check_required_fields(
    entity: Dict[str, Any], required_fields: List[str], entity_urn: Optional[str] = None
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Check if an entity has all required fields

    Args:
        entity: Entity to check
        required_fields: List of required field paths (dot notation)
        entity_urn: URN of the entity (for reporting)

    Returns:
        Tuple of (success, message, details)
    """
    missing_fields = []

    for field_path in required_fields:
        parts = field_path.split(".")
        current = entity

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                missing_fields.append(field_path)
                break

    if missing_fields:
        return (
            False,
            f"Entity is missing required fields: {', '.join(missing_fields)}",
            {"missingFields": missing_fields},
        )

    return True, "All required fields are present", None


def check_string_length(
    value: str,
    field_name: str,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Check if a string value has the required length

    Args:
        value: String value to check
        field_name: Name of the field being checked
        min_length: Minimum allowed length (None for no minimum)
        max_length: Maximum allowed length (None for no maximum)

    Returns:
        Tuple of (success, message, details)
    """
    if not isinstance(value, str):
        return (
            False,
            f"Field '{field_name}' is not a string",
            {"type": str(type(value))},
        )

    length = len(value)
    details = {"length": length}

    if min_length is not None and length < min_length:
        details["minLength"] = min_length
        return (
            False,
            f"Field '{field_name}' is too short ({length} chars, minimum {min_length})",
            details,
        )

    if max_length is not None and length > max_length:
        details["maxLength"] = max_length
        return (
            False,
            f"Field '{field_name}' is too long ({length} chars, maximum {max_length})",
            details,
        )

    return True, f"Field '{field_name}' has valid length", details
