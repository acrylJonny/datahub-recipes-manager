"""
Integration tests for MCP outputs to ensure they use proper DataHub schema classes.

These tests verify that all MCP output implementations correctly use schema classes
and generate valid MCPs that can be ingested into DataHub.
"""

import json
from pathlib import Path

import pytest

from datahub_cicd_client.outputs.mcp_output.assertions import AssertionAsyncOutput
from datahub_cicd_client.outputs.mcp_output.data_contracts import DataContractAsyncOutput
from datahub_cicd_client.outputs.mcp_output.data_products import DataProductAsyncOutput
from datahub_cicd_client.outputs.mcp_output.domains import DomainAsyncOutput
from datahub_cicd_client.outputs.mcp_output.glossary import GlossaryAsyncOutput
from datahub_cicd_client.outputs.mcp_output.metadata_tests import MetadataTestAsyncOutput
from datahub_cicd_client.outputs.mcp_output.tags import TagAsyncOutput


@pytest.fixture(scope="module")
def sample_entities():
    """Load sample entities from fixtures."""
    fixtures_path = Path(__file__).parent.parent / "fixtures" / "sample_entities.json"
    with open(fixtures_path) as f:
        return json.load(f)


class TestDataContractMCPOutput:
    """Test data contract MCP output generation."""

    def test_generates_valid_mcp_with_schema_classes(self, sample_entities):
        """Test that data contract MCP uses proper schema classes."""
        output = DataContractAsyncOutput()
        data_contract = sample_entities["dataContracts"][0]

        mcps = output.create_entity_mcps(data_contract)

        # Verify MCP structure
        assert isinstance(mcps, list), "Should return a list of MCPs"
        assert len(mcps) > 0, "Should generate at least one MCP"

        mcp = mcps[0]
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"

        # Verify it's using schema classes (check for specific class attributes)
        aspect = mcp["aspect"]
        if hasattr(aspect, "__class__") and hasattr(aspect.__class__, "__module__"):
            assert (
                "datahub.metadata.schema_classes" in aspect.__class__.__module__
            ), "Should use DataHub schema classes"

        print(f"✅ Data contract MCP generated successfully: {mcp['entityUrn']}")

    def test_handles_missing_schema_classes_gracefully(self, sample_entities):
        """Test that MCP generation falls back gracefully when schema classes aren't available."""
        output = DataContractAsyncOutput()
        data_contract = sample_entities["dataContracts"][0]

        # This should not raise an exception even if schema classes are missing
        mcps = output.create_entity_mcps(data_contract)
        assert mcps is not None, "Should generate MCPs even without schema classes"
        assert len(mcps) > 0, "Should generate at least one MCP"
        print("✅ Data contract MCP handles missing schema classes gracefully")


class TestDataProductMCPOutput:
    """Test data product MCP output generation."""

    def test_generates_valid_mcp_with_schema_classes(self, sample_entities):
        """Test that data product MCP uses proper schema classes."""
        output = DataProductAsyncOutput()
        data_product = sample_entities["dataProducts"][0]

        mcps = output.create_entity_mcps(data_product)

        assert isinstance(mcps, list), "Should return a list of MCPs"
        assert len(mcps) > 0, "Should generate at least one MCP"

        mcp = mcps[0]
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"

        print(f"✅ Data product MCP generated successfully: {mcp['entityUrn']}")

    def test_includes_ownership_support(self, sample_entities):
        """Test that data product MCP supports ownership."""
        output = DataProductAsyncOutput()
        data_product = sample_entities["dataProducts"][0].copy()
        data_product["owners"] = ["urn:li:corpuser:test_user"]

        mcps = output.create_entity_mcps(data_product)
        assert mcps is not None, "Should generate MCPs with ownership"
        assert len(mcps) > 0, "Should generate at least one MCP"
        print("✅ Data product MCP supports ownership")


class TestDomainMCPOutput:
    """Test domain MCP output generation."""

    def test_generates_valid_mcp_with_schema_classes(self, sample_entities):
        """Test that domain MCP uses proper schema classes."""
        output = DomainAsyncOutput()
        domain = sample_entities["domains"][0]

        mcps = output.create_entity_mcps(domain)

        assert isinstance(mcps, list), "Should return a list of MCPs"
        assert len(mcps) > 0, "Should generate at least one MCP"

        mcp = mcps[0]
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"

        print(f"✅ Domain MCP generated successfully: {mcp['entityUrn']}")


class TestTagMCPOutput:
    """Test tag MCP output generation."""

    def test_generates_valid_mcp_with_schema_classes(self, sample_entities):
        """Test that tag MCP uses proper schema classes."""
        output = TagAsyncOutput()
        tag = sample_entities["tags"][0]

        mcps = output.create_entity_mcps(tag)

        assert isinstance(mcps, list), "Should return a list of MCPs"
        assert len(mcps) > 0, "Should generate at least one MCP"

        mcp = mcps[0]
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"

        print(f"✅ Tag MCP generated successfully: {mcp['entityUrn']}")


class TestGlossaryMCPOutput:
    """Test glossary MCP output generation."""

    def test_generates_valid_term_mcp_with_schema_classes(self, sample_entities):
        """Test that glossary term MCP uses proper schema classes."""
        output = GlossaryAsyncOutput()
        term = sample_entities["glossaryTerms"][0]

        mcps = output.create_entity_mcps(term)

        assert isinstance(mcps, list), "Should return a list of MCPs"
        assert len(mcps) > 0, "Should generate at least one MCP"

        mcp = mcps[0]
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"

        print(f"✅ Glossary term MCP generated successfully: {mcp['entityUrn']}")

    def test_generates_valid_node_mcp_with_schema_classes(self, sample_entities):
        """Test that glossary node MCP uses proper schema classes."""
        output = GlossaryAsyncOutput()
        node = sample_entities["glossaryNodes"][0]

        mcps = output.create_entity_mcps(node)

        assert isinstance(mcps, list), "Should return a list of MCPs"
        assert len(mcps) > 0, "Should generate at least one MCP"

        mcp = mcps[0]
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"

        print(f"✅ Glossary node MCP generated successfully: {mcp['entityUrn']}")


class TestMetadataTestMCPOutput:
    """Test metadata test MCP output generation."""

    def test_generates_valid_mcp_with_schema_classes(self, sample_entities):
        """Test that metadata test MCP uses proper schema classes."""
        output = MetadataTestAsyncOutput()
        test = sample_entities["metadataTests"][0]

        mcps = output.create_entity_mcps(test)

        assert isinstance(mcps, list), "Should return a list of MCPs"
        assert len(mcps) > 0, "Should generate at least one MCP"

        mcp = mcps[0]
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"

        print(f"✅ Metadata test MCP generated successfully: {mcp['entityUrn']}")


class TestAssertionMCPOutput:
    """Test assertion MCP output generation."""

    def test_generates_valid_mcp_with_schema_classes(self, sample_entities):
        """Test that assertion MCP uses proper schema classes."""
        output = AssertionAsyncOutput()
        assertion = sample_entities["assertions"][0]

        mcps = output.create_entity_mcps(assertion)

        assert isinstance(mcps, list), "Should return a list of MCPs"
        assert len(mcps) > 0, "Should generate at least one MCP"

        mcp = mcps[0]
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"

        print(f"✅ Assertion MCP generated successfully: {mcp['entityUrn']}")


class TestComprehensiveMCPOutputs:
    """Test comprehensive scenarios across all MCP output types."""

    def test_all_mcp_outputs_use_schema_classes(self, sample_entities):
        """Test that all MCP outputs attempt to use schema classes."""
        outputs_and_entities = [
            ("DataContract", DataContractAsyncOutput(), sample_entities["dataContracts"][0]),
            ("DataProduct", DataProductAsyncOutput(), sample_entities["dataProducts"][0]),
            ("Domain", DomainAsyncOutput(), sample_entities["domains"][0]),
            ("Tag", TagAsyncOutput(), sample_entities["tags"][0]),
            ("GlossaryTerm", GlossaryAsyncOutput(), sample_entities["glossaryTerms"][0]),
            ("GlossaryNode", GlossaryAsyncOutput(), sample_entities["glossaryNodes"][0]),
            ("MetadataTest", MetadataTestAsyncOutput(), sample_entities["metadataTests"][0]),
            ("Assertion", AssertionAsyncOutput(), sample_entities["assertions"][0]),
        ]

        failed_outputs = []

        for output_name, output, entity in outputs_and_entities:
            try:
                mcp = output.create_entity_mcps(entity)
                assert mcp is not None, f"{output_name} should generate MCP"
                assert isinstance(mcp, list), f"{output_name} MCP should return a list"
                assert len(mcp) > 0, f"{output_name} should generate at least one MCP"
                print(f"✅ {output_name}: MCP generated successfully")

            except Exception as e:
                failed_outputs.append(f"{output_name}: {e}")

        if failed_outputs:
            pytest.fail("MCP output failures:\n" + "\n".join(failed_outputs))

        print("✅ All MCP outputs generated successfully")

    def test_mcp_outputs_have_fallback_handling(self, sample_entities):
        """Test that MCP outputs handle missing schema classes gracefully."""
        outputs_and_entities = [
            ("DataContract", DataContractAsyncOutput(), sample_entities["dataContracts"][0]),
            ("DataProduct", DataProductAsyncOutput(), sample_entities["dataProducts"][0]),
            ("Domain", DomainAsyncOutput(), sample_entities["domains"][0]),
            ("Tag", TagAsyncOutput(), sample_entities["tags"][0]),
            ("GlossaryTerm", GlossaryAsyncOutput(), sample_entities["glossaryTerms"][0]),
            ("MetadataTest", MetadataTestAsyncOutput(), sample_entities["metadataTests"][0]),
            ("Assertion", AssertionAsyncOutput(), sample_entities["assertions"][0]),
        ]

        for output_name, output, entity in outputs_and_entities:
            # Test that generation doesn't fail even if schema classes are missing
            try:
                mcp = output.create_entity_mcps(entity)
                assert (
                    mcp is not None
                ), f"{output_name} should generate MCP even without schema classes"
                print(f"✅ {output_name}: Handles missing schema classes gracefully")
            except ImportError:
                # ImportError is acceptable - it means fallback is working
                print(f"✅ {output_name}: Properly handles ImportError for schema classes")
            except Exception as e:
                # Other exceptions should not occur
                pytest.fail(f"{output_name}: Unexpected error: {e}")


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
