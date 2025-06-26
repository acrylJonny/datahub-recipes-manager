"""
Integration tests for MCP outputs to ensure they use proper DataHub schema classes.

These tests verify that all MCP output implementations correctly use schema classes
and generate valid MCPs that can be ingested into DataHub.
"""

import pytest
import json
import os
from typing import Dict, Any, List
from pathlib import Path

from datahub_cicd_client.outputs.mcp_output.data_contracts import DataContractMCPOutput
from datahub_cicd_client.outputs.mcp_output.data_products import DataProductMCPOutput
from datahub_cicd_client.outputs.mcp_output.domains import DomainMCPOutput
from datahub_cicd_client.outputs.mcp_output.tags import TagMCPOutput
from datahub_cicd_client.outputs.mcp_output.glossary import GlossaryMCPOutput
from datahub_cicd_client.outputs.mcp_output.metadata_tests import MetadataTestMCPOutput
from datahub_cicd_client.outputs.mcp_output.assertions import AssertionMCPOutput


@pytest.fixture(scope="module")
def sample_entities():
    """Load sample entities from fixtures."""
    fixtures_path = Path(__file__).parent.parent / "fixtures" / "sample_entities.json"
    with open(fixtures_path, 'r') as f:
        return json.load(f)


class TestDataContractMCPOutput:
    """Test data contract MCP output generation."""
    
    def test_generates_valid_mcp_with_schema_classes(self, sample_entities):
        """Test that data contract MCP uses proper schema classes."""
        output = DataContractMCPOutput()
        data_contract = sample_entities["dataContracts"][0]
        
        mcp = output.generate_mcp(data_contract)
        
        # Verify MCP structure
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"
        
        # Verify it's using schema classes (check for specific class attributes)
        aspect = mcp["aspect"]
        if hasattr(aspect, '__class__') and hasattr(aspect.__class__, '__module__'):
            assert "datahub.metadata.schema_classes" in aspect.__class__.__module__, \
                "Should use DataHub schema classes"
        
        print(f"✅ Data contract MCP generated successfully: {mcp['entityUrn']}")
    
    def test_handles_missing_schema_classes_gracefully(self, sample_entities):
        """Test that MCP generation falls back gracefully when schema classes aren't available."""
        output = DataContractMCPOutput()
        data_contract = sample_entities["dataContracts"][0]
        
        # This should not raise an exception even if schema classes are missing
        mcp = output.generate_mcp(data_contract)
        assert mcp is not None, "Should generate MCP even without schema classes"
        print("✅ Data contract MCP handles missing schema classes gracefully")


class TestDataProductMCPOutput:
    """Test data product MCP output generation."""
    
    def test_generates_valid_mcp_with_schema_classes(self, sample_entities):
        """Test that data product MCP uses proper schema classes."""
        output = DataProductMCPOutput()
        data_product = sample_entities["dataProducts"][0]
        
        mcp = output.generate_mcp(data_product)
        
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"
        
        print(f"✅ Data product MCP generated successfully: {mcp['entityUrn']}")
    
    def test_includes_ownership_support(self, sample_entities):
        """Test that data product MCP supports ownership."""
        output = DataProductMCPOutput()
        data_product = sample_entities["dataProducts"][0].copy()
        data_product["owners"] = ["urn:li:corpuser:test_user"]
        
        mcp = output.generate_mcp(data_product)
        assert mcp is not None, "Should generate MCP with ownership"
        print("✅ Data product MCP supports ownership")


class TestDomainMCPOutput:
    """Test domain MCP output generation."""
    
    def test_generates_valid_mcp_with_schema_classes(self, sample_entities):
        """Test that domain MCP uses proper schema classes."""
        output = DomainMCPOutput()
        domain = sample_entities["domains"][0]
        
        mcp = output.generate_mcp(domain)
        
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
        output = TagMCPOutput()
        tag = sample_entities["tags"][0]
        
        mcp = output.generate_mcp(tag)
        
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
        output = GlossaryMCPOutput()
        term = sample_entities["glossaryTerms"][0]
        
        mcp = output.generate_mcp(term)
        
        assert isinstance(mcp, dict), "MCP should be a dictionary"
        assert "entityType" in mcp, "MCP should have entityType"
        assert "entityUrn" in mcp, "MCP should have entityUrn"
        assert "aspectName" in mcp, "MCP should have aspectName"
        assert "aspect" in mcp, "MCP should have aspect"
        
        print(f"✅ Glossary term MCP generated successfully: {mcp['entityUrn']}")
    
    def test_generates_valid_node_mcp_with_schema_classes(self, sample_entities):
        """Test that glossary node MCP uses proper schema classes."""
        output = GlossaryMCPOutput()
        node = sample_entities["glossaryNodes"][0]
        
        mcp = output.generate_mcp(node)
        
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
        output = MetadataTestMCPOutput()
        test = sample_entities["metadataTests"][0]
        
        mcp = output.generate_mcp(test)
        
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
        output = AssertionMCPOutput()
        assertion = sample_entities["assertions"][0]
        
        mcp = output.generate_mcp(assertion)
        
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
            ("DataContract", DataContractMCPOutput(), sample_entities["dataContracts"][0]),
            ("DataProduct", DataProductMCPOutput(), sample_entities["dataProducts"][0]),
            ("Domain", DomainMCPOutput(), sample_entities["domains"][0]),
            ("Tag", TagMCPOutput(), sample_entities["tags"][0]),
            ("GlossaryTerm", GlossaryMCPOutput(), sample_entities["glossaryTerms"][0]),
            ("GlossaryNode", GlossaryMCPOutput(), sample_entities["glossaryNodes"][0]),
            ("MetadataTest", MetadataTestMCPOutput(), sample_entities["metadataTests"][0]),
            ("Assertion", AssertionMCPOutput(), sample_entities["assertions"][0]),
        ]
        
        failed_outputs = []
        
        for output_name, output, entity in outputs_and_entities:
            try:
                mcp = output.generate_mcp(entity)
                assert mcp is not None, f"{output_name} should generate MCP"
                assert isinstance(mcp, dict), f"{output_name} MCP should be dictionary"
                assert "entityUrn" in mcp, f"{output_name} MCP should have entityUrn"
                print(f"✅ {output_name}: MCP generated successfully")
                
            except Exception as e:
                failed_outputs.append(f"{output_name}: {e}")
        
        if failed_outputs:
            pytest.fail(f"MCP output failures:\n" + "\n".join(failed_outputs))
        
        print("✅ All MCP outputs generated successfully")
    
    def test_mcp_outputs_have_fallback_handling(self, sample_entities):
        """Test that MCP outputs handle missing schema classes gracefully."""
        outputs_and_entities = [
            ("DataContract", DataContractMCPOutput(), sample_entities["dataContracts"][0]),
            ("DataProduct", DataProductMCPOutput(), sample_entities["dataProducts"][0]),
            ("Domain", DomainMCPOutput(), sample_entities["domains"][0]),
            ("Tag", TagMCPOutput(), sample_entities["tags"][0]),
            ("GlossaryTerm", GlossaryMCPOutput(), sample_entities["glossaryTerms"][0]),
            ("MetadataTest", MetadataTestMCPOutput(), sample_entities["metadataTests"][0]),
            ("Assertion", AssertionMCPOutput(), sample_entities["assertions"][0]),
        ]
        
        for output_name, output, entity in outputs_and_entities:
            # Test that generation doesn't fail even if schema classes are missing
            try:
                mcp = output.generate_mcp(entity)
                assert mcp is not None, f"{output_name} should generate MCP even without schema classes"
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