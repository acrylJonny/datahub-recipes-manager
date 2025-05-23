#!/usr/bin/env python3
"""
Tests for verifying relationships between DataHub entities.
"""

import logging
from typing import Dict, Any, List, Optional, Set, Tuple

from scripts.metadata_tests.metadata_test_utils import (
    MetadataTest,
    TestResult,
    TestSeverity
)

logger = logging.getLogger(__name__)


class GlossaryTermsInDomainsTest(MetadataTest):
    """Test that glossary terms are assigned to appropriate domains"""
    
    def run(self) -> List[TestResult]:
        results = []
        
        # Get all domains to build a set of valid domain URNs
        domains = self.client.list_domains()
        domain_urns = {domain.get("urn") for domain in domains if domain.get("urn")}
        
        # Get all glossary terms
        terms = self.client.list_glossary_terms()
        
        for term in terms:
            term_urn = term.get("urn")
            if not term_urn:
                continue
                
            # Get detailed term info
            term_info = self.client.export_glossary_term(term_urn)
            if not term_info:
                results.append(self.create_result(
                    False,
                    f"Failed to retrieve detailed info for glossary term: {term_urn}",
                    entity_urn=term_urn
                ))
                continue
            
            # Check if term has domains
            has_domain = False
            invalid_domains = []
            
            if "domains" in term_info and "domains" in term_info["domains"]:
                term_domains = term_info["domains"]["domains"]
                
                if term_domains:
                    has_domain = True
                    
                    # Check if all domains are valid
                    for domain in term_domains:
                        domain_urn = domain.get("urn")
                        if domain_urn and domain_urn not in domain_urns:
                            invalid_domains.append(domain_urn)
            
            # Create test results
            term_name = term_info.get("properties", {}).get("name", term_urn)
            
            if not has_domain:
                results.append(self.create_result(
                    False,
                    f"Glossary term '{term_name}' is not assigned to any domain",
                    entity_urn=term_urn,
                    severity=TestSeverity.WARNING,
                    details={"missingDomain": True}
                ))
            elif invalid_domains:
                results.append(self.create_result(
                    False,
                    f"Glossary term '{term_name}' is assigned to invalid domains: {', '.join(invalid_domains)}",
                    entity_urn=term_urn,
                    details={"invalidDomains": invalid_domains}
                ))
            else:
                results.append(self.create_result(
                    True,
                    f"Glossary term '{term_name}' is properly assigned to valid domains",
                    entity_urn=term_urn,
                    severity=TestSeverity.INFO
                ))
        
        return results


class DomainOwnershipTest(MetadataTest):
    """Test that domains have ownership information"""
    
    def run(self) -> List[TestResult]:
        results = []
        
        # Get all domains
        domains = self.client.list_domains()
        
        for domain in domains:
            domain_urn = domain.get("urn")
            if not domain_urn:
                continue
                
            # Get detailed domain info
            domain_info = self.client.export_domain(domain_urn)
            if not domain_info:
                results.append(self.create_result(
                    False,
                    f"Failed to retrieve detailed info for domain: {domain_urn}",
                    entity_urn=domain_urn
                ))
                continue
            
            # Check if domain has ownership information
            has_owners = False
            owner_count = 0
            
            if "ownership" in domain_info and "owners" in domain_info["ownership"]:
                owners = domain_info["ownership"]["owners"]
                owner_count = len(owners)
                has_owners = owner_count > 0
            
            # Create test results
            domain_name = domain_info.get("properties", {}).get("name", domain_urn)
            
            if not has_owners:
                results.append(self.create_result(
                    False,
                    f"Domain '{domain_name}' has no owners assigned",
                    entity_urn=domain_urn,
                    severity=TestSeverity.ERROR,
                    details={"ownerCount": 0}
                ))
            else:
                results.append(self.create_result(
                    True,
                    f"Domain '{domain_name}' has {owner_count} owner(s) assigned",
                    entity_urn=domain_urn,
                    severity=TestSeverity.INFO,
                    details={"ownerCount": owner_count}
                ))
        
        return results


class GlossaryHierarchyTest(MetadataTest):
    """Test the glossary node and term hierarchy structure"""
    
    def run(self) -> List[TestResult]:
        results = []
        
        # Get all glossary nodes
        nodes = self.client.list_glossary_nodes()
        node_urns = {node.get("urn") for node in nodes if node.get("urn")}
        
        # Track hierarchy issues
        orphaned_nodes = []
        circular_refs = set()
        
        # Check node hierarchy
        for node in nodes:
            node_urn = node.get("urn")
            if not node_urn:
                continue
            
            # Get detailed node info
            node_info = self.client.export_glossary_node(node_urn, include_children=True)
            if not node_info:
                continue
            
            # Check parent-child relationships
            if "parentNode" in node_info:
                parent_urn = node_info["parentNode"].get("urn")
                
                if parent_urn and parent_urn not in node_urns:
                    orphaned_nodes.append((node_urn, parent_urn))
                
                # Check for circular references (simplified check)
                if parent_urn == node_urn:
                    circular_refs.add(node_urn)
        
        # Create test results for hierarchy issues
        if orphaned_nodes:
            for node_urn, parent_urn in orphaned_nodes:
                results.append(self.create_result(
                    False,
                    f"Glossary node references non-existent parent node: {parent_urn}",
                    entity_urn=node_urn,
                    severity=TestSeverity.ERROR,
                    details={"nodeUrn": node_urn, "parentUrn": parent_urn}
                ))
        
        if circular_refs:
            for node_urn in circular_refs:
                results.append(self.create_result(
                    False,
                    f"Glossary node has circular reference (references itself as parent)",
                    entity_urn=node_urn,
                    severity=TestSeverity.ERROR,
                    details={"circularReference": True}
                ))
        
        # Get glossary terms and check their parent nodes
        terms = self.client.list_glossary_terms()
        
        for term in terms:
            term_urn = term.get("urn")
            if not term_urn:
                continue
            
            # Get detailed term info
            term_info = self.client.export_glossary_term(term_urn)
            if not term_info:
                continue
            
            # Check parent node
            if "parentNode" in term_info:
                parent_urn = term_info["parentNode"].get("urn")
                
                if parent_urn and parent_urn not in node_urns:
                    term_name = term_info.get("properties", {}).get("name", term_urn)
                    results.append(self.create_result(
                        False,
                        f"Glossary term '{term_name}' references non-existent parent node: {parent_urn}",
                        entity_urn=term_urn,
                        severity=TestSeverity.ERROR,
                        details={"termUrn": term_urn, "parentNodeUrn": parent_urn}
                    ))
        
        # Add a success result if no issues found
        if not orphaned_nodes and not circular_refs:
            results.append(self.create_result(
                True,
                "Glossary hierarchy is valid with no orphaned nodes or circular references",
                severity=TestSeverity.INFO
            ))
            
        return results 