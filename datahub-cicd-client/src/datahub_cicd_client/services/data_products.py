"""
Data Product service for DataHub operations.
Handles data product management including creation, updates, and asset relationships.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.core.exceptions import DataHubError
from datahub_cicd_client.graphql.mutations.data_products import (
    ADD_ASSETS_TO_DATA_PRODUCT_MUTATION,
    ADD_DATA_PRODUCT_GLOSSARY_TERM_MUTATION,
    ADD_DATA_PRODUCT_OWNER_MUTATION,
    ADD_DATA_PRODUCT_TAG_MUTATION,
    CREATE_DATA_PRODUCT_MUTATION,
    DELETE_DATA_PRODUCT_MUTATION,
    REMOVE_ASSETS_FROM_DATA_PRODUCT_MUTATION,
    REMOVE_DATA_PRODUCT_GLOSSARY_TERM_MUTATION,
    REMOVE_DATA_PRODUCT_OWNER_MUTATION,
    REMOVE_DATA_PRODUCT_TAG_MUTATION,
    SET_DATA_PRODUCT_DOMAIN_MUTATION,
    UNSET_DATA_PRODUCT_DOMAIN_MUTATION,
    UPDATE_DATA_PRODUCT_DESCRIPTION_MUTATION,
    UPSERT_DATA_PRODUCT_STRUCTURED_PROPERTIES_MUTATION,
)
from datahub_cicd_client.graphql.queries.data_products import (
    COUNT_DATA_PRODUCTS_QUERY,
    FIND_DATA_PRODUCTS_BY_DOMAIN_QUERY,
    FIND_DATA_PRODUCTS_BY_OWNER_QUERY,
    GET_DATA_PRODUCT_ASSETS_QUERY,
    GET_DATA_PRODUCT_QUERY,
    LIST_DATA_PRODUCTS_QUERY,
    LIST_DATA_PRODUCTS_SIMPLE_QUERY,
)


class DataProductService(BaseDataHubClient):
    """Service for managing DataHub data products."""

    def __init__(self, connection):
        super().__init__(connection)
        self.logger = logging.getLogger(__name__)

    # Core Data Product Operations
    def list_data_products(
        self, query: str = "*", start: int = 0, count: int = 100, simple: bool = False
    ) -> List[Dict[str, Any]]:
        """
        List data products from DataHub.

        Args:
            query: Search query to filter data products
            start: Starting offset for pagination
            count: Number of data products to return
            simple: Use simple fragment for better performance

        Returns:
            List of data product dictionaries

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Listing data products with query='{query}', start={start}, count={count}, simple={simple}")

        variables = {
            "input": {
                "query": query,
                "types": ["DATA_PRODUCT"],
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            # Use simple or full query based on parameter
            query_to_use = LIST_DATA_PRODUCTS_SIMPLE_QUERY if simple else LIST_DATA_PRODUCTS_QUERY
            result = self.safe_execute_graphql(query_to_use, variables)

            if not result:
                self.logger.warning("GraphQL query returned no data")
                return []

            search_data = result.get("searchAcrossEntities", {})

            if not search_data:
                return []

            search_results = search_data.get("searchResults", [])
            data_products = []

            for result_item in search_results:
                entity = result_item.get("entity", {})
                if entity and entity.get("type") == "DATA_PRODUCT":
                    processed_product = self._process_data_product(entity)
                    if processed_product:
                        data_products.append(processed_product)

            self.logger.info(f"Retrieved {len(data_products)} data products")
            return data_products

        except Exception as e:
            self.logger.error(f"Error listing data products: {str(e)}")
            raise DataHubError(f"Failed to list data products: {str(e)}")

    def get_data_product(self, data_product_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific data product by URN.

        Args:
            data_product_urn: URN of the data product

        Returns:
            Data product dictionary or None if not found

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Getting data product: {data_product_urn}")

        variables = {"urn": data_product_urn}

        try:
            result = self.safe_execute_graphql(GET_DATA_PRODUCT_QUERY, variables)

            if not result:
                self.logger.warning(f"GraphQL query returned no data for data product: {data_product_urn}")
                return None

            product_data = result.get("dataProduct")

            if not product_data:
                self.logger.warning(f"Data product not found: {data_product_urn}")
                return None

            processed_product = self._process_data_product(product_data)
            self.logger.info(f"Retrieved data product: {processed_product.get('name', 'Unknown')}")
            return processed_product

        except Exception as e:
            self.logger.error(f"Error getting data product {data_product_urn}: {str(e)}")
            raise DataHubError(f"Failed to get data product: {str(e)}")

    def create_data_product(
        self,
        product_id: str,
        name: str,
        description: str = "",
        external_url: Optional[str] = None,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """
        Create a new data product.

        Args:
            product_id: ID for the data product (used in URN)
            name: Display name for the data product
            description: Description for the data product
            external_url: External URL for the data product
            custom_properties: Custom properties as key-value pairs

        Returns:
            URN of the created data product or None if failed

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Creating data product: {name}")

        # Build input for creation
        input_data = {
            "id": product_id,
            "name": name,
            "description": description,
        }

        if external_url:
            input_data["externalUrl"] = external_url

        if custom_properties:
            input_data["customProperties"] = [
                {"key": k, "value": v} for k, v in custom_properties.items()
            ]

        variables = {"input": input_data}

        try:
            result = self.safe_execute_graphql(CREATE_DATA_PRODUCT_MUTATION, variables)

            if not result:
                self.logger.error("Failed to create data product - GraphQL query returned no data")
                return None

            created_product = result.get("createDataProduct", {})
            product_urn = created_product.get("urn")

            if product_urn:
                self.logger.info(f"Created data product: {product_urn}")
                return product_urn
            else:
                self.logger.error("Failed to create data product - no URN returned")
                return None

        except Exception as e:
            self.logger.error(f"Error creating data product: {str(e)}")
            raise DataHubError(f"Failed to create data product: {str(e)}")

    def delete_data_product(self, product_urn: str) -> bool:
        """
        Delete a data product.

        Args:
            product_urn: URN of the data product to delete

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Deleting data product: {product_urn}")

        variables = {"urn": product_urn}

        try:
            result = self.safe_execute_graphql(DELETE_DATA_PRODUCT_MUTATION, variables)

            if not result:
                self.logger.error("Failed to delete data product - GraphQL query returned no data")
                return False

            success = result.get("deleteEntity", False)

            if success:
                self.logger.info(f"Deleted data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to delete data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error deleting data product {product_urn}: {str(e)}")
            raise DataHubError(f"Failed to delete data product: {str(e)}")

    def update_data_product_description(self, product_urn: str, description: str) -> bool:
        """
        Update the description of a data product.

        Args:
            product_urn: URN of the data product
            description: New description

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Updating data product description: {product_urn}")

        variables = {
            "urn": product_urn,
            "description": description,
        }

        try:
            result = self.safe_execute_graphql(UPDATE_DATA_PRODUCT_DESCRIPTION_MUTATION, variables)

            if not result:
                self.logger.error("Failed to update data product description - GraphQL query returned no data")
                return False

            success = result.get("updateDescription", False)

            if success:
                self.logger.info(f"Updated data product description: {product_urn}")
            else:
                self.logger.warning(f"Failed to update data product description: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error updating data product description {product_urn}: {str(e)}")
            raise DataHubError(f"Failed to update data product description: {str(e)}")

    # Owner Management
    def add_data_product_owner(
        self,
        product_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> bool:
        """
        Add an owner to a data product.

        Args:
            product_urn: URN of the data product
            owner_urn: URN of the owner to add
            ownership_type: Type of ownership

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Adding owner {owner_urn} to data product {product_urn}")

        variables = {
            "input": {
                "ownerUrn": owner_urn,
                "resourceUrn": product_urn,
                "ownershipTypeUrn": ownership_type,
            }
        }

        try:
            result = self.safe_execute_graphql(ADD_DATA_PRODUCT_OWNER_MUTATION, variables)

            if not result:
                self.logger.error("Failed to add owner to data product - GraphQL query returned no data")
                return False

            success = result.get("addOwner", False)

            if success:
                self.logger.info(f"Added owner to data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to add owner to data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error adding owner to data product: {str(e)}")
            raise DataHubError(f"Failed to add owner to data product: {str(e)}")

    def remove_data_product_owner(
        self,
        product_urn: str,
        owner_urn: str,
        ownership_type: str = "urn:li:ownershipType:__system__business_owner",
    ) -> bool:
        """
        Remove an owner from a data product.

        Args:
            product_urn: URN of the data product
            owner_urn: URN of the owner to remove
            ownership_type: Type of ownership

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Removing owner {owner_urn} from data product {product_urn}")

        variables = {
            "input": {
                "ownerUrn": owner_urn,
                "resourceUrn": product_urn,
                "ownershipTypeUrn": ownership_type,
            }
        }

        try:
            result = self.safe_execute_graphql(REMOVE_DATA_PRODUCT_OWNER_MUTATION, variables)

            if not result:
                self.logger.error("Failed to remove owner from data product - GraphQL query returned no data")
                return False

            success = result.get("removeOwner", False)

            if success:
                self.logger.info(f"Removed owner from data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to remove owner from data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error removing owner from data product: {str(e)}")
            raise DataHubError(f"Failed to remove owner from data product: {str(e)}")

    # Tag Management
    def add_data_product_tag(self, product_urn: str, tag_urn: str) -> bool:
        """
        Add a tag to a data product.

        Args:
            product_urn: URN of the data product
            tag_urn: URN of the tag

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Adding tag {tag_urn} to data product {product_urn}")

        variables = {
            "input": {
                "tagUrn": tag_urn,
                "resourceUrn": product_urn,
            }
        }

        try:
            result = self.safe_execute_graphql(ADD_DATA_PRODUCT_TAG_MUTATION, variables)
            success = result.get("addTag", False)

            if success:
                self.logger.info(f"Added tag to data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to add tag to data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error adding tag to data product: {str(e)}")
            raise DataHubError(f"Failed to add tag to data product: {str(e)}")

    def remove_data_product_tag(self, product_urn: str, tag_urn: str) -> bool:
        """
        Remove a tag from a data product.

        Args:
            product_urn: URN of the data product
            tag_urn: URN of the tag

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Removing tag {tag_urn} from data product {product_urn}")

        variables = {
            "input": {
                "tagUrn": tag_urn,
                "resourceUrn": product_urn,
            }
        }

        try:
            result = self.safe_execute_graphql(REMOVE_DATA_PRODUCT_TAG_MUTATION, variables)
            success = result.get("removeTag", False)

            if success:
                self.logger.info(f"Removed tag from data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to remove tag from data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error removing tag from data product: {str(e)}")
            raise DataHubError(f"Failed to remove tag from data product: {str(e)}")

    # Glossary Term Management
    def add_data_product_glossary_term(self, product_urn: str, term_urn: str) -> bool:
        """
        Add a glossary term to a data product.

        Args:
            product_urn: URN of the data product
            term_urn: URN of the glossary term

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Adding glossary term {term_urn} to data product {product_urn}")

        variables = {
            "input": {
                "termUrn": term_urn,
                "resourceUrn": product_urn,
            }
        }

        try:
            result = self.safe_execute_graphql(ADD_DATA_PRODUCT_GLOSSARY_TERM_MUTATION, variables)
            success = result.get("addTerm", False)

            if success:
                self.logger.info(f"Added glossary term to data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to add glossary term to data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error adding glossary term to data product: {str(e)}")
            raise DataHubError(f"Failed to add glossary term to data product: {str(e)}")

    def remove_data_product_glossary_term(self, product_urn: str, term_urn: str) -> bool:
        """
        Remove a glossary term from a data product.

        Args:
            product_urn: URN of the data product
            term_urn: URN of the glossary term

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Removing glossary term {term_urn} from data product {product_urn}")

        variables = {
            "input": {
                "termUrn": term_urn,
                "resourceUrn": product_urn,
            }
        }

        try:
            result = self.safe_execute_graphql(REMOVE_DATA_PRODUCT_GLOSSARY_TERM_MUTATION, variables)
            success = result.get("removeTerm", False)

            if success:
                self.logger.info(f"Removed glossary term from data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to remove glossary term from data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error removing glossary term from data product: {str(e)}")
            raise DataHubError(f"Failed to remove glossary term from data product: {str(e)}")

    # Domain Management
    def set_data_product_domain(self, product_urn: str, domain_urn: str) -> bool:
        """
        Set the domain for a data product.

        Args:
            product_urn: URN of the data product
            domain_urn: URN of the domain

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Setting domain {domain_urn} for data product {product_urn}")

        variables = {
            "input": {
                "domainUrn": domain_urn,
                "resourceUrn": product_urn,
            }
        }

        try:
            result = self.safe_execute_graphql(SET_DATA_PRODUCT_DOMAIN_MUTATION, variables)
            success = result.get("setDomain", False)

            if success:
                self.logger.info(f"Set domain for data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to set domain for data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error setting domain for data product: {str(e)}")
            raise DataHubError(f"Failed to set domain for data product: {str(e)}")

    def unset_data_product_domain(self, product_urn: str) -> bool:
        """
        Unset the domain for a data product.

        Args:
            product_urn: URN of the data product

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Unsetting domain for data product {product_urn}")

        variables = {
            "input": {
                "resourceUrn": product_urn,
            }
        }

        try:
            result = self.safe_execute_graphql(UNSET_DATA_PRODUCT_DOMAIN_MUTATION, variables)
            success = result.get("unsetDomain", False)

            if success:
                self.logger.info(f"Unset domain for data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to unset domain for data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error unsetting domain for data product: {str(e)}")
            raise DataHubError(f"Failed to unset domain for data product: {str(e)}")

    # Asset Management
    def add_assets_to_data_product(self, product_urn: str, asset_urns: List[str]) -> bool:
        """
        Add assets to a data product.

        Args:
            product_urn: URN of the data product
            asset_urns: List of asset URNs to add

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Adding {len(asset_urns)} assets to data product {product_urn}")

        variables = {
            "input": {
                "dataProductUrn": product_urn,
                "assetUrns": asset_urns,
            }
        }

        try:
            result = self.safe_execute_graphql(ADD_ASSETS_TO_DATA_PRODUCT_MUTATION, variables)
            updated_product = result.get("addAssetsToDataProduct", {})
            success = bool(updated_product.get("urn"))

            if success:
                self.logger.info(f"Added assets to data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to add assets to data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error adding assets to data product: {str(e)}")
            raise DataHubError(f"Failed to add assets to data product: {str(e)}")

    def remove_assets_from_data_product(self, product_urn: str, asset_urns: List[str]) -> bool:
        """
        Remove assets from a data product.

        Args:
            product_urn: URN of the data product
            asset_urns: List of asset URNs to remove

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Removing {len(asset_urns)} assets from data product {product_urn}")

        variables = {
            "input": {
                "dataProductUrn": product_urn,
                "assetUrns": asset_urns,
            }
        }

        try:
            result = self.safe_execute_graphql(REMOVE_ASSETS_FROM_DATA_PRODUCT_MUTATION, variables)
            updated_product = result.get("removeAssetsFromDataProduct", {})
            success = bool(updated_product.get("urn"))

            if success:
                self.logger.info(f"Removed assets from data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to remove assets from data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error removing assets from data product: {str(e)}")
            raise DataHubError(f"Failed to remove assets from data product: {str(e)}")

    def get_data_product_assets(
        self, product_urn: str, start: int = 0, count: int = 100
    ) -> Dict[str, Any]:
        """
        Get assets associated with a data product.

        Args:
            product_urn: URN of the data product
            start: Starting offset for pagination
            count: Number of assets to return

        Returns:
            Dictionary with assets and metadata

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Getting assets for data product: {product_urn}")

        variables = {
            "urn": product_urn,
            "input": {
                "query": f'dataProduct.urn:"{product_urn}"',
                "types": ["DATASET", "CHART", "DASHBOARD", "DATA_JOB", "DATA_FLOW"],
                "start": start,
                "count": count,
            }
        }

        try:
            result = self.safe_execute_graphql(GET_DATA_PRODUCT_ASSETS_QUERY, variables)
            search_data = result.get("searchAcrossEntities", {})

            if not search_data:
                return {"assets": [], "total": 0, "start": 0, "count": 0}

            search_results = search_data.get("searchResults", [])
            assets = []

            for result_item in search_results:
                entity = result_item.get("entity", {})
                if entity:
                    assets.append(entity)

            result_data = {
                "assets": assets,
                "total": search_data.get("total", 0),
                "start": search_data.get("start", 0),
                "count": search_data.get("count", 0),
            }

            self.logger.info(f"Found {len(assets)} assets for data product")
            return result_data

        except Exception as e:
            self.logger.error(f"Error getting data product assets: {str(e)}")
            raise DataHubError(f"Failed to get data product assets: {str(e)}")

    # Search and Filter Operations
    def find_data_products_by_domain(
        self, domain_urn: str, start: int = 0, count: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Find data products in a specific domain.

        Args:
            domain_urn: URN of the domain
            start: Starting offset for pagination
            count: Number of data products to return

        Returns:
            List of data product dictionaries

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Finding data products in domain: {domain_urn}")

        variables = {
            "input": {
                "query": f'domain.urn:"{domain_urn}"',
                "types": ["DATA_PRODUCT"],
                "start": start,
                "count": count,
            }
        }

        try:
            result = self.safe_execute_graphql(FIND_DATA_PRODUCTS_BY_DOMAIN_QUERY, variables)
            search_data = result.get("searchAcrossEntities", {})

            if not search_data:
                return []

            search_results = search_data.get("searchResults", [])
            data_products = []

            for result_item in search_results:
                entity = result_item.get("entity", {})
                if entity and entity.get("type") == "DATA_PRODUCT":
                    processed_product = self._process_data_product(entity)
                    if processed_product:
                        data_products.append(processed_product)

            self.logger.info(f"Found {len(data_products)} data products in domain")
            return data_products

        except Exception as e:
            self.logger.error(f"Error finding data products by domain: {str(e)}")
            raise DataHubError(f"Failed to find data products by domain: {str(e)}")

    def find_data_products_by_owner(
        self, owner_urn: str, start: int = 0, count: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Find data products owned by a specific user or group.

        Args:
            owner_urn: URN of the owner
            start: Starting offset for pagination
            count: Number of data products to return

        Returns:
            List of data product dictionaries

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Finding data products owned by: {owner_urn}")

        variables = {
            "input": {
                "query": f'owners.owner.urn:"{owner_urn}"',
                "types": ["DATA_PRODUCT"],
                "start": start,
                "count": count,
            }
        }

        try:
            result = self.safe_execute_graphql(FIND_DATA_PRODUCTS_BY_OWNER_QUERY, variables)
            search_data = result.get("searchAcrossEntities", {})

            if not search_data:
                return []

            search_results = search_data.get("searchResults", [])
            data_products = []

            for result_item in search_results:
                entity = result_item.get("entity", {})
                if entity and entity.get("type") == "DATA_PRODUCT":
                    processed_product = self._process_data_product(entity)
                    if processed_product:
                        data_products.append(processed_product)

            self.logger.info(f"Found {len(data_products)} data products owned by user")
            return data_products

        except Exception as e:
            self.logger.error(f"Error finding data products by owner: {str(e)}")
            raise DataHubError(f"Failed to find data products by owner: {str(e)}")

    # Structured Properties
    def upsert_data_product_structured_properties(
        self, product_urn: str, structured_properties: List[Dict[str, Any]]
    ) -> bool:
        """
        Upsert structured properties for a data product.

        Args:
            product_urn: URN of the data product
            structured_properties: List of structured property dictionaries

        Returns:
            True if successful, False otherwise

        Raises:
            DataHubError: If the operation fails
        """
        self.logger.info(f"Upserting structured properties for data product: {product_urn}")

        variables = {
            "input": {
                "entityUrn": product_urn,
                "structuredPropertyInputParams": structured_properties,
            }
        }

        try:
            result = self.safe_execute_graphql(
                UPSERT_DATA_PRODUCT_STRUCTURED_PROPERTIES_MUTATION, variables
            )
            updated_entity = result.get("upsertStructuredProperties", {})
            success = bool(updated_entity.get("urn"))

            if success:
                self.logger.info(f"Upserted structured properties for data product: {product_urn}")
            else:
                self.logger.warning(f"Failed to upsert structured properties for data product: {product_urn}")

            return success

        except Exception as e:
            self.logger.error(f"Error upserting structured properties for data product: {str(e)}")
            raise DataHubError(f"Failed to upsert structured properties for data product: {str(e)}")

    # Utility Methods
    def count_data_products(self, query: str = "*") -> int:
        """
        Count the total number of data products matching a query.

        Args:
            query: Search query to filter data products

        Returns:
            Total count of matching data products

        Raises:
            DataHubError: If the operation fails
        """
        variables = {
            "input": {
                "query": query,
                "types": ["DATA_PRODUCT"],
                "start": 0,
                "count": 1,
            }
        }

        try:
            result = self.safe_execute_graphql(COUNT_DATA_PRODUCTS_QUERY, variables)
            search_data = result.get("searchAcrossEntities", {})
            return search_data.get("total", 0)

        except Exception as e:
            self.logger.error(f"Error counting data products: {str(e)}")
            raise DataHubError(f"Failed to count data products: {str(e)}")

    # Private helper methods
    def _process_data_product(self, entity: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a data product entity into standardized format."""
        if not entity or not entity.get("urn"):
            return None

        properties = entity.get("properties", {}) or {}
        ownership = entity.get("ownership", {}) or {}
        structured_props = entity.get("structuredProperties", {}) or {}
        domain = entity.get("domain", {})
        tags = entity.get("tags", {})
        glossary_terms = entity.get("glossaryTerms", {})

        # Process ownership
        owners = []
        owner_names = []
        ownership_owners = ownership.get("owners", []) if ownership else []
        for owner_data in ownership_owners:
            if owner_data:
                owner = owner_data.get("owner", {}) or {}
                ownership_type = owner_data.get("ownershipType", {}) or {}
                owner_properties = owner.get("properties", {}) or {}
                ownership_info = ownership_type.get("info", {}) or {}

                # Extract name from URN if not available in properties
                owner_name = owner.get("username") or owner.get("name")
                if not owner_name and owner.get("urn"):
                    urn_parts = owner.get("urn").split(":")
                    if len(urn_parts) >= 4:
                        owner_name = urn_parts[-1]

                display_name = owner_properties.get("displayName") or owner_name or "Unknown"
                owner_names.append(display_name)

                owner_info = {
                    "urn": owner.get("urn"),
                    "type": owner.get("type"),
                    "name": owner_name or "Unknown",
                    "displayName": display_name,
                    "email": owner_properties.get("email", ""),
                    "ownershipType": {
                        "urn": ownership_type.get("urn"),
                        "name": ownership_info.get("name", "Unknown"),
                    },
                }
                owners.append(owner_info)

        # Process structured properties
        structured_properties = []
        structured_props_list = structured_props.get("properties", []) if structured_props else []
        for prop_data in structured_props_list:
            if prop_data:
                structured_property = prop_data.get("structuredProperty", {}) or {}
                prop_def = structured_property.get("definition", {}) or {}
                values = prop_data.get("values", []) or []

                prop_info = {
                    "urn": structured_property.get("urn"),
                    "displayName": prop_def.get("displayName", ""),
                    "qualifiedName": prop_def.get("qualifiedName", ""),
                    "values": [
                        v.get("stringValue") or v.get("numberValue") for v in values if v
                    ],
                }
                structured_properties.append(prop_info)

        # Process domain
        domain_info = None
        if domain and domain.get("domain"):
            domain_entity = domain["domain"]
            domain_props = domain_entity.get("properties", {})
            domain_info = {
                "urn": domain_entity.get("urn"),
                "name": domain_props.get("name", "Unknown"),
                "description": domain_props.get("description", ""),
            }

        # Process tags
        tag_list = []
        if tags and tags.get("tags"):
            for tag_data in tags["tags"]:
                if tag_data and tag_data.get("tag"):
                    tag_entity = tag_data["tag"]
                    tag_props = tag_entity.get("properties", {})
                    tag_list.append({
                        "urn": tag_entity.get("urn"),
                        "name": tag_props.get("name", "Unknown"),
                        "description": tag_props.get("description", ""),
                    })

        # Process glossary terms
        term_list = []
        if glossary_terms and glossary_terms.get("terms"):
            for term_data in glossary_terms["terms"]:
                if term_data and term_data.get("term"):
                    term_entity = term_data["term"]
                    term_props = term_entity.get("properties", {})
                    term_list.append({
                        "urn": term_entity.get("urn"),
                        "name": term_props.get("name", "Unknown"),
                    })

        return {
            "urn": entity.get("urn"),
            "type": entity.get("type"),
            "name": properties.get("name", "Unknown"),
            "description": properties.get("description", ""),
            "externalUrl": properties.get("externalUrl", ""),
            "numAssets": properties.get("numAssets", 0),
            "customProperties": properties.get("customProperties", []),
            "owners": owners,
            "owners_count": len(owners),
            "owner_names": owner_names,
            "structuredProperties": structured_properties,
            "structured_properties_count": len(structured_properties),
            "domain": domain_info,
            "tags": tag_list,
            "tags_count": len(tag_list),
            "glossaryTerms": term_list,
            "glossary_terms_count": len(term_list),
            "deprecated": entity.get("deprecation", {}).get("deprecated", False),
            "properties": properties,
            "ownership": ownership,
            "institutionalMemory": entity.get("institutionalMemory"),
            "application": entity.get("application"),
        }
