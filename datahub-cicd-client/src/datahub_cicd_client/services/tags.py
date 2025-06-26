"""
Tag service for DataHub operations.
"""

from typing import Any, Dict, List, Optional

from datahub_cicd_client.core.base_client import BaseDataHubClient
from datahub_cicd_client.graphql.mutations.tags import (
    ADD_TAG_OWNER_MUTATION,
    ADD_TAG_TO_ENTITY_MUTATION,
    CREATE_TAG_MUTATION,
    DELETE_TAG_MUTATION,
    REMOVE_TAG_FROM_ENTITY_MUTATION,
    REMOVE_TAG_OWNER_MUTATION,
    SET_TAG_COLOR_MUTATION,
    UPDATE_TAG_DESCRIPTION_MUTATION,
)
from datahub_cicd_client.graphql.queries.tags import (
    COUNT_TAGS_QUERY,
    FIND_ENTITIES_WITH_TAG_QUERY,
    GET_TAG_QUERY,
    LIST_TAGS_QUERY,
)


class TagService(BaseDataHubClient):
    """Service for managing DataHub tags."""

    def list_tags(self, query: str = "*", start: int = 0, count: int = 100) -> List[Dict[str, Any]]:
        """
        List tags in DataHub with comprehensive information including ownership and relationships.

        Args:
            query: Search query to filter tags (default: "*")
            start: Starting offset for pagination
            count: Maximum number of tags to return

        Returns:
            List of tag objects with detailed information
        """
        self.logger.info(f"Listing tags with query: {query}, start: {start}, count: {count}")

        variables = {
            "input": {
                "types": ["TAG"],
                "query": query,
                "start": start,
                "count": count,
                "filters": [],
            }
        }

        try:
            data = self.safe_execute_graphql(LIST_TAGS_QUERY, variables)
            if not data or "searchAcrossEntities" not in data:
                return []

            search_results = data["searchAcrossEntities"]["searchResults"]
            tags = []

            for item in search_results:
                entity = item.get("entity")
                if not entity:
                    continue

                # Extract basic tag information
                properties = entity.get("properties") or {}
                tag = {
                    "urn": entity.get("urn"),
                    "type": entity.get("type"),
                    "name": properties.get("name"),
                    "description": properties.get("description"),
                    "colorHex": properties.get("colorHex"),
                    "properties": properties
                }

                # Add ownership information
                ownership = entity.get("ownership")
                if ownership:
                    tag["ownership"] = ownership

                    # Extract owner details for display
                    owners = ownership.get("owners") or []
                    tag["owners_count"] = len(owners)
                    tag["owner_names"] = []

                    for owner_info in owners:
                        if not owner_info:
                            continue
                        owner = owner_info.get("owner") or {}
                        if owner.get("username"):  # CorpUser
                            owner_props = owner.get("properties") or {}
                            display_name = owner_props.get("displayName")
                            tag["owner_names"].append(display_name or owner["username"])
                        elif owner.get("name"):  # CorpGroup
                            owner_props = owner.get("properties") or {}
                            display_name = owner_props.get("displayName")
                            tag["owner_names"].append(display_name or owner["name"])
                else:
                    tag["owners_count"] = 0
                    tag["owner_names"] = []

                # Set relationships count (could be enhanced with actual relationship query)
                tag["relationships_count"] = 0

                tags.append(tag)

            return tags

        except Exception as e:
            self.logger.error(f"Error listing tags: {str(e)}")
            return []

    def get_tag(self, tag_urn: str) -> Optional[Dict[str, Any]]:
        """
        Get a single tag by URN.

        Args:
            tag_urn: The URN of the tag to retrieve

        Returns:
            Tag data or None if not found
        """
        try:
            variables = {"urn": tag_urn}
            data = self.safe_execute_graphql(GET_TAG_QUERY, variables)

            if not data or "entity" not in data:
                return None

            return data["entity"]

        except Exception as e:
            self.logger.error(f"Error getting tag {tag_urn}: {str(e)}")
            return None

    def get_remote_tags_data(self, query: str = "*", start: int = 0, count: int = 100) -> Dict[str, Any]:
        """
        Get remote tags data for async loading with comprehensive information.

        Args:
            query: Search query to filter tags
            start: Starting offset for pagination
            count: Maximum number of tags to return

        Returns:
            Dict containing enhanced tags data with statistics
        """
        try:
            # Get tags using the list method
            tags = self.list_tags(query=query, start=start, count=count)

            # Get total count
            total_count = self._get_tags_count(query)

            # Calculate statistics
            owned_tags = [tag for tag in tags if tag.get("owners_count", 0) > 0]
            tags_with_relationships = [tag for tag in tags if tag.get("relationships_count", 0) > 0]

            return {
                "success": True,
                "data": {
                    "tags": tags,
                    "total": total_count,
                    "start": start,
                    "count": len(tags),
                    "statistics": {
                        "total_tags": len(tags),
                        "owned_tags": len(owned_tags),
                        "tags_with_relationships": len(tags_with_relationships),
                        "ownership_percentage": (len(owned_tags) / len(tags) * 100) if tags else 0,
                        "relationship_percentage": (len(tags_with_relationships) / len(tags) * 100) if tags else 0,
                    }
                }
            }
        except Exception as e:
            self.logger.error(f"Error getting remote tags data: {str(e)}")
            return {"success": False, "error": str(e)}

    def create_tag(self, tag_id: str, name: str, description: str = "") -> Optional[str]:
        """
        Create a new tag.

        Args:
            tag_id: ID for the tag
            name: Display name for the tag
            description: Optional description

        Returns:
            Created tag URN or None if failed
        """
        try:
            variables = {
                "input": {
                    "id": tag_id,
                    "name": name,
                    "description": description
                }
            }

            data = self.safe_execute_graphql(CREATE_TAG_MUTATION, variables)

            if data and "createTag" in data:
                return data["createTag"]

            return None

        except Exception as e:
            self.logger.error(f"Error creating tag {tag_id}: {str(e)}")
            return None

    def create_or_update_tag(self, tag_id: str, name: str, description: str = "") -> Optional[str]:
        """
        Create a new tag or update existing one.

        Args:
            tag_id: ID for the tag
            name: Display name for the tag
            description: Optional description

        Returns:
            Tag URN or None if failed
        """
        # Try to create first, if it exists it will be updated
        return self.create_tag(tag_id, name, description)

    def delete_tag(self, tag_urn: str) -> bool:
        """
        Delete a tag.

        Args:
            tag_urn: URN of the tag to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            variables = {"urn": tag_urn}
            data = self.safe_execute_graphql(DELETE_TAG_MUTATION, variables)

            return data is not None and "deleteTag" in data

        except Exception as e:
            self.logger.error(f"Error deleting tag {tag_urn}: {str(e)}")
            return False

    def set_tag_color(self, tag_urn: str, color_hex: str) -> bool:
        """
        Set the color of a tag.

        Args:
            tag_urn: URN of the tag
            color_hex: Hex color code

        Returns:
            True if successful, False otherwise
        """
        try:
            variables = {
                "urn": tag_urn,
                "input": {
                    "colorHex": color_hex
                }
            }

            data = self.safe_execute_graphql(SET_TAG_COLOR_MUTATION, variables)
            return data is not None

        except Exception as e:
            self.logger.error(f"Error setting tag color for {tag_urn}: {str(e)}")
            return False

    def update_tag_description(self, tag_urn: str, description: str) -> bool:
        """
        Update tag description.

        Args:
            tag_urn: URN of the tag
            description: New description

        Returns:
            True if successful, False otherwise
        """
        try:
            variables = {
                "urn": tag_urn,
                "input": {
                    "description": description
                }
            }

            data = self.safe_execute_graphql(UPDATE_TAG_DESCRIPTION_MUTATION, variables)
            return data is not None

        except Exception as e:
            self.logger.error(f"Error updating tag description for {tag_urn}: {str(e)}")
            return False

    def add_tag_owner(self, tag_urn: str, owner_urn: str,
                     ownership_type: str = "urn:li:ownershipType:__system__business_owner") -> bool:
        """
        Add an owner to a tag.

        Args:
            tag_urn: URN of the tag
            owner_urn: URN of the owner (user or group)
            ownership_type: Type of ownership

        Returns:
            True if successful, False otherwise
        """
        try:
            variables = {
                "input": {
                    "ownerUrn": owner_urn,
                    "resourceUrn": tag_urn,
                    "ownershipTypeUrn": ownership_type
                }
            }

            data = self.safe_execute_graphql(ADD_TAG_OWNER_MUTATION, variables)
            return data is not None

        except Exception as e:
            self.logger.error(f"Error adding owner to tag {tag_urn}: {str(e)}")
            return False

    def remove_tag_owner(self, tag_urn: str, owner_urn: str,
                        ownership_type: str = "urn:li:ownershipType:__system__business_owner") -> bool:
        """
        Remove an owner from a tag.

        Args:
            tag_urn: URN of the tag
            owner_urn: URN of the owner (user or group)
            ownership_type: Type of ownership

        Returns:
            True if successful, False otherwise
        """
        try:
            variables = {
                "input": {
                    "ownerUrn": owner_urn,
                    "resourceUrn": tag_urn,
                    "ownershipTypeUrn": ownership_type
                }
            }

            data = self.safe_execute_graphql(REMOVE_TAG_OWNER_MUTATION, variables)
            return data is not None

        except Exception as e:
            self.logger.error(f"Error removing owner from tag {tag_urn}: {str(e)}")
            return False

    def add_tag_to_entity(self, entity_urn: str, tag_urn: str, color_hex: str = None) -> bool:
        """
        Add a tag to an entity.

        Args:
            entity_urn: URN of the entity
            tag_urn: URN of the tag
            color_hex: Optional color for this tag association

        Returns:
            True if successful, False otherwise
        """
        try:
            tag_input = {"tagUrn": tag_urn}
            if color_hex:
                tag_input["context"] = color_hex

            variables = {
                "input": {
                    "resourceUrn": entity_urn,
                    "tags": [tag_input]
                }
            }

            data = self.safe_execute_graphql(ADD_TAG_TO_ENTITY_MUTATION, variables)
            return data is not None

        except Exception as e:
            self.logger.error(f"Error adding tag {tag_urn} to entity {entity_urn}: {str(e)}")
            return False

    def remove_tag_from_entity(self, entity_urn: str, tag_urn: str) -> bool:
        """
        Remove a tag from an entity.

        Args:
            entity_urn: URN of the entity
            tag_urn: URN of the tag

        Returns:
            True if successful, False otherwise
        """
        try:
            variables = {
                "input": {
                    "resourceUrn": entity_urn,
                    "tagUrns": [tag_urn]
                }
            }

            data = self.safe_execute_graphql(REMOVE_TAG_FROM_ENTITY_MUTATION, variables)
            return data is not None

        except Exception as e:
            self.logger.error(f"Error removing tag {tag_urn} from entity {entity_urn}: {str(e)}")
            return False

    def find_entities_with_tag(self, tag_urn: str, start: int = 0, count: int = 50) -> Dict[str, Any]:
        """
        Find entities that have a specific tag.

        Args:
            tag_urn: URN of the tag to search for
            start: Starting offset for pagination
            count: Maximum number of entities to return

        Returns:
            Dictionary with search results
        """
        try:
            variables = {
                "input": {
                    "types": ["DATASET", "DASHBOARD", "CHART"],
                    "query": "*",
                    "start": start,
                    "count": count,
                    "filters": [
                        {
                            "field": "tags",
                            "value": tag_urn,
                            "condition": "EQUAL"
                        }
                    ]
                }
            }

            data = self.safe_execute_graphql(FIND_ENTITIES_WITH_TAG_QUERY, variables)

            if not data or "searchAcrossEntities" not in data:
                return {"entities": [], "total": 0}

            search_data = data["searchAcrossEntities"]
            return {
                "entities": search_data.get("searchResults", []),
                "total": search_data.get("total", 0),
                "start": search_data.get("start", 0),
                "count": search_data.get("count", 0)
            }

        except Exception as e:
            self.logger.error(f"Error finding entities with tag {tag_urn}: {str(e)}")
            return {"entities": [], "total": 0}

    def _get_tags_count(self, query: str = "*") -> int:
        """Get total count of tags."""
        try:
            variables = {
                "input": {
                    "types": ["TAG"],
                    "query": query,
                    "start": 0,
                    "count": 1,
                    "filters": [],
                }
            }

            data = self.safe_execute_graphql(COUNT_TAGS_QUERY, variables)

            if data and "searchAcrossEntities" in data:
                return data["searchAcrossEntities"].get("total", 0)

            return 0

        except Exception as e:
            self.logger.error(f"Error getting tags count: {str(e)}")
            return 0
