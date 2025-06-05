"""
Mutation Store for DataHub metadata entities.
Handles environment-specific transformations for CI/CD processes.
"""

import logging
from typing import Dict, Any, List, Optional
from django.db.models import Model
from utils.urn_utils import apply_entity_mutation

logger = logging.getLogger(__name__)


class MutationStore:
    """
    Store for managing mutations to DataHub metadata entities.
    Provides methods for transforming entities based on environment configuration.
    """

    def __init__(self):
        self.entity_transformers = {
            "glossaryNode": self._transform_glossary_node,
            "glossaryTerm": self._transform_glossary_term,
            "tag": self._transform_tag,
            "domain": self._transform_domain,
            "assertion": self._transform_assertion,
            "container": self._transform_container,
            "dataset": self._transform_dataset,
        }

    def transform_entity(
        self, entity: Model, environment_config: Optional[Dict[str, Any]] = None
    ) -> Model:
        """
        Apply environment-specific transformations to an entity.

        Args:
            entity: The entity to transform
            environment_config: Environment configuration dictionary

        Returns:
            The transformed entity
        """
        if not entity:
            return entity

        # Skip if no environment config
        if not environment_config:
            return entity

        # Determine entity type
        entity_type = self._get_entity_type(entity)
        if not entity_type:
            logger.warning(f"Unknown entity type for entity {entity}")
            return entity

        # Apply type-specific transformer if available
        transformer = self.entity_transformers.get(entity_type)
        if transformer:
            return transformer(entity, environment_config)

        # Default to basic entity mutation
        return apply_entity_mutation(entity, entity_type, environment_config)

    def transform_entities(
        self, entities: List[Model], environment_config: Optional[Dict[str, Any]] = None
    ) -> List[Model]:
        """
        Apply environment-specific transformations to a list of entities.

        Args:
            entities: List of entities to transform
            environment_config: Environment configuration dictionary

        Returns:
            List of transformed entities
        """
        if not entities:
            return entities

        # Skip if no environment config
        if not environment_config:
            return entities

        # Transform each entity
        transformed = []
        for entity in entities:
            transformed.append(self.transform_entity(entity, environment_config))

        return transformed

    def _get_entity_type(self, entity: Model) -> Optional[str]:
        """
        Determine the DataHub entity type from a Django model instance.

        Args:
            entity: Django model instance

        Returns:
            DataHub entity type string or None if unknown
        """
        # Import models here to avoid circular imports
        from web_ui.metadata_manager.models import (
            GlossaryNode,
            GlossaryTerm,
            Tag,
            Domain,
            Assertion,
        )

        if isinstance(entity, GlossaryNode):
            return "glossaryNode"
        elif isinstance(entity, GlossaryTerm):
            return "glossaryTerm"
        elif isinstance(entity, Tag):
            return "tag"
        elif isinstance(entity, Domain):
            return "domain"
        elif isinstance(entity, Assertion):
            return "assertion"

        # Check for container or dataset types based on attributes
        if hasattr(entity, "container_type"):
            container_type = getattr(entity, "container_type", "").lower()
            if container_type in ["database", "schema", "container"]:
                return "container"
            elif container_type in ["dataset", "table", "view"]:
                return "dataset"

        # Check class name as fallback
        class_name = entity.__class__.__name__.lower()
        if "container" in class_name:
            return "container"
        elif "dataset" in class_name or "table" in class_name:
            return "dataset"

        # Unknown entity type
        return None

    def _transform_glossary_node(
        self, node: Model, environment_config: Dict[str, Any]
    ) -> Model:
        """
        Transform a GlossaryNode entity.

        Args:
            node: GlossaryNode instance
            environment_config: Environment configuration

        Returns:
            Transformed GlossaryNode
        """
        # Apply basic entity mutation
        node = apply_entity_mutation(node, "glossaryNode", environment_config)

        # Additional transformations specific to glossary nodes can be added here
        # For example, environment-specific descriptions or names
        if (
            environment_config.get("add_environment_to_description", False)
            and node.description
        ):
            env_name = environment_config.get("name", "Unknown")
            node.description = f"[{env_name}] {node.description}"

        return node

    def _transform_glossary_term(
        self, term: Model, environment_config: Dict[str, Any]
    ) -> Model:
        """
        Transform a GlossaryTerm entity.

        Args:
            term: GlossaryTerm instance
            environment_config: Environment configuration

        Returns:
            Transformed GlossaryTerm
        """
        # Apply basic entity mutation
        term = apply_entity_mutation(term, "glossaryTerm", environment_config)

        # Additional transformations specific to glossary terms can be added here
        # For example, environment-specific sources
        if (
            environment_config.get("name")
            and hasattr(term, "term_source")
            and term.term_source
        ):
            env_name = environment_config.get("name")
            if not term.term_source.startswith(f"[{env_name}]"):
                term.term_source = f"[{env_name}] {term.term_source}"

        return term

    def _transform_tag(self, tag: Model, environment_config: Dict[str, Any]) -> Model:
        """
        Transform a Tag entity.

        Args:
            tag: Tag instance
            environment_config: Environment configuration

        Returns:
            Transformed Tag
        """
        # Apply basic entity mutation
        tag = apply_entity_mutation(tag, "tag", environment_config)

        # Additional transformations specific to tags can be added here
        # For example, environment-specific colors
        if environment_config.get("tag_color_override") and hasattr(tag, "color"):
            tag.color = environment_config.get("tag_color_override")

        return tag

    def _transform_domain(
        self, domain: Model, environment_config: Dict[str, Any]
    ) -> Model:
        """
        Transform a Domain entity.

        Args:
            domain: Domain instance
            environment_config: Environment configuration

        Returns:
            Transformed Domain
        """
        # Apply basic entity mutation
        domain = apply_entity_mutation(domain, "domain", environment_config)

        # Additional transformations specific to domains can be added here

        return domain

    def _transform_assertion(
        self, assertion: Model, environment_config: Dict[str, Any]
    ) -> Model:
        """
        Transform an Assertion entity.

        Args:
            assertion: Assertion instance
            environment_config: Environment configuration

        Returns:
            Transformed Assertion
        """
        # For assertions, we might need to transform the configuration rather than the URN
        if hasattr(assertion, "config") and assertion.config:
            # Update assertion config with environment-specific values
            try:
                config = assertion.config

                # Replace environment-specific values in the config
                env_name = environment_config.get("name")
                if env_name:
                    # For database or schema references
                    if "platform" in config and "database" in config:
                        # Add environment prefix to database name if not already present
                        if not config["database"].startswith(f"{env_name}_"):
                            config["database"] = f"{env_name}_{config['database']}"

                    # For entity references
                    if "entities" in config and isinstance(config["entities"], list):
                        for i, entity in enumerate(config["entities"]):
                            if isinstance(entity, str) and "urn:li:" in entity:
                                # Transform URNs in entity references
                                # This is a simplified example - actual logic may be more complex
                                parts = entity.split(":")
                                if len(parts) > 3:
                                    entity_type = parts[2]
                                    entity_id = parts[3]
                                    if not entity_id.startswith(f"{env_name}_"):
                                        config["entities"][i] = (
                                            f"urn:li:{entity_type}:{env_name}_{entity_id}"
                                        )

                assertion.config = config
            except Exception as e:
                logger.error(f"Error transforming assertion config: {str(e)}")

        return assertion

    def _transform_container(
        self, container: Model, environment_config: Dict[str, Any]
    ) -> Model:
        """
        Transform a Container entity.

        Args:
            container: Container instance (Database, Schema, etc.)
            environment_config: Environment configuration

        Returns:
            Transformed Container
        """
        # For containers, we need to extract container parameters
        container_params = {}

        # Try to extract container parameters
        try:
            # Get basic container parameters
            if hasattr(container, "platform"):
                container_params["platform"] = container.platform
            if hasattr(container, "instance"):
                container_params["instance"] = container.instance
            if hasattr(container, "database"):
                container_params["database"] = container.database
            if hasattr(container, "schema"):
                container_params["schema"] = container.schema

            # Use container type if available
            if hasattr(container, "container_type"):
                container_params["type"] = container.container_type
            else:
                container_params["type"] = "container"

            # Set container parameters for URN generation
            setattr(container, "container_params", container_params)
        except Exception as e:
            logger.error(f"Error extracting container parameters: {str(e)}")

        # Apply basic entity mutation
        container = apply_entity_mutation(container, "container", environment_config)

        return container

    def _transform_dataset(
        self, dataset: Model, environment_config: Dict[str, Any]
    ) -> Model:
        """
        Transform a Dataset entity.

        Args:
            dataset: Dataset instance (Table, View, etc.)
            environment_config: Environment configuration

        Returns:
            Transformed Dataset
        """
        # Similar to container, extract dataset parameters
        dataset_params = {}

        # Try to extract dataset parameters
        try:
            # Get basic dataset parameters
            if hasattr(dataset, "platform"):
                dataset_params["platform"] = dataset.platform
            if hasattr(dataset, "instance"):
                dataset_params["instance"] = dataset.instance
            if hasattr(dataset, "database"):
                dataset_params["database"] = dataset.database
            if hasattr(dataset, "schema"):
                dataset_params["schema"] = dataset.schema
            if hasattr(dataset, "table"):
                dataset_params["table"] = dataset.table
            if hasattr(dataset, "view"):
                dataset_params["view"] = dataset.view

            # Set dataset parameters for URN generation
            setattr(dataset, "container_params", dataset_params)
        except Exception as e:
            logger.error(f"Error extracting dataset parameters: {str(e)}")

        # Apply basic entity mutation
        dataset = apply_entity_mutation(dataset, "dataset", environment_config)

        return dataset


# Singleton instance for use throughout the application
mutation_store = MutationStore()
