from django.shortcuts import render, redirect, get_object_or_404
from django.views import View
from django.contrib import messages
from django.http import JsonResponse
import logging
import os
import sys
from django.views.decorators.http import require_POST
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.utils import timezone

# Add project root to sys.path
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

# Import the deterministic URN utilities
from utils.urn_utils import get_full_urn_from_name
from utils.datahub_utils import get_datahub_client, test_datahub_connection
from web_ui.models import GitSettings
from .models import StructuredProperty

logger = logging.getLogger(__name__)


class PropertyListView(View):
    """View to list and create structured properties"""

    def get(self, request):
        """Display list of structured properties"""
        try:
            logger.info("Starting PropertyListView.get")

            # Get all properties
            properties = StructuredProperty.objects.all().order_by("name")
            logger.debug(f"Found {properties.count()} total properties")

            # Get DataHub connection info
            logger.debug("Testing DataHub connection from PropertyListView")
            connected, client = test_datahub_connection()
            logger.debug(f"DataHub connection test result: {connected}")

            # Initialize context
            context = {
                "properties": properties,
                "page_title": "DataHub Structured Properties",
                "has_datahub_connection": connected,
                "has_git_integration": False,
            }

            # Fetch remote properties if connected
            synced_properties = []
            local_properties = []
            remote_only_properties = []
            datahub_url = None

            if connected and client:
                logger.debug("Connected to DataHub, fetching remote properties")
                try:
                    # Get all remote properties from DataHub
                    remote_properties = client.list_structured_properties(count=1000)
                    logger.debug(
                        f"Fetched {len(remote_properties) if remote_properties else 0} remote properties"
                    )

                    # Get DataHub URL for direct links
                    datahub_url = client.server_url
                    if datahub_url.endswith("/api/gms"):
                        datahub_url = datahub_url[
                            :-8
                        ]  # Remove /api/gms to get base URL

                    # Extract property URNs that exist locally
                    local_property_urns = set(
                        properties.values_list("deterministic_urn", flat=True)
                    )

                    # Process properties by comparing local and remote
                    for prop in properties:
                        prop_urn = str(prop.deterministic_urn)
                        remote_match = next(
                            (p for p in remote_properties if p.get("urn") == prop_urn),
                            None,
                        )

                        if remote_match:
                            synced_properties.append(
                                {"local": prop, "remote": remote_match}
                            )
                        else:
                            local_properties.append(prop)

                    # Find properties that exist remotely but not locally
                    remote_only_properties = [
                        p
                        for p in remote_properties
                        if p.get("urn") not in local_property_urns
                    ]

                    logger.debug(
                        f"Categorized properties: {len(synced_properties)} synced, {len(local_properties)} local-only, {len(remote_only_properties)} remote-only"
                    )

                except Exception as e:
                    logger.error(f"Error fetching remote property data: {str(e)}")
            else:
                # All properties are local-only if not connected
                local_properties = properties

            # Update context with processed properties
            context.update(
                {
                    "synced_properties": synced_properties,
                    "local_properties": local_properties,
                    "remote_only_properties": remote_only_properties,
                    "datahub_url": datahub_url,
                }
            )

            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context["has_git_integration"] = (
                    github_settings and github_settings.enabled
                )
                logger.debug(
                    f"Git integration enabled: {context['has_git_integration']}"
                )
            except Exception as e:
                logger.warning(f"Error checking git integration: {str(e)}")
                pass

            logger.info("Rendering property list template")
            return render(request, "metadata_manager/properties/list.html", context)
        except Exception as e:
            logger.error(f"Error in property list view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return render(
                request,
                "metadata_manager/properties/list.html",
                {"error": str(e), "page_title": "DataHub Structured Properties"},
            )

    def post(self, request):
        """Create a new structured property"""
        try:
            # Get basic property info
            name = request.POST.get("name")
            description = request.POST.get("description", "")
            qualified_name = request.POST.get("qualified_name", "").strip()
            value_type = request.POST.get("value_type", "STRING")
            cardinality = request.POST.get("cardinality", "SINGLE")
            immutable = request.POST.get("immutable") == "on"

            # Get entity types
            entity_types = request.POST.getlist("entity_types", [])
            if not entity_types:
                entity_types = [
                    "DATASET",
                    "DASHBOARD",
                    "CHART",
                    "DATA_FLOW",
                    "DATA_JOB",
                ]

            # Get display settings
            show_in_search_filters = request.POST.get("show_in_search_filters") == "on"
            show_as_asset_badge = request.POST.get("show_as_asset_badge") == "on"
            show_in_asset_summary = request.POST.get("show_in_asset_summary") == "on"
            show_in_columns_table = request.POST.get("show_in_columns_table") == "on"
            is_hidden = request.POST.get("is_hidden") == "on"

            # Get allowed values if any
            allowed_values = []
            allowed_value_count = int(request.POST.get("allowed_value_count", "0"))

            for i in range(allowed_value_count):
                value = request.POST.get(f"allowed_value_{i}", "").strip()
                description = request.POST.get(
                    f"allowed_value_description_{i}", ""
                ).strip()

                if value:
                    allowed_values.append({"value": value, "description": description})

            # Validation
            if not name:
                messages.error(request, "Property name is required")
                return redirect("metadata_manager:property_list")

            # Generate qualified name if not provided
            if not qualified_name:
                qualified_name = name.lower().replace(" ", "_")

            # Generate deterministic URN
            deterministic_urn = get_full_urn_from_name(
                "structuredProperty", qualified_name
            )

            # Check if property with this URN already exists
            if StructuredProperty.objects.filter(
                deterministic_urn=deterministic_urn
            ).exists():
                messages.error(request, f"Property with name '{name}' already exists")
                return redirect("metadata_manager:property_list")

            # Create the property
            StructuredProperty.objects.create(
                name=name,
                description=description,
                qualified_name=qualified_name,
                value_type=value_type,
                cardinality=cardinality,
                immutable=immutable,
                entity_types=entity_types,
                allowed_values=allowed_values,
                deterministic_urn=deterministic_urn,
                sync_status="LOCAL_ONLY",
                show_in_search_filters=show_in_search_filters,
                show_as_asset_badge=show_as_asset_badge,
                show_in_asset_summary=show_in_asset_summary,
                show_in_columns_table=show_in_columns_table,
                is_hidden=is_hidden,
            )

            messages.success(request, f"Property '{name}' created successfully")
            return redirect("metadata_manager:property_list")
        except Exception as e:
            logger.error(f"Error creating property: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:property_list")


class PropertyDetailView(View):
    """View to display, edit and delete structured properties"""

    def get(self, request, property_id):
        """Display property details"""
        try:
            property = get_object_or_404(StructuredProperty, id=property_id)

            # Initialize context with property data
            context = {
                "property": property,
                "page_title": f"Property: {property.name}",
                "has_git_integration": False,  # Set this based on checking GitHub settings
            }

            # Check if git integration is enabled
            try:
                github_settings = GitSettings.objects.first()
                context["has_git_integration"] = (
                    github_settings and github_settings.enabled
                )
            except:
                pass

            # Get property values if DataHub connection is available
            client = get_datahub_client()
            if client and client.test_connection():
                property_urn = property.deterministic_urn

                # Get remote property information if possible
                if property.sync_status != "LOCAL_ONLY":
                    try:
                        remote_property = client.get_structured_property(property_urn)
                        if remote_property:
                            context["remote_property"] = remote_property

                            # Check if the property needs to be synced
                            needs_sync = False

                            # Compare name/description
                            remote_definition = (
                                remote_property.get("definition", {}) or {}
                            )
                            remote_name = remote_definition.get("displayName", "")
                            remote_description = remote_definition.get(
                                "description", ""
                            )

                            if (
                                property.name != remote_name
                                or property.description != remote_description
                            ):
                                needs_sync = True

                            # Compare settings
                            remote_settings = remote_property.get("settings", {}) or {}
                            if (
                                property.show_in_search_filters
                                != remote_settings.get("showInSearchFilters", True)
                                or property.show_as_asset_badge
                                != remote_settings.get("showAsAssetBadge", True)
                                or property.show_in_asset_summary
                                != remote_settings.get("showInAssetSummary", True)
                                or property.show_in_columns_table
                                != remote_settings.get("showInColumnsTable", False)
                                or property.is_hidden
                                != remote_settings.get("isHidden", False)
                            ):
                                needs_sync = True

                            # Update sync status if needed
                            if needs_sync and property.sync_status != "MODIFIED":
                                property.sync_status = "MODIFIED"
                                property.save(update_fields=["sync_status"])
                            elif not needs_sync and property.sync_status == "MODIFIED":
                                property.sync_status = "SYNCED"
                                property.save(update_fields=["sync_status"])
                    except Exception as e:
                        logger.warning(
                            f"Error fetching remote property information: {str(e)}"
                        )

                context["has_datahub_connection"] = True
            else:
                context["has_datahub_connection"] = False

            return render(request, "metadata_manager/properties/detail.html", context)
        except Exception as e:
            logger.error(f"Error in property detail view: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:property_list")

    def post(self, request, property_id):
        """Update a property"""
        try:
            property = get_object_or_404(StructuredProperty, id=property_id)

            # Get basic property info for update
            name = request.POST.get("name")
            description = request.POST.get("description", "")

            # Get display settings
            show_in_search_filters = request.POST.get("show_in_search_filters") == "on"
            show_as_asset_badge = request.POST.get("show_as_asset_badge") == "on"
            show_in_asset_summary = request.POST.get("show_in_asset_summary") == "on"
            show_in_columns_table = request.POST.get("show_in_columns_table") == "on"
            is_hidden = request.POST.get("is_hidden") == "on"

            # Validation
            if not name:
                messages.error(request, "Property name is required")
                return redirect(
                    "metadata_manager:property_detail", property_id=property_id
                )

            # Check if anything changed
            changed = (
                property.name != name
                or property.description != description
                or property.show_in_search_filters != show_in_search_filters
                or property.show_as_asset_badge != show_as_asset_badge
                or property.show_in_asset_summary != show_in_asset_summary
                or property.show_in_columns_table != show_in_columns_table
                or property.is_hidden != is_hidden
            )

            if changed:
                # Update the property
                property.name = name
                property.description = description
                property.show_in_search_filters = show_in_search_filters
                property.show_as_asset_badge = show_as_asset_badge
                property.show_in_asset_summary = show_in_asset_summary
                property.show_in_columns_table = show_in_columns_table
                property.is_hidden = is_hidden

                # If already synced, mark as modified
                if property.sync_status == "SYNCED":
                    property.sync_status = "MODIFIED"

                property.save()

                messages.success(request, f"Property '{name}' updated successfully")
            else:
                messages.info(request, "No changes were made")

            return redirect("metadata_manager:property_detail", property_id=property_id)
        except Exception as e:
            logger.error(f"Error updating property: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:property_detail", property_id=property_id)

    def delete(self, request, property_id):
        """Delete a property"""
        try:
            property = get_object_or_404(StructuredProperty, id=property_id)

            # Delete the property
            property_name = property.name
            property.delete()

            messages.success(
                request, f"Property '{property_name}' deleted successfully"
            )
            return JsonResponse({"success": True})
        except Exception as e:
            logger.error(f"Error deleting property: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})


@method_decorator(require_POST, name="dispatch")
class PropertyDeployView(View):
    """View to deploy a property to DataHub"""

    def post(self, request, property_id):
        """Deploy a property to DataHub"""
        try:
            property = get_object_or_404(StructuredProperty, id=property_id)

            # Get DataHub client
            client = get_datahub_client()
            if not client or not client.test_connection():
                messages.error(
                    request,
                    "Not connected to DataHub. Please check your connection settings.",
                )
                return redirect(
                    "metadata_manager:property_detail", property_id=property_id
                )

            # Deploy to DataHub
            if property.sync_status == "LOCAL_ONLY":
                # Create new property
                urn = client.create_structured_property(
                    display_name=property.name,
                    description=property.description,
                    value_type=property.value_type,
                    entity_types=property.entity_types,
                    cardinality=property.cardinality,
                    allowed_values=property.allowed_values,
                    immutable=property.immutable,
                    show_in_search=property.show_in_search_filters,
                    show_as_badge=property.show_as_asset_badge,
                    show_in_summary=property.show_in_asset_summary,
                    qualified_name=property.qualified_name,
                )

                if urn:
                    # Mark as synced
                    property.sync_status = "SYNCED"
                    property.last_synced = timezone.now()
                    property.save()

                    messages.success(
                        request,
                        f"Property '{property.name}' created in DataHub successfully",
                    )
                else:
                    messages.error(
                        request,
                        f"Failed to create property '{property.name}' in DataHub",
                    )
            elif property.sync_status == "MODIFIED":
                # Update existing property
                success = client.update_structured_property(
                    property_urn=property.deterministic_urn,
                    display_name=property.name,
                    description=property.description,
                    show_in_search=property.show_in_search_filters,
                    show_as_badge=property.show_as_asset_badge,
                    show_in_summary=property.show_in_asset_summary,
                )

                if success:
                    # Mark as synced
                    property.sync_status = "SYNCED"
                    property.last_synced = timezone.now()
                    property.save()

                    messages.success(
                        request,
                        f"Property '{property.name}' updated in DataHub successfully",
                    )
                else:
                    messages.error(
                        request,
                        f"Failed to update property '{property.name}' in DataHub",
                    )
            else:
                messages.info(
                    request,
                    f"Property '{property.name}' is already synced with DataHub",
                )

            return redirect("metadata_manager:property_detail", property_id=property_id)
        except Exception as e:
            logger.error(f"Error deploying property: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:property_detail", property_id=property_id)


@method_decorator(require_POST, name="dispatch")
class PropertyPullView(View):
    """View to pull properties from DataHub"""

    def post(self, request, only_post=False):
        """Pull properties from DataHub"""
        try:
            # Get DataHub client
            client = get_datahub_client()
            if not client or not client.test_connection():
                messages.error(
                    request,
                    "Not connected to DataHub. Please check your connection settings.",
                )
                return redirect("metadata_manager:property_list")

            # Get remote properties
            remote_properties = client.list_structured_properties(count=1000)

            if not remote_properties:
                messages.info(request, "No properties found in DataHub")
                return redirect("metadata_manager:property_list")

            # Process each remote property
            imported_count = 0
            updated_count = 0

            for remote_property in remote_properties:
                # Extract property data
                urn = remote_property.get("urn")

                # Skip if no URN
                if not urn:
                    continue

                # Check if we already have this property
                existing_property = StructuredProperty.objects.filter(
                    deterministic_urn=urn
                ).first()

                # Process property data
                try:
                    definition = remote_property.get("definition", {}) or {}
                    settings = remote_property.get("settings", {}) or {}

                    # Extract basic property info
                    name = definition.get("displayName", "")
                    qualified_name = definition.get("qualifiedName", "")
                    description = definition.get("description", "")
                    cardinality = definition.get("cardinality", "SINGLE")
                    immutable = definition.get("immutable", False)

                    # Extract value type
                    value_type_info = definition.get("valueType", {}) or {}
                    value_type = value_type_info.get("type", "STRING")

                    # Extract entity types
                    entity_types_info = definition.get("entityTypes", []) or []
                    entity_types = [
                        et.get("type") for et in entity_types_info if et.get("type")
                    ]

                    # Extract allowed values
                    allowed_values_info = definition.get("allowedValues", []) or []
                    allowed_values = []

                    for av in allowed_values_info:
                        value_obj = av.get("value", {}) or {}
                        description = av.get("description", "")

                        # Handle different value types
                        value = None
                        if "stringValue" in value_obj:
                            value = value_obj.get("stringValue")
                        elif "numberValue" in value_obj:
                            value = value_obj.get("numberValue")
                        elif "booleanValue" in value_obj:
                            value = value_obj.get("booleanValue")

                        if value is not None:
                            allowed_values.append(
                                {"value": value, "description": description}
                            )

                    # Extract display settings
                    show_in_search_filters = settings.get("showInSearchFilters", True)
                    show_as_asset_badge = settings.get("showAsAssetBadge", True)
                    show_in_asset_summary = settings.get("showInAssetSummary", True)
                    show_in_columns_table = settings.get("showInColumnsTable", False)
                    is_hidden = settings.get("isHidden", False)

                    # Create or update property
                    if existing_property:
                        # Update existing property
                        existing_property.name = name
                        existing_property.description = description
                        existing_property.qualified_name = qualified_name
                        existing_property.value_type = value_type
                        existing_property.cardinality = cardinality
                        existing_property.immutable = immutable
                        existing_property.entity_types = entity_types
                        existing_property.allowed_values = allowed_values
                        existing_property.show_in_search_filters = (
                            show_in_search_filters
                        )
                        existing_property.show_as_asset_badge = show_as_asset_badge
                        existing_property.show_in_asset_summary = show_in_asset_summary
                        existing_property.show_in_columns_table = show_in_columns_table
                        existing_property.is_hidden = is_hidden
                        existing_property.sync_status = "SYNCED"
                        existing_property.last_synced = timezone.now()
                        existing_property.save()

                        updated_count += 1
                    else:
                        # Create new property
                        StructuredProperty.objects.create(
                            name=name,
                            description=description,
                            qualified_name=qualified_name,
                            value_type=value_type,
                            cardinality=cardinality,
                            immutable=immutable,
                            entity_types=entity_types,
                            allowed_values=allowed_values,
                            deterministic_urn=urn,
                            original_urn=urn,
                            sync_status="SYNCED",
                            last_synced=timezone.now(),
                            show_in_search_filters=show_in_search_filters,
                            show_as_asset_badge=show_as_asset_badge,
                            show_in_asset_summary=show_in_asset_summary,
                            show_in_columns_table=show_in_columns_table,
                            is_hidden=is_hidden,
                        )

                        imported_count += 1
                except Exception as e:
                    logger.error(f"Error processing property {urn}: {str(e)}")
                    continue

            # Show success message
            if imported_count > 0 or updated_count > 0:
                messages.success(
                    request,
                    f"Successfully imported {imported_count} new properties and updated {updated_count} existing properties",
                )
            else:
                messages.info(request, "No properties were imported or updated")

            return redirect("metadata_manager:property_list")
        except Exception as e:
            logger.error(f"Error pulling properties: {str(e)}")
            messages.error(request, f"An error occurred: {str(e)}")
            return redirect("metadata_manager:property_list")


@method_decorator(csrf_exempt, name="dispatch")
class PropertyValuesView(View):
    """View to list entity property values"""

    def get(self, request):
        """List entities with their property values"""
        try:
            # Get query parameters
            entity_type = request.GET.get("entity_type", "")
            property_urn = request.GET.get("property_urn", "")
            query = request.GET.get("query", "*")
            start = int(request.GET.get("start", "0"))
            count = int(request.GET.get("count", "20"))

            # Get DataHub client
            client = get_datahub_client()
            if not client or not client.test_connection():
                return JsonResponse(
                    {
                        "success": False,
                        "error": "Not connected to DataHub. Please check your connection settings.",
                    }
                )

            # TODO: Implement fetching entities with property values when method is added to DataHubRestClient
            # For now, return placeholder data
            return JsonResponse(
                {
                    "success": True,
                    "data": {
                        "entity_type": entity_type,
                        "property_urn": property_urn,
                        "query": query,
                        "start": start,
                        "count": count,
                        "total": 0,
                        "entities": [],
                    },
                }
            )
        except Exception as e:
            logger.error(f"Error fetching property values: {str(e)}")
            return JsonResponse({"success": False, "error": str(e)})
