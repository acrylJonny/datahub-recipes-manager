"""
DataHub domain management scripts.
"""

from scripts.domains.domain_utils import (
    load_domain_from_file,
    save_domain_to_file,
    get_domain_name,
    get_domain_urn_from_id,
    get_domain_id_from_urn,
    get_parent_domains,
    get_domain_children,
    check_domain_dependencies,
    create_owner_object,
)
