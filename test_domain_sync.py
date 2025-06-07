#!/usr/bin/env python
import os
import sys
import django

# Add the web_ui directory to the Python path
sys.path.insert(0, '/Users/jonny/Documents/GitHub/datahub-recipes-manager/web_ui')
sys.path.insert(0, '/Users/jonny/Documents/GitHub/datahub-recipes-manager')

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web_ui.settings')
django.setup()

from metadata_manager.models import Domain
from utils.datahub_connection import test_datahub_connection

def test_domain_sync():
    print("Testing domain sync functionality...")
    
    # Test connection
    connected, client = test_datahub_connection()
    if not connected:
        print("❌ Failed to connect to DataHub")
        return
    
    print("✅ Connected to DataHub successfully")
    
    # Get domains from GraphQL
    domains = client.list_domains(count=3)
    print(f"📊 Found {len(domains)} domains from GraphQL")
    
    if not domains:
        print("❌ No domains found in DataHub")
        return
    
    # Test the first domain
    domain_data = domains[0]
    domain_urn = domain_data.get('urn')
    domain_name = domain_data.get('properties', {}).get('name', 'Unknown')
    
    print(f"\n🔍 Testing domain: {domain_name}")
    print(f"   URN: {domain_urn}")
    
    # Check display properties
    display_props = domain_data.get('displayProperties')
    if display_props:
        print("🎨 Display Properties found:")
        if display_props.get('colorHex'):
            print(f"   Color: {display_props['colorHex']}")
        if display_props.get('icon'):
            icon = display_props['icon']
            print(f"   Icon: {icon.get('name', 'None')} ({icon.get('style', 'solid')}, {icon.get('iconLibrary', 'font-awesome')})")
    else:
        print("⚪ No display properties found")
    
    # Check ownership
    ownership = domain_data.get('ownership')
    owners_count = len(ownership.get('owners', [])) if ownership else 0
    print(f"👥 Owners: {owners_count}")
    
    # Check relationships
    relationships = domain_data.get('relationships')
    relationships_count = len(relationships.get('relationships', [])) if relationships else 0
    print(f"🔗 Relationships: {relationships_count}")
    
    # Check if domain exists locally
    existing_domain = Domain.objects.filter(deterministic_urn=domain_urn).first()
    if existing_domain:
        print(f"\n📝 Found existing local domain: {existing_domain.name}")
        print(f"   Current color: {existing_domain.color_hex}")
        print(f"   Current icon: {existing_domain.icon_name}")
        print(f"   Current owners count: {existing_domain.owners_count}")
        print(f"   Current relationships count: {existing_domain.relationships_count}")
        
        # Update the domain with new data
        print("\n🔄 Updating domain with GraphQL data...")
        
        # Update display properties
        if display_props:
            existing_domain.color_hex = display_props.get('colorHex')
            icon_data = display_props.get('icon', {})
            existing_domain.icon_name = icon_data.get('name')
            existing_domain.icon_style = icon_data.get('style', 'solid')
            existing_domain.icon_library = icon_data.get('iconLibrary', 'font-awesome')
        
        # Update counts
        existing_domain.owners_count = owners_count
        existing_domain.relationships_count = relationships_count
        
        # Store raw data
        existing_domain.raw_data = domain_data
        existing_domain.save()
        
        print("✅ Domain updated successfully!")
        print(f"   New color: {existing_domain.color_hex}")
        print(f"   New icon: {existing_domain.icon_name}")
        print(f"   New owners count: {existing_domain.owners_count}")
        print(f"   New relationships count: {existing_domain.relationships_count}")
        
    else:
        print(f"\n❌ Domain {domain_name} not found locally")
        print("   You may need to pull domains first")

if __name__ == "__main__":
    test_domain_sync() 