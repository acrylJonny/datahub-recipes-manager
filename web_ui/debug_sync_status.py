#!/usr/bin/env python
import os
import django

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web_ui.settings')
django.setup()

from metadata_manager.models import Assertion

print("=== Assertion Sync Status Debug ===")

# Check all assertions
all_assertions = Assertion.objects.all()
print(f"Total assertions: {all_assertions.count()}")

# Check synced assertions
synced = Assertion.objects.filter(sync_status='SYNCED')
print(f"Synced assertions: {synced.count()}")

# Check assertions with original_urn
with_original_urn = Assertion.objects.exclude(original_urn__isnull=True).exclude(original_urn='')
print(f"Assertions with original_urn: {with_original_urn.count()}")

print("\n=== Sample Assertions ===")
for i, assertion in enumerate(all_assertions[:10]):
    sync_status = getattr(assertion, 'sync_status', 'NO_FIELD')
    original_urn = getattr(assertion, 'original_urn', 'NO_FIELD')
    print(f"{i+1}. {assertion.name[:50]}...")
    print(f"   sync_status: {sync_status}")
    print(f"   original_urn: {original_urn}")
    print()

print("\n=== Synced Assertions Details ===")
for assertion in synced:
    print(f"- {assertion.name}")
    print(f"  sync_status: {assertion.sync_status}")
    print(f"  original_urn: {assertion.original_urn}")
    print(f"  last_synced: {getattr(assertion, 'last_synced', 'NO_FIELD')}")
    print() 