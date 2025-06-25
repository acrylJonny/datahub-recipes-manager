# Generated manually - Data migration to migrate AppSettings to Connection
from django.db import migrations
from django.utils import timezone


def migrate_appsettings_to_connection(apps, schema_editor):
    """Migrate existing AppSettings to a default Connection."""
    Settings = apps.get_model('web_ui', 'Settings')
    Connection = apps.get_model('web_ui', 'Connection')
    
    # Get existing settings
    datahub_url = None
    datahub_token = None
    verify_ssl = True
    timeout = 30
    
    try:
        url_setting = Settings.objects.get(key='datahub_url')
        datahub_url = url_setting.value
    except Settings.DoesNotExist:
        pass
    
    try:
        token_setting = Settings.objects.get(key='datahub_token')
        datahub_token = token_setting.value
    except Settings.DoesNotExist:
        pass
    
    try:
        ssl_setting = Settings.objects.get(key='verify_ssl')
        verify_ssl = ssl_setting.value.lower() in ('true', 't', 'yes', 'y', '1')
    except Settings.DoesNotExist:
        pass
    
    try:
        timeout_setting = Settings.objects.get(key='timeout')
        timeout = int(timeout_setting.value)
    except (Settings.DoesNotExist, ValueError):
        pass
    
    # Only create a connection if we have a URL
    if datahub_url:
        # Check if a default connection already exists
        if not Connection.objects.filter(is_default=True).exists():
            Connection.objects.create(
                name='Default Connection',
                description='Migrated from AppSettings configuration',
                datahub_url=datahub_url,
                datahub_token=datahub_token,
                verify_ssl=verify_ssl,
                timeout=timeout,
                is_active=True,
                is_default=True,
                connection_status='unknown',
                created_at=timezone.now(),
                updated_at=timezone.now()
            )


def reverse_migration(apps, schema_editor):
    """Reverse migration - remove the default connection and restore AppSettings."""
    Connection = apps.get_model('web_ui', 'Connection')
    Settings = apps.get_model('web_ui', 'Settings')
    
    # Get the default connection
    default_connection = Connection.objects.filter(is_default=True).first()
    
    if default_connection:
        # Restore settings
        Settings.objects.update_or_create(
            key='datahub_url',
            defaults={'value': default_connection.datahub_url}
        )
        
        if default_connection.datahub_token:
            Settings.objects.update_or_create(
                key='datahub_token',
                defaults={'value': default_connection.datahub_token}
            )
        
        Settings.objects.update_or_create(
            key='verify_ssl',
            defaults={'value': 'true' if default_connection.verify_ssl else 'false'}
        )
        
        Settings.objects.update_or_create(
            key='timeout',
            defaults={'value': str(default_connection.timeout)}
        )
        
        # Delete the connection
        default_connection.delete()


class Migration(migrations.Migration):
    dependencies = [
        ('web_ui', '0003_add_connection_model'),
    ]

    operations = [
        migrations.RunPython(
            migrate_appsettings_to_connection,
            reverse_migration,
        ),
    ] 