from django.db import models
from django.utils import timezone
import json

class RecipeTemplate(models.Model):
    """Model for recipe templates."""
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    recipe_type = models.CharField(max_length=50)
    content = models.TextField()
    tags = models.CharField(max_length=255, blank=True)
    executor_id = models.CharField(max_length=50, default='default')
    cron_schedule = models.CharField(max_length=50, default='0 0 * * *')
    timezone = models.CharField(max_length=50, default='Etc/UTC')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    def get_tags_list(self):
        """Get list of tags from comma-separated string."""
        if not self.tags:
            return []
        return [tag.strip() for tag in self.tags.split(',')]

    def set_tags_list(self, tags):
        """Set tags from a list."""
        self.tags = ','.join(tags)

    def get_variables_dict(self):
        """Get dictionary of variables from content."""
        try:
            content_dict = json.loads(self.content)
            return content_dict.get('variables', {})
        except:
            return {}
