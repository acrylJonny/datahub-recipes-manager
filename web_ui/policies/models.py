from django.db import models
from django.utils import timezone

class Policy(models.Model):
    """Model for DataHub policies."""
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    content = models.TextField()
    tags = models.CharField(max_length=255, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    is_deployed = models.BooleanField(default=False)
    deployed_at = models.DateTimeField(null=True, blank=True)

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

    def deploy(self):
        """Mark policy as deployed."""
        self.is_deployed = True
        self.deployed_at = timezone.now()
        self.save()
