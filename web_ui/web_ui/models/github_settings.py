from django.db import models

class GitHubSettings(models.Model):
    """
    Model for storing GitHub integration settings
    """
    token = models.CharField(max_length=255, blank=True, null=True)
    repository = models.CharField(max_length=255, blank=True, null=True)
    base_branch = models.CharField(max_length=255, default='main')
    username = models.CharField(max_length=255, blank=True, null=True)
    email = models.CharField(max_length=255, blank=True, null=True)
    
    def __str__(self):
        return f"GitHub Settings for {self.repository or 'not configured'}"
    
    class Meta:
        verbose_name = "GitHub Settings"
        verbose_name_plural = "GitHub Settings" 