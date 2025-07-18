from django.db import models
import uuid
import json


class ScriptRun(models.Model):
    """Represents a script execution run."""

    STATUS_CHOICES = (
        ("pending", "Pending"),
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
    )

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    script_name = models.CharField(max_length=100)
    parameters = models.JSONField(default=dict)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.script_name} ({self.created_at.strftime('%Y-%m-%d %H:%M:%S')})"

    def get_parameters_display(self):
        """Return a formatted string of parameters."""
        if not self.parameters:
            return "No parameters"

        result = []
        for key, value in self.parameters.items():
            result.append(f"{key}: {value}")
        return ", ".join(result)


class ScriptResult(models.Model):
    """Stores the result of a script execution."""

    script_run = models.OneToOneField(
        ScriptRun, on_delete=models.CASCADE, related_name="result"
    )
    output = models.TextField(blank=True)
    error = models.TextField(blank=True)

    def __str__(self):
        return f"Result for {self.script_run}"

    def output_as_json(self):
        """Attempt to parse output as JSON and return it."""
        try:
            return json.loads(self.output)
        except json.JSONDecodeError:
            return None


class Artifact(models.Model):
    """Represents a file generated by a script execution."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    script_result = models.ForeignKey(
        ScriptResult, on_delete=models.CASCADE, related_name="artifacts"
    )
    name = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    filename = models.CharField(max_length=255)
    file = models.FileField(upload_to="artifacts/")
    content_type = models.CharField(max_length=100)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name
