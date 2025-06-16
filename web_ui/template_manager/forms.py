from django import forms
from .models import RecipeTemplate


class RecipeTemplateForm(forms.ModelForm):
    """Form for recipe templates."""

    class Meta:
        model = RecipeTemplate
        fields = [
            "name",
            "description",
            "recipe_type",
            "content",
            "tags",
            "executor_id",
            "cron_schedule",
            "timezone",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            "content": forms.Textarea(attrs={"rows": 10}),
            "tags": forms.TextInput(attrs={"placeholder": "Comma-separated tags"}),
        }
