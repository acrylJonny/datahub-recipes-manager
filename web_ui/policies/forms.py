from django import forms
from .models import Policy

class PolicyForm(forms.ModelForm):
    """Form for policies."""
    class Meta:
        model = Policy
        fields = ['name', 'description', 'content', 'tags']
        widgets = {
            'description': forms.Textarea(attrs={'rows': 3}),
            'content': forms.Textarea(attrs={'rows': 10}),
            'tags': forms.TextInput(attrs={'placeholder': 'Comma-separated tags'}),
        } 