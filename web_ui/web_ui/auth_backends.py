from django.contrib.auth.backends import BaseBackend
from django.contrib.auth.models import User
from django.contrib.auth import get_user_model

class AnonymousUserBackend(BaseBackend):
    """
    Authentication backend that automatically authenticates all users.
    This should only be used in local development environments.
    """
    
    def authenticate(self, request, **kwargs):
        """
        Always return the admin user (or create it if it doesn't exist).
        """
        User = get_user_model()
        
        # Try to get the admin user
        try:
            user = User.objects.get(username='admin')
        except User.DoesNotExist:
            # Create an admin user if one doesn't exist
            user = User.objects.create_superuser(
                username='admin', 
                email='admin@example.com', 
                password='admin'
            )
            
        return user
    
    def get_user(self, user_id):
        User = get_user_model()
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None 