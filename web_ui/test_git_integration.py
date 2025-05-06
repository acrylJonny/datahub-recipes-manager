#!/usr/bin/env python
import os
import sys
import django

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'web_ui.settings')
django.setup()

from web_ui.models import GitIntegration, Policy, RecipeTemplate

def test_integration():
    """Test GitIntegration to ensure all methods we need are available"""
    print("Testing GitIntegration class...")
    
    # Check push_to_git exists
    if hasattr(GitIntegration, 'push_to_git'):
        print("✅ GitIntegration.push_to_git method exists")
    else:
        print("❌ GitIntegration.push_to_git method does NOT exist")
    
    # Check for the old method
    if hasattr(GitIntegration, 'push_to_github'):
        print("❗ Old GitIntegration.push_to_github method still exists")
    else:
        print("✅ Old GitIntegration.push_to_github method does not exist")
    
    # Check if stage_changes exists
    if hasattr(GitIntegration, 'stage_changes'):
        print("✅ GitIntegration.stage_changes method exists")
    else:
        print("❌ GitIntegration.stage_changes method does NOT exist")
        
    print("\nGitIntegration instance methods:")
    git = GitIntegration()
    for method_name in dir(git):
        if not method_name.startswith('_') and callable(getattr(git, method_name)):
            print(f"  - {method_name}")
    
    # Check if Policy model is correctly defined
    try:
        print("\nVerifying Policy model...")
        policy_fields = [f.name for f in Policy._meta.get_fields()]
        print(f"Policy fields: {', '.join(policy_fields)}")
        print("✅ Policy model exists and can be imported")
    except Exception as e:
        print(f"❌ Error with Policy model: {str(e)}")
        
if __name__ == "__main__":
    test_integration() 