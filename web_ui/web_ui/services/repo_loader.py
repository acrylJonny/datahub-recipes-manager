import os
import yaml
import json
import logging
from pathlib import Path
from django.conf import settings
from web_ui.models import (
    Environment, 
    RecipeTemplate, 
    EnvVarsTemplate, 
    EnvVarsInstance, 
    RecipeInstance
)

logger = logging.getLogger(__name__)

class RepositoryLoader:
    """
    Service for loading data from the repository directory structure.
    This is used to initialize the database from files on startup.
    """
    
    @classmethod
    def load_all(cls):
        """
        Load all data from the repository
        """
        logger.info("Starting repository data load")
        
        # Load in the correct order to maintain relationships
        environments_loaded = cls.load_environments()
        template_vars_loaded = cls.load_env_vars_templates()
        recipe_templates_loaded = cls.load_recipe_templates()
        env_vars_instances_loaded = cls.load_env_vars_instances()
        recipe_instances_loaded = cls.load_recipe_instances()
        
        logger.info(f"Repository data load complete: environments={environments_loaded}, " +
                  f"template_vars={template_vars_loaded}, recipe_templates={recipe_templates_loaded}, " +
                  f"env_vars_instances={env_vars_instances_loaded}, recipe_instances={recipe_instances_loaded}")
        
        return {
            "environments": environments_loaded,
            "template_vars": template_vars_loaded,
            "recipe_templates": recipe_templates_loaded,
            "env_vars_instances": env_vars_instances_loaded,
            "recipe_instances": recipe_instances_loaded,
        }
    
    @classmethod
    def load_environments(cls):
        """
        Load environments from the repository structure
        - Get from params/environments and recipes/instances directories
        """
        environments_loaded = 0
        
        # Get environment names from params directory
        params_dir = Path(settings.BASE_DIR).parent / 'params' / 'environments'
        if params_dir.exists() and params_dir.is_dir():
            for env_dir in params_dir.iterdir():
                if env_dir.is_dir():
                    env_name = env_dir.name
                    Environment.objects.get_or_create(name=env_name)
                    environments_loaded += 1
        
        # Get environment names from recipes/instances directory
        instances_dir = Path(settings.BASE_DIR).parent / 'recipes' / 'instances'
        if instances_dir.exists() and instances_dir.is_dir():
            for env_dir in instances_dir.iterdir():
                if env_dir.is_dir():
                    env_name = env_dir.name
                    Environment.objects.get_or_create(name=env_name)
                    environments_loaded += 1
        
        # Get environment names from recipes/templates directory
        templates_dir = Path(settings.BASE_DIR).parent / 'recipes' / 'templates'
        if templates_dir.exists() and templates_dir.is_dir():
            for env_dir in templates_dir.iterdir():
                if env_dir.is_dir():
                    env_name = env_dir.name
                    Environment.objects.get_or_create(name=env_name)
                    environments_loaded += 1
        
        # If no environments were created, create the default ones
        if Environment.objects.count() == 0:
            Environment.objects.create(name="dev", is_default=True)
            Environment.objects.create(name="test")
            Environment.objects.create(name="staging")
            Environment.objects.create(name="prod")
            environments_loaded = 4
        
        logger.info(f"Loaded {environments_loaded} environments")
        return environments_loaded
    
    @classmethod
    def load_env_vars_templates(cls):
        """
        Load environment variable templates from params/templates
        """
        templates_loaded = 0
        
        # Load from recipes/templates directory structure
        templates_dir = Path(settings.BASE_DIR).parent / 'recipes' / 'templates'
        if templates_dir.exists():
            # Load from environment-specific subdirectories
            for env_dir in templates_dir.iterdir():
                if env_dir.is_dir():
                    env_name = env_dir.name
                    try:
                        env = Environment.objects.get(name=env_name)
                        for template_file in env_dir.glob('*.yml'):
                            if cls._load_env_vars_template(template_file, env):
                                templates_loaded += 1
                    except Environment.DoesNotExist:
                        logger.warning(f"Skipping templates for unknown environment: {env_name}")
            
            # Also load from the root templates directory
            for template_file in templates_dir.glob('*.yml'):
                if cls._load_env_vars_template(template_file):
                    templates_loaded += 1
        
        logger.info(f"Loaded {templates_loaded} environment variable templates")
        return templates_loaded
    
    @classmethod
    def _load_env_vars_template(cls, template_file, environment=None):
        """
        Load a single environment variable template from a YAML file
        """
        try:
            with open(template_file, 'r') as f:
                template_data = yaml.safe_load(f)
            
            if not template_data:
                logger.warning(f"Empty template file: {template_file}")
                return False
            
            recipe_type = None
            name = template_file.stem
            variables_dict = {}
            
            # Try to extract recipe type from the file content
            if 'source' in template_data and 'type' in template_data['source']:
                recipe_type = template_data['source']['type']
            
            # Extract variables by looking at $ references
            if 'source' in template_data and 'config' in template_data['source']:
                for key, value in template_data['source']['config'].items():
                    if isinstance(value, str) and value.startswith('$'):
                        # Extract variable name from ${VAR} or $VAR format
                        var_name = value.strip('${}')
                        variables_dict[var_name] = {
                            'description': f'Variable for {key}',
                            'required': True,
                            'is_secret': False,
                            'data_type': 'text',
                            'default_value': ''
                        }
            
            # Create or update the template
            template, created = EnvVarsTemplate.objects.update_or_create(
                name=name,
                defaults={
                    'recipe_type': recipe_type or 'other',
                    'variables': json.dumps(variables_dict),
                    'description': f'Template for {name} loaded from repository'
                }
            )
            
            logger.info(f"{'Created' if created else 'Updated'} template: {template.name} ({template_file})")
            return True
            
        except Exception as e:
            logger.error(f"Error loading template from {template_file}: {str(e)}")
            return False
    
    @classmethod
    def load_recipe_templates(cls):
        """
        Load recipe templates from recipes/templates
        """
        templates_loaded = 0
        
        # Get templates directory
        templates_dir = Path(settings.BASE_DIR).parent / 'recipes' / 'templates'
        if templates_dir.exists():
            # Load from the root templates directory
            for template_file in templates_dir.glob('*.yml'):
                if cls._load_recipe_template(template_file):
                    templates_loaded += 1
            
            # Also load from environment-specific directories
            for env_dir in templates_dir.iterdir():
                if env_dir.is_dir():
                    for template_file in env_dir.glob('*.yml'):
                        if cls._load_recipe_template(template_file):
                            templates_loaded += 1
        
        logger.info(f"Loaded {templates_loaded} recipe templates")
        return templates_loaded
    
    @classmethod
    def _load_recipe_template(cls, template_file):
        """
        Load a single recipe template from a YAML file
        """
        try:
            with open(template_file, 'r') as f:
                template_data = yaml.safe_load(f)
            
            if not template_data:
                logger.warning(f"Empty template file: {template_file}")
                return False
            
            recipe_type = None
            name = template_file.stem
            
            # Extract recipe type and name
            if isinstance(template_data, dict):
                if 'name' in template_data:
                    name = template_data['name']
                if 'recipe_type' in template_data:
                    recipe_type = template_data['recipe_type']
                elif 'source' in template_data and 'type' in template_data['source']:
                    recipe_type = template_data['source']['type']
            
            # Get content as YAML string
            content = yaml.dump(template_data, default_flow_style=False)
            
            # Create or update the template
            template, created = RecipeTemplate.objects.update_or_create(
                name=name,
                defaults={
                    'recipe_type': recipe_type or template_file.stem,
                    'content': content,
                    'description': f'Recipe template for {name} loaded from repository'
                }
            )
            
            logger.info(f"{'Created' if created else 'Updated'} recipe template: {template.name} ({template_file})")
            return True
            
        except Exception as e:
            logger.error(f"Error loading recipe template from {template_file}: {str(e)}")
            return False
    
    @classmethod
    def load_env_vars_instances(cls):
        """
        Load environment variable instances from params/environments
        """
        instances_loaded = 0
        
        # Get params directory
        params_dir = Path(settings.BASE_DIR).parent / 'params' / 'environments'
        if params_dir.exists() and params_dir.is_dir():
            # Load from environment-specific subdirectories
            for env_dir in params_dir.iterdir():
                if env_dir.is_dir():
                    env_name = env_dir.name
                    try:
                        env = Environment.objects.get(name=env_name)
                        for instance_file in env_dir.glob('*.yml'):
                            if cls._load_env_vars_instance(instance_file, env):
                                instances_loaded += 1
                    except Environment.DoesNotExist:
                        logger.warning(f"Skipping instances for unknown environment: {env_name}")
        
        # Also check recipes/instances directory for environment variables
        instances_dir = Path(settings.BASE_DIR).parent / 'recipes' / 'instances'
        if instances_dir.exists() and instances_dir.is_dir():
            # Load from environment-specific subdirectories
            for env_dir in instances_dir.iterdir():
                if env_dir.is_dir():
                    env_name = env_dir.name
                    try:
                        env = Environment.objects.get(name=env_name)
                        for instance_file in env_dir.glob('*.yml'):
                            if cls._load_env_vars_instance_from_recipe(instance_file, env):
                                instances_loaded += 1
                    except Environment.DoesNotExist:
                        logger.warning(f"Skipping instances for unknown environment: {env_name}")
        
        logger.info(f"Loaded {instances_loaded} environment variable instances")
        return instances_loaded
    
    @classmethod
    def _load_env_vars_instance(cls, instance_file, environment):
        """
        Load a single environment variable instance from a YAML file
        """
        try:
            with open(instance_file, 'r') as f:
                instance_data = yaml.safe_load(f)
            
            if not instance_data:
                logger.warning(f"Empty instance file: {instance_file}")
                return False
            
            # Extract basic information
            name = instance_file.stem
            recipe_type = None
            description = None
            template = None
            variables_dict = {}
            
            if isinstance(instance_data, dict):
                if 'name' in instance_data:
                    name = instance_data['name']
                if 'description' in instance_data:
                    description = instance_data['description']
                if 'recipe_type' in instance_data:
                    recipe_type = instance_data['recipe_type']
                
                # Try to find matching template
                if recipe_type:
                    template = EnvVarsTemplate.objects.filter(recipe_type=recipe_type).first()
                
                # Extract variables
                if 'parameters' in instance_data:
                    for key, value in instance_data['parameters'].items():
                        variables_dict[key] = {
                            'value': value,
                            'isSecret': False
                        }
                
                # Add secret references if present
                if 'secret_references' in instance_data:
                    for key in instance_data['secret_references']:
                        if key not in variables_dict:
                            variables_dict[key] = {}
                        variables_dict[key]['isSecret'] = True
                        variables_dict[key]['value'] = f"<secret_{key}>"
            
            # Create or update the instance
            instance, created = EnvVarsInstance.objects.update_or_create(
                name=name,
                environment=environment,
                defaults={
                    'recipe_type': recipe_type or 'other',
                    'template': template,
                    'variables': json.dumps(variables_dict),
                    'description': description or f'Environment variables for {name} in {environment.name}'
                }
            )
            
            logger.info(f"{'Created' if created else 'Updated'} env vars instance: {instance.name} ({instance_file})")
            return True
            
        except Exception as e:
            logger.error(f"Error loading env vars instance from {instance_file}: {str(e)}")
            return False
    
    @classmethod
    def _load_env_vars_instance_from_recipe(cls, instance_file, environment):
        """
        Load environment variables from a recipe instance file
        """
        try:
            with open(instance_file, 'r') as f:
                instance_data = yaml.safe_load(f)
            
            if not instance_data:
                logger.warning(f"Empty recipe instance file: {instance_file}")
                return False
            
            # Skip if there are no environment variables
            if not instance_data.get('parameters') and not instance_data.get('secret_references'):
                return False
            
            # Extract basic information
            name = instance_file.stem
            recipe_type = None
            description = None
            template = None
            variables_dict = {}
            
            if isinstance(instance_data, dict):
                if 'name' in instance_data:
                    name = instance_data['name']
                if 'description' in instance_data:
                    description = instance_data['description']
                if 'recipe_type' in instance_data:
                    recipe_type = instance_data['recipe_type']
                
                # Try to find matching template
                if recipe_type:
                    template = EnvVarsTemplate.objects.filter(recipe_type=recipe_type).first()
                
                # Extract variables
                if 'parameters' in instance_data:
                    for key, value in instance_data['parameters'].items():
                        variables_dict[key] = {
                            'value': value,
                            'isSecret': False
                        }
                
                # Add secret references if present
                if 'secret_references' in instance_data:
                    for key in instance_data['secret_references']:
                        if key not in variables_dict:
                            variables_dict[key] = {}
                        variables_dict[key]['isSecret'] = True
                        variables_dict[key]['value'] = f"<secret_{key}>"
            
            # Only create if we have variables
            if variables_dict:
                # Create or update the instance
                instance_name = f"{name}_env_vars"
                instance, created = EnvVarsInstance.objects.update_or_create(
                    name=instance_name,
                    environment=environment,
                    defaults={
                        'recipe_type': recipe_type or 'other',
                        'template': template,
                        'variables': json.dumps(variables_dict),
                        'description': description or f'Environment variables extracted from {name} in {environment.name}'
                    }
                )
                
                logger.info(f"{'Created' if created else 'Updated'} env vars instance from recipe: {instance.name} ({instance_file})")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error loading env vars from recipe instance {instance_file}: {str(e)}")
            return False
    
    @classmethod
    def load_recipe_instances(cls):
        """
        Load recipe instances from recipes/instances
        """
        instances_loaded = 0
        
        # Get instances directory
        instances_dir = Path(settings.BASE_DIR).parent / 'recipes' / 'instances'
        if instances_dir.exists() and instances_dir.is_dir():
            # Load from environment-specific subdirectories
            for env_dir in instances_dir.iterdir():
                if env_dir.is_dir():
                    env_name = env_dir.name
                    try:
                        env = Environment.objects.get(name=env_name)
                        for instance_file in env_dir.glob('*.yml'):
                            if cls._load_recipe_instance(instance_file, env):
                                instances_loaded += 1
                    except Environment.DoesNotExist:
                        logger.warning(f"Skipping recipe instances for unknown environment: {env_name}")
        
        logger.info(f"Loaded {instances_loaded} recipe instances")
        return instances_loaded
    
    @classmethod
    def _load_recipe_instance(cls, instance_file, environment):
        """
        Load a single recipe instance from a YAML file
        """
        try:
            with open(instance_file, 'r') as f:
                instance_data = yaml.safe_load(f)
            
            if not instance_data:
                logger.warning(f"Empty recipe instance file: {instance_file}")
                return False
            
            # Extract basic information
            name = instance_file.stem
            recipe_type = None
            description = None
            template = None
            env_vars_instance = None
            
            if isinstance(instance_data, dict):
                if 'name' in instance_data:
                    name = instance_data['name']
                if 'description' in instance_data:
                    description = instance_data['description']
                if 'recipe_type' in instance_data:
                    recipe_type = instance_data['recipe_type']
                
                # Find matching template based on recipe type
                if recipe_type:
                    template = RecipeTemplate.objects.filter(recipe_type=recipe_type).first()
                    
                    # If there are multiple templates, try to find the best match
                    if not template:
                        templates = RecipeTemplate.objects.filter(recipe_type=recipe_type)
                        if templates.count() > 0:
                            # Use the first one for now
                            template = templates.first()
                
                # Find matching env vars instance
                instance_name = f"{name}_env_vars"
                env_vars_instance = EnvVarsInstance.objects.filter(
                    name=instance_name,
                    environment=environment
                ).first()
                
                # If we can't find one with the matching name, just use a compatible one
                if not env_vars_instance and recipe_type:
                    env_vars_instance = EnvVarsInstance.objects.filter(
                        recipe_type=recipe_type,
                        environment=environment
                    ).first()
            
            if not template:
                logger.warning(f"Can't find matching template for recipe instance: {name} (type: {recipe_type})")
                return False
            
            # Create or update the recipe instance
            instance, created = RecipeInstance.objects.update_or_create(
                name=name,
                environment=environment,
                defaults={
                    'template': template,
                    'env_vars_instance': env_vars_instance,
                    'description': description or f'Recipe instance for {name} in {environment.name}',
                }
            )
            
            logger.info(f"{'Created' if created else 'Updated'} recipe instance: {instance.name} ({instance_file})")
            return True
            
        except Exception as e:
            logger.error(f"Error loading recipe instance from {instance_file}: {str(e)}")
            return False 