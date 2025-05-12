from django.core.management.base import BaseCommand
import os
from pathlib import Path
import logging
import yaml
from django.conf import settings

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Initialize the repository directory structure for recipes and environment variables'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Initializing repository structure...'))
        
        # Get base project directory
        base_dir = Path(settings.BASE_DIR).parent
        
        # Directory structure to create
        directories = [
            base_dir / 'recipes' / 'templates',
            base_dir / 'recipes' / 'instances',
            base_dir / 'recipes' / 'pulled',
            base_dir / 'params' / 'environments',
            base_dir / 'policies',
            base_dir / 'scripts',
        ]
        
        # Add environment directories
        environments = ['dev', 'test', 'staging', 'prod']
        for env in environments:
            directories.extend([
                base_dir / 'recipes' / 'templates' / env,
                base_dir / 'recipes' / 'instances' / env,
                base_dir / 'params' / 'environments' / env,
                base_dir / 'policies' / env,
            ])
        
        # Create all directories
        created_count = 0
        for directory in directories:
            if not directory.exists():
                directory.mkdir(parents=True, exist_ok=True)
                self.stdout.write(f'Created: {directory}')
                created_count += 1
        
        # Create README files with explanations
        readme_files = {
            base_dir / 'recipes' / 'README.md': """# Recipes Directory

This directory contains DataHub recipe templates and instances.

- `templates/`: Contains recipe template files
- `instances/`: Contains recipe instance files for specific environments
- `pulled/`: Contains recipes pulled from DataHub
""",
            base_dir / 'recipes' / 'templates' / 'README.md': """# Recipe Templates

This directory contains recipe templates that can be instantiated with environment variables.
Template files are organized by recipe type, with environment-specific templates in subdirectories.
""",
            base_dir / 'recipes' / 'instances' / 'README.md': """# Recipe Instances

This directory contains recipe instances organized by environment.
Each subdirectory corresponds to an environment (dev, test, staging, prod).
""",
            base_dir / 'params' / 'README.md': """# Parameters Directory

This directory contains environment variables and other parameters for recipes.

- `environments/`: Contains environment variables organized by environment
- `default_params.yaml`: Default parameters that apply to all environments
""",
            base_dir / 'params' / 'environments' / 'README.md': """# Environment Variables 

This directory contains environment variable definitions organized by environment.
Each subdirectory corresponds to an environment (dev, test, staging, prod).
Files are in YAML format with the following structure:

```yaml
name: "MySQL Environment Variables"
description: "Environment variables for MySQL integration"
recipe_type: "mysql"
parameters:
  HOST: "localhost"
  PORT: 3306
  DATABASE: "example"
secret_references:
  - USERNAME
  - PASSWORD
```

Secret references point to GitHub secrets that will be used during deployment.
"""
        }
        
        # Create README files
        for file_path, content in readme_files.items():
            if not file_path.exists():
                with open(file_path, 'w') as f:
                    f.write(content)
                self.stdout.write(f'Created: {file_path}')
        
        # Create example environment variable template for MySQL
        mysql_template_dir = base_dir / 'recipes' / 'templates' / 'dev'
        mysql_template_file = mysql_template_dir / 'mysql.yml'
        if not mysql_template_file.exists():
            mysql_template = {
                'name': 'MySQL Template',
                'description': 'Template for MySQL database connection',
                'recipe_type': 'mysql',
                'source': {
                    'type': 'mysql',
                    'config': {
                        'host': '${HOST}',
                        'port': '${PORT}',
                        'database': '${DATABASE}',
                        'username': '${USERNAME}',
                        'password': '${PASSWORD}',
                        'includeViews': '${INCLUDE_VIEWS}',
                        'includeTables': '${INCLUDE_TABLES}'
                    }
                }
            }
            
            with open(mysql_template_file, 'w') as f:
                yaml.dump(mysql_template, f, default_flow_style=False)
            self.stdout.write(f'Created example template: {mysql_template_file}')
        
        # Create example environment variables
        mysql_vars_dir = base_dir / 'params' / 'environments' / 'dev'
        mysql_vars_file = mysql_vars_dir / 'mysql_vars.yml'
        if not mysql_vars_file.exists():
            mysql_vars = {
                'name': 'MySQL Environment Variables',
                'description': 'Environment variables for MySQL database connections',
                'recipe_type': 'mysql',
                'variables': {
                    'HOST': {
                        'description': 'Database host',
                        'required': True,
                        'is_secret': False,
                        'data_type': 'text',
                        'default_value': 'localhost'
                    },
                    'PORT': {
                        'description': 'Database port',
                        'required': True,
                        'is_secret': False,
                        'data_type': 'number',
                        'default_value': '3306'
                    },
                    'DATABASE': {
                        'description': 'Database name',
                        'required': True,
                        'is_secret': False,
                        'data_type': 'text',
                        'default_value': 'mydb'
                    },
                    'USERNAME': {
                        'description': 'Database username',
                        'required': True,
                        'is_secret': True,
                        'data_type': 'text',
                        'default_value': ''
                    },
                    'PASSWORD': {
                        'description': 'Database password',
                        'required': True,
                        'is_secret': True,
                        'data_type': 'text',
                        'default_value': ''
                    },
                    'INCLUDE_VIEWS': {
                        'description': 'Include views in ingestion',
                        'required': False,
                        'is_secret': False,
                        'data_type': 'boolean',
                        'default_value': 'true'
                    },
                    'INCLUDE_TABLES': {
                        'description': 'Tables to include (comma-separated)',
                        'required': False,
                        'is_secret': False,
                        'data_type': 'text',
                        'default_value': '*'
                    }
                }
            }
            
            with open(mysql_vars_file, 'w') as f:
                yaml.dump(mysql_vars, f, default_flow_style=False)
            self.stdout.write(f'Created example variables: {mysql_vars_file}')
        
        # Create example MySQL instance for dev
        mysql_instance_dir = base_dir / 'params' / 'environments' / 'dev'
        mysql_instance_file = mysql_instance_dir / 'mysql-dev.yml'
        if not mysql_instance_file.exists():
            mysql_instance = {
                'name': 'MySQL Dev Instance',
                'description': 'Development MySQL connection configuration',
                'recipe_type': 'mysql',
                'template': 'MySQL Template',
                'parameters': {
                    'HOST': 'localhost',
                    'PORT': 3306,
                    'DATABASE': 'dev_database',
                    'INCLUDE_VIEWS': True,
                    'INCLUDE_TABLES': '*'
                },
                'secret_references': [
                    'USERNAME',
                    'PASSWORD'
                ]
            }
            
            with open(mysql_instance_file, 'w') as f:
                yaml.dump(mysql_instance, f, default_flow_style=False)
            self.stdout.write(f'Created example instance: {mysql_instance_file}')
        
        # Create example recipe instance reference
        recipe_instance_dir = base_dir / 'recipes' / 'instances' / 'dev'
        recipe_instance_file = recipe_instance_dir / 'mysql-dev.yml'
        if not recipe_instance_file.exists():
            recipe_instance = {
                'name': 'MySQL Dev Database',
                'description': 'MySQL development database ingestion',
                'recipe_type': 'mysql',
                'template_name': 'MySQL Template',
                'env_vars_instance': 'MySQL Dev Instance',
                'parameters': {
                    'HOST': 'localhost',
                    'PORT': 3306,
                    'DATABASE': 'dev_database',
                    'INCLUDE_VIEWS': True,
                    'INCLUDE_TABLES': '*'
                },
                'secret_references': [
                    'USERNAME',
                    'PASSWORD'
                ]
            }
            
            with open(recipe_instance_file, 'w') as f:
                yaml.dump(recipe_instance, f, default_flow_style=False)
            self.stdout.write(f'Created example recipe instance: {recipe_instance_file}')
        
        # Create default parameters file
        default_params_file = base_dir / 'params' / 'default_params.yaml'
        if not default_params_file.exists():
            default_params = {
                'datahub': {
                    'url': 'http://localhost:8080',
                    'token': '${DATAHUB_TOKEN}'
                },
                'logging': {
                    'level': 'INFO',
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                },
                'execution': {
                    'timeout': 300,
                    'retry': 3,
                    'debug': False
                }
            }
            
            with open(default_params_file, 'w') as f:
                yaml.dump(default_params, f, default_flow_style=False)
            self.stdout.write(f'Created default parameters: {default_params_file}')
        
        # Create .gitignore file
        gitignore_file = base_dir / 'params' / '.gitignore'
        if not gitignore_file.exists():
            gitignore_content = """# Ignore local environment files
.env
*.env
"""
            with open(gitignore_file, 'w') as f:
                f.write(gitignore_content)
            self.stdout.write(f'Created .gitignore: {gitignore_file}')
            
        self.stdout.write(self.style.SUCCESS(f'Repository structure initialized successfully with {created_count} new directories')) 