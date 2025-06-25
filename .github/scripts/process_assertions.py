#!/usr/bin/env python3
"""
Script to process DataHub assertion files and execute the appropriate GraphQL mutations.
This script reads JSON files from metadata-manager/{environment}/assertions/ and calls
the corresponding DataHub GraphQL APIs.
"""

import json
import os
import sys
import glob
from pathlib import Path
from typing import Dict, List, Any, Optional
import requests


class DataHubAssertionProcessor:
    """Processes assertion files and calls DataHub GraphQL APIs"""
    
    def __init__(self, datahub_url: str, datahub_token: str, environment: str, dry_run: bool = False):
        self.datahub_url = datahub_url.rstrip('/')
        self.datahub_token = datahub_token
        self.environment = environment
        self.dry_run = dry_run
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {datahub_token}',
            'Content-Type': 'application/json'
        })
        
        # Track results
        self.results = {
            'processed': 0,
            'created': 0,
            'updated': 0,
            'errors': [],
            'details': []
        }
    
    def process_assertions(self) -> Dict[str, Any]:
        """Process all assertion files for the environment"""
        assertions_dir = Path(f'metadata-manager/{self.environment}/assertions')
        
        if not assertions_dir.exists():
            print(f"No assertions directory found for environment: {self.environment}")
            return self.results
        
        assertion_files = list(assertions_dir.glob('*.json'))
        print(f"Found {len(assertion_files)} assertion files to process")
        
        for file_path in assertion_files:
            try:
                self.process_assertion_file(file_path)
            except Exception as e:
                error_msg = f"Error processing {file_path}: {str(e)}"
                print(f"ERROR: {error_msg}")
                self.results['errors'].append(error_msg)
        
        # Write results to file for workflow
        with open(f'assertion-results-{self.environment}.json', 'w') as f:
            json.dump(self.results, f, indent=2)
        
        return self.results
    
    def process_assertion_file(self, file_path: Path) -> None:
        """Process a single assertion file"""
        print(f"Processing: {file_path}")
        
        with open(file_path, 'r') as f:
            assertion_data = json.load(f)
        
        operation = assertion_data.get('operation', 'create')
        assertion_type = assertion_data.get('assertion_type', '')
        name = assertion_data.get('name', '')
        graphql_input = assertion_data.get('graphql_input', {})
        
        if not graphql_input:
            raise ValueError(f"No GraphQL input found in {file_path}")
        
        mutation = graphql_input.get('mutation')
        input_data = graphql_input.get('input')
        
        if not mutation or not input_data:
            raise ValueError(f"Invalid GraphQL input in {file_path}")
        
        print(f"  Operation: {operation}")
        print(f"  Type: {assertion_type}")
        print(f"  Mutation: {mutation}")
        
        if self.dry_run:
            print(f"  DRY RUN: Would execute {mutation} for {name}")
            self.results['details'].append({
                'name': name,
                'type': assertion_type,
                'success': True,
                'message': f'DRY RUN: Would execute {mutation}'
            })
        else:
            result = self.execute_graphql_mutation(mutation, input_data)
            if result['success']:
                if operation == 'create':
                    self.results['created'] += 1
                else:
                    self.results['updated'] += 1
                
                self.results['details'].append({
                    'name': name,
                    'type': assertion_type,
                    'success': True,
                    'message': f'Successfully {operation}d assertion'
                })
            else:
                self.results['details'].append({
                    'name': name,
                    'type': assertion_type,
                    'success': False,
                    'message': result['error']
                })
        
        self.results['processed'] += 1
    
    def execute_graphql_mutation(self, mutation: str, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a GraphQL mutation"""
        
        # Build the GraphQL query based on mutation type
        query = self.build_graphql_query(mutation, input_data)
        
        if not query:
            return {'success': False, 'error': f'Unsupported mutation: {mutation}'}
        
        try:
            response = self.session.post(
                f'{self.datahub_url}/api/graphql',
                json={'query': query},
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                if 'errors' in result:
                    return {'success': False, 'error': str(result['errors'])}
                return {'success': True, 'data': result.get('data')}
            else:
                return {'success': False, 'error': f'HTTP {response.status_code}: {response.text}'}
                
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def build_graphql_query(self, mutation: str, input_data: Dict[str, Any]) -> Optional[str]:
        """Build GraphQL query string for the given mutation"""
        
        # Convert input data to GraphQL format
        input_str = self.dict_to_graphql(input_data)
        
        if mutation == 'createDatasetAssertion':
            return f"""
            mutation {{
                {mutation}(input: {input_str}) {{
                    urn
                    info {{
                        type
                        description
                    }}
                }}
            }}
            """
        
        elif mutation == 'createFreshnessAssertion':
            return f"""
            mutation {{
                {mutation}(input: {input_str}) {{
                    urn
                    info {{
                        type
                        description
                    }}
                }}
            }}
            """
        
        elif mutation == 'createVolumeAssertion':
            return f"""
            mutation {{
                {mutation}(input: {input_str}) {{
                    urn
                    info {{
                        type
                        description
                    }}
                }}
            }}
            """
        
        elif mutation == 'createSqlAssertion':
            return f"""
            mutation {{
                {mutation}(input: {input_str}) {{
                    urn
                    info {{
                        type
                        description
                    }}
                }}
            }}
            """
        
        elif mutation == 'createFieldAssertion':
            return f"""
            mutation {{
                {mutation}(input: {input_str}) {{
                    urn
                    info {{
                        type
                        description
                    }}
                }}
            }}
            """
        
        elif mutation == 'upsertCustomAssertion':
            urn = input_data.get('urn', '')
            input_str = self.dict_to_graphql(input_data.get('input', {}))
            urn_param = f'urn: "{urn}", ' if urn else ''
            return f"""
            mutation {{
                {mutation}({urn_param}input: {input_str}) {{
                    urn
                    info {{
                        type
                        description
                    }}
                }}
            }}
            """
        
        elif mutation.startswith('upsertDataset') and mutation.endswith('AssertionMonitor'):
            # For upsert mutations, we may need to handle URN parameter
            assertion_urn = input_data.get('assertionUrn', '')
            urn_param = f'assertionUrn: "{assertion_urn}", ' if assertion_urn else ''
            return f"""
            mutation {{
                {mutation}({urn_param}input: {input_str}) {{
                    urn
                    info {{
                        type
                        description
                    }}
                }}
            }}
            """
        
        elif mutation == 'updateDatasetAssertion':
            urn = input_data.get('urn', '')
            input_str = self.dict_to_graphql(input_data.get('input', {}))
            return f"""
            mutation {{
                {mutation}(urn: "{urn}", input: {input_str}) {{
                    urn
                    info {{
                        type
                        description
                    }}
                }}
            }}
            """
        
        return None
    
    def dict_to_graphql(self, data: Any, indent: int = 0) -> str:
        """Convert Python dict to GraphQL input format"""
        if isinstance(data, dict):
            if not data:
                return '{}'
            
            indent_str = '  ' * indent
            next_indent_str = '  ' * (indent + 1)
            
            items = []
            for key, value in data.items():
                formatted_value = self.dict_to_graphql(value, indent + 1)
                items.append(f'{next_indent_str}{key}: {formatted_value}')
            
            return '{\n' + '\n'.join(items) + f'\n{indent_str}}}'
        
        elif isinstance(data, list):
            if not data:
                return '[]'
            items = [self.dict_to_graphql(item, indent) for item in data]
            return '[' + ', '.join(items) + ']'
        
        elif isinstance(data, str):
            return f'"{data}"'
        
        elif isinstance(data, bool):
            return 'true' if data else 'false'
        
        elif data is None:
            return 'null'
        
        else:
            return str(data)


def main():
    """Main function"""
    datahub_url = os.getenv('DATAHUB_URL')
    datahub_token = os.getenv('DATAHUB_TOKEN')
    environment = os.getenv('ENVIRONMENT', 'dev')
    dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
    
    if not datahub_url or not datahub_token:
        print("ERROR: DATAHUB_URL and DATAHUB_TOKEN environment variables are required")
        sys.exit(1)
    
    print(f"Processing assertions for environment: {environment}")
    print(f"DataHub URL: {datahub_url}")
    print(f"Dry run: {dry_run}")
    
    processor = DataHubAssertionProcessor(datahub_url, datahub_token, environment, dry_run)
    results = processor.process_assertions()
    
    print(f"\nProcessing complete!")
    print(f"Processed: {results['processed']}")
    print(f"Created: {results['created']}")
    print(f"Updated: {results['updated']}")
    print(f"Errors: {len(results['errors'])}")
    
    if results['errors']:
        print("\nErrors:")
        for error in results['errors']:
            print(f"  - {error}")
        sys.exit(1)


if __name__ == '__main__':
    main() 