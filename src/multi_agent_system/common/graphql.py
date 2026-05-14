"""GraphQL API for flexible querying."""

import json
import logging
from typing import Dict, List, Optional, Any, Callable
from enum import Enum

logger = logging.getLogger('graphql')


class GraphQLOperation(Enum):
    QUERY = "query"
    MUTATION = "mutation"
    SUBSCRIPTION = "subscription"


class GraphQLField:
    """Represents a GraphQL field."""

    def __init__(self, name: str, field_type: str,
                 resolver: Callable = None, args: Dict = None):
        self.name = name
        self.field_type = field_type
        self.resolver = resolver
        self.args = args or {}


class GraphQLType:
    """Represents a GraphQL type."""

    def __init__(self, name: str, fields: List[GraphQLField]):
        self.name = name
        self.fields = {f.name: f for f in fields}


class GraphQLSchema:
    """Simple GraphQL schema builder."""

    def __init__(self):
        self.types: Dict[str, GraphQLType] = {}
        self.queries: Dict[str, GraphQLField] = {}
        self.mutations: Dict[str, GraphQLField] = {}

    def add_type(self, name: str, fields: List[GraphQLField]):
        self.types[name] = GraphQLType(name, fields)

    def add_query(self, name: str, field: GraphQLField):
        self.queries[name] = field

    def add_mutation(self, name: str, field: GraphQLField):
        self.mutations[name] = field

    def execute(self, query: str, variables: Dict = None,
                context: Dict = None) -> Dict:
        """Execute GraphQL query."""
        try:
            parsed = self._parse_query(query)
            operation = parsed['operation']
            selection = parsed['selection']
            variables = variables or {}
            context = context or {}

            if operation == 'query':
                return {'data': self._execute_query(selection, context)}
            elif operation == 'mutation':
                return {'data': self._execute_mutation(selection, variables, context)}
            else:
                return {'errors': [{'message': 'Unknown operation'}]}
        except Exception as e:
            logger.error(f'GraphQL error: {e}')
            return {'errors': [{'message': str(e)}]}

    def _parse_query(self, query: str) -> Dict:
        """Parse simple GraphQL query."""
        query = query.strip()

        if query.startswith('query'):
            operation = 'query'
            selection_start = query.find('{') + 1
        elif query.startswith('mutation'):
            operation = 'mutation'
            selection_start = query.find('{') + 1
        else:
            operation = 'query'
            selection_start = query.find('{') + 1

        selection_end = query.rfind('}')
        selection = query[selection_start:selection_end].strip()

        return {
            'operation': operation,
            'selection': selection
        }

    def _execute_query(self, selection: str, context: Dict) -> Dict:
        """Execute query selection."""
        result = {}
        fields = selection.split()

        for i in range(0, len(fields), 2):
            field_name = fields[i].strip()
            if field_name in self.queries:
                resolver = self.queries[field_name].resolver
                if resolver:
                    result[field_name] = resolver(context)
                else:
                    result[field_name] = None
            elif field_name in self.types:
                # Complex type selection
                result[field_name] = {}

        return result

    def _execute_mutation(self, selection: str, variables: Dict, context: Dict) -> Dict:
        """Execute mutation."""
        result = {}
        # Parse mutation fields
        parts = selection.split()
        for part in parts:
            if part in self.mutations:
                resolver = self.mutations[part].resolver
                if resolver:
                    result[part] = resolver(variables, context)
        return result

    def to_schema_string(self) -> str:
        """Generate GraphQL schema string."""
        lines = ['type Query {']
        for name, field in self.queries.items():
            args_str = ''
            if field.args:
                args_str = '(' + ', '.join(f'{k}: {v}' for k, v in field.args.items()) + ')'
            lines.append(f'  {name}{args_str}: {field.field_type}')
        lines.append('}')

        lines.append('')
        lines.append('type Mutation {')
        for name, field in self.mutations.items():
            lines.append(f'  {name}: {field.field_type}')
        lines.append('}')

        return '\n'.join(lines)


class GraphQLResolver:
    """Resolves GraphQL queries against orchestrator."""

    def __init__(self, orchestrator=None):
        self.orchestrator = orchestrator

    def resolve_workers(self, context: Dict) -> List[Dict]:
        if self.orchestrator:
            return self.orchestrator.get_workers_status()
        return []

    def resolve_tasks(self, context: Dict) -> List[Dict]:
        if self.orchestrator:
            return [
                {
                    'task_id': t.task_id,
                    'status': t.status,
                    'task_type': t.task_type,
                    'result': t.result
                }
                for t in self.orchestrator.tasks.values()
            ]
        return []

    def resolve_task(self, context: Dict, task_id: str = None) -> Optional[Dict]:
        if self.orchestrator and task_id:
            return self.orchestrator.get_task_status(task_id)
        return None

    def resolve_submit_task(self, variables: Dict, context: Dict) -> Dict:
        if self.orchestrator:
            task_type = variables.get('task_type', '')
            task_data = variables.get('task_data', {})
            task_id = self.orchestrator.submit_task(task_type, task_data)
            return {'task_id': task_id, 'status': 'pending'}
        return {'error': 'No orchestrator'}


def create_graphql_schema(orchestrator=None) -> GraphQLSchema:
    """Create GraphQL schema for multi-agent system."""
    schema = GraphQLSchema()
    resolver = GraphQLResolver(orchestrator)

    # Query types
    schema.add_query('workers', GraphQLField('workers', '[Worker]', resolver.resolve_workers))
    schema.add_query('tasks', GraphQLField('tasks', '[Task]', resolver.resolve_tasks))
    schema.add_query('task', GraphQLField('task', 'Task',
                                          lambda ctx: resolver.resolve_task(ctx, ctx.get('task_id')),
                                          {'task_id': 'String'}))

    # Mutation types
    schema.add_mutation('submitTask', GraphQLField('submitTask', 'TaskResult',
                                                   lambda v, c: resolver.resolve_submit_task(v, c)))
    schema.add_mutation('cancelTask', GraphQLField('cancelTask', 'Boolean'))

    return schema


class GraphQLHandler:
    """HTTP handler for GraphQL queries."""

    def __init__(self, schema: GraphQLSchema, context_provider: Callable = None):
        self.schema = schema
        self.context_provider = context_provider or (lambda: {})

    def handle(self, body: Dict, headers: Dict) -> Dict:
        """Handle GraphQL request."""
        query = body.get('query', '')
        variables = body.get('variables', {})
        operation_name = body.get('operationName')

        context = self.context_provider()
        context['headers'] = headers

        result = self.schema.execute(query, variables, context)
        return result


# Global GraphQL handler
_graphql_handler = None


def setup_graphql(orchestrator) -> GraphQLHandler:
    global _graphql_handler
    schema = create_graphql_schema(orchestrator)
    _graphql_handler = GraphQLHandler(schema)
    return _graphql_handler


def get_graphql_handler() -> Optional[GraphQLHandler]:
    return _graphql_handler