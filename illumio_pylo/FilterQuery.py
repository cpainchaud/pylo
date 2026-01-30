"""
Filter Query Engine for illumio_pylo

Provides a SQL-like query language to filter objects in the library.

Example queries:
    "name == 'SRV158'"
    "(name == 'SRV158' or name == 'SRV48889') and ip_address == '192.168.2.54'"
    "last_heartbeat <= '2022-09-12' and online == true"
    "hostname contains 'prod' and label.env == 'Production'"
    "name matches 'SRV[0-9]+'"
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, date
from enum import Enum, auto
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union
import re

import illumio_pylo as pylo


class TokenType(Enum):
    """Token types for the query lexer"""
    LPAREN = auto()      # (
    RPAREN = auto()      # )
    AND = auto()         # and
    OR = auto()          # or
    NOT = auto()         # not
    EQ = auto()          # ==
    NEQ = auto()         # !=
    LT = auto()          # <
    GT = auto()          # >
    LTE = auto()         # <=
    GTE = auto()         # >=
    CONTAINS = auto()    # contains
    MATCHES = auto()     # matches (regex)
    IDENTIFIER = auto()  # field names
    STRING = auto()      # 'value' or "value"
    NUMBER = auto()      # 123 or 123.45
    BOOLEAN = auto()     # true or false
    DATE = auto()        # date value parsed from string
    EOF = auto()         # end of input


@dataclass
class Token:
    """A single token from the query string"""
    type: TokenType
    value: Any
    position: int


class QueryLexer:
    """Tokenizes a query string into tokens"""

    KEYWORDS = {
        'and': TokenType.AND,
        'or': TokenType.OR,
        'not': TokenType.NOT,
        'contains': TokenType.CONTAINS,
        'matches': TokenType.MATCHES,
        'true': TokenType.BOOLEAN,
        'false': TokenType.BOOLEAN,
    }

    OPERATORS = {
        '==': TokenType.EQ,
        '!=': TokenType.NEQ,
        '<=': TokenType.LTE,
        '>=': TokenType.GTE,
        '<': TokenType.LT,
        '>': TokenType.GT,
    }

    def __init__(self, query: str):
        self.query = query
        self.pos = 0
        self.length = len(query)

    def _skip_whitespace(self):
        while self.pos < self.length and self.query[self.pos].isspace():
            self.pos += 1

    def _read_string(self, quote_char: str) -> str:
        """Read a quoted string, handling escape sequences"""
        result = []
        self.pos += 1  # skip opening quote

        while self.pos < self.length:
            char = self.query[self.pos]
            if char == '\\' and self.pos + 1 < self.length:
                # Handle escape sequences
                next_char = self.query[self.pos + 1]
                if next_char in (quote_char, '\\'):
                    result.append(next_char)
                    self.pos += 2
                    continue
            elif char == quote_char:
                self.pos += 1  # skip closing quote
                return ''.join(result)
            result.append(char)
            self.pos += 1

        raise pylo.PyloEx(f"Unterminated string starting at position {self.pos}")

    def _read_identifier(self) -> str:
        """Read an identifier (field name, including dots for nested fields)"""
        start = self.pos
        while self.pos < self.length:
            char = self.query[self.pos]
            if char.isalnum() or char in ('_', '.'):
                self.pos += 1
            else:
                break
        return self.query[start:self.pos]

    def _read_number(self) -> Union[int, float]:
        """Read a numeric value"""
        start = self.pos
        has_dot = False

        while self.pos < self.length:
            char = self.query[self.pos]
            if char.isdigit():
                self.pos += 1
            elif char == '.' and not has_dot:
                has_dot = True
                self.pos += 1
            else:
                break

        value_str = self.query[start:self.pos]
        return float(value_str) if has_dot else int(value_str)

    def tokenize(self) -> List[Token]:
        """Convert the query string into a list of tokens"""
        tokens = []

        while self.pos < self.length:
            self._skip_whitespace()
            if self.pos >= self.length:
                break

            start_pos = self.pos
            char = self.query[self.pos]

            # Single character tokens
            if char == '(':
                tokens.append(Token(TokenType.LPAREN, '(', start_pos))
                self.pos += 1
            elif char == ')':
                tokens.append(Token(TokenType.RPAREN, ')', start_pos))
                self.pos += 1
            # Quoted strings
            elif char in ('"', "'"):
                value = self._read_string(char)
                tokens.append(Token(TokenType.STRING, value, start_pos))
            # Numbers
            elif char.isdigit():
                value = self._read_number()
                tokens.append(Token(TokenType.NUMBER, value, start_pos))
            # Two-character operators
            elif self.pos + 1 < self.length:
                two_char = self.query[self.pos:self.pos + 2]
                if two_char in self.OPERATORS:
                    tokens.append(Token(self.OPERATORS[two_char], two_char, start_pos))
                    self.pos += 2
                elif char in '<>':
                    tokens.append(Token(self.OPERATORS[char], char, start_pos))
                    self.pos += 1
                elif char.isalpha() or char == '_':
                    # Identifier or keyword
                    identifier = self._read_identifier()
                    lower_id = identifier.lower()
                    if lower_id in self.KEYWORDS:
                        token_type = self.KEYWORDS[lower_id]
                        value = True if lower_id == 'true' else (False if lower_id == 'false' else lower_id)
                        tokens.append(Token(token_type, value, start_pos))
                    else:
                        tokens.append(Token(TokenType.IDENTIFIER, identifier, start_pos))
                else:
                    raise pylo.PyloEx(f"Unexpected character '{char}' at position {self.pos}")
            elif char in '<>':
                tokens.append(Token(self.OPERATORS[char], char, start_pos))
                self.pos += 1
            elif char.isalpha() or char == '_':
                identifier = self._read_identifier()
                lower_id = identifier.lower()
                if lower_id in self.KEYWORDS:
                    token_type = self.KEYWORDS[lower_id]
                    value = True if lower_id == 'true' else (False if lower_id == 'false' else lower_id)
                    tokens.append(Token(token_type, value, start_pos))
                else:
                    tokens.append(Token(TokenType.IDENTIFIER, identifier, start_pos))
            else:
                raise pylo.PyloEx(f"Unexpected character '{char}' at position {self.pos}")

        tokens.append(Token(TokenType.EOF, None, self.pos))
        return tokens


class QueryNode(ABC):
    """Base class for all AST nodes"""

    @abstractmethod
    def evaluate(self, obj: Any, registry: 'FilterRegistry') -> bool:
        """Evaluate this node against an object"""
        pass


class AndNode(QueryNode):
    """Represents an AND operation between two nodes"""

    def __init__(self, left: QueryNode, right: QueryNode):
        self.left = left
        self.right = right

    def evaluate(self, obj: Any, registry: 'FilterRegistry') -> bool:
        return self.left.evaluate(obj, registry) and self.right.evaluate(obj, registry)

    def __repr__(self):
        return f"AndNode({self.left}, {self.right})"


class OrNode(QueryNode):
    """Represents an OR operation between two nodes"""

    def __init__(self, left: QueryNode, right: QueryNode):
        self.left = left
        self.right = right

    def evaluate(self, obj: Any, registry: 'FilterRegistry') -> bool:
        return self.left.evaluate(obj, registry) or self.right.evaluate(obj, registry)

    def __repr__(self):
        return f"OrNode({self.left}, {self.right})"


class NotNode(QueryNode):
    """Represents a NOT operation on a node"""

    def __init__(self, operand: QueryNode):
        self.operand = operand

    def evaluate(self, obj: Any, registry: 'FilterRegistry') -> bool:
        return not self.operand.evaluate(obj, registry)

    def __repr__(self):
        return f"NotNode({self.operand})"


class ConditionNode(QueryNode):
    """Represents a single condition (field operator value)"""

    def __init__(self, field: str, operator: TokenType, value: Any):
        self.field = field
        self.operator = operator
        self.value = value

    def evaluate(self, obj: Any, registry: 'FilterRegistry') -> bool:
        return registry.evaluate_condition(obj, self.field, self.operator, self.value)

    def __repr__(self):
        return f"ConditionNode({self.field} {self.operator.name} {self.value!r})"


class QueryParser:
    """Parses a list of tokens into an AST"""

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0

    def _current(self) -> Token:
        return self.tokens[self.pos]

    def _advance(self) -> Token:
        token = self.tokens[self.pos]
        self.pos += 1
        return token

    def _expect(self, token_type: TokenType) -> Token:
        token = self._current()
        if token.type != token_type:
            raise pylo.PyloEx(
                f"Expected {token_type.name} but got {token.type.name} at position {token.position}"
            )
        return self._advance()

    def parse(self) -> QueryNode:
        """Parse the tokens into an AST"""
        if self._current().type == TokenType.EOF:
            raise pylo.PyloEx("Empty query")

        node = self._parse_or()

        if self._current().type != TokenType.EOF:
            raise pylo.PyloEx(
                f"Unexpected token '{self._current().value}' at position {self._current().position}"
            )

        return node

    def _parse_or(self) -> QueryNode:
        """Parse OR expressions (lowest precedence)"""
        left = self._parse_and()

        while self._current().type == TokenType.OR:
            self._advance()  # consume 'or'
            right = self._parse_and()
            left = OrNode(left, right)

        return left

    def _parse_and(self) -> QueryNode:
        """Parse AND expressions"""
        left = self._parse_not()

        while self._current().type == TokenType.AND:
            self._advance()  # consume 'and'
            right = self._parse_not()
            left = AndNode(left, right)

        return left

    def _parse_not(self) -> QueryNode:
        """Parse NOT expressions"""
        if self._current().type == TokenType.NOT:
            self._advance()  # consume 'not'
            operand = self._parse_not()  # Allow chained NOT
            return NotNode(operand)

        return self._parse_primary()

    def _parse_primary(self) -> QueryNode:
        """Parse primary expressions (conditions or parenthesized expressions)"""
        token = self._current()

        if token.type == TokenType.LPAREN:
            self._advance()  # consume '('
            node = self._parse_or()
            self._expect(TokenType.RPAREN)
            return node

        if token.type == TokenType.IDENTIFIER:
            return self._parse_condition()

        raise pylo.PyloEx(
            f"Unexpected token '{token.value}' at position {token.position}"
        )

    def _parse_condition(self) -> ConditionNode:
        """Parse a single condition: field operator value"""
        field_token = self._expect(TokenType.IDENTIFIER)
        field = field_token.value

        # Get operator
        op_token = self._current()
        if op_token.type in (TokenType.EQ, TokenType.NEQ, TokenType.LT, TokenType.GT,
                             TokenType.LTE, TokenType.GTE, TokenType.CONTAINS, TokenType.MATCHES):
            self._advance()
            operator = op_token.type
        else:
            raise pylo.PyloEx(
                f"Expected comparison operator but got '{op_token.value}' at position {op_token.position}"
            )

        # Get value
        value_token = self._current()
        if value_token.type == TokenType.STRING:
            value = value_token.value
        elif value_token.type == TokenType.NUMBER:
            value = value_token.value
        elif value_token.type == TokenType.BOOLEAN:
            value = value_token.value
        elif value_token.type == TokenType.IDENTIFIER:
            # Allow unquoted identifiers as values (for enums, etc.)
            value = value_token.value
        else:
            raise pylo.PyloEx(
                f"Expected value but got '{value_token.value}' at position {value_token.position}"
            )
        self._advance()

        return ConditionNode(field, operator, value)


class ValueType(Enum):
    """Supported value types for filter fields"""
    STRING = auto()
    INT = auto()
    FLOAT = auto()
    BOOLEAN = auto()
    DATE = auto()
    DATETIME = auto()
    IP_ADDRESS = auto()


T = TypeVar('T')


@dataclass
class FilterField(Generic[T]):
    """Definition of a filterable field"""
    name: str
    value_type: ValueType
    getter: Callable[[T], Any]
    description: str = ""
    supported_operators: Optional[List[TokenType]] = None

    def __post_init__(self):
        if self.supported_operators is None:
            # Set default operators based on value type
            if self.value_type == ValueType.STRING:
                self.supported_operators = [
                    TokenType.EQ, TokenType.NEQ, TokenType.CONTAINS, TokenType.MATCHES
                ]
            elif self.value_type in (ValueType.INT, ValueType.FLOAT):
                self.supported_operators = [
                    TokenType.EQ, TokenType.NEQ, TokenType.LT, TokenType.GT,
                    TokenType.LTE, TokenType.GTE
                ]
            elif self.value_type == ValueType.BOOLEAN:
                self.supported_operators = [TokenType.EQ, TokenType.NEQ]
            elif self.value_type in (ValueType.DATE, ValueType.DATETIME):
                self.supported_operators = [
                    TokenType.EQ, TokenType.NEQ, TokenType.LT, TokenType.GT,
                    TokenType.LTE, TokenType.GTE
                ]
            elif self.value_type == ValueType.IP_ADDRESS:
                self.supported_operators = [
                    TokenType.EQ, TokenType.NEQ, TokenType.CONTAINS
                ]


class FilterRegistry(ABC, Generic[T]):
    """
    Base class for filter registries.
    Subclass this to define filterable fields for specific object types.
    """

    def __init__(self):
        self._fields: Dict[str, FilterField[T]] = {}

    def register_field(self, field: FilterField[T]):
        """Register a filterable field"""
        self._fields[field.name.lower()] = field

    def get_field(self, name: str) -> Optional[FilterField[T]]:
        """Get a field by name (case-insensitive)"""
        return self._fields.get(name.lower())

    def get_all_fields(self) -> Dict[str, FilterField[T]]:
        """Get all registered fields"""
        return self._fields.copy()

    def evaluate_condition(self, obj: T, field_name: str, operator: TokenType, value: Any) -> bool:
        """Evaluate a single condition against an object"""
        field = self.get_field(field_name)
        if field is None:
            raise pylo.PyloEx(f"Unknown field '{field_name}'. Available fields: {', '.join(self._fields.keys())}")

        if operator not in field.supported_operators:
            raise pylo.PyloEx(
                f"Operator {operator.name} is not supported for field '{field_name}'. "
                f"Supported operators: {[op.name for op in field.supported_operators]}"
            )

        # Get the actual value from the object
        actual_value = field.getter(obj)

        # Convert value if needed based on field type
        converted_value = self._convert_value(value, field.value_type)

        # Perform the comparison
        return self._compare(actual_value, operator, converted_value, field.value_type)

    def _convert_value(self, value: Any, value_type: ValueType) -> Any:
        """Convert a parsed value to the appropriate type"""
        if value is None:
            return None

        try:
            if value_type == ValueType.STRING:
                return str(value)
            elif value_type == ValueType.INT:
                return int(value)
            elif value_type == ValueType.FLOAT:
                return float(value)
            elif value_type == ValueType.BOOLEAN:
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes')
                return bool(value)
            elif value_type == ValueType.DATE:
                if isinstance(value, date):
                    return value
                if isinstance(value, str):
                    return datetime.strptime(value, '%Y-%m-%d').date()
                raise pylo.PyloEx(f"Cannot convert {value!r} to date")
            elif value_type == ValueType.DATETIME:
                if isinstance(value, datetime):
                    return value
                if isinstance(value, str):
                    # Try multiple formats
                    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
                    raise pylo.PyloEx(f"Cannot parse datetime from '{value}'")
                raise pylo.PyloEx(f"Cannot convert {value!r} to datetime")
            elif value_type == ValueType.IP_ADDRESS:
                return str(value)
        except (ValueError, TypeError) as e:
            raise pylo.PyloEx(f"Cannot convert value '{value}' to {value_type.name}: {e}")

        return value

    def _compare(self, actual: Any, operator: TokenType, expected: Any, value_type: ValueType) -> bool:
        """Compare actual value with expected value using the operator"""
        # Handle None values
        if actual is None:
            if operator == TokenType.EQ:
                return expected is None or (isinstance(expected, str) and expected.lower() == 'none')
            elif operator == TokenType.NEQ:
                return expected is not None and not (isinstance(expected, str) and expected.lower() == 'none')
            return False

        # String comparisons (case-insensitive by default)
        if value_type == ValueType.STRING:
            actual_lower = actual.lower() if isinstance(actual, str) else str(actual).lower()
            expected_lower = expected.lower() if isinstance(expected, str) else str(expected).lower()

            if operator == TokenType.EQ:
                return actual_lower == expected_lower
            elif operator == TokenType.NEQ:
                return actual_lower != expected_lower
            elif operator == TokenType.CONTAINS:
                return expected_lower in actual_lower
            elif operator == TokenType.MATCHES:
                try:
                    return bool(re.search(expected, actual, re.IGNORECASE))
                except re.error as e:
                    raise pylo.PyloEx(f"Invalid regex pattern '{expected}': {e}")

        # Numeric comparisons
        elif value_type in (ValueType.INT, ValueType.FLOAT):
            if operator == TokenType.EQ:
                return actual == expected
            elif operator == TokenType.NEQ:
                return actual != expected
            elif operator == TokenType.LT:
                return actual < expected
            elif operator == TokenType.GT:
                return actual > expected
            elif operator == TokenType.LTE:
                return actual <= expected
            elif operator == TokenType.GTE:
                return actual >= expected

        # Boolean comparisons
        elif value_type == ValueType.BOOLEAN:
            if operator == TokenType.EQ:
                return actual == expected
            elif operator == TokenType.NEQ:
                return actual != expected

        # Date/DateTime comparisons
        elif value_type in (ValueType.DATE, ValueType.DATETIME):
            # Convert actual to comparable format if it's a datetime and expected is a date
            if isinstance(actual, datetime) and isinstance(expected, date) and not isinstance(expected, datetime):
                actual = actual.date()

            if operator == TokenType.EQ:
                return actual == expected
            elif operator == TokenType.NEQ:
                return actual != expected
            elif operator == TokenType.LT:
                return actual < expected
            elif operator == TokenType.GT:
                return actual > expected
            elif operator == TokenType.LTE:
                return actual <= expected
            elif operator == TokenType.GTE:
                return actual >= expected

        # IP Address comparisons
        elif value_type == ValueType.IP_ADDRESS:
            # Handle list of IPs (from interfaces)
            if isinstance(actual, (list, tuple)):
                if operator == TokenType.EQ:
                    return expected in actual
                elif operator == TokenType.NEQ:
                    return expected not in actual
                elif operator == TokenType.CONTAINS:
                    return expected in actual
            else:
                if operator == TokenType.EQ:
                    return actual == expected
                elif operator == TokenType.NEQ:
                    return actual != expected
                elif operator == TokenType.CONTAINS:
                    return expected in str(actual)

        return False


class FilterQuery(Generic[T]):
    """
    Main class for executing filter queries against objects.

    Usage:
        registry = WorkloadFilterRegistry()
        query = FilterQuery(registry)
        results = query.execute("name contains 'prod' and online == true", workloads)
    """

    def __init__(self, registry: FilterRegistry[T]):
        self.registry = registry
        self._ast: Optional[QueryNode] = None
        self._query_string: Optional[str] = None

    def parse(self, query_string: str) -> 'FilterQuery[T]':
        """Parse a query string into an AST"""
        self._query_string = query_string
        lexer = QueryLexer(query_string)
        tokens = lexer.tokenize()
        parser = QueryParser(tokens)
        self._ast = parser.parse()
        return self

    def evaluate(self, obj: T) -> bool:
        """Evaluate the parsed query against a single object"""
        if self._ast is None:
            raise pylo.PyloEx("No query has been parsed. Call parse() first.")
        return self._ast.evaluate(obj, self.registry)

    def execute(self, query_string: str, objects: List[T]) -> List[T]:
        """Parse a query and execute it against a list of objects"""
        self.parse(query_string)
        return [obj for obj in objects if self.evaluate(obj)]

    def execute_to_dict(self, query_string: str, objects: Dict[str, T]) -> Dict[str, T]:
        """Parse a query and execute it against a dict of objects"""
        self.parse(query_string)
        return {key: obj for key, obj in objects.items() if self.evaluate(obj)}


# =============================================================================
# Workload Filter Registry
# =============================================================================

class WorkloadFilterRegistry(FilterRegistry['pylo.Workload']):
    """
    Filter registry for Workload objects.
    Defines all filterable fields for workloads.

    If an Organization is provided, label fields will be dynamically registered
    for all label types configured in that PCE (not just the default role, app, env, loc).
    """

    def __init__(self, org: Optional['pylo.Organization'] = None):
        super().__init__()
        self._org = org
        self._register_fields()

    def _register_fields(self):
        """Register all filterable fields for Workloads"""

        # Basic identity fields
        self.register_field(FilterField(
            name='name',
            value_type=ValueType.STRING,
            getter=lambda w: w.get_name(),
            description='Workload name (forced_name if set, otherwise hostname)'
        ))

        self.register_field(FilterField(
            name='hostname',
            value_type=ValueType.STRING,
            getter=lambda w: w.hostname,
            description='Workload hostname'
        ))

        self.register_field(FilterField(
            name='forced_name',
            value_type=ValueType.STRING,
            getter=lambda w: w.forced_name,
            description='Manually set workload name'
        ))

        self.register_field(FilterField(
            name='href',
            value_type=ValueType.STRING,
            getter=lambda w: w.href,
            description='Workload HREF'
        ))

        self.register_field(FilterField(
            name='description',
            value_type=ValueType.STRING,
            getter=lambda w: w.description or '',
            description='Workload description'
        ))

        # Status fields
        self.register_field(FilterField(
            name='online',
            value_type=ValueType.BOOLEAN,
            getter=lambda w: w.online,
            description='Whether the workload is online'
        ))

        self.register_field(FilterField(
            name='managed',
            value_type=ValueType.BOOLEAN,
            getter=lambda w: not w.unmanaged,
            description='Whether the workload is managed (has VEN)'
        ))

        self.register_field(FilterField(
            name='unmanaged',
            value_type=ValueType.BOOLEAN,
            getter=lambda w: w.unmanaged,
            description='Whether the workload is unmanaged'
        ))

        self.register_field(FilterField(
            name='deleted',
            value_type=ValueType.BOOLEAN,
            getter=lambda w: w.deleted,
            description='Whether the workload is deleted'
        ))

        # OS fields
        self.register_field(FilterField(
            name='os_id',
            value_type=ValueType.STRING,
            getter=lambda w: w.os_id or '',
            description='Operating system identifier'
        ))

        self.register_field(FilterField(
            name='os_detail',
            value_type=ValueType.STRING,
            getter=lambda w: w.os_detail or '',
            description='Operating system details'
        ))

        # IP address field
        self.register_field(FilterField(
            name='ip_address',
            value_type=ValueType.IP_ADDRESS,
            getter=lambda w: [iface.ip for iface in w.interfaces if iface.ip],
            description='Workload IP addresses (checks all interfaces)'
        ))

        # Alias for ip_address
        self.register_field(FilterField(
            name='ip',
            value_type=ValueType.IP_ADDRESS,
            getter=lambda w: [iface.ip for iface in w.interfaces if iface.ip],
            description='Workload IP addresses (alias for ip_address)'
        ))

        # VEN/Agent fields
        self.register_field(FilterField(
            name='last_heartbeat',
            value_type=ValueType.DATETIME,
            getter=lambda w: w.ven_agent.get_last_heartbeat_date() if w.ven_agent else None,
            description='Last VEN heartbeat timestamp'
        ))

        # Alias with different naming
        self.register_field(FilterField(
            name='last_heartbeat_received',
            value_type=ValueType.DATETIME,
            getter=lambda w: w.ven_agent.get_last_heartbeat_date() if w.ven_agent else None,
            description='Last VEN heartbeat timestamp (alias)'
        ))

        self.register_field(FilterField(
            name='agent.status',
            value_type=ValueType.STRING,
            getter=lambda w: w.ven_agent.status if w.ven_agent else None,
            description='VEN agent status (active, stopped, suspended, uninstalled)'
        ))

        self.register_field(FilterField(
            name='agent.mode',
            value_type=ValueType.STRING,
            getter=lambda w: w.ven_agent.mode if w.ven_agent else None,
            description='VEN agent mode (idle, build, test, enforced)'
        ))

        self.register_field(FilterField(
            name='mode',
            value_type=ValueType.STRING,
            getter=lambda w: w.ven_agent.mode if w.ven_agent else None,
            description='VEN agent mode (alias for agent.mode)'
        ))

        self.register_field(FilterField(
            name='agent.version',
            value_type=ValueType.STRING,
            getter=lambda w: w.ven_agent.software_version.version_string if w.ven_agent and w.ven_agent.software_version else None,
            description='VEN software version'
        ))

        # Register label fields dynamically based on organization's label types
        self._register_label_fields()

        # Date fields
        self.register_field(FilterField(
            name='created_at',
            value_type=ValueType.DATETIME,
            getter=lambda w: w.created_at_datetime(),
            description='Workload creation timestamp'
        ))

        # Reference tracking
        self.register_field(FilterField(
            name='reference_count',
            value_type=ValueType.INT,
            getter=lambda w: w.count_references(),
            description='Number of references to this workload'
        ))

    def _register_label_fields(self):
        """
        Register label fields dynamically.
        If an Organization is provided, register fields for all label types configured in the PCE.
        Otherwise, register only the default label types (role, app, env, loc).
        """
        if self._org is not None:
            # Use the label types from the organization
            label_types = self._org.LabelStore.label_types
        else:
            # Fall back to default label types
            label_types = ['role', 'app', 'env', 'loc']

        for label_type in label_types:
            # Create a closure to capture the label_type value correctly
            def make_getter(lt: str):
                return lambda w: w.get_label(lt).name if w.get_label(lt) else None

            getter = make_getter(label_type)

            # Register with 'label.' prefix
            self.register_field(FilterField(
                name=f'label.{label_type}',
                value_type=ValueType.STRING,
                getter=getter,
                description=f'{label_type.capitalize()} label name'
            ))

            # Register shorthand alias (just the label type name)
            self.register_field(FilterField(
                name=label_type,
                value_type=ValueType.STRING,
                getter=getter,
                description=f'{label_type.capitalize()} label name (alias for label.{label_type})'
            ))


# Registry cache: maps organization id (or None for default) to WorkloadFilterRegistry
_workload_filter_registry_cache: Dict[Optional[int], WorkloadFilterRegistry] = {}


def get_workload_filter_registry(org: Optional['pylo.Organization'] = None) -> WorkloadFilterRegistry:
    """
    Get a WorkloadFilterRegistry instance.

    If an Organization is provided, returns a registry with label fields for all
    label types configured in that PCE. Registries are cached per organization.

    :param org: Optional Organization to get label types from
    :return: WorkloadFilterRegistry instance
    """
    global _workload_filter_registry_cache

    # Use org id as cache key, or None for default registry
    cache_key = org.id if org is not None else None

    if cache_key not in _workload_filter_registry_cache:
        _workload_filter_registry_cache[cache_key] = WorkloadFilterRegistry(org)

    return _workload_filter_registry_cache[cache_key]
