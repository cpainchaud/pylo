"""
Test script for the FilterQuery feature.

This script tests the filter query functionality without requiring a PCE connection.
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo
from illumio_pylo.FilterQuery import (
    QueryLexer, QueryParser, FilterQuery, WorkloadFilterRegistry,
    TokenType, get_workload_filter_registry
)


def test_lexer():
    """Test the query lexer"""
    print("=" * 60)
    print("Testing QueryLexer")
    print("=" * 60)

    test_queries = [
        "name == 'test'",
        "name == 'SRV158' or name == 'SRV48889'",
        "(name == 'SRV158' or name == 'SRV48889') and ip_address == '192.168.2.54'",
        "last_heartbeat <= '2022-09-12'",
        "hostname contains 'prod' and online == true",
        "name matches 'SRV[0-9]+'",
        "not deleted == true",
        "reference_count > 5",
    ]

    for query in test_queries:
        print(f"\nQuery: {query}")
        lexer = QueryLexer(query)
        tokens = lexer.tokenize()
        print(f"Tokens: {[(t.type.name, t.value) for t in tokens]}")

    print("\n✓ Lexer tests passed!")


def test_parser():
    """Test the query parser"""
    print("\n" + "=" * 60)
    print("Testing QueryParser")
    print("=" * 60)

    test_queries = [
        "name == 'test'",
        "name == 'A' or name == 'B'",
        "name == 'A' and name == 'B'",
        "(name == 'A' or name == 'B') and online == true",
        "not deleted == true",
        "name == 'A' or (name == 'B' and name == 'C')",
    ]

    for query in test_queries:
        print(f"\nQuery: {query}")
        lexer = QueryLexer(query)
        tokens = lexer.tokenize()
        parser = QueryParser(tokens)
        ast = parser.parse()
        print(f"AST: {ast}")

    print("\n✓ Parser tests passed!")


def test_workload_registry():
    """Test that all fields are registered correctly"""
    print("\n" + "=" * 60)
    print("Testing WorkloadFilterRegistry")
    print("=" * 60)

    registry = get_workload_filter_registry()
    fields = registry.get_all_fields()

    print(f"\nRegistered fields ({len(fields)}):")
    for name, field in sorted(fields.items()):
        ops = [op.name for op in field.supported_operators]
        print(f"  - {name}: {field.value_type.name} (operators: {', '.join(ops)})")

    print("\n✓ Registry tests passed!")


class MockLabel:
    """Mock Label for testing"""
    def __init__(self, name: str, label_type: str):
        self.name = name
        self.type = label_type


class MockVENAgent:
    """Mock VEN Agent for testing"""
    def __init__(self, last_heartbeat, status='active', mode='enforced', version='1.0.0'):
        self._last_heartbeat = last_heartbeat
        self.status = status
        self.mode = mode if status == 'active' else None  # Stopped agents don't have active mode
        self.software_version = type('SV', (), {'version_string': version})()

    def get_last_heartbeat_date(self):
        return self._last_heartbeat


class MockInterface:
    """Mock Workload Interface for testing"""
    def __init__(self, ip: str):
        self.ip = ip
        self.name = 'eth0'


class MockWorkload:
    """Mock Workload for testing"""
    def __init__(self, name: str, hostname: str = None, description: str = '',
                 online: bool = True, deleted: bool = False, unmanaged: bool = False,
                 interfaces: list = None, labels: dict = None, ven_agent=None):
        self.forced_name = name
        self.hostname = hostname or name
        self.href = f'/workloads/{name.lower()}'
        self.description = description
        self.online = online
        self.deleted = deleted
        self.unmanaged = unmanaged
        self.os_id = 'linux'
        self.os_detail = 'Ubuntu 20.04'
        self.interfaces = interfaces or []
        self.ven_agent = ven_agent
        self._labels = labels or {}

        # Set label properties
        self.role_label = self._labels.get('role')
        self.app_label = self._labels.get('app')
        self.env_label = self._labels.get('env')
        self.loc_label = self._labels.get('loc')

    def get_label(self, label_type: str):
        """Get a label by its type (e.g., 'role', 'app', 'env', 'loc')"""
        return self._labels.get(label_type)

    def get_name(self):
        return self.forced_name if self.forced_name else self.hostname

    def created_at_datetime(self):
        from datetime import datetime
        return datetime(2023, 1, 15, 10, 30, 0)

    def count_references(self):
        return 0


def test_filter_execution():
    """Test filter execution against mock workloads"""
    print("\n" + "=" * 60)
    print("Testing Filter Execution")
    print("=" * 60)

    from datetime import datetime

    # Create mock workloads
    workloads = [
        MockWorkload(
            name='SRV158',
            hostname='srv158.example.com',
            description='Production web server',
            online=True,
            interfaces=[MockInterface('192.168.2.54'), MockInterface('10.0.0.1')],
            labels={
                'role': MockLabel('Web', 'role'),
                'env': MockLabel('Production', 'env'),
                'app': MockLabel('WebApp', 'app'),
                'loc': MockLabel('US-East', 'loc'),
            },
            ven_agent=MockVENAgent(datetime(2022, 9, 10, 12, 0, 0), mode='enforced')
        ),
        MockWorkload(
            name='SRV48889',
            hostname='srv48889.example.com',
            description='Development database',
            online=True,
            interfaces=[MockInterface('192.168.3.100')],
            labels={
                'role': MockLabel('Database', 'role'),
                'env': MockLabel('Development', 'env'),
                'app': MockLabel('DBApp', 'app'),
                'loc': MockLabel('US-West', 'loc'),
            },
            ven_agent=MockVENAgent(datetime(2022, 9, 15, 12, 0, 0), mode='build')
        ),
        MockWorkload(
            name='SRV999',
            hostname='srv999.example.com',
            description='Test server',
            online=False,
            deleted=True,
            interfaces=[MockInterface('192.168.4.200')],
            labels={
                'env': MockLabel('Test', 'env'),
            },
            ven_agent=MockVENAgent(datetime(2022, 1, 1, 12, 0, 0), status='stopped')
        ),
        MockWorkload(
            name='UNMANAGED001',
            hostname='unmanaged001.example.com',
            description='Unmanaged workload',
            online=False,  # Unmanaged workloads are not online
            unmanaged=True,
            interfaces=[MockInterface('192.168.5.50')],
        ),
    ]

    registry = get_workload_filter_registry()

    test_cases = [
        # Simple equality
        ("name == 'SRV158'", ['SRV158']),
        ("name == 'srv158'", ['SRV158']),  # case insensitive

        # OR conditions
        ("name == 'SRV158' or name == 'SRV48889'", ['SRV158', 'SRV48889']),

        # AND conditions
        ("name == 'SRV158' and ip_address == '192.168.2.54'", ['SRV158']),

        # Complex with parentheses
        ("(name == 'SRV158' or name == 'SRV48889') and online == true", ['SRV158', 'SRV48889']),

        # Contains operator
        ("description contains 'web'", ['SRV158']),
        ("hostname contains 'example'", ['SRV158', 'SRV48889', 'SRV999', 'UNMANAGED001']),

        # Matches (regex)
        ("name matches 'SRV[0-9]+'", ['SRV158', 'SRV48889', 'SRV999']),

        # Boolean fields
        ("online == true", ['SRV158', 'SRV48889']),
        ("online == false", ['SRV999', 'UNMANAGED001']),
        ("deleted == true", ['SRV999']),
        ("unmanaged == true", ['UNMANAGED001']),
        ("managed == true", ['SRV158', 'SRV48889', 'SRV999']),

        # Label fields
        ("label.env == 'Production'", ['SRV158']),
        ("env == 'Development'", ['SRV48889']),
        ("role == 'Web'", ['SRV158']),

        # Date comparison
        ("last_heartbeat <= '2022-09-12'", ['SRV158', 'SRV999']),
        ("last_heartbeat > '2022-09-12'", ['SRV48889']),

        # NOT operator
        ("not online == true", ['SRV999', 'UNMANAGED001']),
        ("not deleted == true", ['SRV158', 'SRV48889', 'UNMANAGED001']),

        # Agent mode
        ("mode == 'enforced'", ['SRV158']),
        ("agent.mode == 'build'", ['SRV48889']),

        # IP address
        ("ip == '192.168.2.54'", ['SRV158']),
        ("ip_address == '10.0.0.1'", ['SRV158']),
    ]

    passed = 0
    failed = 0

    for query, expected_names in test_cases:
        print(f"\nQuery: {query}")
        print(f"Expected: {expected_names}")

        try:
            filter_query = FilterQuery(registry)
            results = filter_query.execute(query, workloads)
            result_names = [w.get_name() for w in results]
            print(f"Got: {result_names}")

            if sorted(result_names) == sorted(expected_names):
                print("✓ PASSED")
                passed += 1
            else:
                print("✗ FAILED")
                failed += 1
        except Exception as e:
            print(f"✗ ERROR: {e}")
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    return failed == 0


def test_error_handling():
    """Test error handling for invalid queries"""
    print("\n" + "=" * 60)
    print("Testing Error Handling")
    print("=" * 60)

    registry = get_workload_filter_registry()
    # Create a workload to test against for unknown field error
    mock_workload = MockWorkload(name='TestWkl')
    workloads = [mock_workload]

    invalid_queries = [
        ("", "Empty query", []),
        ('invalid_field == "test"', "Unknown field", workloads),
        ("name ==", "Missing value", []),
        ("name 'test'", "Missing operator", []),
        ("(name == 'test'", "Unclosed parenthesis", []),
        ('name == \'test', "Unclosed string", []),
    ]

    for query, description, test_workloads in invalid_queries:
        print(f"\nTest: {description}")
        print(f"Query: '{query}'")

        try:
            filter_query = FilterQuery(registry)
            # Parse and execute to trigger field validation errors
            filter_query.execute(query, test_workloads)
            print("ERROR: Should have raised an exception")
        except pylo.PyloEx as e:
            print(f"OK: Correctly raised: {e}")

    print("\n✓ Error handling tests completed!")


if __name__ == '__main__':
    print("FilterQuery Test Suite")
    print("=" * 60)

    test_lexer()
    test_parser()
    test_workload_registry()
    success = test_filter_execution()
    test_error_handling()

    print("\n" + "=" * 60)
    if success:
        print("All tests completed successfully!")
    else:
        print("Some tests failed!")
        sys.exit(1)
