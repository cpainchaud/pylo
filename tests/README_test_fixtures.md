# Test Fixtures - Shared Mock Classes

## Overview

The `test_fixtures.py` module provides reusable mock implementations of core pylo classes for testing. All mocks properly inherit from real pylo classes, ensuring type safety and consistent behavior across test suites.

## Available Mock Classes

### MockLabel
Creates Label instances without requiring a full LabelStore setup.

```python
from test_fixtures import MockLabel

label = MockLabel('Production', 'env')
assert label.name == 'Production'
assert label.type == 'env'
```

**Parameters:**
- `name` (str): The name of the label
- `label_type` (str): The type/dimension ('role', 'app', 'env', 'loc', or custom)

---

### MockVENAgent
Creates VEN Agent instances with controllable properties for testing.

```python
from test_fixtures import MockVENAgent
from datetime import datetime

agent = MockVENAgent(
    last_heartbeat=datetime(2024, 1, 1, 12, 0, 0),
    policy_applied_at=datetime(2024, 1, 1, 11, 0, 0),
    sync_state='synced',
    href='/agents/test-agent'
)

assert agent.get_last_heartbeat_date() == datetime(2024, 1, 1, 12, 0, 0)
assert agent.get_status_security_policy_sync_state() == 'synced'
```

**Parameters:**
- `last_heartbeat` (datetime, optional): Last heartbeat timestamp
- `policy_applied_at` (datetime, optional): Last policy application timestamp
- `sync_state` (str): Security policy sync state (default: 'synced')
- `href` (str): The agent's href (default: '/agents/test')

---

### MockInterface
Creates network interface instances for workloads.

```python
from test_fixtures import MockInterface

interface = MockInterface('192.168.1.100')
assert interface.ip == '192.168.1.100'
assert interface.name == 'eth0'
```

**Parameters:**
- `ip` (str): The IP address of the interface
- `owner` (optional): Workload owner (rarely needed in tests)

---

### MockOrganization
Creates Organization instances with configurable label dimensions.

```python
from test_fixtures import MockOrganization

# Standard label types
org = MockOrganization()
assert 'role' in org.LabelStore.label_types

# Custom label types
org = MockOrganization(label_types=['custom1', 'custom2'])
assert 'custom1' in org.LabelStore.label_types
assert 'custom2' in org.LabelStore.label_types
```

**Parameters:**
- `label_types` (list[str], optional): List of label type keys (default: ['role', 'app', 'env', 'loc'])

---

### MockWorkload
Creates Workload instances with full control over properties.

```python
from test_fixtures import MockWorkload, MockLabel, MockInterface, MockVENAgent
from datetime import datetime

workload = MockWorkload(
    name='web-server',
    hostname='web-server.example.com',
    online=True,
    unmanaged=False,
    interfaces=[
        MockInterface('192.168.1.100'),
        MockInterface('10.0.0.50')
    ],
    labels={
        'role': MockLabel('Web', 'role'),
        'app': MockLabel('MyApp', 'app'),
        'env': MockLabel('Production', 'env')
    },
    ven_agent=MockVENAgent(
        last_heartbeat=datetime.now(),
        sync_state='synced'
    )
)

assert workload.name == 'web-server'
assert workload.hostname == 'web-server.example.com'
assert workload.online is True
assert workload.get_label('role').name == 'Web'
assert len(workload.interfaces) == 2
```

**Parameters:**
- `name` (str): The workload name
- `hostname` (str, optional): Hostname (defaults to name)
- `online` (bool): Online status (default: True)
- `unmanaged` (bool): Unmanaged status (default: False)
- `interfaces` (list[MockInterface], optional): Network interfaces
- `labels` (dict[str, MockLabel], optional): Labels by type
- `ven_agent` (MockVENAgent, optional): VEN agent instance

---

## Utility Functions

### create_mock_workload_with_labels()
Convenience function to create workloads with labels from a simple dict.

```python
from test_fixtures import create_mock_workload_with_labels

workload = create_mock_workload_with_labels(
    'web-server',
    {'role': 'Web', 'env': 'Production', 'app': 'MyApp'}
)

assert workload.get_label('role').name == 'Web'
assert workload.get_label('env').name == 'Production'
```

### create_mock_organization_with_standard_labels()
Creates an organization with standard label types.

```python
from test_fixtures import create_mock_organization_with_standard_labels

org = create_mock_organization_with_standard_labels()
assert 'role' in org.LabelStore.label_types
assert 'app' in org.LabelStore.label_types
assert 'env' in org.LabelStore.label_types
assert 'loc' in org.LabelStore.label_types
```

---

## Design Philosophy

### Proper Inheritance
All mock classes inherit from real pylo classes:
- ✅ Type-safe: No type warnings in IDEs
- ✅ `isinstance()` checks work correctly
- ✅ All parent class methods available
- ✅ Automatic updates when parent classes change

### Minimal Dependencies
Mocks create minimal internal dependencies:
```python
# Creates inline mock store objects
mock_store = type('MockStore', (), {'owner': None})()
```

This avoids circular dependencies and complex setup while maintaining compatibility.

### Test Isolation
Each mock instance is independent:
- No shared global state
- No side effects on other tests
- Can be created/destroyed freely

---

## Usage Examples

### Basic Test Pattern
```python
from test_fixtures import MockWorkload, MockLabel, MockOrganization

def test_workload_labels():
    org = MockOrganization(label_types=['role', 'app'])

    workload = MockWorkload(
        name='test-server',
        labels={
            'role': MockLabel('Web', 'role'),
            'app': MockLabel('MyApp', 'app')
        }
    )

    assert workload.get_label('role').name == 'Web'
    assert workload.get_label('app').name == 'MyApp'
```

### Complex Test Scenario
```python
from test_fixtures import (
    MockWorkload, MockLabel, MockInterface,
    MockVENAgent, MockOrganization
)
from datetime import datetime

def test_complex_workload_scenario():
    org = MockOrganization(label_types=['role', 'app', 'env', 'loc'])

    # Create VEN agent
    agent = MockVENAgent(
        last_heartbeat=datetime(2024, 3, 15, 10, 0, 0),
        sync_state='synced'
    )

    # Create workload with full configuration
    workload = MockWorkload(
        name='web-prod-01',
        hostname='web-prod-01.company.com',
        online=True,
        unmanaged=False,
        interfaces=[
            MockInterface('192.168.1.100'),
            MockInterface('10.0.0.50')
        ],
        labels={
            'role': MockLabel('Web', 'role'),
            'app': MockLabel('CRM', 'app'),
            'env': MockLabel('Production', 'env'),
            'loc': MockLabel('US-East', 'loc')
        },
        ven_agent=agent
    )

    # Test various properties
    assert workload.online is True
    assert not workload.unmanaged
    assert len(workload.interfaces) == 2
    assert workload.ven_agent is not None
    assert workload.get_status_string() == 'synced'
```

---

## Migration Guide

### Before (Old Pattern)
```python
# Each test file had its own mock classes
class MockWorkload:
    def __init__(self, name: str):
        self.name = name
        # ... lots of boilerplate ...
```

### After (New Pattern)
```python
# Import from shared fixtures
from test_fixtures import MockWorkload

workload = MockWorkload(name='test')
```

**Benefits:**
- ✅ Less code duplication
- ✅ Consistent behavior across tests
- ✅ Easier maintenance
- ✅ Proper type checking

---

## Testing the Fixtures

The fixtures themselves are tested in `test_workload_export.py`. To verify they work:

```bash
python tests/test_workload_export.py
```

All tests should pass, confirming the fixtures work correctly.

---

## Contributing

When adding new mock classes:

1. **Inherit from real classes** - Don't create standalone mocks
2. **Document parameters** - Add docstrings with examples
3. **Keep it simple** - Only override what's necessary for testing
4. **Test it** - Ensure the mock works in actual tests

Example template:
```python
class MockNewClass(pylo.RealClass):
    """
    Mock for RealClass - brief description.

    Args:
        param1: Description of param1
        param2: Description of param2

    Example:
        mock = MockNewClass(param1='value')
        assert mock.property == 'value'
    """
    def __init__(self, param1: str, param2: int = 0):
        # Create minimal dependencies
        mock_dep = type('MockDep', (), {'attr': None})()
        # Initialize parent
        super().__init__(param1, owner=mock_dep)
        # Set additional properties
        self.param2 = param2
```

---

## Related Files

- `test_fixtures.py` - The fixture implementation
- `test_workload_export.py` - Example usage
- `conftest.py` - Pytest configuration
- `README_workload_export_tests.md` - Test documentation

---

## Questions?

If you have questions about using these fixtures or need to add new ones, refer to:
- The docstrings in `test_fixtures.py`
- Usage examples in `test_workload_export.py`
- The parent class implementations in `illumio_pylo/`
