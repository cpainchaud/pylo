"""
Shared test fixtures and mock classes for pylo testing.

This module provides reusable mock implementations of core pylo classes
that properly inherit from the real classes, ensuring type safety and
consistent behavior across test suites.

Usage:
    from tests.test_fixtures import MockWorkload, MockLabel, MockOrganization

Example:
    org = MockOrganization(label_types=['role', 'app', 'env', 'loc'])
    label = MockLabel('WebServer', 'role')
    workload = MockWorkload(
        name='test-server',
        hostname='test.example.com',
        labels={'role': label}
    )
"""
import os
import sys

# Ensure project root is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import illumio_pylo as pylo


# ============================================================================
# Mock Classes - Inheriting from real pylo classes
# ============================================================================

class MockLabel(pylo.Label):
    """
    Mock Label for testing - inherits from real Label class.

    Provides a simplified way to create Label instances for testing without
    requiring a full LabelStore setup.

    Args:
        name: The name of the label
        label_type: The type/dimension of the label (e.g., 'role', 'app', 'env', 'loc')

    Example:
        label = MockLabel('Production', 'env')
        assert label.name == 'Production'
        assert label.type == 'env'
    """
    def __init__(self, name: str, label_type: str):
        # Create a minimal mock LabelStore
        mock_store = type('MockLabelStore', (), {'owner': None})()
        # Initialize parent with required positional args: name, href, label_type, owner
        super().__init__(name, f'/labels/{name}', label_type, mock_store)


class MockVENAgent(pylo.VENAgent):
    """
    Mock VEN Agent for testing - inherits from real VENAgent class.

    Allows control over agent properties like heartbeat dates and sync state
    for testing workload behavior.

    Args:
        last_heartbeat: Optional datetime of last heartbeat
        policy_applied_at: Optional datetime of last policy application
        sync_state: Security policy sync state (default: 'synced')
        href: The agent's href (default: '/agents/test')

    Example:
        from datetime import datetime
        agent = MockVENAgent(
            last_heartbeat=datetime(2024, 1, 1, 12, 0, 0),
            sync_state='synced'
        )
    """
    def __init__(self, last_heartbeat=None, policy_applied_at=None,
                 sync_state='synced', href='/agents/test'):
        # Create minimal mock AgentStore
        mock_store = type('MockAgentStore', (), {'owner': None})()
        # Initialize parent
        super().__init__(href=href, owner=mock_store)
        self._last_heartbeat = last_heartbeat
        self._policy_applied_at = policy_applied_at
        self._sync_state = sync_state

    def get_last_heartbeat_date(self):
        return self._last_heartbeat

    def get_status_security_policy_applied_at(self):
        return self._policy_applied_at

    def get_status_security_policy_sync_state(self):
        return self._sync_state


class MockInterface(pylo.WorkloadInterface):
    """
    Mock Workload Interface for testing - inherits from real WorkloadInterface class.

    Simplified network interface creation for workload testing.

    Args:
        ip: The IP address of the interface
        owner: Optional workload owner (usually not needed in tests)

    Example:
        interface = MockInterface('192.168.1.100')
        assert interface.ip == '192.168.1.100'
        assert interface.name == 'eth0'
    """
    def __init__(self, ip: str, owner=None):
        # Initialize parent with minimal args
        super().__init__(
            owner=owner,
            name='eth0',
            ip=ip,
            network='',
            gateway='',
            ignored=False
        )


class MockOrganization(pylo.Organization):
    """
    Mock Organization for testing - inherits from real Organization class.

    Provides a full Organization instance with configurable label dimensions
    for testing label-related functionality.

    Args:
        label_types: Optional list of label type keys (default: ['role', 'app', 'env', 'loc'])

    Example:
        # Standard label types
        org = MockOrganization()

        # Custom label types
        org = MockOrganization(label_types=['custom1', 'custom2'])
        assert 'custom1' in org.LabelStore.label_types
    """
    def __init__(self, label_types=None):
        # Initialize parent
        super().__init__(org_id=1)
        # Override label dimensions if provided
        if label_types:
            # Convert string keys to LabelDimension objects
            dimensions = []
            for label_type in label_types:
                dimension = pylo.LabelDimension(
                    key=label_type,
                    display_name=label_type.title(),
                    href=f'/orgs/1/label_dimensions/{label_type}'
                )
                dimensions.append(dimension)
            # Set dimensions directly on the LabelStore
            self.LabelStore._dimensions = dimensions
            # Clear caches
            self.LabelStore._label_types_cache = None
            self.LabelStore._label_types_as_set_cache = None


class MockWorkload(pylo.Workload):
    """
    Mock Workload for testing - inherits from real Workload class.

    Provides a flexible way to create Workload instances with custom properties
    for testing without requiring a full PCE connection or WorkloadStore.

    Args:
        name: The workload name
        hostname: Optional hostname (defaults to name if not provided)
        online: Whether the workload is online (default: True)
        unmanaged: Whether the workload is unmanaged (default: False)
        interfaces: Optional list of MockInterface objects
        labels: Optional dict of label_type -> MockLabel
        ven_agent: Optional MockVENAgent instance

    Example:
        workload = MockWorkload(
            name='web-server',
            hostname='web-server.example.com',
            online=True,
            interfaces=[MockInterface('192.168.1.100')],
            labels={
                'role': MockLabel('Web', 'role'),
                'app': MockLabel('MyApp', 'app')
            },
            ven_agent=MockVENAgent(sync_state='synced')
        )
    """
    def __init__(self, name: str, hostname: str = None,
                 online: bool = True, unmanaged: bool = False,
                 interfaces=None, labels=None, ven_agent=None):
        # Create minimal mock WorkloadStore
        mock_org = type('MockOrg', (), {
            'LabelStore': type('LS', (), {
                'label_types': ['role', 'app', 'env', 'loc']
            })()
        })()
        mock_store = type('MockWorkloadStore', (), {'owner': mock_org})()

        # Initialize parent
        super().__init__(name=name, href=f'/workloads/{name.lower()}', owner=mock_store)

        # Set properties
        self.forced_name = name
        self.hostname = hostname or name
        self.online = online
        self.unmanaged = unmanaged
        self.interfaces = interfaces or []
        self.ven_agent = ven_agent

        # Set labels using the parent class's set_label() method
        if labels:
            for label in labels.values():
                self.set_label(label)


# ============================================================================
# Utility Functions
# ============================================================================

def create_mock_workload_with_labels(name: str, label_dict: dict = None) -> MockWorkload:
    """
    Convenience function to create a workload with labels from a simple dict.

    Args:
        name: The workload name
        label_dict: Dict of label_type -> label_name (e.g., {'role': 'Web', 'app': 'MyApp'})

    Returns:
        MockWorkload with the specified labels

    Example:
        workload = create_mock_workload_with_labels(
            'web-server',
            {'role': 'Web', 'env': 'Production'}
        )
    """
    labels = {}
    if label_dict:
        for label_type, label_name in label_dict.items():
            labels[label_type] = MockLabel(label_name, label_type)

    return MockWorkload(name=name, labels=labels)


def create_mock_organization_with_standard_labels() -> MockOrganization:
    """
    Convenience function to create an organization with standard label types.

    Returns:
        MockOrganization with role, app, env, loc label types

    Example:
        org = create_mock_organization_with_standard_labels()
        assert 'role' in org.LabelStore.label_types
    """
    return MockOrganization(label_types=['role', 'app', 'env', 'loc'])
