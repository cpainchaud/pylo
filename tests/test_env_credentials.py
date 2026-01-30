#!/usr/bin/env python3
"""
Test script for environment variable credential loading functionality
"""
import os
import sys

# Test 1: Missing required environment variables
print("Test 1: Missing required environment variables")
try:
    import illumio_pylo as pylo
    creds = pylo.get_credentials_from_file('ENV', fail_with_an_exception=True)
    print("  FAIL: Should have raised an exception for missing env vars")
except pylo.PyloEx as e:
    if "Missing required environment variables" in str(e):
        print("  PASS: Correctly detected missing env vars")
        print(f"  Error message: {e}")
    else:
        print(f"  FAIL: Unexpected error: {e}")
except Exception as e:
    print(f"  FAIL: Unexpected exception type: {type(e).__name__}: {e}")

# Test 2: Valid environment variables (non-illum.io domain)
print("\nTest 2: Valid environment variables (non-illum.io domain)")
os.environ['PYLO_FQDN'] = 'test.example.com'
os.environ['PYLO_API_USER'] = 'api_user_test'
os.environ['PYLO_API_KEY'] = 'test_api_key_12345'
try:
    import illumio_pylo as pylo
    creds = pylo.get_credentials_from_file('ENV')
    print("  PASS: Credentials loaded from environment")
    print(f"  Name: {creds.name}")
    print(f"  FQDN: {creds.fqdn}")
    print(f"  Port: {creds.port} (expected: 8443)")
    print(f"  API User: {creds.api_user}")
    print(f"  Org ID: {creds.org_id} (expected: 1)")
    print(f"  Verify SSL: {creds.verify_ssl} (expected: True)")
    print(f"  Originating File: {creds.originating_file} (expected: 'environment')")

    # Verify defaults
    assert creds.name == 'ENV', "Name should be 'ENV'"
    assert creds.fqdn == 'test.example.com', "FQDN mismatch"
    assert creds.port == 8443, "Default port should be 8443"
    assert creds.org_id == 1, "Default org_id should be 1"
    assert creds.verify_ssl == True, "Default verify_ssl should be True"
    assert creds.originating_file == 'environment', "Originating file should be 'environment'"
    print("  All assertions passed!")
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {e}")

# Test 3: illum.io domain without ORG_ID (should fail)
print("\nTest 3: illum.io domain without ORG_ID (should fail)")
os.environ['PYLO_FQDN'] = 'test.illum.io'
os.environ.pop('PYLO_ORG_ID', None)
try:
    import illumio_pylo as pylo
    creds = pylo.get_credentials_from_file('ENV')
    print("  FAIL: Should have raised an exception for missing ORG_ID on illum.io domain")
except pylo.PyloEx as e:
    if "PYLO_ORG_ID is required for illum.io domains" in str(e):
        print("  PASS: Correctly detected missing ORG_ID for illum.io domain")
        print(f"  Error message: {e}")
    else:
        print(f"  FAIL: Unexpected error: {e}")
except Exception as e:
    print(f"  FAIL: Unexpected exception type: {type(e).__name__}: {e}")

# Test 4: illum.io domain with ORG_ID
print("\nTest 4: illum.io domain with ORG_ID")
os.environ['PYLO_FQDN'] = 'test.illum.io'
os.environ['PYLO_ORG_ID'] = '5'
try:
    import illumio_pylo as pylo
    creds = pylo.get_credentials_from_file('ENV')
    print("  PASS: Credentials loaded for illum.io domain")
    print(f"  FQDN: {creds.fqdn}")
    print(f"  Port: {creds.port} (expected: 443)")
    print(f"  Org ID: {creds.org_id} (expected: 5)")

    assert creds.port == 443, "Default port for illum.io should be 443"
    assert creds.org_id == 5, "Org ID should be 5"
    print("  All assertions passed!")
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {e}")

# Test 5: Custom port
print("\nTest 5: Custom port")
os.environ['PYLO_FQDN'] = 'test.example.com'
os.environ['PYLO_PORT'] = '9443'
os.environ.pop('PYLO_ORG_ID', None)
try:
    import illumio_pylo as pylo
    creds = pylo.get_credentials_from_file('ENV')
    print("  PASS: Credentials loaded with custom port")
    print(f"  Port: {creds.port} (expected: 9443)")
    assert creds.port == 9443, "Port should be 9443"
    print("  All assertions passed!")
except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {e}")

# Test 6: Invalid port
print("\nTest 6: Invalid port")
os.environ['PYLO_PORT'] = 'invalid'
try:
    import illumio_pylo as pylo
    creds = pylo.get_credentials_from_file('ENV')
    print("  FAIL: Should have raised an exception for invalid port")
except pylo.PyloEx as e:
    if "Invalid PYLO_PORT value" in str(e):
        print("  PASS: Correctly detected invalid port")
        print(f"  Error message: {e}")
    else:
        print(f"  FAIL: Unexpected error: {e}")
except Exception as e:
    print(f"  FAIL: Unexpected exception type: {type(e).__name__}: {e}")

# Test 7: VERIFY_SSL variations
print("\nTest 7: VERIFY_SSL variations")
os.environ.pop('PYLO_PORT', None)
test_values = [
    ('false', False),
    ('0', False),
    ('no', False),
    ('n', False),
    ('true', True),
    ('1', True),
    ('yes', True),
    ('y', True),
    ('TRUE', True),
    ('FALSE', False),
]
for value, expected in test_values:
    os.environ['PYLO_VERIFY_SSL'] = value
    try:
        import illumio_pylo as pylo
        creds = pylo.get_credentials_from_file('ENV')
        if creds.verify_ssl == expected:
            print(f"  PASS: '{value}' -> {expected}")
        else:
            print(f"  FAIL: '{value}' expected {expected}, got {creds.verify_ssl}")
    except Exception as e:
        print(f"  FAIL: '{value}' raised exception: {e}")

# Test 8: Invalid VERIFY_SSL
print("\nTest 8: Invalid VERIFY_SSL")
os.environ['PYLO_VERIFY_SSL'] = 'invalid'
try:
    import illumio_pylo as pylo
    creds = pylo.get_credentials_from_file('ENV')
    print("  FAIL: Should have raised an exception for invalid VERIFY_SSL")
except pylo.PyloEx as e:
    if "Invalid PYLO_VERIFY_SSL value" in str(e):
        print("  PASS: Correctly detected invalid VERIFY_SSL")
        print(f"  Error message: {e}")
    else:
        print(f"  FAIL: Unexpected error: {e}")
except Exception as e:
    print(f"  FAIL: Unexpected exception type: {type(e).__name__}: {e}")

# Test 9: Case insensitive 'env' profile name
print("\nTest 9: Case insensitive 'env' profile name")
os.environ['PYLO_FQDN'] = 'test.example.com'
os.environ.pop('PYLO_VERIFY_SSL', None)
test_names = ['env', 'ENV', 'Env', 'EnV']
for name in test_names:
    try:
        import illumio_pylo as pylo
        creds = pylo.get_credentials_from_file(name)
        print(f"  PASS: '{name}' correctly loaded ENV profile")
    except Exception as e:
        print(f"  FAIL: '{name}' raised exception: {e}")

# Test 10: is_env_credentials_available helper
print("\nTest 10: is_env_credentials_available helper")
try:
    import illumio_pylo as pylo
    from illumio_pylo.API.CredentialsManager import is_env_credentials_available

    # All required vars present
    os.environ['PYLO_FQDN'] = 'test.example.com'
    os.environ['PYLO_API_USER'] = 'api_user_test'
    os.environ['PYLO_API_KEY'] = 'test_api_key_12345'
    if is_env_credentials_available():
        print("  PASS: Correctly detected env credentials are available")
    else:
        print("  FAIL: Should have detected available env credentials")

    # Missing one var
    os.environ.pop('PYLO_API_KEY', None)
    if not is_env_credentials_available():
        print("  PASS: Correctly detected env credentials are NOT available")
    else:
        print("  FAIL: Should have detected missing env credentials")

except Exception as e:
    print(f"  FAIL: {type(e).__name__}: {e}")

print("\n=== Test Suite Complete ===")
