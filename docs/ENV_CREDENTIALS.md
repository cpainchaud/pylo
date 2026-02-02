# Environment Variable Credentials

## Overview

The `CredentialProfile.from_environment_variables()` feature allows you to provide Illumio PCE credentials via environment variables instead of storing them in credential files on disk. This is particularly useful for:

- CI/CD pipelines
- Docker containers
- Security-conscious environments where credentials shouldn't be written to disk
- Temporary credential usage

## Usage

To use environment variable credentials, simply specify the profile name `'ENV'` (case-insensitive) when loading credentials:

```python
import illumio_pylo as pylo

# Load credentials from environment variables
credentials = pylo.get_credentials_from_file('ENV')

# Or use with APIConnector
connector = pylo.APIConnector.create_from_credentials_in_file('ENV')

# Or use with Organization
org = pylo.Organization.get_from_api_using_credential_file('ENV')
```

## Required Environment Variables

- **`PYLO_FQDN`**: Fully qualified domain name of the PCE (e.g., `pce.example.com`)
- **`PYLO_API_USER`**: API username
- **`PYLO_API_KEY`**: API key (can be encrypted with `$encrypted$:` prefix)

## Optional Environment Variables

- **`PYLO_PORT`**: Port number
  - Default: `8443` for standard PCE
  - Default: `443` for illum.io hosted domains (SaaS)
  - Valid range: 1-65535

- **`PYLO_ORG_ID`**: Organization ID
  - Default: `1` for standard PCE
  - **Required** for illum.io hosted domains (no default)
  - Must be a positive integer

- **`PYLO_VERIFY_SSL`**: Verify SSL certificate
  - Default: `true`
  - Accepts: `true`, `false`, `1`, `0`, `yes`, `no`, `y`, `n` (case-insensitive)

## Examples

### Basic Example (Standard PCE)

```bash
# Set environment variables
export PYLO_FQDN="pce.example.com"
export PYLO_API_USER="api_12345"
export PYLO_API_KEY="your_api_key_here"

# Run your Python script
python your_script.py
```

```python
import illumio_pylo as pylo

# Load from environment
org = pylo.Organization.get_from_api_using_credential_file('ENV')
print(f"Connected to {org.connector.fqdn}")
```

### Illumio SaaS Example

```bash
# For illum.io domains, ORG_ID is required
export PYLO_FQDN="mycompany.illum.io"
export PYLO_API_USER="api_12345"
export PYLO_API_KEY="your_api_key_here"
export PYLO_ORG_ID="5"

# Port defaults to 443 for illum.io domains
python your_script.py
```

### Custom Configuration

```bash
# Custom port and disable SSL verification
export PYLO_FQDN="pce.internal.local"
export PYLO_API_USER="api_12345"
export PYLO_API_KEY="your_api_key_here"
export PYLO_PORT="9443"
export PYLO_ORG_ID="2"
export PYLO_VERIFY_SSL="false"

python your_script.py
```

### Docker Container Example

```bash
docker run -e PYLO_FQDN="pce.example.com" \
           -e PYLO_API_USER="api_12345" \
           -e PYLO_API_KEY="your_api_key" \
           your-container:latest
```

### Encrypted API Key

If you have an encrypted API key (generated with the `cred-manager` tool), you can use it directly:

```bash
export PYLO_FQDN="pce.example.com"
export PYLO_API_USER="api_12345"
export PYLO_API_KEY='$encrypted$:ssh-ChaCha20Poly1305:...'

python your_script.py
```

The API key will be automatically decrypted using your SSH agent.

## Checking Availability

You can check if required environment variables are set before attempting to load:

```python
from illumio_pylo.API.CredentialsManager import is_env_credentials_available

if is_env_credentials_available():
    print("Environment credentials are available")
    credentials = pylo.get_credentials_from_file('ENV')
else:
    print("Missing required environment variables")
    # Fall back to file-based credentials
    credentials = pylo.get_credentials_from_file('default')
```

## Validation and Error Handling

The implementation includes comprehensive validation:

- **Missing required variables**: Clear error message listing which variables are missing
- **Invalid port**: Must be a valid integer between 1-65535
- **Invalid org_id**: Must be a positive integer
- **Invalid verify_ssl**: Must be a boolean-like value
- **illum.io domains**: Automatically defaults port to 443 and requires ORG_ID

Example error messages:

```
Missing required environment variables for ENV profile: PYLO_FQDN, PYLO_API_KEY.
Required: PYLO_FQDN, PYLO_API_USER, PYLO_API_KEY.
Optional: PYLO_PORT, PYLO_ORG_ID, PYLO_VERIFY_SSL

Invalid PYLO_PORT value 'abc': must be a valid port number (1-65535)

PYLO_ORG_ID is required for illum.io domains (no default available)

Invalid PYLO_VERIFY_SSL value 'maybe': must be true/false/1/0/yes/no/y/n (case-insensitive)
```

## Security Considerations

### Advantages
- Credentials never written to disk
- Easier to rotate in CI/CD environments
- No risk of accidentally committing credentials to version control
- Better integration with secrets management tools (AWS Secrets Manager, HashiCorp Vault, etc.)

### Best Practices
1. **Never log environment variables** - Be careful not to print or log the actual API key
2. **Use encrypted keys when possible** - Leverage SSH agent encryption for additional security
3. **Rotate credentials regularly** - Environment-based approach makes rotation easier
4. **Use secrets management** - In production, use proper secrets management tools to inject environment variables
5. **Limit scope** - Set environment variables only for the specific process that needs them

### Example with AWS Secrets Manager

```python
import boto3
import json
import os
import illumio_pylo as pylo

# Fetch credentials from AWS Secrets Manager
client = boto3.client('secretsmanager')
secret = client.get_secret_value(SecretId='pylo/pce/credentials')
creds = json.loads(secret['SecretString'])

# Set environment variables
os.environ['PYLO_FQDN'] = creds['fqdn']
os.environ['PYLO_API_USER'] = creds['api_user']
os.environ['PYLO_API_KEY'] = creds['api_key']

# Use ENV profile
org = pylo.Organization.get_from_api_using_credential_file('ENV')
```

## Notes

- The profile name is always `'ENV'` (case-insensitive: `'env'`, `'Env'`, `'ENV'` all work)
- The `originating_file` attribute is set to `'environment'` for env-based profiles
- Environment credentials are **never** included in `get_all_credentials()` - they must be explicitly requested
- Environment credentials don't require any credential files to exist on disk
