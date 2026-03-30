# Credential Manager Command

## Overview

The `cred-manager` command manages PCE API credentials stored in configuration files. It provides a comprehensive interface for creating, updating, testing, encrypting, and deleting credential profiles. The command also includes a web-based editor for managing credentials through a browser interface.

## Command Syntax

```bash
pylo cred-manager <SUBCOMMAND> [OPTIONS]
```

## Subcommands

### list

List all available credential profiles.

```bash
pylo cred-manager list
```

**Output**: Displays a table with profile names, URLs, API users, and originating file locations.

---

### create

Create a new credential profile.

```bash
pylo cred-manager create [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--name` | string | Interactive | Name of the credential profile |
| `--fqdn` | string | Interactive | FQDN of the PCE (e.g., `pce1.mycompany.com`) |
| `--port` | integer | Interactive | Port of the PCE (e.g., `8443`) |
| `--org` | integer | Interactive | Organization ID |
| `--api-user` | string | Interactive | API user name |
| `--verify-ssl` | boolean | Interactive | Whether to verify SSL/TLS certificates |

**Notes:**
- If options are not provided via command line, the command will prompt interactively
- API key is always prompted interactively for security
- Supports optional API key encryption using SSH agent keys (RSA or Ed25519)
- Credentials can be saved in current working directory or user home directory

**Example:**
```bash
pylo cred-manager create --name prod-pce --fqdn pce.company.com --port 8443 --org 1
```

---

### update

Update an existing credential profile.

```bash
pylo cred-manager update [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--name` | string | Interactive | Name of the credential profile to update |
| `--fqdn` | string | Optional | New FQDN of the PCE |
| `--port` | integer | Optional | New port of the PCE |
| `--org` | integer | Optional | New organization ID |
| `--api-user` | string | Optional | New API user name |
| `--verify-ssl` | boolean | Optional | New SSL verification setting |

**Notes:**
- Only specified fields will be updated
- API key update is optional and prompted separately
- Supports encryption of the updated API key

---

### delete

Delete a credential profile.

```bash
pylo cred-manager delete [OPTIONS]
```

**Options:**

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--name` | - | string | Name of the credential profile to delete |
| `--yes` | `-y` | flag | Skip confirmation prompt |

**Example:**
```bash
pylo cred-manager delete --name old-pce --yes
```

---

### test

Test a credential profile by connecting to the PCE.

```bash
pylo cred-manager test [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--name` | string | Interactive | Name of the credential profile to test |

**Notes:**
- Performs a test API call to verify connectivity and authentication
- Automatically decrypts encrypted API keys if SSH agent is available

---

### encrypt

Encrypt an existing credential's API key using SSH agent keys.

```bash
pylo cred-manager encrypt [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--name` | string | Interactive | Name of the credential profile to encrypt |

**Requirements:**
- SSH agent must be running
- Supported key types: RSA or Ed25519
- ECDSA NIST-P curves are NOT supported

**Notes:**
- Encrypts API key using ChaCha20-Poly1305 with SSH agent signature
- Verifies encryption by immediately decrypting and comparing
- Updates the credential file in-place

---

### web-editor

Start a web-based credential editor.

```bash
pylo cred-manager web-editor [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--host` | string | `127.0.0.1` | Host to bind the web server to |
| `--port` | integer | `5000` | Port to bind the web server to |

**Features:**
- Full CRUD operations for credentials via web UI
- Test credentials directly from the browser
- Encrypt/decrypt API keys using SSH agent
- View all credential profiles in a table
- Secure by default (binds to localhost only)

**Example:**
```bash
pylo cred-manager web-editor --host 127.0.0.1 --port 8080
```

**Web UI Endpoints:**
- `http://127.0.0.1:5000/` - Main credential management interface
- API endpoints available at `/api/credentials`, `/api/ssh-keys`, etc.

---

## Credential File Locations

Credentials are stored in JSON format in the following locations:

1. **Current working directory**: `.pylo_credentials.json`
2. **User home directory**: `~/.pylo_credentials.json`

The tool searches both locations and merges credentials from all files.

## API Key Encryption

### Overview

API keys can be encrypted using SSH agent keys for enhanced security. The encryption uses:
- **Algorithm**: ChaCha20-Poly1305 (authenticated encryption)
- **Key derivation**: SSH agent signature of random nonce
- **Supported keys**: RSA, Ed25519

### Benefits

- API keys are not stored in plain text
- Encryption tied to specific SSH key in agent
- Requires SSH agent to be running for decryption
- Can leverage hardware security keys (YubiKey, etc.) via SSH agent

### Workflow

1. Generate random nonce
2. Request SSH agent to sign the nonce
3. Derive encryption key from signature using HKDF
4. Encrypt API key using ChaCha20-Poly1305
5. Store encrypted payload with nonce and key fingerprint

## Examples

### Create a New Credential Profile

```bash
# Interactive mode
pylo cred-manager create

# With command-line arguments
pylo cred-manager create \
  --name production \
  --fqdn pce.company.com \
  --port 443 \
  --org 1 \
  --api-user api_12345
```

### List All Credentials

```bash
pylo cred-manager list
```

### Test a Credential

```bash
pylo cred-manager test --name production
```

### Encrypt an Existing Credential

```bash
pylo cred-manager encrypt --name production
```

### Update a Credential

```bash
pylo cred-manager update --name production --port 8443
```

### Delete a Credential

```bash
pylo cred-manager delete --name old-test --yes
```

### Start Web Editor

```bash
pylo cred-manager web-editor --port 8080
```

## Security Considerations

1. **Plain Text Storage**: Without encryption, API keys are stored in plain text. Use file system permissions to protect credential files.

2. **SSH Agent**: When using encryption, ensure your SSH agent is configured securely:
   - Use a PIN/passphrase for SSH keys when possible
   - Consider hardware security keys (YubiKey, etc.)
   - Be aware of SSH agent forwarding risks

3. **Web Editor**: The web editor binds to localhost by default. Only change the host if you understand the security implications.

4. **Credential Files**: Credential files should have restrictive permissions (e.g., `600` on Unix-like systems).

## Troubleshooting

### Encryption Not Available

**Problem**: "Encryption is not available" message

**Solutions:**
- Ensure SSH agent is running: `ssh-add -l`
- Add a supported key type: `ssh-add ~/.ssh/id_rsa` or `ssh-add ~/.ssh/id_ed25519`
- ECDSA NIST-P curves are not supported; use RSA or Ed25519

### Cannot Find Profile

**Problem**: "Cannot find a profile named 'X'"

**Solutions:**
- List all profiles: `pylo cred-manager list`
- Check spelling of profile name (case-sensitive)
- Verify credential file exists in expected location

### Connection Test Fails

**Problem**: Test command fails to connect

**Solutions:**
- Verify network connectivity to PCE
- Check firewall rules
- Confirm PCE FQDN and port are correct
- Verify API credentials are valid
- Check SSL/TLS certificate settings

## Related Commands

- Use credentials with other commands via the `--profile` global option
- See credential in use with PCE connection commands
