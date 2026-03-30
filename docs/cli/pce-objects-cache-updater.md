# PCE Objects Cache Updater Command

## Overview

The `pce-objects-cache-updater` command downloads and caches all PCE objects and settings to a local JSON file. This cached data can be used for offline analysis, faster subsequent operations, or as a backup of PCE configuration.

## Command Syntax

```bash
pylo pce-objects-cache-updater [OPTIONS]
```

## Purpose and Use Cases

### Primary Use Cases

1. **Offline Analysis**: Work with PCE data without constant API calls
2. **Performance Optimization**: Speed up repeated operations using cached data
3. **Backup and Documentation**: Create snapshots of PCE configuration
4. **Historical Comparison**: Track configuration changes over time
5. **Development and Testing**: Test scripts against cached data without PCE access

### When to Use

- Before performing bulk analysis operations
- When working with slow or rate-limited API connections
- To create configuration backups before major changes
- For offline policy analysis and reporting

## Output

### Cache File

The command creates a file named: `cache_<pce-fqdn>.json`

Example: `cache_pce.mycompany.com.json`

### File Contents

The JSON cache file includes:

| Section | Content |
|---------|---------|
| `generation_date` | ISO 8601 timestamp of when cache was created |
| `pce_version` | PCE software version at time of cache creation |
| `data` | Complete snapshot of all requested PCE objects |

### Objects Included

Depending on configuration, the cache may include:

- Workloads (managed and unmanaged)
- Labels (all types and dimensions)
- IP Lists
- Services
- Rulesets and Rules
- Virtual Services
- Security Settings
- Label Dimensions
- Pairing Profiles
- Enforcement Boundaries
- And more...

## How It Works

1. **Connect to PCE**: Uses configured credentials to connect
2. **Fetch Objects**: Downloads all requested object types via API
3. **Generate Metadata**: Captures PCE version and timestamp
4. **Serialize to JSON**: Converts objects to JSON format
5. **Write to File**: Saves cache file in current directory

## Options

This command uses global CLI options:

| Global Option | Description |
|---------------|-------------|
| `--profile` | Credential profile to use |
| `--use-cache` | Not applicable for this command (it creates the cache) |
| `--cache-file` | Not applicable for this command (output name is auto-generated) |

**Note**: Unlike other commands, this one has no specific command-line options. It generates the cache based on your PCE connection configuration.

## Examples

### Basic Cache Creation

```bash
pylo pce-objects-cache-updater
```

This creates a cache file in the current directory with all PCE objects.

### Using Specific Profile

```bash
pylo pce-objects-cache-updater --profile production
```

### Output Example

```
Connecting to PCE...
Fetching objects from PCE...
 - Workloads: 1,234
 - Labels: 156
 - IP Lists: 45
 - Services: 89
 - Rulesets: 23
 - Rules: 456

PCE objects and settings were saved to file 'cache_pce.mycompany.com.json' with a size of 15MB

```

## Using Cached Data

### With Other Commands

Most pylo commands support the `--use-cache` option to read from cache files instead of connecting to the PCE:

```bash
# Create cache
pylo pce-objects-cache-updater --profile production

# Use cache with other commands
pylo workload-export --use-cache --cache-file cache_pce.mycompany.com.json
pylo rule-export --use-cache --cache-file cache_pce.mycompany.com.json
pylo iplist-analyzer --use-cache --cache-file cache_pce.mycompany.com.json
```

### Benefits of Using Cache

1. **Speed**: No API calls needed; instant data access
2. **Reliability**: Works offline or with intermittent connectivity
3. **Consistency**: Multiple analyses use the same snapshot
4. **API Rate Limiting**: Avoid hitting API rate limits during bulk operations

### Limitations When Using Cache

1. **Read-Only**: Cannot make changes to PCE using cached data
2. **Staleness**: Data becomes outdated as PCE changes
3. **No Real-Time**: Cannot see changes made after cache creation
4. **Disk Space**: Large environments can produce large cache files

## Cache File Structure

### Sample Structure

```json
{
  "generation_date": "2024-02-11T10:30:00.000Z",
  "pce_version": "22.2.0",
  "data": {
    "workloads": [ ... ],
    "labels": [ ... ],
    "ip_lists": [ ... ],
    "services": [ ... ],
    "rulesets": [ ... ],
    "rules": [ ... ],
    ...
  }
}
```

### Metadata Fields

- **generation_date**: When the cache was created (UTC timezone)
- **pce_version**: PCE software version (e.g., "22.2.0", "21.5.32")
- **data**: Nested object containing all PCE data structures

## Common Workflows

### Daily Backup Workflow

```bash
#!/bin/bash
# Daily PCE backup script

DATE=$(date +%Y%m%d)
BACKUP_DIR="/backups/pce"

# Create cache
pylo pce-objects-cache-updater --profile production

# Rename with date
mv cache_pce.mycompany.com.json "${BACKUP_DIR}/pce-backup-${DATE}.json"

# Keep only last 30 days
find "${BACKUP_DIR}" -name "pce-backup-*.json" -mtime +30 -delete
```

### Fast Analysis Workflow

```bash
# Step 1: Create cache once
pylo pce-objects-cache-updater

# Step 2: Run multiple analyses quickly using cache
pylo workload-export --use-cache --cache-file cache_pce.mycompany.com.json -rf xlsx
pylo rule-export --use-cache --cache-file cache_pce.mycompany.com.json -rf xlsx
pylo iplist-analyzer --use-cache --cache-file cache_pce.mycompany.com.json -rf xlsx
pylo traffic-export --use-cache --cache-file cache_pce.mycompany.com.json --timeframe-hours 24
```

### Change Tracking Workflow

```bash
# Before changes
pylo pce-objects-cache-updater
mv cache_pce.mycompany.com.json cache-before.json

# Make changes in PCE UI or via API

# After changes
pylo pce-objects-cache-updater
mv cache_pce.mycompany.com.json cache-after.json

# Compare (using jq or other JSON diff tools)
jq -S . cache-before.json > before-sorted.json
jq -S . cache-after.json > after-sorted.json
diff -u before-sorted.json after-sorted.json
```

### Offline Development

```bash
# On production system with PCE access
pylo pce-objects-cache-updater --profile production
scp cache_pce.mycompany.com.json dev-laptop:/data/

# On development laptop (offline)
pylo workload-export --use-cache --cache-file /data/cache_pce.mycompany.com.json
# Develop and test scripts using cached data
```

## Cache File Size Considerations

### Typical Sizes

| Environment Size | Approximate Cache Size |
|-----------------|----------------------|
| Small (< 100 workloads) | 1-5 MB |
| Medium (100-1000 workloads) | 5-50 MB |
| Large (1000-10000 workloads) | 50-500 MB |
| Very Large (> 10000 workloads) | 500 MB - 2 GB+ |

### Factors Affecting Size

- Number of workloads
- Number of rules
- Historical traffic data (if included)
- Label complexity
- Number of IP lists and services

### Managing Large Caches

1. **Compression**: Compress cache files when storing long-term
   ```bash
   gzip cache_pce.mycompany.com.json
   ```

2. **Selective Caching**: Configure to cache only needed object types (if supported in future versions)

3. **Cleanup**: Delete old cache files regularly
   ```bash
   find . -name "cache_*.json" -mtime +7 -delete
   ```

## Performance Considerations

### Cache Creation Time

- **Small environments**: A few seconds
- **Medium environments**: 30 seconds - 2 minutes
- **Large environments**: 2-10 minutes
- **Very large environments**: 10+ minutes

### Factors Affecting Performance

- PCE API response time
- Network latency
- Number of objects in PCE
- PCE server load
- API rate limiting

## Troubleshooting

### Cache Creation Fails

**Problem**: Command fails to create cache

**Possible Causes:**
- Network connectivity issues
- Invalid credentials
- Insufficient API permissions
- PCE server overload

**Solutions:**
- Verify network connectivity: `ping pce.mycompany.com`
- Test credentials: `pylo cred-manager test --name <profile>`
- Check API user permissions in PCE
- Retry during off-peak hours
- Check PCE server status

### Cache File Too Large

**Problem**: Cache file is excessively large

**Solutions:**
- Expected for large environments
- Compress file: `gzip cache_*.json`
- Consider selective export commands instead of full cache
- Archive old caches to free space

### Out of Disk Space

**Problem**: Not enough disk space to write cache

**Solutions:**
- Free up disk space
- Write cache to different directory with more space:
  ```bash
  cd /path/with/more/space
  pylo pce-objects-cache-updater
  ```

### Corrupted Cache File

**Problem**: Cache file exists but cannot be read

**Solutions:**
- Verify JSON validity: `jq . cache_file.json > /dev/null`
- Delete and recreate: `rm cache_*.json && pylo pce-objects-cache-updater`
- Check file permissions
- Verify disk is not full (may have been truncated during write)

## Best Practices

1. **Regular Updates**: Recreate cache regularly (daily/weekly) to keep data fresh

2. **Date-Stamped Backups**: Rename cache files with dates for historical tracking

3. **Pre-Operation Caching**: Create cache before bulk operations to ensure consistency

4. **Version Control**: Consider storing cache files in version control for configuration history (if not too large)

5. **Automation**: Automate cache creation via cron or scheduled tasks

6. **Documentation**: Document when caches were created and for what purpose

7. **Cleanup**: Remove old caches to save disk space

## Related Commands

- All read-only commands support `--use-cache` option
- `workload-export` - Use cache for fast workload exports
- `rule-export` - Use cache for fast rule exports
- `iplist-analyzer` - Use cache for fast IP list analysis
- `traffic-export` - Use cache for metadata (workloads, labels)

## Security Considerations

1. **Sensitive Data**: Cache files contain complete PCE configuration including:
   - Workload information
   - IP addresses
   - Security policy rules
   - Network topology

2. **File Permissions**: Protect cache files with appropriate permissions:
   ```bash
   chmod 600 cache_*.json
   ```

3. **Storage Location**: Store cache files in secure locations

4. **Retention**: Don't keep cache files longer than necessary

5. **Sharing**: Sanitize cache files before sharing externally
