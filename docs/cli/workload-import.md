# Workload Import Command

## Overview

The `workload-import` command imports unmanaged workloads into the PCE from CSV or Excel files. It creates workloads with specified labels, IP addresses, and metadata, and can automatically create missing labels.

## Command Syntax

```bash
pylo workload-import --input-file FILE [OPTIONS]
```

## Required Arguments

| Argument | Short | Type | Description |
|----------|-------|------|-------------|
| `--input-file` | `-i` | string | Path to CSV or Excel input file |

## Options

### Input File Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--input-file-delimiter` | string | `,` | CSV field delimiter |
| `--label-type-header-prefix` | string | `label_` | Prefix for label column headers |
| `--ignore-missing-headers` | flag | false | Don't require label columns for all label types |

### Collision Handling

| Option | Type | Description |
|--------|------|-------------|
| `--ignore-hostname-collision` | flag | Skip entries if hostname already exists |
| `--ignore-ip-collision` | flag | Skip entries if IP already in use |
| `--ignore-all-sorts-collisions` | flag | Skip any entries with collisions |
| `--ignore-empty-ip-entries` | flag | Skip entries with no IP address |

### Execution Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--proceed-with-creation` | `-p` | flag | Actually create workloads (otherwise dry-run) |
| `--no-confirmation-required` | `-n` | flag | Skip confirmation prompt |
| `--batch-size` | - | integer | Workloads per API call (default: 500) |

### Output Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--report-format` | `-rf` | choice | `csv` | Report format (csv, xlsx, json) |
| `--output-dir` | `-o` | string | `output` | Output directory |

## Input File Format

### Required Columns

| Column | Type | Optional | Description |
|--------|------|----------|-------------|
| `name` | string | Yes | Custom workload name (defaults to hostname if empty) |
| `hostname` | string | No | Workload hostname |
| `ip` | string | No | IP address(es), comma-separated |
| `description` | string | Yes | Workload description |

### Label Columns

For each label type in your PCE (e.g., env, app, role, loc):
- Column name: `label_<type>` (e.g., `label_env`, `label_app`)
- Value: Label name
- Optional unless `--ignore-missing-headers` is not used

### Example CSV

```csv
name,hostname,ip,description,label_env,label_app,label_role,label_loc
,web-server-01,10.1.1.10,Production web server,Production,WebApp,Web,US-East
,web-server-02,10.1.1.11,Production web server,Production,WebApp,Web,US-East
Custom-Name,db-server-01,10.1.2.10,Primary database,Production,Database,DB-Primary,US-East
,api-server-01,"10.1.3.10,10.1.3.11",API gateway with two IPs,Production,API,Gateway,US-West
```

## Workflow

### Step 1: Prepare CSV File

Create CSV with required columns and data.

### Step 2: Dry-Run Validation

```bash
pylo workload-import --input-file workloads.csv
```

This validates:
- CSV format and required columns
- Name/hostname/IP collisions
- Label existence (creates list of missing labels)
- IP address formats

### Step 3: Review Output

Check for:
- Collision warnings
- Missing labels that will be created
- Validation errors

### Step 4: Execute Import

```bash
pylo workload-import \
  --input-file workloads.csv \
  --proceed-with-creation
```

Or skip confirmation:

```bash
pylo workload-import \
  --input-file workloads.csv \
  --proceed-with-creation \
  --no-confirmation-required
```

## Collision Detection

### Name/Hostname Collisions

The command checks:
1. **Within CSV**: No duplicate names/hostnames in input file
2. **With PCE**: No conflicts with existing workload names/hostnames

**Behavior without flags**: Error and abort
**With `--ignore-hostname-collision`**: Skip conflicting entries
**With `--ignore-all-sorts-collisions`**: Skip all collision entries

### IP Address Collisions

Checks if IP addresses are already used by:
- Existing workloads in PCE
- Other entries in the CSV file

**Behavior without flags**: Error and abort
**With `--ignore-ip-collision`**: Skip conflicting entries
**With `--ignore-all-sorts-collisions`**: Skip all collision entries

## Label Creation

### Automatic Creation

Missing labels are automatically created if they don't exist:

1. Command identifies missing labels
2. Lists them with types
3. Prompts for confirmation (unless `--no-confirmation-required`)
4. Creates labels before creating workloads

### Example Output

```
 * 3 Labels need to be created before Workloads can be imported, listing:
   - Label: NewApp (type=app)
   - Label: Testing (type=env)
   - Label: Backend (type=role)
Do you want to proceed with the creation of these labels? [y/N]:
```

## Examples

### Basic Import

```bash
pylo workload-import -i workloads.csv --proceed-with-creation
```

### Import with Custom Delimiter

```bash
pylo workload-import \
  -i workloads.tsv \
  --input-file-delimiter "\t" \
  --proceed-with-creation
```

### Ignore Collisions

```bash
pylo workload-import \
  -i workloads.csv \
  --ignore-all-sorts-collisions \
  --proceed-with-creation \
  --no-confirmation-required
```

### Custom Label Prefix

If your CSV uses different column names:

```bash
pylo workload-import \
  -i workloads.csv \
  --label-type-header-prefix "lbl_" \
  --proceed-with-creation
```

Expects columns: `lbl_env`, `lbl_app`, `lbl_role`, etc.

### Smaller Batches

```bash
pylo workload-import \
  -i workloads.csv \
  --batch-size 100 \
  --proceed-with-creation
```

## Output Reports

Generated files:
- `import-umw-results_YYYYMMDD-HHMMSS.csv`
- `import-umw-results_YYYYMMDD-HHMMSS.xlsx`

### Report Columns

| Column | Description |
|--------|-------------|
| `name` | Workload name |
| `hostname` | Workload hostname |
| `ip` | IP addresses |
| `description` | Description |
| `label_<type>` | Label values from CSV |
| `href` | PCE API reference (populated after creation) |
| `**not_created_reason**` | Reason if workload was not created |

## Troubleshooting

### Duplicate Names

**Problem**: "CSV contains workloads with duplicates name/hostname"

**Solutions:**
- Remove duplicates from CSV
- Use `--ignore-hostname-collision` to skip
- Rename workloads in CSV

### Missing Label Columns

**Problem**: "CSV/Excel file is missing the column 'label_env'"

**Solutions:**
- Add missing columns to CSV
- Use `--ignore-missing-headers` if intentional
- Check `--label-type-header-prefix` setting

### Invalid IP Address

**Problem**: "CSV line #5 contains invalid IP addresses"

**Solutions:**
- Verify IP format (IPv4: `10.1.1.10`, IPv6: `2001:db8::1`)
- Check for typos or extra characters
- Ensure proper comma separation for multiple IPs

### IP Already in Use

**Problem**: "Duplicate IP address X found in the PCE"

**Solutions:**
- Verify IP is not already assigned
- Use `--ignore-ip-collision` to skip
- Update CSV with different IP

### No Workloads Created

**Problem**: All workloads marked as not created

**Causes:**
- `--proceed-with-creation` not used
- All entries have collisions
- User aborted confirmation

**Solutions:**
- Add `-p` or `--proceed-with-creation` flag
- Review collision settings
- Confirm when prompted

## Best Practices

1. **Test with Small Set**: Import 5-10 workloads first
2. **Validate CSV**: Use dry-run to catch issues early
3. **Backup**: Export existing workloads before large imports
4. **Consistent Naming**: Use consistent hostname patterns
5. **Label Standards**: Define label naming conventions
6. **Document IPs**: Keep IP allocation records
7. **Batch Imports**: For thousands of workloads, import in batches
8. **Review Reports**: Always check output reports for failures

## Related Commands

- `workload-export` - Export existing workloads
- `workload-update` - Update imported workloads
- `workload-resync-names` - Fix workload names
- `label-delete-unused` - Clean up unused labels
