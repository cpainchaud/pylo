# IP List Import Command

## Overview

The `iplist-import` command imports IP lists into the PCE from CSV or Excel files. It supports bulk creation of IP lists with their associated IP addresses, CIDR ranges, and exclusions.

## Command Syntax

```bash
pylo iplist-import --input-file FILE [OPTIONS]
```

## Required Arguments

| Argument | Short | Type | Description |
|----------|-------|------|-------------|
| `--input-file` | `-i` | string | Path to CSV or Excel input file |

## Options

### Input File Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--input-file-delimiter` | string | `,` | CSV field delimiter (only for CSV files) |
| `--network-delimiter` | string | `,` | Delimiter used within the networks column to separate multiple entries |

### Behavior Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--ignore-if-iplist-exists` | - | flag | Skip CSV entries if an IP list with the same name already exists |
| `--proceed` | `-p` | flag | Actually create the IP lists (without this, it's a dry-run) |
| `--no-confirmation-required` | `-n` | flag | Skip confirmation prompt before creating IP lists |

### Output Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--output-dir` | `-o` | string | `output` | Directory where output reports will be saved |

## Input File Format

### Required Columns

| Column | Type | Optional | Description |
|--------|------|----------|-------------|
| `name` | string | No | Name of the IP list |
| `description` | string | Yes | Description of the IP list |
| `networks` | string | No | Comma-separated list of IP entries (or custom delimiter) |

### Network Entry Formats

The `networks` column accepts multiple formats:

1. **Individual IP Addresses**
   - IPv4: `192.168.1.10`
   - IPv6: `2001:db8::1`

2. **CIDR Notation**
   - IPv4: `10.0.0.0/8`
   - IPv6: `2001:db8::/32`

3. **IP Ranges**
   - IPv4: `192.168.1.1-192.168.1.254`
   - IPv6: `2001:db8::1-2001:db8::ffff`

4. **Exclusions** (prefix with `!`)
   - `!192.168.1.100` - Exclude specific IP
   - `!10.0.0.0/24` - Exclude CIDR range
   - `!192.168.1.1-192.168.1.10` - Exclude IP range

### Multiple Networks

Use the delimiter (default `,`) to separate multiple network entries:

```
10.0.0.0/8,!10.0.1.0/24,192.168.1.0/24
```

### Example CSV

```csv
name,description,networks
Private_Networks,RFC 1918 private networks,"10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
DMZ_Servers,DMZ server IPs,192.168.100.10-192.168.100.50
Web_Servers_Prod,Production web servers,"10.1.1.10,10.1.1.11,10.1.1.12"
Internal_With_Exclusion,Internal minus guest,"10.0.0.0/8,!10.50.0.0/16"
```

## Workflow

### 1. Dry-Run (Default)

First, run without `--proceed` to validate your input:

```bash
pylo iplist-import --input-file iplists.csv
```

This will:
- Load and parse the CSV file
- Check for name collisions with existing IP lists
- Validate network entries (IP format, CIDR notation, ranges)
- Display what would be created
- **NOT** make any changes to the PCE

### 2. Review and Confirm

Review the output for any errors or warnings:
- Duplicate names (in CSV or PCE)
- Invalid IP addresses or ranges
- Invalid CIDR notation

### 3. Execute Import

Once validated, proceed with creation:

```bash
pylo iplist-import --input-file iplists.csv --proceed
```

Or skip confirmation prompt:

```bash
pylo iplist-import --input-file iplists.csv --proceed --no-confirmation-required
```

## Collision Detection

### Name Collisions

The command checks for IP list name collisions in two places:

1. **Within CSV file**: Names must be unique within the input file
2. **With existing IP lists**: Names must not conflict with PCE IP lists

**Behavior with `--ignore-if-iplist-exists`:**
- CSV entries with names matching existing IP lists are skipped
- No error is thrown; entries are marked as ignored in the report
- Useful for incremental imports or updates

**Without the flag:**
- Any name collision causes an error
- Import is aborted before any API calls
- User must resolve conflicts manually

## Output Reports

The command generates two report files:

- `import-iplists-results_YYYYMMDD-HHMMSS.csv`
- `import-iplists-results_YYYYMMDD-HHMMSS.xlsx`

### Report Columns

| Column | Description |
|--------|-------------|
| `name` | IP list name from CSV |
| `description` | IP list description from CSV |
| `networks` | Network entries from CSV |
| `href` | PCE API reference (only populated after creation) |
| `**not_created_reason**` | Reason if IP list was not created |

## Examples

### Basic Import

```bash
pylo iplist-import -i iplists.csv --proceed
```

### Import with Custom Delimiter

If your CSV uses semicolons for network separation:

```bash
pylo iplist-import \
  --input-file iplists.csv \
  --network-delimiter ";" \
  --proceed
```

### Import with Newline-Separated Networks

```bash
pylo iplist-import \
  --input-file iplists.csv \
  --network-delimiter "\n" \
  --proceed
```

**Note**: In the CSV, use actual newlines within quoted cells:
```csv
name,description,networks
Multi_Line,"Multiple networks","10.0.0.0/8
172.16.0.0/12
192.168.0.0/16"
```

### Ignore Existing IP Lists

```bash
pylo iplist-import \
  --input-file new-iplists.csv \
  --ignore-if-iplist-exists \
  --proceed \
  --no-confirmation-required
```

### Custom Output Directory

```bash
pylo iplist-import \
  --input-file iplists.csv \
  --output-dir /path/to/reports \
  --proceed
```

## Validation Rules

### Name Validation

- Must not be empty
- Must be unique within CSV
- Must not conflict with existing IP lists (unless `--ignore-if-iplist-exists` is used)

### Network Validation

- At least one network entry required per IP list
- IPv4 addresses must be valid
- IPv6 addresses must be valid
- CIDR masks must be valid (0-32 for IPv4, 0-128 for IPv6)
- IP ranges must have valid from/to addresses
- Range format: `from-to` with single dash

### Error Examples

**Invalid IP address:**
```
ERROR: Iplist at line #5 contains invalid IP addresses: '192.168.1.999'
```

**Invalid CIDR mask:**
```
ERROR: Iplist at line #3 has invalid network mask in CIDR 10.0.0.0/33
```

**Empty networks:**
```
ERROR: Iplist at line #7 has no network entry
```

**Missing name:**
```
ERROR: Iplist at line #2 is missing a name in CSV
```

## Common Workflows

### Bulk Import from Spreadsheet

1. Create spreadsheet with required columns
2. Export as CSV
3. Validate with dry-run:
   ```bash
   pylo iplist-import -i iplists.csv
   ```
4. Import:
   ```bash
   pylo iplist-import -i iplists.csv --proceed
   ```

### Migration from Another System

1. Export IP lists from source system
2. Transform to required CSV format
3. Run dry-run to catch any issues
4. Import in batches if needed:
   ```bash
   pylo iplist-import -i batch1.csv --proceed -n
   pylo iplist-import -i batch2.csv --proceed -n
   ```

### Incremental Updates

To add new IP lists without affecting existing ones:

```bash
pylo iplist-import \
  -i new-additions.csv \
  --ignore-if-iplist-exists \
  --proceed \
  --no-confirmation-required
```

## Troubleshooting

### Duplicate Name Error

**Problem**: "CSV contains iplists with duplicates name"

**Solution**:
- Check CSV for duplicate names
- Names are case-insensitive
- Remove or rename duplicates

### Name Conflict with PCE

**Problem**: "PCE contains iplists with duplicates name from CSV"

**Solutions**:
- Use `--ignore-if-iplist-exists` to skip existing entries
- Or rename IP lists in CSV
- Or delete conflicting IP lists from PCE first

### Invalid Network Entry

**Problem**: "Invalid IP address format" or "Invalid CIDR"

**Solutions**:
- Verify IP addresses are correctly formatted
- Check CIDR notation (e.g., `/24` not `/255.255.255.0`)
- Ensure no extra spaces or characters
- Use proper range notation with single dash

### Empty Networks Column

**Problem**: "No network entry"

**Solutions**:
- Ensure networks column is not empty
- Check delimiter matches `--network-delimiter` setting
- Verify CSV parsing is correct

## Best Practices

1. **Always Dry-Run First**: Validate before creating anything
2. **Use Descriptive Names**: Make IP list names clear and consistent
3. **Document in Description**: Use the description field for context
4. **Group Related IPs**: Keep related IP ranges in the same IP list
5. **Use Exclusions Wisely**: Exclusions can simplify large range definitions
6. **Check Existing Lists**: Review current IP lists before importing to avoid duplicates
7. **Batch Large Imports**: For hundreds of IP lists, consider importing in smaller batches

## Related Commands

- `iplist-analyzer` - Analyze IP list coverage after import
- `rule-export` - Export rules to see how IP lists are used
- `workload-export` - Export workloads to verify IP coverage
