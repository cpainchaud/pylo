# Workload Resync Names Command

## Overview

The `workload-resync-names` command resets forced workload names to match their hostnames. It's useful for fixing workloads that had custom names assigned in the past but should now use their hostname-based names.

## Command Syntax

```bash
pylo workload-resync-names [OPTIONS]
```

## Purpose

When workloads have forced names that don't match their hostnames, this command:
- Identifies managed workloads with mismatching names
- Resets forced names to null or derived from hostname to avoid mismatch
- Generates reports of changes

## Options

### Execution Options

| Option | Type | Description |
|--------|------|-------------|
| `--confirm` | flag | Actually make changes (without this, analysis only) |
| `--batch-size` | integer | Workloads per API call (default: 500) |

### Output Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--report-format` | `-rf` | choice | `csv` | Report format (csv, xlsx, json) |
| `--output-dir` | `-o` | string | `output` | Output directory |
| `--output-filename` | - | string | Auto-generated | Custom filename |

## How It Works

### Name Comparison

The command compares:
1. **Forced Name**: Custom name set on workload
2. **Hostname**: Actual workload hostname

Both are converted to short names (FQDN stripped) and compared case-insensitively.

### Matching Logic

- Strips FQDNs to short names (e.g., `server.company.com` → `server`)
- Compares case-insensitively (`SERVER` = `server`)
- Only processes **managed** workloads (with VEN agents)

### Reset Behavior

When names are reset:
- Forced name is set to the short hostname
- PCE recalculates display name based on hostname + labels
- Workload remains managed and operational

## Examples

### Analysis (Dry-Run)

See which workloads would be affected:

```bash
pylo workload-resync-names
```

### Execute Name Resync

Actually reset the names:

```bash
pylo workload-resync-names --confirm
```

### Custom Batch Size

Process in smaller batches:

```bash
pylo workload-resync-names --confirm --batch-size 100
```

### Multiple Output Formats

```bash
pylo workload-resync-names \
  --confirm \
  --report-format xlsx \
  --report-format json
```

## Console Output

### Analysis Phase

```
 * Summary of Analysis:
 - Found 1,234 Managed Workloads
 - Found 856 Workloads with Forced Names
 - Found 42 Workloads with Mismatching Forced Names

Found mismatching forced name for web-server-01.company.com (hostname=WebServer01)
Found mismatching forced name for db-server-02.company.com (hostname=DATABASE-02)
...

Changes have not been confirmed. Use the --confirm flag to confirm the changes and push to the PCE
```

### Execution Phase

```
Sending payload for batch 1 of 1 (42 workloads)
 - Updated web-server-01 (/orgs/1/workloads/abc123)
 - Updated db-server-02 (/orgs/1/workloads/def456)
...
```

## Report Columns

| Column | Description |
|--------|-------------|
| `name` | Current workload name |
| `hostname` | Workload hostname |
| `status` | Update status (`pending`, `updated`, `failed`) |
| `reason` | Reason for status |
| `href` | Workload HREF |

## Common Scenarios

### After Hostname Standardization

If hostnames were recently standardized but workloads retain old forced names:

```bash
pylo workload-resync-names --confirm
```

### After Label Restructure

When label structure changes and names should reflect new conventions:

```bash
# Resync names to pick up new label-based naming
pylo workload-resync-names --confirm
```

### Cleanup After Migration

After migrating from another system with custom names:

```bash
pylo workload-resync-names --confirm
```

## Workflow

### Complete Resync Workflow

```bash
# Step 1: Export current state
pylo workload-export --report-format xlsx --output-filename before-resync

# Step 2: Analyze what would change
pylo workload-resync-names --report-format xlsx --output-filename resync-analysis

# Step 3: Review analysis report
# Open Excel file, check "reason" column

# Step 4: Execute resync
pylo workload-resync-names --confirm

# Step 5: Verify in PCE UI
# Check workload names match hostnames

# Step 6: Export final state
pylo workload-export --report-format xlsx --output-filename after-resync
```

## Understanding Results

### Success Indicators

- Status: `updated`
- High count of successful updates
- No failures

### Warning Signs

- Status: `failed` with error messages
- Many workloads with same failure reason
- Batch failures

## Troubleshooting

### No Workloads Found

**Problem**: "Found 0 Workloads with Mismatching Forced Names"

**Causes:**
- All workload names already match hostnames
- Only unmanaged workloads exist
- Names are close enough (FQDN differences only)

**Solutions:**
- Verify forced names in PCE UI
- Check workload management status
- Review name comparison logic expectations

### API Update Failed

**Problem**: "Failed to update workload X"

**Causes:**
- Insufficient permissions
- Workload in transitional state
- PCE API error

**Solutions:**
- Check API user permissions
- Retry failed workloads
- Review PCE logs
- Contact Illumio support if persistent

### Partial Batch Failure

**Problem**: Some workloads in batch failed

**Solutions:**
- Review error messages in report
- Check individual workload status in PCE
- Retry with smaller batch size
- Fix specific issues and re-run

### Changes Not Reflected

**Problem**: Ran command with --confirm but names unchanged

**Causes:**
- Did not use `--confirm` flag
- API call failed silently
- Browser cache in PCE UI

**Solutions:**
- Ensure `--confirm` was used
- Check report for actual status
- Refresh PCE UI (hard refresh)
- Verify via API directly

## Best Practices

1. **Always Analyze First**: Run without `--confirm` to review changes

2. **Export Before**: Create backup export before making changes

3. **Test on Subset**: If possible, test on a small group first

4. **Review Reports**: Check output reports for any failures

5. **Coordinate**: Inform teams before bulk name changes

6. **Monitor**: Watch PCE after changes for any issues

7. **Document**: Keep records of when and why resyncs were performed

## Impact Assessment

### Low Risk Operations

- Resetting names to match hostnames
- Names only affect display, not functionality
- No impact on policy or enforcement

### Considerations

- **Policy Rules**: Rules using workload names may be affected (rare)
- **External Integrations**: Systems relying on workload names need update
- **User Confusion**: Users accustomed to old names may be confused temporarily
- **Reporting**: Historical reports may reference old names

## Validation

After running the command:

1. **PCE UI Check**: Verify names in workload list
2. **Spot Check**: Manually verify several workloads
3. **Report Review**: Check all entries marked as `updated`
4. **Functional Test**: Ensure policy and enforcement still work
5. **User Feedback**: Gather feedback from PCE users

## Related Commands

- `workload-export` - Export workload data including names
- `workload-update` - Update workload labels and properties
- `workload-import` - Import workloads with specific names

## Technical Details

### Name Stripping

FQDN stripping removes domain portion:
- `web-server-01.company.com` → `web-server-01`
- `DATABASE-02.INTERNAL.NET` → `DATABASE-02`

### Case Insensitivity

Comparison ignores case:
- `WebServer01` = `webserver01` = `WEBSERVER01`

### Managed Workloads Only

- Only processes workloads with VEN agents
- Unmanaged workloads are skipped
- Ensures names can be properly recalculated

## Performance

- **Small environments** (< 100 workloads): Seconds
- **Medium environments** (100-1000 workloads): Under a minute
- **Large environments** (1000+ workloads): Several minutes

Batch processing reduces API calls and improves performance.
