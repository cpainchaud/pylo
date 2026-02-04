# Label Delete Unused

## Overview

The `label-delete-unused` command identifies and removes unused labels from the Illumio Policy Compute Engine (PCE). Unused labels are those that are not referenced by any PCE objects such as workloads, rulesets, IP lists, services, or security principals. This command helps maintain a clean label inventory by automatically detecting and optionally deleting labels that are no longer in use.

## Key Features

- **Safe by Default**: Runs in dry-run mode unless explicitly confirmed with `--confirm` flag
- **Comprehensive Usage Detection**: Checks all usage types reported by the PCE API
- **Deletion Limiting**: Control how many labels to delete in a single operation
- **Detailed Reporting**: Generate Excel or CSV reports with complete label information and deletion status
- **Direct PCE Links**: Report includes clickable links to view labels in PCE UI
- **Sorted Output**: Reports are automatically sorted by label type and value for easy review

## Basic Usage

```bash
pylo label-delete-unused [options]
```

## Command Options

### Execution Control

| Option | Description |
|--------|-------------|
| `--confirm` | Execute the actual deletion in PCE. Without this flag, the command runs in dry-run mode and only generates a report of what would be deleted |
| `--limit` | Maximum number of unused labels to delete (default: all found unused labels). Labels beyond this limit are marked as "ignored (limit reached)" in the report |

### Report Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--report-format` | `-rf` | choice | `csv` | Report format: `csv` or `xlsx` (can be repeated for multiple formats) |
| `--output-dir` | `-o` | string | `output` | Directory where to write the report file(s) |
| `--output-filename` | - | string | Auto-generated | Custom filename for the report. If multiple formats are requested, the extension is adjusted per format |

**Note**: When `--output-filename` is not specified, a timestamped filename is automatically generated in the format: `label-delete-unused_YYYYMMDD_HHMMSS.<format>`

## Label Usage Detection

The command queries the PCE API with the `get_usage=true` parameter to retrieve comprehensive usage information for each label. A label is considered **unused** when ALL of the following usage types are false:

- **workload** - Not assigned to any workloads
- **ruleset** - Not referenced in any rulesets
- **rule_set** - Not referenced in any rule sets (alternative API field)
- **label_group** - Not part of any label groups
- **static_policy_scopes** - Not used in static policy scopes
- **pairing_profile** - Not referenced in pairing profiles
- **permission** - Not used in permissions
- **sec_policy** - Not part of security policies
- **virtual_service** - Not bound to virtual services
- **virtual_server** - Not used in virtual servers
- **firewall_coexistence** - Not referenced in firewall coexistence settings
- **containers_inherit_host_policy_scopes** - Not used in container policy scopes
- **container_workload_profiles** - Not referenced in container workload profiles
- **blocked_connection_reject_scopes** - Not used in blocked connection reject scopes

If a label has **any** usage type set to `true`, it is considered **in use** and will be skipped.

## Logic Flow

### 1. Data Collection
```
Fetch all labels from PCE with usage information
    ↓
API call: objects_label_get(max_results=199000, get_usage=True)
    ↓
Receive labels with embedded usage data
```

### 2. Usage Analysis
```
For each label:
    ↓
Check usage dictionary for any true values
    ↓
If ANY usage type is true:
    → Label is IN USE - Skip deletion
    → Log: "Label '<value>' is used in '<usage_type>', skipping deletion."
    
If ALL usage types are false:
    → Label is UNUSED - Mark for deletion
    → Log: "Label '<value>' is unused, marking for deletion."
```

### 3. Execution Phase

#### Dry-Run Mode (Default - without `--confirm`):
```
Generate report with action: "TO BE DELETED (no confirm option used)"
    ↓
Write report to disk
    ↓
Display message: "No change will be implemented in the PCE until you use 
                  the '--confirm' flag to confirm you're good with them 
                  after review."
```

#### Confirmed Deletion Mode (with `--confirm`):
```
Apply deletion limit (if specified)
    ↓
Create multi-deletion tracker
    ↓
Add all unused labels to tracker
    ↓
Execute batch deletion via PCE API
    ↓
Check deletion results:
    • SUCCESS → Report action: "deleted"
    • ERROR → Report action: "API error" + error message
    ↓
Log summary: "Deletion completed: X labels deleted successfully, 
              Y errors encountered."
```

### 4. Report Generation
```
Sort report by type and value
    ↓
For each requested format (CSV/XLSX):
    Generate output file with all label details
    ↓
Save to output directory
```

## Report Structure

The generated report contains the following columns:

| Column | Description |
|--------|-------------|
| `key` | Label key/type (e.g., `role`, `app`, `env`, `loc`) |
| `value` | Label value/name |
| `type` | Label type (same as key) |
| `created_at` | Timestamp when the label was created in PCE |
| `updated_at` | Timestamp when the label was last updated |
| `external_data_set` | External data set reference (if any) |
| `external_data_reference` | External data reference (if any) |
| `usage_list` | Comma-separated list of usage types that reported `true` (empty for unused labels) |
| `action` | Action taken: "deleted", "TO BE DELETED (no confirm option used)", "ignored (limit reached)", or "API error" |
| `error_message` | Error details if deletion failed (empty on success) |
| `link_to_pce` | Clickable hyperlink to view the label in PCE UI |
| `href` | Full API href path to the label |

**Report Sorting**: All reports are automatically sorted by `type` (ascending) then `value` (ascending) for easier review and analysis.

## Usage Examples

### Example 1: Dry-Run to Identify Unused Labels
Generate a report of all unused labels without making any changes:

```bash
pylo label-delete-unused
```

**Output**: CSV report in `output/label-delete-unused_20260204_143025.csv`

### Example 2: Dry-Run with Excel Report
Identify unused labels and generate an Excel report:

```bash
pylo label-delete-unused --report-format xlsx
```

### Example 3: Dry-Run with Both CSV and Excel Reports
Generate both report formats:

```bash
pylo label-delete-unused --report-format csv --report-format xlsx
```
or using the short option:
```bash
pylo label-delete-unused -rf csv -rf xlsx
```

### Example 4: Delete All Unused Labels (Confirmed)
Actually delete all unused labels found:

```bash
pylo label-delete-unused --confirm
```

**Expected Output**:
```
Fetching all Labels from the PCE... OK!
Analyzing 1234 labels to find unused ones... 
Label 'Old-App' is unused, marking for deletion.
Label 'Decommissioned-Service' is unused, marking for deletion.
...

Found 47 unused labels vs total of 1234 labels.

Proceeding to delete unused labels up to the limit of 'all'...
 - SUCCESS deleting label 'Old-App'
 - SUCCESS deleting label 'Decommissioned-Service'
 - ERROR deleting label 'Protected-Label': Label is referenced by archived policy
...

Deletion completed: 45 labels deleted successfully, 2 errors encountered.
 * Writing report file 'output/label-delete-unused_20260204_143025.csv' ... DONE
```

### Example 5: Limited Deletion
Delete only the first 10 unused labels:

```bash
pylo label-delete-unused --confirm --limit 10
```

This is useful for:
- Testing the deletion process safely
- Gradually cleaning up unused labels
- Reviewing deletion results before proceeding with more

Labels beyond the limit will be included in the report with action: "ignored (limit reached)"

### Example 6: Custom Output Location
Save report to a custom directory with a specific filename:

```bash
pylo label-delete-unused --output-dir reports/labels --output-filename cleanup-2026-02.xlsx -rf xlsx
```

### Example 7: Complete Production Cleanup
Review first, then delete with multiple report formats:

**Step 1 - Review**:
```bash
pylo label-delete-unused -rf xlsx -o cleanup-reports --output-filename unused-labels-review
```
Review the Excel file, verify the labels are truly unused.

**Step 2 - Execute**:
```bash
pylo label-delete-unused --confirm -rf xlsx -rf csv -o cleanup-reports --output-filename unused-labels-deleted
```

## Best Practices

### 1. Always Review Before Deletion
```bash
# Step 1: Generate report without deletion
pylo label-delete-unused -rf xlsx

# Step 2: Review the Excel report carefully

# Step 3: Execute deletion only after verification
pylo label-delete-unused --confirm
```

### 2. Use Limits for Large Cleanups
If you have hundreds of unused labels, delete in batches:
```bash
# Delete 50 at a time
pylo label-delete-unused --confirm --limit 50

# Review results, then repeat
pylo label-delete-unused --confirm --limit 50
```

### 3. Keep Historical Records
Generate timestamped reports for compliance and audit trails:
```bash
# Reports automatically include timestamp in filename
pylo label-delete-unused --confirm -rf xlsx -rf csv
```

### 4. Use Excel for Complex Analysis
Excel reports provide better readability and allow:
- Filtering by label type
- Sorting by creation date
- Analyzing external data references
- Clicking links to verify in PCE

```bash
pylo label-delete-unused -rf xlsx
```

### 5. Monitor API Errors
If deletions fail, the report's `error_message` column contains details. Common errors:
- **Permission denied**: User lacks delete permissions
- **Referenced by archived objects**: Label still has hidden references
- **Rate limiting**: Too many API calls (use `--limit` to throttle)

## Troubleshooting

### Issue: "No unused labels found"
**Possible Causes**:
- All labels are actively in use
- Labels may have been recently cleaned up
- PCE usage tracking is functioning correctly

**Action**: This is typically good news - your label inventory is clean!

### Issue: Deletion fails with "permission denied"
**Cause**: API user lacks label deletion permissions

**Solution**: 
1. Verify API credentials have appropriate role
2. Check PCE user permissions for label management
3. Contact PCE administrator to grant necessary permissions

### Issue: Label appears unused but deletion fails
**Possible Causes**:
- Label referenced by archived/draft policies
- Label has hidden dependencies not reported by usage API
- PCE database constraints preventing deletion

**Solution**:
1. Check the `error_message` column in the report
2. Use the `link_to_pce` column to inspect the label in PCE UI
3. Manually verify label usage in PCE
4. Contact Illumio support if issue persists

### Issue: Process is very slow
**Cause**: Large label inventory (10,000+ labels)

**Solutions**:
- Be patient - the API call for usage data is intensive
- Consider running during off-peak hours
- Process shows progress: "Fetching all Labels from PCE... OK!"

### Issue: Report file already exists
**Cause**: Timestamped filename collision (unlikely) or custom filename already exists

**Solution**:
- Wait a second and retry (timestamps include seconds)
- Use `--output-filename` to specify a unique name
- Move or rename existing report files

## Safety Considerations

### Protected by Default
- **No accidental deletions**: Without `--confirm`, no changes are made to PCE
- **Comprehensive reporting**: All actions are logged with timestamps
- **Error handling**: Failures don't stop the process; all results are reported

### Batch Deletion Safety
The command uses PCE's multi-deletion tracker which:
- Batches API calls efficiently
- Handles individual failures gracefully
- Provides detailed error tracking per label
- Doesn't cascade failures across labels

### Recommended Workflow
1. ✅ Run without `--confirm` to generate report
2. ✅ Review report in Excel for accuracy
3. ✅ Verify labels in PCE UI using report links
4. ✅ Run with `--confirm` to execute deletion
5. ✅ Review deletion results in final report
6. ✅ Archive reports for compliance/audit

## Technical Notes

### API Efficiency
- Single API call fetches all labels with usage data
- Batch deletion minimizes API round-trips
- Maximum query limit: 199,000 labels (well above typical deployments)

### Performance Characteristics
- **Fetch time**: ~5-30 seconds depending on label count
- **Analysis time**: Instant (<1 second for thousands of labels)
- **Deletion time**: ~1-3 seconds per 100 labels
- **Report generation**: ~1-2 seconds

### PCE Version Compatibility
This command is compatible with Illumio PCE versions that support:
- Label usage API endpoint
- `get_usage` parameter on label queries
- Multi-object deletion trackers

Tested with PCE versions 21.5+

### Label Type Coverage
The command handles all label types:
- **role**: Application roles
- **app**: Applications
- **env**: Environments
- **loc**: Locations
- **custom**: Custom label types (if configured)

### External Data References
Labels with `external_data_set` or `external_data_reference` values are treated normally:
- Usage is determined by PCE API, not external references
- External references are preserved in the report
- Consider your integration strategy before deleting externally managed labels

## Integration Considerations

### Automation and Scheduling
The command is suitable for automated cleanup:

```powershell
# Example: Weekly cleanup script
$date = Get-Date -Format "yyyyMMdd"
pylo label-delete-unused --confirm --report-format xlsx --output-dir "\\reports\labels" --output-filename "cleanup-$date"
```

**Recommendations for Automation**:
- Always use `--confirm` explicitly
- Generate XLSX reports for human review
- Store reports in centralized location
- Set up alerts if errors exceed threshold
- Run during maintenance windows

### CI/CD Integration
Example for cleanup as part of PCE maintenance:

```yaml
# Example GitHub Actions workflow
- name: Cleanup Unused Labels
  run: |
    pylo label-delete-unused --confirm --limit 100 -rf csv -o artifacts/
  
- name: Upload Cleanup Report
  uses: actions/upload-artifact@v2
  with:
    name: label-cleanup-report
    path: artifacts/*.csv
```

### Audit and Compliance
Reports include all necessary audit information:
- Timestamp of operation (in filename)
- Complete label metadata (creation date, updates)
- Action taken per label
- Error details for failures
- Direct PCE links for verification

Archive reports for compliance requirements.
