# VEN Duplicate Remover

## Overview

The `ven-duplicate-remover` command identifies and removes duplicate workload entries in Illumio PCE that share the same hostname. This commonly occurs when:
- VENs are reinstalled without proper unpairing
- Virtual machines are cloned without changing hostnames
- Systems are reimaged but old workload entries remain in PCE
- Migration or disaster recovery scenarios create duplicate entries

The command provides flexible options to control which duplicates are deleted while ensuring critical workloads remain protected.

## Key Features

- **Safe by Default**: Runs in dry-run mode unless explicitly told to delete
- **Smart Selection**: Keeps the most relevant workload based on configurable criteria
- **Flexible Filtering**: Target specific workloads using label filters
- **Comprehensive Reporting**: Generate Excel or CSV reports documenting all actions
- **Protection Rules**: Multiple options to prevent deletion of important workloads

## Basic Usage

```bash
pylo ven-duplicate-remover [options]
```

## Command Options

### Execution Control

| Option | Short | Description |
|--------|-------|-------------|
| `--proceed-with-deletion` | - | Actually perform deletions. Without this flag, the command runs in dry-run mode |
| `--do-not-require-deletion-confirmation` | - | Skip interactive confirmation prompt before deleting workloads |
| `--verbose` | `-v` | Enable verbose output for detailed processing information |

### Pre-Filter Options

Pre-filters reduce the initial set of workloads that the command will process **before** duplicate detection and protection logic is applied. These are useful for:
- Limiting scope to specific environments, applications, or locations
- Targeting specific hostname patterns or naming conventions
- Excluding certain workload types from consideration

**Key Difference from Protection Options**: Pre-filters determine which workloads are considered for processing, while protection options (below) determine which workloads are kept safe during deletion decisions.

| Option | Short | Description |
|--------|-------|-------------|
| `--filter-in-label` | `-fil` | Only process workloads matching specified labels (can be repeated for multiple labels). Workloads without these labels are excluded from all processing |
| `--filter-in-hostname-regex` | - | Only process workloads whose hostname matches the specified regular expression (case-insensitive). Hostnames not matching this pattern are completely excluded |
| `--filter-out-unmanaged` | - | Exclude unmanaged workloads from duplicate detection and deletion processing |

### Protection Options

These options prevent specific workloads from being deleted:

| Option | Short | Description |
|--------|-------|-------------|
| `--do-not-delete-the-most-recent-workload` | `-nrc` | Protect the workload with the most recent creation date |
| `--do-not-delete-the-most-recently-heartbeating-workload` | `-nrh` | Protect the workload with the most recent heartbeat |
| `--do-not-delete-if-last-heartbeat-is-more-recent-than <DAYS>` | - | Protect workloads that have heartbeated within the specified number of days |
| `--do-not-delete-if-labels-mismatch` | - | Skip deletion for hostname duplicates where workloads have different label sets |
| `--limit-number-of-deleted-workloads` | `-l` | Limit the total number of workloads deleted (useful for testing) |

### Advanced Options

| Option | Short | Description |
|--------|-------|-------------|
| `--override-pce-offline-timer-to <DAYS>` | - | Override PCE's offline timer to classify workloads as online/offline based on custom day threshold |
| `--ignore-pce-online-status` | - | Allow online workloads to be considered for deletion (bypasses default protection) |

### Report Options

| Option | Short | Description |
|--------|-------|-------------|
| `--report-format` | `-rf` | Report format: `csv` or `xlsx` (can be repeated for multiple formats). Default: `xlsx` |
| `--output-dir` | `-o` | Directory for report files. Default: `output` |
| `--output-filename` | - | Custom filename for the report (extension adjusted per format if multiple formats requested) |

## Logic Flow

### 1. Workload Loading and Pre-Filtering
```
Load all workloads from PCE
    ↓
Apply label pre-filters (--filter-in-label, if specified)
    ↓
Apply hostname regex pre-filter (--filter-in-hostname-regex, if specified)
    ↓
Exclude deleted workloads
    ↓
Optionally exclude unmanaged workloads (--filter-out-unmanaged)
    ↓
Group remaining workloads by hostname (case-insensitive)
```

### 2. Duplicate Detection
```
For each hostname with multiple workloads:
    ↓
Classify workloads as: Online, Offline, or Unmanaged
    ↓
Apply offline timer override (if specified)
    ↓
Check for label mismatches (if protection enabled)
```

### 3. Deletion Candidate Selection

#### For Managed Workloads:
```
If no VENs are online AND --ignore-pce-online-status is NOT set:
    → Skip this hostname (ignore all workloads)
    
If online workloads exist AND --ignore-pce-online-status is NOT set:
    → Keep all online workloads
    → Consider only offline workloads for deletion
    
If --ignore-pce-online-status IS set:
    → Consider both online and offline workloads for deletion

For each deletion candidate:
    Apply protection rules:
    • Most recent creation date?
    • Most recent heartbeat?
    • Heartbeat within specified days?
    • Limit reached?
    
    If not protected:
        → Add to deletion list
```

#### For Unmanaged Workloads:
```
If all workloads for hostname are unmanaged:
    Keep the most recently created unmanaged workload
    Mark others for deletion
    
If mixed (managed + unmanaged):
    Mark all unmanaged workloads for deletion
    (assuming managed workflow handles the hostname)
```

### 4. Execution Phase
```
If --proceed-with-deletion is set:
    List all workloads to be deleted
        ↓
    Request confirmation (unless --do-not-require-deletion-confirmation is set)
        ↓
    If confirmed:
        Execute deletions via API
        Unpair VEN agents
        Record results (success/errors)
Else:
    Dry-run mode: Report what would be deleted
```

### 5. Report Generation
```
Generate report including:
    • Hostname
    • Labels
    • Online status
    • Last heartbeat
    • Creation date
    • Action taken (deleted, ignored + reason)
    • Link to PCE UI
    ↓
Save to specified format(s) and location
```

## Examples

### Example 1: Dry-Run (Safe Discovery)

Find all duplicate hostnames without making any changes:

```bash
pylo ven-duplicate-remover
```

**Output**: Excel report showing all duplicates and what would be deleted.

### Example 2: Delete Duplicates with Confirmation

Delete duplicate workloads, but ask for confirmation first:

```bash
pylo ven-duplicate-remover --proceed-with-deletion
```

### Example 3: Automated Deletion (No Confirmation)

Delete duplicates without prompts - useful for automation:

```bash
pylo ven-duplicate-remover --proceed-with-deletion --do-not-require-deletion-confirmation
```

### Example 4: Filter by Application Label

Only process workloads with specific labels:

```bash
pylo ven-duplicate-remover --filter-in-label "App:WebServers" --filter-in-label "Env:Production"
```

### Example 5: Protect Recent Workloads

Keep workloads that were created most recently and those that heartbeated within the last 30 days:

```bash
pylo ven-duplicate-remover \
    --do-not-delete-the-most-recent-workload \
    --do-not-delete-if-last-heartbeat-is-more-recent-than 30 \
    --proceed-with-deletion
```

### Example 6: Conservative Cleanup with Multiple Protections

Maximum protection - only delete very old, clearly stale duplicates:

```bash
pylo ven-duplicate-remover \
    --do-not-delete-the-most-recent-workload \
    --do-not-delete-the-most-recently-heartbeating-workload \
    --do-not-delete-if-last-heartbeat-is-more-recent-than 90 \
    --do-not-delete-if-labels-mismatch \
    --filter-out-unmanaged \
    --proceed-with-deletion
```

### Example 7: Limited Test Run

Test deletion on a small number of workloads first:

```bash
pylo ven-duplicate-remover \
    --limit-number-of-deleted-workloads 5 \
    --proceed-with-deletion
```

### Example 8: Custom Report Location and Format

Generate both CSV and Excel reports in a custom location:

```bash
pylo ven-duplicate-remover \
    -rf csv -rf xlsx \
    --output-dir "C:\Reports\VEN_Cleanup" \
    --output-filename "duplicate_cleanup_2026-02-03"
```

### Example 9: Override Offline Timer

Treat workloads as offline if they haven't heartbeated in 7 days (instead of PCE's default):

```bash
pylo ven-duplicate-remover \
    --override-pce-offline-timer-to 7 \
    --proceed-with-deletion
```

### Example 12: Aggressive Cleanup (Include Online Workloads)

⚠️ **Caution**: This bypasses the safety mechanism that protects online workloads:

```bash
pylo ven-duplicate-remover \
    --ignore-pce-online-status \
    --do-not-delete-the-most-recently-heartbeating-workload \
    --proceed-with-deletion
```

### Example 13: Production-Safe Workflow

Recommended for production environments:

```bash
# Step 1: Discovery
pylo ven-duplicate-remover \
    --filter-in-label "Env:Production" \
    --output-filename "prod_duplicates_discovery"

# Step 2: Review the generated report manually

# Step 3: Conservative deletion with protections
pylo ven-duplicate-remover \
    --filter-in-label "Env:Production" \
    --do-not-delete-the-most-recent-workload \
    --do-not-delete-the-most-recently-heartbeating-workload \
    --do-not-delete-if-last-heartbeat-is-more-recent-than 60 \
    --do-not-delete-if-labels-mismatch \
    --limit-number-of-deleted-workloads 10 \
    --proceed-with-deletion \
    --output-filename "prod_duplicates_cleanup"
```

## Understanding the Report

The generated report contains the following columns:

| Column | Description |
|--------|-------------|
| **hostname** | The hostname of the workload |
| **label_*** | Label values for each label dimension (app, env, loc, role, etc.) |
| **online** | Whether the workload is currently online, offline, or unmanaged |
| **last_heartbeat** | Timestamp of the last VEN heartbeat |
| **created_at** | When the workload was created in PCE |
| **action** | What action was taken or why it was ignored |
| **link_to_pce** | Clickable link to view the workload in PCE UI |
| **href** | API reference path for the workload |

### Common Action Values

- `deleted` - Workload was successfully deleted
- `TO BE DELETED (no confirm option used)` - Dry-run mode: would be deleted
- `TO BE DELETED (aborted by user)` - User declined confirmation
- `ignored (VEN is online)` - Protected because VEN is currently online
- `ignored (no VEN online)` - Skipped because no duplicate is online
- `ignored (it is the most recently created)` - Protected by creation date rule
- `ignored (it is the most recently heartbeating)` - Protected by heartbeat rule
- `ignored (last heartbeat is more recent than N days)` - Protected by recent heartbeat rule
- `ignored (labels mismatch)` - Protected because duplicates have different labels
- `ignored (limit of N workloads to be deleted was reached)` - Deletion limit reached
- `API error: <message>` - Deletion failed with specific error

## Important Safety Notes

### Default Protections

By default, the command:
- ✅ Runs in dry-run mode (no deletions unless `--proceed-with-deletion` is used)
- ✅ Asks for confirmation before deleting (unless bypassed)
- ✅ Never deletes workloads if no online VEN exists for that hostname
- ✅ Never deletes online workloads (unless `--ignore-pce-online-status` is used)
- ✅ Always keeps at least one workload per hostname

### When Duplicates Are Ignored

The entire duplicate set is ignored (no deletions) when:
- No managed workload is online AND `--ignore-pce-online-status` is not set
- Workloads have mismatching labels AND `--do-not-delete-if-labels-mismatch` is set

### Recommended Best Practices

1. **Always start with dry-run**: Run without `--proceed-with-deletion` first to review what would happen
2. **Use label filters**: Target specific environments or applications to limit scope
3. **Apply protection rules**: Use combination of protection options for critical environments
4. **Test with limits**: Use `--limit-number-of-deleted-workloads` for initial testing
5. **Review reports**: Check generated reports before running actual deletions
6. **Incremental cleanup**: For large environments, clean up in smaller batches
7. **Keep audit trail**: Save reports from each run for compliance and troubleshooting

## Common Scenarios

### Scenario 1: VM Cloning Cleanup

**Problem**: VMs were cloned without changing hostnames, creating duplicates.

**Solution**:
```bash
pylo ven-duplicate-remover \
    --do-not-delete-the-most-recently-heartbeating-workload \
    --do-not-delete-if-labels-mismatch \
    --proceed-with-deletion
```

### Scenario 2: Migration Leftovers

**Problem**: After datacenter migration, old workload entries still exist.

**Solution**:
```bash
pylo ven-duplicate-remover \
    --filter-in-label "Loc:OldDatacenter" \
    --override-pce-offline-timer-to 30 \
    --proceed-with-deletion
```

### Scenario 3: Stale Unmanaged Workloads

**Problem**: Many old unmanaged workload entries cluttering PCE.

**Solution**:
```bash
pylo ven-duplicate-remover \
    --do-not-delete-the-most-recent-workload \
    --proceed-with-deletion
```

### Scenario 4: Regular Maintenance

**Problem**: Need to regularly clean up duplicates as part of PCE hygiene.

**Solution**: Set up scheduled task with conservative settings:
```bash
pylo ven-duplicate-remover \
    --do-not-delete-the-most-recent-workload \
    --do-not-delete-the-most-recently-heartbeating-workload \
    --do-not-delete-if-last-heartbeat-is-more-recent-than 90 \
    --do-not-delete-if-labels-mismatch \
    --do-not-require-deletion-confirmation \
    --proceed-with-deletion \
    --output-dir "C:\Logs\PCE_Maintenance"
```

### Scenario 5: Targeted Cleanup by Naming Convention

**Problem**: Your organization follows naming conventions, and you want to clean up specific tiers or types of systems.

**Solution**: Use regex to target specific patterns:

```bash
# Target only database tier duplicates
pylo ven-duplicate-remover \
    --filter-in-hostname-regex "^db-.*|.*-db-[0-9]+" \
    --do-not-delete-the-most-recent-workload \
    --proceed-with-deletion

# Target only temporary/ephemeral systems
pylo ven-duplicate-remover \
    --filter-in-hostname-regex "^(ephemeral|temporary|tmp)-.*" \
    --ignore-pce-online-status \
    --proceed-with-deletion

# Target specific numbered range of servers
pylo ven-duplicate-remover \
    --filter-in-hostname-regex "^web-(0[1-9]|1[0-9])$" \
    --proceed-with-deletion
```

## Troubleshooting

### No Duplicates Found

If the command reports no duplicates:
- Verify workloads are loaded correctly
- Check if label pre-filters are too restrictive (`--filter-in-label`)
- Check if hostname regex pre-filter is too restrictive or has an error (`--filter-in-hostname-regex`)
- Ensure you're not excluding the workloads with `--filter-out-unmanaged`

### Duplicates Not Being Deleted

Common reasons:
1. Missing `--proceed-with-deletion` flag (dry-run mode)
2. No online VEN exists for that hostname
3. All duplicates are online (protected by default)
4. Protection rules are excluding the workloads
5. Deletion limit was reached

Check the generated report's "action" column to see why workloads were ignored.

### "No VEN Online" Messages

If you see many "IGNORED: there is no VEN online" messages:
- This is a safety feature - at least one VEN should be online
- If you need to delete offline duplicates, use `--ignore-pce-online-status` (carefully!)
- Or adjust the offline timer with `--override-pce-offline-timer-to`

### Regex Pattern Not Matching Expected Workloads

If your regex filter isn't matching the workloads you expect:

**Problem**: Regex filters out too many or too few workloads

**Solutions**:
1. **Test your regex pattern first** - Use an online regex tester (regex101.com) with sample hostnames
2. **Remember it's case-insensitive** - "WEB-01" matches "^web-.*"
3. **Pattern matches anywhere** - "web" matches "my-web-server" (use `^web` to match only at start)
4. **Check stripped hostnames** - The regex applies to hostname WITHOUT domain suffix
   - Example: "web-01.company.com" is matched as just "web-01"
5. **Escape special characters** - Use `\.` for literal dots in patterns
6. **Check the filtering output** - The command shows: "OK! X workloads left after regex filtering"

**Testing approach**:
```bash
# Step 1: Dry run with regex to see how many match
pylo ven-duplicate-remover --filter-in-hostname-regex "YOUR_PATTERN"

# Step 2: Check the output line:
#   "OK! X workloads left after regex filtering"
#   If X is 0, your pattern matched nothing
#   If X is too high, your pattern is too broad

# Step 3: Adjust pattern and retry until X is reasonable
```

**Common regex mistakes**:
- ❌ `web-.*` - Matches ANY hostname containing "web-" anywhere (too broad)
- ✅ `^web-.*` - Matches hostnames STARTING with "web-"
- ❌ `-prod` - Matches anywhere: "web-production-server", "prod-web"
- ✅ `-prod$` - Matches ONLY if ending with "-prod"
- ❌ `test|dev` - Matches "test" OR "dev" anywhere (might catch "testing", "device")
- ✅ `^(test|dev)-` - Matches starting with "test-" OR "dev-"
- ❌ `web-*` - Invalid regex (needs .* not just *)
- ✅ `^web-.*` - Correct: "web-" followed by any characters

### Invalid Regular Expression Error

If you see "Invalid regular expression" error:
- Check for unescaped special characters: `( ) [ ] { } . * + ? ^ $ | \`
- Use online regex validators (regex101.com) to test your pattern
- Common fixes:
  - `^web-.*` not `^web-*` (use `.*` for "any characters", `*` alone is invalid)
  - `(test|dev)` not `(test,dev)` (use `|` not `,` for OR)
  - Escape dots in IP patterns: `10\.0\.0\.1` not `10.0.0.1`
  - Close all parentheses: `^(web|app)` not `^(web|app`

**Example error and fix**:
```bash
# Error: Invalid regular expression: nothing to repeat at position 5
pylo ven-duplicate-remover --filter-in-hostname-regex "^web-*"

# Fix: Use .* instead of just *
pylo ven-duplicate-remover --filter-in-hostname-regex "^web-.*"
```

## Related Commands

- `workload-list` - List all workloads
- `workload-delete` - Delete individual workloads
- `ven-idle-to-illumination` - Convert idle VENs to illumination mode

## Support

For issues or questions:
1. Check the generated reports for detailed action explanations
2. Run with `--verbose` flag for more detailed output
3. Start with dry-run mode to safely explore behavior
4. Review this documentation for examples matching your use case
