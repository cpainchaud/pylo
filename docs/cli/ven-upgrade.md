# VEN Upgrade Command

## Overview

The `ven-upgrade` command upgrades VEN (Virtual Enforcement Node) agents to a specified target version. It provides filtering options to control which agents are upgraded and generates reports of the upgrade process.

## Command Syntax

```bash
pylo ven-upgrade --target-version VERSION [OPTIONS]
```

## Required Arguments

| Argument | Type | Description |
|----------|------|-------------|
| `--target-version` | string | Target VEN version to upgrade to (e.g., "22.2.0", "21.5.32") |

## Options

### Filter Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--filter-label` | `-fl` | string | Filter by label name (can be repeated) |
| `--filter-ven-versions` | - | list | Filter by specific current VEN versions (space-separated) |
| `--filter-on-href-from-file` | - | string | CSV file containing workload HREFs to upgrade |

### Execution Options

| Option | Type | Description |
|--------|------|-------------|
| `--confirm` | flag | Actually perform the upgrade (without this, it's a dry-run) |

## Version Support

- **Minimum Supported Version**: 18.3.0
- **Versions Below Minimum**: Automatically filtered out (cannot be upgraded via API)
- **Higher Versions**: Agents with version higher than target are skipped

## How It Works

### Workflow

1. **List Current Versions**: Shows VEN version distribution across all agents
2. **Apply Filters**: Narrows down agents based on labels, versions, or HREF file
3. **Validate Target**: Ensures target version is valid
4. **Filter Incompatible**: Removes agents with unsupported versions or higher versions than target
5. **Dry-Run/Upgrade**: Lists agents to upgrade (or performs upgrade if `--confirm`)
6. **Report**: Shows final filtered list with version counts

### Automatic Filtering

The command automatically excludes:
- Agents with versions below 18.3.0 (not supported by API)
- Agents already at or above the target version
- Agents that don't match filter criteria

## Examples

### Check Current VEN Versions

View version distribution without making changes:

```bash
pylo ven-upgrade --target-version 22.2.0
```

### Dry-Run with Filters

See which agents would be upgraded:

```bash
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label E-Production
```

### Upgrade Specific Versions

Upgrade only agents running version 21.5.0:

```bash
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-ven-versions "21.5.0" \
  --confirm
```

### Upgrade Multiple Specific Versions

```bash
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-ven-versions "21.5.0" "21.5.32" "22.1.0" \
  --confirm
```

### Upgrade by Label

Upgrade all agents in test environment:

```bash
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label E-Test \
  --confirm
```

### Upgrade from HREF File

```bash
# Create CSV with workload HREFs
cat > workloads-to-upgrade.csv << EOF
href
/orgs/1/workloads/abc123
/orgs/1/workloads/def456
EOF

# Upgrade only these workloads
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-on-href-from-file workloads-to-upgrade.csv \
  --confirm
```

### Combined Filters

Upgrade specific version in specific environment:

```bash
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label E-Production \
  --filter-ven-versions "21.5.32" \
  --confirm
```

## Console Output

### Version Distribution (Before Filters)

```
 * Listing VEN Agents TOTAL count per version:
   - 18.3.0     : 5
   - 19.1.0     : 12
   - 21.5.0     : 45
   - 21.5.32    : 134
   - 22.1.0     : 23
   - 22.2.0     : 8
    - TOTAL: 227 Agents
```

### Target Version

```
 * Parsing target version '22.2.0'
```

### After Filtering

```
 * Filter out VEN Agents which aren't matching filters:

  * DONE

 * Listing VEN Agents FILTERED count per version:
   - 21.5.0     : 15
   - 21.5.32    : 42
   - 22.1.0     : 8
    - TOTAL: 65 Agents
```

### Upgrade Execution

```
 *** Now Requesting Agents Upgrades from the PCE ***
 - Agent #1/65: wkl NAME:'web-server-01' HREF:/orgs/1/workloads/abc123 Labels:R-Web|A-Frontend|E-Production|L-US-East
 - Agent #2/65: wkl NAME:'web-server-02' HREF:/orgs/1/workloads/def456 Labels:R-Web|A-Frontend|E-Production|L-US-East
...

 ** All Agents Upgraded **
```

## Common Workflows

### Safe Phased Upgrade

**Phase 1: Test Environment**
```bash
# Dry-run first
pylo ven-upgrade --target-version 22.2.0 --filter-label E-Test

# Review output, then confirm
pylo ven-upgrade --target-version 22.2.0 --filter-label E-Test --confirm
```

**Phase 2: Development**
```bash
pylo ven-upgrade --target-version 22.2.0 --filter-label E-Dev --confirm
```

**Phase 3: Production (Gradual)**
```bash
# Upgrade one old version at a time in production
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label E-Production \
  --filter-ven-versions "21.5.0" \
  --confirm

# Wait, monitor, then proceed with next version
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label E-Production \
  --filter-ven-versions "21.5.32" \
  --confirm
```

### Application-Specific Upgrades

```bash
# Upgrade web servers first
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label A-WebServer \
  --confirm

# Then API services
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label A-API \
  --confirm

# Finally databases (most critical)
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label A-Database \
  --confirm
```

### Complete Upgrade Workflow

```bash
# Step 1: Check current state
pylo ven-upgrade --target-version 22.2.0

# Step 2: Export workload list for records
pylo workload-export --report-format xlsx --output-filename pre-upgrade-state

# Step 3: Test with small group
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label E-Test \
  --confirm

# Step 4: Monitor and verify
# Check PCE UI for successful upgrades

# Step 5: Proceed with larger rollout
pylo ven-upgrade \
  --target-version 22.2.0 \
  --filter-label E-Production \
  --confirm

# Step 6: Verify completion
pylo ven-upgrade --target-version 22.2.0
# Should show most/all agents at target version
```

## Upgrade Behavior

### API Request

The command sends upgrade requests to the PCE API, which:
1. Queues the upgrade for each agent
2. Agents check in and receive upgrade instruction
3. Agents download new version and upgrade
4. Agents restart and reconnect

### Timing

- **Request**: Immediate (seconds)
- **Execution**: Depends on agent check-in interval
- **Completion**: Minutes to hours depending on environment

### Agent Behavior

During upgrade, agents:
1. Download new VEN package
2. Stop current VEN service
3. Install new version
4. Start new VEN service
5. Reconnect to PCE

**Note**: Brief connectivity interruption during agent restart.

## Troubleshooting

### No Agents Upgraded

**Problem**: TOTAL shows 0 agents after filtering

**Causes:**
- All agents already at target version or higher
- Filters too restrictive
- All agents below minimum supported version (18.3.0)

**Solutions:**
- Check actual VEN versions: review "TOTAL count per version" output
- Remove or adjust filters
- For old agents, consider manual upgrade process

### Unsupported Version

**Problem**: "NOT SUPPORTED" shown for some versions

**Cause**: Agents running versions below 18.3.0

**Solutions:**
- These agents cannot be upgraded via API
- Must be upgraded manually (re-install VEN)
- Consider upgrading through other management tools

### Upgrade Request Fails

**Problem**: API error when requesting upgrade

**Solutions:**
- Check API user permissions
- Verify workload is online and connected
- Ensure target version exists in PCE
- Review PCE logs for errors

### Agents Not Upgrading

**Problem**: Upgrade requested but agents stay on old version

**Causes:**
- Agents not checking in (offline)
- Network connectivity issues preventing download
- Insufficient disk space on workload
- Permission issues on workload

**Solutions:**
- Verify agent connectivity: check heartbeat in PCE
- Check network access to VEN repository
- Review agent logs on workload
- Ensure adequate disk space

### Using Cache Mode

**Problem**: "SKIPPING Upgrade process as --use-cache option was used"

**Cause**: The command was run with `--use-cache` global flag

**Solution**: Remove `--use-cache` flag. Upgrades require live PCE connection.

## Best Practices

1. **Always Dry-Run First**: Never use `--confirm` without reviewing the planned upgrades

2. **Test First**: Upgrade test/dev environments before production

3. **Gradual Rollout**: Don't upgrade entire production at once

4. **Monitor**: Watch for issues after each phase before proceeding

5. **Maintenance Window**: Schedule upgrades during maintenance windows

6. **Version Strategy**:
   - Upgrade to latest stable version
   - Skip very old versions incrementally if needed
   - Follow Illumio's recommended upgrade paths

7. **Document**: Keep records of upgrade timing and results

8. **Coordinate**: Inform application teams of upgrade schedules

9. **Backup Plan**: Have rollback procedure ready

10. **Verify**: Check agent versions in PCE UI after upgrade

## Version Considerations

### Choosing Target Version

- **Latest Stable**: Recommended for most environments
- **Tested Version**: Use versions validated in your test environment
- **Feature Requirements**: Upgrade to versions supporting needed features
- **Compatibility**: Ensure PCE version supports target VEN version

### Compatibility Matrix

Consult Illumio documentation for VEN-to-PCE compatibility:
- Each PCE version supports specific VEN version ranges
- Newer VENs may not work with older PCEs
- Always verify compatibility before upgrading

## Performance Considerations

- **Large Environments**: Upgrades take time; plan accordingly
- **Network Impact**: Agents download upgrade packages
- **PCE Load**: Large batch upgrades can increase PCE load
- **Stagger Upgrades**: Consider upgrading in batches to reduce impact

## Related Commands

- `ven-compatibility-report-export` - Check agent compatibility
- `ven-idle-to-visibility` - Change agent modes after upgrade
- `workload-export` - Export workload/agent information
- `ven-duplicate-remover` - Clean up duplicate agents

## Safety Notes

### Impact Assessment

- **Low Risk**: Agent upgrades are generally safe
- **Brief Downtime**: Short disruption during agent restart
- **Rollback**: Difficult; plan carefully
- **Testing**: Always test in non-production first

### Production Considerations

- Schedule during maintenance windows
- Upgrade critical systems last
- Have support resources available
- Monitor closely during and after upgrade
- Keep PCE team informed of large upgrade operations
