# Rule Export Command

## Overview

The `rule-export` command exports security policy rules from PCE rulesets into structured reports. It provides flexible formatting options for consumer/provider objects and supports multiple output formats.

## Command Syntax

```bash
pylo rule-export [OPTIONS]
```

## Options

### Output Format Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--report-format` | `-rf` | choice | `csv` | Report format: `csv`, `xlsx`, or `json` (can be repeated for multiple formats) |
| `--output-file` | `-o` | string | Auto-generated | Output file path (relative to `./output/` or absolute path) |
| `--output-file-timestamp` | `-oft` | flag | - | Append timestamp to output filename |

**Note**: When `--output-file` is not specified, a timestamped filename is automatically generated in the format: `rule-export_YYYYMMDD-HHMMSS.<format>`. Use `--output-file-timestamp` to add timestamps when providing a custom filename.

### Object Display Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--prefix-objects-with-type` | flag | `false` | Prefix objects with their type (e.g., `label:Web`, `iplist:Private_Networks`) |
| `--object-types-as-section` | flag | `false` | Group objects by type with section headers (see example below) |

## Report Columns

| Column | Description |
|--------|-------------|
| `ruleset` | Name of the ruleset containing the rule |
| `scope` | Scope labels defining where the rule applies |
| `type` | Rule type: `intra` (intra-scope) or `extra` (extra-scope) |
| `consumers` | Source/consumer objects (workloads, labels, IP lists) |
| `providers` | Destination/provider objects (workloads, labels, IP lists, services) |
| `services` | Network services allowed (ports/protocols) |
| `options` | Rule options (disabled, secure-connect, stateless, machine_auth) |
| `ruleset_url` | Direct link to the ruleset in PCE UI |
| `ruleset_href` | PCE API reference (HREF) for the ruleset |

## Object Display Formats

### Default Format

By default, objects are listed without type prefixes:

```
Web
Database
API-Gateway
```

### With Type Prefixes (`--prefix-objects-with-type`)

Objects are prefixed with their type:

```
label:Web
label:Database
iplist:Private_Networks
```

### With Section Headers (`--object-types-as-section`)

Objects are grouped by type with section headers:

```
LABELS:
Web
Database
API-Gateway

IPLISTS:
Private_Networks
DMZ_Networks
```

## Rule Types

### Intra-Scope Rules

- Apply to traffic within the scope
- Both source and destination must match the scope
- Listed as `type: intra` in the report

### Extra-Scope Rules

- Apply to traffic crossing scope boundaries
- Either source or destination (or both) outside the scope
- Listed as `type: extra` in the report

## Rule Options

The `options` column may contain:

| Option | Description |
|--------|-------------|
| `disabled` | Rule is disabled and not enforced |
| `secure-connect` | Requires Illumio Secure Connect (encrypted tunnel) |
| `stateless` | Stateless rule (no connection tracking) |
| `machine_auth` | Requires machine authentication |

## Examples

### Basic Export

Export all rules to CSV:

```bash
pylo rule-export
```

### Export with Type Prefixes

Add type prefixes to all objects:

```bash
pylo rule-export --prefix-objects-with-type
```

### Export with Section Headers

Group objects by type with section headers:

```bash
pylo rule-export --object-types-as-section
```

### Multiple Output Formats

Generate both Excel and JSON reports:

```bash
pylo rule-export --report-format xlsx --report-format json
```

### Custom Output Location

Save to specific directory with custom filename:

```bash
pylo rule-export \
  --output-file /path/to/exports/policy-rules-2024.xlsx \
  --report-format xlsx
```

### Full Example with All Options

```bash
pylo rule-export \
  --prefix-objects-with-type \
  --report-format xlsx \
  --report-format csv \
  --output-file ./policy-exports/current-rules.csv
```

## Output Examples

### Sample CSV Output (Default)

```csv
ruleset,scope,type,consumers,providers,services,options,ruleset_url,ruleset_href
Web-to-DB,E-Production,extra,"Web
API","Database",MySQL,,"https://pce.example.com:8443/...",/orgs/1/sec_policy/draft/rule_sets/123
Internal-Access,*ALL LABELS*,intra,All Workloads,All Workloads,"SSH
HTTPS",disabled,"https://pce.example.com:8443/...",/orgs/1/sec_policy/draft/rule_sets/456
```

### Sample with Type Prefixes

```csv
ruleset,scope,type,consumers,providers,services,options,ruleset_url,ruleset_href
Web-to-DB,E-Production,extra,"label:Web
label:API","label:Database","service:MySQL",,"https://pce.example.com:8443/...",/orgs/1/sec_policy/draft/rule_sets/123
DMZ-Access,E-Production,extra,"iplist:External_Partners","label:DMZ-Servers","service:HTTPS",secure-connect,"https://pce.example.com:8443/...",/orgs/1/sec_policy/draft/rule_sets/789
```

### Sample with Section Headers

```
ruleset: Infrastructure-Ruleset
scope: E-Production
consumers:
LABELS:
R-Web
R-API

IPLISTS:
Private_Networks

providers:
LABELS:
R-Database

services: MySQL, HTTPS
```

## Common Workflows

### Policy Documentation

Export rules for compliance documentation:

```bash
pylo rule-export \
  --prefix-objects-with-type \
  --report-format xlsx \
  --output-filename policy-documentation
```

### Rule Review and Audit

Generate comprehensive report for security review:

```bash
pylo rule-export \
  --object-types-as-section \
  --report-format xlsx \
  --output-dir ./audit-reports
```

### Before/After Comparison

1. Export current state:
   ```bash
   pylo rule-export --output-filename rules-before --report-format csv
   ```

2. Make policy changes in PCE

3. Export new state:
   ```bash
   pylo rule-export --output-filename rules-after --report-format csv
   ```

4. Compare using diff tools or spreadsheet comparison

### Automation and Integration

Export in JSON format for automated processing:

```bash
pylo rule-export --report-format json --output-dir /integration/input
```

The JSON format provides a flat array of rule objects suitable for:
- CI/CD pipeline validation
- Automated compliance checking
- Integration with other security tools
- Custom reporting and analytics

## Understanding Scope

### All Labels Scope

When a ruleset has `*ALL LABELS*` as its scope:
- Rules apply to all workloads in the organization
- Typically used for global policies (e.g., management access)
- Use cautiously as changes affect entire environment

### Specific Label Scope

When scope shows specific labels:
- Rules only apply to workloads with those labels
- More granular control
- Better isolation and security

Example scope:
```
E-Production
A-WebApp
```

Rules in this ruleset only affect workloads with both Production environment label AND WebApp application label.

## Tips and Best Practices

1. **Regular Exports**: Export rules regularly for version control and backup

2. **Use Type Prefixes for Clarity**: When sharing exports with others, use `--prefix-objects-with-type` for clarity

3. **Excel for Human Review**: Use XLSX format for manual review and analysis

4. **JSON for Automation**: Use JSON format for programmatic processing

5. **Check Disabled Rules**: Review rules with `disabled` option - they may be outdated

6. **Document Rule URLs**: The `ruleset_url` column provides direct links to rulesets in the PCE UI

7. **Combine with Other Exports**: Cross-reference with workload exports and IP list analysis for complete picture

## Troubleshooting

### Empty Report

**Possible Causes:**
- No rulesets defined in the PCE
- All rulesets are empty
- Connection or permission issues

**Solutions:**
- Verify rulesets exist in PCE UI
- Check API permissions
- Review credential configuration

### Missing Rules

**Possible Causes:**
- Filtering at PCE API level
- Only draft rules are exported (not active policy)

**Solutions:**
- Verify you're looking at the correct policy version (draft vs active)
- Check organizational visibility permissions

### Formatting Issues

**Problem**: Objects not displaying as expected

**Solutions:**
- Try different display options (`--prefix-objects-with-type` or `--object-types-as-section`)
- Check for multi-line formatting in CSV (may need to adjust import settings)
- Use Excel format for better handling of multi-line cells

## Related Commands

- `workload-export` - Export workloads referenced in rules
- `iplist-analyzer` - Analyze IP lists used in rules
- `traffic-export` - Export traffic flows to validate rule effectiveness
- `label-delete-unused` - Clean up unused labels before export
