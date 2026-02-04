# Traffic Export Command

## Overview

The `traffic-export` command exports traffic records from the Illumio Policy Compute Engine (PCE) based on specified filters and settings. It provides flexible options for filtering, formatting, and customizing the exported data.

## Command Syntax

```bash
pylo traffic-export [OPTIONS]
```

## Options

### Output Format Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--format` | `-f` | choice | `excel` | Output file format (choices: `csv`, `excel`) |
| `--output-dir` | `-o` | string | `output` | Directory where to save the output file |

### Filter Options

#### Source and Destination Filters

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--source-filters` | `-sf` | list | None | Source filters to apply (e.g., `label:Web`, `iplist:Private_Networks`) |
| `--destination-filters` | `-df` | list | None | Destination filters to apply (e.g., `label:DB`, `iplist:Public_NATed`) |

**Filter Format:**
- `label:<label_name>` - Filter by label name
- `iplist:<iplist_name>` - Filter by IP list name
- Multiple filters can be comma-separated
- Case-insensitive label matching

**Examples:**
```bash
--source-filters "label:Web,label:API" "iplist:Private_Networks"
--destination-filters "label:DB"
```

#### Time Range Filters

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--since-timestamp` | `-st` | string | None | Export traffic records since this timestamp (ISO 8601 format) |
| `--until-timestamp` | `-ut` | string | None | Export traffic records until this timestamp (ISO 8601 format) |
| `--timeframe-hours` | `-tfh` | integer | None | Export traffic records from the last X hours (overrides timestamp options) |

**Notes:**
- Either `--since-timestamp` or `--timeframe-hours` must be provided
- `--timeframe-hours` cannot be used together with `--since-timestamp` or `--until-timestamp`
- Timestamps must be in ISO 8601 format (e.g., `2024-01-15T10:30:00`)

### Data Processing Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--records-count-limit` | `-rl` | integer | `10000` | Maximum number of records to export |
| `--draft-mode-enabled` | `-dme` | flag | `false` | Enable draft mode to recalculate policy decisions based on draft rules |
| `--protocol-names` | `-pn` | flag | `false` | Translate common protocol numbers to names (e.g., 6 → TCP) |
| `--timezone` | `-tz` | string | None | Convert timestamps to this timezone (e.g., `America/New_York`, `Europe/Paris`). If not specified, timestamps remain in UTC |

**Protocol Translation:**
When `--protocol-names` is enabled, the following protocols are translated:
- `1` → `ICMP`
- `6` → `TCP`
- `17` → `UDP`
- `50` → `ESP`
- `51` → `AH`
- `132` → `SCTP`

### Label Formatting Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--consolidate-labels` | `-cl` | flag | `false` | Consolidate all workload labels into a single column (`src_labels`, `dst_labels`) as comma-separated values, ordered by label types |
| `--label-separator` | `-ls` | string | `,` | Separator to use when consolidating labels (only applies when `--consolidate-labels` is enabled). Examples: `", "`, `" "`, `"|"`, `";"` |

### Column Customization Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--omit-columns` | `-oc` | list | None | Column names to omit from the export |
| `--disable-wrap-text` | `-dwt` | flag | `false` | Disable text wrapping for all report columns (enabled by default) |

**Available Column Names:**

**Base Columns:**
- `src_ip` - Source IP address
- `src_iplist` - Source IP list name(s)
- `src_workload` - Source workload hostname
- `dst_ip` - Destination IP address
- `dst_iplist` - Destination IP list name(s)
- `dst_workload` - Destination workload hostname
- `protocol` - Network protocol
- `port` - Service port
- `policy_decision` - Policy decision for the traffic
- `draft_policy_decision` - Draft policy decision (only available when draft mode is enabled)
- `first_detected` - First detection timestamp
- `last_detected` - Last detection timestamp

**Dynamic Label Columns:**
- When `--consolidate-labels` is **not** used: `src_<label_type>`, `dst_<label_type>` (e.g., `src_app`, `dst_env`)
- When `--consolidate-labels` is used: `src_labels`, `dst_labels`

## Output

The command generates a file with the following naming convention:
```
traffic-export_<timestamp>.<extension>
```

### Output Columns

The output file includes columns in the following order:
1. Source information (IP, IPList, Workload)
2. Source labels (individual or consolidated)
3. Destination information (IP, IPList, Workload)
4. Destination labels (individual or consolidated)
5. Service information (Protocol, Port)
6. Policy decisions
7. Time information (First/Last detected)

## Examples

### Basic Export

Export traffic from the last 24 hours:
```bash
pylo traffic-export --timeframe-hours 24
```

### Export with Source and Destination Filters

Export traffic from Web servers to DB servers in the last week:
```bash
pylo traffic-export \
  --timeframe-hours 168 \
  --source-filters "label:Web" \
  --destination-filters "label:DB"
```

### Export with Timestamp Range

Export traffic between specific dates:
```bash
pylo traffic-export \
  --since-timestamp "2024-01-01T00:00:00" \
  --until-timestamp "2024-01-31T23:59:59"
```

### Export with Draft Mode and Protocol Names

Export with draft policy decisions and readable protocol names:
```bash
pylo traffic-export \
  --timeframe-hours 24 \
  --draft-mode-enabled \
  --protocol-names
```

### Export with Consolidated Labels

Export with all labels in single columns:
```bash
pylo traffic-export \
  --timeframe-hours 24 \
  --consolidate-labels \
  --label-separator " | "
```

### Export with Timezone Conversion

Export with timestamps converted to a specific timezone:
```bash
pylo traffic-export \
  --timeframe-hours 24 \
  --timezone "America/New_York"
```

### Export with Custom Column Selection

Export only specific columns:
```bash
pylo traffic-export \
  --timeframe-hours 24 \
  --omit-columns src_iplist dst_iplist draft_policy_decision
```

### Export to CSV

Export to CSV format instead of Excel:
```bash
pylo traffic-export \
  --timeframe-hours 24 \
  --format csv \
  --output-dir ./reports
```

### Complex Export with Multiple Options

Export with multiple filters and custom formatting:
```bash
pylo traffic-export \
  --timeframe-hours 72 \
  --source-filters "label:Web,label:API" "iplist:Private_Networks" \
  --destination-filters "label:DB" \
  --protocol-names \
  --consolidate-labels \
  --label-separator ", " \
  --timezone "Europe/Paris" \
  --format excel \
  --output-dir ./exports \
  --records-count-limit 50000
```

## Required Objects

The command automatically loads the following object types from the PCE:
- Labels
- Label Groups
- IP Lists
- Services

## Error Handling

The command will raise errors in the following cases:
- Label not found in PCE
- Multiple labels found with the same name (use more specific name or enable case sensitivity)
- IP List not found in PCE
- Invalid filter format
- Invalid timestamp format
- `--timeframe-hours` used together with `--since-timestamp` or `--until-timestamp`
- Neither `--since-timestamp` nor `--timeframe-hours` provided
- Invalid timezone specified
- Invalid column names in `--omit-columns`
- All columns omitted (at least one must remain)

## Notes

- If no traffic records match the filters, no file will be exported
- The output directory is created automatically if it doesn't exist
- Text wrapping is enabled by default for better readability in Excel
- IP lists are formatted as comma-separated names, sorted alphabetically (case-insensitive)
- When multiple IP lists match, all names are included
- Draft policy decisions are only included when draft mode is enabled
