This is a README for developers, it is not meant to be read by end users. It provides an overview of the CLI code structure and organization for developers and agents working on the project.

# CLI Code Structure Overview

- `__init__.py`: this file contains the entry point for the CLI application. It sets up the command-line interface and dispatches commands to the appropriate handlers.
- `__main__.py`: only purpose is to allow the CLI to be run with `python -m illumio_pylo.cli` command. It imports the `run()` function from `__init__.py` and executes it.
- `commands/`: this directory contains the implementation of individual CLI commands. Each command is implemented in its file or grouped logically (e.g., `reporting_commands.py` for all reporting related commands). Each command file defines a function that implements the command's functionality and is registered in the CLI entry point.
- `commands/utils/`: this subdirectory contains utility functions that are used by multiple CLI commands. These utilities help with common tasks such as formatting output, handling user input, or interacting with the core library.
- `commands/ui/`: this subdirectory contains functions related to user interface elements of the CLI, such as displaying tables, progress bars, or interactive prompts. These functions are used by the command implementations to enhance the user experience.
- `web_static/`: static files for global web UI

# Web UI (--web)

A minimal experimental web UI is available via the `--web` flag. When starting the CLI with `--web`, the CLI will start a small FastAPI + Uvicorn server that serves a single-page app and a small set of JSON endpoints to query available commands, credential profiles and run commands.

Usage (developer machine):

- Install the required web dependencies:

```powershell
pip install fastapi uvicorn
```

- Start the web UI on localhost (default port 8000):

```powershell
python -m illumio_pylo.cli --web
```

- Open http://127.0.0.1:8000/ in your browser.

Notes:
- This is an experimental, lightweight implementation intended for local development. It executes commands synchronously and captures stdout/stderr. It is not hardened for production use.
- By default the server binds to 127.0.0.1:8000. Do not expose this process on public networks without adding authentication and TLS.
- **Cache Control**: The server disables caching for all responses to ensure you always get the latest changes during development. Cache busting is implemented via HTTP headers.

## Web UI Architecture & Workflow

The Web UI is a single-page application (SPA) with two main panels and a clear workflow for command execution.

### File Structure

- `index.html`: HTML structure with two panels (landing and parameters)
- `app.js`: JavaScript application logic (725 lines)
- `styles.css`: Styling and layout (359 lines)

### Architecture Overview

```
User Browser
    ↓
index.html (static SPA)
    ├─ panel-landing (command selection)
    └─ panel-params (command execution)
        ├─ command-form (dynamic argument cards)
        ├─ cli-preview (readonly CLI command display)
        └─ logs (command output)
    ↓
app.js (DOM manipulation & event handling)
    ├─ Fetches /api/commands (available commands list)
    ├─ Fetches /api/credentials (PCE profiles list)
    ├─ Fetches /api/commands/{name} (command metadata)
    └─ POSTs to /api/run (execute command)
    ↓
FastAPI Backend
    ├─ /api/commands (JSON list of all commands)
    ├─ /api/credentials (JSON list of PCE profiles)
    ├─ /api/commands/{name} (JSON command metadata & arguments)
    └─ /api/run (POST endpoint to execute command)
```

### User Workflow

1. **Landing Panel** - User sees available commands
   - Displays command cards in a grid layout
   - Search box to filter commands by name
   - Click on a command card to proceed to execution

2. **Parameters Panel** - User configures and runs command
   - Displays back button and command name
   - PCE Configuration section (credential selection)
   - Argument cards in responsive grid (2 columns on desktop, 1 on mobile)
   - CLI preview showing the equivalent command-line
   - Logs area showing command output
   - Hover tooltips on disabled fields hint how to override defaults

3. **Execution & Results**
   - Form submission triggers command execution
   - Output captured and displayed in logs area
   - User can copy the CLI preview command
   - Back button returns to landing panel

### Panel System

Two main panels controlled by CSS classes and body element state:

**Landing Panel** (`#panel-landing`):
- Shown when `body` does NOT have `params-visible` class
- Contains command search and card grid
- Displayed on page load

**Parameters Panel** (`#panel-params`):
- Shown when `body` has `params-visible` class
- Contains command form, CLI preview, and logs
- Hidden by default (`hidden` attribute + `hidden` class)

Panel switching functions:
- `showPanel('landing')` - Removes `params-visible` class, hides params panel
- `showPanel('params')` - Adds `params-visible` class, shows params panel
- Triggered by command card click or back button

### Command Form Layout Design

The command form uses a **responsive 2-column grid** for argument organization:

```
┌─ PCE Configuration ────────────────────────┐
│ [Dropdown for PCE profile selection]       │
└────────────────────────────────────────────┘

┌──────────────────────┐  ┌──────────────────────┐
│ Argument 1           │  │ Argument 2           │
│ [Input] Help text    │  │ [Input] Help text    │
└──────────────────────┘  └──────────────────────┘

┌──────────────────────┐  ┌──────────────────────┐
│ Argument 3           │  │ Argument 4           │
│ [Dropdown] Help text │  │ [Checkbox] Help text │
└──────────────────────┘  └──────────────────────┘

[RUN]
```

**Grid Styling Details:**
- CSS Grid: `grid-template-columns: repeat(auto-fit, minmax(320px, 1fr))`
- Minimum card width: 320px
- Gap between cards: 12px
- Card padding: 12px
- Responsive: 1 column on mobile, 2 on tablet/desktop, 3+ on wide screens

**Form Elements Structure (HTML Generated by JavaScript):**

```html
<div class="form-section">
  <h3 class="form-section-title">PCE CONFIGURATION</h3>
  <div class="form-section-content">
    <label>PCE profile (if required):</label>
    <select name="pce">...</select>
  </div>
</div>

<div class="arguments-grid">
  <div class="argument-card">
    <label class="argument-label">argument_name</label>
    <div class="argument-control">
      <input type="text" name="argument_name" />
    </div>
    <p class="argument-help">Help text for the argument</p>
  </div>
  <!-- More argument cards... -->
</div>

<button type="submit">RUN</button>
```

### Key JavaScript Functions

**Core Application Functions:**

- `selectCommand(name)` - Fetches command metadata and dynamically builds the form
  - Creates PCE Configuration section
  - Creates responsive grid of argument cards
  - Sets up event listeners for form inputs
  - Initializes CLI preview

- `runCommand(name, form, meta)` - Executes the selected command
  - Collects form data into JSON payload
  - POSTs to /api/run endpoint
  - Displays output in logs area
  - Handles PCE selection (dropdown or manual input)

- `updateCliPreview(form, meta)` - Generates CLI command preview
  - Reads current form values
  - Constructs equivalent CLI command
  - Updates readonly textarea with preview
  - Only includes arguments that differ from defaults

- `showPanel(name)` - Switches between landing and parameters panels
  - Toggles `params-visible` class on body
  - Manages focus (search box vs back button)
  - Updates command name display

**Form Input Handling:**

- `selectCommand()` creates appropriate controls based on argument type:
  - **Text inputs**: Basic text input with optional null checkbox
    - **Default Value Display**: When a string parameter has a default value, shows placeholder like `"default_value <click to edit>"` instead of the actual value
    - **Focus Behavior**: Clicking the input replaces placeholder with actual default value
    - **Blur Behavior**: If unchanged, restores placeholder on blur
  - **Number inputs**: Number input with optional null checkbox
  - **Dropdowns**: Select element for arguments with predefined choices
  - **Checkboxes**: Boolean flags with hidden fallback for unchecked state
    - **Inline Label**: Checkbox now has inline label with parameter name
    - **Layout**: Checkbox appears before label on same horizontal line
    - **Click Behavior**: Clicking label text toggles checkbox

- **Null Checkbox Pattern**: For arguments with default value of `null`:
  - Creates checkbox with name `{arg_name}__isnull`
  - When checked: input is disabled, shows hint tooltip on hover
  - When unchecked: input is enabled, user can enter custom value
  - Tooltip text: "Uncheck the null checkbox to enable this field and provide a custom value"

**Event Delegation:**

- Form-level `input` and `change` event listeners update CLI preview in real-time
- Individual input listeners for special cases (null checkbox toggle, select change, etc.)
- Back button wired via `wireBackButton()` function
- Escape key closes params panel via `attachEscapeHandler()` function

### Styling System

**CSS Variables (Light & Dark Theme):**

```css
--bg                    /* Main background */
--text-primary          /* Primary text color */
--muted-text            /* Muted/secondary text */
--border                /* Border color */
--card-bg               /* Card background */
--card-border           /* Card border color */
--card-shadow           /* Card shadow */
--input-bg              /* Input field background */
--input-border          /* Input field border */
--focus-ring            /* Focus state color */
--button-bg             /* Button background */
--button-border         /* Button border */
--button-text           /* Button text color */
--log-bg                /* Log panel background */
--cli-bg                /* CLI preview background */
--status-text           /* Status message color */
```

**Key CSS Classes:**

- `.form-section` - PCE Configuration section container
- `.arguments-grid` - Responsive grid container for argument cards
- `.argument-card` - Individual argument card with border and padding
- `.argument-label` - Argument name label (bold)
- `.argument-control` - Control wrapper (input, select, checkbox)
- `.argument-help` - Help text (muted, smaller font)
- `.checkbox-container` - Flex container for boolean checkboxes with inline labels
- `body.params-visible` - State class for showing params panel

**Responsive Breakpoints:**

- **Mobile** (< 640px): 1 column, full-width cards
- **Tablet** (640px - 1000px): 2 columns
- **Desktop** (> 1000px): 2-3 columns with optimal spacing

### Cache Control & Development

The web server implements cache busting via HTTP headers to ensure developers always see the latest changes:

**HTTP Headers:**
- `Cache-Control: no-cache, no-store, must-revalidate`
- `Pragma: no-cache`
- `Expires: 0`

**Implementation:**
- Global middleware applies cache control headers to all responses
- Clean approach using only HTTP headers (no compression, no query parameters)

**Development Workflow:**
- No manual cache clearing needed during development
- Changes to CSS/JS are immediately visible on page reload
- API responses are never cached

**Troubleshooting:**
If you still see cached content:
1. Try Ctrl+F5 (hard refresh) in your browser
2. Open browser in incognito/private mode
3. Clear browser cache manually
4. Restart the development server

### API Endpoints (Expected Backend)

**GET /api/commands**
- Returns JSON array of available commands
- Used to populate landing panel command cards
- Response format: `[{ name: string, description: string }, ...]`

**GET /api/credentials**
- Returns JSON array of configured PCE profiles
- Used to populate PCE profile dropdown
- Response format: `[{ name: string, fqdn: string }, ...]`

**GET /api/commands/{name}**
- Returns metadata for a specific command
- Includes command description and all arguments
- Response format includes:
  ```json
  {
    "name": "command_name",
    "description": "...",
    "arguments": [
      {
        "dest": "arg_name",
        "type": "str|int|float|bool",
        "default": null|value,
        "help": "Description of argument",
        "choices": ["option1", "option2"],
        "option_strings": ["-x", "--arg-name"]
      }
    ]
  }
  ```

**POST /api/run**
- Executes a command with provided arguments
- Request body:
  ```json
  {
    "command": "command_name",
    "pce": "profile_name",
    "args": { "arg_name": value, ... }
  }
  ```
- Response: `{ "stdout": "...", "stderr": "..." }`

### Development Guidelines for Agents

When modifying the Web UI, keep these principles in mind:

1. **Maintain Panel-Based Navigation**
   - Landing panel for discovery
   - Parameters panel for execution
   - Use `showPanel()` function to switch
   - Always provide a back button on parameters panel

2. **Form Generation**
   - Always use the grid card layout for arguments
   - Create cards dynamically in `selectCommand()`
   - Apply appropriate control type based on argument metadata
   - Include help text from argument description

3. **CLI Preview**
   - Keep the readonly CLI preview updated in real-time
   - Use `updateCliPreview()` function
   - Only show arguments that differ from defaults
   - Help users understand the equivalent command-line

4. **Accessibility & UX**
   - Add hover tooltips for disabled fields (use HTML `title` attribute)
   - Provide clear visual feedback (focus states, hover effects)
   - Ensure keyboard navigation (Tab, Enter, Escape)
   - Support both light and dark themes via CSS variables

5. **Responsive Design**
   - Test on mobile, tablet, and desktop
   - Use CSS Grid's `auto-fit` with `minmax()` for flexible layouts
   - Ensure touch-friendly spacing and button sizes
   - Don't hardcode widths; use relative units and flexbox/grid

6. **File Organization**
   - Keep HTML structure minimal in `index.html`
   - Implement logic in `app.js` with clear function names
   - Use `styles.css` exclusively for styling
   - Avoid inline styles or event handlers

7. **Performance Considerations**
   - Fetch credentials and command list asynchronously
   - Use event delegation for form inputs when possible
   - Cache command metadata after fetching
   - Capture command output to logs without DOM flooding

8. **Testing**
   - Test all argument types (text, number, select, checkbox, null)
   - Verify CLI preview accuracy
   - Test panel navigation (landing ↔ parameters)
   - Verify theme switching (light ↔ dark mode)
   - Test on different screen sizes

# Instructions for Developers and Agents
- When adding a new CLI command, create a new file in the `commands/` directory or add it to an existing command file if it fits logically. Implement the command's functionality in a function and ensure it is registered in the CLI entry point.
- If the new command requires utility functions that may be used by other commands, add those functions to the `commands/utils/` directory.
- When creating or updating a Command, ensure that tests and documentation are also created or updated accordingly.
- When changing updates are made to the UI, ensure this README file is updated with any relevant information about the changes, such as new UI elements, changes to existing elements, or updates to the user workflow.
- ReportWriter class should be the default way to write reports for CLI commands, and any new report types should be added as methods to this class. This ensures consistency in report generation across all CLI commands.
- Propose to update this README file with any relevant information that you think is missing or could be useful for developers working on the CLI codebase. This may include details about coding conventions, testing practices, or any specific patterns used in the CLI implementation.
- When writing tests for CLI commands, ensure that you cover both the command's functionality and its interaction with the core library. Use mock objects to isolate the command's behavior and test it independently from the core library's implementation. Additionally, consider testing edge cases and error handling to ensure robustness of the CLI commands. You MUST TEST the __main() function of the Command.
