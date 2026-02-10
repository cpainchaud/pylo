This is a README for developers, it is not meant to be read by end users. It provides an overview of the CLI code structure and organization for developers and agents working on the project.

# CLI Code Structure Overview

- `__init__.py`: this file contains the entry point for the CLI application. It sets up the command-line interface and dispatches commands to the appropriate handlers.
- `__main__.py`: only purpose is to allow the CLI to be run with `python -m illumio_pylo.cli` command. It imports the `run()` function from `__init__.py` and executes it.
- `commands/`: this directory contains the implementation of individual CLI commands. Each command is implemented in its file or grouped logically (e.g., `reporting_commands.py` for all reporting related commands). Each command file defines a function that implements the command's functionality and is registered in the CLI entry point.
- `commands/utils/`: this subdirectory contains utility functions that are used by multiple CLI commands. These utilities help with common tasks such as formatting output, handling user input, or interacting with the core library.
- `commands/ui/`: this subdirectory contains functions related to user interface elements of the CLI, such as displaying tables, progress bars, or interactive prompts. These functions are used by the command implementations to enhance the user experience.
- `web_static/`: static files for the experimental web UI.

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

# Instructions for Developers and Agents
- When adding a new CLI command, create a new file in the `commands/` directory or add it to an existing command file if it fits logically. Implement the command's functionality in a function and ensure it is registered in the CLI entry point.
- If the new command requires utility functions that may be used by other commands, add those functions to the `commands/utils/` directory.
- For any user interface elements needed by the new command, implement those in the `commands/ui/` directory to keep UI-related code organized and reusable.
- When creating or updating a Command, ensure that tests and documentation are also created or updated accordingly.
- ReportWriter class should be the default way to write reports for CLI commands, and any new report types should be added as methods to this class. This ensures consistency in report generation across all CLI commands.
- Propose to update this README file with any relevant information that you think is missing or could be useful for developers working on the CLI codebase. This may include details about coding conventions, testing practices, or any specific patterns used in the CLI implementation.
- When writing tests for CLI commands, ensure that you cover both the command's functionality and its interaction with the core library. Use mock objects to isolate the command's behavior and test it independently from the core library's implementation. Additionally, consider testing edge cases and error handling to ensure robustness of the CLI commands. You MUST TEST the __main() function of the Command.
