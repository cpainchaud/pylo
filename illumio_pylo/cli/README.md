This is a README for developers, it is not meant to be read by end users. It provides an overview of the CLI code structure and organization for developers and agents working on the project.

# CLI Code Structure Overview

- `__init__.py`: this file contains the entry point for the CLI application. It sets up the command-line interface and dispatches commands to the appropriate handlers.
- `__main__.py`: only purpose is to allow the CLI to be run with `python -m illumio_pylo.cli` command. It imports the `run()` function from `__init__.py` and executes it.
- `commands/`: this directory contains the implementation of individual CLI commands. Each command is implemented in its file or grouped logically (e.g., `reporting_commands.py` for all reporting related commands). Each command file defines a function that implements the command's functionality and is registered in the CLI entry point.
- `commands/utils/`: this subdirectory contains utility functions that are used by multiple CLI commands. These utilities help with common tasks such as formatting output, handling user input, or interacting with the core library.
- `commands/ui/`: this subdirectory contains functions related to user interface elements of the CLI, such as displaying tables, progress bars, or interactive prompts. These functions are used by the command implementations to enhance the user experience.