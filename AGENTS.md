# AI Agents & Automation

## Development Agents

### Coding Agents
- **Purpose**: Primary development assistant for code generation, refactoring, and testing
- **Usage**: Interactive coding sessions, test generation, documentation
- **Access**: Via IDE integration or CLI

## Guidelines

### When to Use Agents
- Writing new test suites
- Refactoring complex code sections
- Generating boilerplate code
- Debugging assistance

### Best Practices
- Provide clear context about the codebase structure
- Reference existing patterns in the project
- Review and test all generated code
- Keep agents informed about project conventions (e.g., testing patterns, mock fixtures)

## Project-Specific Context

### Key Patterns
- Mock classes inherit from real pylo classes (see test_fixtures.py)
- Integration tests use __main() function testing pattern
- Test fixtures are shared across test modules

### Code Folder Organization
- **`/illumio_pylo/`**: Core library code
- **`/illumio_pylo/API/`**: Low level API interactions and data models to communicate with the PCE
- **`/illumio_pylo/cli/`**: CLI command implementations which utilizes core library functions. A specific `/illumio_pylo/cli/README.md` file in this directory provides some information about the CLI/Commands code structure and instructions, you MUST read if you have planning to deal CLI/Command code. Usage documentation for end users for CLI commands is located in the `/docs/cli/` directory.
- **`/tests/`**: Test suites for core library and CLI commands, organized by functionality
- **`/docs/`**: Documentation for the project, including usage guides and API references
- **`/docs/cli`**: Documentation specific to CLI usage and commands

### Tests Organization
- **`tests/` (root)**: Tests for core components only (filters, queries, report writers, credentials, etc.)
- **`tests/cli/`**: CLI commands and integration tests
- This separation ensures core library tests remain distinct from CLI-specific tests

#### Important Files
- `test_fixtures.py`: Shared mock classes and test utilities
- Test files follow naming convention: `test_*.py`
- `tests/README_test_fixtures.md`: Documentation for test fixtures and organization
