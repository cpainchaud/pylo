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
- Refactor CLI commands by extracting business logic into pure, testable functions
- Keep print statements in __main() for user experience while making logic testable
- Use structured return values (dicts/tuples) from extracted functions for easy assertions

### Code Folder Organization
- **`/illumio_pylo/`**: Core library code
- **`/illumio_pylo/API/`**: Low level API interactions and data models to communicate with the PCE
- **`/illumio_pylo/cli/`**: CLI command implementations which utilizes core library functions. A specific `/illumio_pylo/cli/README.md` file in this directory provides some information about the CLI/Commands + its UI code structure and instructions, you MUST read if you have planning to deal CLI/Command code. Usage documentation for end users for CLI commands is located in the `/docs/cli/` directory.
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
- `tests/cli/README_*_tests.md`: Command-specific test documentation (created for each command with comprehensive tests)

### Testing Patterns for CLI Commands

#### Test File Structure
Each CLI command should have two test files:
1. **Unit tests** (`test_<command_name>.py`): Test individual functions in isolation
2. **Integration tests** (`test_<command_name>_integration.py`): Test the full __main() function flow

#### Refactoring for Testability
When preparing a CLI command for testing:

1. **Extract Pure Functions**:
   - Move business logic out of __main() into separate functions
   - Pure functions take simple inputs (dicts, lists) and return structured outputs
   - Examples: data filtering, transformation, validation logic

2. **Isolate External Dependencies**:
   - Extract API-dependent logic into separate functions
   - Pass connector/org as parameters rather than accessing globally
   - Examples: URL building, API calls, deletion operations

3. **Preserve User Experience**:
   - Keep all print statements in __main() for CLI output
   - Don't remove user-facing messages during refactoring
   - Extracted functions should focus on logic, not presentation

4. **Use Structured Returns**:
   - Return dicts with named keys for complex results (e.g., `{'successful': [], 'failed': [], 'errors': {}}`)
   - Return tuples for simple multi-value results (e.g., `(to_delete, ignored)`)
   - Makes assertions clear and maintainable

#### Unit Test Patterns
- Test pure functions with simple mock objects or plain data structures
- Use lightweight mocks (MockSheet, MockAPIConnector) defined in test file
- Focus on edge cases: empty inputs, None values, boundary conditions
- No need for test_fixtures.py mocks if testing pure functions

#### Integration Test Patterns
- Test __main() function with complete mock environment
- Create helper functions to build mock connectors with test data
- Use temporary directories (tempfile.TemporaryDirectory) for output files
- Verify both API behavior (deletions, updates) and report generation (CSV/JSON)
- Test all command flags and parameter combinations
- Test error scenarios (API failures, invalid inputs)

#### Test Documentation
Create a `README_<command_name>_tests.md` file documenting:
- What each test file tests
- How to run the tests
- Test coverage summary
- Mock objects used
- Special patterns or refactoring notes

#### Reference Examples
Best practice examples to reference when creating new tests:
- **Command**: `illumio_pylo/cli/commands/label_delete_unused.py`
  - **Unit tests**: `tests/cli/test_label_delete_unused.py`
  - **Integration tests**: `tests/cli/test_label_delete_unused_integration.py`
  - **Documentation**: `tests/cli/README_label_delete_unused_tests.md`
  - **Patterns demonstrated**: Pure function extraction, structured returns, mock API connector, comprehensive test coverage

- **Command**: `illumio_pylo/cli/commands/traffic_export.py`
  - **Unit tests**: `tests/cli/test_traffic_export.py`
  - **Integration tests**: `tests/cli/test_traffic_export_integration.py`
  - **Patterns demonstrated**: Complex data transformation, filter parsing, multiple output formats

- **Command**: `illumio_pylo/cli/commands/workload_export.py`
  - **Unit tests**: `tests/cli/test_workload_export.py`
  - **Integration tests**: `tests/cli/test_workload_export_integration.py`
  - **Documentation**: `tests/cli/README_workload_export_tests.md`
  - **Patterns demonstrated**: Filter matching, extensibility patterns, report generation
