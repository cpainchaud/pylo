# AI Agents & Automation

## Development Agents

### Claude Code
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

### Test Organization
- **`tests/` (root)**: Tests for core components only (filters, queries, report writers, credentials, etc.)
- **`tests/cli/`**: CLI commands and integration tests
- This separation ensures core library tests remain distinct from CLI-specific tests

### Important Files
- `test_fixtures.py`: Shared mock classes and test utilities
- Test files follow naming convention: `test_*.py`
- `tests/README_test_fixtures.md`: Documentation for test fixtures and organization
