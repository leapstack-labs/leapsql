# Agent Guidelines

Instructions for AI agents working on this codebase.

## Test Strategy

Use progressive disclosure when running tests - start with minimal output and drill down only when needed.

### Test Commands (Taskfile)

| Command                        | When to Use                                                          |
| ------------------------------ | -------------------------------------------------------------------- |
| `task test`                    | Default. Package-level summary with inline failures. Use this first. |
| `task test:pkg PKG=./path/...` | Focus on a specific package after finding failures                   |
| `task test:watch`              | During development, auto-reruns on file changes                      |
| `task test:all`                | Includes integration tests (slower)                                  |

### Workflow

1. **Start broad**: Run `task test` to get package-level pass/fail
2. **Identify failures**: Failed packages show test names and output inline
3. **Drill down**: Use `task test:pkg PKG=./internal/foo/...` for specific package
4. **Fix and verify**: Run `task test` again to confirm all green

### Writing Tests

- Use table-driven tests with `testify/require` and `testify/assert`
- `require`: Fatal assertions - use for setup and preconditions
- `assert`: Non-fatal assertions - use for verifications
- Structure test cases with `setup`, `operation`, and `verify` funcs as needed

## Linting

Run `task lint`.

## Combined Check

Run `task check` to execute both tests and linting in sequence.
