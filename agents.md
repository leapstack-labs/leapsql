# Agent Guidelines

Instructions for AI agents working on this codebase.

## Test Strategy

Use progressive disclosure when running tests - start with minimal output and drill down only when needed.

### Test Commands (Taskfile)

| Command                        | When to Use                                                          |
| ------------------------------ | -------------------------------------------------------------------- |
| `task test`                    | Default. Package-level summary with inline failures. Use this first. |
| `task test:names`              | When you need to see individual test names                           |
| `task test:pkg PKG=./path/...` | Focus on a specific package after finding failures                   |
| `task test:watch`              | During development, auto-reruns on file changes                      |
| `task test:verbose`            | Last resort - full verbose output                                    |
| `task test:go`                 | Standard `go test` without gotestsum                                 |

### Workflow

1. **Start broad**: Run `task test` to get package-level pass/fail
2. **Identify failures**: Failed packages show test names and output inline
3. **Drill down**: Use `task test:pkg PKG=./internal/foo/...` for specific package
4. **Fix and verify**: Run `task test` again to confirm all green
