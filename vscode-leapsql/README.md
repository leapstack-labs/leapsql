# LeapSQL for VS Code

Language support for LeapSQL - SQL with templating and macros.

## Features

- **Syntax Highlighting**: Full support for SQL, YAML frontmatter, and Starlark template expressions
- **Diagnostics**: Real-time error detection and reporting
- **Completions**: Auto-complete for SQL functions, macros, and template builtins
- **Hover Documentation**: View function signatures and documentation on hover
- **Go-to-Definition**: Jump to macro definitions in `.star` files

## Requirements

- **LeapSQL CLI**: The `leapsql` binary must be installed and available in your PATH

Install LeapSQL:
```bash
go install github.com/leapsql/leapsql/cmd/leapsql@latest
```

## Installation

### From VSIX (Local)

1. Build the extension:
   ```bash
   cd vscode-leapsql
   pnpm install
   pnpm run compile
   pnpm run package
   ```

2. Install in VS Code:
   - Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on macOS)
   - Select "Extensions: Install from VSIX..."
   - Choose the generated `.vsix` file

### Development Mode

1. Open the `vscode-leapsql` folder in VS Code
2. Press `F5` to launch the Extension Development Host
3. Open a LeapSQL project to test

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `leapsql.path` | `leapsql` | Path to the leapsql executable |
| `leapsql.trace.server` | `off` | LSP trace level (`off`, `messages`, `verbose`) |

## Supported File Types

- `.sql` files in `models/` directories
- Projects containing `models/**/*.sql` or `macros/**/*.star`

## Syntax Features

### Frontmatter

```sql
/*---
materialized: table
schema: staging
---*/
SELECT * FROM users
```

### Template Expressions

```sql
SELECT * FROM {{ ref("users") }}

{* if config.get("include_deleted") *}
WHERE deleted_at IS NULL
{* endif *}
```

### Template Builtins

- `ref(model_name)` - Reference another model
- `source(source_name, table_name)` - Reference a source table
- `config` - Access model configuration
- `this` - Current model reference
- `env` - Environment variables
- `var(name, default)` - Project variables

## Troubleshooting

### Language server not starting

1. Verify `leapsql` is installed: `which leapsql`
2. Check the Output panel (View → Output → LeapSQL)
3. Enable trace logging: Set `leapsql.trace.server` to `verbose`

### No syntax highlighting

Ensure you're in a LeapSQL project (has `models/` directory with `.sql` files).

## License

MIT
