### 1\. Core Architecture

- **Language:** Go (Single binary transpiler).
- **State:** SQLite (Stores project graph and metadata).
- **Parsing Strategy:** **Single-Pass System**.
  1.  Extracts YAML frontmatter → config (name, materialized, owner, etc.).
  2.  Renders Starlark template → pure SQL.
  3.  Parses SQL with lineage package → extracts table dependencies + column lineage.
  4.  Builds DAG from extracted sources.

---

### 2\. File Structure & Frontmatter

- **Format:** Standard `.sql` files.
- **Header:** **YAML** inside a specific comment block `/*--- ---*/`. Used for **static configuration** (materialization, owner, tags, schema tests).
- **Body:** SQL mixed with Starlark logic.

<!-- end list -->

```sql
/*---
  name: monthly_revenue
  materialized: incremental
  owner: finance
  tests:
    - unique: [order_id]
---*/

SELECT * FROM orders
WHERE created_at > '2024-01-01'
```

---

### 3\. Templating Syntax (Starlark)

We use a **Split Syntax** to separate output from logic, embedding the Starlark (Python dialect) runtime.

- **`{{ ... }}` (Expressions):** Evaluates a Starlark expression and injects the string result.
  - _Example:_ `{{ target.schema }}`, `{{ utils.clean_str(col) }}`
- **`{* ... *}` (Statements):** Executes control flow or variable definition. No output.
  - _Example:_ `{* for col in columns: *}`, `{* if is_prod: *}`

---

### 4\. Macro System

- **Definition:** Pure Starlark code in **`.star`** files (not SQL).
- **Location:** Stored in a `macros/` directory.
- **Auto-Loading (Namespacing):** No `load()` tags required. Files are automatically namespaced by their filename.
  - File `macros/datetime.star` is available globally as the `datetime` object.
  - **Usage:** `{{ datetime.now() }}`.

---

### 5\. Package Management

- **Config:** `packages.yaml` at project root.
- **Source:** **Git-based** (Decentralized). No central hub required.
- **Versioning:** Semantic versioning via Git Tags.
- **Installation:** Clones into `_vendor/` and generates a `packages.lock` file.
- **Usage:** External packages are namespaced by the package name.
  - _Example:_ `{{ dbt_utils.slugify(col) }}`.

<!-- end list -->

```yaml
# packages.yaml
packages:
  - name: dbt_utils
    git: "https://github.com/dbt-labs/dbt-utils-starlark.git"
    version: "v1.0.0"
```

### 6\. Standard Library (Injected Globals)

Your Go binary injects these explicitly into the Starlark context:

- **`config`**: Dictionary containing the parsed YAML Frontmatter.
- **`env`**: String indicating current environment (e.g., "prod", "dev").
- **`target`**: Object containing adapter specifics (e.g., `target.type`, `target.schema`).
- **`this`**: Current model info (e.g., `this.name`, `this.schema`).

**Note:** No `ref()` function - table dependencies are automatically extracted from SQL by the lineage parser.
