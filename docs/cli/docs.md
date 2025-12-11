---
title: docs
description: Generate and serve documentation for your models
---

# docs

The `docs` command generates a static documentation site for your LeapSQL project, displaying model information, dependencies, and SQL code.

## Usage

```bash
leapsql docs <subcommand> [options]
```

## Subcommands

### build

Generate a static documentation site.

```bash
leapsql docs build [options]
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `-models` | `models` | Path to models directory |
| `-output` | `./docs-site` | Output directory for generated site |
| `-project` | `LeapSQL Project` | Project name for documentation |

**Example:**

```bash
leapsql docs build -output ./documentation -project "My Data Warehouse"
```

### serve

Build and serve documentation locally with a web server.

```bash
leapsql docs serve [options]
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `-models` | `models` | Path to models directory |
| `-output` | `./.leapsql-docs` | Output directory for generated site |
| `-project` | `LeapSQL Project` | Project name for documentation |
| `-port` | `8080` | Port to serve on |

**Example:**

```bash
leapsql docs serve -port 3000
```

## Examples

### Generate static documentation

```bash
leapsql docs build
```

Output:
```
Building documentation...
  Models:  models
  Output:  ./docs-site
  Project: LeapSQL Project

Documentation generated successfully!
Open ./docs-site/index.html in your browser
```

### Serve documentation locally

```bash
leapsql docs serve
```

Output:
```
Building documentation...
  Models:  models
  Project: LeapSQL Project

Serving documentation at http://localhost:8080
Press Ctrl+C to stop
```

### Custom project name and port

```bash
leapsql docs serve -project "Analytics Warehouse" -port 9000
```

## Generated Content

The documentation site includes:

### Model Overview

- List of all models with materialization types
- Execution order and dependency count
- Path to source file

### Dependency Graph

- Visual representation of model relationships
- Interactive navigation between related models

### Model Details

For each model:
- Full SQL code with syntax highlighting
- Frontmatter configuration
- Upstream dependencies (models this depends on)
- Downstream dependents (models that depend on this)
- Column information (when available)

## Deployment

The `build` command generates static HTML, CSS, and JavaScript files that can be deployed to any static hosting service:

- GitHub Pages
- Netlify
- Vercel
- S3 + CloudFront
- Any web server

```bash
# Build for production
leapsql docs build -output ./public

# Deploy to your hosting service
# e.g., for Netlify: netlify deploy --prod --dir=./public
```

## Use Cases

### Team onboarding

Generate documentation to help new team members understand the data model.

### Data discovery

Provide analysts with a searchable catalog of available models and their purposes.

### Impact analysis

Use the dependency graph to understand the impact of changes to a model.

### Audit and compliance

Document your data transformations for compliance requirements.
