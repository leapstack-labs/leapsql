#!/bin/bash
# Vendor frontend dependencies for the docs site
# Downloads ESM bundles from esm.sh and stores them in internal/docs/vendor

set -e

VENDOR_DIR="internal/docs/vendor"

# Versions
VERSION_PREACT="10.24.0"
VERSION_XYFLOW="12.10.0"
VERSION_MINISEARCH="7.2.0"
VERSION_DAGRE="0.8.5"
VERSION_HLJS="11.9.0"

# Base URL
BASE="https://esm.sh"

echo "Vendoring frontend dependencies..."

# Create directories
mkdir -p "$VENDOR_DIR/preact"
mkdir -p "$VENDOR_DIR/xyflow"
mkdir -p "$VENDOR_DIR/highlight"

# Preact core and related modules - use bundle-deps to inline everything
echo "Downloading Preact..."
curl -sL "$BASE/preact@$VERSION_PREACT/es2022/preact.mjs" -o "$VENDOR_DIR/preact/preact.mjs"
curl -sL "$BASE/preact@$VERSION_PREACT/es2022/hooks.mjs" -o "$VENDOR_DIR/preact/hooks.mjs"
curl -sL "$BASE/preact@$VERSION_PREACT/es2022/compat.mjs" -o "$VENDOR_DIR/preact/compat.mjs"
curl -sL "$BASE/preact@$VERSION_PREACT/es2022/jsx-runtime.mjs" -o "$VENDOR_DIR/preact/jsx-runtime.mjs"

# React Flow (xyflow) - use bundle-deps to get complete bundle
echo "Downloading React Flow..."
curl -sL "$BASE/@xyflow/react@$VERSION_XYFLOW/es2022/react.bundle.mjs" -o "$VENDOR_DIR/xyflow/react.mjs"
curl -sL "https://cdn.jsdelivr.net/npm/@xyflow/react@$VERSION_XYFLOW/dist/style.css" -o "$VENDOR_DIR/xyflow/style.css"

# Dagre for layout
echo "Downloading Dagre..."
curl -sL "$BASE/dagre@$VERSION_DAGRE/es2022/dagre.bundle.mjs" -o "$VENDOR_DIR/dagre.mjs"

# MiniSearch
echo "Downloading MiniSearch..."
curl -sL "$BASE/minisearch@$VERSION_MINISEARCH/es2022/minisearch.bundle.mjs" -o "$VENDOR_DIR/minisearch.mjs"

# Highlight.js - core and SQL language
echo "Downloading Highlight.js..."
curl -sL "$BASE/highlight.js@$VERSION_HLJS/es2022/lib/core.mjs" -o "$VENDOR_DIR/highlight/core.mjs"
curl -sL "$BASE/highlight.js@$VERSION_HLJS/es2022/lib/languages/sql.mjs" -o "$VENDOR_DIR/highlight/sql.mjs"
curl -sL "https://cdn.jsdelivr.net/npm/highlight.js@$VERSION_HLJS/styles/github-dark.min.css" -o "$VENDOR_DIR/highlight/github-dark.css"

echo "Done! Dependencies vendored to $VENDOR_DIR"
