import { LitElement } from "lit";
import { customElement, property } from "lit/decorators.js";
import { EditorView, basicSetup } from "codemirror";
import { sql, SQLite } from "@codemirror/lang-sql";
import { keymap } from "@codemirror/view";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags as t } from "@lezer/highlight";

/**
 * <sql-editor> - CodeMirror 6 SQL editor with Datastar integration
 *
 * Emits custom events for Datastar signal binding:
 * - editor-change: Fired when content changes, detail is the new content string
 * - editor-execute: Fired when user presses Cmd/Ctrl+Enter
 *
 * @example
 * <sql-editor
 *   initial-value="SELECT * FROM models"
 *   placeholder="Enter SQL query..."
 *   data-on:editor-change="$sql = evt.detail"
 *   data-on:editor-execute="@post('/api/query/execute')"
 * ></sql-editor>
 */
@customElement("sql-editor")
export class SqlEditor extends LitElement {
  /** Initial content for the editor */
  @property({ attribute: "initial-value" })
  initialValue = "";

  /** Placeholder text when empty */
  @property()
  placeholder = "SELECT * FROM models LIMIT 10;";

  /** Whether the editor is read-only */
  @property({ type: Boolean, attribute: "read-only" })
  readOnly = false;

  private _editor: EditorView | null = null;
  private _container: HTMLDivElement | null = null;

  // Light DOM - inherit global CSS variables
  override createRenderRoot() {
    return this;
  }

  override connectedCallback() {
    super.connectedCallback();
    requestAnimationFrame(() => this._initEditor());
  }

  override disconnectedCallback() {
    super.disconnectedCallback();
    this._editor?.destroy();
    this._editor = null;
  }

  /** Get current editor content */
  getValue(): string {
    return this._editor?.state.doc.toString() ?? "";
  }

  /** Set editor content programmatically */
  setValue(value: string): void {
    if (!this._editor) return;
    this._editor.dispatch({
      changes: { from: 0, to: this._editor.state.doc.length, insert: value },
    });
  }

  /** Clear editor content */
  clear(): void {
    this.setValue("");
  }

  /** Focus the editor */
  override focus(): void {
    this._editor?.focus();
  }

  private _initEditor(): void {
    if (this._editor) return; // Already initialized

    if (!this._container) {
      this._container = document.createElement("div");
      this._container.className = "sql-editor__container";
      this.appendChild(this._container);
    }

    // Theme using CSS variables from existing theme system
    const theme = EditorView.theme({
      "&": {
        backgroundColor: "var(--card)",
        color: "var(--foreground)",
        height: "100%",
        minHeight: "180px",
        maxHeight: "300px",
      },
      ".cm-content": {
        fontFamily: "var(--font-mono)",
        fontSize: "var(--text-sm)",
        padding: "var(--spacing-sm)",
        caretColor: "var(--primary)",
      },
      ".cm-cursor": {
        borderLeftColor: "var(--primary)",
        borderLeftWidth: "2px",
      },
      ".cm-gutters": {
        backgroundColor: "var(--muted)",
        color: "var(--muted-foreground)",
        border: "none",
        borderRight: "1px solid var(--border)",
      },
      ".cm-activeLineGutter": {
        backgroundColor: "var(--accent)",
        color: "var(--accent-foreground)",
      },
      ".cm-activeLine": {
        backgroundColor: "oklch(from var(--primary) l c h / 0.05)",
      },
      ".cm-selectionBackground, ::selection": {
        backgroundColor: "oklch(from var(--primary) l c h / 0.2) !important",
      },
      ".cm-focused .cm-selectionBackground": {
        backgroundColor: "oklch(from var(--primary) l c h / 0.3) !important",
      },
      ".cm-scroller": {
        overflow: "auto",
      },
      ".cm-placeholder": {
        color: "var(--muted-foreground)",
        fontStyle: "italic",
      },
    });

    // Syntax highlighting using theme chart colors
    const highlighting = HighlightStyle.define([
      { tag: t.keyword, color: "var(--primary)", fontWeight: "600" },
      { tag: t.string, color: "var(--chart-3)" },
      { tag: t.number, color: "var(--chart-4)" },
      { tag: t.comment, color: "var(--muted-foreground)", fontStyle: "italic" },
      { tag: t.operator, color: "var(--accent-foreground)" },
      { tag: t.function(t.variableName), color: "var(--chart-2)" },
      { tag: t.typeName, color: "var(--chart-1)" },
      { tag: t.bool, color: "var(--chart-5)" },
      { tag: t.null, color: "var(--muted-foreground)" },
    ]);

    // Cmd/Ctrl+Enter to execute
    const executeKeymap = keymap.of([
      {
        key: "Mod-Enter",
        run: () => {
          this.dispatchEvent(
            new CustomEvent("editor-execute", {
              bubbles: true,
              composed: true,
            })
          );
          return true;
        },
      },
    ]);

    this._editor = new EditorView({
      doc: this.initialValue,
      parent: this._container,
      extensions: [
        basicSetup,
        sql({ dialect: SQLite }),
        theme,
        syntaxHighlighting(highlighting),
        executeKeymap,
        EditorView.lineWrapping,
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            this.dispatchEvent(
              new CustomEvent("editor-change", {
                detail: update.state.doc.toString(),
                bubbles: true,
                composed: true,
              })
            );
          }
        }),
        EditorView.editable.of(!this.readOnly),
      ],
    });

    // Emit initial value so Datastar signal is populated
    if (this.initialValue) {
      this.dispatchEvent(
        new CustomEvent("editor-change", {
          detail: this.initialValue,
          bubbles: true,
          composed: true,
        })
      );
    }

    // Expose for debugging
    (window as any).sqlEditor = this._editor;
  }

  override render() {
    return null; // Light DOM - container created imperatively
  }
}

declare global {
  interface HTMLElementTagNameMap {
    "sql-editor": SqlEditor;
  }
}
