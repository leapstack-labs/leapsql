Here is the comprehensive summary of our architecture and findings for building the LeapSQL LSP with `glsp` and `sqlite`. This blueprint focuses on achieving a "magic" developer experience by leveraging Go's performance and a robust state management strategy.

### 1. Core Philosophy: "Zero Config, Zero Waiting"

This is the most critical part of the developer experience. If you get this right, the tool feels "magic." If you get it wrong, it feels "broken" or "slow."

Since you are writing this in Go (which is extremely fast), you have a luxury that Python-based tools (like standard dbt) do not: you can afford to run "heavy" tasks more often.

**The Ideal User Flow:**

1.  **User Opens VS Code:** They see their project immediately.
2.  **Immediate Feedback:** A small spinner in the status bar says `LeapSQL: Indexing...`.
3.  **Non-Blocking:** They can start reading code instantly. If they hover over a table _before_ indexing is done, they might not get a result, but the editor doesn't freeze.
4.  **Completion:** After < 1 second (thanks to Go), the spinner stops. Now `Go to Definition` works instantly.
5.  **User Edits & Saves:** They hit `Cmd+S`. The status bar flashes `LeapSQL: Updating...` for 200ms. Done.

---

### 2. Architecture & State Management

#### A. Storage: Single SQLite Database (WAL Mode)

We use one SQLite database (`leapsql.db`) as both the **Project State** and **LSP Cache**.

- **Mode:** Must use **WAL (Write-Ahead Logging)** (`PRAGMA journal_mode=WAL;`) to allow concurrent reads (LSP lookups) while writing (Discovery).
- **Schema:**
  - `models`: `id`, `name`, `file_path`, `content_hash`, `status`
  - `columns`: `model_id`, `name`, `line_number`, `type` (Critical for accurate "Go to Definition")
  - `lineage`: `source_model_id`, `target_model_id`

#### B. The Discovery Strategy: Flat Inverted Index

We avoid complex Merkle Trees. Instead, we use a **Flat Hash Map** approach which is simpler and incredibly fast in Go.

- **Logic:** When `discover` runs, iterate through the file list.
- **Optimization:** Store a `content_hash` (MD5/SHA256) of the file content in SQLite.
  - **Match?** Skip parsing (99% of files).
  - **Mismatch?** Re-parse only this file and update its row.
- **Graph Resolution:** Re-run dependency linking on the whole set (fast in memory) after parsing.

---

### 3. The 3 Triggers for `Discover`

Automation is key to DX. Do not rely on manual user commands.

#### Trigger A: `Initialize` (On Startup)

- **Event:** The LSP starts.
- **Action:**
  1.  Check for existing `leapsql.db`.
  2.  **If yes:** Load it immediately so the user has _some_ features (even if stale).
  3.  **Then:** Launch a background `go routine` to run `discover` and ensure freshness.
- **DX Benefit:** Instant startup feeling.

#### Trigger B: `textDocument/didSave` (On Save)

- **Event:** User saves a file (e.g., `revenue.sql`).
- **Action:** Run the incremental `discover` process.
- **Why:** Ensures the lineage graph is always chemically pure and correct on disk. In Go, checking 500 files takes < 500ms.

#### Trigger C: Manual Command (The Escape Hatch)

- **Event:** User runs "LeapSQL: Restart & Rebuild" command.
- **Action:** Force kill processes, wipe the DB cache, and run a fresh `discover`.
- **Why:** Fixes corrupted state without restarting the editor.

---

### 4. Handling "Dirty" (Unsaved) Files

We distinguish between the **Global Graph** (Saved State) and **Local Diagnostics** (Unsaved State).

- **Global Graph:** Only cares about **Saved** files. If a model isn't saved, it doesn't exist in the lineage.
- **Local Diagnostics:** On `textDocument/didChange` (Debounced ~300ms):
  1.  Run a **lightweight parser** on the active file's unsaved text.
  2.  Check for **Syntax Errors**.
  3.  Check dependencies against the **Saved SQLite Index**.
  4.  **Result:** Immediate red squiggles if referencing a non-existent table, _without_ updating the database.

---

### 5. Best Practices Checklist for DX

1.  **Status Bar is Mandatory:** Use LSP `$/progress` to show "Indexing..." or "Updating...". Users panic if they don't know what's happening.
2.  **Graceful Degradation:** If `revenue.sql` has a syntax error:
    - Log `status = 'error'` in SQLite.
    - **Keep** old valid data for other files.
    - **Do not crash** the discovery process; allow the rest of the project to function.
3.  **Debounce:** Wait ~300ms after typing stops before running the local syntax check to avoid error spam.

### 6. Next Steps: Semantic Hashing (Level 2 Optimization)

Once the "Flat Hash" (MD5 of file content) is working, upgrade to **Semantic Hashing** for even better performance.

- **The Problem:** Adding a comment or changing indentation changes the file hash, triggering a re-parse, even though logic didn't change.
- **The Solution:**
  1.  Parse the file into an AST.
  2.  Compute a hash of the **AST Structure** (ignoring whitespace/comments).
  3.  If `FileHash` changes but `ASTHash` remains the same: Update the file hash in DB, but **SKIP** the heavy lineage resolution steps.

Would you like to start by setting up the `glsp` server skeleton, or would you prefer to refine the `discover` function to include the `content_hash` logic first?
