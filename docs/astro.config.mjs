// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import catppuccin from "@catppuccin/starlight";

import theme from "toolbeam-docs-theme";

// https://astro.build/config
export default defineConfig({
  integrations: [
    starlight({
      plugins: [
        catppuccin({
          dark: { flavor: "mocha", accent: "peach" },
        }),
      ],

      // Custom component overrides
      components: {
        Header: "./src/components/Header.astro",
      },

      expressiveCode: {
        themes: ["catppuccin-mocha"],
      },

      title: "LeapSQL",

      customCss: ["./src/styles/custom.css"],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/yacobolo/leapsql",
        },
      ],
      sidebar: [
        {
          label: "Getting Started",
          items: [
            { label: "Introduction", slug: "introduction" },
            { label: "Quickstart", slug: "quickstart" },
            { label: "Installation", slug: "installation" },
            { label: "Project Structure", slug: "project-structure" },
          ],
        },
        {
          label: "Concepts",
          items: [
            { label: "Models", slug: "concepts/models" },
            { label: "Frontmatter", slug: "concepts/frontmatter" },
            { label: "Materializations", slug: "concepts/materializations" },
            { label: "Dependencies", slug: "concepts/dependencies" },
            { label: "Seeds", slug: "concepts/seeds" },
          ],
        },
        {
          label: "Templating",
          items: [
            { label: "Overview", slug: "templating/overview" },
            { label: "Expressions", slug: "templating/expressions" },
            { label: "Control Flow", slug: "templating/control-flow" },
            { label: "Global Variables", slug: "templating/globals" },
          ],
        },
        {
          label: "Macros",
          items: [
            { label: "Overview", slug: "macros/overview" },
            { label: "Writing Macros", slug: "macros/writing-macros" },
            { label: "Using Macros", slug: "macros/using-macros" },
            { label: "Built-in Functions", slug: "macros/builtins" },
          ],
        },
        {
          label: "Lineage",
          items: [
            { label: "Overview", slug: "lineage/overview" },
            { label: "Table Lineage", slug: "lineage/table-lineage" },
            { label: "Column Lineage", slug: "lineage/column-lineage" },
          ],
        },
        {
          label: "State Management",
          items: [
            { label: "Overview", slug: "state/overview" },
            { label: "Run History", slug: "state/runs" },
          ],
        },
        {
          label: "CLI Reference",
          items: [
            { label: "Overview", slug: "cli/overview" },
            { label: "run", slug: "cli/run" },
            { label: "list", slug: "cli/list" },
            { label: "dag", slug: "cli/dag" },
            { label: "seed", slug: "cli/seed" },
            { label: "docs", slug: "cli/docs" },
          ],
        },
        {
          label: "Adapters",
          items: [
            { label: "Overview", slug: "adapters/overview" },
            { label: "DuckDB", slug: "adapters/duckdb" },
          ],
        },
      ],
    }),
  ],
});
