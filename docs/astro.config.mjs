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
          href: "https://github.com/withastro/starlight",
        },
      ],
      sidebar: [
        {
          label: "Guides",
          items: [
            // Each item here is one entry in the navigation menu.
            { label: "Example Guide", slug: "guides/example" },
          ],
        },
        {
          label: "Reference",
          autogenerate: { directory: "reference" },
        },
      ],
    }),
  ],
});
