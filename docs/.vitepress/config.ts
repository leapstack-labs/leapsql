import type { DefaultTheme } from 'vitepress'
import UnoCSS from 'unocss/vite'
import { defineConfig } from 'vitepress'
import llmstxt, { copyOrDownloadAsMarkdownButtons } from 'vitepress-plugin-llms'
import { description, github, name, ogImage, ogUrl } from './meta'

export default defineConfig({
  title: name,
  description,
  base: '/leapsql/',
  head: [
    ['link', { rel: 'icon', href: '/favicon.svg', type: 'image/svg+xml' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:url', content: ogUrl }],
    ['meta', { property: 'og:title', content: name }],
    ['meta', { property: 'og:description', content: description }],
    ['meta', { property: 'og:image', content: ogImage }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
  ],

  vite: {
    plugins: [UnoCSS(), llmstxt()],
  },

  themeConfig: {
    logo: '/favicon.svg',

    nav: [
      { text: 'Guide', link: '/introduction' },
      { text: 'CLI', link: '/cli/' },
      { text: 'Concepts', link: '/concepts/models' },
    ],

    sidebar: sidebar(),

    socialLinks: [
      { icon: 'github', link: github },
    ],

    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2024-PRESENT',
    },

    search: {
      provider: 'local',
    },
  },
  markdown: {
    config(md) {
      md.use(copyOrDownloadAsMarkdownButtons)
    },
  },
})

function sidebar(): DefaultTheme.Sidebar {
  return {
    '/': [
      {
        text: 'Getting Started',
        items: [
          { text: 'Introduction', link: '/introduction' },
          { text: 'Quickstart', link: '/quickstart' },
          { text: 'Installation', link: '/installation' },
          { text: 'Project Structure', link: '/project-structure' },
        ],
      },
      {
        text: 'Concepts',
        items: [
          { text: 'Models', link: '/concepts/models' },
          { text: 'Frontmatter', link: '/concepts/frontmatter' },
          { text: 'Materializations', link: '/concepts/materializations' },
          { text: 'Dependencies', link: '/concepts/dependencies' },
          { text: 'Seeds', link: '/concepts/seeds' },
        ],
      },
      {
        text: 'Templating',
        items: [
          { text: 'Overview', link: '/templating/overview' },
          { text: 'Expressions', link: '/templating/expressions' },
          { text: 'Control Flow', link: '/templating/control-flow' },
          { text: 'Global Variables', link: '/templating/globals' },
        ],
      },
      {
        text: 'Macros',
        items: [
          { text: 'Overview', link: '/macros/overview' },
          { text: 'Writing Macros', link: '/macros/writing-macros' },
          { text: 'Using Macros', link: '/macros/using-macros' },
          { text: 'Built-in Functions', link: '/macros/builtins' },
        ],
      },
      {
        text: 'Lineage',
        items: [
          { text: 'Overview', link: '/lineage/overview' },
          { text: 'Table Lineage', link: '/lineage/table-lineage' },
          { text: 'Column Lineage', link: '/lineage/column-lineage' },
        ],
      },
      {
        text: 'State Management',
        items: [
          { text: 'Overview', link: '/state/overview' },
          { text: 'Run History', link: '/state/runs' },
        ],
      },
      {
        text: 'CLI Reference',
        items: [
          { text: 'Overview', link: '/cli/' },
          { text: 'run', link: '/cli/run' },
          { text: 'list', link: '/cli/list' },
          { text: 'dag', link: '/cli/dag' },
          { text: 'seed', link: '/cli/seed' },
          { text: 'docs', link: '/cli/docs' },
        ],
      },
      {
        text: 'Adapters',
        items: [
          { text: 'Overview', link: '/adapters/' },
          { text: 'DuckDB', link: '/adapters/duckdb' },
        ],
      },
    ],
  }
}
