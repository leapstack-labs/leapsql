import * as esbuild from "esbuild";

const isDev = process.argv.includes("--dev");

const commonOptions = {
  bundle: true,
  format: "esm",
  minify: !isDev,
  sourcemap: isDev,
  define: {
    "process.env.NODE_ENV": isDev ? '"development"' : '"production"',
  },
  metafile: true,
};

// Build sql-editor.ts
await esbuild
  .build({
    ...commonOptions,
    entryPoints: ["src/sql-editor.ts"],
    outfile: "static/js/sql-editor.js",
  })
  .then((result) => {
    const output = Object.values(result.metafile.outputs)[0];
    const sizeKb = (output.bytes / 1024).toFixed(1);
    console.log(`Built: static/js/sql-editor.js (${sizeKb}kb)`);
  });
