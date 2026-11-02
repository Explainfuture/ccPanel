import { build } from 'esbuild';
import { existsSync, mkdirSync,readFileSync,writeFileSync } from 'node:fs';
mkdirSync('dist', { recursive: true });
const version=readFileSync('packages/server/src/migrate.ts','utf8').match(/^export const schemaVersion=(\d+);$/m)?.[1];
if(!version)throw new Error('Missing explicit migration version');
writeFileSync('dist/schema-version.txt',version+'\n');
for (const [name, entry] of Object.entries({ api: 'apps/api/src/main.ts', worker: 'apps/worker/src/main.ts', migrate: 'packages/server/src/migrate.ts', bootstrap: 'packages/server/src/bootstrap.ts', schema: 'packages/schema/src/index.ts', 'deletion-ledger':'packages/server/src/deletion-ledger.ts','pressure-observe':'scripts/pressure-observe.ts','ingest-preflight':'scripts/ingest-preflight.ts','consumer-progress':'scripts/consumer-progress-cli.ts','kafka-durability':'scripts/kafka-durability-check.ts','company-provision':'scripts/company-provision-cli.ts' })) {
  if (existsSync(entry)) await build({ entryPoints: [entry], outfile: `dist/${name}.mjs`, bundle: true, platform: 'node', target: 'node22', format: 'esm', packages: 'external', sourcemap: true });
}
if (existsSync('apps/web/vite.config.ts')) {
  const { build: buildWeb } = await import('vite');
  await buildWeb({ configFile: 'apps/web/vite.config.ts' });
}
if (existsSync('packages/sdk-browser/src/index.ts')) await import('./build-sdk.mjs');
