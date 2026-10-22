import { build } from 'esbuild';
import { execFileSync } from 'node:child_process';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { readFileSync } from 'node:fs';
import { gzipSync } from 'node:zlib';
const root=resolve(fileURLToPath(new URL('..',import.meta.url)));
for(const name of ['browser','node']){
  const dir=resolve(root,`packages/sdk-${name}`);
  await build({entryPoints:[dir+'/src/index.ts'],outfile:dir+'/dist/index.js',bundle:true,format:'esm',platform:name==='browser'?'browser':'node',target:name==='browser'?'es2022':'node22',minify:true});
  execFileSync(process.execPath,[resolve(root,'node_modules/typescript/bin/tsc'),'-p',dir+'/tsconfig.build.json'],{stdio:'inherit'});
  console.log(`sdk-${name} gzip bytes: ${gzipSync(readFileSync(dir+'/dist/index.js')).length}`);
}
