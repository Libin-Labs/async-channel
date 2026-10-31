import { cp, mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const tsc = join(root, 'node_modules', 'typescript', 'bin', 'tsc');

for (const config of ['tsconfig.json', 'tsconfig.cjs.json']) {
  const result = spawnSync(process.execPath, [tsc, '-p', join(root, config)], {
    cwd: root,
    stdio: 'inherit',
  });
  if (result.status !== 0) process.exit(result.status ?? 1);
}

const esm = join(root, 'dist', 'esm');
const cjs = join(root, 'dist', 'cjs');
await mkdir(cjs, { recursive: true });
await writeFile(join(cjs, 'package.json'), '{"type":"commonjs"}\n');
async function copyDeclarations(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const source = join(directory, entry.name);
    if (entry.isDirectory()) {
      await copyDeclarations(source);
    } else if (entry.name.endsWith('.d.ts')) {
      const target = join(cjs, entry.name.replace(/\.d\.ts$/, '.d.cts'));
      await mkdir(dirname(target), { recursive: true });
      await cp(source, target);
    }
  }
}
await copyDeclarations(esm);
