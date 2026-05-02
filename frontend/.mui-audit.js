const fs = require('fs'), path = require('path');
function walk(d, o = []) {
  for (const e of fs.readdirSync(d, { withFileTypes: true })) {
    const p = path.join(d, e.name);
    if (e.isDirectory()) walk(p, o);
    else if (/\.(js|jsx)$/.test(e.name)) o.push(p);
  }
  return o;
}
const files = walk('src');
const re = /import\s+([^;]+?)\s+from\s+['"]@mui\/(material|icons-material|system|styles)(?:\/([^'"]+))?['"]/gs;
const need = {};
for (const f of files) {
  const s = fs.readFileSync(f, 'utf8');
  let m;
  while ((m = re.exec(s))) {
    const pkg = '@mui/' + m[2] + (m[3] ? '/' + m[3] : '');
    const spec = m[1].trim();
    need[pkg] = need[pkg] || { named: new Set(), dflt: false, files: new Set() };
    need[pkg].files.add(f);
    const bm = spec.match(/\{([\s\S]+)\}/);
    if (bm) {
      bm[1].split(',').map(x => x.trim().split(/\s+as\s+/)[0].trim()).filter(Boolean)
        .forEach(x => need[pkg].named.add(x));
    }
    const before = spec.split('{')[0].replace(/,/g, '').trim();
    if (before) need[pkg].dflt = true;
  }
}
let bad = 0;
for (const [k, v] of Object.entries(need)) {
  let mod;
  try { mod = require(k); }
  catch (e) { console.error('CANNOT REQUIRE', k, '-', e.message); bad++; continue; }
  for (const n of v.named) {
    if (!(n in mod)) { console.error('MISSING NAMED', k, '->', n, '(eg', [...v.files][0] + ')'); bad++; }
  }
  if (v.dflt && !mod.default && typeof mod !== 'function') {
    console.error('MISSING DEFAULT', k);
    bad++;
  }
}
console.log('checked', Object.keys(need).length, 'mui import paths;', bad, 'problems');
process.exit(bad ? 1 : 0);
